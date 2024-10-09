#include "columnstore/columnstore_table.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"

#include "lake/lake.hpp"
#include <sys/stat.h>

extern bool enable_local_cache;
namespace duckdb {

const int64_t x_min_available_space = 1024 * 1024 * 1024;

constexpr const char *CACHE_DIRECTORY = "mooncake_cache/";

class SingleFileCachedWriteFileSystem : public FileSystem {
public:
    SingleFileCachedWriteFileSystem(ClientContext &context, const std::string &file_name)
        : tmp_file_path(CACHE_DIRECTORY + file_name), file_size(0), internal(GetFileSystem(context)) {}

    unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
                                    optional_ptr<FileOpener> opener = nullptr) {
        if (FileSystem::IsRemoteFile(path) && enable_local_cache) {
            auto disk_space = internal.GetAvailableDiskSpace(CACHE_DIRECTORY);
            if (disk_space.IsValid() && disk_space.GetIndex() > x_min_available_space) {
                tmp_file = internal.OpenFile(tmp_file_path, flags, opener);
            }
        }
        return internal.OpenFile(path, flags, opener);
    }
    int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
        file_size += nr_bytes;
        if (tmp_file) {
            int64_t written = internal.Write(*tmp_file, buffer, nr_bytes);
            assert(written == nr_bytes);
        }
        return internal.Write(handle, buffer, nr_bytes);
    }
    void CacheFile() {
        if (tmp_file) {
            tmp_file->Close();
        }
    }
    std::string GetName() const {
        return "SingleFileCachedWriteFileSystem";
    }
    idx_t GetCurrentFileSize() const {
        return file_size;
    }

private:
    std::string tmp_file_path;
    idx_t file_size;
    unique_ptr<FileHandle> tmp_file;
    FileSystem &internal;
};

class DataFileWriter {
public:
    DataFileWriter(ClientContext &context, FileSystem &fs, string file_name, vector<LogicalType> types,
                   vector<string> names, ChildFieldIDs fieldIds)
        : collection(context, types, ColumnDataAllocatorType::HYBRID),
          writer(context, fs, std::move(file_name), std::move(types), std::move(names),
                 duckdb_parquet::format::CompressionCodec::SNAPPY /*codec*/, std::move(fieldIds) /*field_ids*/,
                 {} /*kv_metadata*/, {} /*encryption_config*/, 1.0 /*dictionary_compression_ratio_threshold*/,
                 {} /*compression_level*/, true /*debug_use_openssl*/) {
        collection.InitializeAppend(append_state);
    }

public:
    // Return true if needs to rotate to a new data file
    bool Write(DataChunk &chunk) {
        collection.Append(append_state, chunk);
        if (collection.Count() >= x_row_group_size || collection.SizeInBytes() >= x_row_group_size_bytes) {
            writer.Flush(collection);
            append_state.current_chunk_state.handles.clear();
            collection.InitializeAppend(append_state);
            return writer.FileSize() >= x_file_size_bytes;
        }
        return false;
    }

    void Finalize() {
        writer.Flush(collection);
        writer.Finalize();
    }

private:
    static const idx_t x_row_group_size = duckdb::Storage::ROW_GROUP_SIZE;
    static const idx_t x_row_group_size_bytes = x_row_group_size * 1024;
    static const idx_t x_file_size_bytes = 1 << 30;

    ColumnDataCollection collection;
    ColumnDataAppendState append_state;
    ParquetWriter writer;
};

class ColumnstoreWriter {
public:
    ColumnstoreWriter(Oid oid, ColumnstoreMetadata &metadata, vector<LogicalType> types, vector<string> names)
        : oid(oid), metadata(metadata), path(metadata.TablesSearch(oid)), types(std::move(types)),
          names(std::move(names)) {}

public:
    void Write(ClientContext &context, DataChunk &chunk) {
        if (!writer) {
            file_name = UUID::ToString(UUID::GenerateRandomUUID()) + ".parquet";
            ChildFieldIDs fieldIds;
            for (int col = 0; col < names.size(); col++) {
                fieldIds.ids->insert(std::make_pair(names[col], duckdb::FieldID(col)));
            }
            m_file_system = make_uniq<SingleFileCachedWriteFileSystem>(context, file_name);
            writer =
                make_uniq<DataFileWriter>(context, *m_file_system, path + file_name, types, names, std::move(fieldIds));
        }
        if (writer->Write(chunk)) {
            FinalizeDataFile();
        }
    }

    void Finalize() {
        if (writer) {
            FinalizeDataFile();
        }
    }

private:
    void FinalizeDataFile() {
        writer->Finalize();
        writer.reset();
        metadata.DataFilesInsert(oid, file_name.c_str());
        m_file_system->CacheFile();
        duckdb::idx_t size = m_file_system->GetCurrentFileSize();
        LakeAddFile(oid, file_name.c_str(), size);
        m_file_system.reset();
    }

private:
    Oid oid;
    ColumnstoreMetadata &metadata;
    string path;
    string file_name;
    unique_ptr<SingleFileCachedWriteFileSystem> m_file_system;
    vector<LogicalType> types;
    vector<string> names;
    unique_ptr<DataFileWriter> writer;
};

ColumnstoreTable::ColumnstoreTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Oid oid,
                                   Snapshot snapshot)
    : TableCatalogEntry(catalog, schema, info), oid(oid), metadata(make_uniq<ColumnstoreMetadata>(snapshot)) {}

ColumnstoreTable::~ColumnstoreTable() {
    D_ASSERT(!writer);
}

// HACK: force update_is_del_and_insert
TableStorageInfo ColumnstoreTable::GetStorageInfo(ClientContext &context) {
    IndexInfo index_info;
    for (column_t i = 0; i < columns.LogicalColumnCount(); i++) {
        index_info.column_set.insert(i);
    }
    TableStorageInfo result;
    result.index_info.push_back(std::move(index_info));
    return result;
}

void ColumnstoreTable::Insert(ClientContext &context, DataChunk &chunk) {
    if (!writer) {
        writer = make_uniq<ColumnstoreWriter>(oid, *metadata, columns.GetColumnTypes(), columns.GetColumnNames());
    }
    writer->Write(context, chunk);
}

void ColumnstoreTable::FinalizeInsert() {
    if (writer) {
        writer->Finalize();
        writer.reset();
    }
}

void BuildActualFilePath(std::vector<ColumnstoreMetadata::FileInfo> &files, const string &path) {
    if (enable_local_cache && FileSystem::IsRemoteFile(path)) {
        for (auto &file : files) {
            struct stat info;
            string cached_file_path = CACHE_DIRECTORY + file.file_name;
            if (stat(cached_file_path.c_str(), &info) == 0) {
                file.file_path = std::move(cached_file_path);
            } else {
                file.file_path = path + file.file_name;
            }
        }
    } else {
        for (auto &file : files) {
            file.file_path = path + file.file_name;
        }
    }
}

void ColumnstoreTable::Delete(ClientContext &context, vector<row_t> &row_ids) {
    std::sort(row_ids.begin(), row_ids.end());
    auto path = metadata->TablesSearch(oid);
    auto data_files = metadata->DataFilesSearch(oid);
    BuildActualFilePath(data_files, path);

    for (idx_t row_ids_index = 0; row_ids_index < row_ids.size();) {
        int32_t file_number = row_ids[row_ids_index] >> 32;
        uint32_t next_file_row_number = row_ids[row_ids_index] & 0xFFFFFFFF;
        ParquetOptions parquet_options;
        ParquetReader reader(context, data_files[file_number].file_path, parquet_options, nullptr /*metadata*/);
        for (idx_t i = 0; i < reader.GetTypes().size(); i++) {
            reader.reader_data.column_mapping.push_back(i);
            reader.reader_data.column_ids.push_back(i);
        }
        ParquetReaderScanState state;
        vector<idx_t> groups_to_read(reader.GetFileMetadata()->row_groups.size());
        std::iota(groups_to_read.begin(), groups_to_read.end(), 0);
        reader.InitializeScan(context, state, std::move(groups_to_read));

        DataChunk chunk;
        chunk.Initialize(context, reader.GetTypes());
        SelectionVector sel;
        sel.Initialize(STANDARD_VECTOR_SIZE);
        uint32_t file_row_number = 0;
        reader.Scan(state, chunk);
        while (chunk.size()) {
            idx_t sel_size = 0;
            for (idx_t chunk_row_number = 0; chunk_row_number < chunk.size(); chunk_row_number++, file_row_number++) {
                if (file_row_number == next_file_row_number) {
                    row_ids_index++;
                    if (row_ids_index < row_ids.size() && row_ids[row_ids_index] >> 32 == file_number) {
                        next_file_row_number = row_ids[row_ids_index] & 0xFFFFFFFF;
                    }
                } else {
                    sel.set_index(sel_size++, chunk_row_number);
                }
            }
            chunk.Slice(sel, sel_size);
            if (chunk.size()) {
                Insert(context, chunk);
            }
            chunk.Reset();
            reader.Scan(state, chunk);
        }
        metadata->DataFilesDelete(data_files[file_number].file_id);
        LakeDeleteFile(oid, data_files[file_number].file_name.c_str());
    }
    FinalizeInsert();
}

} // namespace duckdb
