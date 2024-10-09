#include "columnstore/columnstore_table.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "lake/lake.hpp"

namespace duckdb {

class DataFileWriter {
public:
    DataFileWriter(ClientContext &context, string file_name, vector<LogicalType> types, vector<string> names)
        : collection(context, types, ColumnDataAllocatorType::HYBRID),
          writer(context, FileSystem::GetFileSystem(context), std::move(file_name), std::move(types), std::move(names),
                 duckdb_parquet::format::CompressionCodec::SNAPPY /*codec*/, {} /*field_ids*/, {} /*kv_metadata*/,
                 {} /*encryption_config*/, 1.0 /*dictionary_compression_ratio_threshold*/, {} /*compression_level*/,
                 true /*debug_use_openssl*/) {
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
            writer = make_uniq<DataFileWriter>(context, path + file_name, types, names);
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
        idx_t size = 1;
        LakeAddFile(oid, file_name.c_str(), size);
    }

private:
    Oid oid;
    ColumnstoreMetadata &metadata;
    string path;
    string file_name;
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

void ColumnstoreTable::Delete(ClientContext &context, vector<row_t> &row_ids) {
    std::sort(row_ids.begin(), row_ids.end());
    auto path = metadata->TablesSearch(oid);
    auto data_files = metadata->DataFilesSearch(oid);
    for (idx_t row_ids_index = 0; row_ids_index < row_ids.size();) {
        int32_t file_number = row_ids[row_ids_index] >> 32;
        uint32_t next_file_row_number = row_ids[row_ids_index] & 0xFFFFFFFF;
        ParquetOptions parquet_options;
        ParquetReader reader(context, path + data_files[file_number], parquet_options, nullptr /*metadata*/);
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
        metadata->DataFilesDelete(data_files[file_number]);
    }
    FinalizeInsert();
}

} // namespace duckdb
