#include "columnstore/columnstore_table.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "lake/lake.hpp"
#include "parquet_reader.hpp"
#include "parquet_writer.hpp"
#include "pgmooncake_guc.hpp"

namespace duckdb {

const char *x_mooncake_local_cache = "mooncake_local_cache/";

void ColumnstoreStats::AddStats(const string &column, BaseStatistics &col_stats) {
    if (stats_map.count(column) == 0) {
        stats_map[column] = col_stats.ToUnique();
    } else {
        stats_map[column]->Merge(col_stats);
    }
}

BaseStatistics *ColumnstoreStats::GetStats(const string &column) {
    if (stats_map.count(column)) {
        return stats_map[column].get();
    }
    return nullptr;
}

void ColumnstoreStats::Serialize(Serializer &serializer) {
    auto it = stats_map.begin();
    serializer.WriteList(100, "stats_map", stats_map.size(), [&](Serializer::List &list, idx_t id) {
        list.WriteObject([&](Serializer &obj) {
            obj.WriteProperty(200, "column", it->first);
            it->second->Serialize(obj);
        });
        it++;
    });
};

ColumnstoreStats ColumnstoreStats::Deserialize(Deserializer &deserializer) {
    ColumnstoreStats ret;
    vector<string> c;
    vector<unique_ptr<BaseStatistics>> s;
    deserializer.ReadList(100, "stats_map", [&](Deserializer::List &list, idx_t id) {
        list.ReadObject([&](Deserializer &obj) {
            string column;
            obj.ReadProperty(200, "column", column);
            auto stats = BaseStatistics::Deserialize(obj);
            c.push_back(column);
            s.push_back(stats.ToUnique());
        });
    });
    for (int i = 0; i < c.size(); i++) {
        ret.stats_map[c[i]].swap(s[i]);
    }
    return ret;
}

class SingleFileCachedWriteFileSystem : public FileSystem {
public:
    SingleFileCachedWriteFileSystem(ClientContext &context, const string &file_name)
        : fs(GetFileSystem(context)), cached_file_path(x_mooncake_local_cache + file_name), file_size(0) {}

public:
    unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
                                    optional_ptr<FileOpener> opener = nullptr) override {
        if (IsRemoteFile(path) && mooncake_enable_local_cache) {
            auto disk_space = fs.GetAvailableDiskSpace(x_mooncake_local_cache);
            if (disk_space.IsValid() && disk_space.GetIndex() > x_min_disk_space) {
                cached_file = fs.OpenFile(cached_file_path, flags, opener);
            }
        }
        return fs.OpenFile(path, flags, opener);
    }

    int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
        file_size += nr_bytes;
        if (cached_file) {
            int64_t bytes_written = fs.Write(*cached_file, buffer, nr_bytes);
            D_ASSERT(bytes_written == nr_bytes);
        }
        return fs.Write(handle, buffer, nr_bytes);
    }

    string GetName() const override {
        return "SingleFileCachedWriteFileSystem";
    }

    idx_t GetFileSize() const {
        return file_size;
    }

private:
    static const idx_t x_min_disk_space = 1024 * 1024 * 1024;

    FileSystem &fs;
    string cached_file_path;
    unique_ptr<FileHandle> cached_file;
    idx_t file_size;
};

class DataFileWriter {
public:
    DataFileWriter(ClientContext &context, FileSystem &fs, string file_name, vector<LogicalType> types,
                   vector<string> names, ChildFieldIDs field_ids)
        : collection(context, types, ColumnDataAllocatorType::HYBRID),
          writer(context, fs, std::move(file_name), types, names,
                 duckdb_parquet::format::CompressionCodec::SNAPPY /*codec*/, std::move(field_ids), {} /*kv_metadata*/,
                 {} /*encryption_config*/, 1.0 /*dictionary_compression_ratio_threshold*/, {} /*compression_level*/,
                 true /*debug_use_openssl*/),
          types(std::move(types)), names(std::move(names)) {
        collection.InitializeAppend(append_state);
    }

public:
    // Return true if needs to rotate to a new data file
    bool Write(DataChunk &chunk) {
        collection.Append(append_state, chunk);
        if (collection.Count() >= x_row_group_size || collection.SizeInBytes() >= x_row_group_size_bytes) {
            FlushRowGroup();
            append_state.current_chunk_state.handles.clear();
            collection.InitializeAppend(append_state);
            return writer.FileSize() >= x_file_size_bytes;
        }
        return false;
    }

    void FlushRowGroup() {
        if (collection.Count() == 0) {
            return;
        }
        PreparedRowGroup prepared_row_group;
        writer.PrepareRowGroup(collection, prepared_row_group);
        collection.Reset();
        writer.FlushRowGroup(prepared_row_group);
        for (int i = 0; i < types.size(); i++) {
            auto type = types[i];
            auto parquet_stats = prepared_row_group.row_group.columns[i].meta_data.statistics;
            duckdb_parquet::format::SchemaElement schema_ele;
            schema_ele.type = ParquetWriter::DuckDBTypeToParquetType(type);
            ParquetWriter::SetSchemaProperties(type, schema_ele);
            if (types[i].IsNumeric() || types[i].IsTemporal() || types[i].id() == LogicalTypeId::DECIMAL) {
                Value min;
                Value max;
                min =
                    ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.min_value).DefaultCastAs(type);
                max =
                    ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.max_value).DefaultCastAs(type);
                auto group_stats = NumericStats::CreateUnknown(type);
                NumericStats::SetMin(group_stats, min);
                NumericStats::SetMax(group_stats, max);
                group_stats.Set(StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES);
		        if (parquet_stats.__isset.null_count && parquet_stats.null_count == 0) {
			        group_stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		        }
                stats.AddStats(names[i], group_stats);
            }
        }
    }

    void Finalize() {
        FlushRowGroup();
        writer.Finalize();
    }

    void WriteStats(Serializer &serializer) {
        stats.Serialize(serializer);
    }

private:
    static const idx_t x_row_group_size = duckdb::Storage::ROW_GROUP_SIZE;
    static const idx_t x_row_group_size_bytes = x_row_group_size * 1024;
    static const idx_t x_file_size_bytes = 1 << 30;

    ColumnDataCollection collection;
    ColumnDataAppendState append_state;
    ParquetWriter writer;
    vector<LogicalType> types;
    vector<string> names;
    ColumnstoreStats stats;
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
            fs = make_uniq<SingleFileCachedWriteFileSystem>(context, file_name);
            ChildFieldIDs field_ids;
            for (idx_t i = 0; i < names.size(); i++) {
                (*field_ids.ids)[names[i]] = duckdb::FieldID(i);
            }
            writer = make_uniq<DataFileWriter>(context, *fs, path + file_name, types, names, std::move(field_ids));
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
        MemoryStream stream;
        BinarySerializer stats(stream);
        stats.Begin();
        writer->WriteStats(stats);
        stats.End();
        writer.reset();
        idx_t file_size = fs->GetFileSize();
        fs.reset();
        metadata.DataFilesInsert(oid, file_name.c_str(), reinterpret_cast<const char *>(stream.GetData()),
                                 stream.GetPosition());
        LakeAddFile(oid, file_name, file_size);
    }

private:
    Oid oid;
    ColumnstoreMetadata &metadata;
    string path;
    string file_name;
    vector<LogicalType> types;
    vector<string> names;
    unique_ptr<SingleFileCachedWriteFileSystem> fs;
    unique_ptr<DataFileWriter> writer;
};

ColumnstoreTable::ColumnstoreTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Oid oid,
                                   Snapshot snapshot)
    : TableCatalogEntry(catalog, schema, info), oid(oid), metadata(make_uniq<ColumnstoreMetadata>(snapshot)) {}

ColumnstoreTable::~ColumnstoreTable() = default;

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

void ColumnstoreTable::Delete(ClientContext &context, unordered_set<row_t> &row_ids_set) {
    vector<row_t> row_ids(row_ids_set.begin(), row_ids_set.end());
    std::sort(row_ids.begin(), row_ids.end());
    auto path = metadata->TablesSearch(oid);
    auto file_names = metadata->DataFilesSearch(oid);
    auto file_paths = GetFilePaths(path, file_names);

    for (idx_t row_ids_index = 0; row_ids_index < row_ids.size();) {
        int32_t file_number = row_ids[row_ids_index] >> 32;
        uint32_t next_file_row_number = row_ids[row_ids_index] & 0xFFFFFFFF;
        ParquetOptions parquet_options;
        ParquetReader reader(context, file_paths[file_number], parquet_options, nullptr /*metadata*/);
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
        SelectionVector sel(STANDARD_VECTOR_SIZE);
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
        metadata->DataFilesDelete(file_names[file_number]);
        LakeDeleteFile(oid, file_names[file_number]);
    }
    FinalizeInsert();
}

vector<string> ColumnstoreTable::GetFilePaths(const string &path, const vector<string> &file_names) {
    vector<string> file_paths;
    if (mooncake_enable_local_cache && FileSystem::IsRemoteFile(path)) {
        auto local_fs = FileSystem::CreateLocal();
        for (auto &file_name : file_names) {
            string cached_file_path = x_mooncake_local_cache + file_name;
            if (local_fs->FileExists(cached_file_path)) {
                file_paths.push_back(cached_file_path);
            } else {
                file_paths.push_back(path + file_name);
            }
        }
    } else {
        for (auto &file_name : file_names) {
            file_paths.push_back(path + file_name);
        }
    }
    return file_paths;
}

} // namespace duckdb
