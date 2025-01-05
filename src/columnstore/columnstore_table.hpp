#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "pgduckdb/pg/declarations.hpp"

namespace duckdb {

class ColumnDataCollection;
class ColumnstoreMetadata;
class ColumnstoreWriter;
class DataChunk;

class ColumnstoreTable : public TableCatalogEntry {
public:
    ColumnstoreTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Oid oid, Snapshot snapshot);

    ~ColumnstoreTable() override;

public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
        throw NotImplementedException("GetStatistics not supported yet");
    }

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

public:
    void Insert(ClientContext &context, DataChunk &chunk);

    void FinalizeInsert();

    void Delete(ClientContext &context, unordered_set<row_t> &row_ids_set,
                ColumnDataCollection *return_collection = nullptr);

private:
    vector<string> GetFilePaths(const string &path, const vector<string> &file_names);
    vector<string> GetFilePathsAndWarmCache(const string &path, const vector<string> &file_names);

    // Read from [data_file] to [cache_file] on local filesystem.
    // Return whether the read operation succeeds or not.
    bool FetchReadCache(const string &data_file, const string &cache_file);

    // TODO(hjiang): Should use string view instead of string here.
    //
    // Fetch files to prefetch in parallel.
    // Return those successfully fetched cache file. Failed ones will be cleaned up.
    unordered_set<string> ParallelFetchReadCache(const unordered_map<string, string> &files_to_cache);

private:
    Oid oid;
    unique_ptr<ColumnstoreMetadata> metadata;
    unique_ptr<ColumnstoreWriter> writer;
};

} // namespace duckdb
