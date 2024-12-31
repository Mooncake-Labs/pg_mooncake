#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
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

private:
    Oid oid;
    unique_ptr<ColumnstoreMetadata> metadata;
    unique_ptr<ColumnstoreWriter> writer;
};

} // namespace duckdb
