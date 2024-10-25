#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

struct SnapshotData;

namespace duckdb {

class ColumnstoreMetadata;
class ColumnstoreWriter;
class DataChunk;
using Oid = unsigned int;
using Snapshot = SnapshotData *;

class ColumnstoreTable : public TableCatalogEntry {
public:
    ColumnstoreTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Oid oid, Snapshot snapshot);

    ~ColumnstoreTable();

public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
        throw NotImplementedException("GetStatistics not supported yet");
    }

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

public:
    void Insert(ClientContext &context, DataChunk &chunk);

    void FinalizeInsert();

    void Delete(ClientContext &context, vector<row_t> &row_ids);

private:
    Oid oid;
    unique_ptr<ColumnstoreMetadata> metadata;
    unique_ptr<ColumnstoreWriter> writer;
};

} // namespace duckdb
