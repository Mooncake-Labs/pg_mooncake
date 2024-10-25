#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

typedef unsigned int Oid;

class ColumnstoreTable : public TableCatalogEntry {
public:
    ColumnstoreTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, Oid oid)
        : TableCatalogEntry(catalog, schema, info), oid(oid) {}

public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
        throw NotImplementedException("GetStatistics not supported yet");
    }

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
    Oid oid;
};

} // namespace duckdb
