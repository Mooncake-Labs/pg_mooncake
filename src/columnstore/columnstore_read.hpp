#pragma once

#include "pgduckdb/catalog/pgduckdb_table.hpp"

namespace duckdb {

class ColumnstoreTable : public PostgresTable {
  public:
    ColumnstoreTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, ::Relation rel,
                     Cardinality cardinality, Snapshot snapshot);

  public:
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;
};

} // namespace duckdb
