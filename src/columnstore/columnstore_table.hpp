#pragma once

#include "pgduckdb/catalog/pgduckdb_table.hpp"

extern "C" {
#include "utils/rel.h"
}

#include "columnstore/columnstore.hpp"

namespace duckdb {

class ColumnstoreTable {
public:
    ColumnstoreTable(::Relation rel) : rel(rel) {}

    std::vector<const char *> GetDataFiles() {
        return DataFilesGet(RelationGetRelid(rel));
    }

    void Insert(DataChunk &chunk) {
        ColumnstoreInsert(rel, chunk);
    }

    void Delete(vector<row_t> &row_ids) {
        std::sort(row_ids.begin(), row_ids.end());
    }

    ::Relation rel;
};

class ColumnstoreTableCatalogEntry : public PostgresTable {
public:
    ColumnstoreTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, ::Relation rel,
                                 Cardinality cardinality, Snapshot snapshot)
        : PostgresTable(catalog, schema, info, rel, cardinality, snapshot), table(rel) {}

public:
    ColumnstoreTable &GetTable() {
        return table;
    }

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
        throw duckdb::NotImplementedException("GetStatistics not supported yet");
    }

    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;

    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    ::Relation GetRelation() {
        return rel;
    }

private:
    ColumnstoreTable table;
};

} // namespace duckdb
