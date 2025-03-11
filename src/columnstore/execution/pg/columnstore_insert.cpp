#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "pgduckdb/pg/relations.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

extern "C" {
#include "postgres.h"
#include "catalog/namespace.h"
#include "executor/tuptable.h"
#include "utils/lsyscache.h"
}

#include "pgduckdb/pgduckdb_detoast.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

void duckdb::Columnstore::PgInsert(::Relation rel, TupleTableSlot **slots, int nslots) {
    auto &context = pgduckdb::DuckDBManager::Get().GetConnection()->context;
    const char *schema_name = pgduckdb::GetNamespaceName(rel);
    const char *table_name = pgduckdb::GetRelationName(rel);
    duckdb::CreateTableInfo info("", schema_name, table_name);

    Catalog &catalog = Catalog::GetCatalog(*context, string("mooncake"));
    SchemaCatalogEntry &schema = catalog.GetSchema(*context, string("mooncake"));

    TupleDesc desc = pgduckdb::RelationGetDescr(rel);
    duckdb::vector<duckdb::LogicalType> types;
    for (int i = 0; i < desc->natts; i++) {
        Form_pg_attribute attr = pgduckdb::GetAttr(desc, i);
        auto col_name = duckdb::string(pgduckdb::GetAttName(attr));
        auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
        ColumnDefinition column(col_name, duck_type);
        info.columns.AddColumn(std::move(column));
        types.push_back(duck_type);
    }

    duckdb::DataChunk chunk;
    chunk.Initialize(*context, types);
    for (int row = 0; row < nslots; row++) {
        TupleTableSlot *slot = slots[row];
        for (int col = 0; col < desc->natts; col++) {
            auto &vec = chunk.data[col];
            if (slot->tts_isnull[col]) {
                duckdb::FlatVector::Validity(vec).SetInvalid(chunk.size());
            } else {
                if (desc->attrs[col].attlen == -1) {
                    bool should_free = false;
                    Datum value = pgduckdb::DetoastPostgresDatum(reinterpret_cast<varlena *>(slot->tts_values[col]),
                                                                 &should_free);
                    pgduckdb::ConvertPostgresToDuckValue(slot->tts_tableOid, value, vec, chunk.size());
                    if (should_free) {
                        duckdb_free(reinterpret_cast<void *>(value));
                    }
                } else {
                    pgduckdb::ConvertPostgresParameterToDuckValue(slot->tts_values[col], chunk.size());
                }
            }
        }
    }
    chunk.SetCardinality(chunk.size() + 1);

    Oid rel_oid = get_relname_relid(table_name, get_namespace_oid("mooncake", false /*missing_ok*/));
    ColumnstoreTable table(catalog, schema, info, rel_oid, NULL);
    table.Insert(*context, chunk);
}