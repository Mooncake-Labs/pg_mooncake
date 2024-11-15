#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_process_lock.hpp"
#include "pgduckdb/catalog/pgduckdb_schema.hpp"
#include "pgduckdb/catalog/pgduckdb_table.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "pgduckdb/scan/postgres_seq_scan.hpp"
#include "pgduckdb/scan/postgres_scan.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "storage/bufmgr.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/plancat.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/relcache.h"
#include "access/htup_details.h"
#include "parser/parsetree.h"
}

namespace duckdb {

PostgresTable::PostgresTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, ::Relation rel,
                             Cardinality cardinality, Snapshot snapshot)
    : TableCatalogEntry(catalog, schema, info), rel(rel), cardinality(cardinality), snapshot(snapshot) {
}

PostgresTable::~PostgresTable() {
	std::lock_guard<std::mutex> lock(pgduckdb::DuckdbProcessLock::GetLock());
	RelationClose(rel);
}

::Relation
PostgresTable::OpenRelation(Oid relid) {
	std::lock_guard<std::mutex> lock(pgduckdb::DuckdbProcessLock::GetLock());
	return pgduckdb::PostgresFunctionGuard<::Relation>(RelationIdGetRelation, relid);
}

void
PostgresTable::SetTableInfo(CreateTableInfo &info, ::Relation rel) {
	auto desc = RelationGetDescr(rel);

	for (int i = 0; i < desc->natts; ++i) {
		Form_pg_attribute attr = &desc->attrs[i];
		auto col_name = duckdb::string(NameStr(attr->attname));
		auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(attr);
		ColumnDefinition column(col_name, duck_type);
		if (attr->atthasdef) {
			Node *node = TupleDescGetDefault(desc, i + 1);
			if (!IsA(node, Const)) {
				elog(ERROR, "column \"%s\" has unsupported default value ", NameStr(attr->attname));
			}
			Const *val = castNode(Const, node);
			if (val->constisnull) {
				column.SetDefaultValue(make_uniq<ConstantExpression>(Value(duck_type)));
			} else {
				column.SetDefaultValue(make_uniq<ConstantExpression>(
				    pgduckdb::ConvertPostgresParameterToDuckValue(val->constvalue, val->consttype)));
			}
		} else if (attr->attidentity) {
			Oid seqid = getIdentitySequence(rel, i + 1, false /*missing_ok*/);
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_uniq<ConstantExpression>(Value::UINTEGER(seqid)));
			column.SetDefaultValue(make_uniq<FunctionExpression>("pg_nextval", std::move(children)));
		}
		info.columns.AddColumn(std::move(column));
		/* Log column name and type */
		elog(DEBUG2, "(DuckDB/SetTableInfo) Column name: %s, Type: %s --", col_name.c_str(),
		     duck_type.ToString().c_str());
	}
}

Cardinality
PostgresTable::GetTableCardinality(::Relation rel) {
	Cardinality cardinality;
	BlockNumber n_pages;
	double allvisfrac;
	estimate_rel_size(rel, NULL, &n_pages, &cardinality, &allvisfrac);
	return cardinality;
}

//===--------------------------------------------------------------------===//
// PostgresHeapTable
//===--------------------------------------------------------------------===//

PostgresHeapTable::PostgresHeapTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     ::Relation rel, Cardinality cardinality, Snapshot snapshot)
    : PostgresTable(catalog, schema, info, rel, cardinality, snapshot) {
}

unique_ptr<BaseStatistics>
PostgresHeapTable::GetStatistics(ClientContext &context, column_t column_id) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

TableFunction
PostgresHeapTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	bind_data = duckdb::make_uniq<pgduckdb::PostgresSeqScanFunctionData>(rel, cardinality, snapshot);
	return pgduckdb::PostgresSeqScanFunction();
}

TableStorageInfo
PostgresHeapTable::GetStorageInfo(ClientContext &context) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

} // namespace duckdb
