#include "duckdb.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "columnstore/columnstore.hpp"

duckdb::unique_ptr<duckdb::TableRef> ColumnstoreReplacementScan(Oid relid, const duckdb::string &table_name) {
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
    children.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, DataFilesGet(relid))));
    auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
    table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>("read_parquet", std::move(children));
    table_function->alias = table_name;
    return std::move(table_function);
}
