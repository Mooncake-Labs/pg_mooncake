#include "duckdb.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include "columnstore/columnstore.hpp"

duckdb::unique_ptr<duckdb::TableRef> ColumnstoreReplacementScan(Oid oid, const duckdb::string &table_name) {
    std::vector<const char *> file_names = DataFilesGet(oid);
    duckdb::vector<duckdb::Value> values;
    for (const char *file_name : file_names) {
        values.push_back(duckdb::Value(file_name));
    }
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
    children.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::LIST(values)));
    auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
    table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>("read_parquet", std::move(children));
    table_function->alias = table_name;
    return std::move(table_function);
}
