#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"

namespace duckdb {

TableFunction ColumnstoreTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    std::vector<const char *> file_names = DataFilesGet(oid);
    duckdb::vector<duckdb::Value> values;
    for (const char *file_name : file_names) {
        values.push_back(duckdb::Value(file_name));
    }
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
    children.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::LIST(values)));
    auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
    table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>("read_parquet", std::move(children));
    duckdb::unique_ptr<TableRef> table = std::move(table_function);
    auto bound_table = Binder::CreateBinder(context)->Bind(*table);
    auto &get = bound_table->Cast<BoundTableFunction>().get->Cast<LogicalGet>();
    bind_data = std::move(get.bind_data);
    return get.function;
}

TableStorageInfo ColumnstoreTable::GetStorageInfo(ClientContext &context) {
    throw NotImplementedException("GetStorageInfo not supported yet");
}

} // namespace duckdb
