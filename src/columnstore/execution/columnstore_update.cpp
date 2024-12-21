#include "columnstore/execution/columnstore_update.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/update_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
ColumnstoreUpdate::ColumnstoreUpdate(vector<LogicalType> types, TableCatalogEntry &tableref, ColumnstoreTable &table,
                                     vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
                                     vector<unique_ptr<Expression>> bound_defaults,
                                     vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
                                     bool return_chunk)
    : PhysicalOperator(PhysicalOperatorType::UPDATE, std::move(types), estimated_cardinality), tableref(tableref),
      table(table), columns(std::move(columns)), expressions(std::move(expressions)),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints)),
      return_chunk(return_chunk) {}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class ColumnstoreUpdateGlobalSinkState : public GlobalSinkState {
public:
    ColumnstoreUpdateGlobalSinkState(ClientContext &context, const vector<LogicalType> &types,
                                     const vector<LogicalType> &table_types,
                                     const vector<unique_ptr<Expression>> &expressions,
                                     const vector<unique_ptr<Expression>> &bound_defaults,
                                     const vector<unique_ptr<BoundConstraint>> &bound_constraints)
        : updated_count(0), return_collection(context, types), default_executor(context, bound_defaults),
          bound_constraints(bound_constraints) {
        auto &allocator = Allocator::Get(context);
        vector<LogicalType> update_types;
        update_types.reserve(expressions.size());
        for (auto &expr : expressions) {
            update_types.emplace_back(expr->return_type);
        }
        update_chunk.Initialize(allocator, update_types);
        mock_chunk.Initialize(allocator, table_types);
    }

    idx_t updated_count;
    unordered_set<row_t> updated_rows;
    ColumnDataCollection return_collection;
    ExpressionExecutor default_executor;
    unique_ptr<TableDeleteState> delete_state;
    unique_ptr<TableUpdateState> update_state;
    const vector<unique_ptr<BoundConstraint>> &bound_constraints;
    DataChunk update_chunk;
    DataChunk mock_chunk;
};

SinkResultType ColumnstoreUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &gstate = input.global_state.Cast<ColumnstoreUpdateGlobalSinkState>();
    DataChunk &update_chunk = gstate.update_chunk;
    DataChunk &mock_chunk = gstate.mock_chunk;

    chunk.Flatten();
    gstate.default_executor.SetChunk(chunk);

    auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
    update_chunk.Reset();
    update_chunk.SetCardinality(chunk);

    for (idx_t i = 0; i < expressions.size(); i++) {
        if (expressions[i]->type == ExpressionType::VALUE_DEFAULT) {
            // default expression, set to the default value of the column
            gstate.default_executor.ExecuteExpression(columns[i].index, update_chunk.data[i]);
        } else {
            D_ASSERT(expressions[i]->type == ExpressionType::BOUND_REF);
            // index into child chunk
            auto &binding = expressions[i]->Cast<BoundReferenceExpression>();
            update_chunk.data[i].Reference(chunk.data[binding.index]);
        }
    }

    auto row_id_data = FlatVector::GetData<row_t>(row_ids);
    SelectionVector sel(STANDARD_VECTOR_SIZE);
    idx_t update_count = 0;
    for (idx_t idx = 0; idx < update_chunk.size(); ++idx) {
        auto row_id = row_id_data[idx];
        const bool is_new = gstate.updated_rows.insert(row_id).second;
        if (is_new) {
            sel.set_index(update_count++, idx);
        }
    }
    if (update_count != update_chunk.size()) {
        // we need to slice here
        update_chunk.Slice(sel, update_count);
    }
    mock_chunk.SetCardinality(update_chunk);
    for (idx_t idx = 0; idx < columns.size(); ++idx) {
        mock_chunk.data[columns[idx].index].Reference(update_chunk.data[idx]);
    }

    if (return_chunk) {
        gstate.return_collection.Append(mock_chunk);
    }
    gstate.updated_count += chunk.size();

    table.Insert(context.client, mock_chunk);
    return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType ColumnstoreUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<ColumnstoreUpdateGlobalSinkState>();
    table.Delete(context, gstate.updated_rows);
    return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> ColumnstoreUpdate::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<ColumnstoreUpdateGlobalSinkState>(context, GetTypes(), table.GetTypes(), expressions,
                                                       bound_defaults, bound_constraints);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class ColumnstoreUpdateSourceState : public GlobalSourceState {
public:
    explicit ColumnstoreUpdateSourceState(const ColumnstoreUpdate &op) {
        if (op.return_chunk) {
            D_ASSERT(op.sink_state);
            auto &g = op.sink_state->Cast<ColumnstoreUpdateGlobalSinkState>();
            g.return_collection.InitializeScan(scan_state);
        }
    }

    ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> ColumnstoreUpdate::GetGlobalSourceState(ClientContext &context) const {
    return make_uniq<ColumnstoreUpdateSourceState>(*this);
}

SourceResultType ColumnstoreUpdate::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
    auto &gstate = sink_state->Cast<ColumnstoreUpdateGlobalSinkState>();
    if (!return_chunk) {
        chunk.SetCardinality(1);
        chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.updated_rows.size())));
        return SourceResultType::FINISHED;
    }

    auto &state = input.global_state.Cast<ColumnstoreUpdateSourceState>();
    gstate.return_collection.Scan(state.scan_state, chunk);
    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Column store
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> Columnstore::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                     unique_ptr<PhysicalOperator> plan) {
    D_ASSERT(op.update_is_del_and_insert);
    auto update = make_uniq<ColumnstoreUpdate>(
        op.types, op.table, op.table.Cast<ColumnstoreTable>(), op.columns, std::move(op.expressions),
        std::move(op.bound_defaults), std::move(op.bound_constraints), op.estimated_cardinality, op.return_chunk);
    update->children.emplace_back(std::move(plan));
    return std::move(update);
}

} // namespace duckdb
