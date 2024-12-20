#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"

namespace duckdb {

class ColumnstoreDeleteGlobalState : public GlobalSinkState {
public:
    unordered_set<row_t> row_ids;
};

class ColumnstoreDelete : public PhysicalOperator {
public:
    ColumnstoreDelete(vector<LogicalType> types, idx_t estimated_cardinality, ColumnstoreTable &table,
                      idx_t row_id_index)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
          row_id_index(row_id_index) {}

    ColumnstoreTable &table;
    idx_t row_id_index;

public:
    string GetName() const override {
        return "COLUMNSTORE_DELETE";
    }

public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
        auto &gstate = sink_state->Cast<ColumnstoreDeleteGlobalState>();
        chunk.SetCardinality(1);
        chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.row_ids.size())));
        return SourceResultType::FINISHED;
    }

    bool IsSource() const override {
        return true;
    }

public:
    // Sink interface
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
        auto &gstate = input.global_state.Cast<ColumnstoreDeleteGlobalState>();
        auto &row_ids = chunk.data[row_id_index];
        row_ids.Flatten(chunk.size());
        auto row_ids_data = FlatVector::GetData<row_t>(row_ids);
        for (idx_t i = 0; i < chunk.size(); i++) {
            gstate.row_ids.insert(row_ids_data[i]);
        }
        return SinkResultType::NEED_MORE_INPUT;
    }

    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override {
        auto &gstate = input.global_state.Cast<ColumnstoreDeleteGlobalState>();
        table.Delete(context, gstate.row_ids);
        return SinkFinalizeType::READY;
    }

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
        return make_uniq<ColumnstoreDeleteGlobalState>();
    }

    bool IsSink() const override {
        return true;
    }
};

unique_ptr<PhysicalOperator> Columnstore::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                     unique_ptr<PhysicalOperator> plan) {
    auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
    auto del = make_uniq<ColumnstoreDelete>(op.types, op.estimated_cardinality, op.table.Cast<ColumnstoreTable>(),
                                            bound_ref.index);
    del->children.push_back(std::move(plan));
    return std::move(del);
}

} // namespace duckdb
