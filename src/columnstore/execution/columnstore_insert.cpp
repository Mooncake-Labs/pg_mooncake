#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {

class ColumnstoreInsertGlobalState : public GlobalSinkState {
public:
    ColumnstoreInsertGlobalState(ClientContext &context, const vector<LogicalType> &types,
                                 const vector<unique_ptr<Expression>> &bound_defaults)
        : executor(context, bound_defaults), insert_count(0) {
        chunk.Initialize(Allocator::Get(context), types);
    }

    DataChunk chunk;
    ExpressionExecutor executor;
    idx_t insert_count;
};

class ColumnstoreInsert : public PhysicalOperator {
public:
    ColumnstoreInsert(vector<LogicalType> types, idx_t estimated_cardinality, ColumnstoreTable &table,
                      physical_index_vector_t<idx_t> column_index_map, vector<unique_ptr<Expression>> bound_defaults)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
          column_index_map(std::move(column_index_map)), bound_defaults(std::move(bound_defaults)) {}

    ColumnstoreTable &table;
    physical_index_vector_t<idx_t> column_index_map;
    vector<unique_ptr<Expression>> bound_defaults;

public:
    string GetName() const override {
        return "COLUMNSTORE_INSERT";
    }

public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
        auto &gstate = sink_state->Cast<ColumnstoreInsertGlobalState>();
        chunk.SetCardinality(1);
        chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
        return SourceResultType::FINISHED;
    }

    bool IsSource() const override {
        return true;
    }

public:
    // Sink interface
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
        auto &gstate = input.global_state.Cast<ColumnstoreInsertGlobalState>();
        chunk.Flatten();
        gstate.executor.SetChunk(chunk);
        gstate.chunk.Reset();
        gstate.chunk.SetCardinality(chunk);
        if (!column_index_map.empty()) {
            // columns specified by the user, use column_index_map
            for (auto &col : table.GetColumns().Physical()) {
                auto storage_idx = col.StorageOid();
                auto mapped_index = column_index_map[col.Physical()];
                if (mapped_index == DConstants::INVALID_INDEX) {
                    // insert default value
                    gstate.executor.ExecuteExpression(storage_idx, gstate.chunk.data[storage_idx]);
                } else {
                    // get value from child chunk
                    D_ASSERT(mapped_index < chunk.ColumnCount());
                    D_ASSERT(gstate.chunk.data[storage_idx].GetType() == chunk.data[mapped_index].GetType());
                    gstate.chunk.data[storage_idx].Reference(chunk.data[mapped_index]);
                }
            }
        } else {
            // no columns specified, just append directly
            for (idx_t i = 0; i < gstate.chunk.ColumnCount(); i++) {
                D_ASSERT(gstate.chunk.data[i].GetType() == chunk.data[i].GetType());
                gstate.chunk.data[i].Reference(chunk.data[i]);
            }
        }
        gstate.insert_count += gstate.chunk.size();
        table.Insert(context.client, gstate.chunk);
        return SinkResultType::NEED_MORE_INPUT;
    }

    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override {
        table.FinalizeInsert();
        return SinkFinalizeType::READY;
    }

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
        return make_uniq<ColumnstoreInsertGlobalState>(context, table.GetTypes(), bound_defaults);
    }

    bool IsSink() const override {
        return true;
    }
};

unique_ptr<PhysicalOperator> Columnstore::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                     unique_ptr<PhysicalOperator> plan) {
    auto insert = make_uniq<ColumnstoreInsert>(op.types, op.estimated_cardinality, op.table.Cast<ColumnstoreTable>(),
                                               op.column_index_map, std::move(op.bound_defaults));
    insert->children.push_back(std::move(plan));
    return std::move(insert);
}

} // namespace duckdb
