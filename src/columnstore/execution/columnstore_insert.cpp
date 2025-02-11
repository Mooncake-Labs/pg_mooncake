#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
namespace duckdb {

class ColumnstoreInsertSourceState : public GlobalSourceState {
public:
    ColumnDataScanState scan_state;
};

class ColumnstoreInsertGlobalState : public GlobalSinkState {
public:
    ColumnstoreInsertGlobalState(ClientContext &context, const vector<LogicalType> &types,
                                 const vector<unique_ptr<Expression>> &bound_defaults)
        : executor(context, bound_defaults), insert_count(0), return_collection(context, types) {
        chunk.Initialize(Allocator::Get(context), types);
    }

    DataChunk chunk;
    ExpressionExecutor executor;
    idx_t insert_count;
    ColumnDataCollection return_collection;
};

class ColumnstoreInsert : public PhysicalOperator {
public:
    ColumnstoreInsert(vector<LogicalType> types, idx_t estimated_cardinality, ColumnstoreTable &table,
                      physical_index_vector_t<idx_t> column_index_map, vector<unique_ptr<Expression>> bound_defaults,
                      vector<unique_ptr<BoundConstraint>> bound_constraints, bool return_chunk)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
          column_index_map(std::move(column_index_map)), bound_defaults(std::move(bound_defaults)),
          bound_constraints(std::move(bound_constraints)), return_chunk(return_chunk) {}

    ColumnstoreTable &table;
    physical_index_vector_t<idx_t> column_index_map;
    vector<unique_ptr<Expression>> bound_defaults;
    vector<unique_ptr<BoundConstraint>> bound_constraints;
    bool return_chunk;

public:
    string GetName() const override {
        return "COLUMNSTORE_INSERT";
    }

public:
    // Source interface
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
        auto state = make_uniq<ColumnstoreInsertSourceState>();
        auto &gstate = sink_state->Cast<ColumnstoreInsertGlobalState>();
        if (return_chunk) {
            gstate.return_collection.InitializeScan(state->scan_state);
        }
        return std::move(state);
    }

    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
        auto &state = input.global_state.Cast<ColumnstoreInsertSourceState>();
        auto &gstate = sink_state->Cast<ColumnstoreInsertGlobalState>();
        if (!return_chunk) {
            chunk.SetCardinality(1);
            chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
            return SourceResultType::FINISHED;
        }
        gstate.return_collection.Scan(state.scan_state, chunk);
        return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
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

        auto &constraints = table.GetConstraints();
        for (idx_t i = 0; i < bound_constraints.size(); i++) {
            auto &base_constraint = constraints[i];
            auto &bound_constraint = bound_constraints[i];
            if (base_constraint->type == ConstraintType::NOT_NULL) {
                auto &bound_not_null = bound_constraint->Cast<BoundNotNullConstraint>();
                auto &not_null = base_constraint->Cast<NotNullConstraint>();
                auto &col = table.GetColumns().GetColumn(LogicalIndex(not_null.index));
                if (VectorOperations::HasNull(gstate.chunk.data[bound_not_null.index.index], gstate.chunk.size())) {
                    throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col.Name());
                }
            }
        }
        if (return_chunk) {
            gstate.return_collection.Append(gstate.chunk);
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
                                               op.column_index_map, std::move(op.bound_defaults),
                                               std::move(op.bound_constraints), op.return_chunk);
    insert->children.push_back(std::move(plan));
    return std::move(insert);
}

} // namespace duckdb
