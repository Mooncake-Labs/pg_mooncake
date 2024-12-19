#include "columnstore/execution/columnstore_insert.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
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

SinkCombineResultType ColumnstoreInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
    return SinkCombineResultType::FINISHED;
}

SinkResultType ColumnstoreInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
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

    if (return_chunk) {
        gstate.return_collection.Append(gstate.chunk);
    }
    gstate.insert_count += gstate.chunk.size();

    table.Insert(context.client, gstate.chunk);
    return SinkResultType::NEED_MORE_INPUT;
}

unique_ptr<GlobalSinkState> ColumnstoreInsert::GetGlobalSinkState(ClientContext &context) const {
    return make_uniq<ColumnstoreInsertGlobalState>(context, table.GetTypes(), bound_defaults);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class ColumnstoreInsertSourceState : public GlobalSourceState {
public:
    explicit ColumnstoreInsertSourceState(const ColumnstoreInsert &op) {
        if (op.return_chunk) {
            D_ASSERT(op.sink_state);
            auto &g = op.sink_state->Cast<ColumnstoreInsertGlobalState>();
            g.return_collection.InitializeScan(scan_state);
        }
    }

    ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> ColumnstoreInsert::GetGlobalSourceState(ClientContext &context) const {
    return make_uniq<ColumnstoreInsertSourceState>(*this);
}

SourceResultType ColumnstoreInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
    auto &state = input.global_state.Cast<ColumnstoreInsertSourceState>();
    auto &insert_gstate = sink_state->Cast<ColumnstoreInsertGlobalState>();
    if (!return_chunk) {
        chunk.SetCardinality(1);
        chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
        return SourceResultType::FINISHED;
    }

    insert_gstate.return_collection.Scan(state.scan_state, chunk);
    return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Column store
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> Columnstore::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                     unique_ptr<PhysicalOperator> plan) {
    auto insert = make_uniq<ColumnstoreInsert>(op.types, op.estimated_cardinality, op.table.Cast<ColumnstoreTable>(),
                                               op.column_index_map, std::move(op.bound_defaults),
                                               std::move(op.bound_constraints), op.return_chunk);
    insert->children.emplace_back(std::move(plan));
    return std::move(insert);
}

} // namespace duckdb
