#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
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
        : return_collection(context, types) {}

    mutex insert_lock;
    idx_t insert_count;
    ColumnDataCollection return_collection;
};

class ColumnstoreInsertLocalState : public LocalSinkState {
public:
    ColumnstoreInsertLocalState(ClientContext &context, const vector<LogicalType> &types,
                                const vector<unique_ptr<Expression>> &bound_defaults)
        : executor(context, bound_defaults), insert_count(0), return_collection(context, types) {
        insert_chunk.Initialize(Allocator::Get(context), types);
    }

    DataChunk insert_chunk;
    ExpressionExecutor executor;
    idx_t insert_count;
    ColumnDataCollection return_collection;
};

class ColumnstoreInsert : public PhysicalOperator {
public:
    ColumnstoreInsert(vector<LogicalType> types, idx_t estimated_cardinality, ColumnstoreTable &table,
                      physical_index_vector_t<idx_t> column_index_map, vector<unique_ptr<Expression>> bound_defaults,
                      bool return_chunk, bool parallel)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
          column_index_map(std::move(column_index_map)), bound_defaults(std::move(bound_defaults)),
          return_chunk(return_chunk), parallel(parallel) {}

    ColumnstoreTable &table;
    physical_index_vector_t<idx_t> column_index_map;
    vector<unique_ptr<Expression>> bound_defaults;
    bool return_chunk;
    bool parallel;

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
    void ResolveDefaults(DataChunk &chunk, ExpressionExecutor &default_executor, DataChunk &result) const {
        chunk.Flatten();
        default_executor.SetChunk(chunk);
        result.Reset();
        result.SetCardinality(chunk);
        if (!column_index_map.empty()) {
            // columns specified by the user, use column_index_map
            for (auto &col : table.GetColumns().Physical()) {
                auto storage_idx = col.StorageOid();
                auto mapped_index = column_index_map[col.Physical()];
                if (mapped_index == DConstants::INVALID_INDEX) {
                    // insert default value
                    default_executor.ExecuteExpression(storage_idx, result.data[storage_idx]);
                } else {
                    // get value from child chunk
                    D_ASSERT((idx_t)mapped_index < chunk.ColumnCount());
                    D_ASSERT(result.data[storage_idx].GetType() == chunk.data[mapped_index].GetType());
                    result.data[storage_idx].Reference(chunk.data[mapped_index]);
                }
            }
        } else {
            // no columns specified, just append directly
            for (idx_t i = 0; i < result.ColumnCount(); i++) {
                D_ASSERT(result.data[i].GetType() == chunk.data[i].GetType());
                result.data[i].Reference(chunk.data[i]);
            }
        }
    }

    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
        auto &gstate = input.global_state.Cast<ColumnstoreInsertGlobalState>();
        auto &lstate = input.local_state.Cast<ColumnstoreInsertLocalState>();
        ResolveDefaults(chunk, lstate.executor, lstate.insert_chunk);
        if (!parallel) {
            if (return_chunk) {
                gstate.return_collection.Append(lstate.insert_chunk);
            }

            gstate.insert_count += lstate.insert_chunk.size();
            table.Insert(context.client, lstate.insert_chunk);
        } else {
            if (return_chunk) {
                lstate.return_collection.Append(lstate.insert_chunk);
            }
            lstate.insert_count += lstate.insert_chunk.size();
        }
        return SinkResultType::NEED_MORE_INPUT;
    }

    SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override {
        if (!parallel) {
            return SinkCombineResultType::FINISHED;
        }
        auto &gstate = input.global_state.Cast<ColumnstoreInsertGlobalState>();
        auto &lstate = input.local_state.Cast<ColumnstoreInsertLocalState>();
        lock_guard<mutex> lock(gstate.insert_lock);
        table.Insert(context.client, lstate.insert_chunk);
        gstate.insert_count += lstate.insert_count;
        if (return_chunk) {
            for (auto &chunk : lstate.return_collection.Chunks()) {
                gstate.return_collection.Append(chunk);
            }
        }
        return SinkCombineResultType::FINISHED;
    }

    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override {
        table.FinalizeInsert();
        return SinkFinalizeType::READY;
    }

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
        return make_uniq<ColumnstoreInsertGlobalState>(context, table.GetTypes(), bound_defaults);
    }

    unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override {
        return make_uniq<ColumnstoreInsertLocalState>(context.client, table.GetTypes(), bound_defaults);
    }

    bool IsSink() const override {
        return true;
    }

    bool ParallelSink() const override {
        return parallel;
    }
};

unique_ptr<PhysicalOperator> Columnstore::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                     unique_ptr<PhysicalOperator> plan) {
    bool parallel_streaming_insert = !PhysicalPlanGenerator::PreserveInsertionOrder(context, *plan);
    auto num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();

    auto insert = make_uniq<ColumnstoreInsert>(op.types, op.estimated_cardinality, op.table.Cast<ColumnstoreTable>(),
                                               op.column_index_map, std::move(op.bound_defaults), op.return_chunk,
                                               parallel_streaming_insert && num_threads > 1);
    std::cout << "For parallelism number of threads: " << num_threads << "and streaming insert state"
              << parallel_streaming_insert << std::endl;
    insert->children.push_back(std::move(plan));
    return std::move(insert);
}

} // namespace duckdb
