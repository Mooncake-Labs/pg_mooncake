#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

class ColumnstoreUpdateSourceState : public GlobalSourceState {
public:
    ColumnDataScanState scan_state;
};

class ColumnstoreUpdateGlobalState : public GlobalSinkState {
public:
    ColumnstoreUpdateGlobalState(ClientContext &context, const vector<LogicalType> &types)
        : return_collection(context, types) {
        chunk.Initialize(Allocator::Get(context), types);
    }

    DataChunk chunk;
    unordered_set<row_t> row_ids;
    ColumnDataCollection return_collection;
};

class ColumnstoreUpdate : public PhysicalOperator {
public:
    ColumnstoreUpdate(vector<LogicalType> types, idx_t estimated_cardinality, ColumnstoreTable &table,
                      vector<PhysicalIndex> columns, bool return_chunk)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
          columns(std::move(columns)), return_chunk(return_chunk) {}

    ColumnstoreTable &table;
    vector<PhysicalIndex> columns;
    bool return_chunk;

public:
    string GetName() const override {
        return "COLUMNSTORE_UPDATE";
    }

public:
    // Source interface
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
        auto state = make_uniq<ColumnstoreUpdateSourceState>();
        auto &gstate = sink_state->Cast<ColumnstoreUpdateGlobalState>();
        if (return_chunk) {
            gstate.return_collection.InitializeScan(state->scan_state);
        }
        return std::move(state);
    }

    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
        auto &state = input.global_state.Cast<ColumnstoreUpdateSourceState>();
        auto &gstate = sink_state->Cast<ColumnstoreUpdateGlobalState>();
        if (!return_chunk) {
            chunk.SetCardinality(1);
            chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.row_ids.size())));
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
        auto &gstate = input.global_state.Cast<ColumnstoreUpdateGlobalState>();
        auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
        row_ids.Flatten(chunk.size());
        auto row_ids_data = FlatVector::GetData<row_t>(row_ids);

        SelectionVector sel(STANDARD_VECTOR_SIZE);
        idx_t count = 0;
        for (idx_t i = 0; i < chunk.size(); i++) {
            row_t row_id = row_ids_data[i];
            if (gstate.row_ids.find(row_id) == gstate.row_ids.end()) {
                gstate.row_ids.insert(row_id);
                sel.set_index(count++, i);
            }
        }
        if (count != chunk.size()) {
            chunk.Slice(sel, count);
        }

        gstate.chunk.SetCardinality(chunk);
        for (idx_t i = 0; i < columns.size(); i++) {
            gstate.chunk.data[columns[i].index].Reference(chunk.data[i]);
        }
        table.Insert(context.client, gstate.chunk);
        if (return_chunk) {
            gstate.return_collection.Append(gstate.chunk);
        }
        return SinkResultType::NEED_MORE_INPUT;
    }

    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override {
        auto &gstate = input.global_state.Cast<ColumnstoreUpdateGlobalState>();
        table.Delete(context, gstate.row_ids);
        return SinkFinalizeType::READY;
    }

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
        return make_uniq<ColumnstoreUpdateGlobalState>(context, table.GetTypes());
    }

    bool IsSink() const override {
        return true;
    }
};

unique_ptr<PhysicalOperator> Columnstore::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                     unique_ptr<PhysicalOperator> plan) {
    D_ASSERT(op.update_is_del_and_insert);
    auto update = make_uniq<ColumnstoreUpdate>(op.types, op.estimated_cardinality, op.table.Cast<ColumnstoreTable>(),
                                               std::move(op.columns), op.return_chunk);
    update->children.push_back(std::move(plan));
    return std::move(update);
}

} // namespace duckdb
