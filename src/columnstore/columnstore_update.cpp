#include "duckdb.hpp"

#include "duckdb/planner/operator/logical_update.hpp"

#include "pgduckdb/catalog/pgduckdb_catalog.hpp"

#include "columnstore/columnstore_table.hpp"

namespace duckdb {

class ColumnstoreUpdateGlobalState : public GlobalSinkState {
public:
    explicit ColumnstoreUpdateGlobalState() {}

    vector<row_t> row_ids;
};

class ColumnstoreUpdate : public PhysicalOperator {
public:
    ColumnstoreUpdate(vector<LogicalType> types, idx_t estimated_cardinality, ColumnstoreTable &table)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table) {}

    ColumnstoreTable &table;

public:
    string GetName() const override {
        return "COLUMNSTORE_UPDATE";
    }

public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override {
        auto &gstate = sink_state->Cast<ColumnstoreUpdateGlobalState>();
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
        auto &gstate = input.global_state.Cast<ColumnstoreUpdateGlobalState>();
        auto &row_ids = chunk.data[chunk.ColumnCount() - 1];
        row_ids.Flatten(chunk.size());
        auto row_ids_data = FlatVector::GetData<row_t>(row_ids);
        for (idx_t i = 0; i < chunk.size(); i++) {
            gstate.row_ids.push_back(row_ids_data[i]);
        }
        table.Insert(chunk);
        return SinkResultType::NEED_MORE_INPUT;
    }

    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override {
        auto &gstate = input.global_state.Cast<ColumnstoreUpdateGlobalState>();
        table.Delete(gstate.row_ids);
        return SinkFinalizeType::READY;
    }

    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
        return make_uniq<ColumnstoreUpdateGlobalState>();
    }

    bool IsSink() const override {
        return true;
    }
};

unique_ptr<PhysicalOperator> PostgresCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                         unique_ptr<PhysicalOperator> plan) {
    D_ASSERT(op.update_is_del_and_insert);
    auto update = make_uniq<ColumnstoreUpdate>(op.types, op.estimated_cardinality,
                                               op.table.Cast<ColumnstoreTableCatalogEntry>().GetTable());
    update->children.push_back(std::move(plan));
    return std::move(update);
}

} // namespace duckdb
