#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

class ColumnstoreUpdate : public PhysicalOperator {
public:
    ColumnstoreUpdate(vector<LogicalType> types, TableCatalogEntry &tableref, ColumnstoreTable &table,
                      vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
                      vector<unique_ptr<Expression>> bound_defaults,
                      vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
                      bool return_chunk);

    TableCatalogEntry &tableref;
    ColumnstoreTable &table;
    vector<PhysicalIndex> columns;
    vector<unique_ptr<Expression>> expressions;
    vector<unique_ptr<Expression>> bound_defaults;
    vector<unique_ptr<BoundConstraint>> bound_constraints;
    //! If the returning statement is present, return the whole chunk
    bool return_chunk;

public:
    string GetName() const override {
        return "COLUMNSTORE_UPDATE";
    }

public:
    // Source interface
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

    bool IsSource() const override {
        return true;
    }

public:
    // Sink interface
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

    bool IsSink() const override {
        return true;
    }
};

} // namespace duckdb
