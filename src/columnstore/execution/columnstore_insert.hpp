// Implementation of insertion physical operation with column storage.

#pragma once

#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

namespace duckdb {

class ColumnstoreInsert : public PhysicalOperator {
public:
    ColumnstoreInsert(vector<LogicalType> types, idx_t estimated_cardinality, ColumnstoreTable &table,
                      physical_index_vector_t<idx_t> column_index_map, vector<unique_ptr<Expression>> bound_defaults,
                      vector<unique_ptr<BoundConstraint>> bound_constraints, bool return_chunk)
        : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
          column_index_map(std::move(column_index_map)), bound_defaults(std::move(bound_defaults)),
          bound_constraints(std::move(bound_constraints)), insert_types(table.GetTypes()), return_chunk(return_chunk) {}

    ColumnstoreTable &table;
    //! The map from insert column index to table column index
    physical_index_vector_t<idx_t> column_index_map;
    //! The default expressions of the columns for which no value is provided
    vector<unique_ptr<Expression>> bound_defaults;
    //! The bound constraints for the table
    vector<unique_ptr<BoundConstraint>> bound_constraints;
    //! The insert types
    vector<LogicalType> insert_types;
    // ! Whether to return for insertion operation.
    bool return_chunk = false;

public:
    string GetName() const override {
        return "COLUMNSTORE_INSERT";
    }

public:
    // Source interface
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
    SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

    bool IsSource() const override {
        return true;
    }

public:
    // Sink interface
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
    SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override {
        table.FinalizeInsert();
        return SinkFinalizeType::READY;
    }

    bool IsSink() const override {
        return true;
    }
};

} // namespace duckdb
