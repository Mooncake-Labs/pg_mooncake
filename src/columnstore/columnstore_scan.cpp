#include "duckdb.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"

extern "C" {
#include "postgres.h"

#include "utils/rel.h"
}

#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_table.hpp"

namespace duckdb {

struct ColumnstoreScanMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
    ColumnstoreScanMultiFileReaderGlobalState(vector<LogicalType> extra_columns,
                                              optional_ptr<const MultiFileList> file_list)
        : MultiFileReaderGlobalState(extra_columns, file_list) {}

    idx_t row_id_index = DConstants::INVALID_INDEX;
    idx_t file_row_number_index = DConstants::INVALID_INDEX;
};

struct ColumnstoreScanMultiFileReader : public MultiFileReader {
    static unique_ptr<MultiFileReader> Create() {
        return std::move(make_uniq<ColumnstoreScanMultiFileReader>());
    }

    unique_ptr<MultiFileReaderGlobalState>
    InitializeGlobalState(ClientContext &context, const MultiFileReaderOptions &file_options,
                          const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                          const vector<LogicalType> &global_types, const vector<string> &global_names,
                          const vector<column_t> &global_column_ids) override {
        auto it = std::find_if(global_column_ids.begin(), global_column_ids.end(),
                               [](column_t global_column_id) { return IsRowIdColumnId(global_column_id); });
        vector<LogicalType> extra_columns;
        if (it != global_column_ids.end()) {
            extra_columns.push_back(LogicalType::BIGINT);
        }
        auto global_state = make_uniq<ColumnstoreScanMultiFileReaderGlobalState>(std::move(extra_columns), file_list);
        if (it != global_column_ids.end()) {
            global_state->row_id_index = NumericCast<idx_t>(std::distance(global_column_ids.begin(), it));
            global_state->file_row_number_index = global_column_ids.size();
        }
        return std::move(global_state);
    }

    void CreateMapping(const string &file_name, const vector<LogicalType> &local_types,
                       const vector<string> &local_names, const vector<LogicalType> &global_types,
                       const vector<string> &global_names, const vector<column_t> &global_column_ids,
                       optional_ptr<TableFilterSet> filters, MultiFileReaderData &reader_data,
                       const string &initial_file, const MultiFileReaderBindData &options,
                       optional_ptr<MultiFileReaderGlobalState> global_state) override {
        MultiFileReader::CreateMapping(file_name, local_types, local_names, global_types, global_names,
                                       global_column_ids, filters, reader_data, initial_file, options, global_state);
        auto &gstate = global_state->Cast<ColumnstoreScanMultiFileReaderGlobalState>();
        if (gstate.file_row_number_index != DConstants::INVALID_INDEX) {
            auto it = std::find_if(local_names.begin(), local_names.end(), [](const string &local_name) {
                return StringUtil::CIEquals(local_name, "file_row_number");
            });
            D_ASSERT(it != local_names.end());
            reader_data.column_mapping.push_back(gstate.file_row_number_index);
            reader_data.column_ids.push_back(NumericCast<idx_t>(std::distance(local_names.begin(), it)));
        }
    }

    void FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
                       const MultiFileReaderData &reader_data, DataChunk &chunk,
                       optional_ptr<MultiFileReaderGlobalState> global_state) override {
        MultiFileReader::FinalizeChunk(context, bind_data, reader_data, chunk, global_state);
        auto &gstate = global_state->Cast<ColumnstoreScanMultiFileReaderGlobalState>();
        if (gstate.row_id_index != DConstants::INVALID_INDEX) {
            auto &file_row_numbers = chunk.data[gstate.file_row_number_index];
            file_row_numbers.Flatten(chunk.size());
            auto file_row_numbers_data = FlatVector::GetData<int64_t>(file_row_numbers);
            auto &row_ids = chunk.data[gstate.row_id_index];
            row_ids.SetVectorType(VectorType::FLAT_VECTOR);
            auto row_ids_data = FlatVector::GetData<int64_t>(row_ids);
            const uint64_t file_number = NumericCast<int32_t>(reader_data.file_list_idx.GetIndex());
            for (idx_t i = 0; i < chunk.size(); i++) {
                row_ids_data[i] = (file_number << 32) + NumericCast<uint32_t>(file_row_numbers_data[i]);
            }
        }
    }
};

struct ColumnstoreScanBindData : public TableFunctionData {
    explicit ColumnstoreScanBindData(ColumnstoreTable &table, TableFunction parquet_scan)
        : table(table), parquet_scan(std::move(parquet_scan)) {}

    ColumnstoreTable &table;
    TableFunction parquet_scan;
};

struct ColumnstoreScanGlobalState : public GlobalTableFunctionState {
    unique_ptr<const FunctionData> parquet_scan_bind_data;
    unique_ptr<TableFunctionInitInput> parquet_scan_input;
    unique_ptr<GlobalTableFunctionState> parquet_scan_gstate;
};

struct ColumnstoreScanLocalState : public LocalTableFunctionState {
    unique_ptr<LocalTableFunctionState> parquet_scan_lstate;
};

unique_ptr<GlobalTableFunctionState> ColumnstoreScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ColumnstoreScanBindData>();
    vector<const char *> file_names = bind_data.table.GetDataFiles();
    vector<Value> values;
    for (const char *file_name : file_names) {
        values.push_back(Value(file_name));
    }
    vector<Value> inputs;
    inputs.push_back(Value::LIST(values));
    named_parameter_map_t named_parameters{{"file_row_number", Value(true)}};
    vector<LogicalType> input_table_types;
    vector<string> input_table_names;
    TableFunction parquet_scan = bind_data.parquet_scan;
    TableFunctionBindInput parquet_scan_bind_input(inputs, named_parameters, input_table_types, input_table_names,
                                                   nullptr, nullptr, parquet_scan, {});
    vector<LogicalType> return_types;
    vector<string> names;

    // UPDATE can generate duplicate global_column_ids which ParquetReader doesn't expect
    unordered_map<column_t, idx_t> column_ids_map;
    vector<column_t> column_ids;
    for (idx_t i = 0; i < input.column_ids.size(); i++) {
        if (column_ids_map.count(input.column_ids[i]) == 0) {
            column_ids_map[input.column_ids[i]] = column_ids.size();
            column_ids.push_back(input.column_ids[i]);
        }
    }
    vector<idx_t> projection_ids(input.projection_ids);
    for (idx_t i = 0; i < projection_ids.size(); i++) {
        projection_ids[i] = column_ids_map[input.column_ids[projection_ids[i]]];
    }

    auto gstate = make_uniq<ColumnstoreScanGlobalState>();
    gstate->parquet_scan_bind_data = bind_data.parquet_scan.bind(context, parquet_scan_bind_input, return_types, names);
    gstate->parquet_scan_input =
        make_uniq<TableFunctionInitInput>(gstate->parquet_scan_bind_data, column_ids, projection_ids, input.filters);
    gstate->parquet_scan_gstate = bind_data.parquet_scan.init_global(context, *gstate->parquet_scan_input);
    return std::move(gstate);
}

unique_ptr<LocalTableFunctionState> ColumnstoreScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
    auto &bind_data = input.bind_data->Cast<ColumnstoreScanBindData>();
    auto &gstate = global_state->Cast<ColumnstoreScanGlobalState>();
    auto lstate = make_uniq<ColumnstoreScanLocalState>();
    lstate->parquet_scan_lstate =
        bind_data.parquet_scan.init_local(context, *gstate.parquet_scan_input, gstate.parquet_scan_gstate.get());
    return std::move(lstate);
}

void ColumnstoreScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<ColumnstoreScanBindData>();
    auto &gstate = data.global_state->Cast<ColumnstoreScanGlobalState>();
    auto &lstate = data.local_state->Cast<ColumnstoreScanLocalState>();
    TableFunctionInput parquet_scan_data = {gstate.parquet_scan_bind_data, lstate.parquet_scan_lstate,
                                            gstate.parquet_scan_gstate};
    bind_data.parquet_scan.function(context, parquet_scan_data, output);
}

TableFunction ColumnstoreTableCatalogEntry::GetScanFunction(ClientContext &context,
                                                            unique_ptr<FunctionData> &bind_data) {
    TableFunction parquet_scan =
        ExtensionUtil::GetTableFunction(*context.db, "parquet_scan")
            .functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});
    parquet_scan.get_multi_file_reader = ColumnstoreScanMultiFileReader::Create;
    bind_data = make_uniq<ColumnstoreScanBindData>(GetTable(), std::move(parquet_scan));
    TableFunction table_function("columnstore_scan", {}, ColumnstoreScan, nullptr /*bind*/, ColumnstoreScanInitGlobal,
                                 ColumnstoreScanInitLocal);
    table_function.projection_pushdown = true;
    table_function.filter_pushdown = true;
    table_function.filter_prune = true;
    return table_function;

    // table_function.cardinality = ParquetCardinality;
    // table_function.get_batch_index = ParquetScanGetBatchIndex;
    // table_function.pushdown_complex_filter = ParquetComplexFilterPushdown;
}

} // namespace duckdb
