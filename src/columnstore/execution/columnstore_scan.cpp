#include "columnstore/columnstore_metadata.hpp"
#include "columnstore/columnstore_table.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_table_function.hpp"

namespace duckdb {

struct ColumnstoreScanMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
    ColumnstoreScanMultiFileReaderGlobalState(vector<LogicalType> extra_columns,
                                              optional_ptr<const MultiFileList> file_list)
        : MultiFileReaderGlobalState(extra_columns, file_list) {}

    idx_t row_id_index = DConstants::INVALID_INDEX;
    idx_t file_row_number_index = DConstants::INVALID_INDEX;
    unique_ptr<Vector> row_ids;
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
            global_state->row_ids = make_uniq<Vector>(LogicalType::BIGINT);
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
        if (gstate.row_id_index != DConstants::INVALID_INDEX) {
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
            gstate.row_ids->SetVectorType(VectorType::FLAT_VECTOR);
            auto row_ids_data = FlatVector::GetData<row_t>(*gstate.row_ids);
            const idx_t file_number = NumericCast<int32_t>(reader_data.file_list_idx.GetIndex());
            for (idx_t i = 0; i < chunk.size(); i++) {
                row_ids_data[i] = (file_number << 32) + NumericCast<uint32_t>(file_row_numbers_data[i]);
            }
            chunk.data[gstate.row_id_index].Reference(*gstate.row_ids);
        }
    }
};

TableFunction GetParquetScan(ClientContext &context) {
    return ExtensionUtil::GetTableFunction(*context.db, "parquet_scan")
        .functions.GetFunctionByArguments(context, {LogicalType::LIST(LogicalType::VARCHAR)});
}

unique_ptr<GlobalTableFunctionState> ColumnstoreScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
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

    TableFunctionInitInput new_input(input.bind_data, column_ids, projection_ids, input.filters);
    return GetParquetScan(context).init_global(context, new_input);
}

void EmptyColumnstoreScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {}

void BuildActualFilePath(std::vector<ColumnstoreMetadata::FileInfo> &files, const string &path);

TableFunction ColumnstoreTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    auto path = metadata->TablesSearch(oid);
    auto file_names = metadata->DataFilesSearch(oid);
    BuildActualFilePath(file_names, path);
    if (file_names.empty()) {
        return TableFunction("columnstore_scan", {} /*arguments*/, EmptyColumnstoreScan);
    }

    TableFunction columnstore_scan = GetParquetScan(context);
    columnstore_scan.name = "columnstore_scan";
    columnstore_scan.init_global = ColumnstoreScanInitGlobal;
    columnstore_scan.get_multi_file_reader = ColumnstoreScanMultiFileReader::Create;

    vector<Value> values;
    for (auto &file_name : file_names) {
        values.push_back(Value(file_name.file_path));
    }
    vector<Value> inputs;
    inputs.push_back(Value::LIST(values));
    named_parameter_map_t named_parameters{{"file_row_number", Value(true)}};
    vector<LogicalType> input_table_types;
    vector<string> input_table_names;
    TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names, nullptr /*info*/,
                                      nullptr /*binder*/, columnstore_scan, {} /*ref*/);
    vector<LogicalType> return_types;
    vector<string> names;
    bind_data = columnstore_scan.bind(context, bind_input, return_types, names);
    return columnstore_scan;
}

} // namespace duckdb
