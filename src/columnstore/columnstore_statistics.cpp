#include "columnstore/columnstore_statistics.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

DataFileStatistics::DataFileStatistics(ParquetReader &reader, const ColumnList &columns) {
    num_rows = reader.NumRows();
    for (auto &col : columns.Physical()) {
        auto name = col.GetName();
        column_stats[name] = reader.ReadStatistics(name);
    }
}

ObjectCache columnstore_stats;

} // namespace duckdb
