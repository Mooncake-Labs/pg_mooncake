#include "columnstore/columnstore_statistics.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

DataFileStatistics::DataFileStatistics(ClientContext &context, const ColumnList &columns,
                                       shared_ptr<ParquetFileMetadataCache> metadata) {
    ParquetReader stats_reader(context, "/dev/null", ParquetOptions(), metadata);
    for (auto &col : columns.Physical()) {
        auto name = col.GetName();
        column_stats[name] = stats_reader.ReadStatistics(name);
    }
}

ObjectCache columnstore_stats;

} // namespace duckdb
