#include "columnstore/columnstore_statistics.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

DataFileStatistics::DataFileStatistics(ClientContext &context, const ColumnList &columns,
                                       shared_ptr<ParquetFileMetadataCache> metadata) {
    for (auto &col : columns.Physical()) {
        auto name = col.GetName();
        column_stats[name] = ParquetReader::ReadStatistics(context, ParquetOptions{}, metadata, name);
    }
}

ObjectCache columnstore_stats;

} // namespace duckdb
