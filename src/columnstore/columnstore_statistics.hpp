#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

class BaseStatistics;
class ColumnList;
class ParquetFileMetadataCache;

class DataFileStatistics : public ObjectCacheEntry {
public:
    DataFileStatistics(ClientContext &context, const ColumnList &columns,
                       shared_ptr<ParquetFileMetadataCache> metadata);

public:
    static string ObjectType() {
        return "data_file_statistics";
    }

    string GetObjectType() override {
        return ObjectType();
    }

    BaseStatistics *Get(const string &name) {
        return column_stats.at(name).get();
    }

private:
    unordered_map<string, unique_ptr<BaseStatistics>> column_stats;
};

extern ObjectCache columnstore_stats;

} // namespace duckdb
