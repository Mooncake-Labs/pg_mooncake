#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {
struct ColumnstoreStats {
    void AddStats(const string &column, BaseStatistics &stats);

    BaseStatistics *GetStats(const string &column);

    void Serialize(Serializer &serializer);

    static ColumnstoreStats Deserialize(Deserializer &deserializer);

    map<string, unique_ptr<BaseStatistics>> stats_map;
};

struct ColumnstoreStatsMap {
    void LoadStats(const string file, const char *data, int len);

    BaseStatistics *GetStats(const string path, const string col) {
        auto file_name = StringUtil::GetFileName(path);
        if (file_stats.count(file_name) == 0) {
            return nullptr;
        }
        return file_stats[file_name].GetStats(col);
    }
    map<string, ColumnstoreStats> file_stats;
};

} // namespace duckdb