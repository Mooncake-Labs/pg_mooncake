#pragma once

#include "duckdb/common/vector.hpp"

struct SnapshotData;

namespace duckdb {

using Oid = unsigned int;
using Snapshot = SnapshotData *;

class ColumnstoreMetadata {
public:
    explicit ColumnstoreMetadata(Snapshot snapshot) : snapshot(snapshot) {}

public:
    void TablesInsert(Oid oid, const string &path);
    void TablesDelete(Oid oid);
    string TablesSearch(Oid oid);

    string GetTablePath(Oid oid);
    void GetTableMetadata(Oid oid, string &table_name /*out*/, vector<string> &column_names /*out*/,
                          vector<string> &column_types /*out*/);

    void DataFilesInsert(Oid oid, const string &file_name);
    void DataFilesDelete(const string &file_name);
    void DataFilesDelete(Oid oid);
    vector<string> DataFilesSearch(Oid oid);

    vector<string> SecretsGetDuckdbQueries();
    string SecretsSearchDeltaOptions(const string &path);

private:
    Snapshot snapshot;
};

} // namespace duckdb
