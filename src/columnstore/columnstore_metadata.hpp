#pragma once

#include "duckdb/common/vector.hpp"

struct SnapshotData;

namespace duckdb {

typedef struct SnapshotData *Snapshot;
typedef unsigned int Oid;

class ColumnstoreMetadata {
public:
    ColumnstoreMetadata(Snapshot snapshot) : snapshot(snapshot) {}

public:
    void TablesInsert(Oid oid, const string &path);
    void TablesDelete(Oid oid);
    string TablesSearch(Oid oid);

    void DataFilesInsert(Oid oid, const string &file_name);
    void DataFilesDelete(const string &file_name);
    void DataFilesDelete(Oid oid);
    vector<string> DataFilesSearch(Oid oid);

private:
    Snapshot snapshot;
};

} // namespace duckdb
