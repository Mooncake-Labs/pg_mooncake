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

    int64_t DataFilesInsert(Oid oid, const string &file_name);
    void DataFilesDelete(int64_t file_id);
    void DataFilesDelete(Oid oid);
    struct FileInfo {
        int64_t file_id;
        string file_path;
        string file_name;
    };

    vector<FileInfo> DataFilesSearch(Oid oid);

    string SecretGet();
    string GenerateFullPath(Oid oid);

private:
    Snapshot snapshot;
};

} // namespace duckdb