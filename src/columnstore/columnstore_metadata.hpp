#pragma once

#include "duckdb/common/vector.hpp"

namespace duckdb {

typedef unsigned int Oid;

class ColumnstoreMetadata {
public:
    static void TablesInsert(Oid oid, const string &path);
    static void TablesDelete(Oid oid);
    static string TablesSearch(Oid oid);

    static void DataFilesInsert(Oid oid, const string &file_name);
    static void DataFilesDelete(const string &file_name);
    static void DataFilesDelete(Oid oid);
    static vector<string> DataFilesSearch(Oid oid);
};

} // namespace duckdb
