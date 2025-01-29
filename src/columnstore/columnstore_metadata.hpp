#pragma once

#include "duckdb/common/vector.hpp"
#include "pgduckdb/pg/declarations.hpp"

namespace duckdb {

class ClientContext;
class ColumnList;
struct string_t;

class ColumnstoreMetadata {
public:
    explicit ColumnstoreMetadata(Snapshot snapshot) : snapshot(snapshot) {}

public:
    void TablesInsert(Oid oid, const string &path);
    void TablesDelete(Oid oid);
    std::tuple<string /*path*/, string /*timeline_id*/> TablesSearch(Oid oid);

    string GetTablePath(Oid oid);
    std::tuple<string /*table_name*/, vector<string> /*column_names*/, vector<string> /*column_types*/>
    GetTableMetadata(Oid oid);

    void DataFilesInsert(Oid oid, const string &file_name, const string_t &file_metadata);
    void DataFilesDelete(const string &file_name);
    void DataFilesDelete(Oid oid);
    vector<string> DataFilesSearch(Oid oid, ClientContext *context = nullptr, const string *path = nullptr,
                                   const ColumnList *columns = nullptr);

    vector<string> SecretsGetDuckdbQueries();
    string SecretsSearchDeltaOptions(const string &path);

private:
    Snapshot snapshot;
};

} // namespace duckdb
