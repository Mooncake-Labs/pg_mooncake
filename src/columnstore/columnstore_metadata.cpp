#include "columnstore/columnstore_metadata.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgmooncake.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace duckdb {

namespace {

constexpr int x_tables_natts = 2;
constexpr int x_data_files_natts = 2;
constexpr int x_secrets_natts = 5;

Oid Mooncake() {
    return get_namespace_oid("mooncake", false /*missing_ok*/);
}
Oid Tables() {
    return get_relname_relid("tables", Mooncake());
}
Oid TablesOid() {
    return get_relname_relid("tables_oid", Mooncake());
}
Oid DataFiles() {
    return get_relname_relid("data_files", Mooncake());
}
Oid DataFilesOid() {
    return get_relname_relid("data_files_oid", Mooncake());
}
Oid DataFilesFileName() {
    return get_relname_relid("data_files_file_name", Mooncake());
}
Oid Secrets() {
    return get_relname_relid("secrets", Mooncake());
}

} // namespace

void ColumnstoreMetadata::TablesInsert(Oid oid, const string &path) {
    ::Relation table = table_open(Tables(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_tables_natts] = {oid, CStringGetTextDatum(path.c_str())};
    bool nulls[x_tables_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, nulls);
    pgduckdb::PostgresFunctionGuard(CatalogTupleInsert, table, tuple);
    CommandCounterIncrement();
    table_close(table, RowExclusiveLock);
}

void ColumnstoreMetadata::TablesDelete(Oid oid) {
    ::Relation table = table_open(Tables(), RowExclusiveLock);
    ::Relation index = index_open(TablesOid(), RowExclusiveLock);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    HeapTuple tuple;
    if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        pgduckdb::PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

string ColumnstoreMetadata::TablesSearch(Oid oid) {
    ::Relation table = table_open(Tables(), AccessShareLock);
    ::Relation index = index_open(TablesOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    string path;
    HeapTuple tuple;
    Datum values[x_tables_natts];
    bool isnull[x_tables_natts];
    if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        path = TextDatumGetCString(values[1]);
    }

    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
    return path;
}

string ColumnstoreMetadata::GetTablePath(Oid oid) {
    ::Relation table = table_open(oid, AccessShareLock);
    string path =
        StringUtil::Format("mooncake_%s_%s_%d/", get_database_name(MyDatabaseId), RelationGetRelationName(table), oid);
    table_close(table, AccessShareLock);
    if (mooncake_default_bucket != nullptr && mooncake_default_bucket[0] != '\0') {
        path = StringUtil::Format("%s/%s", mooncake_default_bucket, path);
    } else if (mooncake_allow_local_tables) {
        path = StringUtil::Format("%s/mooncake_local_tables/%s", DataDir, path);
    } else {
        elog(ERROR, "Columnstore tables on local disk are not allowed. Set mooncake.default_bucket to default "
                    "S3 bucket");
    }
    return path;
}

void ColumnstoreMetadata::GetTableMetadata(Oid oid, string &table_name /*out*/, vector<string> &column_names /*out*/,
                                           vector<string> &column_types /*out*/) {
    // Clear out params before emplace.
    column_names.clear();
    column_types.clear();

    ::Relation table = table_open(oid, AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    table_name = RelationGetRelationName(table);

    column_names.reserve(desc->natts);
    column_types.reserve(desc->natts);
    for (int i = 0; i < desc->natts; i++) {
        Form_pg_attribute attr = &desc->attrs[i];
        column_names.emplace_back(NameStr(attr->attname));
        column_types.emplace_back(format_type_be(attr->atttypid));
    }
    table_close(table, AccessShareLock);
}

void ColumnstoreMetadata::DataFilesInsert(Oid oid, const string &file_name) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_data_files_natts] = {oid, CStringGetTextDatum(file_name.c_str())};
    bool isnull[x_data_files_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, isnull);
    pgduckdb::PostgresFunctionGuard(CatalogTupleInsert, table, tuple);
    CommandCounterIncrement();
    table_close(table, RowExclusiveLock);
}

void ColumnstoreMetadata::DataFilesDelete(const string &file_name) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    ::Relation index = index_open(DataFilesFileName(), RowExclusiveLock);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 2 /*attributeNumber*/, BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(file_name.c_str()));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    HeapTuple tuple;
    if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        pgduckdb::PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

void ColumnstoreMetadata::DataFilesDelete(Oid oid) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    ::Relation index = index_open(DataFilesOid(), RowExclusiveLock);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    HeapTuple tuple;
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        pgduckdb::PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

vector<string> ColumnstoreMetadata::DataFilesSearch(Oid oid) {
    ::Relation table = table_open(DataFiles(), AccessShareLock);
    ::Relation index = index_open(DataFilesOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    vector<string> file_names;
    HeapTuple tuple;
    Datum values[x_data_files_natts];
    bool isnull[x_data_files_natts];
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        file_names.emplace_back(TextDatumGetCString(values[1]));
    }

    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
    return file_names;
}

vector<string> ColumnstoreMetadata::SecretsGetDuckdbQueries() {
    ::Relation table = table_open(Secrets(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    SysScanDescData *scan =
        systable_beginscan(table, InvalidOid /*indexId*/, false /*indexOK*/, snapshot, 0 /*nkeys*/, NULL /*key*/);

    vector<string> queries;
    HeapTuple tuple;
    Datum values[x_secrets_natts];
    bool isnull[x_secrets_natts];
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        if (strcmp(TextDatumGetCString(values[1]), "S3") == 0) {
            queries.push_back(TextDatumGetCString(values[3]));
        }
    }

    systable_endscan(scan);
    table_close(table, AccessShareLock);
    return queries;
}

string ColumnstoreMetadata::SecretsSearchDeltaOptions(const string &path) {
    if (!FileSystem::IsRemoteFile(path)) {
        return "{}";
    }

    ::Relation table = table_open(Secrets(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    SysScanDescData *scan =
        systable_beginscan(table, InvalidOid /*indexId*/, false /*indexOK*/, snapshot, 0 /*nkeys*/, NULL /*key*/);

    string option = "{}";
    size_t longest_match = 0;
    HeapTuple tuple;
    Datum values[x_secrets_natts];
    bool isnull[x_secrets_natts];
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        if (strcmp(TextDatumGetCString(values[1]), "S3") == 0) {
            string scope = TextDatumGetCString(values[2]);
            if ((scope.empty() || StringUtil::StartsWith(path, scope)) && longest_match <= scope.length()) {
                option = TextDatumGetCString(values[4]);
                longest_match = scope.length();
            }
        }
    }

    systable_endscan(scan);
    table_close(table, AccessShareLock);
    return option;
}

} // namespace duckdb
