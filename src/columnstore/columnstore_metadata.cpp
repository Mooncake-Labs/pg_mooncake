#include "columnstore/columnstore_metadata.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
}

namespace duckdb {

Oid Mooncake() {
    return get_namespace_oid("mooncake", false /*missing_ok*/);
}

const int x_tables_natts = 2;

Oid Tables() {
    return get_relname_relid("tables", Mooncake());
}

Oid TablesOid() {
    return get_relname_relid("tables_oid", Mooncake());
}

void ColumnstoreMetadata::TablesInsert(Oid oid, const string &path) {
    ::Relation table = table_open(Tables(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_tables_natts] = {oid, CStringGetTextDatum(path.c_str())};
    bool nulls[x_tables_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, nulls);
    CatalogTupleInsert(table, tuple);
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
        CatalogTupleDelete(table, &tuple->t_self);
    }

    systable_endscan_ordered(scan);
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
    CommandCounterIncrement();
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
    return path;
}

const int x_data_files_natts = 2;

Oid DataFiles() {
    return get_relname_relid("data_files", Mooncake());
}

Oid DataFilesOid() {
    return get_relname_relid("data_files_oid", Mooncake());
}

Oid DataFilesFileName() {
    return get_relname_relid("data_files_file_name", Mooncake());
}

void ColumnstoreMetadata::DataFilesInsert(Oid oid, const string &file_name) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_data_files_natts] = {oid, CStringGetTextDatum(file_name.c_str())};
    bool isnull[x_data_files_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, isnull);
    CatalogTupleInsert(table, tuple);
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
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        CatalogTupleDelete(table, &tuple->t_self);
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
        CatalogTupleDelete(table, &tuple->t_self);
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
        file_names.push_back(TextDatumGetCString(values[1]));
    }

    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
    return file_names;
}

Oid SecretsOid() {
    return get_relname_relid("secrets", Mooncake());
}

const int x_secrets_natts = 3;

string ColumnstoreMetadata::SecretGet() {
    Relation table = table_open(SecretsOid(), AccessShareLock);
    HeapTuple tuple;
    Datum values[x_secrets_natts];
    bool isnull[x_secrets_natts];
    TupleDesc desc = RelationGetDescr(table);
    SysScanDescData *scan = systable_beginscan(table, InvalidOid /*indexId*/, false /*indexOK*/,
                                               GetTransactionSnapshot(), 0 /*nkeys*/, NULL /*key*/);
    string ret = "{}";
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        // ToDo: support other types and multiple secret with same type
        //
        if (strcmp(TextDatumGetCString(values[1]), "S3")) {
            ret = TextDatumGetCString(values[2]);
        }
    }
    systable_endscan(scan);
    table_close(table, AccessShareLock);
    return ret;
}

string ColumnstoreMetadata::GenerateFullPath(Oid oid, const string& path)
{
    string ret = path;
    if (ret.empty()) {
        Relation rel = RelationIdGetRelation(oid);
        ret = psprintf("mooncake_%s_%d/", RelationGetRelationName(rel), oid);
        RelationClose(rel);
    }
    if (ret.back() != '/') {
        ret += "/";
    }
    if (ret[0] != '/') {
        const char* data_directory = GetConfigOption("data_directory", false, false);
        ret = string(data_directory) + "/" + ret.c_str();
    }
    return ret;
}
} // namespace duckdb
