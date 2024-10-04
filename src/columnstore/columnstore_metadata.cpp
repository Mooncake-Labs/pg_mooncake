#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

Oid MooncakeOid() {
    return get_namespace_oid("mooncake", false /*missing_ok*/);
}

Oid DataFilesOid() {
    return get_relname_relid("data_files", MooncakeOid());
}

Oid TableInfoOid() {
    return get_relname_relid("table_info", MooncakeOid());
}

Oid TableInfoIndexOid() {
    return get_relname_relid("table_info_pkey", MooncakeOid());
}

const int x_data_files_natts = 2;

void DataFilesAdd(Oid relid, const char *file_name) {
    Relation relation = table_open(DataFilesOid(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(relation);
    Datum values[x_data_files_natts] = {relid, CStringGetTextDatum(file_name)};
    bool isnull[x_data_files_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, isnull);
    CatalogTupleInsert(relation, tuple);
    CommandCounterIncrement();
    table_close(relation, RowExclusiveLock);
}

duckdb::vector<duckdb::Value> DataFilesGet(Oid relid) {
    // TODO: index scan
    Relation relation = table_open(DataFilesOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(relation);
    SysScanDescData *scan = systable_beginscan(relation, InvalidOid /*indexId*/, false /*indexOK*/,
                                               GetTransactionSnapshot(), 0 /*nkeys*/, NULL /*key*/);
    duckdb::vector<duckdb::Value> file_names;
    HeapTuple tuple;
    Datum values[x_data_files_natts];
    bool isnull[x_data_files_natts];
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        if (DatumGetObjectId(values[0]) == relid) {
            file_names.push_back(duckdb::Value(TextDatumGetCString(values[1])));
        }
    }
    systable_endscan(scan);
    table_close(relation, AccessShareLock);
    return file_names;
}

const int x_table_info_natts = 4;

void TableInfoAdd(Oid relid, const char *storage_path, const char *lake_format) {
    Relation relation = table_open(TableInfoOid(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(relation);
    Datum values[x_table_info_natts] = {relid, CStringGetTextDatum(storage_path), CStringGetTextDatum(lake_format),
                                        JsonbPGetDatum(NULL)};
    bool nulls[x_table_info_natts] = {false, false, false, true};
    HeapTuple tuple = heap_form_tuple(desc, values, nulls);
    CatalogTupleInsert(relation, tuple);
    CommandCounterIncrement();
    table_close(relation, RowExclusiveLock);
}

void TableInfoGet(Oid relid, const char **storage_path, const char **lake_format) {
    Relation relation = table_open(TableInfoOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(relation);
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0], 1 /* AttrNum */, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));

    Relation index = index_open(TableInfoIndexOid(), AccessShareLock);

    SysScanDesc scan = systable_beginscan_ordered(relation, index, NULL, 1, scanKey);
    HeapTuple tuple = systable_getnext_ordered(scan, ForwardScanDirection);
    Datum values[x_data_files_natts];
    bool isnull[x_data_files_natts];

    if (HeapTupleIsValid(tuple)) {
        heap_deform_tuple(tuple, desc, values, isnull);
        *storage_path = TextDatumGetCString(values[1]);
        *lake_format = TextDatumGetCString(values[2]);
    }
    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(relation, AccessShareLock);
}