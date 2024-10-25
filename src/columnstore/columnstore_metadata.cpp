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
}

#include "columnstore/columnstore.hpp"

Oid MooncakeOid() {
    return get_namespace_oid("mooncake", false /*missing_ok*/);
}

const int x_tables_natts = 2;

Oid TablesOid() {
    return get_relname_relid("tables", MooncakeOid());
}

Oid TablesPkeyOid() {
    return get_relname_relid("tables_pkey", MooncakeOid());
}

void TablesAdd(Oid oid, const char *path) {
    Relation table = table_open(TablesOid(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_tables_natts] = {oid, CStringGetTextDatum(path)};
    bool nulls[x_tables_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, nulls);
    CatalogTupleInsert(table, tuple);
    CommandCounterIncrement();
    table_close(table, RowExclusiveLock);
}

const char *TablesGet(Oid oid) {
    Relation table = table_open(TablesOid(), AccessShareLock);
    Relation index = index_open(TablesPkeyOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, NULL /*snapshot*/, 1 /*nkeys*/, key);

    const char *path = NULL;
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

const int x_data_files_natts = 2;

Oid DataFilesOid() {
    return get_relname_relid("data_files", MooncakeOid());
}

Oid DataFilesKeyOid() {
    return get_relname_relid("data_files_key", MooncakeOid());
}

void DataFilesAdd(Oid oid, const char *file_name) {
    Relation table = table_open(DataFilesOid(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_data_files_natts] = {oid, CStringGetTextDatum(file_name)};
    bool isnull[x_data_files_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, isnull);
    CatalogTupleInsert(table, tuple);
    CommandCounterIncrement();
    table_close(table, RowExclusiveLock);
}

std::vector<const char *> DataFilesGet(Oid oid) {
    Relation table = table_open(DataFilesOid(), AccessShareLock);
    Relation index = index_open(DataFilesKeyOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, NULL /*snapshot*/, 1 /*nkeys*/, key);

    std::vector<const char *> file_names;
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
