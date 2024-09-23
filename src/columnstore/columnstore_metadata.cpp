#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
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
