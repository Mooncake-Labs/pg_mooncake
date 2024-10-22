#include "columnstore/columnstore_metadata.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

#include <sys/stat.h>

extern bool allow_local_disk_table;
extern char *default_storage_bucket;

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

Oid CacheOid() {
    return get_relname_relid("cache", Mooncake());
}

Oid CacheKeyOid() {
    return get_relname_relid("cache_key", Mooncake());
}

const int x_cache_natts = 2;

void ColumnstoreMetadata::CacheAdd(Oid oid, int64_t file_id) {
    Relation table = table_open(CacheOid(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_cache_natts] = {oid, Int64GetDatum(file_id)};
    bool isnull[x_cache_natts] = {false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, isnull);
    CatalogTupleInsert(table, tuple);
    CommandCounterIncrement();
    table_close(table, RowExclusiveLock);
}

string GetCachedFile(int64_t file_id) {
    struct stat info;
    string cache_file_path = "mooncake_cache/" + std::to_string(file_id) + ".parquet";
    // Check cache entry for safety
    //
    if (stat(cache_file_path.c_str(), &info) != 0) {
        return "";
    }
    return cache_file_path;
};

void ReplaceRemoteFileWithCache(Oid oid, std::vector<ColumnstoreMetadata::FileInfo> &files) {
    Relation table = table_open(CacheOid(), AccessShareLock);
    Relation index = index_open(CacheKeyOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, NULL /*snapshot*/, 1 /*nkeys*/, key);

    HeapTuple tuple;
    Datum values[x_cache_natts];
    bool isnull[x_cache_natts];
    int i = 0;
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        int64_t cached_file = DatumGetInt64(values[1]);
        while (i < files.size() && files[i].file_id < cached_file) {
            i++;
        }
        if (i == files.size()) {
            break;
        }
        if (files[i].file_id == cached_file) {
            string cached_file_path = GetCachedFile(cached_file);
            if (!cached_file_path.empty()) {
                files[i].file_path = cached_file_path;
            }
        }
    }
    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
}

const int x_data_files_natts = 3;

Oid DataFiles() {
    return get_relname_relid("data_files", Mooncake());
}

Oid DataFilesOid() {
    return get_relname_relid("data_files_oid", Mooncake());
}

Oid DataFilesPK() {
    return get_relname_relid("data_files_pkey", Mooncake());
}

Oid DataFilesSequenceOid() {
    return get_relname_relid("data_files_id_seq", Mooncake());
}

int64_t ColumnstoreMetadata::DataFilesInsert(Oid oid, const string &file_name) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);

    int64_t file_id = nextval_internal(DataFilesSequenceOid(), true);
    Datum values[x_data_files_natts] = {Int64GetDatum(file_id), oid, CStringGetTextDatum(file_name.c_str())};
    bool isnull[x_data_files_natts] = {false, false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, isnull);
    CatalogTupleInsert(table, tuple);
    CommandCounterIncrement();
    table_close(table, RowExclusiveLock);
    return file_id;
}

void ColumnstoreMetadata::DataFilesDelete(int64_t file_id) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    ::Relation index = index_open(DataFilesPK(), RowExclusiveLock);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(file_id));
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
    ScanKeyInit(&key[0], 2 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
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

vector<ColumnstoreMetadata::FileInfo> ColumnstoreMetadata::DataFilesSearch(Oid oid, const string &path) {
    ::Relation table = table_open(DataFiles(), AccessShareLock);
    ::Relation index = index_open(DataFilesOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 2 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    vector<FileInfo> files;
    HeapTuple tuple;
    Datum values[x_data_files_natts];
    bool isnull[x_data_files_natts];
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        files.push_back({DatumGetInt64(values[0]), path + TextDatumGetCString(values[2])});
    }

    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
    ReplaceRemoteFileWithCache(oid, files);
    return files;
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

string ColumnstoreMetadata::GenerateFullPath(Oid oid, const string &path, bool remote) {
    string ret = path;
    if (ret.empty()) {
        Relation rel = RelationIdGetRelation(oid);
        ret = psprintf("mooncake_%s_%d/", RelationGetRelationName(rel), oid);
        RelationClose(rel);
    }
    if (ret.back() != '/') {
        ret += "/";
    }
    if (!remote && ret[0] != '/') {
        if (default_storage_bucket != NULL) {
            ret = string(default_storage_bucket) + "/" + ret.c_str();
        } else if (allow_local_disk_table) {
            const char *data_directory = GetConfigOption("data_directory", false, false);
            ret = string(data_directory) + "/" + ret.c_str();
        } else {
            elog(ERROR, "create columnstore table with local disk is not supported"
                        ", provide a remote path or set pg_mooncake.default_storage_bucket");
        }
    } else if (!remote && !allow_local_disk_table) {
        elog(ERROR, "create columnstore table with local disk is not supported"
                    ", provide a remote path or set pg_mooncake.default_storage_bucket");
    }
    return ret;
}
} // namespace duckdb
