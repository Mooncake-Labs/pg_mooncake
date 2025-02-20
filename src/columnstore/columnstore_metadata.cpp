#include "columnstore/columnstore_metadata.hpp"
#include "columnstore/columnstore_statistics.hpp"
#include "parquet_reader.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgmooncake_guc.hpp"

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

Datum StringGetTextDatum(const string &s) {
    return PointerGetDatum(cstring_to_text_with_len(s.data(), s.size()));
}
Datum StringGetTextDatum(const string_t &s) {
    return PointerGetDatum(cstring_to_text_with_len(s.GetData(), s.GetSize()));
}

constexpr int x_tables_natts = 3;
constexpr int x_data_files_natts = 3;
constexpr int x_dead_data_files_natts = 3;
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
Oid DeadDataFiles() {
    return get_relname_relid("dead_data_files", Mooncake());
}
Oid DeadDataFilesOid() {
    return get_relname_relid("dead_data_files_oid", Mooncake());
}
Oid Secrets() {
    return get_relname_relid("secrets", Mooncake());
}

} // namespace

void ColumnstoreMetadata::TablesInsert(Oid oid, const string &path) {
    ::Relation table = table_open(Tables(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_tables_natts] = {oid, StringGetTextDatum(path), CStringGetTextDatum(mooncake_timeline_id)};
    bool nulls[x_tables_natts] = {false, false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, nulls);
    PostgresFunctionGuard(CatalogTupleInsert, table, tuple);
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
        PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

std::tuple<string /*path*/, string /*timeline_id*/> ColumnstoreMetadata::TablesSearch(Oid oid) {
    ::Relation table = table_open(Tables(), AccessShareLock);
    ::Relation index = index_open(TablesOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    string path;
    string timeline_id;
    HeapTuple tuple;
    Datum values[x_tables_natts];
    bool isnull[x_tables_natts];
    if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        path = TextDatumGetCString(values[1]);
        timeline_id = TextDatumGetCString(values[2]);
    }

    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
    return {std::move(path), std::move(timeline_id)};
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

std::tuple<string /*table_name*/, vector<string> /*column_names*/, vector<string> /*column_types*/>
ColumnstoreMetadata::GetTableMetadata(Oid oid) {
    ::Relation table = table_open(oid, AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    string table_name = RelationGetRelationName(table);
    vector<string> column_names;
    vector<string> column_types;
    for (int i = 0; i < desc->natts; i++) {
        Form_pg_attribute attr = &desc->attrs[i];
        column_names.emplace_back(NameStr(attr->attname));
        column_types.emplace_back(format_type_be(attr->atttypid));
    }
    table_close(table, AccessShareLock);
    return {std::move(table_name), std::move(column_names), std::move(column_types)};
}

void ColumnstoreMetadata::DataFilesInsert(Oid oid, const string &file_name, const string_t &file_metadata) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    Datum values[x_data_files_natts] = {oid, StringGetTextDatum(file_name), StringGetTextDatum(file_metadata)};
    bool isnull[x_data_files_natts] = {false, false, false};
    HeapTuple tuple = heap_form_tuple(desc, values, isnull);
    PostgresFunctionGuard(CatalogTupleInsert, table, tuple);
    CommandCounterIncrement();
    table_close(table, RowExclusiveLock);
}

// A wrapper for inserting tuple from `data_files` to `dead_data_files`
//  - table: `dead_data_files` relation
//  - desc: TupleDesc of `data_files`
//  - tuple: HeapTuple of `data_files`
static inline void DataFilesTrackDelete(::Relation table, TupleDesc desc, HeapTuple tuple) {
    Datum values[x_dead_data_files_natts] = {0};
    bool isnull[x_dead_data_files_natts] = {0};
    values[0] = heap_getattr(tuple, 1, desc, &isnull[0]);
    values[1] = heap_getattr(tuple, 2, desc, &isnull[1]);
    values[2] = TimestampGetDatum(GetCurrentTransactionStartTimestamp());
    D_ASSERT(!isnull[0] && !isnull[1]);
    HeapTuple new_tuple = heap_form_tuple(RelationGetDescr(table), values, isnull);
    PostgresFunctionGuard(CatalogTupleInsert, table, new_tuple);
}

void ColumnstoreMetadata::DataFilesDelete(const string &file_name) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    ::Relation index = index_open(DataFilesFileName(), RowExclusiveLock);
    ::Relation dead_data_files_table = table_open(DeadDataFiles(), RowExclusiveLock);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 2 /*attributeNumber*/, BTEqualStrategyNumber, F_TEXTEQ, StringGetTextDatum(file_name));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    HeapTuple tuple;
    if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        DataFilesTrackDelete(dead_data_files_table, RelationGetDescr(table), tuple);
        PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
        columnstore_stats.Delete(file_name);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
    table_close(dead_data_files_table, RowExclusiveLock);
    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

void ColumnstoreMetadata::DataFilesDelete(Oid oid) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    ::Relation index = index_open(DataFilesOid(), RowExclusiveLock);
    ::Relation dead_data_files_table = table_open(DeadDataFiles(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    HeapTuple tuple;
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        bool isnull;
        auto file_name = TextDatumGetCString(heap_getattr(tuple, 2 /*attnum*/, desc, &isnull));
        columnstore_stats.Delete(file_name);
        DataFilesTrackDelete(dead_data_files_table, desc, tuple);
        PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
    table_close(dead_data_files_table, RowExclusiveLock);
    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

vector<string> ColumnstoreMetadata::DataFilesSearch(Oid oid, ClientContext *context, const string *path,
                                                    const ColumnList *columns) {
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
        auto file_name = TextDatumGetCString(values[1]);
        file_names.emplace_back(file_name);
        if (context && !columnstore_stats.Get<DataFileStatistics>(file_name)) {
            using duckdb_apache::thrift::protocol::TCompactProtocolT;
            using duckdb_apache::thrift::transport::TMemoryBuffer;
            using duckdb_parquet::FileMetaData;

            auto file_metadata_text = PG_DETOAST_DATUM_PACKED(values[2]);
            auto transport = std::make_shared<TMemoryBuffer>(data_ptr_cast(VARDATA_ANY(file_metadata_text)),
                                                             VARSIZE_ANY_EXHDR(file_metadata_text));
            auto protocol = make_uniq<TCompactProtocolT<TMemoryBuffer>>(std::move(transport));
            auto file_metadata = make_uniq<FileMetaData>();
            file_metadata->read(protocol.get());
            auto metadata = make_shared_ptr<ParquetFileMetadataCache>(
                std::move(file_metadata), std::numeric_limits<time_t>::max() /*read_time*/, nullptr /*geo_metadata*/);
            if (mooncake_enable_memory_metadata_cache) {
                ObjectCache::GetObjectCache(*context).Put(*path + file_name, metadata);
                ObjectCache::GetObjectCache(*context).Put(string(x_mooncake_local_cache) + file_name, metadata);
            }
            // HACK: use a dummy file_name since reader only reads statistics from metadata
            ParquetReader reader(*context, "/dev/null", ParquetOptions{}, std::move(metadata));
            auto file_stats = make_shared_ptr<DataFileStatistics>(reader, *columns);
            columnstore_stats.Put(file_name, std::move(file_stats));
        }
    }

    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
    return file_names;
}

vector<string> ColumnstoreMetadata::DeadDataFilesSearch(Oid oid) {
    ::Relation table = table_open(DeadDataFiles(), AccessShareLock);
    ::Relation index = index_open(DeadDataFilesOid(), AccessShareLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    vector<string> file_names;
    HeapTuple tuple;
    bool isnull;
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        auto datum = heap_getattr(tuple, 2, desc, &isnull);
        D_ASSERT(!isnull);
        auto file_name = TextDatumGetCString(datum);
        file_names.emplace_back(file_name);
        pfree(file_name);
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
            queries.emplace_back(TextDatumGetCString(values[3]));
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
