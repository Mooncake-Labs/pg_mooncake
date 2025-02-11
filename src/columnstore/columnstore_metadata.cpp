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
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

#include "duckdb/common/local_file_system.hpp"

#include <fstream>

namespace {
std::string GetTextFromDatun(Datum datum) {
    if (DatumGetPointer(datum) == NULL) {
        return "";
    }
    text *t = DatumGetTextP(datum);
    return std::string(VARDATA(t), VARSIZE(t) - VARHDRSZ);
}

template <typename IntType> std::vector<IntType> DatumToIntArray(Datum datum) {
    ArrayType *arr = DatumGetArrayTypeP(datum);
    D_ASSERT(arr != nullptr);
    int num_elements = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

    std::vector<IntType> result;
    result.reserve(num_elements);
    IntType *data = (IntType *)ARR_DATA_PTR(arr);
    for (int idx = 0; idx < num_elements; ++idx) {
        result.emplace_back(data[idx]);
    }
    return result;
}

std::vector<std::string> ExtractFileNames(Datum file_paths_datum) {
    ArrayType *file_paths_array = DatumGetArrayTypeP(file_paths_datum);
    D_ASSERT(file_paths_array != nullptr);
    const int array_length = ArrayGetNItems(ARR_NDIM(file_paths_array), ARR_DIMS(file_paths_array));

    std::vector<std::string> file_names;
    file_names.reserve(array_length);
    for (int idx = 0; idx < array_length; ++idx) {
        int indx[1] = {idx};
        bool isNull = false;
        Datum element = array_get_element(file_paths_datum, /*nSubscripts=*/1, indx, /*arraytyplen=*/-1,
                                          /*elmlen=*/sizeof(text), /*elmbyval=*/false, /*elmalign=*/'d', &isNull);
        file_names.emplace_back(GetTextFromDatun(element));
    }
    return file_names;
}
} // namespace

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
constexpr int x_secrets_natts = 5;
constexpr int x_delta_natts = 6;

// Delta updata record table, whose lifecycle is decoupled from txn lifecycle, and never destructs until process
// termination.
::Relation delta_update_record_table = NULL;

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
Oid DeltaUpdateRecord() {
    return get_relname_relid("delta_update_records", Mooncake());
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
    column_names.reserve(desc->natts);
    vector<string> column_types;
    column_types.reserve(desc->natts);
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

void ColumnstoreMetadata::DataFilesDelete(const string &file_name) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    ::Relation index = index_open(DataFilesFileName(), RowExclusiveLock);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 2 /*attributeNumber*/, BTEqualStrategyNumber, F_TEXTEQ, StringGetTextDatum(file_name));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    HeapTuple tuple;
    if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
        columnstore_stats.Delete(file_name);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

void ColumnstoreMetadata::DataFilesDelete(Oid oid) {
    ::Relation table = table_open(DataFiles(), RowExclusiveLock);
    ::Relation index = index_open(DataFilesOid(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(table);
    ScanKeyData key[1];
    ScanKeyInit(&key[0], 1 /*attributeNumber*/, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1 /*nkeys*/, key);

    HeapTuple tuple;
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        bool isnull;
        auto file_name = TextDatumGetCString(heap_getattr(tuple, 2 /*attnum*/, desc, &isnull));
        columnstore_stats.Delete(file_name);
        PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }

    systable_endscan_ordered(scan);
    CommandCounterIncrement();
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
    unique_ptr<ParquetReader> reader;
    while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        heap_deform_tuple(tuple, desc, values, isnull);
        auto file_name = TextDatumGetCString(values[1]);
        file_names.emplace_back(file_name);
        if (context && !columnstore_stats.Get<DataFileStatistics>(file_name)) {
            using duckdb_apache::thrift::protocol::TCompactProtocolT;
            using duckdb_apache::thrift::transport::TMemoryBuffer;
            using duckdb_parquet::format::FileMetaData;

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
            if (!reader) {
                // HACK: use a dummy file_name since reader only reads statistics from metadata
                reader = make_uniq<ParquetReader>(*context, "/dev/null", ParquetOptions{}, std::move(metadata));
            } else {
                reader->metadata = std::move(metadata);
            }
            auto file_stats = make_shared_ptr<DataFileStatistics>(*reader, *columns);
            columnstore_stats.Put(file_name, std::move(file_stats));
        }
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

void ColumnstoreMetadata::InitializeDeltaUpdateRecordTable() {
    if (delta_update_record_table == NULL) {
        delta_update_record_table = table_open(DeltaUpdateRecord(), RowExclusiveLock);
    }
}

void ColumnstoreMetadata::InsertDeltaRecord(const string &path, const string &delta_options,
                                            const vector<string> &file_names, const vector<int64_t> &file_sizes,
                                            const vector<int8_t> &is_add_files) {
    Datum file_paths_datum[file_names.size()];
    for (size_t idx = 0; idx < file_names.size(); ++idx) {
        file_paths_datum[idx] = StringGetTextDatum(file_names[idx]);
    }
    Datum file_sizes_datum[file_sizes.size()];
    for (size_t idx = 0; idx < file_sizes.size(); ++idx) {
        file_sizes_datum[idx] = Int64GetDatum(file_sizes[idx]);
    }
    Datum is_add_files_datum[is_add_files.size()];
    for (size_t idx = 0; idx < is_add_files.size(); ++idx) {
        is_add_files_datum[idx] = Int8GetDatum(is_add_files[idx]);
    }
    Datum file_paths_array = PointerGetDatum(construct_array(
        /*elems=*/file_paths_datum,
        /*nelems=*/file_names.size(),
        /*elmtype=*/TEXTOID,
        /*elmlen=*/sizeof(text),
        /*elmbyval=*/false,
        /*elmalign=*/'d'));
    Datum file_sizes_array = PointerGetDatum(construct_array(
        /*elems=*/file_sizes_datum,
        /*nelems=*/file_sizes.size(),
        /*elmtype=*/INT8OID,
        /*elmlen=*/sizeof(int64_t),
        /*elmbyval=*/true,
        /*elmalign=*/'d'));
    Datum is_add_files_array = PointerGetDatum(construct_array(
        /*elems=*/is_add_files_datum,
        /*nelems=*/is_add_files.size(),
        /*elmtype=*/CHAROID,
        /*elmlen=*/sizeof(int8_t),
        /*elmbyval=*/true,
        /*elmalign=*/'d'));
    Datum values[x_delta_natts] = {
        static_cast<Datum>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count()),                 // timestamp
        StringGetTextDatum(path),          // path
        StringGetTextDatum(delta_options), // delta_option
        file_paths_array,                  // file_paths (TEXT[])
        file_sizes_array,                  // file_sizes (BIGINT[])
        is_add_files_array                 // is_add_files (SMALLINT[])
    };
    constexpr bool is_null[x_delta_natts] = {false, false, false, false, false, false};

    HeapTuple new_tuple = heap_form_tuple(
        /*tupleDescriptor=*/RelationGetDescr(delta_update_record_table),
        /*values=*/values,
        /*isnull=*/is_null);

    PostgresFunctionGuard(CatalogTupleInsert, delta_update_record_table, new_tuple);
    CommandCounterIncrement();
    // TODO(hjiang): We should not close the table here.
    // table_close(delta_update_record_table, RowExclusiveLock);
}

void ColumnstoreMetadata::FlushDeltaRecords(std::function<void(DeltaRecord)> dump_func) {
    ::Relation delta_records_table = table_open(DeltaUpdateRecord(), RowExclusiveLock);
    TupleDesc desc = RelationGetDescr(delta_records_table);
    SysScanDesc scan =
        systable_beginscan(delta_records_table, InvalidOid, false /*indexOK*/, snapshot, 0 /*nkeys*/, NULL /*key*/);

    // Maps from timestamp to delta record.
    map<int64_t, DeltaRecord> delta_records;

    HeapTuple tuple;
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        bool isnull = false;
        Datum col;
        DeltaRecord cur_delta_record;

        col = heap_getattr(tuple, 1 /*attnum*/, desc, &isnull);
        const int64_t timestamp = static_cast<int64_t>(col);

        col = heap_getattr(tuple, 2 /*attnum*/, desc, &isnull);
        cur_delta_record.path = GetTextFromDatun(col);

        col = heap_getattr(tuple, 3 /*attnum*/, desc, &isnull);
        cur_delta_record.delta_options = GetTextFromDatun(col);

        col = heap_getattr(tuple, 4 /*attnum*/, desc, &isnull);
        cur_delta_record.file_names = ExtractFileNames(col);

        col = heap_getattr(tuple, 5 /*attnum*/, desc, &isnull);
        cur_delta_record.file_sizes = DatumToIntArray<int64_t>(col);

        col = heap_getattr(tuple, 6 /*attnum*/, desc, &isnull);
        cur_delta_record.is_add_files = DatumToIntArray<int8_t>(col);

        delta_records.emplace(timestamp, std::move(cur_delta_record));
        PostgresFunctionGuard(CatalogTupleDelete, delta_records_table, &tuple->t_self);
    }

    // Dump delta records in the order of timestamp.
    for (auto &[timestamp, cur_delta_record] : delta_records) {
        dump_func(std::move(cur_delta_record));
    }
    systable_endscan(scan);
    table_close(delta_records_table, RowExclusiveLock);
}

} // namespace duckdb
