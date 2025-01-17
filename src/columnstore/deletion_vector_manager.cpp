// dv_manager.cpp
#include "deletion_vector_manager.hpp"
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

Oid Mooncake() {
    return get_namespace_oid("mooncake", false /*missing_ok*/);
}
static Oid DeletionVectors() {
    return get_relname_relid("deletion_vectors", Mooncake());
}
Oid DeletionVectorsFileGroupChunk() {
    return get_relname_relid("deletion_vectors_file_chunk", Mooncake());
}

} // namespace

void DVManager::UpsertDV(const std::string &file_name,
                         uint64_t chunk_idx,
                         const DeletionVector &deletion_vector) {
    UpsertDVOperation op;
    op.file_name = file_name;
    op.chunk_idx = chunk_idx;
    op.deletion_vector = deletion_vector;

    upsert_operations_buffer.push_back(std::move(op));
    if (upsert_operations_buffer.size() >= x_op_batch_size) {
        Flush();
    }
}

DeletionVector DVManager::FetchDV(const string &file_name, const uint64_t chunk_idx) {
    ::Relation table = table_open(DeletionVectors(), AccessShareLock);
    ::Relation index = index_open(DeletionVectorsFileGroupChunk(), AccessShareLock);
    ScanKeyData key[2];
    ScanKeyInit(&key[0], 1 /* attributeNumber */, BTEqualStrategyNumber, F_TEXTEQ, StringGetTextDatum(file_name));
    ScanKeyInit(&key[1], 2 /* attributeNumber */, BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(chunk_idx));
    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 2 /*nkeys*/, key);

    string deletion_vector;
    HeapTuple tuple;
    Datum values[x_deletion_vectors_natts];
    bool isnull[x_deletion_vectors_natts];
    if (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
        heap_deform_tuple(tuple, RelationGetDescr(table), values, isnull);
        deletion_vector = TextDatumGetCString(values[2]);
    }

    systable_endscan_ordered(scan);
    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);

    return DeletionVector::Deserialize(deletion_vector);
}

void DVManager::FlushUpsertDV()
{
    ::Relation table = table_open(DeletionVectors(), RowExclusiveLock);
    ::Relation index = index_open(DeletionVectorsFileGroupChunk(), RowExclusiveLock);

    for (auto &op : upsert_operations_buffer) {
        DeleteDV(op, table, index, snapshot);
        InsertDV(op, table);
    }

    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

void DVManager::DeleteDV(const UpsertDVOperation &op,
                                 ::Relation table,
                                 ::Relation index,
                                 Snapshot snapshot)
{
    ScanKeyData key[2];
    ScanKeyInit(&key[0],
                1 /* attributeNumber */,
                BTEqualStrategyNumber,
                F_TEXTEQ,
                StringGetTextDatum(op.file_name));
    ScanKeyInit(&key[1],
                2 /* attributeNumber */,
                BTEqualStrategyNumber,
                F_INT8EQ,
                Int64GetDatum(op.chunk_idx));

    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 2, key);
    HeapTuple tuple = systable_getnext_ordered(scan, ForwardScanDirection);

    if (HeapTupleIsValid(tuple)) {
        PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }
    systable_endscan_ordered(scan);
}

void DVManager::InsertDV(const UpsertDVOperation &op, ::Relation table)
{
    bool nulls[x_deletion_vectors_natts] = {false, false, false};
    Datum values[x_deletion_vectors_natts];

    values[0] = StringGetTextDatum(op.file_name);
    values[1] = Int64GetDatum(op.chunk_idx);
    values[2] = StringGetTextDatum(DeletionVector::Serialize(op.deletion_vector));

    HeapTuple new_tuple = heap_form_tuple(RelationGetDescr(table), values, nulls);
    PostgresFunctionGuard(CatalogTupleInsert, table, new_tuple);
    CommandCounterIncrement();
}


} // namespace duckdb
