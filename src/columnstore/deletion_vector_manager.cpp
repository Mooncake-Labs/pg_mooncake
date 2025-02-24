#include "deletion_vector_manager.hpp"
#include "duckdb/common/constants.hpp"
#include "filesystem"
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

Datum StringGetTextDatum(const string_t &s) {
    return PointerGetDatum(cstring_to_text_with_len(s.GetData(), s.GetSize()));
}

Datum StringGetUUIDDatum(const std::string &file_path) {
    std::filesystem::path p(file_path);
    const auto &uuid_str = p.stem().string();
    return DirectFunctionCall1(uuid_in, CStringGetDatum(uuid_str.c_str()));
}

string_t TextDatumGetStringT(Datum d) {
    auto validity_mask_text = PG_DETOAST_DATUM_PACKED(d);
    uint32_t size = VARSIZE_ANY_EXHDR(validity_mask_text);
    char *ptr = VARDATA_ANY(validity_mask_text);
    return string_t(ptr, size);
}

Oid Mooncake() {
    return get_namespace_oid("mooncake", false);
}

static Oid DeletionVectors() {
    return get_relname_relid("deletion_vectors", Mooncake());
}

Oid DeletionVectorsFileGroupChunk() {
    return get_relname_relid("deletion_vectors_uuid_chunk", Mooncake());
}

} // namespace

void DVManager::UpsertDV(const std::string &file_path, uint64_t chunk_idx, const DeletionVector &deletion_vector) {
    UpsertDVOperation op;
    op.file_path = file_path;
    op.chunk_idx = chunk_idx;
    op.deletion_vector = deletion_vector;

    upsert_operations_buffer.push_back(std::move(op));
    if (upsert_operations_buffer.size() >= x_op_batch_size) {
        Flush();
    }
}

DeletionVector DVManager::FetchDV(const string &file_path, const uint64_t chunk_idx) {
    auto file_it = dv_prefetch_cache.find(file_path);
    if (file_it == dv_prefetch_cache.end()) {
        return DeletionVector();
    }
    auto &chunk_map = file_it->second;
    auto chunk_it = chunk_map.find(chunk_idx);
    if (chunk_it == chunk_map.end()) {
        return DeletionVector();
    }
    return DeletionVector::Deserialize(chunk_it->second);
}

void DVManager::PreFetchDVs(const vector<string> &file_paths) {
    ::Relation table = table_open(DeletionVectors(), AccessShareLock);
    ::Relation index = index_open(DeletionVectorsFileGroupChunk(), AccessShareLock);

    for (auto &file_path : file_paths) {
        if (dv_prefetch_cache.find(file_path) != dv_prefetch_cache.end()) {
            continue;
        }

        dv_prefetch_cache[file_path] = {};

        ScanKeyData key[1];
        ScanKeyInit(&key[0], 1, BTEqualStrategyNumber, F_UUID_EQ, StringGetUUIDDatum(file_path));

        SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 1, key);
        HeapTuple tuple;

        Datum values[x_deletion_vectors_natts];
        bool isnull[x_deletion_vectors_natts];

        while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection))) {
            heap_deform_tuple(tuple, RelationGetDescr(table), values, isnull);

            if (!isnull[1]) {
                auto chunk_idx = DatumGetInt64(values[1]);
                if (!isnull[2]) {
                    auto serialized_mask = TextDatumGetStringT(values[2]);
                    dv_prefetch_cache[file_path][chunk_idx] = serialized_mask;
                }
            }
        }
        systable_endscan_ordered(scan);
    }

    index_close(index, AccessShareLock);
    table_close(table, AccessShareLock);
}

void DVManager::FlushUpsertDV() {
    ::Relation table = table_open(DeletionVectors(), RowExclusiveLock);
    ::Relation index = index_open(DeletionVectorsFileGroupChunk(), RowExclusiveLock);

    for (auto &op : upsert_operations_buffer) {
        FlushDeleteDV(op, table, index, snapshot);
        FlushInsertDV(op, table);
    }

    index_close(index, RowExclusiveLock);
    table_close(table, RowExclusiveLock);
}

void DVManager::FlushDeleteDV(const UpsertDVOperation &op, ::Relation table, ::Relation index, Snapshot snapshot) {
    ScanKeyData key[2];
    ScanKeyInit(&key[0], 1, BTEqualStrategyNumber, F_UUID_EQ, StringGetUUIDDatum(op.file_path));
    ScanKeyInit(&key[1], 2, BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(op.chunk_idx));

    SysScanDesc scan = systable_beginscan_ordered(table, index, snapshot, 2, key);
    HeapTuple tuple = systable_getnext_ordered(scan, ForwardScanDirection);

    if (HeapTupleIsValid(tuple)) {
        PostgresFunctionGuard(CatalogTupleDelete, table, &tuple->t_self);
    }
    systable_endscan_ordered(scan);
}

void DVManager::FlushInsertDV(UpsertDVOperation &op, ::Relation table) {
    bool nulls[x_deletion_vectors_natts] = {false, false, false};

    MemoryStream write_stream;
    DeletionVector::Serialize(write_stream, op.deletion_vector);
    auto serialized_mask = string_t(reinterpret_cast<const char *>(write_stream.GetData()),
                                    static_cast<uint32_t>(write_stream.GetPosition()));

    Datum values[x_deletion_vectors_natts] = {StringGetUUIDDatum(op.file_path), Int64GetDatum(op.chunk_idx),
                                              StringGetTextDatum(serialized_mask)};

    HeapTuple new_tuple = heap_form_tuple(RelationGetDescr(table), values, nulls);
    PostgresFunctionGuard(CatalogTupleInsert, table, new_tuple);
    CommandCounterIncrement();
}

void DVManager::ApplyDeletionVectors(const FileChunkDVMap &file_chunk_map, const vector<string> &file_paths) {
    for (auto &file_entry : file_chunk_map) {
        const auto file_number = file_entry.first;
        const auto &file_path = file_paths[file_number];
        const auto &chunk_map = file_entry.second;

        for (auto &chunk_entry : chunk_map) {
            auto chunk_idx = chunk_entry.first;
            const auto &new_dv = chunk_entry.second;
            DeletionVector old_dv = FetchDV(file_path, chunk_idx);
            DeletionVector combined_dv = new_dv;
            combined_dv.Combine(old_dv, old_dv.Size());
            UpsertDV(file_path, chunk_idx, std::move(combined_dv));
        }
    }
}

void DVManager::FilterChunk(const string &file_path, int64_t *file_row_numbers_data, DataChunk &chunk) {
    idx_t sel_size = 0;
    SelectionVector sel(STANDARD_VECTOR_SIZE);
    idx_t last_chunk_idx = DConstants::INVALID_INDEX;
    DeletionVector dv;

    for (idx_t i = 0; i < chunk.size(); i++) {
        auto offset_in_file = NumericCast<uint32_t>(file_row_numbers_data[i]);
        auto chunk_idx = offset_in_file / STANDARD_VECTOR_SIZE;
        auto offset_in_chunk = offset_in_file % STANDARD_VECTOR_SIZE;

        if (last_chunk_idx != chunk_idx) {
            dv = FetchDV(file_path, chunk_idx);
            last_chunk_idx = chunk_idx;
        }
        if (!dv.IsDeleted(offset_in_chunk)) {
            sel.set_index(sel_size++, i);
        }
    }

    chunk.Slice(sel, sel_size);
}

FileChunkDVMap DVManager::BuildFileChunkDVs(const vector<row_t> &row_ids) {
    FileChunkDVMap file_chunk_map;
    for (const auto row_id : row_ids) {
        auto file_number = int32_t(row_id >> 32);
        auto offset_in_file = uint32_t(row_id & 0xFFFFFFFF);
        idx_t chunk_idx = offset_in_file / STANDARD_VECTOR_SIZE;
        idx_t offset_in_chunk = offset_in_file % STANDARD_VECTOR_SIZE;
        file_chunk_map[file_number][chunk_idx].MarkDeleted(offset_in_chunk);
    }
    return file_chunk_map;
}

void DVManager::ReadAndAppendDeletedRows(ClientContext &context, ParquetReader &reader, const ChunkDVMap &chunk_dv_map,
                                         const ColumnList &columns, ColumnDataCollection *return_collection) {
    ParquetReaderScanState state;
    DataChunk chunk;
    chunk.Initialize(Allocator::Get(context), columns.GetColumnTypes());

    vector<idx_t> groups_to_read(reader.GetFileMetadata()->row_groups.size());
    reader.InitializeScan(context, state, std::move(groups_to_read));

    for (auto &[chunk_idx, current_dv] : chunk_dv_map) {
        idx_t current_chunk_idx = 0;
        while (true) {
            chunk.Reset();
            reader.Scan(state, chunk);
            if (chunk.size() == 0) {
                break;
            }
            if (current_chunk_idx == chunk_idx) {
                current_dv.ApplyToChunk(chunk);
                return_collection->Append(chunk);
                break;
            }
            current_chunk_idx++;
        }
    }
}

} // namespace duckdb
