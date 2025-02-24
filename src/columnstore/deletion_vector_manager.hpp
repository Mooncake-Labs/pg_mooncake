#pragma once

#include "columnstore_deletion_vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "parquet_reader.hpp"
#include "pgduckdb/pg/declarations.hpp"

namespace duckdb {

using ChunkDVMap = std::unordered_map<idx_t, DeletionVector>;
using FileChunkDVMap = std::unordered_map<idx_t, ChunkDVMap>;

constexpr int x_deletion_vectors_natts = 3;
constexpr int x_op_batch_size = duckdb::Storage::ROW_GROUP_SIZE;

struct UpsertDVOperation {
    string file_path;
    uint64_t chunk_idx;
    DeletionVector deletion_vector;
};

class DVManager {
public:
    explicit DVManager(Snapshot snapshot) : snapshot(snapshot) {}

    void UpsertDV(const std::string &file_path, uint64_t chunk_idx, const DeletionVector &deletion_vector);

    void PreFetchDVs(const vector<string> &file_paths);

    DeletionVector FetchDV(const std::string &file_path, uint64_t chunk_idx);

    void ApplyDeletionVectors(const FileChunkDVMap &file_chunk_map, const vector<string> &file_paths);

    void FilterChunk(const string &file_path, int64_t *file_row_numbers_data, DataChunk &chunk);

    static FileChunkDVMap BuildFileChunkDVs(const vector<row_t> &row_ids);

    static void ReadAndAppendDeletedRows(ClientContext &context, ParquetReader &reader, const ChunkDVMap &chunk_dv_map,
                                         const ColumnList &columns, ColumnDataCollection *return_collection);

    inline void Flush() {
        FlushUpsertDV();
        upsert_operations_buffer.clear();
    }

private:
    Snapshot snapshot;
    std::vector<UpsertDVOperation> upsert_operations_buffer;
    std::unordered_map<std::string, std::unordered_map<uint64_t, string_t>> dv_prefetch_cache;

    void FlushUpsertDV();
    void FlushDeleteDV(const UpsertDVOperation &op, ::Relation table, ::Relation index, Snapshot snapshot);
    void FlushInsertDV(UpsertDVOperation &op, ::Relation table);
};

} // namespace duckdb
