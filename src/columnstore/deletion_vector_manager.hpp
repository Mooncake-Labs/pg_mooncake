#pragma once

#include "columnstore_deletion_vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "pgduckdb/pg/declarations.hpp"

namespace duckdb {

constexpr int x_deletion_vectors_natts = 3;
constexpr int x_op_batch_size = duckdb::Storage::ROW_GROUP_SIZE;

struct UpsertDVOperation {
    string file_name;
    uint64_t chunk_idx;
    DeletionVector deletion_vector;
};

class DVManager {
public:
    explicit DVManager(Snapshot snapshot) : snapshot(snapshot) {}

    void UpsertDV(const std::string &file_name, uint64_t chunk_idx, const DeletionVector &deletion_vector);

    DeletionVector FetchDV(const std::string &file_name, uint64_t chunk_idx);

    void Flush() {
        FlushUpsertDV();
        upsert_operations_buffer.clear();
    };

private:
    Snapshot snapshot;
    std::vector<UpsertDVOperation> upsert_operations_buffer;

    void FlushUpsertDV();
    void DeleteDV(const UpsertDVOperation &op, ::Relation table, ::Relation index, Snapshot snapshot);
    void InsertDV(const UpsertDVOperation &op, ::Relation table);
};

} // namespace duckdb
