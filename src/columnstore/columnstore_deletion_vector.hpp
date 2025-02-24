#pragma once

#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
class DeletionVector {
public:
    DeletionVector();

    void MarkDeleted(idx_t offset);
    bool IsDeleted(idx_t row_id) const;
    void ApplyToChunk(DataChunk &chunk) const;

    void Combine(const DeletionVector &other, idx_t count) {
        validity_mask.Combine(other.validity_mask, count);
    }

    size_t Size() const {
        return count;
    }

    static void Serialize(MemoryStream &write_stream, DeletionVector &deletion_vector);
    static DeletionVector Deserialize(const string_t &data);

private:
    idx_t count;
    ValidityMask validity_mask;
};

} // namespace duckdb
