#pragma once

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
class DeletionVector {
public:
    DeletionVector() : bitmap(STANDARD_VECTOR_SIZE, false) {}

    void MarkDeleted(const idx_t offset);
    bool IsDeleted(size_t row_id) const;

    size_t size() const {
        return bitmap.size();
    }

    static std::string Serialize(const DeletionVector &deletion_vector);
    static DeletionVector Deserialize(const std::string &serialized);

private:
    std::vector<bool> bitmap;
};

} // namespace duckdb
