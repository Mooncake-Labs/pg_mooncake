#include "columnstore_deletion_vector.hpp"

namespace duckdb {

void DeletionVector::MarkDeleted(const idx_t offset) {
    bitmap[offset] = true;
}

bool DeletionVector::IsDeleted(size_t row_id) const {
    return row_id < bitmap.size() && bitmap[row_id];
}

std::string DeletionVector::Serialize(const DeletionVector &deletion_vector) {
    std::string result(deletion_vector.bitmap.size(), '0');
    for (size_t i = 0; i < deletion_vector.bitmap.size(); ++i) {
        if (deletion_vector.bitmap[i]) {
            result[i] = '1';
        }
    }
    return result;
}

DeletionVector DeletionVector::Deserialize(const std::string &serialized) {
    DeletionVector deletion_vector;
    for (size_t i = 0; i < serialized.size(); ++i) {
        if (serialized[i] == '1') {
            deletion_vector.MarkDeleted(i);
        }
    }
    return deletion_vector;
}

} // namespace duckdb
