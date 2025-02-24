#include "columnstore/columnstore_deletion_vector.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

DeletionVector::DeletionVector() : count(STANDARD_VECTOR_SIZE), validity_mask(STANDARD_VECTOR_SIZE) {
    validity_mask.Initialize(count);
}

void DeletionVector::MarkDeleted(const idx_t offset) {
    validity_mask.SetInvalid(offset);
}

bool DeletionVector::IsDeleted(idx_t row_id) const {
    if (row_id >= count) {
        return false;
    }
    return !validity_mask.RowIsValid(row_id);
}

void DeletionVector::ApplyToChunk(DataChunk &chunk) const {
    idx_t chunk_size = chunk.size();
    if (validity_mask.AllValid() || validity_mask.CheckAllValid(chunk_size)) {
        chunk.SetCardinality(0);
        return;
    }
    if (validity_mask.CountValid(chunk_size) == 0) {
        return;
    }
    auto valid_count = validity_mask.CountValid(chunk_size);
    auto invalid_count = chunk_size - valid_count;
    SelectionVector sel(invalid_count);
    idx_t sel_idx = 0;
    for (idx_t i = 0; i < chunk_size; i++) {
        if (!validity_mask.RowIsValid(i)) {
            sel.set_index(sel_idx++, i);
        }
    }
    chunk.Slice(sel, invalid_count);
}

void DeletionVector::Serialize(MemoryStream &write_stream, DeletionVector &deletion_vector) {
    deletion_vector.validity_mask.Write(write_stream, deletion_vector.Size());
}

DeletionVector DeletionVector::Deserialize(const string_t &data) {
    MemoryStream read_stream((data_ptr_t)data.GetData(), (idx_t)data.GetSize());
    DeletionVector dv;
    dv.validity_mask.Read(read_stream, dv.Size());
    return dv;
}

} // namespace duckdb
