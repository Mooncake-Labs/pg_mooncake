#include "columnstore/columnstore.hpp"
#include "columnstore_metadata.hpp"

namespace duckdb {

void Columnstore::CreateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    metadata.TablesInsert(oid, "" /*path*/);
}

void Columnstore::DropTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    metadata.DataFilesDelete(oid);
    metadata.TablesDelete(oid);
}

void Columnstore::TruncateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    metadata.DataFilesDelete(oid);
}

} // namespace duckdb
