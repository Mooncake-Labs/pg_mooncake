#include "columnstore/columnstore.hpp"
#include "columnstore_metadata.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

void Columnstore::CreateTable(ClientContext &context, Oid oid, const string &path) {
    if (!path.empty()) {
        FileSystem::GetFileSystem(context).CreateDirectory(path);
    }
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    metadata.TablesInsert(oid, path);
}

// TODO
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
