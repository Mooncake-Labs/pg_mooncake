#include "columnstore/columnstore.hpp"
#include "columnstore_metadata.hpp"
#include "duckdb/common/file_system.hpp"
#include "lake/lake.hpp"
namespace duckdb {

void Columnstore::CreateTable(ClientContext &context, Oid oid, const string &path) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    string full_path = metadata.GenerateFullPath(oid, path, duckdb::FileSystem::IsRemoteFile(path));
    if (!full_path.empty() && !duckdb::FileSystem::IsRemoteFile(full_path)) {
        FileSystem::GetFileSystem(context).CreateDirectory(full_path);
    }
    metadata.TablesInsert(oid, full_path);
    LakeCreateTable(oid, full_path.c_str());
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

string Columnstore::GetTableInfo(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    return metadata.TablesSearch(oid);
}

string Columnstore::GetSecretForPath(const string &path) {
    if (!duckdb::FileSystem::IsRemoteFile(path)) {
        return "{}";
    }
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    return metadata.SecretGet();
}
} // namespace duckdb
