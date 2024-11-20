#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "lake/lake.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

namespace duckdb {

void Columnstore::CreateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    string path = metadata.GetTablePath(oid);
    if (!path.empty() && !duckdb::FileSystem::IsRemoteFile(path)) {
        FileSystem::CreateLocal()->CreateDirectory(path);
    }
    metadata.TablesInsert(oid, path);
    pgduckdb::DuckDBFunctionGuard<void>(LakeCreateTable, "LakeCreateTable", oid, path);
}

void Columnstore::TruncateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    vector<string> file_names = metadata.DataFilesSearch(oid);
    metadata.DataFilesDelete(oid);
    for (auto file_name : file_names) {
        LakeDeleteFile(oid, file_name);
    }
}

void Columnstore::Abort() {
    LakeAbort();
}

void Columnstore::Commit() {
    pgduckdb::DuckDBFunctionGuard<void>(LakeCommit, "LakeCommit");
}

void Columnstore::LoadSecrets(ClientContext &context) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    context.transaction.BeginTransaction();
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    auto secrets = SecretManager::Get(context).AllSecrets(transaction);
    for (auto secret : secrets) {
        SecretManager::Get(context).DropSecretByName(context, secret.secret->GetName(),
                                                     duckdb::OnEntryNotFound::RETURN_NULL);
    }
    context.transaction.Commit();
    auto queries = metadata.SecretsGetDuckdbQueries();
    for (const auto &query : queries) {
        pgduckdb::DuckDBQueryOrThrow(context, query);
    }
}

} // namespace duckdb
