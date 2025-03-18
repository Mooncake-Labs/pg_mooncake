#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "columnstore/columnstore_storage.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "lake/lake.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/dependency.h"
}

namespace duckdb {

void Columnstore::CreateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    string path = metadata.GetTablePath(oid);
    if (!path.empty() && !duckdb::FileSystem::IsRemoteFile(path)) {
        FileSystem::CreateLocal()->CreateDirectory(path);
    }
    metadata.TablesInsert(oid, path);
    InvokeCPPFunc(LakeCreateTable, oid, path);
}

void Columnstore::TruncateTable(Oid oid) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    vector<string> file_names = metadata.DataFilesSearch(oid);
    metadata.DataFilesDelete(oid);
    for (auto file_name : file_names) {
        LakeDeleteFile(oid, file_name);
    }
}

void Columnstore::DropTable(Oid oid, int flags) {
    // Mark the table's data files as dead. Physical deletion of the data files occurs either after the transaction
    // commits or during the VACUUM process.
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    metadata.DataFilesDelete(oid);

    // Skip storage deletion if the DROP is triggered internally, e.g., drop a temp table.
    if (!(flags & PERFORM_DELETION_INTERNAL)) {
        ColumnstoreStorageContextState::Get().RelationDropStorage(oid);
    }
}

void Columnstore::Abort() {
    LakeAbort();
}

void Columnstore::Commit() {
    InvokeCPPFunc(LakeCommit);
}

void Columnstore::LoadSecrets(ClientContext &context) {
    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    bool require_new_transaction = !context.transaction.HasActiveTransaction();
    if (require_new_transaction) {
        context.transaction.BeginTransaction();
    }
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    auto secrets = SecretManager::Get(context).AllSecrets(transaction);
    for (auto secret : secrets) {
        SecretManager::Get(context).DropSecretByName(context, secret.secret->GetName(),
                                                     duckdb::OnEntryNotFound::RETURN_NULL);
    }
    if (require_new_transaction) {
        context.transaction.Commit();
    }
    auto queries = metadata.SecretsGetDuckdbQueries();
    for (const auto &query : queries) {
        pgduckdb::DuckDBQueryOrThrow(context, query);
    }
}

} // namespace duckdb
