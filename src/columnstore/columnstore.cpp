#include "columnstore_handler.hpp"
#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "lake/lake.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"
#include "pgduckdb/catalog/pgduckdb_catalog.hpp"
#include "pgduckdb/catalog/pgduckdb_transaction.hpp"

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

ColumnstoreTable &Columnstore::GetTable(ClientContext &context, Oid oid) {
    if (!IsColumnstoreTable(oid)) {
        throw duckdb::InvalidInputException("(Columnstore/GetTable) Table oid " + std::to_string(oid) + " is not a columnstore table");
    }

    duckdb::DatabaseManager &db_manager = duckdb::DatabaseManager::Get(*context.db);
    duckdb::optional_ptr<duckdb::AttachedDatabase> pgmooncake_db = db_manager.GetDatabase(context, "pgmooncake");
    pgduckdb::PostgresCatalog &catalog = pgmooncake_db->GetCatalog().Cast<pgduckdb::PostgresCatalog>();
    pgduckdb::PostgresTransaction &transaction = duckdb::Transaction::Get(context, catalog).Cast<pgduckdb::PostgresTransaction>();;

    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    std::tuple<string /*table_name*/, string /*schema_name*/> table_name_and_schema_name = metadata.GetTableNameAndSchemaName(oid);
    const string &schema_name = std::get<1>(table_name_and_schema_name);
    const string &table_name = std::get<0>(table_name_and_schema_name);

    // load in schema entry first so that we can retrieve the table entry
    duckdb::optional_ptr<duckdb::CatalogEntry> schema = transaction.GetCatalogEntry(duckdb::CatalogType::SCHEMA_ENTRY, schema_name, "");
    D_ASSERT(schema);
    duckdb::optional_ptr<duckdb::CatalogEntry> table = transaction.GetCatalogEntry(duckdb::CatalogType::TABLE_ENTRY, schema_name, table_name);
    D_ASSERT(table);

    return table->Cast<duckdb::ColumnstoreTable>();
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
