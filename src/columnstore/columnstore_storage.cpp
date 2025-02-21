#include "columnstore/columnstore_storage.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "columnstore/columnstore_table.hpp"

#include "duckdb/common/file_system.hpp"

#include "pgduckdb/logger.hpp"
#include "pgduckdb/pg/transactions.hpp"

namespace duckdb {

void ColumnstoreStorageContextState::QueryEnd() {
    pending_deletes.clear();
}

void ColumnstoreStorageContextState::RelationDropStorage(Oid relid) {
    PendingRelDelete pending = {
        .oid = relid, .nest_level = pgduckdb::pg::GetCurrentTransactionNestLevel(), .at_commit = true};

    ColumnstoreMetadata metadata(NULL /*snapshot*/);
    pending.table_path = metadata.GetTablePath(relid);
    pending.file_names = metadata.DeadDataFilesSearch(relid);
    pending_deletes.emplace_back(pending);
}

void ColumnstoreStorageContextState::DoPendingDeletes(bool isCommit) {
    auto nest_level = pgduckdb::pg::GetCurrentTransactionNestLevel();

    pending_deletes.remove_if([this, nest_level, isCommit](auto &pending) {
        if (pending.nest_level < nest_level)
            return false;

        if (pending.at_commit == isCommit) {
            pd_log(DEBUG1, "start deleting files of table: %d", pending.oid);
            auto file_path = ColumnstoreTable::GetFilePaths(pending.table_path, pending.file_names, false);
            auto &context = *pgduckdb::DuckDBManager::GetConnectionUnsafe()->context;
            bool require_new_transaction = !context.transaction.HasActiveTransaction();
            if (require_new_transaction) {
                context.transaction.BeginTransaction();
            }
            auto &fs = FileSystem::GetFileSystem(context);
            try {
                for (auto &path : file_path) {
                    // TODO parallelize
                    // TODO httpfs s3fs does not implement RemoveFile interface
                    pd_log(DEBUG1, "delete file: %s", path);
                    fs.RemoveFile(path);
                }
            } catch (const Exception &e) {
                pd_log(WARNING, "Failed to delete dead file for relation %d, errmsg: %s", pending.oid, e.what());
            }

            if (require_new_transaction) {
                context.transaction.Commit();
            }
        }
        return true;
    });
}

void ColumnstoreStorageContextState::AtSubCommit() {
    auto nest_level = pgduckdb::pg::GetCurrentTransactionNestLevel();
    for (auto &pending : pending_deletes) {
        if (pending.nest_level >= nest_level) {
            pending.nest_level = nest_level - 1;
        }
    }
}

void ColumnstoreStorageContextState::AtSubAbort() {
    DoPendingDeletes(false);
}

} // namespace duckdb
