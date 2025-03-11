#include "columnstore/columnstore_storage.hpp"
#include "columnstore/columnstore_metadata.hpp"
#include "columnstore/columnstore_table.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include "pgduckdb/logger.hpp"
#include "pgduckdb/pg/transactions.hpp"

namespace duckdb {

class TransactionGuard {
public:
    TransactionGuard() {
        auto &context = *pgduckdb::DuckDBManager::GetConnectionUnsafe()->context;
        if (!context.transaction.HasActiveTransaction()) {
            context.transaction.BeginTransaction();
            require_new_transaction = true;
        }
    }
    ~TransactionGuard() {
        if (require_new_transaction) {
            auto &context = *pgduckdb::DuckDBManager::GetConnectionUnsafe()->context;
            context.transaction.Commit();
        }
    }

private:
    bool require_new_transaction = false;
};

class FileDeleteTask : public BaseExecutorTask {
public:
    FileDeleteTask(TaskExecutor &executor, FileSystem &fs, const string &path, atomic<bool> &success)
        : BaseExecutorTask(executor), fs(fs), path(path), success(success) {}

    void ExecuteTask() override {
        try {
            pd_log(DEBUG1, "Deleting file: %s", path.c_str());
            fs.RemoveFile(path);
        } catch (IOException &e) {
            std::string_view errmsg(e.what());
            // Ignore ENOENT exception
            if (errmsg.find("No such file or directory") == string::npos) {
                pd_log(WARNING, "Failed to delete file: %s", e.what());
                success = false;
            }
        } catch (std::exception &e) {
            pd_log(WARNING, "Failed to delete file: %s", e.what());
            success = false;
        }
    }

private:
    FileSystem &fs;
    const string &path;
    atomic<bool> &success;
};

bool ColumnstoreStorage::DeleteFiles(const vector<string> &file_paths) {
    TransactionGuard transaction_guard;
    auto &context = *pgduckdb::DuckDBManager::GetConnectionUnsafe()->context;
    auto &fs = FileSystem::GetFileSystem(context);
    // Use DuckDB's task system for parallelization
    TaskExecutor executor(context);
    std::atomic<bool> success{true};

    for (const auto &path : file_paths) {
        auto task = make_uniq<FileDeleteTask>(executor, fs, path, success);
        executor.ScheduleTask(std::move(task));
    }
    executor.WorkOnTasks();

    return success;
}

bool ColumnstoreStorage::DeleteDirectory(const std::string &directory) {
    TransactionGuard transaction_guard;
    auto &context = *pgduckdb::DuckDBManager::GetConnectionUnsafe()->context;
    auto &fs = FileSystem::GetFileSystem(context);

    try {
        pd_log(DEBUG1, "Deleting directory: %s", directory.c_str());
        fs.RemoveDirectory(directory);
        return true;
    } catch (std::exception &e) {
        pd_log(WARNING, "Failed to delete directory: %s", e.what());
        return false;
    }
}

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
            if (!ColumnstoreStorage::DeleteFiles(file_path) ||
                !ColumnstoreStorage::DeleteDirectory(pending.table_path)) {
                return false;
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
