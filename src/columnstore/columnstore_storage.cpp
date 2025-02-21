#include "columnstore/columnstore_storage.hpp"

#include "pgduckdb/logger.hpp"
#include "pgduckdb/pg/transactions.hpp"

namespace duckdb {

void ColumnstoreStorageContextState::QueryEnd() {
    pending_deletes.clear();
}

void ColumnstoreStorageContextState::RelationDropStorage(Oid relid) {
    pending_deletes.push_back({relid, pgduckdb::pg::GetCurrentTransactionNestLevel(), true});
}

void ColumnstoreStorageContextState::DoPendingDeletes(bool isCommit) {
    auto nest_level = pgduckdb::pg::GetCurrentTransactionNestLevel();

    pending_deletes.remove_if([this, nest_level, isCommit](auto &pending) {
        if (pending.nest_level < nest_level)
            return false;

        if (pending.at_commit == isCommit) {
            this->DropStorage(pending.oid);
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

bool ColumnstoreStorageContextState::DropStorage(Oid relid) {
    pd_log(INFO, "physical delete: %d", relid);
    return true;
}

} // namespace duckdb
