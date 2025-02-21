#pragma once

#include "duckdb/common/list.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "pgduckdb/pg/declarations.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

namespace duckdb {

struct PendingRelDelete {
public:
    Oid oid;
    int nest_level;
    bool at_commit;
};

class ColumnstoreStorageContextState : public duckdb::ClientContextState {

public:
    static ColumnstoreStorageContextState &Get() {
        auto &context = *pgduckdb::DuckDBManager::GetConnectionUnsafe()->context;
        return Get(context);
    }

    static ColumnstoreStorageContextState &Get(duckdb::ClientContext &context) {
        return *context.registered_state->GetOrCreate<ColumnstoreStorageContextState>("pgmooncake_storage");
    }

    void QueryEnd() override;

    void RelationDropStorage(Oid relid);

    void DoPendingDeletes(bool isCommit);

    void AtSubCommit();

    void AtSubAbort();
private:
    // Physically delete the storage of a relation.
    // Return true if the storage was successfully deleted, false otherwise.
    bool DropStorage(Oid relid);

    duckdb::list<PendingRelDelete> pending_deletes;
};

} // namespace duckdb