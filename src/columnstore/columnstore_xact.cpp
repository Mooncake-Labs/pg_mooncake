#include "pgduckdb/pg/transactions.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

#include "columnstore/columnstore.hpp"
#include "columnstore/columnstore_xact.hpp"
#include "columnstore/columnstore_storage.hpp"

namespace duckdb {

void MooncakeXactCallback(XactEvent event) {
    switch (event) {
    case XACT_EVENT_COMMIT:
        ColumnstoreStorageContextState::Get().DoPendingDeletes(true);
        break;
    default:
        break;
    }
}

void MooncakeSubXactCallback(SubXactEvent event) {
    switch (event) {
    case SUBXACT_EVENT_COMMIT_SUB:
        ColumnstoreStorageContextState::Get().AtSubCommit();
        break;
    case SUBXACT_EVENT_ABORT_SUB:
        ColumnstoreStorageContextState::Get().AtSubAbort();
        break;
    default:
        break;
    }
}

} // namespace duckdb