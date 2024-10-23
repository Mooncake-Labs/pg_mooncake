#include "columnstore/columnstore.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "tcop/utility.h"
}

using duckdb::string;
#include "lake/lake.hpp"

bool IsColumnstore(Oid oid);

bool DuckdbCopy(PlannedStmt *pstmt, const char *query_string, struct QueryEnvironment *query_env, uint64 *processed);

ProcessUtility_hook_type prev_process_utility_hook = NULL;

void ProcessUtilityHook(PlannedStmt *pstmt, const char *query_string, bool read_only_tree,
                        ProcessUtilityContext context, ParamListInfo params, QueryEnvironment *query_env,
                        DestReceiver *dest, QueryCompletion *qc) {
    if (IsA(pstmt->utilityStmt, CreateStmt)) {
        CreateStmt *stmt = castNode(CreateStmt, pstmt->utilityStmt);
        if (stmt->accessMethod && strcmp(stmt->accessMethod, "columnstore") == 0) {
            prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
            Oid oid = RangeVarGetRelid(stmt->relation, AccessShareLock, false /*missing_ok*/);
            duckdb::Connection con(pgduckdb::DuckDBManager::Get().GetDatabase());
            duckdb::Columnstore::CreateTable(*con.context, oid);
            return;
        }
    } else if (IsA(pstmt->utilityStmt, CopyStmt)) {
        CopyStmt *stmt = castNode(CopyStmt, pstmt->utilityStmt);
        if (IsColumnstore(RangeVarGetRelid(stmt->relation, AccessShareLock, false /*missing_ok*/))) {
            if (!stmt->filename) {
                elog(ERROR, "DuckDB does not support this query");
            }
            uint64 processed;
            if (DuckdbCopy(pstmt, query_string, query_env, &processed)) {
                if (qc) {
                    SetQueryCompletion(qc, CMDTAG_COPY, processed);
                }
                return;
            } else {
                elog(ERROR, "DuckDB does not support this query");
            }
        }
    }
    prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
}

void XactHook(XactEvent event, void *arg) {
    if (event == XactEvent::XACT_EVENT_COMMIT) {
        LakeCommit();
    }
}

void InitDuckdbHooks() {
    prev_process_utility_hook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
    ProcessUtility_hook = ProcessUtilityHook;
    RegisterXactCallback(XactHook, NULL);

    duckdb::Connection con(pgduckdb::DuckDBManager::Get().GetDatabase());
    if (!duckdb::FileSystem::GetFileSystem(*con.context).DirectoryExists("mooncake_cache")) {
        duckdb::FileSystem::GetFileSystem(*con.context).CreateDirectory("mooncake_cache");
    }
    if (!duckdb::FileSystem::GetFileSystem(*con.context).DirectoryExists("mooncake_local_tables")) {
        duckdb::FileSystem::GetFileSystem(*con.context).CreateDirectory("mooncake_local_tables");
    }
}
