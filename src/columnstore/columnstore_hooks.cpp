#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "tcop/utility.h"
}

#include "columnstore/columnstore.hpp"
#include "lakehouse_interface/lakehouse_interface.hpp"

ColumnstoreOptions ParseColumnstoreOptions(List *list) {
    ColumnstoreOptions options;
    ListCell *cell;
    foreach (cell, list) {
        DefElem *elem = castNode(DefElem, lfirst(cell));
        if (strcmp(elem->defname, "path") == 0) {
            options.path = defGetString(elem);
            if (options.path[strlen(options.path) - 1] != '/') {
                options.path = psprintf("%s/", options.path);
            }
        } else {
            elog(ERROR, "Unrecognized columnstore option \"%s\"", elem->defname);
        }
    }
    return options;
}

ProcessUtility_hook_type prev_process_utility_hook = NULL;

void ProcessUtilityHook(PlannedStmt *pstmt, const char *query_string, bool read_only_tree,
                        ProcessUtilityContext context, ParamListInfo params, QueryEnvironment *query_env,
                        DestReceiver *dest, QueryCompletion *qc) {
    if (IsA(pstmt->utilityStmt, CreateStmt)) {
        CreateStmt *stmt = castNode(CreateStmt, pstmt->utilityStmt);
        if (stmt->accessMethod && strcmp(stmt->accessMethod, "columnstore") == 0) {
            ColumnstoreOptions options = ParseColumnstoreOptions(stmt->options);
            stmt->options = NIL;
            prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
            Oid oid = RangeVarGetRelid(stmt->relation, AccessShareLock, false /*missing_ok*/);
            ColumnstoreCreateTable(oid, options);
            LakeHouseCreateTable(oid, options.path, "delta");
            return;
        }
    }
    prev_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
    if (IsA(pstmt->utilityStmt, CopyStmt)) {
        ColumnstoreFinalize();
    }
}

ExecutorEnd_hook_type prev_executor_end_hook = NULL;

void ExecutorEndHook(QueryDesc *query_desc) {
    prev_executor_end_hook(query_desc);
    ColumnstoreFinalize();
}

void XactCallbackHook(XactEvent event, void *arg) {
    if (event == XactEvent::XACT_EVENT_COMMIT) {
        LakeHouseCommitXact();
    }
}

void InitColumnstore() {
    prev_process_utility_hook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
    ProcessUtility_hook = ProcessUtilityHook;
    prev_executor_end_hook = ExecutorEnd_hook ? ExecutorEnd_hook : standard_ExecutorEnd;
    ExecutorEnd_hook = ExecutorEndHook;
    RegisterXactCallback(XactCallbackHook, NULL);
}
