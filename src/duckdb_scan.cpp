#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

#include "columnstore/columnstore.hpp"

struct DuckdbScanState {
    CustomScanState css; /* must be first field */
    duckdb::Connection *duckdb_connection;
    duckdb::PreparedStatement *prepared_statement;
    bool is_executed;
    bool fetch_next;
    duckdb::unique_ptr<duckdb::QueryResult> query_results;
    duckdb::idx_t column_count;
    duckdb::unique_ptr<duckdb::DataChunk> current_data_chunk;
    duckdb::idx_t current_row;
};

void CleanupDuckdbScanState(DuckdbScanState *state) {
    state->query_results.reset();
    delete state->prepared_statement;
    delete state->duckdb_connection;
}

void BeginDuckdbScan(CustomScanState *cscanstate, EState *estate, int eflags) {
    DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)cscanstate;
    duckdb_scan_state->css.ss.ps.ps_ResultTupleDesc = duckdb_scan_state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    HOLD_CANCEL_INTERRUPTS();
}

void ExecuteQuery(DuckdbScanState *state) {
    elog(NOTICE, "DuckDB");

    auto &prepared = *state->prepared_statement;
    auto &query_results = state->query_results;
    auto &connection = state->duckdb_connection;

    auto pending = prepared.PendingQuery();
    duckdb::PendingExecutionResult execution_result;
    do {
        execution_result = pending->ExecuteTask();
        if (QueryCancelPending) {
            // Send an interrupt
            connection->Interrupt();
            auto &executor = duckdb::Executor::Get(*connection->context);
            // Wait for all tasks to terminate
            executor.CancelTasks();

            // Delete the scan state
            CleanupDuckdbScanState(state);
            // Process the interrupt on the Postgres side
            ProcessInterrupts();
            elog(ERROR, "Query cancelled");
        }
    } while (!duckdb::PendingQueryResult::IsResultReady(execution_result));
    if (execution_result == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
        elog(ERROR, "Duckdb execute returned an error: %s", pending->GetError().c_str());
    }
    query_results = pending->Execute();
    state->column_count = query_results->ColumnCount();
    state->is_executed = true;
}

TupleTableSlot *ExecDuckdbScan(CustomScanState *node) {
    DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
    TupleTableSlot *slot = duckdb_scan_state->css.ss.ss_ScanTupleSlot;
    MemoryContext old_context;

    if (!duckdb_scan_state->is_executed) {
        ExecuteQuery(duckdb_scan_state);
    }

    if (duckdb_scan_state->fetch_next) {
        duckdb_scan_state->current_data_chunk = duckdb_scan_state->query_results->Fetch();
        duckdb_scan_state->current_row = 0;
        duckdb_scan_state->fetch_next = false;
        if (!duckdb_scan_state->current_data_chunk || duckdb_scan_state->current_data_chunk->size() == 0) {
            MemoryContextReset(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
            ExecClearTuple(slot);
            return slot;
        }
    }

    MemoryContextReset(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
    ExecClearTuple(slot);

    /* MemoryContext used for allocation */
    old_context = MemoryContextSwitchTo(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

    for (idx_t col = 0; col < duckdb_scan_state->column_count; col++) {
        // FIXME: we should not use the Value API here, it's complicating the LIST conversion logic
        auto value = duckdb_scan_state->current_data_chunk->GetValue(col, duckdb_scan_state->current_row);
        if (value.IsNull()) {
            slot->tts_isnull[col] = true;
        } else {
            slot->tts_isnull[col] = false;
            pgduckdb::ConvertDuckToPostgresValue(slot, value, col);
        }
    }

    MemoryContextSwitchTo(old_context);

    duckdb_scan_state->current_row++;
    if (duckdb_scan_state->current_row >= duckdb_scan_state->current_data_chunk->size()) {
        delete duckdb_scan_state->current_data_chunk.release();
        duckdb_scan_state->fetch_next = true;
    }

    ExecStoreVirtualTuple(slot);
    return slot;
}

void EndDuckdbScan(CustomScanState *node) {
    DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
    CleanupDuckdbScanState(duckdb_scan_state);
    RESUME_CANCEL_INTERRUPTS();
}

void ReScanDuckdbScan(CustomScanState *node) {}

void ExplainDuckdbScan(CustomScanState *node, List *ancestors, ExplainState *es) {
    DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
    auto res = duckdb_scan_state->prepared_statement->Execute();
    duckdb::string explain_output = "\n\n";
    auto chunk = res->Fetch();
    if (!chunk || chunk->size() == 0) {
        return;
    }
    /* Is it safe to hardcode this as result of DuckDB explain? */
    auto value = chunk->GetValue(1, 0);
    explain_output += value.GetValue<duckdb::string>();
    explain_output += "\n";
    ExplainPropertyText("DuckDB Execution Plan", explain_output.c_str(), es);
}

CustomExecMethods duckdb_exec_methods = {"DuckDB",
                                         BeginDuckdbScan,
                                         ExecDuckdbScan,
                                         EndDuckdbScan,
                                         ReScanDuckdbScan,
                                         NULL /*MarkPosCustomScan*/,
                                         NULL /*RestrPosCustomScan*/,
                                         NULL /*EstimateDSMCustomScan*/,
                                         NULL /*InitializeDSMCustomScan*/,
                                         NULL /*ReInitializeDSMCustomScan*/,
                                         NULL /*InitializeWorkerCustomScan*/,
                                         NULL /*ShutdownCustomScan*/,
                                         ExplainDuckdbScan};

Node *CreateDuckdbScanState(CustomScan *cscan) {
    DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)newNode(sizeof(DuckdbScanState), T_CustomScanState);
    CustomScanState *custom_scan_state = &duckdb_scan_state->css;
    duckdb_scan_state->duckdb_connection = (duckdb::Connection *)linitial(cscan->custom_private);
    duckdb_scan_state->prepared_statement = (duckdb::PreparedStatement *)lsecond(cscan->custom_private);
    duckdb_scan_state->is_executed = false;
    duckdb_scan_state->fetch_next = true;
    custom_scan_state->methods = &duckdb_exec_methods;
    return (Node *)custom_scan_state;
}

CustomScanMethods duckdb_scan_methods = {"DuckDB", CreateDuckdbScanState};

PlannerInfo *PlanQuery(Query *parse, ParamListInfo bound_params) {
    PlannerGlobal *glob = makeNode(PlannerGlobal);

    glob->boundParams = bound_params;
    glob->subplans = NIL;
    glob->subroots = NIL;
    glob->rewindPlanIDs = NULL;
    glob->finalrtable = NIL;
    glob->finalrteperminfos = NIL;
    glob->finalrowmarks = NIL;
    glob->resultRelations = NIL;
    glob->appendRelations = NIL;
    glob->relationOids = NIL;
    glob->invalItems = NIL;
    glob->paramExecTypes = NIL;
    glob->lastPHId = 0;
    glob->lastRowMarkId = 0;
    glob->lastPlanNodeId = 0;
    glob->transientPlan = false;
    glob->dependsOnRole = false;

    return subquery_planner(glob, parse, NULL, false, 0.0);
}

Plan *CreatePlan(Query *query, const char *query_string, ParamListInfo bound_params) {
    List *rtables = query->rtable;

    /* Extract required vars for table */
    int flags = PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS;
    List *vars = list_concat(pull_var_clause((Node *)query->targetList, flags),
                             pull_var_clause((Node *)query->jointree->quals, flags));

    PlannerInfo *query_planner_info = PlanQuery(query, bound_params);
    auto duckdb_connection = pgduckdb::DuckdbCreateConnection(rtables, query_planner_info, vars, query_string);
    auto context = duckdb_connection->context;

    auto prepared_query = context->Prepare(query_string);

    if (prepared_query->HasError()) {
        elog(WARNING, "(DuckDB) %s", prepared_query->GetError().c_str());
        return nullptr;
    }

    CustomScan *duckdb_node = makeNode(CustomScan);

    auto &prepared_result_types = prepared_query->GetTypes();

    for (auto i = 0; i < prepared_result_types.size(); i++) {
        auto &column = prepared_result_types[i];
        Oid postgresColumnOid = pgduckdb::GetPostgresDuckDBType(column);

        HeapTuple tp;
        Form_pg_type typtup;

        tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(postgresColumnOid));
        if (!HeapTupleIsValid(tp))
            elog(ERROR, "cache lookup failed for type %u", postgresColumnOid);

        typtup = (Form_pg_type)GETSTRUCT(tp);

        Var *var = makeVar(INDEX_VAR, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

        duckdb_node->custom_scan_tlist =
            lappend(duckdb_node->custom_scan_tlist,
                    makeTargetEntry((Expr *)var, i + 1, (char *)prepared_query->GetNames()[i].c_str(), false));

        ReleaseSysCache(tp);
    }

    duckdb_node->custom_private = list_make2(duckdb_connection.release(), prepared_query.release());
    duckdb_node->methods = &duckdb_scan_methods;

    return (Plan *)duckdb_node;
}

PlannedStmt *DuckdbPlanner(Query *parse, int cursor_options, ParamListInfo bound_params) {
    const char *query_string = pgduckdb_pg_get_querydef(parse, false);
    /* We need to check can we DuckDB create plan */
    Plan *duckdb_plan = CreatePlan(parse, query_string, bound_params);
    if (!duckdb_plan) {
        return nullptr;
    }

    PlannedStmt *plan = makeNode(PlannedStmt);
    plan->commandType = parse->commandType;
    plan->queryId = parse->queryId;
    plan->hasReturning = (parse->returningList != NIL);
    plan->hasModifyingCTE = parse->hasModifyingCTE;
    plan->canSetTag = parse->canSetTag;
    plan->planTree = reinterpret_cast<Plan *>(duckdb_plan);
    plan->utilityStmt = parse->utilityStmt;
    plan->stmt_location = parse->stmt_location;
    plan->stmt_len = parse->stmt_len;
    return plan;
}

bool HasColumnstore(List *tables) {
    foreach_node(RangeTblEntry, table, tables) {
        if (table->rtekind == RTE_SUBQUERY) {
            if (HasColumnstore(table->subquery->rtable)) {
                return true;
            }
        }
        if (table->relid && IsColumnstore(table->relid)) {
            return true;
        }
    }
    return false;
}

bool HasCatalog(List *tables) {
    foreach_node(RangeTblEntry, table, tables) {
        if (table->rtekind == RTE_SUBQUERY) {
            if (HasCatalog(table->subquery->rtable)) {
                return true;
            }
        }
        if (table->relid) {
            Relation rel = RelationIdGetRelation(table->relid);
            Oid namespace_oid = RelationGetNamespace(rel);
            if (namespace_oid == PG_CATALOG_NAMESPACE || namespace_oid == PG_TOAST_NAMESPACE) {
                RelationClose(rel);
                return true;
            }
            RelationClose(rel);
        }
    }
    return false;
}

planner_hook_type prev_planner_hook = NULL;

PlannedStmt *PlannerHook(Query *parse, const char *query_string, int cursor_options, ParamListInfo bound_params) {
    if (pgduckdb::IsExtensionRegistered() && parse->commandType == CMD_SELECT && HasColumnstore(parse->rtable)) {
        if (parse->hasModifyingCTE) {
            elog(ERROR, "DuckDB does not support modifying CTEs INSERT/UPDATE/DELETE");
        }
        if (HasCatalog(parse->rtable)) {
            elog(ERROR, "DuckDB has its own pg_catalog tables that contain different data");
        }
        PlannedStmt *plan = DuckdbPlanner(parse, cursor_options, bound_params);
        if (!plan) {
            elog(ERROR, "DuckDB does not support this query");
        }
        return plan;
    }
    return prev_planner_hook(parse, query_string, cursor_options, bound_params);
}

void InitDuckdbScan() {
    prev_planner_hook = planner_hook ? planner_hook : standard_planner;
    planner_hook = PlannerHook;
    RegisterCustomScanMethods(&duckdb_scan_methods);
}
