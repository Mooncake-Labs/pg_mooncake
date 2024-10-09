#include "duckdb.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "tcop/pquery.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pgduckdb/vendor/pg_ruleutils.h"
}

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/vendor/pg_explain.hpp"
#include "pgduckdb/vendor/pg_list.hpp"

#include "columnstore/columnstore.hpp"

bool duckdb_explain_analyze = false;

std::tuple<duckdb::unique_ptr<duckdb::PreparedStatement>, duckdb::unique_ptr<duckdb::Connection>>
DuckdbPrepare(const Query *query) {
    Query *copied_query = (Query *)copyObjectImpl(query);
    const char *query_string = pgduckdb_pg_get_querydef(copied_query, false);

    if (ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
        if (duckdb_explain_analyze) {
            query_string = psprintf("EXPLAIN ANALYZE %s", query_string);
        } else {
            query_string = psprintf("EXPLAIN %s", query_string);
        }
    }

    elog(DEBUG2, "(PGDuckDB/DuckdbPrepare) Preparing: %s", query_string);

    auto duckdb_connection = pgduckdb::DuckDBManager::Get().GetConnection();
    auto context = duckdb_connection->context;
    auto prepared_query = context->Prepare(query_string);
    return {std::move(prepared_query), std::move(duckdb_connection)};
}

struct DuckdbScanState {
    CustomScanState css; /* must be first field */
    const Query *query;
    ParamListInfo params;
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
    auto prepare_result = DuckdbPrepare(duckdb_scan_state->query);
    auto prepared_query = std::move(std::get<0>(prepare_result));
    auto duckdb_connection = std::move(std::get<1>(prepare_result));

    if (prepared_query->HasError()) {
        elog(ERROR, "DuckDB re-planning failed %s", prepared_query->GetError().c_str());
    }

    duckdb_scan_state->duckdb_connection = duckdb_connection.release();
    duckdb_scan_state->prepared_statement = prepared_query.release();
    duckdb_scan_state->params = estate->es_param_list_info;
    duckdb_scan_state->is_executed = false;
    duckdb_scan_state->fetch_next = true;
    duckdb_scan_state->css.ss.ps.ps_ResultTupleDesc = duckdb_scan_state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    HOLD_CANCEL_INTERRUPTS();
}

void ExecuteQuery(DuckdbScanState *state) {
    elog(NOTICE, "DuckDB");

    auto &prepared = *state->prepared_statement;
    auto &query_results = state->query_results;
    auto &connection = state->duckdb_connection;
    auto pg_params = state->params;
    const auto num_params = pg_params ? pg_params->numParams : 0;
    duckdb::vector<duckdb::Value> duckdb_params;
    for (int i = 0; i < num_params; i++) {
        ParamExternData *pg_param;
        ParamExternData tmp_workspace;

        /* give hook a chance in case parameter is dynamic */
        if (pg_params->paramFetch != NULL)
            pg_param = pg_params->paramFetch(pg_params, i + 1, false, &tmp_workspace);
        else
            pg_param = &pg_params->params[i];

        if (pg_param->isnull) {
            duckdb_params.push_back(duckdb::Value());
        } else {
            if (!OidIsValid(pg_param->ptype)) {
                elog(ERROR, "parameter with invalid type during execution");
            }
            duckdb_params.push_back(pgduckdb::ConvertPostgresParameterToDuckValue(pg_param->value, pg_param->ptype));
        }
    }

    auto pending = prepared.PendingQuery(duckdb_params, true);
    if (pending->HasError()) {
        elog(ERROR, "DuckDB execute returned an error: %s", pending->GetError().c_str());
    }
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
        CleanupDuckdbScanState(state);
        elog(ERROR, "(PGDuckDB/ExecuteQuery) %s", pending->GetError().c_str());
    }
    query_results = pending->Execute();
    state->column_count = query_results->ColumnCount();
    state->is_executed = true;
}

TupleTableSlot *ExecDuckdbScan(CustomScanState *node) {
    DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
    TupleTableSlot *slot = duckdb_scan_state->css.ss.ss_ScanTupleSlot;
    MemoryContext old_context;

    bool already_executed = duckdb_scan_state->is_executed;
    if (!already_executed) {
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
            if (!pgduckdb::ConvertDuckToPostgresValue(slot, value, col)) {
                CleanupDuckdbScanState(duckdb_scan_state);
                elog(ERROR, "(PGDuckDB/Duckdb_ExecCustomScan) Value conversion failed");
            }
        }
    }

    MemoryContextSwitchTo(old_context);

    duckdb_scan_state->current_row++;
    if (duckdb_scan_state->current_row >= duckdb_scan_state->current_data_chunk->size()) {
        duckdb_scan_state->current_data_chunk.reset();
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
    ExecuteQuery(duckdb_scan_state);
    auto chunk = duckdb_scan_state->query_results->Fetch();
    if (!chunk || chunk->size() == 0) {
        return;
    }
    /* Is it safe to hardcode this as result of DuckDB explain? */
    auto value = chunk->GetValue(1, 0);
    std::string explain_output = "\n\n";
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

    duckdb_scan_state->query = (const Query *)linitial(cscan->custom_private);
    custom_scan_state->methods = &duckdb_exec_methods;
    return (Node *)custom_scan_state;
}

CustomScanMethods duckdb_scan_methods = {"DuckDB", CreateDuckdbScanState};

Plan *CreatePlan(Query *query) {
    /*
     * Prepare the query, se we can get the returned types and column names.
     */
    auto prepare_result = DuckdbPrepare(query);
    auto prepared_query = std::move(std::get<0>(prepare_result));

    if (prepared_query->HasError()) {
        elog(WARNING, "(PGDuckDB/CreatePlan) Prepared query returned an error: '%s",
             prepared_query->GetError().c_str());
        return nullptr;
    }

    CustomScan *duckdb_node = makeNode(CustomScan);

    auto &prepared_result_types = prepared_query->GetTypes();

    for (auto i = 0; i < prepared_result_types.size(); i++) {
        auto &column = prepared_result_types[i];
        Oid postgresColumnOid = pgduckdb::GetPostgresDuckDBType(column);

        if (!OidIsValid(postgresColumnOid)) {
            elog(WARNING, "(PGDuckDB/CreatePlan) Cache lookup failed for type %u", postgresColumnOid);
            return nullptr;
        }

        HeapTuple tp;
        Form_pg_type typtup;

        tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(postgresColumnOid));
        if (!HeapTupleIsValid(tp)) {
            elog(WARNING, "(PGDuckDB/CreatePlan) Cache lookup failed for type %u", postgresColumnOid);
            return nullptr;
        }

        typtup = (Form_pg_type)GETSTRUCT(tp);

        Var *var = makeVar(INDEX_VAR, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

        duckdb_node->custom_scan_tlist =
            lappend(duckdb_node->custom_scan_tlist,
                    makeTargetEntry((Expr *)var, i + 1, (char *)pstrdup(prepared_query->GetNames()[i].c_str()), false));

        ReleaseSysCache(tp);
    }

    duckdb_node->custom_private = list_make1(query);
    duckdb_node->methods = &duckdb_scan_methods;

    return (Plan *)duckdb_node;
}

PlannedStmt *DuckdbPlanner(Query *parse, int cursor_options) {
    /* We need to check can we DuckDB create plan */
    Plan *duckdb_plan = CreatePlan(parse);
    if (!duckdb_plan) {
        return nullptr;
    }

    PlannedStmt *plan = makeNode(PlannedStmt);
    plan->commandType = parse->commandType;
    plan->queryId = parse->queryId;
    plan->hasReturning = (parse->returningList != NIL);
    plan->hasModifyingCTE = parse->hasModifyingCTE;
    plan->canSetTag = parse->canSetTag;
    plan->planTree = duckdb_plan;
    plan->utilityStmt = parse->utilityStmt;
    plan->stmt_location = parse->stmt_location;
    plan->stmt_len = parse->stmt_len;
    return plan;
}

bool IsColumnstore(Oid oid) {
    Relation table = RelationIdGetRelation(oid);
    bool res = IsColumnstore(table);
    RelationClose(table);
    return res;
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
        PlannedStmt *plan = DuckdbPlanner(parse, cursor_options);
        if (!plan) {
            elog(ERROR, "DuckDB does not support this query");
        }
        return plan;
    }
    return prev_planner_hook(parse, query_string, cursor_options, bound_params);
}

ExplainOneQuery_hook_type prev_explain_one_query_hook = NULL;

void ExplainOneQueryHook(Query *query, int cursor_options, IntoClause *into, ExplainState *es, const char *query_string,
                         ParamListInfo params, QueryEnvironment *query_env) {
    /*
     * It might seem sensible to store this data in the custom_private
     * field of the CustomScan node, but that's not a trivial change to make.
     * Storing this in a global variable works fine, as long as we only use
     * this variable during planning when we're actually executing an explain
     * QUERY (this can be checked by checking the commandTag of the
     * ActivePortal). This even works when plans would normally be cached,
     * because EXPLAIN always execute this hook whenever they are executed.
     * EXPLAIN queries are also always re-planned (see
     * standard_ExplainOneQuery).
     */
    duckdb_explain_analyze = es->analyze;
    prev_explain_one_query_hook(query, cursor_options, into, es, query_string, params, query_env);
}

void InitDuckdbScan() {
    RegisterCustomScanMethods(&duckdb_scan_methods);
    prev_planner_hook = planner_hook ? planner_hook : standard_planner;
    planner_hook = PlannerHook;
    prev_explain_one_query_hook = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
    ExplainOneQuery_hook = ExplainOneQueryHook;
}
