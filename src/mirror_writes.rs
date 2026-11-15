//! Rejects INSERT, UPDATE, DELETE and MERGE on mirrors before pg_duckdb plans them.
//!
//! moonlink is the only writer of a mirror, so every write fails anyway, but inside pg_duckdb some of them fail badly:
//! deparsing `UPDATE <mirror> SET (a, b) = (SELECT ...)` hits an assertion that aborts the backend on assert-enabled
//! builds, and others surface as DuckDB internal errors. pg_mooncake is loaded after pg_duckdb, so this planner hook runs
//! first and raises a plain error instead.

use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use pgrx::PgList;
use std::ffi::{c_char, c_int, CStr};

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;

pub(crate) fn init() {
    unsafe {
        PREV_PLANNER_HOOK = pg_sys::planner_hook;
        pg_sys::planner_hook = Some(mooncake_planner);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn mooncake_planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: c_int,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    reject_writes_to_mirrors(parse);
    match PREV_PLANNER_HOOK {
        Some(prev) => prev(parse, query_string, cursor_options, bound_params),
        None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
    }
}

/// Checks the statement's target table and those of data-modifying CTEs (`WITH x AS (DELETE FROM ...)`).
unsafe fn reject_writes_to_mirrors(query: *mut pg_sys::Query) {
    if query.is_null() {
        return;
    }
    let query = &*query;
    let writes = !matches!(
        query.commandType,
        pg_sys::CmdType::CMD_SELECT | pg_sys::CmdType::CMD_UTILITY | pg_sys::CmdType::CMD_NOTHING
    );
    if writes && query.resultRelation > 0 {
        let rtable = PgList::<pg_sys::RangeTblEntry>::from_pg(query.rtable);
        if let Some(rte) = rtable.get_ptr(query.resultRelation as usize - 1) {
            if (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION && is_mirror((*rte).relid) {
                report_write_to_mirror((*rte).relid);
            }
        }
    }
    for cte in PgList::<pg_sys::CommonTableExpr>::from_pg(query.cteList).iter_ptr() {
        let cte_query = (*cte).ctequery;
        if !cte_query.is_null() && (*cte_query).type_ == pg_sys::NodeTag::T_Query {
            reject_writes_to_mirrors(cte_query.cast());
        }
    }
}

unsafe fn is_mirror(relid: pg_sys::Oid) -> bool {
    let mooncake_am = pg_sys::get_table_am_oid(c"mooncake".as_ptr(), true);
    if mooncake_am == pg_sys::InvalidOid {
        return false;
    }
    let rel = pg_sys::RelationIdGetRelation(relid);
    if rel.is_null() {
        return false;
    }
    let relam = (*(*rel).rd_rel).relam;
    pg_sys::RelationClose(rel);
    relam == mooncake_am
}

unsafe fn report_write_to_mirror(relid: pg_sys::Oid) -> ! {
    let name = |ptr: *const c_char| {
        if ptr.is_null() {
            "?".to_owned()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    };
    let qualified = format!(
        "{}.{}",
        name(pg_sys::get_namespace_name(pg_sys::get_rel_namespace(relid))),
        name(pg_sys::get_rel_name(relid))
    );
    ErrorReport::new(
        PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
        format!("cannot modify mirror {qualified}"),
        function_name!(),
    )
    .set_detail("Mirrors are read-only: moonlink keeps them in sync with their source table.")
    .set_hint("Modify the source table instead.")
    .report(PgLogLevel::ERROR);
    unreachable!("ERROR does not return")
}
