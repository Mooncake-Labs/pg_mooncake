//! Rejects statements that would modify mirrors, or break moonlink's hold on them, before pg_duckdb sees them.
//!
//! moonlink is the only writer of a mirror, so every write fails anyway, but inside pg_duckdb some of them fail badly:
//! deparsing `UPDATE <mirror> SET (a, b) = (SELECT ...)` hits an assertion that aborts the backend on assert-enabled
//! builds, and others surface as DuckDB internal errors. Other statements succeed when they should not: TRUNCATE
//! empties the Postgres side only, and a mirror attached as a partition or inheritance child takes writes through its
//! parent. moonlink keeps each mirror under the name it was created with, so a renamed mirror (or one in a renamed
//! schema) silently stops receiving changes. pg_mooncake is loaded after pg_duckdb, so these hooks run first and raise
//! a plain error instead.
//!
//! The planner hook covers INSERT, UPDATE, DELETE and MERGE (including data-modifying CTEs); the utility hook covers
//! TRUNCATE, COPY FROM, renames, SET SCHEMA, ATTACH PARTITION and INHERIT.

use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use pgrx::PgList;
use std::ffi::{c_char, c_int, CStr};

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;
static mut PREV_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;

pub(crate) fn init() {
    unsafe {
        PREV_PLANNER_HOOK = pg_sys::planner_hook;
        pg_sys::planner_hook = Some(mooncake_planner);
        PREV_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(mooncake_process_utility);
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

#[pg_guard]
#[allow(clippy::too_many_arguments)]
unsafe extern "C-unwind" fn mooncake_process_utility(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    if !pstmt.is_null() {
        check_utility_statement((*pstmt).utilityStmt);
    }
    match PREV_PROCESS_UTILITY_HOOK {
        Some(prev) => prev(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        ),
        None => pg_sys::standard_ProcessUtility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        ),
    }
}

unsafe fn check_utility_statement(stmt: *mut pg_sys::Node) {
    if stmt.is_null() {
        return;
    }
    match (*stmt).type_ {
        pg_sys::NodeTag::T_TruncateStmt => {
            let stmt = &*stmt.cast::<pg_sys::TruncateStmt>();
            for relation in PgList::<pg_sys::RangeVar>::from_pg(stmt.relations).iter_ptr() {
                if let Some(relid) = mirror_oid(relation) {
                    report_write_to_mirror(relid);
                }
            }
        }
        pg_sys::NodeTag::T_CopyStmt => {
            let stmt = &*stmt.cast::<pg_sys::CopyStmt>();
            if stmt.is_from {
                if let Some(relid) = mirror_oid(stmt.relation) {
                    report_write_to_mirror(relid);
                }
            }
        }
        pg_sys::NodeTag::T_RenameStmt => {
            let stmt = &*stmt.cast::<pg_sys::RenameStmt>();
            match stmt.renameType {
                pg_sys::ObjectType::OBJECT_TABLE => {
                    if let Some(relid) = mirror_oid(stmt.relation) {
                        report_rename_mirror(format!("cannot rename mirror {}", mirror_name(relid)));
                    }
                }
                pg_sys::ObjectType::OBJECT_SCHEMA if !stmt.subname.is_null() => {
                    let namespace = pg_sys::get_namespace_oid(stmt.subname, true);
                    if namespace != pg_sys::InvalidOid && schema_has_mirrors(namespace) {
                        let schema = CStr::from_ptr(stmt.subname).to_string_lossy();
                        report_rename_mirror(format!(
                            "cannot rename schema {schema} because it contains mirrors"
                        ));
                    }
                }
                _ => {}
            }
        }
        pg_sys::NodeTag::T_AlterObjectSchemaStmt => {
            let stmt = &*stmt.cast::<pg_sys::AlterObjectSchemaStmt>();
            if stmt.objectType == pg_sys::ObjectType::OBJECT_TABLE {
                if let Some(relid) = mirror_oid(stmt.relation) {
                    report_rename_mirror(format!(
                        "cannot move mirror {} to another schema",
                        mirror_name(relid)
                    ));
                }
            }
        }
        pg_sys::NodeTag::T_AlterTableStmt => {
            let stmt = &*stmt.cast::<pg_sys::AlterTableStmt>();
            for cmd in PgList::<pg_sys::AlterTableCmd>::from_pg(stmt.cmds).iter_ptr() {
                let child = match (*cmd).subtype {
                    pg_sys::AlterTableType::AT_AttachPartition if !(*cmd).def.is_null() => {
                        (*(*cmd).def.cast::<pg_sys::PartitionCmd>()).name
                    }
                    pg_sys::AlterTableType::AT_AddInherit => stmt.relation,
                    _ => continue,
                };
                if let Some(relid) = mirror_oid(child) {
                    report_attach_mirror(relid);
                }
            }
        }
        _ => {}
    }
}

/// The relation a statement names, if it exists and is a mirror. Takes no lock: the statement itself locks it next,
/// and a concurrent change can only turn the mirror into something the statement handles on its own.
unsafe fn mirror_oid(relation: *mut pg_sys::RangeVar) -> Option<pg_sys::Oid> {
    if relation.is_null() {
        return None;
    }
    let relid = pg_sys::RangeVarGetRelidExtended(
        relation,
        pg_sys::NoLock as pg_sys::LOCKMODE,
        pg_sys::RVROption::RVR_MISSING_OK as _,
        None,
        std::ptr::null_mut(),
    );
    (relid != pg_sys::InvalidOid && is_mirror(relid)).then_some(relid)
}

unsafe fn schema_has_mirrors(namespace: pg_sys::Oid) -> bool {
    Spi::get_one::<bool>(&format!(
        "SELECT EXISTS (SELECT FROM pg_class c JOIN pg_am a ON a.oid = c.relam \
         WHERE a.amname = 'mooncake' AND c.relnamespace = {})",
        namespace.to_u32()
    ))
    .ok()
    .flatten()
    .unwrap_or(false)
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

pub(crate) unsafe fn mirror_name(relid: pg_sys::Oid) -> String {
    let name = |ptr: *const c_char| {
        if ptr.is_null() {
            "?".to_owned()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    };
    format!(
        "{}.{}",
        name(pg_sys::get_namespace_name(pg_sys::get_rel_namespace(relid))),
        name(pg_sys::get_rel_name(relid))
    )
}

unsafe fn report_write_to_mirror(relid: pg_sys::Oid) -> ! {
    ErrorReport::new(
        PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
        format!("cannot modify mirror {}", mirror_name(relid)),
        function_name!(),
    )
    .set_detail("Mirrors are read-only: moonlink keeps them in sync with their source table.")
    .set_hint("Modify the source table instead.")
    .report(PgLogLevel::ERROR);
    unreachable!("ERROR does not return")
}

unsafe fn report_attach_mirror(relid: pg_sys::Oid) -> ! {
    ErrorReport::new(
        PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
        format!(
            "cannot make mirror {} a partition or child of another table",
            mirror_name(relid)
        ),
        function_name!(),
    )
    .set_detail("Writes to the parent table would reach the mirror, which only moonlink may write.")
    .report(PgLogLevel::ERROR);
    unreachable!("ERROR does not return")
}

fn report_rename_mirror(message: String) -> ! {
    ErrorReport::new(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        message,
        function_name!(),
    )
    .set_detail("moonlink keeps a mirror under the name it was created with.")
    .set_hint(
        "Drop the mirror and create it again under the new name: \
         DROP TABLE <mirror>; CALL mooncake.create_table('<new name>', '<source>');",
    )
    .report(PgLogLevel::ERROR);
    unreachable!("ERROR does not return")
}
