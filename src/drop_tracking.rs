//! Drops mooncake mirrors in moonlink when the transaction that dropped them commits.
//!
//! The `sql_drop` event trigger runs after the catalog rows are gone, so it cannot tell a mirror from any
//! other table. `object_access_hook` runs for every dropped object (including cascades and DROP SCHEMA)
//! *before* deletion, while `pg_class.relam` is still readable. The trigger then queues the mirrors among the
//! dropped objects, and the pre-commit transaction callback tells moonlink.

use crate::functions::TableName;
use crate::utils::{is_mirror, rpc, RpcError, DATABASE};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_int, c_void, CStr};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Bounds the total time all moonlink drops of one transaction may take: the dropping transaction holds
/// AccessExclusiveLocks on the dropped relations meanwhile, and the wait cannot be cancelled.
const DROP_TABLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Drops {
    /// Mirrors seen by `object_access_hook` in this transaction, by relation oid.
    seen: HashMap<u32, TableName>,
    /// Mirrors the `sql_drop` trigger reported dropped, in oid order.
    queued: BTreeMap<u32, TableName>,
}

static DROPS: Mutex<Option<Drops>> = Mutex::new(None);

static mut PREV_OBJECT_ACCESS_HOOK: pg_sys::object_access_hook_type = None;

pub(crate) fn init() {
    unsafe {
        PREV_OBJECT_ACCESS_HOOK = pg_sys::object_access_hook;
        pg_sys::object_access_hook = Some(mooncake_object_access);
        pg_sys::RegisterXactCallback(Some(mooncake_xact_callback), std::ptr::null_mut());
    }
}

fn with_drops<T>(f: impl FnOnce(&mut Drops) -> T) -> T {
    let mut guard = DROPS.lock().unwrap_or_else(PoisonError::into_inner);
    f(guard.get_or_insert_with(Drops::default))
}

/// Called by the `sql_drop` trigger for each dropped table. Only mirrors are queued; dropping any other table
/// never depends on moonlink being reachable.
pub(crate) fn queue_drop(relid: pg_sys::Oid) {
    with_drops(|drops| {
        if let Some(name) = drops.seen.remove(&relid.to_u32()) {
            drops.queued.insert(relid.to_u32(), name);
        }
    });
}

/// Queued mirrors whose drop is still in effect. A drop undone by ROLLBACK TO SAVEPOINT (or a PL/pgSQL
/// exception block) leaves the relation visible again, so it must not be dropped in moonlink.
fn take_effective_drops() -> Vec<TableName> {
    let queued = with_drops(|drops| std::mem::take(&mut drops.queued));
    queued
        .into_iter()
        .filter(|(relid, _)| unsafe {
            !pg_sys::SearchSysCacheExists(
                pg_sys::SysCacheIdentifier::RELOID as c_int,
                pg_sys::Datum::from(pg_sys::Oid::from(*relid)),
                pg_sys::Datum::from(0),
                pg_sys::Datum::from(0),
                pg_sys::Datum::from(0),
            )
        })
        .map(|(_, name)| name)
        .collect()
}

#[pg_guard]
unsafe extern "C-unwind" fn mooncake_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT => {
            let mirrors = take_effective_drops();
            if !mirrors.is_empty() {
                drop_in_moonlink(mirrors);
            }
        }
        pg_sys::XactEvent::XACT_EVENT_PRE_PREPARE => {
            // moonlink cannot take part in two-phase commit: the drop would have to happen at COMMIT PREPARED,
            // possibly in another session.
            if !take_effective_drops().is_empty() {
                error!("PREPARE TRANSACTION is not supported in a transaction that drops a mooncake mirror");
            }
        }
        pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PREPARE => {
            *DROPS.lock().unwrap_or_else(PoisonError::into_inner) = None;
        }
        _ => {}
    }
}

/// Dropping a mirror always succeeds in Postgres, so "drop and re-create the mirror" keeps working while
/// moonlink is down or the table is in an error state. A failure only leaves moonlink-side state behind.
fn drop_in_moonlink(mirrors: Vec<TableName>) {
    let deadline = Instant::now() + DROP_TABLE_TIMEOUT;
    // Set once moonlink is unreachable, so the remaining mirrors don't wait for it again.
    let mut unreachable: Option<String> = None;
    for key in mirrors.iter().map(TableName::moonlink_name) {
        let reason = match &unreachable {
            Some(reason) => reason.clone(),
            None => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let Err(e) = rpc!(
                    timeout = remaining;
                    moonlink_rpc::drop_table, DATABASE.clone(), key.clone()
                ) else {
                    continue;
                };
                let reason = match &e {
                    RpcError::Timeout(_) => {
                        format!("no response from moonlink within {DROP_TABLE_TIMEOUT:?}")
                    }
                    e => e.to_string(),
                };
                if e.is_unreachable() {
                    unreachable = Some(reason.clone());
                }
                reason
            }
        };
        ErrorReport::new(
            PgSqlErrorCode::ERRCODE_WARNING,
            format!("mirror {key} was dropped, but moonlink did not drop it: {reason}"),
            function_name!(),
        )
        .set_hint(
            "moonlink may still hold the mirror's files and replication (see mooncake.list_tables()). Once moonlink \
             is reachable, clean up by re-creating the mirror with mooncake.create_table, or with: CREATE TABLE \
             <same name> (id int) USING mooncake; DROP TABLE <same name>;",
        )
        .report(PgLogLevel::WARNING);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn mooncake_object_access(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: c_int,
    arg: *mut c_void,
) {
    if let Some(prev) = PREV_OBJECT_ACCESS_HOOK {
        prev(access, class_id, object_id, sub_id, arg);
    }
    if access != pg_sys::ObjectAccessType::OAT_DROP
        || class_id != pg_sys::RelationRelationId
        || sub_id != 0
    {
        return;
    }
    if !is_mirror(object_id) {
        return;
    }
    let schema = pg_sys::get_namespace_name(pg_sys::get_rel_namespace(object_id));
    let table = pg_sys::get_rel_name(object_id);
    if schema.is_null() || table.is_null() {
        return;
    }
    let name = TableName {
        schema: CStr::from_ptr(schema).to_string_lossy().into_owned(),
        table: CStr::from_ptr(table).to_string_lossy().into_owned(),
    };
    with_drops(|drops| drops.seen.insert(object_id.to_u32(), name));
}
