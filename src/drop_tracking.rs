//! Remembers which relations being dropped are mooncake mirrors.
//!
//! The `sql_drop` event trigger runs after the catalog rows are gone, so it cannot tell a mirror from any
//! other table. `object_access_hook` runs for every dropped object (including cascades and DROP SCHEMA)
//! *before* deletion, while `pg_class.relam` is still readable.

use pgrx::pg_sys;
use std::collections::HashMap;
use std::ffi::{c_int, c_void, CStr};
use std::sync::{Mutex, PoisonError};

/// Mirrors dropped in the current transaction: relation oid -> moonlink table name (`schema.table`).
static DROPPED_MIRRORS: Mutex<Option<HashMap<u32, String>>> = Mutex::new(None);

static mut PREV_OBJECT_ACCESS_HOOK: pg_sys::object_access_hook_type = None;

pub(crate) fn init() {
    unsafe {
        PREV_OBJECT_ACCESS_HOOK = pg_sys::object_access_hook;
        pg_sys::object_access_hook = Some(mooncake_object_access);
    }
}

/// Removes and returns the moonlink name of `relid` if it is a mirror dropped in this transaction.
pub(crate) fn take_dropped_mirror(relid: u32) -> Option<String> {
    let mut guard = DROPPED_MIRRORS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    guard.as_mut().and_then(|map| map.remove(&relid))
}

fn clear() {
    *DROPPED_MIRRORS
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = None;
}

#[pgrx::pg_guard]
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
    let mooncake_am = pg_sys::get_table_am_oid(c"mooncake".as_ptr(), true);
    if mooncake_am == pg_sys::InvalidOid {
        return;
    }
    let rel = pg_sys::RelationIdGetRelation(object_id);
    if rel.is_null() {
        return;
    }
    let rd_rel = (*rel).rd_rel;
    let name = if (*rd_rel).relam == mooncake_am {
        let schema = pg_sys::get_namespace_name((*rd_rel).relnamespace);
        let table = CStr::from_ptr((*rd_rel).relname.data.as_ptr());
        (!schema.is_null()).then(|| {
            format!(
                "{}.{}",
                CStr::from_ptr(schema).to_string_lossy(),
                table.to_string_lossy()
            )
        })
    } else {
        None
    };
    pg_sys::RelationClose(rel);
    let Some(name) = name else { return };

    let mut guard = DROPPED_MIRRORS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if guard.is_none() {
        // Forget everything at transaction end, including entries whose DROP was rolled back.
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, clear);
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, clear);
    }
    guard
        .get_or_insert_with(HashMap::new)
        .insert(object_id.to_u32(), name);
}
