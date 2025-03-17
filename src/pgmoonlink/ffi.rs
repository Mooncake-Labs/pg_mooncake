use super::client::{scan_table_begin, scan_table_end};
use super::common::TableId;
use pgrx::prelude::*;

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_scan_table_begin(
    table_id: u32,
    lsn: u64,
    data: *mut *mut u8,
    len: *mut usize,
) {
    let table_id = TableId {
        database_id: unsafe { pgrx::pg_sys::MyDatabaseId.to_u32() },
        table_id,
    };
    let mut bytes = scan_table_begin(table_id, lsn);
    unsafe { *data = bytes.as_mut_ptr() };
    unsafe { *len = bytes.len() };
    std::mem::forget(bytes);
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_scan_table_end(table_id: u32, data: *mut u8, len: usize) {
    let table_id = TableId {
        database_id: unsafe { pgrx::pg_sys::MyDatabaseId.to_u32() },
        table_id,
    };
    unsafe { Vec::from_raw_parts(data, len, len) };
    scan_table_end(table_id);
}
