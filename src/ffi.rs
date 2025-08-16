use crate::utils::{block_on, get_stream, DATABASE};
use moonlink_rpc::{scan_table_begin, scan_table_end};
use pgrx::prelude::*;
use std::ffi::{c_char, CStr};

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_scan_table_begin(
    schema: *const c_char,
    table: *const c_char,
    lsn: u64,
    data: *mut *mut u8,
    len: *mut usize,
) {
    let table = format!("{}.{}", ptr_to_str(schema), ptr_to_str(table));
    let mut bytes = block_on(scan_table_begin(
        &mut *get_stream(),
        DATABASE.clone(),
        table,
        lsn,
    ))
    .expect("scan_table_begin failed");
    unsafe { *data = bytes.as_mut_ptr() };
    unsafe { *len = bytes.len() };
    std::mem::forget(bytes);
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_scan_table_end(
    schema: *const c_char,
    table: *const c_char,
    data: *mut u8,
    len: usize,
) {
    unsafe { Vec::from_raw_parts(data, len, len) };
    let table = format!("{}.{}", ptr_to_str(schema), ptr_to_str(table));
    block_on(scan_table_end(&mut *get_stream(), DATABASE.clone(), table))
        .expect("scan_table_end failed");
}

fn ptr_to_str(ptr: *const c_char) -> &'static str {
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str().expect("DuckDB string should be valid UTF-8")
}
