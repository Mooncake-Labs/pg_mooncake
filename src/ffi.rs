use crate::utils::{block_on, get_stream, return_stream, DATABASE};
use moonlink_rpc::{scan_table_begin, scan_table_end};
use pgrx::prelude::*;
use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_get_table_cardinality(
    schema: *const c_char,
    table: *const c_char,
) -> u64 {
    static CARDINALITIES: Mutex<Option<(Instant, HashMap<String, u64>)>> = Mutex::new(None);
    let table = format!("{}.{}", ptr_to_str(schema), ptr_to_str(table));
    if let Some((t, cardinalities)) = CARDINALITIES.lock().unwrap().as_ref() {
        if t.elapsed() < Duration::from_secs(300) {
            return *cardinalities.get(&table).unwrap_or(&0);
        }
    }
    let mut stream = get_stream();
    let tables = block_on(moonlink_rpc::list_tables(&mut stream)).expect("list_tables failed");
    return_stream(stream);
    let cardinalities: HashMap<String, u64> = tables
        .into_iter()
        .filter(|table| table.database == *DATABASE)
        .map(|table| (table.table, table.cardinality))
        .collect();
    let cardinality = *cardinalities.get(&table).unwrap_or(&0);
    *CARDINALITIES.lock().unwrap() = Some((Instant::now(), cardinalities));
    cardinality
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_scan_table_begin(
    schema: *const c_char,
    table: *const c_char,
    _lsn: u64,
    data: *mut *mut u8,
    len: *mut usize,
) {
    let table = format!("{}.{}", ptr_to_str(schema), ptr_to_str(table));
    let mut stream = get_stream();
    let mut bytes = block_on(scan_table_begin(&mut stream, DATABASE.clone(), table, 0))
        .expect("scan_table_begin failed");
    return_stream(stream);
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
    let mut stream = get_stream();
    block_on(scan_table_end(&mut stream, DATABASE.clone(), table)).expect("scan_table_end failed");
    return_stream(stream);
}

fn ptr_to_str(ptr: *const c_char) -> &'static str {
    let cstr = unsafe { CStr::from_ptr(ptr) };
    cstr.to_str().expect("DuckDB string should be valid UTF-8")
}
