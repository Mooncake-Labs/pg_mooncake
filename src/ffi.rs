use crate::utils::{block_on, get_stream, DATABASE};
use moonlink_rpc::{get_parquet_metadatas, list_tables, scan_table_begin, scan_table_end};
use pgrx::prelude::*;
use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::sync::Mutex;
use std::time::{Duration, Instant};

type ParquetMetadatas = Vec<Vec<u8>>;

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_parquet_metadatas_new(
    data_files_len: usize,
    data_files_ptr: *const *const c_char,
) -> *mut ParquetMetadatas {
    let data_files = unsafe { std::slice::from_raw_parts(data_files_ptr, data_files_len) };
    let data_files = data_files
        .iter()
        .map(|&data_file| ptr_to_str(data_file).to_owned())
        .collect();
    let parquet_metadatas = block_on(get_parquet_metadatas(&mut *get_stream(), data_files))
        .expect("get_parquet_metadatas failed");
    Box::into_raw(Box::new(parquet_metadatas))
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_parquet_metadatas_drop(parquet_metadatas: *mut ParquetMetadatas) {
    unsafe { drop(Box::from_raw(parquet_metadatas)) }
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn mooncake_parquet_metadatas_get(
    parquet_metadatas: *mut ParquetMetadatas,
    i: usize,
    data: *mut *mut u8,
    len: *mut usize,
) {
    let parquet_metadatas = unsafe { &mut *parquet_metadatas };
    let parquet_metadata = &mut parquet_metadatas[i];
    unsafe { *data = parquet_metadata.as_mut_ptr() };
    unsafe { *len = parquet_metadata.len() };
}

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
    let tables = block_on(list_tables(&mut *get_stream())).expect("list_tables failed");
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
