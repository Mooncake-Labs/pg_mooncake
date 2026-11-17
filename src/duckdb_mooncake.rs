use std::ffi::{c_char, c_void, CStr, CString};

extern "C" {
    fn get_database_name(dbid: u32) -> *mut c_char;
    fn pfree(pointer: *mut c_void);
}

#[no_mangle]
pub extern "C" fn pgmooncake_drop_cstring(cstring: *mut c_char) {
    unsafe { drop(CString::from_raw(cstring)) }
}

#[no_mangle]
extern "C" fn pgmooncake_get_init_query() -> *mut c_char {
    // This may not run in the backend's main thread, but it should be the only active thread
    let ptr = unsafe { get_database_name(pgrx::pg_sys::MyDatabaseId.to_u32()) };
    let database = unsafe { CStr::from_ptr(ptr).to_str().unwrap().to_owned() };
    unsafe { pfree(ptr as *mut c_void) };

    let socket = crate::data_directory::socket_path().replace('\'', "''");
    let init_query = format!(
        "ATTACH DATABASE 'mooncake' (TYPE mooncake, URI '{socket}', DATABASE '{database}')"
    );
    CString::new(init_query)
        .expect("init_query should not contain an internal 0 byte")
        .into_raw()
}

#[no_mangle]
extern "C" fn pgmooncake_get_lsn() -> u64 {
    unsafe { pgrx::pg_sys::XactLastCommitEnd }
}
