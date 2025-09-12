use crate::utils::DATABASE;
use std::ffi::{c_char, CString};

#[no_mangle]
pub extern "C" fn pgmooncake_drop_cstring(cstring: *mut c_char) {
    unsafe { drop(CString::from_raw(cstring)) }
}

#[no_mangle]
extern "C" fn pgmooncake_get_init_query() -> *mut c_char {
    let init_query = format!("ATTACH DATABASE 'mooncake' (TYPE mooncake, URI 'pg_mooncake/moonlink.sock', DATABASE '{}')", *DATABASE);
    CString::new(init_query)
        .expect("init_query should not contain an internal 0 byte")
        .into_raw()
}

#[no_mangle]
extern "C" fn pgmooncake_get_lsn() -> u64 {
    unsafe { pgrx::pg_sys::XactLastCommitEnd }
}
