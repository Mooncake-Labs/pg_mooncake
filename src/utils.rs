use core::ffi::CStr;
use pgrx::prelude::*;
use std::future::Future;
use std::sync::{LazyLock, Mutex, MutexGuard};
use tokio::net::UnixStream;
use tokio::runtime::{Builder, Runtime};

pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime")
    });
    RUNTIME.block_on(future)
}

pub(crate) fn get_stream() -> MutexGuard<'static, UnixStream> {
    static STREAM: LazyLock<Mutex<UnixStream>> = LazyLock::new(|| {
        Mutex::new(
            block_on(UnixStream::connect("pg_mooncake/moonlink.sock"))
                .expect("Failed to connect to moonlink"),
        )
    });
    STREAM.lock().unwrap()
}

pub(crate) fn get_database_id() -> u32 {
    unsafe { pgrx::pg_sys::MyDatabaseId.to_u32() }
}

pub(crate) fn get_loopback_uri(database: &str) -> String {
    let hosts = unsafe { CStr::from_ptr(pg_sys::Unix_socket_directories) };
    let host = hosts.to_str().unwrap().split(", ").next().unwrap();
    let port: i32 = unsafe { pg_sys::PostPortNumber };
    let user = unsafe { CStr::from_ptr(pg_sys::GetUserNameFromId(pg_sys::GetUserId(), false)) };
    let user = user.to_str().unwrap();
    format!(
        "postgresql:///{}?host={}&port={port}&user={}",
        uri_encode(database),
        uri_encode(host),
        uri_encode(user)
    )
}

fn uri_encode(input: &str) -> String {
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
    const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut result = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                result.push(byte as char)
            }
            _ => {
                result.push('%');
                result.push(HEX_DIGITS[(byte >> 4) as usize] as char);
                result.push(HEX_DIGITS[(byte & 15) as usize] as char);
            }
        }
    }
    result
}
