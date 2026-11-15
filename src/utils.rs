use pgrx::pg_sys;
use std::ffi::CStr;
use std::future::Future;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};
use tokio::net::UnixStream;
use tokio::runtime::{Builder, Runtime};

pub(crate) static DATABASE: LazyLock<String> = LazyLock::new(|| {
    let database = unsafe { CStr::from_ptr(pg_sys::get_database_name(pg_sys::MyDatabaseId)) };
    database.to_str().unwrap().to_owned()
});

pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime")
    });
    RUNTIME.block_on(future)
}

/// This backend's connection to moonlink. `None` until first use and after a transport error, so the next
/// call reconnects instead of reusing a stream the server closed (e.g. after a moonlink restart).
static STREAM: Mutex<Option<UnixStream>> = Mutex::new(None);

/// Poison-tolerant: a panic while the lock was held must not make every later call in this session fail.
pub(crate) fn lock_stream() -> MutexGuard<'static, Option<UnixStream>> {
    STREAM.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(crate) fn connect(slot: &mut Option<UnixStream>) -> Result<&mut UnixStream, String> {
    if slot.is_none() {
        let stream = block_on(UnixStream::connect("pg_mooncake/moonlink.sock"))
            .map_err(|e| format!("failed to connect to moonlink: {e}"))?;
        *slot = Some(stream);
    }
    Ok(slot.as_mut().unwrap())
}

/// Whether the connection must be discarded after `error`. Server-side errors (`Rpc`) leave the stream in
/// sync, and encode errors and oversized packets fail before anything is written.
pub(crate) fn is_transport_error(error: &moonlink_rpc::Error) -> bool {
    matches!(
        error,
        moonlink_rpc::Error::Io(_) | moonlink_rpc::Error::Decode(_)
    )
}

/// Calls a `moonlink_rpc` function on this backend's connection, yielding `Result<T, String>`. The lock is
/// released before the result is returned, so callers may raise a Postgres ERROR with it.
/// `rpc!(timeout = <Duration>; func, args..)` bounds the call; on timeout the connection is discarded.
macro_rules! rpc {
    (timeout = $timeout:expr; $func:path $(, $arg:expr)* $(,)?) => {{
        let mut slot = $crate::utils::lock_stream();
        match $crate::utils::connect(&mut slot) {
            Err(e) => Err(e),
            Ok(stream) => {
                let call = $func(stream $(, $arg)*);
                // The timer must be created inside the runtime, so build it within the future.
                let timeout = $timeout;
                match $crate::utils::block_on(async move { tokio::time::timeout(timeout, call).await }) {
                    Err(_) => {
                        *slot = None;
                        Err(format!("no response from moonlink within {timeout:?}"))
                    }
                    Ok(result) => {
                        if let Err(e) = &result {
                            if $crate::utils::is_transport_error(e) {
                                *slot = None;
                            }
                        }
                        result.map_err(|e| e.to_string())
                    }
                }
            }
        }
    }};
    ($func:path $(, $arg:expr)* $(,)?) => {{
        let mut slot = $crate::utils::lock_stream();
        match $crate::utils::connect(&mut slot) {
            Err(e) => Err(e),
            Ok(stream) => {
                let result = $crate::utils::block_on($func(stream $(, $arg)*));
                if let Err(e) = &result {
                    if $crate::utils::is_transport_error(e) {
                        *slot = None;
                    }
                }
                result.map_err(|e| e.to_string())
            }
        }
    }};
}
pub(crate) use rpc;
