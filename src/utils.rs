use pgrx::pg_sys;
use std::ffi::CStr;
use std::future::Future;
use std::os::fd::AsRawFd;
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
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

/// Why a moonlink call failed.
pub(crate) enum RpcError {
    Connect(std::io::Error),
    Timeout(Duration),
    Transport(moonlink_rpc::Error),
    Server(moonlink_rpc::Error),
}

impl RpcError {
    /// moonlink is not accepting connections or not answering, so later calls would fail the same way. A
    /// transport error only breaks this connection (the next call reconnects).
    pub(crate) fn is_unreachable(&self) -> bool {
        matches!(self, RpcError::Connect(_) | RpcError::Timeout(_))
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Connect(e) => write!(f, "failed to connect to moonlink: {e}"),
            RpcError::Timeout(timeout) => write!(f, "no response from moonlink within {timeout:?}"),
            RpcError::Transport(e) | RpcError::Server(e) => write!(f, "{e}"),
        }
    }
}

pub(crate) fn connect(slot: &mut Option<UnixStream>) -> Result<&mut UnixStream, RpcError> {
    if slot.as_ref().is_some_and(|stream| !is_open(stream)) {
        *slot = None;
    }
    if slot.is_none() {
        let stream = block_on(UnixStream::connect(crate::data_directory::socket_path()))
            .map_err(RpcError::Connect)?;
        *slot = Some(stream);
    }
    Ok(slot.as_mut().unwrap())
}

/// Whether an idle cached stream can still be used. RPCs are strict request/response, so a healthy idle stream
/// has nothing to read. EOF or a reset means moonlink went away (e.g. the worker restarted), and unread bytes mean
/// the stream is out of sync; either way, reconnect instead of failing the next call.
fn is_open(stream: &UnixStream) -> bool {
    let mut byte = 0u8;
    let n = unsafe {
        libc::recv(
            stream.as_raw_fd(),
            std::ptr::addr_of_mut!(byte).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    n < 0
        && matches!(
            std::io::Error::last_os_error().kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
        )
}

/// Classifies a failed call. Transport errors discard the connection; server-side errors (`Rpc`) leave the
/// stream in sync, and encode errors and oversized packets fail before anything is written.
pub(crate) fn classify(slot: &mut Option<UnixStream>, error: moonlink_rpc::Error) -> RpcError {
    if matches!(
        error,
        moonlink_rpc::Error::Io(_) | moonlink_rpc::Error::Decode(_)
    ) {
        *slot = None;
        RpcError::Transport(error)
    } else {
        RpcError::Server(error)
    }
}

/// Calls a `moonlink_rpc` function on this backend's connection, yielding `Result<T, RpcError>`. The lock is
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
                        Err($crate::utils::RpcError::Timeout(timeout))
                    }
                    Ok(result) => result.map_err(|e| $crate::utils::classify(&mut slot, e)),
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
                result.map_err(|e| $crate::utils::classify(&mut slot, e))
            }
        }
    }};
}
pub(crate) use rpc;

/// Whether `relid` is a mooncake mirror: a relation using the `mooncake` table access method.
pub(crate) unsafe fn is_mirror(relid: pg_sys::Oid) -> bool {
    let mooncake_am = pg_sys::get_table_am_oid(c"mooncake".as_ptr(), true);
    if mooncake_am == pg_sys::InvalidOid {
        return false;
    }
    let rel = pg_sys::RelationIdGetRelation(relid);
    if rel.is_null() {
        return false;
    }
    let relam = (*(*rel).rd_rel).relam;
    pg_sys::RelationClose(rel);
    relam == mooncake_am
}
