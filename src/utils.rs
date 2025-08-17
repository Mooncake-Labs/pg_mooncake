use crate::gucs::MOONLINK_URI;
use pgrx::pg_sys;
use std::ffi::CStr;
use std::future::Future;
use std::sync::{LazyLock, Mutex};
use tokio::net::TcpStream;
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

static POOL: Mutex<Vec<TcpStream>> = Mutex::new(Vec::new());

pub(crate) fn get_stream() -> TcpStream {
    if let Some(stream) = POOL.lock().unwrap().pop() {
        return stream;
    }
    let uri = MOONLINK_URI.get().expect("mooncake.moonlink_uri not set");
    let uri = uri.to_str().expect("moonlink_uri should be valid UTF-8");
    block_on(TcpStream::connect(uri)).expect("Failed to connect to moonlink")
}

pub(crate) fn return_stream(stream: TcpStream) {
    let mut pool = POOL.lock().unwrap();
    pool.push(stream);
}
