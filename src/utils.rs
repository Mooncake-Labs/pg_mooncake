use pgrx::pg_sys;
use std::ffi::CStr;
use std::future::Future;
use std::sync::{LazyLock, Mutex};
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

static POOL: Mutex<Vec<UnixStream>> = Mutex::new(Vec::new());

pub(crate) fn get_stream() -> UnixStream {
    if let Some(stream) = POOL.lock().unwrap().pop() {
        return stream;
    }
    block_on(UnixStream::connect("pg_mooncake/moonlink.sock"))
        .expect("Failed to connect to moonlink")
}

pub(crate) fn return_stream(stream: UnixStream) {
    let mut pool = POOL.lock().unwrap();
    pool.push(stream);
}
