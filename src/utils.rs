use pgrx::pg_sys;
use std::ffi::CStr;
use std::future::Future;
use std::sync::{LazyLock, Mutex, MutexGuard};
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

pub(crate) fn get_stream() -> MutexGuard<'static, UnixStream> {
    static STREAM: LazyLock<Mutex<UnixStream>> = LazyLock::new(|| {
        Mutex::new(
            block_on(UnixStream::connect("pg_mooncake/moonlink.sock"))
                .expect("Failed to connect to moonlink"),
        )
    });
    STREAM.lock().unwrap()
}
