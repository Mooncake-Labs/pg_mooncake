use std::future::Future;
use std::net::SocketAddr;
use std::sync::{LazyLock, Mutex, MutexGuard};
use tokio::net::TcpStream;
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

pub(crate) fn get_stream() -> MutexGuard<'static, TcpStream> {
    static STREAM: LazyLock<Mutex<TcpStream>> = LazyLock::new(|| {
        let addr: SocketAddr = "34.19.1.175:3031"
            .parse()
            .expect("Failed to parse IP address");
        // TODO(hjiang): Need to reuse the same connection.
        Mutex::new(
            block_on(TcpStream::connect(addr)).expect("Failed to connect to moonlink"),
        )
    });
    STREAM.lock().unwrap()
}

pub(crate) fn get_database_id() -> u32 {
    unsafe { pgrx::pg_sys::MyDatabaseId.to_u32() }
}
