use super::server;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use std::time::Duration;

pub(crate) fn init() {
    BackgroundWorkerBuilder::new("pg_moonlink")
        .set_library("pg_mooncake")
        .set_function("pgmoonlink_main")
        .enable_shmem_access(None)
        .set_restart_time(Some(Duration::from_secs(15)))
        .load();
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn pgmoonlink_main(_arg: pg_sys::Datum) {
    std::env::set_var("RUST_BACKTRACE", "1");
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    server::start();
}
