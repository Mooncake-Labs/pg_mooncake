use moonlink_service::{start_with_config, ServiceConfig};
use pgrx::bgworkers::{
    BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
};
use pgrx::prelude::*;
use std::time::Duration;

pub(crate) fn init() {
    BackgroundWorkerBuilder::new("moonlink")
        .set_library("pg_mooncake")
        .set_function("moonlink_main")
        .enable_shmem_access(None)
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .set_restart_time(Some(Duration::from_secs(15)))
        .load();
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn moonlink_main(_arg: pg_sys::Datum) {
    std::env::set_var("RUST_BACKTRACE", "1");
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    start();
}

#[tokio::main]
pub async fn start() {
    let config = ServiceConfig {
        base_path: "pg_mooncake".to_owned(),
        data_server_uri: None,
        rest_api_port: None,
        otel_api_port: None,
        tcp_port: None,
        log_directory: None,
    };
    start_with_config(config).await.unwrap();
}
