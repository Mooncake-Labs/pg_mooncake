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
    // moonlink's own SIGTERM handling (tokio::signal) chains to the pgrx handler above, which calls into Postgres and
    // panics on any thread but this one; the panic can also lose the shutdown request. Signals sent to the process go
    // to a thread that doesn't block them, so block them in every runtime thread and leave only this thread open.
    // Threads inherit the mask of the thread that creates them; on_thread_start also covers blocking-pool threads
    // started later from runtime threads. SIGQUIT (immediate shutdown) stays deliverable everywhere.
    set_shutdown_signal_mask(libc::SIG_BLOCK);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| set_shutdown_signal_mask(libc::SIG_BLOCK))
        .build()
        .expect("failed to build the moonlink runtime");
    set_shutdown_signal_mask(libc::SIG_UNBLOCK);
    runtime.block_on(start());
}

fn set_shutdown_signal_mask(how: libc::c_int) {
    unsafe {
        let mut signals: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut signals);
        for signal in [libc::SIGTERM, libc::SIGHUP, libc::SIGINT] {
            libc::sigaddset(&mut signals, signal);
        }
        let rc = libc::pthread_sigmask(how, &signals, std::ptr::null_mut());
        assert_eq!(rc, 0, "pthread_sigmask failed");
    }
}

async fn start() {
    let config = ServiceConfig {
        base_path: "pg_mooncake".to_owned(),
        data_server_uri: None,
        rest_api_port: None,
        otel_ingestion_api_port: None,
        tcp_port: None,
        log_directory: None,
        otel_export_target: None,
    };
    start_with_config(config).await.unwrap();
}
