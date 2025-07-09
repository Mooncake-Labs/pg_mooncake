use crate::utils::get_loopback_uri;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use postgres::{Client, NoTls};
use std::time::Duration;

pub(crate) fn init() {
    BackgroundWorkerBuilder::new("moonlink")
        .set_library("pg_mooncake")
        .set_function("moonlink_main")
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(15)))
        .load();
}

#[pg_guard]
#[no_mangle]
extern "C-unwind" fn moonlink_main(_arg: pg_sys::Datum) {
    std::env::set_var("RUST_BACKTRACE", "1");
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(None, None);
    let uris = BackgroundWorker::transaction(|| {
        let uri = get_loopback_uri("template1");
        let mut client = Client::connect(&uri, NoTls).expect("error connecting to server");
        let get_databases_query = "SELECT datname FROM pg_database WHERE NOT datistemplate";
        client
            .query(get_databases_query, &[])
            .expect("error reading databases")
            .into_iter()
            .map(|row| get_loopback_uri(row.get(0)))
            .collect()
    });
    start(uris);
}

#[tokio::main]
pub async fn start(uris: Vec<String>) {
    moonlink_service::start("pg_mooncake".to_owned(), uris)
        .await
        .unwrap();
}
