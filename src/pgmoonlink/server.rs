use super::common::*;
use bincode::{Decode, Encode};
use moonlink_backend::{MoonlinkBackend, ReadState};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};

static BACKEND: LazyLock<MoonlinkBackend<TableId>> = LazyLock::new(MoonlinkBackend::default);

#[tokio::main]
pub(super) async fn start() {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    LazyLock::force(&BACKEND);
    if fs::metadata(SOCKET_PATH).await.is_ok() {
        fs::remove_file(SOCKET_PATH).await.unwrap();
    }
    let listener = UnixListener::bind(SOCKET_PATH).unwrap();
    loop {
        tokio::select! {
            _ = sigterm.recv() => break,
            Ok((stream, _addr)) = listener.accept() => {
                tokio::spawn(async move { handle_stream(stream).await });
            }
        }
    }
}

async fn handle_stream(mut stream: UnixStream) {
    let mut map = HashMap::new();
    loop {
        match read(&mut stream).await {
            Request::CreateSnapshot { table_id, lsn } => {
                create_snapshot(&mut stream, table_id, lsn).await
            }
            Request::CreateTable {
                table_id,
                uri,
                table,
            } => create_table(&mut stream, table_id, table, uri).await,
            Request::ScanTableBegin { table_id, lsn } => {
                scan_table_begin(&mut stream, &mut map, table_id, lsn).await
            }
            Request::ScanTableEnd { table_id } => {
                scan_table_end(&mut stream, &mut map, table_id).await
            }
        }
    }
}

async fn create_snapshot(stream: &mut UnixStream, table_id: TableId, lsn: u64) {
    BACKEND
        .create_iceberg_snapshot(&table_id, lsn)
        .await
        .unwrap();
    write(stream, &()).await;
}

async fn create_table(stream: &mut UnixStream, table_id: TableId, table: String, uri: String) {
    BACKEND.create_table(table_id, &table, &uri).await.unwrap();
    write(stream, &()).await;
}

async fn scan_table_begin(
    stream: &mut UnixStream,
    map: &mut HashMap<TableId, Arc<ReadState>>,
    table_id: TableId,
    lsn: u64,
) {
    let state = BACKEND.scan_table(&table_id, Some(lsn)).await.unwrap();
    write(stream, &state.data).await;
    map.insert(table_id, state);
}

async fn scan_table_end(
    stream: &mut UnixStream,
    map: &mut HashMap<TableId, Arc<ReadState>>,
    table_id: TableId,
) {
    map.remove(&table_id);
    write(stream, &()).await;
}

async fn write<E: Encode>(stream: &mut UnixStream, data: &E) {
    let bytes = bincode::encode_to_vec(data, BINCODE_CONFIG).unwrap();
    let len = u32::try_from(bytes.len()).unwrap();
    stream.write_all(&len.to_ne_bytes()).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
}

async fn read<D: Decode<()>>(stream: &mut UnixStream) -> D {
    let mut buf = [0; 4];
    stream.read_exact(&mut buf).await.unwrap();
    let len = u32::from_ne_bytes(buf);
    let mut bytes = vec![0; len as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    bincode::decode_from_slice(&bytes, BINCODE_CONFIG)
        .unwrap()
        .0
}
