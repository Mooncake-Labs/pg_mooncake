use super::common::*;
use bincode::{Decode, Encode};
use moonlink_backend::MoonlinkBackend;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};

#[tokio::main]
pub(super) async fn start() {
    let mut sigterm = signal(SignalKind::terminate()).expect("error setting SIGTERM handler");
    let backend = Arc::new(MoonlinkBackend::new("./pg_mooncake".to_owned()));
    if fs::metadata(SOCKET_PATH).await.is_ok() {
        fs::remove_file(SOCKET_PATH)
            .await
            .expect("error removing socket");
    }
    let listener = UnixListener::bind(SOCKET_PATH).expect("error binding socket");
    loop {
        tokio::select! {
            _ = sigterm.recv() => break,
            Ok((stream, _addr)) = listener.accept() => {
                let backend = Arc::clone(&backend);
                tokio::spawn(async move { handle_stream(backend, stream).await });
            }
        }
    }
}

async fn handle_stream(
    backend: Arc<MoonlinkBackend<u32, u32>>,
    mut stream: UnixStream,
) -> Result<(), Eof> {
    let mut map = HashMap::new();
    loop {
        match read(&mut stream).await? {
            Request::CreateSnapshot {
                database_id,
                table_id,
                lsn,
            } => {
                backend
                    .create_snapshot(database_id, table_id, lsn)
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::CreateTable {
                database_id,
                table_id,
                dst_uri,
                src,
                src_uri,
            } => {
                backend
                    .create_table(database_id, table_id, dst_uri, src, src_uri)
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::DropTable {
                database_id,
                table_id,
            } => {
                backend.drop_table(database_id, table_id).await;
                write(&mut stream, &()).await?;
            }
            Request::ScanTableBegin {
                database_id,
                table_id,
                lsn,
            } => {
                let state = backend
                    .scan_table(database_id, table_id, Some(lsn))
                    .await
                    .unwrap();
                write(&mut stream, &state.data).await?;
                map.insert((database_id, table_id), state);
            }
            Request::ScanTableEnd {
                database_id,
                table_id,
            } => {
                map.remove(&(database_id, table_id));
                write(&mut stream, &()).await?;
            }
        }
    }
}

async fn write<E: Encode>(stream: &mut UnixStream, data: &E) -> Result<(), Eof> {
    let bytes = bincode::encode_to_vec(data, BINCODE_CONFIG).expect("error encoding packet");
    let len = u32::try_from(bytes.len()).expect("packet too long");
    check_eof(stream.write_all(&len.to_ne_bytes()).await)?;
    check_eof(stream.write_all(&bytes).await)
}

async fn read<D: Decode<()>>(stream: &mut UnixStream) -> Result<D, Eof> {
    let mut buf = [0; 4];
    check_eof(stream.read_exact(&mut buf).await)?;
    let len = u32::from_ne_bytes(buf);
    let mut bytes = vec![0; len as usize];
    check_eof(stream.read_exact(&mut bytes).await)?;
    let (data, _) =
        bincode::decode_from_slice(&bytes, BINCODE_CONFIG).expect("error decoding packet");
    Ok(data)
}

struct Eof;

fn check_eof<T>(r: std::io::Result<T>) -> Result<T, Eof> {
    use std::io::ErrorKind::*;
    if let Err(ref e) = r {
        if matches!(e.kind(), BrokenPipe | ConnectionReset | UnexpectedEof) {
            return Err(Eof);
        }
    }
    Ok(r.expect("IO error"))
}
