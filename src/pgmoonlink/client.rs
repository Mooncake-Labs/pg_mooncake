use super::common::*;
use anyhow::Result;
use bincode::{Decode, Encode};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Mutex;

pub(crate) fn create_snapshot(table_id: u32, lsn: u64) -> Result<()> {
    rpc(Request::CreateSnapshot {
        database_id: get_database_id(),
        table_id,
        lsn,
    })
}

pub(crate) fn create_table(
    table_id: u32,
    dst_uri: String,
    src: String,
    src_uri: String,
) -> Result<()> {
    rpc(Request::CreateTable {
        database_id: get_database_id(),
        table_id,
        dst_uri,
        src,
        src_uri,
    })
}

pub(crate) fn drop_table(table_id: u32) -> Result<()> {
    rpc(Request::DropTable {
        database_id: get_database_id(),
        table_id,
    })
}

pub(super) fn scan_table_begin(table_id: u32, lsn: u64) -> Result<Vec<u8>> {
    rpc(Request::ScanTableBegin {
        database_id: get_database_id(),
        table_id,
        lsn,
    })
}

pub(super) fn scan_table_end(table_id: u32) -> Result<()> {
    panic!("wtf!!!!!");
    rpc(Request::ScanTableEnd {
        database_id: get_database_id(),
        table_id,
    })
}

fn get_database_id() -> u32 {
    unsafe { pgrx::pg_sys::MyDatabaseId.to_u32() }
}

fn rpc<E: Encode, D: Decode<()>>(request: E) -> Result<D> {
    static STREAM: Mutex<Option<UnixStream>> = Mutex::new(None);
    let mut guard = STREAM.lock().expect("rpc() should not panic");
    if guard.is_none() {
        *guard = Some(UnixStream::connect(SOCKET_PATH)?);
    }
    let stream = guard.as_mut().expect("stream should be set above");

    (|| {
        let bytes = bincode::encode_to_vec(request, BINCODE_CONFIG)?;
        let len = u32::try_from(bytes.len())?;
        stream.write_all(&len.to_ne_bytes())?;
        stream.write_all(&bytes)?;

        let mut buf = [0; 4];
        stream.read_exact(&mut buf)?;
        let len = u32::from_ne_bytes(buf);
        let mut bytes = vec![0; len as usize];
        stream.read_exact(&mut bytes)?;
        Ok(bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?.0)
    })()
    .inspect_err(|_| *guard = None)
}
