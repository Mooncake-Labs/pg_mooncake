use super::common::*;
use bincode::{Decode, Encode};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{LazyLock, Mutex};

static STREAM: LazyLock<Mutex<UnixStream>> =
    LazyLock::new(|| Mutex::new(UnixStream::connect(SOCKET_PATH).unwrap()));

pub(crate) fn create_snapshot(table_id: u32, lsn: u64) {
    let mut stream = STREAM.lock().unwrap();
    let request = Request::CreateSnapshot {
        database_id: get_database_id(),
        table_id,
        lsn,
    };
    write(&mut stream, &request);
    read(&mut stream)
}

pub(crate) fn create_table(table_id: u32, dst_uri: String, src: String, src_uri: String) {
    let mut stream = STREAM.lock().unwrap();
    let request = Request::CreateTable {
        database_id: get_database_id(),
        table_id,
        dst_uri,
        src,
        src_uri,
    };
    write(&mut stream, &request);
    read(&mut stream)
}

pub(crate) fn drop_table(table_id: u32) {
    let mut stream = STREAM.lock().unwrap();
    let request = Request::DropTable {
        database_id: get_database_id(),
        table_id,
    };
    write(&mut stream, &request);
    read(&mut stream)
}

pub(super) fn scan_table_begin(table_id: u32, lsn: u64) -> Vec<u8> {
    let mut stream = STREAM.lock().unwrap();
    let request = Request::ScanTableBegin {
        database_id: get_database_id(),
        table_id,
        lsn,
    };
    write(&mut stream, &request);
    read(&mut stream)
}

pub(super) fn scan_table_end(table_id: u32) {
    let mut stream = STREAM.lock().unwrap();
    let request = Request::ScanTableEnd {
        database_id: get_database_id(),
        table_id,
    };
    write(&mut stream, &request);
    read(&mut stream)
}

fn get_database_id() -> u32 {
    unsafe { pgrx::pg_sys::MyDatabaseId.to_u32() }
}

fn write<E: Encode>(stream: &mut UnixStream, data: &E) {
    let bytes = bincode::encode_to_vec(data, BINCODE_CONFIG).unwrap();
    let len = u32::try_from(bytes.len()).unwrap();
    stream.write_all(&len.to_ne_bytes()).unwrap();
    stream.write_all(&bytes).unwrap();
}

fn read<D: Decode<()>>(stream: &mut UnixStream) -> D {
    let mut buf = [0; 4];
    stream.read_exact(&mut buf).unwrap();
    let len = u32::from_ne_bytes(buf);
    let mut bytes = vec![0; len as usize];
    stream.read_exact(&mut bytes).unwrap();
    bincode::decode_from_slice(&bytes, BINCODE_CONFIG)
        .unwrap()
        .0
}
