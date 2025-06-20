use super::common::*;
use bincode::{Decode, Encode};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{LazyLock, Mutex};

static STREAM: LazyLock<Mutex<UnixStream>> =
    LazyLock::new(|| Mutex::new(UnixStream::connect(SOCKET_PATH).unwrap()));

pub(crate) fn create_snapshot(database_id: u32, table_id: u32, lsn: u64) {
    let mut stream = STREAM.lock().unwrap();
    let table_id = TableId {
        database_id,
        table_id,
    };
    write(&mut stream, &Request::CreateSnapshot { table_id, lsn });
    read(&mut stream)
}

pub(crate) fn create_table(database_id: u32, table_id: u32, table: String, uri: String) {
    let mut stream = STREAM.lock().unwrap();
    let table_id = TableId {
        database_id,
        table_id,
    };
    write(
        &mut stream,
        &Request::CreateTable {
            table_id,
            table,
            uri,
        },
    );
    read(&mut stream)
}

pub(crate) fn drop_table(database_id: u32, table_id: u32) {
    let mut stream = STREAM.lock().unwrap();
    let table_id = TableId {
        database_id,
        table_id,
    };
    write(&mut stream, &Request::DropTable { table_id });
    read(&mut stream)
}

pub(super) fn scan_table_begin(table_id: TableId, lsn: u64) -> Vec<u8> {
    let mut stream = STREAM.lock().unwrap();
    write(&mut stream, &Request::ScanTableBegin { table_id, lsn });
    read(&mut stream)
}

pub(super) fn scan_table_end(table_id: TableId) {
    let mut stream = STREAM.lock().unwrap();
    write(&mut stream, &Request::ScanTableEnd { table_id });
    read(&mut stream)
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
