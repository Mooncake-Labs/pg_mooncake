use bincode::{config, Decode, Encode};

pub(super) const BINCODE_CONFIG: config::Configuration = config::standard();

pub(super) const SOCKET_PATH: &str = "pg_mooncake/moonlink.sock";

#[derive(Debug, Encode, Decode)]
pub(super) enum Request {
    CreateSnapshot {
        database_id: u32,
        table_id: u32,
        lsn: u64,
    },
    CreateTable {
        database_id: u32,
        table_id: u32,
        dst_uri: String,
        src: String,
        src_uri: String,
    },
    DropTable {
        database_id: u32,
        table_id: u32,
    },
    ScanTableBegin {
        database_id: u32,
        table_id: u32,
        lsn: u64,
    },
    ScanTableEnd {
        database_id: u32,
        table_id: u32,
    },
}
