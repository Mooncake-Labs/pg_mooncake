use bincode::{config, Decode, Encode};
use std::fmt;

pub(super) const BINCODE_CONFIG: config::Configuration = config::standard();

pub(super) const SOCKET_PATH: &str = "pg_mooncake/moonlink.sock";

#[derive(Clone, Debug, PartialEq, Eq, Hash, Encode, Decode)]
pub(super) struct TableId {
    pub(super) database_id: u32,
    pub(super) table_id: u32,
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.database_id, self.table_id)
    }
}

#[derive(Debug, Encode, Decode)]
pub(super) enum Request {
    CreateSnapshot {
        table_id: TableId,
        lsn: u64,
    },
    CreateTable {
        table_id: TableId,
        table: String,
        uri: String,
    },
    DropTable {
        table_id: TableId,
    },
    ScanTableBegin {
        table_id: TableId,
        lsn: u64,
    },
    ScanTableEnd {
        table_id: TableId,
    },
}
