#[cfg(feature = "bgworker")]
mod bgworker;
mod drop_tracking;
mod duckdb_mooncake;
mod functions;
mod mirror_writes;
mod table;
mod utils;

use pgrx::prelude::*;

pg_module_magic!();
extension_sql_file!("./sql/bootstrap.sql", bootstrap);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    #[cfg(feature = "bgworker")]
    bgworker::init();
    table::init();
    drop_tracking::init();
    mirror_writes::init();
}
