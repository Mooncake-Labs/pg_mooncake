#[cfg(feature = "bgworker")]
mod bgworker;
mod duckdb_mooncake;
mod functions;
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
}
