#[cfg(feature = "bgworker")]
mod bgworker;
mod data_directory;
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
    // The moonlink worker, mooncake.data_directory and the hooks all need to be set up by the postmaster.
    if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        pgrx::pg_sys::panic::ErrorReport::new(
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "pg_mooncake must be loaded via shared_preload_libraries",
            function_name!(),
        )
        .set_hint("Add pg_duckdb,pg_mooncake to shared_preload_libraries and restart the server.")
        .report(PgLogLevel::ERROR);
    }
    data_directory::init();
    #[cfg(feature = "bgworker")]
    bgworker::init();
    table::init();
    drop_tracking::init();
    mirror_writes::init();
}
