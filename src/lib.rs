mod pgmoonlink;
mod table;

use pgrx::prelude::*;

pg_module_magic!();
extension_sql_file!("./sql/bootstrap.sql", bootstrap);

extern "C" {
    fn pgduckdb_init();
}

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    unsafe { pgduckdb_init() };
    pgmoonlink::init();
}
