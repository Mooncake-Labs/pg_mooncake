use crate::drop_tracking::take_dropped_mirror;
use crate::utils::{rpc, DATABASE};
use core::ffi::CStr;
use native_tls::TlsConnector;
use pgrx::{direct_function_call, prelude::*};
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;
use regex::Regex;

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.create_snapshot(dst text) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn create_snapshot(dst: &str) {
    let dst = parse_table(dst).moonlink_name();
    let lsn = unsafe { pgrx::pg_sys::XactLastCommitEnd };
    rpc!(moonlink_rpc::create_snapshot, DATABASE.clone(), dst, lsn)
        .unwrap_or_else(|e| error!("mooncake.create_snapshot failed: {e}"));
}

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.create_table(dst text, src text, src_uri text DEFAULT NULL, table_config json DEFAULT NULL) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn create_table(dst: &str, src: &str, src_uri: Option<&str>, table_config: Option<&str>) {
    let dst = parse_table(dst);
    let src = parse_table(src);
    let dst_uri = get_loopback_uri();
    let src_uri = src_uri.unwrap_or(&dst_uri).to_owned();
    create_mooncake_table(&dst.quoted(), &dst_uri, &src.quoted(), &src_uri);
    let (dst, src) = (dst.moonlink_name(), src.moonlink_name());
    let table_config = table_config.unwrap_or("{}").to_owned();
    rpc!(
        moonlink_rpc::create_table,
        DATABASE.clone(),
        dst,
        src,
        src_uri,
        table_config
    )
    .unwrap_or_else(|e| error!("mooncake.create_table failed: {e}"));
}

#[pg_extern(sql = "
CREATE FUNCTION mooncake_drop_trigger() RETURNS event_trigger LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE EVENT TRIGGER mooncake_drop_trigger ON sql_drop EXECUTE FUNCTION mooncake_drop_trigger();
")]
fn drop_trigger() {
    Spi::connect(|client| {
        let get_dropped_tables_query = "SELECT objid::int8 FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table'";
        let dropped_tables = client
            .select(get_dropped_tables_query, None, &[])
            .expect("error reading dropped objects");
        for dropped_table in dropped_tables {
            let relid: i64 = dropped_table
                .get(1)
                .expect("error reading dropped table")
                .expect("error reading dropped table");
            // Only mirrors: dropping other tables must never depend on moonlink being reachable.
            let Some(table) = take_dropped_mirror(relid as u32) else {
                continue;
            };
            {
                let table = table.clone();
                pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::PreCommit, move || {
                    drop_moonlink_table(table)
                });
            }
            pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::ParallelPreCommit, move || {
                drop_moonlink_table(table)
            });
        }
    });
}

/// Bounded so that a hung moonlink cannot block DROP (the dropping transaction holds its locks meanwhile).
const DROP_TABLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Dropping a mirror always succeeds in Postgres, so "drop and re-create the mirror" keeps working while
/// moonlink is down or the table is in an error state. A failure only leaves moonlink-side state behind.
fn drop_moonlink_table(table: String) {
    if let Err(e) = rpc!(
        timeout = DROP_TABLE_TIMEOUT;
        moonlink_rpc::drop_table, DATABASE.clone(), table.clone()
    ) {
        warning!(
            "mirror {table} was dropped, but moonlink did not drop it: {e}\n\
             HINT: moonlink may still hold the mirror's files and replication (see mooncake.list_tables()). \
             Once moonlink is reachable, clean up with: CREATE TABLE <same name> (id int) USING mooncake; \
             DROP TABLE <same name>;"
        );
    }
}

#[pg_extern(sql = "
CREATE FUNCTION mooncake.list_tables() RETURNS TABLE (
    \"table\" text,
    commit_lsn pg_lsn,
    flush_lsn pg_lsn,
    iceberg_warehouse_location text
) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn list_tables() -> TableIterator<
    'static,
    (
        name!(table, String),
        name!(commit_lsn, i64),
        name!(flush_lsn, Option<i64>),
        name!(iceberg_warehouse_location, String),
    ),
> {
    let tables = rpc!(moonlink_rpc::list_tables)
        .unwrap_or_else(|e| error!("mooncake.list_tables failed: {e}"));
    TableIterator::new(
        tables
            .into_iter()
            .filter(|table| table.database == *DATABASE)
            .map(|table| {
                (
                    table.table,
                    table.commit_lsn as i64,
                    table.flush_lsn.map(|lsn| lsn as i64),
                    table.iceberg_warehouse_location,
                )
            }),
    )
}

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.optimize_table(dst text, mode text) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn optimize_table(dst: &str, mode: &str) {
    let dst = parse_table(dst).moonlink_name();
    rpc!(
        moonlink_rpc::optimize_table,
        DATABASE.clone(),
        dst,
        mode.to_owned()
    )
    .unwrap_or_else(|e| error!("mooncake.optimize_table failed: {e}"));
}

struct TableName {
    schema: String,
    table: String,
}

impl TableName {
    /// Unquoted `schema.table`: the key moonlink registers the table under. duckdb_mooncake looks tables up
    /// by unquoted `schema.table` too, and moonlink's Postgres connector splits source names on '.'.
    fn moonlink_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Quoted form for SQL that pg_mooncake runs itself.
    fn quoted(&self) -> String {
        spi::quote_qualified_identifier(&self.schema, &self.table)
    }
}

fn parse_table(input: &str) -> TableName {
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    let ident = r#"([\w$]+|"([^"]|"")+")"#;
    let pattern = format!(r#"^((?<schema>{ident})\.)?(?<table>{ident})$"#);
    let re = Regex::new(&pattern).unwrap();
    let caps = re
        .captures(input)
        .unwrap_or_else(|| panic!("invalid input: {input}"));
    let schema = caps.name("schema").map_or_else(
        || {
            let schema: &CStr =
                unsafe { direct_function_call(pg_sys::current_schema, &[]).unwrap() };
            schema.to_str().unwrap().to_owned()
        },
        |m| normalize_identifier(m.as_str()),
    );
    let table = normalize_identifier(&caps["table"]);
    if schema.contains('.') || table.contains('.') {
        panic!("identifiers containing '.' are not supported: {input}");
    }
    TableName { schema, table }
}

/// Postgres identifier semantics: quoted identifiers keep their case (with `""` unescaped), unquoted
/// identifiers are folded to lower case.
fn normalize_identifier(ident: &str) -> String {
    match ident.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => ident.to_lowercase(),
    }
}

fn get_loopback_uri() -> String {
    let hosts = unsafe { CStr::from_ptr(pg_sys::Unix_socket_directories) };
    let host = hosts.to_str().unwrap().split(",").next().unwrap().trim();
    let port: i32 = unsafe { pg_sys::PostPortNumber };
    let user = unsafe { CStr::from_ptr(pg_sys::GetUserNameFromId(pg_sys::GetUserId(), false)) };
    let user = user.to_str().unwrap();
    format!(
        "postgresql:///{}?host={}&port={port}&user={}",
        uri_encode(&DATABASE),
        uri_encode(host),
        uri_encode(user)
    )
}

fn uri_encode(input: &str) -> String {
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
    const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut result = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                result.push(byte as char)
            }
            _ => {
                result.push('%');
                result.push(HEX_DIGITS[(byte >> 4) as usize] as char);
                result.push(HEX_DIGITS[(byte & 15) as usize] as char);
            }
        }
    }
    result
}

fn create_mooncake_table(dst: &str, dst_uri: &str, src: &str, src_uri: &str) {
    let tls_connector = TlsConnector::new().expect("error creating tls connector");
    let make_tls_connector = MakeTlsConnector::new(tls_connector);
    let mut client = Client::connect(src_uri, make_tls_connector.clone())
        .unwrap_or_else(|_| panic!("error connecting to server: {src_uri}"));

    let get_columns_query = format!(
        "SELECT string_agg(
                format(
                '%I %s%s',
                attname,
                format_type(atttypid, atttypmod),
                CASE WHEN attnotnull THEN ' NOT NULL' ELSE '' END
            ),
            ', ' ORDER BY attnum
        )
        FROM pg_attribute
        WHERE attrelid = '{}'::regclass::oid AND attnum > 0 AND NOT attisdropped",
        src.replace("'", "''")
    );
    let columns: String = client
        .query_one(&get_columns_query, &[])
        .unwrap_or_else(|_| panic!("relation does not exist: {src}"))
        .get(0);

    if dst_uri != src_uri {
        client = Client::connect(dst_uri, make_tls_connector)
            .unwrap_or_else(|_| panic!("error connecting to server: {dst_uri}"));
    }

    let create_table_query = format!("CREATE TABLE {dst} ({columns}) USING mooncake");
    client
        .simple_query(&create_table_query)
        .unwrap_or_else(|_| panic!("error creating table: {dst}"));
}
