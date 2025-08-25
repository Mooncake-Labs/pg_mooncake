use crate::utils::{block_on, get_stream, return_stream, DATABASE};
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
    let dst = parse_table(dst);
    let lsn = unsafe { pgrx::pg_sys::XactLastCommitEnd };
    let mut stream = get_stream();
    block_on(moonlink_rpc::create_snapshot(
        &mut stream,
        DATABASE.clone(),
        dst,
        lsn,
    ))
    .expect("create_snapshot failed");
    return_stream(stream);
}

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.create_table(dst text, src text, src_uri text DEFAULT NULL, table_config json DEFAULT NULL) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn create_table(dst: &str, src: &str, src_uri: Option<&str>, table_config: Option<&str>) {
    let dst = parse_table(dst);
    let src = parse_table(src);
    let dst_uri = get_loopback_uri();
    let src_uri = src_uri.unwrap_or(&dst_uri).to_owned();
    create_mooncake_table(&dst, &dst_uri, &src, &src_uri);
    let table_config = table_config.unwrap_or("{}").to_owned();
    let mut stream = get_stream();
    block_on(moonlink_rpc::create_table(
        &mut stream,
        DATABASE.clone(),
        dst,
        src,
        src_uri,
        table_config,
    ))
    .expect("create_table failed");
    return_stream(stream);
}

#[pg_extern(sql = "
CREATE FUNCTION mooncake_drop_trigger() RETURNS event_trigger LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE EVENT TRIGGER mooncake_drop_trigger ON sql_drop EXECUTE FUNCTION mooncake_drop_trigger();
")]
fn drop_trigger() {
    Spi::connect(|client| {
        let get_dropped_tables_query =
            "SELECT quote_ident(schema_name) || '.' || quote_ident(object_name) FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table'";
        let dropped_tables = client
            .select(get_dropped_tables_query, None, &[])
            .expect("error reading dropped objects");
        for dropped_table in dropped_tables {
            let table: String = dropped_table
                .get(1)
                .expect("error reading dropped table")
                .expect("error reading dropped table");
            {
                let table = table.clone();
                pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::PreCommit, move || {
                    let mut stream = get_stream();
                    block_on(moonlink_rpc::drop_table(
                        &mut stream,
                        DATABASE.clone(),
                        table,
                    ))
                    .expect("drop_table failed");
                    return_stream(stream);
                });
            }
            pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::ParallelPreCommit, move || {
                let mut stream = get_stream();
                block_on(moonlink_rpc::drop_table(
                    &mut stream,
                    DATABASE.clone(),
                    table,
                ))
                .expect("drop_table failed");
                return_stream(stream);
            });
        }
    });
}

#[pg_extern(sql = "
CREATE FUNCTION mooncake.list_tables() RETURNS TABLE (
    \"table\" text,
    cardinality bigint,
    commit_lsn pg_lsn,
    flush_lsn pg_lsn,
    iceberg_warehouse_location text
) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn list_tables() -> TableIterator<
    'static,
    (
        name!(table, String),
        name!(cardinality, i64),
        name!(commit_lsn, i64),
        name!(flush_lsn, Option<i64>),
        name!(iceberg_warehouse_location, String),
    ),
> {
    let mut stream = get_stream();
    let tables = block_on(moonlink_rpc::list_tables(&mut stream)).expect("list_tables failed");
    return_stream(stream);
    TableIterator::new(
        tables
            .into_iter()
            .filter(|table| table.database == *DATABASE)
            .map(|table| {
                (
                    table.table,
                    table.cardinality as i64,
                    table.commit_lsn as i64,
                    table.flush_lsn.map(|lsn| lsn as i64),
                    table.iceberg_warehouse_location,
                )
            }),
    )
}

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.load_files(dst text, files text[]) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn load_files(dst: &str, files: Vec<String>) {
    let dst = parse_table(dst);
    let mut stream = get_stream();
    block_on(moonlink_rpc::load_files(
        &mut stream,
        DATABASE.clone(),
        dst,
        files,
    ))
    .expect("load_files failed");
    return_stream(stream);
}

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.optimize_table(dst text, mode text) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn optimize_table(dst: &str, mode: &str) {
    let dst = parse_table(dst);
    let mut stream = get_stream();
    block_on(moonlink_rpc::optimize_table(
        &mut stream,
        DATABASE.clone(),
        dst,
        mode.to_owned(),
    ))
    .expect("optimize_table failed");
    return_stream(stream);
}

fn parse_table(table: &str) -> String {
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    let ident = r#"([\w$]+|"([^"]|"")+")"#;
    let pattern = format!(r#"^((?<schema>{ident})\.)?(?<table>{ident})$"#);
    let re = Regex::new(&pattern).unwrap();
    let caps = re
        .captures(table)
        .unwrap_or_else(|| panic!("invalid input: {table}"));
    let schema = caps.name("schema").map_or_else(
        || {
            let schema: &CStr =
                unsafe { direct_function_call(pg_sys::current_schema, &[]).unwrap() };
            schema.to_str().unwrap()
        },
        |m| m.as_str(),
    );
    spi::quote_qualified_identifier(schema, &caps["table"])
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
