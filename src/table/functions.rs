use crate::pgmoonlink;
use core::ffi::CStr;
use pgrx::{direct_function_call, prelude::*};
use postgres::{Client, NoTls};
use regex::Regex;

extern "C" {
    fn GetActiveLsn() -> u64;
}

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.create_table(dst TEXT, src TEXT, src_uri TEXT DEFAULT NULL) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn create_table(dst: &str, src: &str, src_uri: Option<&str>) {
    let dst = parse_table(dst);
    let src = parse_table(src);
    let dst_uri = get_loopback_uri();
    let src_uri = src_uri.unwrap_or(&dst_uri).to_owned();
    let database_id = unsafe { pg_sys::MyDatabaseId.to_u32() };
    let table_id = create_mooncake_table(&dst, &dst_uri, &src, &src_uri);
    pgmoonlink::create_table(database_id, table_id, src, src_uri);
}

#[pg_extern(sql = "
CREATE FUNCTION mooncake_drop_trigger() RETURNS event_trigger LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE EVENT TRIGGER mooncake_drop_trigger ON sql_drop EXECUTE FUNCTION mooncake_drop_trigger();
")]
fn drop_trigger() {
    Spi::connect(|client| {
        let database_id = unsafe { pg_sys::MyDatabaseId.to_u32() };
        let get_dropped_tables_query =
            "SELECT objid FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table'";
        let dropped_tables = client
            .select(get_dropped_tables_query, None, &[])
            .expect("error reading dropped objects");
        for dropped_table in dropped_tables {
            let table_id = dropped_table
                .get::<pg_sys::Oid>(1)
                .expect("error reading dropped object")
                .expect("error reading dropped object")
                .to_u32();
            let callback = move || {
                pgmoonlink::drop_table(database_id, table_id);
            };
            pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::PreCommit, callback);
            pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::ParallelPreCommit, callback);
        }
    });
}

#[pg_extern(sql = "
CREATE PROCEDURE mooncake.create_snapshot(dst TEXT) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
")]
fn create_snapshot(dst: &str) {
    let dst = parse_table(dst);
    let dst_uri = get_loopback_uri();
    let database_id = unsafe { pg_sys::MyDatabaseId.to_u32() };
    let mut client = Client::connect(&dst_uri, NoTls)
        .unwrap_or_else(|_| panic!("error connecting to server: {dst_uri}"));
    let get_table_id_query = format!("SELECT '{}'::regclass::oid", dst.replace("'", "''"));
    let table_id: u32 = client
        .query_one(&get_table_id_query, &[])
        .unwrap_or_else(|_| panic!("relation does not exist: {dst}"))
        .get(0);
    let lsn = unsafe { GetActiveLsn() };
    pgmoonlink::create_snapshot(database_id, table_id, lsn);
}

fn get_loopback_uri() -> String {
    let hosts = unsafe { CStr::from_ptr(pg_sys::Unix_socket_directories) };
    let host = hosts.to_str().unwrap().split(", ").next().unwrap();
    let port: i32 = unsafe { pg_sys::PostPortNumber };
    let database: &CStr = unsafe { direct_function_call(pg_sys::current_database, &[]).unwrap() };
    let database = database.to_str().unwrap();
    let user = unsafe { CStr::from_ptr(pg_sys::GetUserNameFromId(pg_sys::GetUserId(), false)) };
    let user = user.to_str().unwrap();
    format!(
        "postgresql:///{}?host={}&port={port}&user={}",
        uri_encode(database),
        uri_encode(host),
        uri_encode(user)
    )
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

fn create_mooncake_table(dst: &str, dst_uri: &str, src: &str, src_uri: &str) -> u32 {
    let mut client = Client::connect(src_uri, NoTls)
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
        client = Client::connect(dst_uri, NoTls)
            .unwrap_or_else(|_| panic!("error connecting to server: {dst_uri}"));
    }

    let create_table_query = format!("CREATE TABLE {dst} ({columns}) USING mooncake");
    client
        .simple_query(&create_table_query)
        .unwrap_or_else(|_| panic!("error creating table: {dst}"));

    let get_table_id_query = format!("SELECT '{}'::regclass::oid", dst.replace("'", "''"));
    client
        .query_one(&get_table_id_query, &[])
        .unwrap_or_else(|_| panic!("relation does not exist: {dst}"))
        .get(0)
}
