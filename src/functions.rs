use crate::drop_tracking::queue_drop;
use crate::utils::{rpc, DATABASE};
use core::ffi::CStr;
use native_tls::TlsConnector;
use pgrx::{direct_function_call, prelude::*};
use postgres::config::Host;
use postgres::Client;
use postgres_native_tls::MakeTlsConnector;

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
    let mut client = create_mooncake_table(&dst.quoted(), &dst_uri, &src.quoted(), &src_uri);
    let created_oid = relation_oid(&mut client, &dst.quoted());
    let table_config = table_config.unwrap_or("{}").to_owned();
    let result = remove_orphan(&dst).and_then(|cleanup_error| {
        rpc!(
            moonlink_rpc::create_table,
            DATABASE.clone(),
            dst.moonlink_name(),
            src.moonlink_name(),
            src_uri,
            table_config
        )
        .map_err(|e| match cleanup_error {
            Some(cleanup_error) => format!("{e} (removing it first failed: {cleanup_error})"),
            None => e.to_string(),
        })
    });
    if let Err(e) = result {
        // Otherwise the new mirror table stays readable while moonlink has no table, or a stale one, behind it. The
        // table is created on a separate connection, so another session may have replaced it by now: drop it only if
        // the name still refers to the table this call created.
        if created_oid.is_some() && relation_oid(&mut client, &dst.quoted()) == created_oid {
            if let Err(drop_error) = client.simple_query(&format!("DROP TABLE {}", dst.quoted())) {
                warning!(
                    "could not drop {} after the failed create_table: {drop_error}",
                    dst.quoted()
                );
            }
        } else {
            warning!(
                "did not drop {} after the failed create_table: it is not the table this call created",
                dst.quoted()
            );
        }
        error!("mooncake.create_table failed: {e}");
    }
}

/// The Postgres mirror table was just created, so its name was free: anything moonlink still has registered under
/// it was left behind by a DROP that moonlink never saw (moonlink was down, `DROP EXTENSION ... CASCADE`, or
/// `session_replication_role = replica`). Re-registering would fail, or leave the new table reading the old one.
///
/// Fails only when moonlink is unreachable. A server-side error is returned as `Ok(Some(error))` for the caller to
/// mention if `CreateTable` fails too: moonlink refuses to register a key that is still taken, so `CreateTable` then
/// decides, and an orphan can never end up behind the new table.
fn remove_orphan(dst: &TableName) -> Result<Option<String>, String> {
    match rpc!(
        moonlink_rpc::drop_table,
        DATABASE.clone(),
        dst.moonlink_name()
    ) {
        Ok(_) => Ok(None),
        Err(e) if e.is_unreachable() => Err(e.to_string()),
        Err(e) => Ok(Some(e.to_string())),
    }
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
            queue_drop(pg_sys::Oid::from(relid as u32));
        }
    });
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

pub(crate) struct TableName {
    pub(crate) schema: String,
    pub(crate) table: String,
}

impl TableName {
    /// Unquoted `schema.table`: the key moonlink registers the table under. duckdb_mooncake looks tables up
    /// by unquoted `schema.table` too, and moonlink's Postgres connector splits source names on '.'.
    pub(crate) fn moonlink_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Quoted form for SQL that pg_mooncake runs itself.
    fn quoted(&self) -> String {
        spi::quote_qualified_identifier(&self.schema, &self.table)
    }
}

/// Parses a possibly qualified table name with Postgres' identifier rules: unquoted names are downcased (ASCII
/// only, as Postgres does in multibyte encodings), quoted ones keep their case, and each part is truncated to
/// NAMEDATALEN - 1 bytes. An unqualified name gets the current schema.
fn parse_table(input: &str) -> TableName {
    let parts: Vec<String> = unsafe {
        direct_function_call(
            pg_sys::parse_ident,
            &[input.into_datum(), true.into_datum()],
        )
    }
    .unwrap_or_else(|| error!("invalid table name: {input}"));
    let mut parts = parts.into_iter().map(truncate_identifier);
    let (schema, table) = match (parts.next(), parts.next(), parts.next()) {
        (Some(table), None, None) => {
            let schema: &CStr =
                unsafe { direct_function_call(pg_sys::current_schema, &[]).unwrap() };
            (schema.to_str().unwrap().to_owned(), table)
        }
        (Some(schema), Some(table), None) => (schema, table),
        _ => error!("invalid table name, expected [schema.]table: {input}"),
    };
    // moonlink splits source names on '.'.
    if schema.contains('.') || table.contains('.') {
        error!("identifiers containing '.' are not supported: {input}");
    }
    TableName { schema, table }
}

/// Same as Postgres' truncate_identifier(): clip to NAMEDATALEN - 1 bytes on a character boundary.
fn truncate_identifier(ident: String) -> String {
    let max = pg_sys::NAMEDATALEN as usize - 1;
    if ident.len() <= max {
        return ident;
    }
    let len =
        unsafe { pg_sys::pg_mbcliplen(ident.as_ptr().cast(), ident.len() as i32, max as i32) };
    let truncated = ident[..len as usize].to_owned();
    notice!("identifier \"{ident}\" will be truncated to \"{truncated}\"");
    truncated
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
    // Percent-encodes all but RFC 3986's unreserved characters (https://www.rfc-editor.org/rfc/rfc3986#section-2.3).
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

/// The oid `quoted_name` currently refers to on `client`'s server, if any.
fn relation_oid(client: &mut Client, quoted_name: &str) -> Option<i64> {
    client
        .query_one("SELECT to_regclass($1)::oid::int8", &[&quoted_name])
        .ok()
        .and_then(|row| row.get(0))
}

/// Creates the Postgres mirror table and returns the connection it was created on.
fn create_mooncake_table(dst: &str, dst_uri: &str, src: &str, src_uri: &str) -> Client {
    let tls_connector = TlsConnector::new()
        .unwrap_or_else(|e| error!("mooncake.create_table failed: could not set up TLS: {e}"));
    let make_tls_connector = MakeTlsConnector::new(tls_connector);
    let connect = |uri: &str, server: &str| {
        Client::connect(uri, make_tls_connector.clone()).unwrap_or_else(|e| {
            error!(
                "mooncake.create_table failed: could not connect to {}: {}",
                describe_server(uri, server),
                libpq_cause(&e)
            )
        })
    };
    let mut client = connect(src_uri, "the source server");

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
        .unwrap_or_else(|e| {
            error!(
                "mooncake.create_table failed: could not read the columns of {src}: {}",
                libpq_cause(&e)
            )
        })
        .get(0);

    if dst_uri != src_uri {
        client = connect(dst_uri, "this server");
    }

    let create_table_query = format!("CREATE TABLE {dst} ({columns}) USING mooncake");
    client
        .simple_query(&create_table_query)
        .unwrap_or_else(|e| {
            error!(
                "mooncake.create_table failed: could not create {dst}: {}",
                libpq_cause(&e)
            )
        });
    client
}

/// The server's message for an error the server reported, else the client-side error.
fn libpq_cause(e: &postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => db.message().to_owned(),
        None => e.to_string(),
    }
}

/// `server`, with the host, port, database and user that `uri` names. The connection string itself is never shown: it
/// may contain a password.
fn describe_server(uri: &str, server: &str) -> String {
    let Ok(config) = uri.parse::<postgres::Config>() else {
        return server.to_owned();
    };
    let join = |parts: Vec<String>| parts.join(",");
    let mut fields = Vec::new();
    let hosts = config
        .get_hosts()
        .iter()
        .map(|host| match host {
            Host::Tcp(host) => host.clone(),
            Host::Unix(path) => path.display().to_string(),
        })
        .collect::<Vec<_>>();
    if !hosts.is_empty() {
        fields.push(format!("host={}", join(hosts)));
    }
    let ports = config
        .get_ports()
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>();
    if !ports.is_empty() {
        fields.push(format!("port={}", join(ports)));
    }
    if let Some(dbname) = config.get_dbname() {
        fields.push(format!("dbname={dbname}"));
    }
    if let Some(user) = config.get_user() {
        fields.push(format!("user={user}"));
    }
    if fields.is_empty() {
        server.to_owned()
    } else {
        format!("{server} ({})", fields.join(" "))
    }
}
