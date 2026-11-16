# pg_mooncake architecture

**[pins]** Describes pg_mooncake 0.3.0 (unreleased) with moonlink `c1a99ef`, pg_duckdb `12b6de7` (`7daa8e5` plus the upstream TimeZone hook fix), duckdb_mooncake `bd9e5f3`, and
DuckDB v1.4.1. Statements tagged **[pins]** must be re-verified when a submodule pin moves.

## Processes and data flow

```
            ┌──────────────────────── one Postgres instance ─────────────────────────┐
  client ──►│ backend (pg_mooncake.so + pg_duckdb.so + embedded DuckDB)               │
            │   CALL mooncake.create_table / drop / optimize / snapshot ──┐           │
            │   SELECT … FROM <mirror> → pg_duckdb → DuckDB catalog       │ RPC       │
            │        "mooncake" (duckdb_mooncake ext) ─── RPC ───────────┤ (Unix     │
            │        reads parquet + puffin files directly ◄───┐          │  socket)  │
            │                                                   │          ▼           │
            │ bgworker "moonlink" (pg_mooncake.so → moonlink_service)                 │
            │   Unix RPC server  $PGDATA/pg_mooncake/moonlink.sock                      │
            │   per src_uri:  logical replication client (slot moonlink_slot_<db>,     │
            │                 publication moonlink_pub) ◄── pgoutput from the source    │
            │   per mirror:   table handler → mem buffers → parquet + index files       │
            │                 → Iceberg snapshots (filesystem catalog) → files ─────────┘
            └─────────────────────────────────────────────────────────────────────────┘
```

1. **`CREATE EXTENSION pg_mooncake CASCADE`** creates schema `mooncake`, the procedures/functions, the `mooncake`
   table access method, the `sql_drop` event trigger, and installs the DuckDB `mooncake` extension
   (`duckdb.install_extension('mooncake','community')`, a prebuilt download by default).
2. **`_PG_init`** (needs `shared_preload_libraries`, after `pg_duckdb`) registers the `moonlink` background worker
   (cargo feature `bgworker`; start at consistent state; restart after 15 s on non-zero exit) and registers the `mooncake`
   AM with pg_duckdb (`RegisterDuckdbTableAm`).
3. **The worker** runs `moonlink_service::start_with_config` with `base_path = "pg_mooncake"` (relative to `$PGDATA`):
   sqlite metadata store, recovery of every registered table, then the Unix-socket RPC server. Logs go to stdout/stderr,
   which ends up in the Postgres log.
4. **`mooncake.create_table(dst, src, src_uri, table_config)`** connects with libpq to `src_uri` (default: loopback via
   the first `unix_socket_directories`) and reads `src`'s columns. It then runs `CREATE TABLE dst (…) USING mooncake` on a
   **separate, autocommitted connection** (outside the caller's transaction), then sends RPC `CreateTable`. moonlink adds
   `src` to the publication, copies an initial snapshot, and streams changes from the slot. **[pins]**
5. **A query on `dst`**: pg_duckdb sends it to DuckDB. On load, duckdb_mooncake runs `pgmooncake_get_init_query`
   (`ATTACH … TYPE mooncake …`). Table lookup does RPC `GetTableSchema`. The scan does RPC
   `ScanTableBegin(lsn = this backend's XactLastCommitEnd)`, then DuckDB's parquet reader reads the returned data files,
   applies the deletion vectors and positional deletes, and finally sends `ScanTableEnd`. Rows not yet flushed are
   materialized as temp parquet files for the duration of the scan.
6. **Dropping a mirror**: an `object_access_hook` notes that the dropped relation is a mirror, then the `sql_drop`
   event trigger queues it, and a pre-commit transaction callback sends RPC `DropTable` if the drop is still in effect
   (one 30 s deadline for all drops of the transaction). A failure becomes a WARNING and the DROP still commits.
   moonlink removes the table's files, its sqlite row, and the source's publication membership. Replication connections are keyed by `src_uri`: once
   the last mirror of a `src_uri` is gone, moonlink drops that slot and publication.

The mirror's own PG table (`USING mooncake`) holds no data. Its AM callbacks are `unimplemented!()` except the ones
needed for DDL and planning, so direct DML on a mirror errors out.

## Wire format (moonlink_rpc), shared with the prebuilt duckdb_mooncake

Framing: u32 native-endian length + bincode `config::standard()` payload. Request = enum `Request` (variant **index**
on the wire). Response = `Result<T, moonlink_error::ErrorStruct>`.

| idx | Request | Fields | Response | Sent by |
|---|---|---|---|---|
| 0 | CreateSnapshot | database, table, lsn: u64 | () | pg_mooncake |
| 1 | CreateTable | database, table, src, src_uri, table_config (JSON string) | () | pg_mooncake |
| 2 | DropTable | database, table | () | pg_mooncake (pre-commit callback) |
| 3 | GetParquetMetadatas | data_files | Vec<Vec<u8>> | nobody |
| 4 | GetTableSchema | database, table | Arrow IPC stream (schema only) | duckdb_mooncake |
| 5 | ListTables | – | Vec<Table{database, table, cardinality, commit_lsn, flush_lsn: Option, iceberg_warehouse_location}> | pg_mooncake |
| 6 | LoadFiles | database, table, files | () (backend no-op) **[pins]** | nobody (the SQL procedure was removed) |
| 7 | OptimizeTable | database, table, mode (`data`/`index`/`full`) | () | pg_mooncake |
| 8 | ScanTableBegin | database, table, lsn: u64 | Vec<u8> (layout below) | duckdb_mooncake |
| 9 | ScanTableEnd | database, table | () | duckdb_mooncake |

`table` is the unquoted `schema.table` string. The server keeps one `ReadState` per (database, table) per connection
between `ScanTableBegin` and `ScanTableEnd`.

`ScanTableBegin` payload (all integers u32, native endian):
```
n_data_files, data_file_offsets[n_data_files+1]
n_puffin_files, puffin_file_offsets[n_puffin_files+1]
n_dv, {data_file_number, puffin_file_number, offset, size}[n_dv]     sorted by data_file_number
n_pos, {data_file_number, data_file_row_number}[n_pos]              sorted
data file paths (concatenated bytes), puffin file paths (concatenated bytes)
```

## On-disk state (`$PGDATA/pg_mooncake`) **[pins]**

| Path | Contents |
|---|---|
| `moonlink_metadata_store.sqlite` | Registered tables + persisted table config: the recovery source of truth |
| `<db>.<schema>.<table>/` | moonlink data files (`data-*.parquet`) and file indexes (`index_block_*.bin`) |
| `<db>/<schema>.<table>/{data,metadata}` | Iceberg table (filesystem catalog) |
| `_wal/<db>.<schema>.<table>/` | moonlink's own WAL (`wal_<N>.json`, `metadata_wal.json`) |
| `temp/`, `read_through_cache/` | Scratch space; recreated at worker start |

## Consistency model

- Within a session, a read after a commit sees that commit: the scan waits for `XactLastCommitEnd`.
- Other sessions see changes once moonlink applies them: about 0.5 s for small transactions, about 1–2 s after a
  200k-row transaction, measured locally.
- **[pins]** The replication slot's `confirmed_flush_lsn` follows moonlink's own WAL flush LSN, falling back to the
  Iceberg flush LSN per table, min over tables (`compute_confirmed_wal_flush_lsn` in
  `moonlink_connectors/src/pg_replicate.rs`).
- **[pins]** Restart recovery = sqlite registry + latest Iceberg snapshot + moonlink WAL replay + slot catch-up.

## Build graph

| Artifact | Built from | Notes |
|---|---|---|
| `pg_duckdb.so`, `libduckdb.so` | `make pg_duckdb` | DuckDB from `pg_duckdb/third_party/duckdb` with json, icu, httpfs (httpfs fetched by git) |
| `mooncake.duckdb_extension` | `make duckdb_mooncake` | Second DuckDB v1.4.1 tree; `moonlink_ffi` staticlib via Corrosion; nanoarrow; CRoaring ≥ 1.x |
| `pg_mooncake.so` | `make install` (`cargo pgrx install --release`) | Links moonlink through `moonlink_service`. **[pins]** Default features → `storage-fs`, sqlite metadata. Release profile is fat LTO |
