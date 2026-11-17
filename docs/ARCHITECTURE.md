# pg_mooncake architecture

**Submodule pins.** moonlink `37b040f9`, pg_duckdb `v1.0.0-mooncake.1` (Mooncake-Labs/pg_duckdb), duckdb_mooncake
`bd9e5f3`, on DuckDB v1.4.1. This is the only place the docs list them. Statements tagged **[pins]**, here and in
[AGENTS.md](../AGENTS.md), hold at these pins and must be re-verified when a pin moves.

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
            │   Unix RPC server  <mooncake.data_directory>/moonlink.sock                │
            │   per src_uri:  logical replication client (slot moonlink_slot_<db>,     │
            │                 publication moonlink_pub) ◄── pgoutput from the source    │
            │   per mirror:   table handler → mem buffers → parquet + index files       │
            │                 → Iceberg snapshots (filesystem catalog) → files ─────────┘
            │                 (no WAL: the slot is acknowledged up to what Iceberg holds) │
            └─────────────────────────────────────────────────────────────────────────┘
```

1. **`CREATE EXTENSION pg_mooncake CASCADE`** creates schema `mooncake`, the procedures/functions, the `mooncake`
   table access method, the `sql_drop` event trigger, and installs the DuckDB `mooncake` extension
   (`duckdb.install_extension('mooncake','community')`, a prebuilt download by default).
2. **`_PG_init`** (needs `shared_preload_libraries`, after `pg_duckdb`) reads `mooncake.data_directory` and, in the
   postmaster, checks it (`src/data_directory.rs`). It registers the `moonlink` background worker (cargo feature
   `bgworker`; start when recovery finishes, so not on standbys but on promotion; restart after 15 s on non-zero exit)
   and registers the `mooncake` AM with pg_duckdb (`RegisterDuckdbTableAm`).
3. **The worker** runs `moonlink_service::start_with_config` with `base_path` = `mooncake.data_directory` (default
   `pg_mooncake`, relative to `$PGDATA`):
   sqlite metadata store, then recovery of every registered table from its latest Iceberg snapshot. A mirror with an
   errored marker stays errored; one that fails recovery is errored (or retried, for a transient failure). Replication
   then starts from each slot's confirmed position, so changes after the last snapshot are replayed from the slot. Then
   the Unix-socket RPC server. Logs go to stdout/stderr, which ends up in the Postgres log.
4. **`mooncake.create_table(dst, src, src_uri, table_config)`** connects with libpq to `src_uri` (default: loopback via
   the first `unix_socket_directories`) and reads `src`'s columns. It then runs `CREATE TABLE dst (…) USING mooncake` on a
   **separate, autocommitted connection** (outside the caller's transaction), then sends RPC `CreateTable`. moonlink rejects
   unsupported column types (and partitioned sources without a primary key), sets `REPLICA IDENTITY FULL` on `src` and
   each of its partitions, adds `src` to the publication, copies an initial snapshot (staged under `initial_copy/`), and
   streams changes from the slot. It answers `CreateTable` only once the copy is persisted to Iceberg and the mirror is
   then recorded in its sqlite metadata store. **[pins]**
5. **A query on `dst`**: pg_duckdb sends it to DuckDB. On load, duckdb_mooncake runs `pgmooncake_get_init_query`
   (`ATTACH … TYPE mooncake …`). Table lookup does RPC `GetTableSchema`. The scan does RPC
   `ScanTableBegin(lsn = this backend's XactLastCommitEnd)`, then DuckDB's parquet reader reads the returned data files,
   applies the deletion vectors and positional deletes, and finally sends `ScanTableEnd`. Rows not yet flushed are
   materialized as temp parquet files for the duration of the scan.
6. **Dropping a mirror**: an `object_access_hook` notes that the dropped relation is a mirror, then the `sql_drop`
   event trigger queues it, and a pre-commit transaction callback sends RPC `DropTable` if the drop is still in effect
   (one 30 s deadline for all drops of the transaction). A failure becomes a WARNING and the DROP still commits
   (invariants: [AGENTS.md](../AGENTS.md#2-other-invariants), section 2, item 5).
   moonlink removes the table's files, its sqlite row, and the source's publication membership. Replication
   connections are keyed by `src_uri`: once the last mirror of a `src_uri` is gone, moonlink drops that slot and
   publication.

The mirror's own PG table (`USING mooncake`) holds no data. Its AM callbacks are `unimplemented!()` except the ones
needed for DDL and planning, so direct DML on a mirror errors out.

## Wire format (moonlink_rpc), shared with the prebuilt duckdb_mooncake

The invariants that keep it compatible are in [AGENTS.md](../AGENTS.md#1-contract-with-duckdb_mooncake-never-change-these),
section 1.

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
| 6 | LoadFiles | database, table, files | always an "unsupported" error **[pins]** | nobody: no SQL procedure exists in 0.3.0; the variant is kept for the wire order |
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

## On-disk state (`mooncake.data_directory`, default `$PGDATA/pg_mooncake`) **[pins]**

| Path | Contents |
|---|---|
| `moonlink_metadata_store.sqlite` | Registered tables + persisted table config: the recovery source of truth |
| `<db>.<schema>.<table>/` | moonlink data files (`data-*.parquet`) and file indexes (`index_block_*.bin`) |
| `<db>/<schema>.<table>/{data,metadata}` | Iceberg table (filesystem catalog) |
| `<db>/indicator.text` | Namespace marker of the filesystem Iceberg catalog |
| `errored_tables/` | One marker per errored mirror with its message, so the errored state survives restarts |
| `initial_copy/table_<oid>/` | Staging for a mirror's initial copy |
| `source_fingerprints/` | Per mirror, the source table's oid, columns and partitions when the mirror was created; compared at recovery and polled while replicating, and any difference errors the mirror |
| `.moonlink_atomic_tmp/` | Temporary files of atomic writes |
| `moonlink.sock` | The RPC socket |
| `moonlink.lock` | Held (`flock`) by the postmaster that uses the directory; records its PID and data directory. Stays after shutdown |
| `temp/`, `read_through_cache/` | Scratch space; recreated at worker start |

## Consistency model

- Within a session, a read after a commit sees that commit: the scan waits for `XactLastCommitEnd`.
- Other sessions see changes once moonlink applies them: about 0.5 s for small transactions, about 1–2 s after a
  200k-row transaction, measured locally.
- **[pins]** moonlink keeps no write-ahead log. The replication slot's `confirmed_flush_lsn` is the minimum, over the
  healthy mirrors of that source, of the LSN each has persisted to Iceberg (`compute_confirmed_flush_lsn` in
  `moonlink_connectors/src/pg_replicate.rs`). Errored mirrors don't hold it back. Postgres therefore keeps WAL in
  `pg_wal` until every healthy mirror has a newer Iceberg snapshot; idle mirrors are snapshotted at least every 300 s
  (`table_handler_timer.rs`).
- **[pins]** Restart recovery = sqlite registry + latest Iceberg snapshot per mirror + replay from the slot.
- **[pins]** A mirror that can't be kept in sync enters the errored state (persisted under `errored_tables/`): scans
  fail with `mirror <name> is out of sync: <reason>; drop and re-create it with mooncake.create_table`, and other
  mirrors keep replicating.

## Build graph

| Artifact | Built from | Notes |
|---|---|---|
| `pg_duckdb.so`, `libduckdb.so` | `make pg_duckdb` | DuckDB from `pg_duckdb/third_party/duckdb` with json, icu, httpfs (httpfs fetched by git) |
| `mooncake.duckdb_extension` | `make duckdb_mooncake` | Second DuckDB v1.4.1 tree; `moonlink_ffi` staticlib via Corrosion; nanoarrow; CRoaring ≥ 1.x |
| `pg_mooncake.so` | `make install` (`cargo pgrx install --release`) | Links moonlink through `moonlink_service`. **[pins]** Default features → `storage-fs`, sqlite metadata. Release profile is fat LTO |
