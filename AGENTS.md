# AGENTS.md: working on pg_mooncake

Guidance for AI coding agents and contributors. Architecture and the full wire format: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Statements tagged **[pins]** describe the current submodule pins (moonlink `c1a99ef`, pg_duckdb `7daa8e5`,
duckdb_mooncake `bd9e5f3`). Re-verify them whenever a submodule pin moves.

## 1. Contract with duckdb_mooncake: never change these

Users get duckdb_mooncake as a **prebuilt binary**: `CREATE EXTENSION` runs
`duckdb.install_extension('mooncake', 'community')` (`src/sql/bootstrap.sql`). That binary was built against
`moonlink_rpc` at the commit locked in `duckdb_mooncake/moonlink_ffi/Cargo.lock` (git dependency on the moonlink repo).
No version is negotiated at runtime, so anything below that changes breaks every user on the default install, even
when your local tests (with a locally built extension) pass.

- **Do not modify `moonlink/src/moonlink_rpc` at all.** The `rpcs!{}` list is the wire format: bincode
  `config::standard()`, where the enum *variant index* is encoded, so order matters. The list happens to be
  alphabetical, so adding a variant "in order" silently renumbers the ones after it.
- **Framing** (`moonlink_rpc/src/lib.rs` `write`/`read`): u32 **native-endian** length prefix, then the bincode payload.
- **Responses are `RpcResult<T> = Result<T, moonlink_error::ErrorStruct>`.** Do not change `ErrorStruct`
  (`moonlink/src/moonlink_error/src/error.rs`: `message`, `status: ErrorStatus`, `source` with custom serde, `location`)
  or `ErrorStatus`. The prebuilt binary decodes every error reply through them.
- **`GetTableSchema`** returns an Arrow IPC *stream* with only the schema (`arrow_ipc::writer::StreamWriter` in
  `moonlink_service/src/rpc_server.rs`), which duckdb_mooncake decodes with nanoarrow.
- **`ScanTableBegin` payload** (`moonlink/src/moonlink_table_metadata/src/table_metadata.rs` `Encode`): u32
  native-endian arrays that `duckdb_mooncake/src/storage/mooncake_table_metadata.cpp` reinterprets in place. Puffin
  deletion-vector blobs are read as portable CRoaring at `offset+20`, length `size-24`. Every file named in the payload
  must stay readable until `ScanTableEnd`.
- **C symbols that duckdb_mooncake `dlsym`s from `pg_mooncake.so`** (`duckdb_mooncake/src/include/pgmooncake.hpp`):
  `pgmooncake_get_init_query`, `pgmooncake_get_lsn`, `pgmooncake_drop_cstring` (`src/duckdb_mooncake.rs`).
  `pgmooncake_drop_cstring` has no Rust caller, but it is not dead code: removing it makes the extension call a
  NULL function pointer.
- **ATTACH string and naming**: `ATTACH DATABASE 'mooncake' (TYPE mooncake, URI 'pg_mooncake/moonlink.sock', DATABASE '<db>')`.
  The socket path is relative to the backend's cwd (`$PGDATA`). Tables are addressed as **unquoted** `schema.table`
  (`format!("{}.{}", schema, table)` in `duckdb_mooncake/moonlink_ffi/src/lib.rs`), so pg_mooncake must register
  mirrors with moonlink under that exact string.

## 2. Other invariants

1. **The moonlink background worker must not panic or error out.** **[pins]** Restart behavior:
   - A panic in a spawned tokio task (per-table handler, per-connection RPC handler) does not exit the worker.
     It surfaces as `early eof` on that connection or as a mirror that silently stops ingesting.
   - An error or panic on the main path exits with status 1 and the worker restarts after 15 s
     (`src/bgworker.rs`: `start_with_config(config).await.unwrap()`). That includes a recovery error, so a table
     that fails recovery crash-loops the worker every 15 s.
   - A clean exit (e.g. the worker alone receives SIGTERM) exits with status 0, and Postgres never restarts it.
2. **`ServiceConfig` is built with a struct literal** in `src/bgworker.rs`. Adding or removing a field in
   `moonlink_service` needs a matching edit there. **[pins]**
3. **On this path moonlink uses only the sqlite metadata store** (`$PGDATA/pg_mooncake/moonlink_metadata_store.sqlite`,
   the recovery source of truth) **and the filesystem Iceberg catalog.** **[pins]**
4. **User-level recovery is "drop and re-create the mirror"** (`DROP TABLE <mirror>; CALL mooncake.create_table(...)`).
   DROP must succeed no matter what state moonlink is in:
   - An `object_access_hook` (`src/drop_tracking.rs`) records which dropped relations are mooncake mirrors (by
     `relam`, before the catalog rows go away; this covers cascades and `DROP SCHEMA`). The `sql_drop` trigger queues
     those, and a pre-commit transaction callback sends `DropTable` for each one that is still gone at top-level
     commit, so a drop undone by `ROLLBACK TO SAVEPOINT` or an exception block keeps the mirror. `PREPARE TRANSACTION`
     after dropping a mirror is an error. Dropping any other table never talks to moonlink.
   - If `DropTable` fails (moonlink down, erroring, or silent), the DROP still commits and emits a WARNING per mirror.
     All drops of a transaction share one 30 s deadline, and once moonlink is unreachable the remaining ones are not
     attempted. moonlink may then still hold the mirror (it shows up in `mooncake.list_tables()`).
   - moonlink also keeps a mirror whose table was dropped without the `sql_drop` trigger firing: by
     `DROP EXTENSION pg_mooncake CASCADE` (the trigger is dropped by the same command) or with
     `session_replication_role = replica` (event triggers don't fire). `mooncake.create_table` therefore drops whatever
     moonlink holds under the new mirror's name before registering it (the name was free in Postgres, so that table is
     an orphan), and drops the new Postgres table again if moonlink refuses it. So re-creating a mirror cleans up its
     orphan; to clean one up without re-creating: `CREATE TABLE <same name> (id int) USING mooncake; DROP TABLE <same name>;`.
   - pg_mooncake 0.2 registered mirrors under the quoted name (`public."Mc$1"`, `quote_ident` of each part). DROP and the
     create_table cleanup also send that key when it differs from the unquoted one (`TableName::moonlink_keys`).
   - RPC errors become normal Postgres ERRORs raised after the connection lock is released (`rpc!` in `src/utils.rs`).
     The lock is poison-tolerant. A connection that saw an IO or decode error is discarded, and a cached connection is
     probed (`MSG_PEEK`) before reuse, so the first call after a moonlink restart reconnects instead of failing.
     Never `.expect()`/`.unwrap()` an RPC result.

## 3. Components and where things live

| Path | Language | Role |
|---|---|---|
| `src/` | Rust (pgrx 0.16.1) | The extension: `mooncake.*` SQL API, `mooncake` table AM, the `moonlink` background worker |
| `moonlink/` (submodule) | Rust | Ingestion engine inside the worker: CDC from a logical slot, Iceberg writer, Unix-socket RPC server |
| `pg_duckdb/` (submodule) | C++ | Embeds DuckDB in each backend. Routes queries on `USING mooncake` tables to DuckDB |
| `duckdb_mooncake/` (submodule) | C++ + Rust FFI | DuckDB extension `mooncake`: gets a snapshot from moonlink, then reads the parquet/puffin files directly |

| What | Where |
|---|---|
| `_PG_init`, bootstrap SQL | `src/lib.rs`, `src/sql/bootstrap.sql` |
| `mooncake.create_table/create_snapshot/optimize_table/list_tables`, drop event trigger, identifier parsing | `src/functions.rs` |
| Which dropped relations are mirrors (`object_access_hook`) | `src/drop_tracking.rs` |
| `mooncake` table AM (mostly `unimplemented!()`) and its registration with pg_duckdb | `src/table.rs` |
| Background worker (`ServiceConfig`, `start_with_config`) | `src/bgworker.rs` |
| Per-backend RPC connection (`rpc!`), tokio runtime, `DATABASE` | `src/utils.rs` |
| C symbols for duckdb_mooncake | `src/duckdb_mooncake.rs` |
| RPC definitions and framing / server / service startup | `moonlink/src/moonlink_rpc/src/lib.rs`, `moonlink/src/moonlink_service/src/{rpc_server.rs,lib.rs}` |
| Backend API, `table_config` JSON, recovery | `moonlink/src/moonlink_backend/src/{lib.rs,table_config.rs,config_utils.rs,recovery_utils.rs}` |
| Postgres CDC (slot, publication, confirmed-LSN feedback) | `moonlink/src/moonlink_connectors/src/pg_replicate*.rs`, `pg_replicate/` |
| Table engine, WAL, Iceberg, union read **[pins]** | `moonlink/src/moonlink/src/{table_handler.rs,storage/,union_read/}` |
| DuckDB side: catalog, scan, payload decoding, `dlsym`s, RPC client | `duckdb_mooncake/src/storage/*.cpp`, `src/include/pgmooncake.hpp`, `src/moonlink/moonlink.cpp`, `moonlink_ffi/src/lib.rs` |
| Regression tests | `tests/pg_regress/{sql,expected}` |

## 4. Setup, build, test (standard toolchain)

There is no CI in this repo (`.github/` only has issue templates). The checks are `make test` (pg_regress) and the
end-to-end gate `scripts/e2e-smoke.sh`, which covers what pg_regress cannot: moonlink down or hung, restarts, crashes,
DDL and TRUNCATE on sources. See TESTING.md for both. Run the gate before changing the worker, the RPC client or DROP
handling.

```bash
git clone --recurse-submodules https://github.com/Mooncake-Labs/pg_mooncake.git && cd pg_mooncake
# Rust: rustup picks up rust-toolchain.toml (1.88.0, clippy, rustfmt). pgrx needs rustfmt at build time.
cargo install --locked cargo-pgrx@0.16.1
cargo pgrx init --pg17 download --pg18 download    # or --pgNN "$(which pg_config)" for an existing install
```
Build and install for one PG major (repeat with `pg17`):
```bash
export PG_VERSION=pg18
PG_BIN="$(dirname "$(cargo pgrx info pg-config $PG_VERSION)")"
make pg_duckdb PG_VERSION=$PG_VERSION                     # pg_duckdb + libduckdb into that PG
PATH="$PG_BIN:$PATH" make install PG_VERSION=$PG_VERSION  # see pitfall 1
```
Add this to the cluster's `postgresql.conf` (pgrx clusters live in `~/.pgrx/data-NN`):
```ini
shared_preload_libraries = 'pg_duckdb,pg_mooncake'   # this order: pg_mooncake needs pg_duckdb's symbols
wal_level = logical
duckdb.allow_community_extensions = true
```
Tests:
```bash
make test PG_VERSION=pg18     # cargo pgrx regress against ~/.pgrx/data-18; PATH does not matter here
make test PG_VERSION=pg17
cargo pgrx run pg18           # interactive psql on the pgrx cluster (`make run` = pg_duckdb + this, PG18)
```
`make test` installs a **debug** build of pg_mooncake. Reinstall with `make install` before measuring performance.

### Using a locally built duckdb_mooncake
Needed only when you change `duckdb_mooncake/`. Build it with `make duckdb_mooncake`. CRoaring ≥ 1.x must be findable by
CMake: the devcontainer uses vcpkg via `VCPKG_TOOLCHAIN_PATH`, and distro packages such as Ubuntu 24.04's
`libroaring-dev` 0.2.66 are too old. Corrosion and nanoarrow are fetched by CMake. Then copy
`duckdb_mooncake/build/release/extension/mooncake/mooncake.duckdb_extension` into
`<duckdb.extension_directory>/v1.4.1/linux_amd64/` **without** a `.info` file, which makes the bootstrap INSTALL a no-op,
and set `duckdb.allow_unsigned_extensions = true` and `duckdb.autoinstall_known_extensions = false`. Both DuckDB trees
(`pg_duckdb/third_party/duckdb`, `duckdb_mooncake/duckdb`) must be the same version (v1.4.1).

## 5. Pitfalls

1. **`cargo pgrx install` uses the first `pg_config` on `PATH`**, not the `PG_CONFIG` the Makefile exports. With another
   Postgres on `PATH` it compiles for the right major but installs into the wrong prefix.
2. **The `bgworker` cargo feature** (default) is what starts moonlink. A `--no-default-features` build has no worker, and
   every call fails with `mooncake.<procedure> failed: failed to connect to moonlink: ...`.
3. **`mooncake.create_table` creates the mirror table on a separate, autocommitted libpq connection** (loopback by default,
   or `src_uri`). It is not part of the caller's transaction, so a `ROLLBACK` does not undo a create_table that
   returned. If the `CreateTable` RPC fails, create_table drops the new table again on that connection.
4. **Replication connections are keyed by `src_uri`** (`moonlink_connectors/src/replication_manager.rs`). The slot
   `moonlink_slot_<db>` and publication `moonlink_pub` are dropped when the last mirror of that `src_uri` is dropped.
5. **No committed `Cargo.lock`** (`/Cargo.lock` is gitignored): the moonlink dependency graph resolves fresh, and
   `postgres`/`postgres-native-tls` track the HEAD of a git branch.
6. **New regress test without an expected file:** `cargo pgrx regress` prompts "Accept [Y, n]?" and panics without a
   terminal. `--auto` accepts new output **and overwrites failing expected files**. Prefer: create an empty
   `expected/<name>.out`, run `make test`, review `tests/pg_regress/results/<name>.out`, then copy it.
   Diffs land in `tests/pg_regress/regression.diffs`.

## 6. Recipe: adding a `mooncake.*` procedure

1. In `src/functions.rs`, follow the existing pattern:
   `#[pg_extern(sql = "CREATE PROCEDURE mooncake.x(...) LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]`.
   Parse table arguments with `parse_table` (Postgres identifier rules). Send `TableName::moonlink_name()` (unquoted
   `schema.table`) over RPC; use `TableName::quoted()` only in SQL you run yourself.
   Call moonlink with `rpc!(moonlink_rpc::<fn>, args..).unwrap_or_else(|e| error!("mooncake.x failed: {e}"))`.
2. Use only **existing** RPCs (section 1: `moonlink_rpc` must not change). A feature that needs a new RPC is a
   coordinated moonlink + duckdb_mooncake release, not a pg_mooncake change. If you do touch moonlink, the change has to
   land in the moonlink repo and the submodule pin has to move. An exhaustive `match` in `rpc_server.rs::handle_stream`
   and a `MoonlinkBackend` method go with every RPC.
3. There are **no extension upgrade scripts** (`pg_mooncake.control`: `default_version = '@CARGO_VERSION@'`). SQL API
   changes reach existing installs only via `DROP EXTENSION` + `CREATE EXTENSION`, which drops all mirrors. **[pins]**
4. Add a regress test (pitfall 6) and run `make test` for every supported major.

## 7. Known bugs at the current pins **[pins]**

`scripts/e2e-known-bugs.txt` is the complete list, with a failure signature per check; keep it current rather than this
summary. Product behavior (fix with a test; don't paper over it):
- Restart after a large, logically streamed transaction: moonlink WAL monotonic-LSN assert, crash loop.
- Wrong data without an error: unconstrained `numeric` (digits shifted), array columns (come back `{}`), `char(n)`
  (padding lost), an UPDATE that leaves a TOASTed column unchanged empties it in the mirror, and `TRUNCATE` of a source
  is ignored. `interval` and `inet` columns are rejected at `create_table`.
- `ALTER TABLE <source> ADD COLUMN` panics moonlink and that mirror stops ingesting; after `DROP COLUMN`, every mirror
  of the database stops.
- The worker is not restarted after a clean exit (section 2, item 1).
- Dropping any mirror deletes the moonlink WAL of every mirror (`WalManager::drop_wal` removes the shared `_wal`
  directory). Rows of other mirrors that are not yet in an Iceberg snapshot are lost at the next restart; run
  `CALL mooncake.create_snapshot(...)` on the remaining mirrors after dropping one.
- Calls other than `DropTable` have no timeout: a hung moonlink blocks them until it answers.
- Identifiers containing `.` are rejected (moonlink splits source names on `.`).

End-to-end checks (`scripts/e2e-smoke.sh`, see TESTING.md): one
`RESULT <scenario>.<check> PASS|FAIL|XFAIL(<bugs>)|XFAIL(limitation:<bugs>)|XPASS(<bugs>)|SKIP` line per check, then
`SUMMARY`, a `BLOCKING` line naming the listed blocking bugs, and `GATE e2e-smoke PASS|FAIL|BLOCKED|INCOMPLETE`.
Expected failures are listed in `scripts/e2e-known-bugs.txt`, each as `class=blocking` (must be fixed for a release;
while one is listed the gate reports `BLOCKED` at best) or `class=limitation` (documented; its XFAILs don't fail the
gate). Read results from that file rather than from hand-kept counts. `XPASS` means a known bug no longer reproduces:
remove its lines, except for a bug whose description starts with `Intermittent:`, where XPASS (`KNOWN ... NOT-SEEN`)
only means this run didn't hit it; keep that entry until its fix is in.

## 8. Conventions

- Rust: `make format` (`cargo fmt` + `cargo clippy`). duckdb_mooncake C++: `make -C duckdb_mooncake format-all`.
- Reads from another session are eventually consistent (about 0.5 s locally). Only the writing session gets
  read-your-writes (a scan waits for that backend's `XactLastCommitEnd`). Tests must poll, or read in the writing session.
