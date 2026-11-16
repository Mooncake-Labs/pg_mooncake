## 0.3.0 (unreleased) <!-- RC: set the date -->

pg_mooncake 0.3.0 runs on a slimmed-down moonlink.
**Mirrors created with 0.2.x must be dropped and re-created after upgrading**; see [Upgrading from 0.2](README.md#upgrading-from-02).

### Changed
- Moonlink no longer keeps its own write-ahead log. The replication slot is acknowledged only up to what is persisted
  in Iceberg, and the rest is replayed from the slot after a restart. Postgres retains more `pg_wal` as a result (idle
  tables are force-snapshotted every 300 s). ([moonlink PR 2356][ml-2356])
- A mirror that can no longer be kept in sync enters an errored state instead of silently diverging or stopping
  replication for other mirrors. Queries on it fail with
  `mirror <name> is out of sync: <reason>; drop and re-create it with mooncake.create_table`. The state survives restarts.
  ([moonlink PR 2357][ml-2357])
- `TRUNCATE` of a source table, and schema changes to a source table (for example `ALTER TABLE ... ADD COLUMN`), put its
  mirrors into the errored state. So do changes the replication stream does not show: a column dropped and re-added,
  partitions attached with rows or detached, and changes made while moonlink was stopped. ([moonlink PR 2360][ml-2360],
  [moonlink PR 2368][ml-2368])
- `mooncake.create_table` sets `REPLICA IDENTITY FULL` on the source table (and each partition of a partitioned table),
  so updates keep unchanged TOAST values. Changing it back puts the mirror into the errored state. ([moonlink PR
  2360][ml-2360])
- `mooncake.create_table` refuses, with the reason, a source it cannot mirror exactly: a column of a type mirrors cannot
  store (`interval`, `inet`), a multi-dimensional array column, generated columns, a table with inheritance children, a
  partitioned table without a primary key or with an unlogged or foreign partition, a partitioned table and one of its
  own partitions both mirrored, and a source the connection's role cannot `SELECT`. ([moonlink PR 2360][ml-2360],
  [moonlink PR 2368][ml-2368])
- Values are stored as Postgres has them (`numeric`, `json`/`jsonb`, arrays, dates before year 1 and after 9999,
  domains, enums), or the mirror enters the errored state: for example on `infinity` dates and timestamps,
  multi-dimensional array values, or more than 10 fractional digits in an unconstrained `numeric`. ([moonlink PR
  2360][ml-2360], [moonlink PR 2368][ml-2368])
- A query that reads a mirror at the session's last commit waits at most 30 s for moonlink to reach it, then fails with
  a retryable error naming the LSN. ([moonlink PR 2365][ml-2365])
- moonlink deletes Iceberg files it no longer needs. Set `MOONLINK_ICEBERG_GC=dry-run` in the server's environment to
  only log them, or `off` to keep everything. A full disk pauses mirrors for up to 30 minutes (or until 512 MiB of
  changes wait in memory) before erroring them. ([moonlink PR 2365][ml-2365])
- Failures talking to moonlink are reported as regular Postgres errors (`mooncake.<procedure> failed: ...`), and the
  next call reconnects.
- `DROP TABLE` of a mirror always succeeds. If moonlink is down or does not answer, the drop completes with a `WARNING`
  describing how to clean up moonlink's copy. All mirror drops of one transaction wait at most 30 s in total.
- `PREPARE TRANSACTION` in a transaction that drops a mirror is an error.
- `INSERT`, `UPDATE`, `DELETE` and `MERGE` on a mirror fail with `cannot modify mirror <name>` before pg_duckdb plans
  them. Before, some forms crashed the backend on assert-enabled builds (`UPDATE ... SET (a, b) = (SELECT ...)`) or
  raised DuckDB internal errors.
- `TRUNCATE` and `COPY ... FROM` on a mirror fail with the same error.
- A mirror cannot be renamed (with `ALTER TABLE` or `ALTER INDEX ... RENAME`, which Postgres also applies to tables),
  moved to another schema (`ALTER TABLE ... SET SCHEMA`), turned into a regular table (`ALTER TABLE ... SET ACCESS
  METHOD`), or made a partition or an inheritance child of another table, and a schema that contains mirrors cannot be
  renamed. moonlink keeps a mirror under the name it was created with, so a renamed mirror would no longer match it;
  drop it and create it again under the new name instead. Writes through a parent table would reach the mirror.

### Fixed
- `DROP TABLE` and `DROP SCHEMA ... CASCADE` of regular tables no longer contact moonlink, so they work
  while moonlink is down.
- An error from one moonlink call no longer makes every later call in the session fail.
- Table names passed to `mooncake.*` procedures follow Postgres' identifier rules: quoted and mixed-case names
  (`"Sales"."Orders 2025"`) and names containing `$` work, unquoted names are downcased like Postgres does, and names
  longer than 63 bytes are truncated like Postgres does.
- A mirror drop undone by `ROLLBACK TO SAVEPOINT` or an exception block no longer removes the mirror from moonlink.
- The first moonlink call in a session after the moonlink worker restarts no longer fails.
- Re-creating a mirror whose earlier `DROP` moonlink never saw (moonlink was down, or `DROP EXTENSION ... CASCADE`)
  works and reads the new source: `mooncake.create_table` first removes the table moonlink kept under that name.
  Before, it failed, and the new mirror table returned the old mirror's rows.
- A `mooncake.create_table` that moonlink rejects no longer leaves the empty mirror table behind. It drops only the
  table it created, not one another session created under the same name in the meantime.
- The moonlink background worker is restarted 15 s after it is stopped with SIGTERM outside a server shutdown (for
  example a `kill` of the worker). Before, it exited cleanly and was never restarted, so every mirror stayed down
  until Postgres restarted.
- Configuration reloads (`pg_reload_conf()`, `pg_ctl reload`, `SIGHUP`, including reloads by logrotate, systemd or
  package scripts) and time zone changes undone at the end of a transaction no longer crash sessions that have run a
  DuckDB query, such as a query on a mirror. The bundled pg_duckdb is now Mooncake-Labs/pg_duckdb `v1.0.0-mooncake.1`
  (a tag on its `v1.0.0-mooncake` branch), which carries the upstream fix for pg_duckdb's TimeZone hook
  (duckdb/pg_duckdb#981) on the DuckDB v1.4.1 that duckdb_mooncake needs. 0.2 has the same crash.
- A fast shutdown no longer panics in the moonlink background worker, which could also make `pg_ctl stop -m fast` hang:
  shutdown signals are now handled only on the worker's main thread.
- Building from source with the Rust toolchain pinned in `rust-toolchain.toml` works again: Cargo now prefers
  dependency versions that support it.
- Dropping a mirror created by 0.2 whose name needs quoting (mixed case, `$`) also removes it from moonlink.

### Fixed in moonlink
- A restart or a reconnect no longer loses or duplicates rows, and `pg_wal` no longer grows behind idle or empty
  mirrors. ([moonlink PR 2364][ml-2364])
- A mirror that cannot follow its source no longer crashes the background worker, stops replication for the other
  mirrors, or silently diverges: it enters the errored state, and dropping it no longer hangs. A mirror that cannot be
  restored at startup is errored too, instead of making the worker crash-loop. ([moonlink PR 2357][ml-2357], [moonlink
  PR 2360][ml-2360])
- Wrong data without an error: arrays came back empty, unconstrained `numeric` values had their digits shifted,
  `TRUNCATE` of a source was ignored, and an `UPDATE` that left a TOASTed column unchanged emptied it in the mirror.
  ([moonlink PR 2360][ml-2360])
- A large `numeric` value (more than 96 bits, such as 38 digits) no longer panics moonlink's statistics and errors the
  mirror. ([moonlink PR 2368][ml-2368])
- Scans no longer break the RPC connection when they race with replication, and a read right after a commit no longer
  returns stale rows. ([moonlink PR 2358][ml-2358])
- Failed flushes, Iceberg commits, compactions and index merges are retried, or error only their table, instead of
  panicking or losing the table. Iceberg metadata writes are atomic, so an interrupted write no longer leaves a table
  that cannot be loaded. ([moonlink PR 2365][ml-2365])
- Lower write amplification under steady writes, and bounded memory and cache use. ([moonlink PR 2366][ml-2366])
- `mooncake.create_snapshot` could return before the data was durable in Iceberg. ([moonlink PR 2369][ml-2369])
- The initial copy of a new mirror converts dates, timestamps and copied rows without allocating per value, which fixes
  its slowdown against 0.2. ([moonlink PR 2370][ml-2370])
- The initial copy stores a text value of backslash-N (`\N`) as that text instead of NULL, and rejects a copied row with
  fewer columns than the table instead of storing its values in the wrong columns. ([moonlink PR 2371][ml-2371])
- `mooncake.create_snapshot` no longer waits forever on an idle mirror after a streamed transaction aborts. ([moonlink
  PR 2372][ml-2372])
- A restart of moonlink right after `mooncake.create_table` returns no longer loses the rows of the new mirror's initial
  copy. `mooncake.create_table` now returns only once the copy is durable in Iceberg, which makes it slower: about
  1.36 s instead of 0.93 s for a 1M-row table. A restart before it returns leaves no mirror behind, so the call fails
  and can be retried. ([moonlink PR 2373][ml-2373])
- A full disk during the initial copy makes `mooncake.create_table` fail promptly with an error instead of waiting for
  space, and dropping a mirror while `mooncake.create_table` is still creating it is safe. ([moonlink PR 2374][ml-2374])

### Removed
- `mooncake.load_files`.
- From moonlink: its write-ahead log, the REST API, Kafka and REST event ingestion, OpenTelemetry ingestion and
  export, standalone deployment, the DataFusion integration, Delta Lake output, REST and Glue Iceberg catalogs, GCS
  storage, and the Postgres metadata store. pg_mooncake stores mirrors on the local filesystem with a filesystem
  Iceberg catalog. ([moonlink PR 2353][ml-2353], [moonlink PR 2356][ml-2356], [moonlink PR 2367][ml-2367])

### Performance
<!-- RC: numbers from the release benchmark against 0.2.x: write amplification, freshness, query latency,
     memory. -->

### Known limitations
- Reading mirrors needs pg_duckdb's `duckdb.postgres_role` and membership in both `pg_read_server_files` and
  `pg_write_server_files`, so in practice a superuser. `GRANT`/`REVOKE` on individual mirrors are not enforced.
- Mirrors and the schemas that contain them cannot be renamed; drop and re-create the mirror instead.
- `DROP EXTENSION pg_mooncake CASCADE` drops the mirror tables in Postgres but not in moonlink. Drop the mirrors first;
  a leftover is removed when a mirror with the same name is created again.
- Identifiers containing `.` are rejected.
- `char(n)` values come back from a mirror without their trailing blanks. Postgres ignores those blanks when it compares
  `char(n)` values and in `length()`, so only the displayed value differs.
- Unconstrained `numeric` columns are stored as `decimal(38, 10)`. A value with more than 10 fractional digits puts the
  mirror into the errored state; give such a column an explicit precision and scale instead.
- `interval` and `inet` columns cannot be mirrored: `mooncake.create_table` rejects a source table that has one.
- Dropping a mirror leaves its empty directory under `pg_mooncake/initial_copy` in the data directory.
- Moonlink calls other than the one made by `DROP TABLE` have no timeout.
- Composite and domain-over-composite columns read back from a mirror as DuckDB STRUCT values, e.g.
  `{'f1': 1, 'f2': 'foo'}` instead of `(1,foo)`. The values are preserved; compare or extract fields (`(c).f1`) rather
  than the row text. Comparing the whole value with a row (`c = ROW(1, 'foo')::t`) fails in DuckDB.

[ml-2353]: https://github.com/Mooncake-Labs/moonlink/pull/2353
[ml-2356]: https://github.com/Mooncake-Labs/moonlink/pull/2356
[ml-2357]: https://github.com/Mooncake-Labs/moonlink/pull/2357
[ml-2358]: https://github.com/Mooncake-Labs/moonlink/pull/2358
[ml-2360]: https://github.com/Mooncake-Labs/moonlink/pull/2360
[ml-2364]: https://github.com/Mooncake-Labs/moonlink/pull/2364
[ml-2365]: https://github.com/Mooncake-Labs/moonlink/pull/2365
[ml-2366]: https://github.com/Mooncake-Labs/moonlink/pull/2366
[ml-2367]: https://github.com/Mooncake-Labs/moonlink/pull/2367
[ml-2368]: https://github.com/Mooncake-Labs/moonlink/pull/2368
[ml-2369]: https://github.com/Mooncake-Labs/moonlink/pull/2369
[ml-2370]: https://github.com/Mooncake-Labs/moonlink/pull/2370
[ml-2371]: https://github.com/Mooncake-Labs/moonlink/pull/2371
[ml-2372]: https://github.com/Mooncake-Labs/moonlink/pull/2372
[ml-2373]: https://github.com/Mooncake-Labs/moonlink/pull/2373
[ml-2374]: https://github.com/Mooncake-Labs/moonlink/pull/2374

## 0.1.2 (2025-02-11)
### Added
- Support NOT NULL constraint (#116)
### Fixed
- Manully load iceberg extension since it's not autoloadable in DuckDB v1.1.3
- Fix use-after-free bug when reading statistics
- Allow non-superusers to set maximum_memory and maximum_threads
- Fix ALTER TABLE ... SET ACCESS METHOD DEFAULT (#115)

## 0.1.1 (2025-01-29)
### Added
- Preload pg_mooncake in Docker image
- Add a command to reset DuckDB
- Expose GUCs to set maximum memory and threads DuckDB can use
### Fixed
- Fix DuckDB extension autoloading (#99)
- Fix query failure involving subplans (#100)
- Suppress unnecessary default value error on Postgres heap tables

## 0.1.0 (2025-01-10)
### Added
- Transactional INSERT, SELECT, UPDATE, DELETE, and COPY
- JOIN with regular Postgres heap tables
- Load Parquet, CSV, and JSON files into columnstore tables
- Read existing Iceberg and Delta Lake tables
- File statistics and skipping
- Write Delta Lake tables
