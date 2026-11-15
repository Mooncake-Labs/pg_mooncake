## 0.3.0 (unreleased) <!-- RC: set the date -->

pg_mooncake 0.3.0 runs on a slimmed-down moonlink. **Mirrors created with 0.2.x must be dropped and re-created after
upgrading**; see [Upgrading from 0.2](README.md#upgrading-from-02).

### Changed
<!-- RC: the moonlink items below come from branches that are not merged yet; re-check each against the final
     moonlink commit. -->
- Moonlink no longer keeps its own write-ahead log. The replication slot is acknowledged only up to what is persisted
  in Iceberg, and the rest is replayed from the slot after a restart. Postgres retains more `pg_wal` as a result (idle
  tables are force-snapshotted every 300 s).
- A mirror that can no longer be kept in sync enters an errored state instead of silently diverging or stopping
  replication for other mirrors. Queries on it fail with
  `mirror <name> is out of sync: <reason>; drop and re-create it with mooncake.create_table`. The state survives restarts.
- `TRUNCATE` of a source table, and schema changes to a source table (for example `ALTER TABLE ... ADD COLUMN`), put
  its mirrors into the errored state.
- `mooncake.create_table` sets `REPLICA IDENTITY FULL` on the source table (and each partition of a partitioned table),
  so updates keep unchanged TOAST values. Changing it back puts the mirror into the errored state.
- Unsupported source columns are rejected by `mooncake.create_table` with an error instead of being mirrored wrongly.
  <!-- RC: list the final rejected types/typmods (numeric precision, multi-dimensional arrays, inheritance parents). -->
- Failures talking to moonlink are reported as regular Postgres errors (`mooncake.<procedure> failed: ...`), and the
  next call reconnects.
- `DROP TABLE` of a mirror always succeeds. If moonlink is down or does not answer, the drop completes with a `WARNING`
  describing how to clean up moonlink's copy. All mirror drops of one transaction wait at most 30 s in total.
- `PREPARE TRANSACTION` in a transaction that drops a mirror is an error.

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
- A `mooncake.create_table` that moonlink rejects no longer leaves the empty mirror table behind.
- Building from source with the Rust toolchain pinned in `rust-toolchain.toml` works again: Cargo now prefers
  dependency versions that support it.
- Dropping a mirror created by 0.2 whose name needs quoting (mixed case, `$`) also removes it from moonlink.
<!-- RC: add the final moonlink fix list (replication, recovery, data-type and read-path fixes) with upstream PR links. -->

### Removed
- `mooncake.load_files`.
- From moonlink: its write-ahead log, the REST API, Kafka and REST event ingestion, OpenTelemetry ingestion and export,
  standalone deployment, the DataFusion integration, Delta Lake output, REST and Glue Iceberg catalogs, and GCS
  storage. pg_mooncake stores mirrors on the local filesystem with a filesystem Iceberg catalog.

### Performance
<!-- RC: numbers from the release benchmark against 0.2.x: write amplification, freshness, query latency,
     memory. -->

### Known limitations
- `DROP EXTENSION pg_mooncake CASCADE` drops the mirror tables in Postgres but not in moonlink. Drop the mirrors first;
  a leftover is removed when a mirror with the same name is created again.
- Identifiers containing `.` are rejected.
- Moonlink calls other than the one made by `DROP TABLE` have no timeout.
- The moonlink background worker is restarted after a crash, but not after it exits cleanly.
<!-- RC: replace with the final list from the release-gate run. -->

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
