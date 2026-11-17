## 0.3.0 (unreleased) <!-- RC: set the date -->

pg_mooncake 0.3.0 is the first release of pg_mooncake built on [moonlink](https://github.com/Mooncake-Labs/moonlink).
It keeps a columnstore mirror of a regular Postgres table in Iceberg, up to date through logical replication, and runs
analytics queries on the mirror in DuckDB through pg_duckdb. It is a new design: 0.1.x stored data in columnstore
tables of its own, and nothing is migrated from it; see [Coming from 0.1.x](README.md#coming-from-01x).

### Overview
- `CALL mooncake.create_table('<mirror>', '<source>')` creates the mirror table, copies the source into it, and from
  then on applies every committed change of the source from a logical replication slot.
- Queries on a mirror run in DuckDB, through pg_duckdb and the duckdb_mooncake extension. They read the mirror's
  Iceberg data plus the changes moonlink has not persisted yet.
- moonlink runs inside Postgres as a background worker. It stores mirrors on the local filesystem in
  `mooncake.data_directory` (`$PGDATA/pg_mooncake` by default), with a filesystem Iceberg catalog. Object storage such
  as S3, REST and Glue Iceberg catalogs, Delta Lake output, and moonlink's standalone service, REST API, and Kafka and
  OpenTelemetry ingestion are not part of this release. ([moonlink PR 2353][ml-2353], [moonlink PR 2356][ml-2356],
  [moonlink PR 2367][ml-2367])
- The bundled pg_duckdb is Mooncake-Labs/pg_duckdb `v1.0.0-mooncake.1`, on the DuckDB v1.4.1 that duckdb_mooncake
  needs. It carries the upstream fix for pg_duckdb's TimeZone hook (duckdb/pg_duckdb#981), so configuration reloads
  (`pg_reload_conf()`, `pg_ctl reload`, `SIGHUP`, including reloads by logrotate, systemd or package scripts) and time
  zone changes undone at the end of a transaction don't crash sessions that have run a DuckDB query.
- The Docker image is `postgres:18` with the bundled pg_duckdb and pg_mooncake built from source.

### Usage
- `mooncake.create_table(mirror, source [, src_uri [, table_config]])` creates a mirror; `src_uri` is a connection
  string for a source on another server. `table_config` is an optional JSON object of moonlink settings (default
  `'{}'`): `{"mooncake": {"skip_index_merge": true, "skip_data_compaction": true, "mem_slice_bytes": 16777216}}` turns
  off background index merges or data compaction, or sets how many bytes of changes are buffered in memory per mirror;
  other keys are ignored or not supported. See the README's [Quick Start](README.md#quick-start) and
  [Table settings](README.md#self-hosting).
- `mooncake.create_snapshot(mirror)` returns once the mirror's changes up to the session's last commit are persisted
  in Iceberg, also on an idle mirror whose last streamed transaction aborted. ([moonlink PR 2369][ml-2369], [moonlink
  PR 2372][ml-2372])
- `mooncake.optimize_table(mirror, mode)` compacts a mirror's data files (`data`), merges its index files (`index`), or
  both (`full`).
- `mooncake.list_tables()` lists the database's mirrors with their commit and flush LSNs and Iceberg location.
- `DROP TABLE <mirror>` removes a mirror.
- Table names passed to `mooncake.*` procedures follow Postgres' identifier rules: quoted and mixed-case names
  (`"Sales"."Orders 2025"`) and names containing `$` work, unquoted names are downcased, and names longer than 63 bytes
  are truncated.

### Behaviour and guarantees

#### Creating a mirror
- `mooncake.create_table` refuses, with the reason, a source it cannot mirror exactly: a column of a type mirrors cannot
  store (see Known limitations), an array column declared with more than one dimension, generated columns, a table
  with inheritance children, a partitioned table without a primary key or with an unlogged or foreign partition, a
  partitioned table and one of its own partitions both mirrored, and a source the connection's role cannot `SELECT`.
  If moonlink rejects the mirror, the call drops the mirror table it created, but not one another session created
  under the same name in the meantime. ([moonlink PR 2360][ml-2360], [moonlink PR 2368][ml-2368])
- It sets `REPLICA IDENTITY FULL` on the source table (and each partition of a partitioned table), so updates keep
  unchanged TOAST values. Changing it back puts the mirror into the errored state. ([moonlink PR 2360][ml-2360])
- It returns only once the initial copy is persisted in Iceberg, so a restart right after it keeps the copied rows. A
  full disk during the copy, or dropping the mirror from another session, makes the call fail and leaves nothing
  behind. ([moonlink PR 2373][ml-2373], [moonlink PR 2374][ml-2374])
- When `mooncake.create_table` fails before moonlink is involved, the error names the cause:
  `mooncake.create_table failed: could not read the columns of <source>: ...`, `could not create <mirror>: ...` or
  `could not connect to <uri>: ...`.
- The initial copy stores the text value backslash-N (`\N`) as that text, not as NULL, and rejects a copied row with
  fewer columns than the table. ([moonlink PR 2371][ml-2371])

#### Values
- Supported types are stored faithfully: `numeric` (including values beyond 96 bits, such as 38 digits),
  `json`/`jsonb`, one-dimensional arrays, dates before year 1 and after 9999, domains and enums. The exceptions are
  listed in Known limitations; a value a mirror cannot store puts the mirror into the errored state. ([moonlink PR
  2360][ml-2360], [moonlink PR 2368][ml-2368])

#### Replication and durability
- moonlink has no write-ahead log of its own. The replication slot is acknowledged only up to what is persisted in
  Iceberg, and the rest is replayed from the slot after a restart, so restarts and reconnects neither lose nor
  duplicate rows. Postgres keeps the unpersisted part in `pg_wal`; idle mirrors are persisted at least every 300 s, and
  idle or empty mirrors don't hold `pg_wal` back. ([moonlink PR 2356][ml-2356], [moonlink PR 2364][ml-2364])
- Failed flushes, Iceberg commits, compactions and index merges are retried, or error only their table. Iceberg metadata
  writes are atomic, so an interrupted write never leaves a table that cannot be loaded. ([moonlink PR 2365][ml-2365])
- A full disk pauses mirrors for up to 30 minutes (or until 512 MiB of changes wait in memory) before erroring them.
  ([moonlink PR 2365][ml-2365])
- moonlink deletes Iceberg files it no longer needs. Set `MOONLINK_ICEBERG_GC=dry-run` in the server's environment to
  only log them, or `off` to keep everything. ([moonlink PR 2365][ml-2365])
- Memory and cache use are bounded. ([moonlink PR 2366][ml-2366])

#### Reads
- A query on a mirror reads it as of the session's last commit. It waits at most 30 s for moonlink to reach that
  commit, then fails with a retryable error naming the LSN. Scans that run while changes are applied see a consistent
  state. ([moonlink PR 2358][ml-2358], [moonlink PR 2365][ml-2365])

#### The errored state
- A mirror that can no longer be kept in sync enters an errored state; the other mirrors and the worker are not
  affected. Queries on it fail with
  `mirror <name> is out of sync: <reason>; drop and re-create it with mooncake.create_table`. The state survives
  restarts, an errored mirror doesn't hold the slot back, and dropping it works. A mirror that cannot be restored at
  startup is errored too. ([moonlink PR 2357][ml-2357], [moonlink PR 2360][ml-2360])
- `TRUNCATE` of a source table, and schema changes to a source table (for example `ALTER TABLE ... ADD COLUMN`), put its
  mirrors into the errored state. So do changes the replication stream does not show: a column dropped and re-added,
  partitions attached with rows or detached, and changes made while moonlink was stopped. ([moonlink PR 2360][ml-2360],
  [moonlink PR 2368][ml-2368])

#### Mirror tables
- `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE` and `COPY ... FROM` on a mirror fail with
  `cannot modify mirror <name>`.
- `DROP TABLE` of a mirror always succeeds. If moonlink is down or does not answer, the drop completes with a `WARNING`
  describing how to clean up moonlink's copy. All mirror drops of one transaction wait at most 30 s in total. A drop
  undone by `ROLLBACK TO SAVEPOINT` or an exception block keeps the mirror in moonlink.
- `PREPARE TRANSACTION` in a transaction that drops a mirror is an error.
- Dropping regular tables, including with `DROP SCHEMA ... CASCADE`, never contacts moonlink, so it works while
  moonlink is down.

#### Operations
- Failures talking to moonlink are reported as regular Postgres errors (`mooncake.<procedure> failed: ...`). They don't
  affect later calls in the session, and the next call reconnects, also after the moonlink worker restarted.
- The moonlink background worker is restarted 15 s after it exits outside a server shutdown, including when it is
  stopped with SIGTERM (for example a `kill` of the worker). A fast shutdown stops it cleanly.
- `mooncake.data_directory`, read at server start, is the directory for moonlink's files and its socket: `pg_mooncake`
  under the data directory by default, or an absolute path. Postgres creates it (mode 0700) and refuses to start if it
  can't use it or another server's moonlink already does. See the README's
  [Backups and standbys](README.md#backups-and-standbys).
- moonlink starts once recovery finishes: not on a standby, and on promotion. A promoted standby keeps the mirrors
  whose files are in its `mooncake.data_directory`; drop and re-create the others.

### Performance
Measured with the project's benchmark on an 8-vCPU, 62 GiB-RAM NVMe machine (PostgreSQL 17.10, release build, a
5M-row, ~1 GB table, a 100 tps mixed workload):
- **Write amplification**: about 1.2 bytes written to local storage per logical byte of CDC changes (0.97 over the
  whole run, including the initial copy).
- **Freshness**: committed changes reach the mirror with a median lag of 272 ms (p95 495 ms) under sustained load, and
  become visible to a scan from another session in about 0.5 s (p95 about 0.53 s).
- **Scans** over rows still in memory take about 19 ms, idle or while 1,000 rows/s are ingested.
- **Initial copy**: about 750k rows/s. `mooncake.create_table`, which returns only once the copy is persisted to
  Iceberg, takes about 6.6 s for 5M rows.

### Known limitations
- Reading mirrors needs pg_duckdb's `duckdb.postgres_role` and membership in both `pg_read_server_files` and
  `pg_write_server_files`, so in practice a superuser. `GRANT`/`REVOKE` on individual mirrors are not enforced.
- A mirror cannot be renamed (with `ALTER TABLE` or `ALTER INDEX ... RENAME`, which Postgres also applies to tables),
  moved to another schema (`ALTER TABLE ... SET SCHEMA`), turned into a regular table (`ALTER TABLE ... SET ACCESS
  METHOD`), or made a partition or an inheritance child of another table, and a schema that contains mirrors cannot be
  renamed. moonlink keeps a mirror under the name it was created with; drop the mirror and create it again under the
  new name instead. `ALTER DATABASE ... RENAME TO` fails while the database has mirrors.
- Mirrors store only these column types: `boolean`, `"char"`, `char(n)`, `varchar`, `text`, `name`, `smallint`,
  `integer`, `bigint`, `real`, `double precision`, `numeric` (precision up to 38, scale from 0 to the precision),
  `bytea`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `json`, `jsonb`, `oid` and enums, and arrays, composites
  and domains built on them. `mooncake.create_table` refuses a source with any other column type, for example
  `interval`, `timetz`, `inet`, `cidr`, `macaddr`, `macaddr8`, `money`, `bit`, `varbit`, `pg_lsn`, `xml`, `tsvector`,
  `tsquery`, range and geometric types, and types from other extensions.
- `infinity` and `-infinity` dates and timestamps cannot be stored. `mooncake.create_table` fails on a source that has
  one, and one written later puts the mirror into the errored state.
- Unconstrained `numeric` columns, and the elements of `numeric` arrays, are stored as `decimal(38, 10)` and read back
  with 10 fractional digits (`1` as `1.0000000000`); the values compare equal. A `numeric` value that doesn't fit its
  stored type (more fractional digits than its scale, more digits than its precision, or `NaN`, `Infinity` and
  `-Infinity`) makes `mooncake.create_table` fail if the source has it, and puts the mirror into the errored state if
  it is written later. Give an unconstrained column an explicit precision and scale to choose the stored type.
- Enum columns compare and sort as text on a mirror, not in the enum's declared order: `ORDER BY`, comparisons and
  `min()`/`max()` follow the labels' text order (`min()` of `'red'` and `'blue'` is `'blue'`). Comparing an enum
  column with a literal (`WHERE c = 'red'`) fails in DuckDB with `Type with name <enum> does not exist`; compare its
  text instead (`WHERE c::text = 'red'`).
- `char(n)` values come back from a mirror without their trailing blanks. Postgres ignores those blanks when it compares
  `char(n)` values and in `length()`, so only the displayed value differs.
- Composite and domain-over-composite columns read back from a mirror as DuckDB STRUCT values, e.g.
  `{'f1': 1, 'f2': 'foo'}` instead of `(1,foo)`. The values are preserved; compare or extract fields (`(c).f1`) rather
  than the row text. Comparing the whole value with a row (`c = ROW(1, 'foo')::t`) fails in DuckDB.
- Multi-dimensional arrays: `mooncake.create_table` refuses a column declared with more than one dimension, and a
  multi-dimensional value in a one-dimensional array column puts the mirror into the errored state.
- Identifiers containing `.` are rejected.
- Only the writing session reads its own changes immediately. Other sessions see a change once moonlink applies it,
  about half a second later (longer after large transactions).
- With `mooncake.data_directory` inside the data directory, as it is by default, `pg_basebackup` fails while any
  mirror exists (`file name too long for tar format`), which also prevents creating a standby with it: moonlink's file
  names are longer than Postgres' tar format allows. Set `mooncake.data_directory` outside the data directory to use
  `pg_basebackup`. Mirror data is not part of a base backup; after a restore, drop and re-create the mirrors.
- Mirrors can't be read on a standby; moonlink runs only on the primary.
- If moonlink restarts while `mooncake.create_table` runs, the call fails. After a restart of only the moonlink worker,
  nothing is left behind and the call can be retried. After a server stop or crash, the new, empty mirror table stays in
  Postgres, and creating the mirror again fails until it is removed with `DROP TABLE`.
- `DROP EXTENSION pg_mooncake CASCADE` drops the mirror tables in Postgres but not in moonlink, and so does a
  `DROP TABLE` of a mirror while moonlink is down. Drop the mirrors first; a leftover is removed when a mirror with the
  same name is created again.
- Dropping a mirror leaves its empty directory under `pg_mooncake/initial_copy` in the data directory.
- Moonlink calls other than the one made by `DROP TABLE` have no timeout.

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
