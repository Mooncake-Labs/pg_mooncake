<div align=center>
<h1 align=center>pg_mooncake 🥮</h1>
<h4 align=center>Real-time analytics on Postgres tables</h4>

[![][docs-shield]][docs-link]
[![][license-shield]][license-link]
[![][slack-shield]][slack-link]
[![][x-shield]][x-link]
</div>

## Overview

**pg_mooncake** is a Postgres extension that creates a columnstore mirror of your Postgres tables in [Iceberg][iceberg-link], enabling fast analytics queries on fresh data: changes are visible about half a second later from another session (immediately in the writing session):
- **Real-time ingestion** powered by [moonlink][moonlink-link] for streaming and batched INSERT/UPDATE/DELETE.
- **Fast analytics** accelerated by [DuckDB][pgduckdb-link].
- **Postgres-native** allowing you to query a columnstore table just like a regular Postgres table.
- **Iceberg-native** making your data readily accessible by other query engines.

## Installation

### Option 1: Docker

For new users, we recommend using the Docker image to get started quickly:
```bash
docker run --name mooncake --rm -e POSTGRES_PASSWORD=password mooncakelabs/pg_mooncake
```

<!-- RC: the image is built on the moving pgduckdb/pgduckdb:18-main tag, whose pg_duckdb (and DuckDB) is not the
     bundled one; pin the base image to the pg_duckdb this release ships with. -->

This will start Postgres with pg_mooncake preinstalled. You can then connect to it using `psql` with the default user `postgres`:
```bash
docker exec -it mooncake psql -U postgres
```

### Option 2: From Source

pg_mooncake supports Postgres 14 to 18 and is built with [Rust][rust-install] and [pgrx][pgrx-install]. It depends on
[pg_duckdb][pgduckdb-link], which is included as a submodule and built from source along with DuckDB. The submodule is
the `v1.0.0-mooncake` branch of [Mooncake-Labs/pg_duckdb][pgduckdb-fork-link] (tag `v1.0.0-mooncake.1`): pg_duckdb
1.0.0 on DuckDB v1.4.1 plus the upstream fix for its TimeZone hook (duckdb/pg_duckdb#981), without which a
configuration reload crashes every session that has run a DuckDB query. Use this pg_duckdb rather than a separately
installed one.
<!-- RC: confirm the Postgres versions against the release test matrix. -->

**Prerequisites.**
- Postgres 14 to 18, including the server development files (`pg_config` and headers).
- [rustup][rust-install]. The Rust version is pinned in `rust-toolchain.toml`, and rustup installs it on first use.
- [The build tools for DuckDB][duckdb-install] (a C++ compiler, CMake, Ninja) and libclang, which pgrx uses to generate
  bindings.

On Debian or Ubuntu, for example:
```bash
sudo apt install build-essential cmake ninja-build pkg-config clang libclang-dev \
  libssl-dev libcurl4-openssl-dev liblz4-dev postgresql-server-dev-18
```
`postgresql-server-dev-18` (and the other majors' packages) comes from the PostgreSQL project's apt repository
(PGDG, apt.postgresql.org); distribution repositories usually carry only one Postgres major.

**Build and install.** Clone the repository with its submodules:
```bash
git clone --recurse-submodules https://github.com/Mooncake-Labs/pg_mooncake.git
cd pg_mooncake
```

Install pgrx and point it at your Postgres:
```bash
cargo install --locked cargo-pgrx@0.16.1
cargo pgrx init --pg18=$(which pg_config)   # Replace with your Postgres version
```

Then build and install pg_duckdb and pg_mooncake for that version:
```bash
make pg_duckdb PG_VERSION=pg18              # Skip only if this bundled pg_duckdb (v1.0.0-mooncake.1) is installed
make install PG_VERSION=pg18
```

`make install` and `make pg_duckdb` install into the Postgres you passed to `cargo pgrx init` for that version. To
install into another one, pass its `pg_config`: `make install PG_VERSION=pg18 PG_CONFIG=/path/to/pg_config` (the major
version must match `PG_VERSION`). If that Postgres is owned by root, install pg_mooncake with
`cargo pgrx install --release --sudo --pg-config /path/to/pg_config` in place of `make install`, and install pg_duckdb
with root privileges as described in [its build instructions][pgduckdb-link].

**Configure Postgres.** Add pg_mooncake to `shared_preload_libraries` in your `postgresql.conf` file and enable logical
replication, then restart Postgres:
```ini
duckdb.allow_community_extensions = true
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
```
- `pg_duckdb` must come before `pg_mooncake`.
- `CREATE EXTENSION pg_mooncake` downloads DuckDB's `mooncake` extension from the DuckDB community extensions
  repository, which is why `duckdb.allow_community_extensions` is needed.

For a complete walkthrough, refer to our [Dockerfile][dockerfile-link].

## Quick Start

First, create the pg_mooncake extension:
```sql
CREATE EXTENSION pg_mooncake CASCADE;
```

Next, create a regular Postgres table `trades`:
```sql
CREATE TABLE trades(
  id bigint PRIMARY KEY,
  symbol text,
  time timestamp,
  price real
);
```

Then, create a columnstore mirror `trades_iceberg` that stays in sync with `trades`:
```sql
CALL mooncake.create_table('trades_iceberg', 'trades');
```

Now, insert some data into `trades`:
```sql
INSERT INTO trades VALUES
  (1,  'AMD', '2024-06-05 10:00:00', 119),
  (2, 'AMZN', '2024-06-05 10:05:00', 207),
  (3, 'AAPL', '2024-06-05 10:10:00', 203),
  (4, 'AMZN', '2024-06-05 10:15:00', 210);
```

Finally, in the same session, query `trades_iceberg` to see that it reflects the up-to-date state of `trades`:
```sql
SELECT avg(price) FROM trades_iceberg WHERE symbol = 'AMZN';
```
A query on a mirror waits until moonlink has applied the session's own last commit, so the writing session always sees
its changes. Other sessions see a change once moonlink applies it, about half a second later (longer after large
transactions).

## Self-Hosting

**Roles.** Creating the extension requires a superuser. `mooncake.create_table` needs, in practice, a superuser too: it
creates the mirror table, creates a replication slot and adds the source to a publication (the `REPLICATION` attribute,
and `CREATE` on the database for the first mirror), sets `REPLICA IDENTITY FULL` on the source (ownership of it), and
reads the source (`SELECT`). Reading mirrors needs, in practice, a superuser as well:

- Mirrors are read by DuckDB through pg_duckdb, so a role needs pg_duckdb's `duckdb.postgres_role` (set it in
  `postgresql.conf`, restart, then `GRANT <that role> TO <user>`). `GRANT SELECT` on the mirror alone is not enough:
  queries fail with `DuckDB execution is not allowed because you have not been granted the duckdb.postgres_role`.
- Mirrors are local Parquet files, and pg_duckdb blocks DuckDB's local file access for roles that are not members of
  both `pg_read_server_files` and `pg_write_server_files`. Without them, reads fail with
  `File system LocalFileSystem has been disabled by configuration`.

Run queries on mirrors as a superuser or as one trusted role that has all three, not as application roles that should
see only some of the data. See [Security](#security).

**Where data lives.** Mirrors are stored on the local filesystem, by default under `$PGDATA/pg_mooncake/` (the
`mooncake.data_directory` setting, see [Backups and standbys](#backups-and-standbys)). This includes the Iceberg tables
(filesystem catalog; `mooncake.list_tables()` shows each table's location), moonlink's metadata, caches, and the Unix
socket `moonlink.sock`. Plan disk space for it alongside your Postgres data.

**The moonlink worker.** Moonlink runs inside Postgres as a background worker named `moonlink` and logs to the
Postgres server log. Postgres restarts it 15 seconds after a crash.

**Local connections.** `mooncake.create_table` connects back to Postgres over its Unix socket as the current user,
without a password, to create the mirror table and replicate from the source. `pg_hba.conf` must accept that local
connection (for example with `trust` or `peer`). moonlink's logical replication connection is matched by the same
per-database entries; `replication` lines in `pg_hba.conf` are only for physical replication and are not needed.

**Replication slots and WAL.** Each database with mirrors uses one logical replication slot (usually named
`moonlink_slot_<database>`) and the publication `moonlink_pub`. Both are created by the first `mooncake.create_table`
and dropped with the last mirror. Changes stay in `pg_wal` until they are persisted to Iceberg. To see how much WAL a
slot retains:
```sql
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS not_yet_persisted
FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%';
```
`retained_wal` is what the slot keeps in `pg_wal`; `not_yet_persisted` is how far moonlink's Iceberg tables are behind.
If you stop using pg_mooncake, drop all mirrors so that the slot is dropped and stops retaining WAL.

**Mirroring from another server.** `mooncake.create_table` takes an optional connection string for the source:
`CALL mooncake.create_table('trades_iceberg', 'trades', 'postgresql://user@host/db')`. The source server needs
`wal_level = logical`, and the user needs permission to replicate and to alter the source table.
<!-- RC: src_uri is not covered by the release tests; test it or drop this paragraph. -->

**Column types.** Mirrors store booleans, character and text types, integers, floating-point numbers, `numeric` (up to
38 digits), `bytea`, dates, times and timestamps (not `timetz`), `uuid`, `json`/`jsonb`, `oid` and enums, and arrays,
composites and domains of these; `mooncake.create_table` refuses a source with any other column type. Some values read
back differently from a mirror (unconstrained `numeric` with 10 fractional digits, `char(n)` without trailing blanks,
composites as DuckDB STRUCT text), and enums sort as text. The CHANGELOG's
[Known limitations](CHANGELOG.md#known-limitations) has the full list.

**Renames and schema changes.** moonlink keeps each mirror under the database, schema and table name it was created
with. pg_mooncake rejects renaming a mirror, moving it to another schema, and renaming a schema that contains mirrors.
`ALTER DATABASE ... RENAME TO` fails while the database has mirrors, because moonlink's replication connections use
it: drop that database's mirrors first and re-create them afterwards. Column changes on a source table (for example
`ALTER TABLE ... ADD COLUMN` or `DROP COLUMN`) put its mirrors into the errored state; drop and re-create them (see
[Durability and Recovery](#durability-and-recovery)).

**Table settings.** The optional fourth argument of `mooncake.create_table` is a JSON object of moonlink settings for
the mirror (default `'{}'`). The supported keys are under `"mooncake"`: `skip_index_merge` and `skip_data_compaction`
(booleans, default `false`) turn off the background index merges and data compaction, and `mem_slice_bytes` (default
16 MiB) is how many bytes of changes are buffered in memory before they are written to a data file. For example
`CALL mooncake.create_table('trades_iceberg', 'trades', NULL, '{"mooncake": {"skip_data_compaction": true}}')`.
Unknown keys are ignored. Other moonlink settings, such as `"iceberg"` storage, are not supported in this release.

**Monitoring.** `SELECT * FROM mooncake.list_tables();` lists the mirrors with their commit and flush LSNs.


**Removing pg_mooncake.** Drop all mirrors first (`DROP TABLE <mirror>;`): `DROP EXTENSION pg_mooncake CASCADE` would
drop the mirror tables without telling moonlink, which keeps replicating them. Then `DROP EXTENSION pg_mooncake;`, remove it
from `shared_preload_libraries`, restart Postgres, and delete `$PGDATA/pg_mooncake/`.

## Backups and standbys

<!-- RC: confirm mooncake.data_directory semantics from the code PR -->
moonlink stores mirrors in the directory named by `mooncake.data_directory`, a setting that takes effect at server
start (in `postgresql.conf`). The default, `pg_mooncake`, is relative to the data directory, so mirrors live in
`$PGDATA/pg_mooncake/`. With that default, `pg_basebackup` fails while any mirror exists
(`file name too long for tar format`): moonlink's file names there are longer than the 99 characters Postgres' tar
stream allows, and this also stops `pg_basebackup` from creating a standby. To use `pg_basebackup` or streaming
standbys, set `mooncake.data_directory` to an absolute path outside `$PGDATA`, for example
`mooncake.data_directory = '/var/lib/pg_mooncake'`, and restart Postgres before creating mirrors.

Mirror data is then not part of a base backup, and a standby has none. Mirrors can be rebuilt from their source tables:
after restoring Postgres from a backup, drop and re-create the mirrors so that they take a new snapshot of their
sources.

## Security

- **Grant `pg_read_server_files` and `pg_write_server_files` only to roles you trust like a superuser.** Reading mirrors
  requires them (see [Roles](#self-hosting)), and they let a role read and write any file the Postgres server can,
  through DuckDB as well as through `COPY`. That includes every mirror's data files.
- **Mirrors don't honour `GRANT` and `REVOKE`.** Privileges on a mirror table are not checked when it is read, so a role
  that can read one mirror can read all of them. Limit access by controlling who has the roles above, not with
  privileges on individual mirrors.
- The privileges of the source table still apply to the source table itself; only reads of mirrors bypass them.

The [CHANGELOG](CHANGELOG.md#known-limitations) lists the other known limitations.

## Durability and Recovery

Moonlink acknowledges the replication slot only up to what is persisted in Iceberg and replays the rest from the slot
after a restart. Idle mirrors are persisted at least every 5 minutes, which bounds how much `pg_wal` they hold back.
Errored mirrors don't hold the slot back.

If a mirror can no longer be kept in sync (for example after `TRUNCATE`, a schema change of its source table, or a
value it cannot store), it enters an errored state. Queries on it fail with a message that says why. Drop it and
re-create it:
```sql
DROP TABLE trades_iceberg;
CALL mooncake.create_table('trades_iceberg', 'trades');
```
Dropping a mirror always succeeds, even while moonlink is down. Moonlink then keeps its copy until the mirror is
created again under the same name, which removes it. After a crash, if a mirror looks wrong, do the same.

## Coming from 0.1.x

pg_mooncake 0.3.0 is a new architecture. 0.1.x stored data in its own columnstore tables; 0.3.0 keeps your data in
regular Postgres tables and mirrors them into Iceberg through logical replication. Nothing is migrated from 0.1.x, and
`ALTER EXTENSION pg_mooncake UPDATE` cannot upgrade it.

1. Copy any data you want to keep into regular tables, for example `CREATE TABLE t_heap AS SELECT * FROM t;`.
   `CREATE TABLE AS` doesn't copy the primary key or other constraints, so add them back
   (`ALTER TABLE t_heap ADD PRIMARY KEY (id);`); a partitioned source needs a primary key to be mirrored.
2. Drop the 0.1 extension and its columnstore tables: `DROP EXTENSION pg_mooncake CASCADE;`.
3. Delete 0.1's data files, which `DROP EXTENSION` leaves behind: `$PGDATA/mooncake_local_tables/` and
   `$PGDATA/mooncake_local_cache/`, and, if you set `mooncake.default_bucket`, the `mooncake_<database>_<table>_<oid>/`
   prefixes in that bucket.
4. Install 0.3.0 (see [Installation](#installation)). Add `pg_duckdb,pg_mooncake` to `shared_preload_libraries`, set
   `wal_level = logical` and `duckdb.allow_community_extensions = true`, and restart Postgres before
   `CREATE EXTENSION pg_mooncake CASCADE`.

Then follow the [Quick Start](#quick-start). See the [CHANGELOG](CHANGELOG.md) for what 0.3.0 does and its known
limitations.

## Contributing

pg_mooncake is an open-source project maintained by [Mooncake Labs][mooncake-link] and licensed under the [MIT License][license-link]. We'd love your help to make it even better! Join [our Slack][slack-link], participate in [discussions][discussions-link], open [issues][issues-link] to report bugs or suggest features, contribute code and documentation, or help us improve the project in any way. All contributions are welcome! 🥮

To build and test a change, see [AGENTS.md](AGENTS.md) and [TESTING.md](TESTING.md).

[discussions-link]: https://github.com/Mooncake-Labs/pg_mooncake/discussions
[dockerfile-link]: https://github.com/Mooncake-Labs/pg_mooncake/blob/main/Dockerfile
[docs-link]: https://docs.mooncake.dev/
[docs-shield]: https://img.shields.io/badge/docs-mooncake?logo=readthedocs&logoColor=white
[duckdb-install]: https://duckdb.org/docs/stable/dev/building/overview.html#prerequisites
[iceberg-link]: https://iceberg.apache.org/
[issues-link]: https://github.com/Mooncake-Labs/pg_mooncake/issues
[license-link]: https://github.com/Mooncake-Labs/pg_mooncake/blob/main/LICENSE
[license-shield]: https://img.shields.io/badge/License-MIT-blue
[mooncake-link]: https://mooncake.dev/
[moonlink-link]: https://github.com/Mooncake-Labs/moonlink
[pgduckdb-fork-link]: https://github.com/Mooncake-Labs/pg_duckdb/tree/v1.0.0-mooncake
[pgduckdb-link]: https://github.com/duckdb/pg_duckdb
[pgrx-install]: https://github.com/pgcentralfoundation/pgrx?tab=readme-ov-file#getting-started
[rust-install]: https://www.rust-lang.org/tools/install
[slack-link]: https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA
[slack-shield]: https://img.shields.io/badge/Mooncake%20Devs-purple?logo=slack
[x-link]: https://x.com/mooncakelabs
[x-shield]: https://img.shields.io/twitter/url?label=%40mooncakelabs&url=https%3A%2F%2Fx.com%2Fmooncakelabs
