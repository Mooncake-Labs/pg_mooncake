<div align=center>
<h1 align=center>pg_mooncake 🥮</h1>
<h4 align=center>Real-time analytics on Postgres tables</h4>

[![][docs-shield]][docs-link]
[![][license-shield]][license-link]
[![][slack-shield]][slack-link]
[![][x-shield]][x-link]
</div>

## Overview

**pg_mooncake** is a Postgres extension that creates a columnstore mirror of your Postgres tables in [Iceberg][iceberg-link], enabling fast analytics queries with sub-second freshness:
- **Real-time ingestion** powered by [moonlink][moonlink-link] for streaming and batched INSERT/UPDATE/DELETE.
- **Fast analytics** accelerated by [DuckDB][pgduckdb-link], ranking top 10 on [ClickBench][clickbench-link].
- **Postgres-native** allowing you to query a columnstore table just like a regular Postgres table.
- **Iceberg-native** making your data readily accessible by other query engines.

## Installation

### Option 1: Docker

For new users, we recommend using the Docker image to get started quickly:
```bash
docker run --name mooncake --rm -e POSTGRES_PASSWORD=password mooncakelabs/pg_mooncake
```

This will start Postgres with pg_mooncake preinstalled. You can then connect to it using `psql` with the default user `postgres`:
```bash
docker exec -it mooncake psql -U postgres
```

### Option 2: From Source

pg_mooncake supports Postgres 14 to 18 and is built with [Rust][rust-install] and [pgrx][pgrx-install]. It depends on
[pg_duckdb][pgduckdb-link], which is included as a submodule and built from source along with DuckDB.
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
make pg_duckdb PG_VERSION=pg18              # Skip if pg_duckdb is already installed
make install PG_VERSION=pg18
```

`make install` installs into the Postgres whose `pg_config` comes first on your `PATH`, so make sure that is the one you
passed to `cargo pgrx init`. If that Postgres is owned by root, install pg_mooncake with
`cargo pgrx install --release --sudo` in place of `make install`, and install pg_duckdb with root privileges as described
in [its build instructions][pgduckdb-link].

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

Finally, query `trades_iceberg` to see that it reflects the up-to-date state of `trades`:
```sql
SELECT avg(price) FROM trades_iceberg WHERE symbol = 'AMZN';
```

## Self-Hosting

**Roles.** The examples here run as a superuser. Creating the extension requires one, and pg_duckdb lets only
superusers, or members of the role set in `duckdb.postgres_role`, run DuckDB queries, which includes reading mirrors.

**Where data lives.** Mirrors are stored on the local filesystem under `$PGDATA/pg_mooncake/`. This includes the
Iceberg tables (filesystem catalog; `mooncake.list_tables()` shows each table's location), moonlink's metadata, caches,
and the Unix socket `moonlink.sock`. Plan disk space for it alongside your Postgres data.

**The moonlink worker.** Moonlink runs inside Postgres as a background worker named `moonlink` and logs to the
Postgres server log. Postgres restarts it 15 seconds after a crash.

**Local connections.** `mooncake.create_table` connects back to Postgres over its Unix socket as the current user,
without a password, to create the mirror table and replicate from the source. `pg_hba.conf` must accept that local
connection (for example with `trust` or `peer`).

**Replication slots and WAL.** Each database with mirrors uses one logical replication slot (usually named
`moonlink_slot_<database>`) and the publication `moonlink_pub`. Both are created by the first `mooncake.create_table`
and dropped with the last mirror. Changes stay in `pg_wal` until they are persisted to Iceberg. To see how much WAL a
slot retains:
```sql
SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%';
```
If you stop using pg_mooncake, drop all mirrors so that the slot is dropped and stops retaining WAL.

**Mirroring from another server.** `mooncake.create_table` takes an optional connection string for the source:
`CALL mooncake.create_table('trades_iceberg', 'trades', 'postgresql://user@host/db')`. The source server needs
`wal_level = logical`, and the user needs permission to replicate and to alter the source table.
<!-- RC: src_uri is not covered by the release tests; test it or drop this paragraph. -->

**Monitoring.** `SELECT * FROM mooncake.list_tables();` lists the mirrors with their commit and flush LSNs.

**Backups.** Mirrors can be rebuilt from their source tables. Replication slots are not part of a base backup, so after
restoring Postgres from a backup, drop and re-create the mirrors.

**Removing pg_mooncake.** Drop all mirrors first (`DROP TABLE <mirror>;`): `DROP EXTENSION pg_mooncake CASCADE` would
drop the mirror tables without telling moonlink, which keeps replicating them. Then `DROP EXTENSION pg_mooncake;`, remove it
from `shared_preload_libraries`, restart Postgres, and delete `$PGDATA/pg_mooncake/`.

## Durability and Recovery

Moonlink acknowledges the replication slot only up to what is persisted in Iceberg and replays the rest from the slot
after a restart. Idle mirrors are persisted at least every 5 minutes, which bounds how much `pg_wal` they hold back.

If a mirror can no longer be kept in sync (for example after `TRUNCATE`, a schema change of its source table, or a
value it cannot store), it enters an errored state. Queries on it fail with a message that says why. Drop it and
re-create it:
```sql
DROP TABLE trades_iceberg;
CALL mooncake.create_table('trades_iceberg', 'trades');
```
Dropping a mirror always succeeds, even while moonlink is down. Moonlink then keeps its copy until the mirror is
created again under the same name, which removes it. After a crash, if a mirror looks wrong, do the same.
<!-- RC: the errored state comes from moonlink changes that are not merged yet; re-check against the final moonlink. -->

## Upgrading from 0.2

pg_mooncake 0.3 changed how moonlink persists data, so mirrors created with 0.2 must be re-created.

1. Write down each mirror and its source table. `SELECT * FROM mooncake.list_tables();` lists the mirrors.
2. While still on 0.2, drop every mirror: `DROP TABLE <mirror>;`.
3. Build and install pg_mooncake 0.3 and the pg_duckdb it ships with (see [Installation](#installation)), then restart
   Postgres.
4. In each database that has the extension, run `ALTER EXTENSION pg_mooncake UPDATE;`.
5. Re-create the mirrors with `CALL mooncake.create_table(...)`.
6. Optionally delete `$PGDATA/pg_mooncake/_wal/`. 0.3 no longer uses it.

If you upgraded without dropping the mirrors first, `ALTER EXTENSION pg_mooncake UPDATE` prints a warning that lists
them. Drop and re-create each one.

`mooncake.load_files` was removed. See the [CHANGELOG](CHANGELOG.md) for all changes and known limitations.

## Contributing

pg_mooncake is an open-source project maintained by [Mooncake Labs][mooncake-link] and licensed under the [MIT License][license-link]. We'd love your help to make it even better! Join [our Slack][slack-link], participate in [discussions][discussions-link], open [issues][issues-link] to report bugs or suggest features, contribute code and documentation, or help us improve the project in any way. All contributions are welcome! 🥮

To build and test a change, see [AGENTS.md](AGENTS.md) and [TESTING.md](TESTING.md).

[clickbench-link]: https://www.mooncake.dev/blog/clickbench-v0.1
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
[pgduckdb-link]: https://github.com/duckdb/pg_duckdb
[pgrx-install]: https://github.com/pgcentralfoundation/pgrx?tab=readme-ov-file#getting-started
[rust-install]: https://www.rust-lang.org/tools/install
[slack-link]: https://join.slack.com/t/mooncake-devs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA
[slack-shield]: https://img.shields.io/badge/Mooncake%20Devs-purple?logo=slack
[x-link]: https://x.com/mooncakelabs
[x-shield]: https://img.shields.io/twitter/url?label=%40mooncakelabs&url=https%3A%2F%2Fx.com%2Fmooncakelabs
