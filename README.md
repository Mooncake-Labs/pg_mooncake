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

To build an image containing the dependency versions in this checkout, initialize the submodules and build locally:
```bash
git submodule update --init --recursive
docker build -t pg_mooncake .
docker run --name mooncake --rm -e POSTGRES_PASSWORD=password pg_mooncake
```

This will start Postgres with pg_mooncake preinstalled. You can then connect to it using `psql` with the default user `postgres`:
```bash
docker exec -it mooncake psql -U postgres
```

### Option 2: From Source

To build pg_mooncake, first install [Rust][rust-install], [pgrx][pgrx-install], and [the build tools for DuckDB][duckdb-install].

Then, clone the repository:
```bash
git clone --recurse-submodules https://github.com/Mooncake-Labs/pg_mooncake.git
```

This source revision uses pg_duckdb 1.2.0, DuckDB 2.0.0, and pgrx/cargo-pgrx 0.16.1. CMake, Ninja, a C++ compiler, libclang and OpenSSL development headers are also required. The checked-in Cargo.lock fixes the Rust dependencies for the declared toolchain.

To build and install for PostgreSQL 18, run:
```bash
cd pg_mooncake
cargo install --locked cargo-pgrx --version 0.16.1
cargo pgrx init --pg18=$(which pg_config)   # Replace with your Postgres version
make pg_duckdb PG_CONFIG=$(which pg_config) # Skip only if pg_duckdb 1.2.0 is installed
make install PG_VERSION=pg18 PG_CONFIG=$(which pg_config)
```

The build also installs the matching Mooncake DuckDB extension into `$(pg_config --sharedir)/extension/pg_mooncake`. Configure that local repository, preload the PostgreSQL extensions, and enable logical replication before creating the extension:
```ini
duckdb.allow_community_extensions = true
duckdb.allow_unsigned_extensions = true
mooncake.extension_repository = '/path/from/pg_config/sharedir/extension/pg_mooncake'
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
```

Replace the repository path with the absolute path for your PostgreSQL installation. Locally built DuckDB extensions are unsigned. If `mooncake.extension_repository` is unset, installation uses `community` and requires a compatible signed binary to have been published there.

`make package` stages both extensions. `make test` runs the SQL regressions against a configured test server. The `tests/integration.py` script requires psycopg 3 and a disposable database; run it with `--pg-major 18`, restart the test server, then run it again with `--after-restart`.

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

## Contributing

pg_mooncake is an open-source project maintained by [Mooncake Labs][mooncake-link] and licensed under the [MIT License][license-link]. We'd love your help to make it even better! Join [our Slack][slack-link], participate in [discussions][discussions-link], open [issues][issues-link] to report bugs or suggest features, contribute code and documentation, or help us improve the project in any way. All contributions are welcome! 🥮

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
