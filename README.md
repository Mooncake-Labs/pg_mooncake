<div align=center>
<h1 align=center>pg_mooncake 🥮</h1>
<h4 align=center>Real-time analytics on Postgres tables</h4>

[![][docs-shield]][docs-link]
[![][license-shield]][license-link]
[![][slack-shield]][slack-link]
[![][x-shield]][x-link]
</div>

## Overview
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FMooncake-Labs%2Fpg_mooncake.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FMooncake-Labs%2Fpg_mooncake?ref=badge_shield)


**pg_mooncake** is a Postgres extension that creates a columnstore mirror of your Postgres tables in [Iceberg][iceberg-link], enabling fast analytics queries with sub-second freshness:
- **Real-time ingestion** powered by [moonlink][moonlink-link] for streaming and batched INSERT/UPDATE/DELETE.
- **Fast analytics** accelerated by [DuckDB][pgduckdb-link], ranking top 10 on [ClickBench][clickbench-link].
- **Postgres-native** allowing you to query a columnstore table just like a regular Postgres table.
- **Iceberg-native** making your data readily accesssible by other query engines.

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

To build pg_mooncake, first install [Rust][rust-install], [pgrx][pgrx-install], and [the build tools for DuckDB][duckdb-install].

Then, clone the repository:
```bash
git clone --recurse-submodules https://github.com/Mooncake-Labs/pg_mooncake.git
```

To build and install for Postgres versions 14-17, run:
```bash
cargo pgrx init --pg17=$(which pg_config)   # Replace with your Postgres version
make pg_duckdb                              # Skip if pg_duckdb is already installed
make install PG_VERSION=pg17
```

Finally, add pg_mooncake to `shared_preload_libraries` in your `postgresql.conf` file and enable logical replication:
```ini
duckdb.allow_community_extensions = true
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
```

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


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FMooncake-Labs%2Fpg_mooncake.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FMooncake-Labs%2Fpg_mooncake?ref=badge_large)