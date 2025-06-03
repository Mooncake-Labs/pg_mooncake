<div align="center">

# pg_mooncake 🥮
The Postgres columnstore extension for fast & up-to-date analytics.

[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/Mooncake-Labs/pg_mooncake/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Mooncake%20Slack-purple?logo=slack)](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![Twitter](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)
[![Docs](https://img.shields.io/badge/docs-mooncake?style=flat&logo=readthedocs&logoColor=white)](https://pgmooncake.com/docs)

</div>

## Overview
**pg_mooncake** is a Clickhouse alternative for real-time analytics on your Postgres tables.

1. Columnstore tables are sync'd with Postgres tables and maintain real-time consistency with transactional data.

2. Queries on columnstore tables are executed by DuckDB and rank among the fastest on [ClickBench](https://www.mooncake.dev/blog/clickbench-v0.1).

3. Columnstore data is stored as Iceberg tables (parquet + metadata) in local file system or cloud storage. 

pg_mooncake is a Postgres extension is maintained by [Mooncake Labs](https://mooncake.dev/), and relies on [Moonlink](https://github.com/Mooncake-Labs/moonlink/tree/main) (real-time Postgres to Iceberg sync). 

## [Installation](https://pgmooncake.com/docs/installation)

### Option 1: Docker
```bash
docker pull mooncakelabs/pg_mooncake

# server
docker run --name mooncake-demo -e POSTGRES_HOST_AUTH_METHOD=trust -d mooncakelabs/pg_mooncake

# client
docker run -it --rm --link mooncake-demo:postgres mooncakelabs/pg_mooncake psql -h postgres -U postgres
```

### Option 2: From Source
Get source code from [releases](https://github.com/Mooncake-Labs/pg_mooncake/releases) or clone:
```bash
git clone --recurse-submodules https://github.com/Mooncake-Labs/pg_mooncake.git
```

Build for Postgres versions 14–17:
```bash
make release -j$(nproc)
make install
```

## [Quick Start](https://pgmooncake.com/docs/quick-start)
1. Enable the extension
```sql
CREATE EXTENSION pg_mooncake;
```
2. Create a regular Postgres table, `user_activity`:
```sql
CREATE TABLE user_activity(
  user_id BIGINT PRIMARY KEY,
  activity_type TEXT,
  activity_timestamp TIMESTAMP,
  duration INT
);
```
3. Create a columnstore copy `user_activity_col` that's in sync with `user_activity`:
```sql
CALL create_mooncake_table('user_activity_col', 'user_activity');
```

4. Insert data into `user_activity`;
```sql
INSERT INTO user_activity VALUES
  (1, 'login', '2024-01-01 08:00:00', 120),
  (2, 'page_view', '2024-01-01 08:05:00', 30),
  (3, 'logout', '2024-01-01 08:30:00', 60),
  (4, 'error', '2024-01-01 08:13:00', 60);
```

5. Query `user_activity_col` which reflects the up-to-date state of `user_activity`
```sql
SELECT * FROM user_activity_col;
```

## [Write Path](https://pgmooncake.com/docs/load-data)

Columnstore tables are created as a copy of a regular Postgres table (rowstore). 

1. Direct writes to the columnstore table are not supported.
2. All data modifications (updates/inserts/deletes) are performed on the regular Postgres table and are replicated to the columnstore with sub-second consistency.
3. (Roadmap) Maintain full table history in columnstore while keeping only recent data in rowstore. 

## [Read Path](https://pgmooncake.com/docs/load-data)
Query a columnstore table as you would a regular postgres table. Including:

1. Join columnstore tables with regular Postgres tables
2. Connect any Postgres-compatible BI tool or ORM to query columnstore data

## Iceberg

Columnstore tables are stored written as Iceberg tables (Parquet Files + Metadata), and can be read by engines like DuckDB, Snowflake, Spark, etc.

This is powered by [Moonlink](https://github.com/Mooncake-Labs/moonlink/tree/main) optimizes, manages and compacts Iceberg state for real-time mirroring from Postgres. 

## Roadmap


- [x] **Efficiently handle point insets/updates/deletes**
- [x] **Create columnstore table that's sync'd with a rowstore table**
- [x] **JOINs between columnstore and rowstore tables**
- [x] **Read from existing Iceberg and Delta Lake tables**
- [x] **File statistics and predicate pushdown**
- [x] **Writing data as Iceberg tables**
- [ ] **Integration with cloud storage + Iceberg REST Catalog**
- [ ] **Periodical truncate rowstore tables, while maintaining full history in columnstore**
- [ ] **Secondary indexes**


## V0.2 vs V0.1
pg_mooncake v0.2 introduces a redesigned architecture that enables:

1. **Real-time mirroring** of rowstore to columnstore tables. 
2. **Optimizations for point insertsupdates/deletes** without creating a parquet file per operation. 
3. **Writing Iceberg table format**. 

Stay tuned for an architecture deep dive. 


