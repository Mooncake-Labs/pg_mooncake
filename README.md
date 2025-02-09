<div align="center">

# pg_mooncake 🥮
Postgres extension for 1000x faster analytics

[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/Mooncake-Labs/pg_mooncake/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Mooncake%20Slack-purple?logo=slack)](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![Twitter](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)
[![Docs](https://img.shields.io/badge/Documentation-pgmooncake.com-blue?style=flat&logo=readthedocs&logoColor=white)](https://pgmooncake.com/docs)

</div>

## Overview
**pg_mooncake** is a Postgres extension that adds columnar storage and vectorized execution (DuckDB) for fast analytics within Postgres. Postgres + pg_mooncake ranks among the top 10 fastest in [ClickBench](https://www.mooncake.dev/blog/clickbench-v0.1). 

Columnstore tables are stored as [Iceberg](https://github.com/apache/iceberg) or [Delta Lake](https://github.com/delta-io/delta) tables in local file system or cloud storage.

The extension is maintained by [Mooncake Labs](https://mooncake.dev/) and is available on [Neon Postgres](https://neon.tech/home). 
<div align="center">
  <a href="https://www.mooncake.dev/blog/how-we-built-pgmooncake">
    <img src="https://www.mooncake.dev/images/blog/blog_4venn.jpg" width="50%"/>
  </a>
</div>

## [Installation](https://pgmooncake.com/docs/installation)

### Option 1: Docker
Get started quickly with our Docker image:
```bash
docker pull mooncakelabs/pg_mooncake

# server
docker run --name mooncake-demo -e POSTGRES_HOST_AUTH_METHOD=trust -d mooncakelabs/pg_mooncake

# client
docker run -it --rm --link mooncake-demo:postgres mooncakelabs/pg_mooncake psql -h postgres -U postgres
```

### Option 2: From Source
Build for Postgres versions 14–17:
```bash
make release -j$(nproc)
make install
```

### Option 3: On Neon Postgres
1. [Create a Neon project](https://console.neon.tech/signup)
2. Enable beta extensions:
```sql
SET neon.allow_unstable_extensions='true';
```

## [Quick Start](https://pgmooncake.com/docs/quick-start)
1. Enable the extension
```sql
CREATE EXTENSION pg_mooncake;
```
2. Create a columnstore table:
```sql
CREATE TABLE user_activity(
  user_id BIGINT,
  activity_type TEXT,
  activity_timestamp TIMESTAMP,
  duration INT
) USING columnstore;
```
3. Insert data:
```sql
INSERT INTO user_activity VALUES
  (1, 'login', '2024-01-01 08:00:00', 120),
  (2, 'page_view', '2024-01-01 08:05:00', 30),
  (3, 'logout', '2024-01-01 08:30:00', 60),
  (4, 'error', '2024-01-01 08:13:00', 60);

SELECT * from user_activity;
```

Columnstore tables behave just like regular Postgres heap tables, supporting transactions, updates, deletes, joins, and more.

## [Cloud Storage](https://pgmooncake.com/docs/cloud-storage)
Columnstore tables are stored in the local file system by default. You can configure `mooncake.default_bucket` to store data in S3 or R2 buckets instead.

> **Note**: On Neon, only cloud storage is supported. Neon users must bring their own S3 or R2 buckets or get a free S3 bucket by signing up at [s3.pgmooncake.com](https://s3.pgmooncake.com/). For cloud storage configuration instructions, see [Cloud Storage](https://pgmooncake.com/docs/cloud-storage). We are working to improve this experience.

## [Load Data](https://pgmooncake.com/docs/load-data)
**pg_mooncake** supports loading data from:
- Postgres heap tables
- Parquet, CSV, JSON files
- Iceberg, Delta Lake tables
- Hugging Face datasets

## Columnstore Tables as Iceberg or Delta Lake Tables
Find your columnstore table location:
```sql
SELECT * FROM mooncake.columnstore_tables;
```

The directory contains a Delta Lake (and soon Iceberg) table that can be queried directly using Pandas, DuckDB, Polars, or Spark.

## Roadmap
- [x] **Transactional INSERT, SELECT, UPDATE, DELETE, and COPY**
- [x] **JOIN with regular Postgres heap tables**
- [x] **Load Parquet, CSV, and JSON files into columnstore tables**
- [x] **Read existing Iceberg and Delta Lake tables**
- [x] **File statistics and skipping**
- [x] **Write Delta Lake tables**
- [ ] **Write Iceberg tables**
- [ ] **Batched small writes and compaction**
- [ ] **Secondary indexes and constraints**
- [ ] **Partitioned tables ^**

> [^](https://github.com/Mooncake-Labs/pg_mooncake/issues/17) File statistics and skipping should cover most use cases of partitioned tables in Postgres, including time series.

[v0.2.0 Roadmap](https://github.com/Mooncake-Labs/pg_mooncake/discussions/91)
