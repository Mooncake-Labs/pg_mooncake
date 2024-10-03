# pg_mooncake
**pg_mooncake** is a PostgreSQL extension that brings columnstore tables, backed by open table formats: **Delta** and **Iceberg**.

[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/Mooncake-Labs/pg_mooncake/blob/main/LICENSE)
[![Slack URL](https://img.shields.io/badge/Join%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fmooncakelabs%2Fshared_invite%2Fzt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=Follow%20%40mooncakelabs)](https://x.com/mooncakelabs)

---

## Key Features
- **Column-oriented Storage**: Accelerate analytical and processing workloads. Queries on columnstore tables are executed by [**pg_duckdb**](https://github.com/duckdb/pg_duckdb) and [**duckdb**](https://github.com/duckdb/duckdb) for optimized performance.
- **Open Table Formats**: Columnstore tables persist on object storage with Delta or Iceberg metadata. You can catalog and query these tables directly from Snowflake, Databricks, and BigQuery.
- **Full Table Semantics and Interoperability**: Write, update, alter schema, and join your columnstore tables with regular PostgreSQL tables.
- **Python-based data processing (LLM/MLOps)**: Run frameworks like Polars, Torch, Pandas, and MLflow to transform and enrich your columnstore tables. Query these updated tables from Postgres directly.

## Roadmap
- [x] **Support for local file system**
- [x] **Transactional insert & select from columnstore**
- [x] **Real-time & mini batch inserts**
- [x] **Fast analytical queries on columnstore**
- [x] **Joins between rowstore & columnstore tables**
- [ ] **Update and Deletes from columnstore tables**
- [ ] **Support for S3**
- [ ] **Exposing Delta & Iceberg metadata**
- [ ] **Load parquet files directly**
- [ ] **Support for secondary indexes on columnstore**
- [ ] **Support for external execution engines on columnstore tables**

## Installation
To compile and install **pg_mooncake** (supports PostgreSQL 16):

```bash
git submodule update --init --recursive
make release
make install
```

## Usage

### 1. Enable the Extension
```sql
CREATE EXTENSION pg_mooncake;
```

### 2. Create a Columnstore Table
```sql
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * FROM generate_series(1, 5);
SELECT * FROM t;
```

### 3. Create a Regular Table
```sql
CREATE TABLE s (b int);
INSERT INTO s VALUES (1), (2), (3);
SELECT * FROM s;
```

### 4. Join Columnstore and Regular Tables
```sql
SELECT * FROM t JOIN s ON t.a = s.b;
```

## Built with ❤️ by Mooncake Labs 🥮
**pg_mooncake** is the first project from our open-source software studio, **Mooncake Labs**. We are building the data intelligence stack for the next billion developers. We are obessed about simple and clean interfaces for this infrastructure.

> [!CAUTION]
> **pg_mooncake** is currently in alpha and under active development. It is not intended for production use cases.
> Some functionality may not continue to be supported and some key features are not yet implemented.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
