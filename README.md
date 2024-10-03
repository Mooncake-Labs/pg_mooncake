# pg_mooncake
**pg_mooncake** is a PostgreSQL extension that brings columnstore tables, backed by open table formats: **Delta** and **Iceberg**.

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
**pg_mooncake** is the first project from our open-source software studio, **Mooncake Labs**. We are focused on creating simple interfaces for data intelligence infrastructure, making it accessible to every developer and application.

## Disclaimer
**pg_mooncake** is currently in alpha and under active development. It is not intended for production use cases. Some functionality may not continue to be supported.


## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
