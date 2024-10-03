# pg_mooncake
**pg_mooncake** is a PostgreSQL extension that brings columnstore tables, backed by open table formats: **Delta** and **Iceberg**.

## Key Features
- **Column-oriented Storage**: Accelerate analytical and processing workloads. Queries on columnstore tables are executed by [**pg_duckdb**](https://github.com/duckdb/pg_duckdb) and [**duckdb**](https://github.com/duckdb/duckdb) query engines for optimized performance.
- **Open Table Formats**: Columnstore tables are backed by **Delta** and **Iceberg** formats on object storage, allowing you to bring other execution engines and frameworks directly onto these tables.
- **SQL-Based Management of Lake Tables**: Easily write, update, and query lakehouse data directly from PostgreSQL, providing a familiar SQL interface.
- **Lake and PostgreSQL Interoperability**: Enable seamless joins between columnstore tables and regular PostgreSQL tables for integrated data access.

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
make
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
