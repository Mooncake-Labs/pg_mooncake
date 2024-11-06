# pg_mooncake 🥮
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/Mooncake-Labs/pg_mooncake/blob/main/LICENSE) [![Slack URL](https://img.shields.io/badge/Mooncake%20Slack-purple?logo=slack&link=https%3A%2F%2Fjoin.slack.com%2Ft%2Fmooncakelabs%2Fshared_invite%2Fzt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA) [![X URL](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)

**pg_mooncake** is a PostgreSQL extension that adds native columnstore tables with DuckDB execution. Columnstore tables are stored as [Iceberg](https://github.com/apache/iceberg) or [Delta Lake](https://github.com/delta-io/delta) tables in object storage.

The extension is maintained by [Mooncake Labs](https://mooncake.dev/) and is available on [Neon Postgres](https://neon.tech/home).

```sql
-- Create a columnstore table in PostgreSQL
CREATE TABLE user_activity (....) USING columnstore;

-- Insert data into a columnstore table
INSERT INTO user_activity VALUES ....;

-- Query a columnstore table in PostgreSQL
SELECT * FROM user_activity LIMIT 5;
```

## Key Features
- **Table Semantics**: Columnstore tables support transactional and batch inserts, updates, and deletes, as well as joins with regular PostgreSQL tables.
- **DuckDB Execution**: Run analytic queries up to 1000x faster than on regular PostgreSQL tables, with performance similar to DuckDB on Parquet.
- **Iceberg and Delta Lake**: Columnstore tables are stored as Parquet files with Iceberg or Delta metadata, allowing external engines (e.g., Snowflake, DuckDB, Pandas, Polars) to query them as native tables.


## Installation
You can install **pg_mooncake** using our Docker image, from source, or on [Neon Postgres](https://neon.tech/home).

### Docker Image:
To quickly get a PostgreSQL instance with **pg_mooncake** extension up and running, pull and run the latest Docker image:
```bash
docker pull mooncakelabs/pg_mooncake
docker run --name mooncake-demo -e POSTGRES_HOST_AUTH_METHOD=trust -d mooncakelabs/pg_mooncake
```
This will start a PostgreSQL 17 instance with **pg_mooncake** extension. You can then connect to it using psql:
```bash
docker run -it --rm --link mooncake-demo:postgres mooncakelabs/pg_mooncake psql -h postgres -U postgres
```

### From Source:
You can compile and install **pg_mooncake** extension to add it to your PostgreSQL instance. PostgreSQL versions 15, 16, and 17 are currently supported.
```bash
git submodule update --init --recursive
make release
make install
```

### On Neon Postgres
To quickly install the **pg_mooncake** extension on Neon, [create a Neon project](https://console.neon.tech/signup) and run the following commands from the [Neon SQL Editor](https://neon.tech/docs/get-started-with-neon/query-with-neon-sql-editor) or a [connected SQL client such as psql](https://neon.tech/docs/connect/query-with-psql-editor):
```sql
SET neon.allow_unstable_extensions='true';
CREATE EXTENSION pg_mooncake;

### Enable the Extension:
```sql
CREATE EXTENSION pg_mooncake;
```

## Use Cases
1. **Run Analytics on Live Application Data in PostgreSQL**
   Run transactions on columnstore tables, ensuring up-to-date analytics without managing individual Parquet files.

2. **Write PostgreSQL Tables to Your Lake or Lakehouse**
   Make application data accessible as native tables for data engineering and science outside of PostgreSQL, without complex ETL, CDC or stitching of files.

3. **Query and Update existing Lakehouse Tables Natively in PostgreSQL** (coming soon)
   Connect existing Lakehouse catalogs and expose them directly as columnstore tables in PostgreSQL.

## Usage
### 1. (Optional) Add S3 secret and bucket
This will be where your columnstore tables are stored. If no S3 configurations are specified, these tables will be created in your local file system.
> **Note**: If you are using **pg_mooncake** on [Neon](https://neon.tech), you will need to bring your own S3 bucket for now. We’re  working to improve this DX.
```sql
SELECT mooncake.create_secret('<name>', 'S3', '<key_id>', '<secret>', '{"REGION": "<s3-region>"}');

SET mooncake.default_bucket = 's3://<bucket>';

SET mooncake.enable_local_cache = false; -- (if you are using Neon)
```

### 2. Creating columnstore tables
Create a columnstore table:
```sql
CREATE TABLE user_activity(
    user_id BIGINT,
    activity_type TEXT,
    activity_timestamp TIMESTAMP,
    duration INT
) USING columnstore;
```
Insert data:
```sql
INSERT INTO user_activity VALUES
    (1, 'login', '2024-01-01 08:00:00', 120),
    (2, 'page_view', '2024-01-01 08:05:00', 30),
    (3, 'logout', '2024-01-01 08:30:00', 60),
    (4, 'error', '2024-01-01 08:13:00', 60);

SELECT * FROM user_activity;
```

You can also insert data directly from a parquet file (can also be from S3):
```sql
COPY user_activity FROM '<parquet_file>'
```

Update and delete rows:
```sql
UPDATE user_activity
SET activity_timestamp = '2024-01-01 09:50:00'
WHERE user_id = 3 AND activity_type = 'logout';

DELETE FROM user_activity
WHERE user_id = 4 AND activity_type = 'error';

SELECT * from user_activity;
```

Run transactions:
```sql
BEGIN;

INSERT INTO user_activity VALUES
    (5, 'login', '2024-01-02 10:00:00', 200),
    (6, 'login', '2024-01-02 10:30:00', 90);

ROLLBACK;

SELECT * FROM user_activity;
```

### 3. Run Analytic queries in PostgreSQL
Run aggregates and groupbys:
```sql
SELECT
    user_id,
    activity_type,
    SUM(duration) AS total_duration,
    COUNT(*) AS activity_count
FROM
    user_activity
GROUP BY
    user_id, activity_type
ORDER BY
    user_id, activity_type;
```

Joins with regular PostgreSQL tables:
```sql
CREATE TABLE users (
    user_id BIGINT,
    username TEXT,
    email TEXT
);

INSERT INTO users VALUES
    (1,'alice', 'alice@example.com'),
    (2, 'bob', 'bob@example.com'),
    (3, 'charlie', 'charlie@example.com');

SELECT * FROM users u
JOIN user_activity a ON u.user_id = a.user_id;
```

### 4. Querying Columnstore tables outside of PostgresSQL
Find path where the columnstore table was created in:
```sql
SELECT * FROM mooncake.columnstore_tables;
```

Create a Polars dataframe directly from this table:
```python
import polars as pl
from deltalake import DeltaTable

delta_table_path = '<path>'
delta_table = DeltaTable(delta_table_path)
df = pl.DataFrame(delta_table.to_pyarrow_table())
```

## Roadmap
- [x] **Support for local file system and Object Store (S3)**
- [x] **Transactional select, insert, copy, updates, and deletes**
- [x] **Real-time and mini-batch inserts**
- [x] **Join with regular Postgres tables**
- [x] **Write Delta Lake format**
- [x] **Directly insert Parquet files into columnstore tables**
- [ ] **Write Iceberg format**
- [ ] **Secondary indexes and constraints (primary, unique, and foreign keys)**
- [ ] **Read existing Iceberg or Delta Lake tables**
- [ ] **Partitioned tables**

> [!CAUTION]
> **pg_mooncake** is currently in preview and actively under development. For inquiries about production use cases, please reach out to us on our [Slack community](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)!
> Some functionality may not continue to be supported and some key features are not yet implemented.

## Built with ❤️ by Mooncake Labs 🥮
**pg_mooncake** is the first project from our open-source software studio, **Mooncake Labs**. Mooncake is building a managed lakehouse with a clean Postgres and Python experience. With Mooncake, developers ship analytics and AI to their applications without complex ETL, CDC and pipelines.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
