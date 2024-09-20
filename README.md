# pg_mooncake
Columnstore Table in Postgres

## Installation
Compile and install the extension (supports Postgres 16+)
```bash
git submodule update --init --recursive
make
make install
```

## Getting Started
Enable the extension
```sql
CREATE EXTENSION pg_mooncake;
```
Create a columnstore table
```sql
CREATE TABLE t (a int) USING columnstore;
COPY t FROM PROGRAM 'seq 5';
SELECT * FROM t;
```
Create a regular table
```sql
CREATE TABLE s (b int);
INSERT INTO s VALUES (1), (2), (3);
SELECT * FROM s;
```
Join two tables
```sql
SELECT * FROM t JOIN s ON t.a = s.b;
```
