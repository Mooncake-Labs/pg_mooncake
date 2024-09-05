# pg_mooncake
Columnstore Table in Postgres

## Installation
Compile and install the extension (supports Postgres 16+)
```bash
make
make install
```

## Getting Started
Enable the extension
```sql
CREATE EXTENSION pg_mooncake;
```
Add two ints
```sql
SELECT add_nums(1, 2);
```
Sub two ints
```sql
SELECT sub_nums(1, 2);
```
