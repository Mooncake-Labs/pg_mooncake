CREATE TABLE t (a jsonb) USING columnstore;

CREATE TYPE point AS (x int, y int);
CREATE TABLE t (a point) USING columnstore;

CREATE TABLE mooncake_numeric_tbl (val numeric(40, 5)) USING columnstore;
CREATE TABLE mooncake_numeric_tbl (val numeric) USING columnstore;
