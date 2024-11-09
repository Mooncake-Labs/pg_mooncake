CREATE TABLE t (a jsonb) USING columnstore;

CREATE TYPE point AS (x int, y int);
CREATE TABLE t (a point) USING columnstore;
