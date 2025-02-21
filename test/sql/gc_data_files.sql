
-- case: mark old data files for deletion
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * from generate_series(1, 1000);
UPDATE t SET a = a + 1;
-- TODO check
DELETE FROM t WHERE a % 2 = 0;
-- TODO check

-- case: simple drop table cmd
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * from generate_series(1, 1000);
INSERT INTO t SELECT * from generate_series(1, 1000);
DROP TABLE t;
-- TODO check

-- case: delete multiple tables
CREATE TABLE t1 (a int) USING columnstore;
CREATE TABLE t2 (a int) USING columnstore;
CREATE TABLE t3 (a int) USING columnstore;
INSERT INTO t1 SELECT * from generate_series(1, 1000);
INSERT INTO t2 SELECT * from generate_series(1, 1000);
INSERT INTO t3 SELECT * from generate_series(1, 1000);
DROP TABLE t1, t2, t3;
-- TODO check

-- case: truncate table
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * from generate_series(1, 1000);
INSERT INTO t SELECT * from generate_series(1, 1000);
TRUNCATE TABLE t;
-- TODO check

-- case:  & drop materialized view
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * from generate_series(1, 1000);
INSERT INTO t SELECT * from generate_series(1, 1000);
CREATE MATERIALIZED VIEW mv USING columnstore AS SELECT * FROM t;
-- TODO check
DROP MATERIALIZED VIEW mv;
-- TODO check
DROP TABLE t;
-- TODO: currently columnstore mv does not refresh

-- case: drop schema cascade
CREATE SCHEMA myschema;
CREATE TABLE myschema.t1 (a int) USING columnstore;
INSERT INTO myschema.t1 SELECT * from generate_series(1, 1000);
CREATE TABLE myschema.t2 (a int) USING columnstore;
INSERT INTO myschema.t2 SELECT * from generate_series(1, 1000);
DROP SCHEMA myschema CASCADE;
-- TODO check

-- case: temp table on commit drop
BEGIN;
CREATE TEMP TABLE t (a int) USING columnstore ON COMMIT DROP;
INSERT INTO t SELECT * from generate_series(1, 1000);
INSERT INTO t SELECT * from generate_series(1, 1000);
COMMIT;
-- TODO check

-- case: temp table
CREATE TEMP TABLE tmp (a int) USING columnstore;
INSERT INTO tmp SELECT * from generate_series(1, 1000);
INSERT INTO tmp SELECT * from generate_series(1, 1000);

-- case: subtxn drop abort
CREATE TABLE t1 (a int) USING columnstore;
CREATE TABLE t2 (a int) USING columnstore;
INSERT INTO t1 SELECT * from generate_series(1, 1000);
INSERT INTO t2 SELECT * from generate_series(1, 1000);
BEGIN;
DROP TABLE t1;
SAVEPOINT sp;
DROP TABLE t2;
ROLLBACK TO sp;
COMMIT;
-- TODO check: t2 should still exists
DROP TABLE t2;

-- case: subtxn drop commit
CREATE TABLE t1 (a int) USING columnstore;
CREATE TABLE t2 (a int) USING columnstore;
INSERT INTO t1 SELECT * from generate_series(1, 1000);
INSERT INTO t2 SELECT * from generate_series(1, 1000);
BEGIN;
DROP TABLE t1;
SAVEPOINT sp;
DROP TABLE t2;
RELEASE SAVEPOINT sp;
COMMIT;
-- TODO check: t2 should be dropped