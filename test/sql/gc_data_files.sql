CREATE TABLE oid_tracker (oid oid, relname text);
TRUNCATE TABLE mooncake.dead_data_files;

-- case: temp table on commit drop
BEGIN;
CREATE TEMP TABLE t (a int) USING columnstore ON COMMIT DROP;
INSERT INTO t SELECT * from generate_series(1, 1000);
INSERT INTO t SELECT * from generate_series(1, 1000);
COMMIT;
SELECT COUNT(*) FROM mooncake.dead_data_files;

-- case: mark old data files for deletion
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * from generate_series(1, 1000);
UPDATE t SET a = a + 1;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = 't'::regclass;
DELETE FROM t WHERE a % 2 = 0;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = 't'::regclass;
-- DROP should move all files to dead_data_files
INSERT INTO oid_tracker (oid, relname) VALUES ('t'::regclass, 't');
DROP TABLE t;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't');

-- case: delete multiple tables
CREATE TABLE t1 (a int) USING columnstore;
CREATE TABLE t2 (a int) USING columnstore;
CREATE TABLE t3 (a int) USING columnstore;
INSERT INTO t1 SELECT * from generate_series(1, 1000);
INSERT INTO t2 SELECT * from generate_series(1, 1000);
INSERT INTO t3 SELECT * from generate_series(1, 1000);
INSERT INTO oid_tracker (oid, relname) VALUES ('t1'::regclass, 't1');
INSERT INTO oid_tracker (oid, relname) VALUES ('t2'::regclass, 't2');
INSERT INTO oid_tracker (oid, relname) VALUES ('t3'::regclass, 't3');
DROP TABLE t1, t2, t3;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't1');
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't2');
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't3');

-- case: truncate table
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * from generate_series(1, 1000);
INSERT INTO t SELECT * from generate_series(1, 1000);
TRUNCATE TABLE t;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = 't'::regclass;
DROP TABLE t;

-- case:  & drop materialized view
CREATE TABLE t (a int) USING columnstore;
INSERT INTO t SELECT * from generate_series(1, 1000);
INSERT INTO t SELECT * from generate_series(1, 1000);
CREATE MATERIALIZED VIEW mv USING columnstore AS SELECT * FROM t;
INSERT INTO oid_tracker (oid, relname) VALUES ('mv'::regclass, 'mv');
DROP MATERIALIZED VIEW mv;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 'mv');
DROP TABLE t;
-- TODO: add case for refresh mv. Currently, columnstore does not support refresh mv.

-- case: drop schema cascade
CREATE SCHEMA myschema;
CREATE TABLE myschema.t1 (a int) USING columnstore;
INSERT INTO myschema.t1 SELECT * from generate_series(1, 1000);
CREATE TABLE myschema.t2 (a int) USING columnstore;
INSERT INTO myschema.t2 SELECT * from generate_series(1, 1000);
INSERT INTO oid_tracker (oid, relname) VALUES ('myschema.t1'::regclass, 'myschema.t1');
INSERT INTO oid_tracker (oid, relname) VALUES ('myschema.t2'::regclass, 'myschema.t2');
DROP SCHEMA myschema CASCADE;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 'myschema.t1');
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 'myschema.t2');

-- case: subtxn drop abort
CREATE TABLE t11 (a int) USING columnstore;
CREATE TABLE t12 (a int) USING columnstore;
INSERT INTO t11 SELECT * from generate_series(1, 1000);
INSERT INTO t12 SELECT * from generate_series(1, 1000);
INSERT INTO oid_tracker (oid, relname) VALUES ('t11'::regclass, 't11');
INSERT INTO oid_tracker (oid, relname) VALUES ('t12'::regclass, 't12');
BEGIN;
DROP TABLE t11;
SAVEPOINT sp;
DROP TABLE t12;
ROLLBACK TO sp;
COMMIT;
-- t11 should be dropped while t12 should not be dropped
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't11');
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't12');
DROP TABLE t12;
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't12');

-- case: subtxn drop commit
CREATE TABLE t21 (a int) USING columnstore;
CREATE TABLE t22 (a int) USING columnstore;
INSERT INTO t21 SELECT * from generate_series(1, 1000);
INSERT INTO t22 SELECT * from generate_series(1, 1000);
INSERT INTO oid_tracker (oid, relname) VALUES ('t21'::regclass, 't21');
INSERT INTO oid_tracker (oid, relname) VALUES ('t22'::regclass, 't22');
BEGIN;
DROP TABLE t21;
SAVEPOINT sp;
DROP TABLE t22;
RELEASE SAVEPOINT sp;
COMMIT;
-- both t21 and t22 should be dropped
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't21');
SELECT COUNT(*) FROM mooncake.dead_data_files WHERE oid = (select oid from oid_tracker where relname = 't22');

-- cleanup
TRUNCATE TABLE mooncake.dead_data_files;
DROP TABLE oid_tracker;
