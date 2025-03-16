CREATE TABLE t1 (a int, b int) USING columnstore;
CREATE TABLE t2 (a int, b int) USING columnstore;
INSERT INTO t1 VALUES (1, 1);
INSERT INTO t1 VALUES (2, 2);
INSERT INTO t1 VALUES (3, 3);
INSERT INTO t2 VALUES (4, 4);
INSERT INTO t2 VALUES (5, 5);
INSERT INTO t2 VALUES (6, 6);
INSERT INTO t2 VALUES (7, 7);

SELECT COUNT(*) FROM mooncake.data_files WHERE oid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 't1');
SELECT COUNT(*) FROM mooncake.data_files WHERE oid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 't2');

VACUUM;

SELECT COUNT(*) FROM mooncake.data_files WHERE oid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 't1');
SELECT COUNT(*) FROM mooncake.data_files WHERE oid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 't2');

SELECT * FROM t1 ORDER BY a;
SELECT * FROM t2 ORDER BY a;

DROP TABLE t1;
DROP TABLE t2;
