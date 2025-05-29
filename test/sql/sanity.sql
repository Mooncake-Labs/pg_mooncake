CREATE TABLE t (a int, b text) USING columnstore;
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
INSERT INTO t VALUES (2, 'f'), (3, 'g'), (4, 'h');
UPDATE t SET b = a + 1 WHERE a > 3;
DELETE FROM t WHERE a < 3;
INSERT INTO t SELECT 2, b FROM t WHERE a = 3;
SELECT * FROM t;
DROP TABLE t;

CREATE TABLE t (a int) USING columnstore;
ANALYZE t;

BEGIN;
INSERT INTO t VALUES (123);
SELECT * FROM t;
ROLLBACK;
SELECT * FROM t;

BEGIN;
INSERT INTO t VALUES (123);
SELECT * FROM t;
COMMIT;
SELECT * FROM t;

DROP TABLE t;
