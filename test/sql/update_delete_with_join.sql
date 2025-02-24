CREATE TABLE s (a int, b int);
INSERT INTO s VALUES (1, 123), (1, 456);

CREATE TABLE t (c int, d int) USING columnstore;
INSERT INTO t VALUES (1, 0), (2, 0);

UPDATE t SET d = b FROM s WHERE c = a;
SELECT * FROM t ORDER BY c;

DELETE FROM t USING s WHERE c = a;
SELECT * FROM t ORDER BY c;

DROP TABLE s, t;
