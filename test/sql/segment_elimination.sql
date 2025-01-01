CREATE TABLE t (a int, b text) USING columnstore;
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t VALUES (123, 'abc');
UPDATE t SET b = 'def' WHERE a = 123;
SELECT * FROM t;
DROP TABLE t;
