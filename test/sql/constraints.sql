CREATE TABLE t (a int, b int NOT NULL, c text NOT NULL) USING columnstore;
INSERT INTO t VALUES (1, 2, 'a'), (3, 4, 'b'), (5, 6, 'c');
INSERT INTO t VALUES (7, NULL, 'd');
INSERT INTO t (a, b) VALUES (7, 8);
INSERT INTO t (b, c) VALUES (8, 'd');
UPDATE t SET b = 0 WHERE a = 1;
UPDATE t SET c = NULL WHERE a = 1;
SELECT * FROM t;
DROP TABLE t;
