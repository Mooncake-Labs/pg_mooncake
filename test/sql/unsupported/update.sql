CREATE TABLE t (a int, b int) USING columnstore;
UPDATE t SET (a, b) = (SELECT 1, 2);
UPDATE t SET (b, a) = (SELECT 1, 2);
DROP TABLE t;
