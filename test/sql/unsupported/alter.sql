CREATE TABLE t (a int) USING columnstore;
ALTER TABLE t ADD COLUMN b int;
DROP TABLE t;
