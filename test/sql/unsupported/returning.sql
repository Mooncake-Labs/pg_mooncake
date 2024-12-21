CREATE TABLE t (a int) USING columnstore;
INSERT INTO t VALUES (1), (2), (3);

-- Test RETURNING clause with UPDATE.
UPDATE t SET a = -a RETURNING a + 1;

-- Test RETURNING clause with DELETE.
DELETE FROM t RETURNING a + 1;

DROP TABLE t;
