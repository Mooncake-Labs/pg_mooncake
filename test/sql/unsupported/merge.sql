CREATE TABLE s (a int) USING columnstore;
INSERT INTO s VALUES (1), (2), (3);

CREATE TABLE t (a int, b int) USING columnstore;
INSERT INTO t VALUES (0), (1);

MERGE INTO t USING s ON t.a = s.a
WHEN MATCHED THEN UPDATE SET b = s.a + 1
WHEN NOT MATCHED THEN INSERT VALUES (s.a, s.a + 1);

DROP TABLE t, s;
