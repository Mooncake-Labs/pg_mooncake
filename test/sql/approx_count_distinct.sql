CREATE TABLE t (a int, b text) USING columnstore;
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
INSERT INTO t VALUES (2, 'f'), (3, 'g'), (4, 'h');
SELECT mooncake.approx_count_distinct(a), mooncake.approx_count_distinct(b) FROM t;
DROP TABLE t;
