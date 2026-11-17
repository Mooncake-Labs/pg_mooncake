-- Partitions that existed and held rows before the mirror was created: the initial copy covers them, and later
-- updates (including ones that move a row to another partition) and deletes reach the mirror.
CREATE TABLE p (a int PRIMARY KEY, b text) PARTITION BY RANGE (a);
CREATE TABLE p1 PARTITION OF p FOR VALUES FROM (0) TO (50);
CREATE TABLE p2 PARTITION OF p FOR VALUES FROM (50) TO (100);
INSERT INTO p SELECT g, 'old_' || g FROM generate_series(0, 79) g;
CALL mooncake.create_table('p_mc', 'p');
SELECT count(*), sum(a) FROM p_mc;
UPDATE p SET b = 'changed' WHERE a % 5 = 0;
UPDATE p SET a = a + 50 WHERE a BETWEEN 40 AND 44;
DELETE FROM p WHERE a % 7 = 0;
INSERT INTO p SELECT g, 'new_' || g FROM generate_series(80, 89) g;
SELECT count(*), sum(a), count(*) FILTER (WHERE b = 'changed') FROM p_mc;
SELECT (SELECT md5(string_agg(a::text || ':' || b, ',' ORDER BY a)) FROM p)
     = (SELECT md5(string_agg(a::text || ':' || b, ',' ORDER BY a)) FROM p_mc) AS mirror_matches_source;
DROP TABLE p_mc, p;
