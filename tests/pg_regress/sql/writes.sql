\set VERBOSITY terse
CREATE TABLE w (a int PRIMARY KEY, b text);
INSERT INTO w VALUES (1, 'x');
CALL mooncake.create_table('w_mc', 'w');
-- Mirrors are read-only; writes fail before pg_duckdb plans them.
INSERT INTO w_mc VALUES (2, 'y');
UPDATE w_mc SET b = 'z';
UPDATE w_mc SET (b, a) = (SELECT 'z', 1);
INSERT INTO w_mc VALUES (1, 'q') ON CONFLICT (a) DO UPDATE SET (b, a) = (SELECT 'q', 1);
DELETE FROM w_mc;
WITH d AS (DELETE FROM w_mc RETURNING *) SELECT count(*) FROM d;
EXPLAIN (COSTS OFF) UPDATE w_mc SET b = 'z';
-- MERGE too (PostgreSQL 14 has no MERGE, see writes_1.out).
MERGE INTO w_mc t USING w s ON t.a = s.a WHEN MATCHED THEN DELETE;
-- So are TRUNCATE (alone or with other tables) and COPY FROM.
TRUNCATE w_mc;
TRUNCATE w, w_mc;
COPY w_mc FROM '/dev/null';
-- A mirror cannot become a partition or an inheritance child: writes to the parent would reach it.
CREATE TABLE w_parted (a int NOT NULL, b text) PARTITION BY RANGE (a);
ALTER TABLE w_parted ATTACH PARTITION w_mc FOR VALUES FROM (0) TO (10);
CREATE TABLE w_parent (a int NOT NULL, b text);
ALTER TABLE w_mc INHERIT w_parent;
DROP TABLE w_parted, w_parent;
-- Nor can it become a regular table (PostgreSQL 14 has no SET ACCESS METHOD, see writes_1.out).
ALTER TABLE w_mc SET ACCESS METHOD heap;
-- COPY out of a mirror is a read.
COPY (SELECT * FROM w_mc ORDER BY a) TO STDOUT;
-- Reads and writes to the source still work.
UPDATE w SET b = 'y';
SELECT * FROM w_mc ORDER BY a;
DROP TABLE w_mc, w;
