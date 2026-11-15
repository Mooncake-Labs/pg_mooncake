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
-- Reads and writes to the source still work.
UPDATE w SET b = 'y';
SELECT * FROM w_mc;
DROP TABLE w_mc, w;
