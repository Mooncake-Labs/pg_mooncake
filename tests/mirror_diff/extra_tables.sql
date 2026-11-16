-- Source-table shapes beyond plain heap tables. Each CREATE TABLE is mirrored by the harness;
-- the result shows whether create_table rejects the shape or the mirror diverges.

-- dropped column before mirroring
CREATE TABLE shp_dropped (id int PRIMARY KEY, gone text, v int);
INSERT INTO shp_dropped VALUES (1, 'x', 10), (2, 'y', 20);
ALTER TABLE shp_dropped DROP COLUMN gone;
INSERT INTO shp_dropped VALUES (3, 30);
UPDATE shp_dropped SET v = v + 1;

-- stored generated column
CREATE TABLE shp_generated (id int PRIMARY KEY, a int, twice int GENERATED ALWAYS AS (a * 2) STORED);
INSERT INTO shp_generated (id, a) VALUES (1, 1), (2, 2);
UPDATE shp_generated SET a = 10 WHERE id = 1;

-- identity column and defaults
CREATE TABLE shp_identity (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text DEFAULT 'dflt');
INSERT INTO shp_identity (v) VALUES ('a'), (DEFAULT);

-- unlogged table (its changes are not logically decoded)
CREATE UNLOGGED TABLE shp_unlogged (id int PRIMARY KEY, v text);
INSERT INTO shp_unlogged VALUES (1, 'before mirror');
INSERT INTO shp_unlogged VALUES (2, 'after mirror');

-- inheritance parent with a child: SELECT on the parent includes child rows
CREATE TABLE shp_parent (id int, v text);
CREATE TABLE shp_child (extra int) INHERITS (shp_parent);
INSERT INTO shp_parent VALUES (1, 'parent row');
INSERT INTO shp_child VALUES (2, 'child row', 0);
SELECT * FROM shp_parent ORDER BY id;

-- REPLICA IDENTITY USING INDEX
CREATE TABLE shp_ri_index (id int NOT NULL, v text);
CREATE UNIQUE INDEX shp_ri_index_id ON shp_ri_index (id);
ALTER TABLE shp_ri_index REPLICA IDENTITY USING INDEX shp_ri_index_id;
INSERT INTO shp_ri_index VALUES (1, 'a'), (2, 'b');
UPDATE shp_ri_index SET v = 'c' WHERE id = 1;
DELETE FROM shp_ri_index WHERE id = 2;

-- mixed-case and special column names
CREATE TABLE shp_colnames ("Id" int PRIMARY KEY, "two words" text, "select" int, "ünï" text);
INSERT INTO shp_colnames VALUES (1, 'a', 1, 'u'), (2, 'b', 2, NULL);
UPDATE shp_colnames SET "two words" = 'c' WHERE "Id" = 2;
SELECT "Id", "two words", "select", "ünï" FROM shp_colnames ORDER BY 1;

-- wide row
CREATE TABLE shp_wide AS SELECT g AS id,
  g AS c01, g AS c02, g AS c03, g AS c04, g AS c05, g AS c06, g AS c07, g AS c08, g AS c09, g AS c10,
  g::text AS t01, g::text AS t02, g::text AS t03, g::text AS t04, g::text AS t05
  FROM generate_series(1, 100) g;
UPDATE shp_wide SET c05 = -c05 WHERE id % 2 = 0;

-- partitioned table (mirrored through the parent)
CREATE TABLE shp_part (id int, k int, v text) PARTITION BY RANGE (k);
CREATE TABLE shp_part_1 PARTITION OF shp_part FOR VALUES FROM (0) TO (10);
CREATE TABLE shp_part_2 PARTITION OF shp_part FOR VALUES FROM (10) TO (20);
INSERT INTO shp_part SELECT g, g % 20, 'p' FROM generate_series(1, 40) g;
DELETE FROM shp_part WHERE id = 1;
UPDATE shp_part SET v = 'q' WHERE id = 2;

-- Statements that must fail cleanly or succeed on a session that has used DuckDB; each crashed
-- the backend (and with it the whole server) at the baseline. The time zone check runs first:
-- after a crash, later statements would no longer have DuckDB initialised.
SELECT count(*) FROM mc.shp_wide;
BEGIN;
SET LOCAL TIME ZONE 'Europe/Paris';
COMMIT; -- md:check=set_local_timezone_commit_after_duckdb
SELECT count(*) FROM mc.shp_wide;
UPDATE mc.shp_ri_index SET (v, id) = (SELECT 'z', 1); -- md:check-error=multiassign_update_on_mirror
