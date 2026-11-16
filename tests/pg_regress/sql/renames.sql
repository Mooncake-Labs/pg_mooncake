\set VERBOSITY terse
SET client_min_messages = warning;
CREATE SCHEMA rn;
CREATE TABLE rn.src (a int PRIMARY KEY);
INSERT INTO rn.src VALUES (1);
CALL mooncake.create_table('rn.m', 'rn.src');
-- moonlink keeps a mirror under the name it was created with, so a mirror cannot be renamed or moved, and neither can
-- its schema.
\set VERBOSITY default
ALTER TABLE rn.m RENAME TO m2;
\set VERBOSITY terse
ALTER TABLE IF EXISTS rn.m RENAME TO m2;
-- Postgres lets ALTER INDEX ... RENAME rename a table too.
ALTER INDEX rn.m RENAME TO m2;
ALTER TABLE rn.m SET SCHEMA public;
ALTER SCHEMA rn RENAME TO rn2;
-- Other tables and schemas without mirrors are unaffected.
CREATE TABLE rn.other (a int);
ALTER TABLE rn.other RENAME TO other2;
ALTER TABLE rn.other2 SET SCHEMA public;
CREATE SCHEMA rn_empty;
ALTER SCHEMA rn_empty RENAME TO rn_empty2;
ALTER TABLE IF EXISTS rn.missing RENAME TO missing2;
-- The mirror keeps replicating.
INSERT INTO rn.src VALUES (2);
CALL mooncake.create_snapshot('rn.m');
SELECT * FROM rn.m ORDER BY a;
DROP SCHEMA rn CASCADE;
DROP SCHEMA rn_empty2;
DROP TABLE public.other2;
