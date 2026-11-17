\set VERBOSITY terse
SET client_min_messages = notice;
CREATE TABLE r (a int PRIMARY KEY);
CALL mooncake.create_table('c', 'r');
INSERT INTO r VALUES (1), (2);
-- A failed moonlink call leaves the session usable.
DO $$
BEGIN
  CALL mooncake.create_snapshot('no_such_mirror');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'create_snapshot failed';
END
$$;
SELECT "table" FROM mooncake.list_tables() WHERE "table" = 'public.c';
CALL mooncake.create_snapshot('c');
SELECT * FROM c ORDER BY a;
-- A drop undone by ROLLBACK TO SAVEPOINT keeps the mirror in moonlink.
BEGIN;
SAVEPOINT s;
DROP TABLE c;
ROLLBACK TO SAVEPOINT s;
COMMIT;
SELECT "table" FROM mooncake.list_tables() WHERE "table" = 'public.c';
SELECT * FROM c ORDER BY a;
-- So does a drop undone by an exception block.
DO $$
BEGIN
  DROP TABLE c;
  RAISE EXCEPTION 'undo';
EXCEPTION WHEN raise_exception THEN
  NULL;
END
$$;
INSERT INTO r VALUES (3);
SELECT * FROM c ORDER BY a;
-- Two-phase commit is rejected.
BEGIN;
DROP TABLE c;
PREPARE TRANSACTION 'drop_c';
SELECT * FROM c ORDER BY a;
-- Drops through DROP SCHEMA ... CASCADE reach moonlink.
CREATE SCHEMA s;
CREATE TABLE s.r1 (a int PRIMARY KEY);
CREATE TABLE s.r2 (a int PRIMARY KEY);
CALL mooncake.create_table('s.m1', 's.r1');
CALL mooncake.create_table('s.m2', 's.r2');
SELECT "table" FROM mooncake.list_tables() WHERE "table" LIKE 's.%' ORDER BY 1;
DROP SCHEMA s CASCADE;
SELECT "table" FROM mooncake.list_tables() WHERE "table" LIKE 's.%';
-- A DROP that moonlink never saw (here: event triggers are off in replica mode) leaves moonlink's table behind.
-- Re-creating the mirror under the same name replaces it instead of reading the old table.
CREATE TABLE r_old (a int PRIMARY KEY);
INSERT INTO r_old VALUES (1), (2);
CALL mooncake.create_table('o', 'r_old');
SET session_replication_role = replica;
DROP TABLE o;
RESET session_replication_role;
SELECT "table" FROM mooncake.list_tables() WHERE "table" = 'public.o';
CREATE TABLE r_new (a int PRIMARY KEY);
INSERT INTO r_new VALUES (10);
CALL mooncake.create_table('o', 'r_new');
SELECT * FROM o ORDER BY a;
SELECT count(*) FROM mooncake.list_tables() WHERE "table" = 'public.o';
-- A create_table that moonlink rejects leaves no mirror table behind.
CREATE TABLE r_bad (a int PRIMARY KEY, i interval);
DO $$
BEGIN
  CALL mooncake.create_table('f', 'r_bad');
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'create_table failed';
END
$$;
SELECT to_regclass('f') IS NULL AS no_table;
SELECT count(*) FROM mooncake.list_tables() WHERE "table" = 'public.f';
DROP TABLE o, r_old, r_new, r_bad;
DROP TABLE c, r;
SELECT count(*) FROM mooncake.list_tables();
