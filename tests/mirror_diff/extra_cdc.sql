-- CDC scenarios the regression suite does not cover well. Run by mirror_diff.py like a regress
-- file: every table is mirrored right after CREATE TABLE and compared after each DML statement.

CREATE TABLE cdc_pk (id int PRIMARY KEY, n int, s text, b bytea, j jsonb, f float8, d numeric(18,6));
CREATE TABLE cdc_nopk (id int, n int, s text);

-- multi-row insert, NULLs in every column
INSERT INTO cdc_pk VALUES (1, 1, 'one', '\x01', '{"a": 1}', 1.5, 1.25),
                          (2, NULL, NULL, NULL, NULL, NULL, NULL),
                          (3, 3, '', '\x', '[]', -0.0, 0);
INSERT INTO cdc_pk SELECT g, g % 7, 'row ' || g, int4send(g), jsonb_build_object('g', g), g / 3.0, g / 7.0
  FROM generate_series(10, 2000) g;
INSERT INTO cdc_nopk SELECT g % 50, g, md5(g::text) FROM generate_series(1, 500) g;

-- NULL transitions and repeated updates of the same rows
UPDATE cdc_pk SET s = NULL, j = NULL WHERE id = 1;
UPDATE cdc_pk SET s = 'back', j = '{"b": [1, 2]}' WHERE id = 1;
UPDATE cdc_pk SET n = n + 1 WHERE id BETWEEN 10 AND 500;
UPDATE cdc_pk SET n = n + 1 WHERE id BETWEEN 10 AND 500;

-- primary key update
UPDATE cdc_pk SET id = id + 100000 WHERE id BETWEEN 1900 AND 2000;

-- UPDATE ... FROM and DELETE ... USING
CREATE TABLE cdc_ref (id int PRIMARY KEY, tag text);
INSERT INTO cdc_ref SELECT g, 'tag' || (g % 5) FROM generate_series(1, 3000, 3) g;
UPDATE cdc_pk p SET s = r.tag FROM cdc_ref r WHERE r.id = p.id;
DELETE FROM cdc_pk p USING cdc_ref r WHERE r.id = p.id AND r.tag = 'tag0';
UPDATE cdc_nopk n SET s = r.tag FROM cdc_ref r WHERE r.id = n.n AND n.id < 10;
DELETE FROM cdc_nopk n USING cdc_ref r WHERE r.id = n.n AND r.tag IN ('tag1', 'tag2');

-- tables without a key: identical duplicate rows, delete/update only some of them
CREATE TABLE cdc_dup (k int, v text);
INSERT INTO cdc_dup SELECT 1, 'same' FROM generate_series(1, 5);
INSERT INTO cdc_dup VALUES (2, 'x'), (2, 'x'), (3, NULL), (3, NULL);
DELETE FROM cdc_dup WHERE ctid = (SELECT min(ctid) FROM cdc_dup WHERE k = 1);
UPDATE cdc_dup SET v = 'changed' WHERE ctid = (SELECT max(ctid) FROM cdc_dup WHERE k = 2);
DELETE FROM cdc_dup WHERE k = 3 AND ctid = (SELECT min(ctid) FROM cdc_dup WHERE k = 3);

-- transactions: rollback, savepoints, insert+update+delete of the same row
BEGIN;
INSERT INTO cdc_pk VALUES (5000, 1, 'rolled back', NULL, NULL, NULL, NULL);
ROLLBACK;
BEGIN;
INSERT INTO cdc_pk VALUES (5001, 1, 'kept', NULL, NULL, NULL, NULL);
SAVEPOINT a;
UPDATE cdc_pk SET s = 'undone' WHERE id = 5001;
INSERT INTO cdc_pk VALUES (5002, 2, 'undone', NULL, NULL, NULL, NULL);
ROLLBACK TO SAVEPOINT a;
INSERT INTO cdc_pk VALUES (5003, 3, 'after savepoint', NULL, NULL, NULL, NULL);
SAVEPOINT b;
DELETE FROM cdc_pk WHERE id = 5003;
RELEASE SAVEPOINT b;
UPDATE cdc_pk SET n = 42 WHERE id = 5001;
COMMIT;
BEGIN;
INSERT INTO cdc_pk VALUES (6000, 1, 'transient', NULL, NULL, NULL, NULL);
UPDATE cdc_pk SET s = 'transient 2' WHERE id = 6000;
DELETE FROM cdc_pk WHERE id = 6000;
INSERT INTO cdc_pk VALUES (6000, 2, 'reinserted', NULL, NULL, NULL, NULL);
COMMIT;
BEGIN;
DELETE FROM cdc_nopk WHERE id = 7;
INSERT INTO cdc_nopk VALUES (7, -1, 'deleted then inserted in one txn');
COMMIT;

-- TOAST: out-of-line values, then updates that leave them unchanged
CREATE TABLE cdc_toast (id int PRIMARY KEY, n int, big text, bin bytea, doc jsonb);
INSERT INTO cdc_toast
  SELECT g, 0,
         (SELECT string_agg(md5((g * 1000 + i)::text), '') FROM generate_series(1, 400) i),
         (SELECT string_agg(sha256(int4send(g * 1000 + i)), '') FROM generate_series(1, 400) i),
         (SELECT jsonb_agg(md5((g + i)::text)) FROM generate_series(1, 400) i)
  FROM generate_series(1, 20) g;
UPDATE cdc_toast SET n = n + 1 WHERE id <= 10;
UPDATE cdc_toast SET big = big || 'x' WHERE id = 11;
UPDATE cdc_toast SET n = -1, bin = NULL WHERE id = 12;
DELETE FROM cdc_toast WHERE id IN (13, 14);
UPDATE cdc_toast SET id = id + 100 WHERE id = 15;

-- a large transaction (spills to disk / may be streamed by the walsender)
INSERT INTO cdc_nopk SELECT -g, g, repeat('l', 100) FROM generate_series(1, 150000) g;
UPDATE cdc_nopk SET n = n + 1 WHERE id < 0 AND id % 3 = 0;
DELETE FROM cdc_nopk WHERE id < 0 AND id % 5 = 0;

-- many small transactions on one row
INSERT INTO cdc_ref VALUES (100001, 'a');
UPDATE cdc_ref SET tag = 'b' WHERE id = 100001;
UPDATE cdc_ref SET tag = 'c' WHERE id = 100001;
DELETE FROM cdc_ref WHERE id = 100001;
INSERT INTO cdc_ref VALUES (100001, 'd');

-- TRUNCATE of a mirrored source (the harness normally drops the mirror first; md:unprotected)
CREATE TABLE cdc_trunc (id int PRIMARY KEY, v text);
INSERT INTO cdc_trunc SELECT g, 'v' FROM generate_series(1, 100) g;
TRUNCATE cdc_trunc; -- md:unprotected
INSERT INTO cdc_trunc VALUES (1, 'after truncate');

-- Iceberg persistence: create_snapshot right after a write in the same session (no compare in
-- between: a SELECT that prunes pages would move the session's LSN to WAL that never commits). The next DML
-- compares the mirror again, now reading the persisted snapshot.
UPDATE cdc_pk SET n = n + 1000 WHERE id < 100; -- md:no-compare
CALL mooncake.create_snapshot('mc.cdc_pk'); -- md:check=snapshot_after_write
UPDATE cdc_pk SET n = n - 1000 WHERE id < 100;

-- create_snapshot right after a transaction that wrote WAL without an xid. A
-- non-transactional logical message is the deterministic stand-in for HOT pruning in a SELECT.
SET statement_timeout = '20s';
SELECT pg_logical_emit_message(false, 'mirror_diff', 'xid-less WAL') IS NOT NULL AS emitted;
CALL mooncake.create_snapshot('mc.cdc_pk'); -- md:check=snapshot_after_xidless_wal
RESET statement_timeout;
