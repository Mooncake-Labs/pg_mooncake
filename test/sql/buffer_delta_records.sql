CREATE TABLE t (a INT) USING columnstore;

INSERT INTO t VALUES (1), (2), (3);
INSERT INTO t VALUES (1), (2), (3);
SELECT COUNT(*) FROM mooncake.delta_update_records;

DELETE FROM t WHERE a = 1;
SELECT COUNT(*) FROM mooncake.delta_update_records;

-- Request to flush buffered records.
SELECT mooncake.dump_delta_update_records();

-- Check buffered records after flush.
SELECT COUNT(*) FROM mooncake.delta_update_records;

-- Check buffered records after flush.
SELECT COUNT(*) FROM mooncake.delta_update_records;

-- Rows inserted inside of an aborted transaction are deleted by postgres.
BEGIN;
INSERT INTO t VALUES (1), (2), (3);
SELECT COUNT(*) FROM mooncake.delta_update_records;
-- Request to flush buffered records inside of an ongoing transaction doesn't flush any records.
SELECT mooncake.dump_delta_update_records();
SELECT COUNT(*) FROM mooncake.delta_update_records;
ROLLBACK;
SELECT COUNT(*) FROM mooncake.delta_update_records;

-- Drop the table after test.
DROP TABLE t;
