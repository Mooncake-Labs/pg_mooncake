DROP TABLESPACE IF EXISTS mooncake_ts;
CREATE TABLESPACE mooncake_ts LOCATION '/tmp/tablespace_dir';
CREATE TABLE t (a int) USING columnstore TABLESPACE mooncake_ts;
SELECT table_name, regexp_replace(path, '[0-9]+/$', 'XXXX/') AS path FROM mooncake.columnstore_tables;
DROP TABLE t;
DROP TABLESPACE mooncake_ts;
