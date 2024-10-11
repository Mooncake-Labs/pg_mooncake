CREATE FUNCTION columnstore_handler(internal) RETURNS table_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER columnstore_handler;

CREATE SCHEMA mooncake;

CREATE TABLE mooncake.tables (
    oid OID NOT NULL,
    path TEXT NOT NULL
);
CREATE UNIQUE INDEX tables_oid ON mooncake.tables (oid);

CREATE TABLE mooncake.data_files (
    oid OID NOT NULL,
    file_name TEXT NOT NULL
);
CREATE INDEX data_files_oid ON mooncake.data_files (oid);
CREATE UNIQUE INDEX data_files_file_name ON mooncake.data_files (file_name);
