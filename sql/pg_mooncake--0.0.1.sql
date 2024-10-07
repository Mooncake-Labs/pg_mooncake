CREATE SCHEMA mooncake;

CREATE TABLE mooncake.table_info (
    relid OID NOT NULL primary key,
    storage_path TEXT NOT NULL,
    lake_format TEXT NOT NULL,
    lakehouse_options JSONB
);

CREATE TABLE mooncake.data_files (
    relid OID NOT NULL,
    file_name TEXT NOT NULL
);

CREATE FUNCTION columnstore_handler(internal) RETURNS table_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER columnstore_handler;
