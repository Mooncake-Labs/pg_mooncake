CREATE SCHEMA mooncake;

CREATE TABLE mooncake.data_files (
    relid OID NOT NULL,
    file_name TEXT NOT NULL
);

CREATE FUNCTION columnstore_handler(internal) RETURNS table_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER columnstore_handler;
