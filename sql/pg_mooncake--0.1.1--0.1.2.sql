CREATE TABLE mooncake.delta_update_records (
    -- Postgres transaction id, which inserts the record into the table.
    txn_id INTEGER,
    -- Timestamp when the update record is made.
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Table OID which the update record is made for.
    oid OID,
    -- Directory, where delta table and parquet files are stored.
    dir TEXT,
    -- Json format for delta table options.
    delta_option TEXT,
    -- Array of parquet file paths.
    file_paths TEXT[],
    -- Array of parquet file sizes.
    file_sizes BIGINT[],
    -- Array of whether it's add files flag.
    is_add_files SMALLINT[]
);

CREATE INDEX delta_update_records_oid ON mooncake.delta_update_records (oid);
CREATE INDEX delta_update_records_timestamp ON mooncake.delta_update_records (ts);

CREATE OR REPLACE FUNCTION mooncake.dump_delta_update_records() RETURNS VOID
    LANGUAGE C AS 'MODULE_PATHNAME', 'mooncake_dump_delta_update_records';
