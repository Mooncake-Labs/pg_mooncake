CREATE TABLE mooncake.delta_update_records (
    -- Timestamp of nanoseconds since epoch unix of the record when it's inserted into the table, which is used to indicate the insertion order.
    -- Intentionally we don't add any index to the table, because we cannot open index at commit stage.
    timestamp BIGINT,
    -- Directory, where delta table and parquet files are stored.
    path TEXT,
    -- Json format for delta table options.
    delta_option TEXT,
    -- Array of parquet file paths.
    file_paths TEXT[],
    -- Array of parquet file sizes.
    file_sizes BIGINT[],
    -- Array of whether it's add files flag.
    is_add_files SMALLINT[]
);

CREATE OR REPLACE FUNCTION mooncake.dump_delta_update_records() RETURNS VOID
    LANGUAGE C AS 'MODULE_PATHNAME', 'mooncake_dump_delta_update_records';
