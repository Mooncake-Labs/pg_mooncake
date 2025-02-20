CREATE TABLE mooncake.dead_data_files (
    oid OID NOT NULL,
    file_name TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX dead_data_files_oid ON mooncake.dead_data_files (oid);
CREATE UNIQUE INDEX dead_data_files_file_name ON mooncake.dead_data_files (file_name);
