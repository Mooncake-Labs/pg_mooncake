CREATE FUNCTION columnstore_handler(internal) RETURNS table_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER columnstore_handler;

CREATE SCHEMA mooncake;

CREATE TABLE mooncake.tables (
    oid OID NOT NULL,
    path TEXT NOT NULL,
    timeline_id TEXT NOT NULL
);
CREATE UNIQUE INDEX tables_oid ON mooncake.tables (oid);

CREATE TABLE mooncake.data_files (
    oid OID NOT NULL,
    file_name TEXT NOT NULL,
    file_metadata BYTEA NOT NULL
);
CREATE INDEX data_files_oid ON mooncake.data_files (oid);
CREATE UNIQUE INDEX data_files_file_name ON mooncake.data_files (file_name);

CREATE FUNCTION mooncake.read_parquet(path text, binary_as_string BOOLEAN DEFAULT FALSE,
                                                   filename BOOLEAN DEFAULT FALSE,
                                                   file_row_number BOOLEAN DEFAULT FALSE,
                                                   hive_partitioning BOOLEAN DEFAULT FALSE,
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_parquet(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION mooncake.read_parquet(path text[], binary_as_string BOOLEAN DEFAULT FALSE,
                                                     filename BOOLEAN DEFAULT FALSE,
                                                     file_row_number BOOLEAN DEFAULT FALSE,
                                                     hive_partitioning BOOLEAN DEFAULT FALSE,
                                                     union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_parquet(TEXT[])` only works with Duckdb execution.';
END;
$func$;

-- Arguments 'columns' and 'nullstr' are currently not supported for read_csv

CREATE FUNCTION mooncake.read_csv(path text, all_varchar BOOLEAN DEFAULT FALSE,
                                               allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                               auto_detect BOOLEAN DEFAULT TRUE,
                                               auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               compression VARCHAR DEFAULT 'auto',
                                               dateformat VARCHAR DEFAULT '',
                                               decimal_separator VARCHAR DEFAULT '.',
                                               delim VARCHAR DEFAULT ',',
                                               escape VARCHAR DEFAULT '"',
                                               filename BOOLEAN DEFAULT FALSE,
                                               force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               header BOOLEAN DEFAULT FALSE,
                                               hive_partitioning BOOLEAN DEFAULT FALSE,
                                               ignore_errors BOOLEAN DEFAULT FALSE,
                                               max_line_size BIGINT DEFAULT 2097152,
                                               names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               new_line VARCHAR DEFAULT '',
                                               normalize_names BOOLEAN DEFAULT FALSE,
                                               null_padding BOOLEAN DEFAULT FALSE,
                                               nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               parallel BOOLEAN DEFAULT FALSE,
                                               quote VARCHAR DEFAULT '"',
                                               sample_size BIGINT DEFAULT 20480,
                                               sep VARCHAR DEFAULT ',',
                                               skip BIGINT DEFAULT 0,
                                               timestampformat VARCHAR DEFAULT '',
                                               types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_csv(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION mooncake.read_csv(path text[],  all_varchar BOOLEAN DEFAULT FALSE,
                                                  allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                                  auto_detect BOOLEAN DEFAULT TRUE,
                                                  auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  compression VARCHAR DEFAULT 'auto',
                                                  dateformat VARCHAR DEFAULT '',
                                                  decimal_separator VARCHAR DEFAULT '.',
                                                  delim VARCHAR DEFAULT ',',
                                                  escape VARCHAR DEFAULT '"',
                                                  filename BOOLEAN DEFAULT FALSE,
                                                  force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  header BOOLEAN DEFAULT FALSE,
                                                  hive_partitioning BOOLEAN DEFAULT FALSE,
                                                  ignore_errors BOOLEAN DEFAULT FALSE,
                                                  max_line_size BIGINT DEFAULT 2097152,
                                                  names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  new_line VARCHAR DEFAULT '',
                                                  normalize_names BOOLEAN DEFAULT FALSE,
                                                  null_padding BOOLEAN DEFAULT FALSE,
                                                  nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  parallel BOOLEAN DEFAULT FALSE,
                                                  quote VARCHAR DEFAULT '"',
                                                  sample_size BIGINT DEFAULT 20480,
                                                  sep VARCHAR DEFAULT ',',
                                                  skip BIGINT DEFAULT 0,
                                                  timestampformat VARCHAR DEFAULT '',
                                                  types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_csv(TEXT[])` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION mooncake.read_json(path text, auto_detect BOOLEAN DEFAULT FALSE,
                                                 compression VARCHAR DEFAULT 'auto',
                                                 dateformat VARCHAR DEFAULT 'iso',
                                                 format VARCHAR DEFAULT 'array',
                                                 ignore_errors BOOLEAN DEFAULT FALSE,
                                                 maximum_depth BIGINT DEFAULT -1,
                                                 maximum_object_size INT DEFAULT 16777216,
                                                 records VARCHAR DEFAULT 'records',
                                                 sample_size BIGINT DEFAULT 20480,
                                                 timestampformat VARCHAR DEFAULT 'iso',
                                                 union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_json(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION mooncake.read_json(path text[], auto_detect BOOLEAN DEFAULT FALSE,
                                                   compression VARCHAR DEFAULT 'auto',
                                                   dateformat VARCHAR DEFAULT 'iso',
                                                   format VARCHAR DEFAULT 'array',
                                                   ignore_errors BOOLEAN DEFAULT FALSE,
                                                   maximum_depth BIGINT DEFAULT -1,
                                                   maximum_object_size INT DEFAULT 16777216,
                                                   records VARCHAR DEFAULT 'records',
                                                   sample_size BIGINT DEFAULT 20480,
                                                   timestampformat VARCHAR DEFAULT 'iso',
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_json(TEXT[])` only works with Duckdb execution.';
END;
$func$;

-- iceberg_* functions optional parameters are extract from source code;
-- https://github.com/duckdb/duckdb_iceberg/tree/main/src/iceberg_functions

CREATE FUNCTION mooncake.iceberg_scan(path text, allow_moved_paths BOOLEAN DEFAULT FALSE,
                                                   mode TEXT DEFAULT '',
                                                   metadata_compression_codec TEXT DEFAULT 'none',
                                                   skip_schema_inference BOOLEAN DEFAULT FALSE,
                                                   version TEXT DEFAULT 'version-hint.text',
                                                   version_name_format TEXT DEFAULT 'v%s%s.metadata.json,%s%s.metadata.json')
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `iceberg_scan(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE TYPE mooncake.iceberg_metadata_record AS (
  manifest_path TEXT,
  manifest_sequence_number NUMERIC,
  manifest_content  TEXT,
  status TEXT,
  content TEXT,
  file_path TEXT
);

CREATE FUNCTION mooncake.iceberg_metadata(path text, allow_moved_paths BOOLEAN DEFAULT FALSE,
                                                       metadata_compression_codec TEXT DEFAULT 'none',
                                                       skip_schema_inference BOOLEAN DEFAULT FALSE,
                                                       version TEXT DEFAULT 'version-hint.text',
                                                       version_name_format TEXT DEFAULT 'v%s%s.metadata.json,%s%s.metadata.json')
RETURNS SETOF mooncake.iceberg_metadata_record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `iceberg_metadata(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE TYPE mooncake.iceberg_snapshots_record AS (
  sequence_number BIGINT,
  snapshot_id BIGINT,
  timestamp_ms TIMESTAMP,
  manifest_list TEXT
);

CREATE FUNCTION mooncake.iceberg_snapshots(path text, metadata_compression_codec TEXT DEFAULT 'none',
                                                        skip_schema_inference BOOLEAN DEFAULT FALSE,
                                                        version TEXT DEFAULT 'version-hint.text',
                                                        version_name_format TEXT DEFAULT 'v%s%s.metadata.json,%s%s.metadata.json')
RETURNS SETOF mooncake.iceberg_snapshots_record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `iceberg_snapshots(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION mooncake.delta_scan(path text)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `delta_scan(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION mooncake.approx_count_distinct_sfunc(bigint, anyelement)
RETURNS bigint LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Aggregate `approx_count_distinct(ANYELEMENT)` only works with Duckdb execution.';
END;
$func$;

CREATE AGGREGATE mooncake.approx_count_distinct(anyelement)
(
    sfunc = mooncake.approx_count_distinct_sfunc,
    stype = bigint,
    initcond = 0
);

CREATE TABLE mooncake.secrets (
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    scope TEXT NOT NULL,
    duckdb_query TEXT NOT NULL,
    delta_options TEXT NOT NULL
);
CREATE UNIQUE INDEX secrets_name ON mooncake.secrets (name);

CREATE SEQUENCE mooncake.secrets_table_seq START WITH 1 INCREMENT BY 1;
SELECT setval('mooncake.secrets_table_seq', 1);

CREATE OR REPLACE FUNCTION mooncake.create_secret(
    name TEXT,
    type TEXT,
    key_id TEXT,
    secret TEXT,
    extra_params JSONB DEFAULT '{}'::JSONB
)
RETURNS VOID
LANGUAGE plpgsql
AS $create_secret$
DECLARE
    s3_allowed_keys TEXT[] := ARRAY['ENDPOINT', 'REGION', 'SCOPE', 'USE_SSL'];
    gcs_allowed_keys TEXT[] := ARRAY['GCS_SECRET', 'PATH', 'SCOPE'];
    gcs_required_keys TEXT[] := ARRAY['type', 'project_id', 'private_key_id', 'private_key', 'client_email', 'client_id'];

    keys TEXT[];
    invalid_keys TEXT[];
    delta_endpoint TEXT;
    gcs_service_account_json JSONB;
BEGIN
    IF type = 'S3' THEN
        keys := ARRAY(SELECT jsonb_object_keys(extra_params));
        invalid_keys := ARRAY(SELECT unnest(keys) EXCEPT SELECT unnest(s3_allowed_keys));
        -- If there are any invalid keys, raise an exception
        IF array_length(invalid_keys, 1) IS NOT NULL THEN
            RAISE EXCEPTION 'Invalid extra parameters: %', array_to_string(invalid_keys, ', ')
            USING HINT = 'Allowed parameters are ENDPOINT, REGION, SCOPE, USE_SSL.';
        END IF;
        delta_endpoint = NULL;
        IF extra_params->>'ENDPOINT' LIKE '%://%' THEN
            RAISE EXCEPTION 'Invalid ENDPOINT format: %', extra_params->>'ENDPOINT'
            USING HINT = 'USE domain name excluding http prefix';
        END IF;
        IF extra_params->>'ENDPOINT' is NOT NULL and NOT(extra_params->>'ENDPOINT' LIKE 's3express%') THEN
            IF (extra_params->>'USE_SSL')::boolean = false THEN
                delta_endpoint = CONCAT('http://', extra_params->>'ENDPOINT');
            ELSE
                delta_endpoint = CONCAT('https://', extra_params->>'ENDPOINT');
            END IF;
        END IF;
        INSERT INTO mooncake.secrets VALUES (
            name,
            type,
            coalesce(extra_params->>'SCOPE', ''),
            format('CREATE SECRET "duckdb_secret_%s" (TYPE %s, KEY_ID %L, SECRET %L', name, type, key_id, secret) ||
                CASE WHEN extra_params->>'REGION' IS NULL THEN '' ELSE format(', REGION %L', extra_params->>'REGION') END ||
                CASE WHEN extra_params->>'ENDPOINT' IS NULL THEN '' ELSE format(', ENDPOINT %L', extra_params->>'ENDPOINT') END ||
                CASE WHEN (extra_params->>'USE_SSL')::boolean = false THEN ', USE_SSL FALSE' ELSE '' END ||
                CASE WHEN extra_params->>'SCOPE' IS NULL THEN '' ELSE format(', SCOPE %L', extra_params->>'SCOPE') END ||
                ');',
            jsonb_build_object('AWS_ACCESS_KEY_ID', key_id, 'AWS_SECRET_ACCESS_KEY', secret) ||
                jsonb_strip_nulls(jsonb_build_object(
                    'ALLOW_HTTP', (NOT (extra_params->>'USE_SSL')::boolean)::varchar,
                    'AWS_REGION', extra_params->>'REGION',
                    'AWS_ENDPOINT', delta_endpoint,
                    'AWS_S3_EXPRESS', (NULLIF(extra_params->>'ENDPOINT' LIKE 's3express%', false))::varchar
                ))
        );
        PERFORM nextval('mooncake.secrets_table_seq');
    ELSIF type = 'GCS' THEN
        keys := ARRAY(SELECT jsonb_object_keys(extra_params));
        invalid_keys := ARRAY(SELECT unnest(keys) EXCEPT SELECT unnest(gcs_allowed_keys));
        IF array_length(invalid_keys, 1) IS NOT NULL THEN
            RAISE EXCEPTION 'Invalid extra parameters: %', array_to_string(invalid_keys, ', ')
            USING HINT = 'Allowed parameters are SCOPE, PATH, GCS_SECRET';
        END IF;
        IF extra_params->>'GCS_SECRET' IS NOT NULL THEN
            gcs_service_account_json := (extra_params->>'GCS_SECRET')::JSONB; -- should be a valid JSON string
        ELSE
            IF extra_params->>'PATH' IS NOT NULL THEN
                IF pg_read_file(extra_params->>'PATH', 0, 1) IS NULL THEN
                    RAISE EXCEPTION 'Service Account file not found or unreadable.'
                    USING HINT = 'Service Account file should be a valid JSON file.';
                ELSE
                    gcs_service_account_json := (pg_read_file(extra_params->>'PATH'))::JSONB;
                END IF;
            ELSE
                RAISE EXCEPTION 'GCP Service Account JSON can not be empty.'
                USING HINT = 'Provide a valid Service Account JSON using GCS_SECRET or PATH.';
            END IF;
        END IF;
        IF NOT(gcs_service_account_json::JSONB ?& gcs_required_keys) THEN
            RAISE EXCEPTION 'Missing required fields in Service Account JSON.'
            USING HINT = 'Required fields in Service accoount JSON: '|| array_to_string(gcs_required_keys, ', ');
        END IF;
        INSERT INTO mooncake.secrets VALUES (
            name,
            type,
            coalesce(extra_params->>'SCOPE', ''),
            format('CREATE SECRET "duckdb_secret_%s" (TYPE %s, KEY_ID %L, SECRET %L', name, type, key_id, secret) ||
                CASE WHEN extra_params->>'SCOPE' IS NULL THEN '' ELSE format(', SCOPE %L', extra_params->>'SCOPE') END ||
                ');',
                jsonb_build_object('service_account_key', (gcs_service_account_json)::VARCHAR)
        );
        PERFORM nextval('mooncake.secrets_table_seq');
    ELSE
        RAISE EXCEPTION 'Unsupported secret type: %', type
        USING HINT = 'Only secrets of type S3/GCS are supported.';
    END IF;
END;
$create_secret$ SECURITY DEFINER;

CREATE OR REPLACE FUNCTION mooncake.drop_secret(secret_name TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $drop_secret$
BEGIN
    DELETE FROM mooncake.secrets WHERE name = secret_name;
    PERFORM nextval('mooncake.secrets_table_seq');
END;
$drop_secret$ SECURITY DEFINER;

CREATE VIEW mooncake.columnstore_tables AS
    SELECT relname AS table_name, path FROM pg_class JOIN mooncake.tables ON pg_class.oid = tables.oid;

CREATE VIEW mooncake.cloud_secrets AS
    SELECT name, type, scope FROM mooncake.secrets;

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA mooncake FROM PUBLIC;
GRANT USAGE ON SCHEMA mooncake TO PUBLIC;
GRANT SELECT ON mooncake.secrets_table_seq TO PUBLIC;
GRANT SELECT ON mooncake.columnstore_tables TO PUBLIC;
GRANT SELECT ON mooncake.cloud_secrets TO PUBLIC;
