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
    allowed_keys TEXT[] := ARRAY['ENDPOINT', 'REGION', 'SCOPE', 'USE_SSL'];
    keys TEXT[];
    invalid_keys TEXT[];
    delta_endpoint TEXT;
BEGIN
    IF type = 'S3' THEN
        keys := ARRAY(SELECT jsonb_object_keys(extra_params));
        invalid_keys := ARRAY(SELECT unnest(keys) EXCEPT SELECT unnest(allowed_keys));
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
    ELSE
        RAISE EXCEPTION 'Unsupported secret type: %', type
        USING HINT = 'Only secrets of type S3 are supported.';
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
