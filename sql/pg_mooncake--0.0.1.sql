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
    secret_name NAME NOT NULL PRIMARY KEY,
    secret_type NAME NOT NULL,
    property TEXT NOT NULL
);

CREATE INDEX secrets_key ON mooncake.secrets (secret_name);

CREATE SCHEMA mooncake_duckdb;

CREATE SEQUENCE mooncake_duckdb.secrets_table_seq START WITH 1 INCREMENT BY 1;
SELECT setval('mooncake_duckdb.secrets_table_seq', 1);

CREATE TABLE mooncake_duckdb.secrets (
    type TEXT NOT NULL,
    key_id TEXT NOT NULL,
    secret TEXT NOT NULL,
    region TEXT,
    session_token TEXT,
    endpoint TEXT,
    r2_account_id TEXT,
    use_ssl BOOLEAN DEFAULT true,
    CONSTRAINT type_constraint CHECK (type IN ('S3', 'GCS', 'R2'))
);

CREATE OR REPLACE FUNCTION mooncake_duckdb.duckdb_update_secrets_table_seq()
RETURNS TRIGGER AS
$$
BEGIN
    PERFORM nextval('mooncake_duckdb.secrets_table_seq');
    RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;

CREATE TRIGGER secrets_table_seq_tr AFTER INSERT OR UPDATE OR DELETE ON mooncake_duckdb.secrets
EXECUTE FUNCTION mooncake_duckdb.duckdb_update_secrets_table_seq();

CREATE OR REPLACE FUNCTION mooncake_duckdb.duckdb_secret_r2_check()
RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.type = 'R2' AND NEW.r2_account_id IS NULL THEN
        Raise Exception '`R2` cloud type secret requires valid `r2_account_id` column value';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;

CREATE TRIGGER duckdb_secret_r2_tr BEFORE INSERT OR UPDATE ON mooncake_duckdb.secrets
FOR EACH ROW EXECUTE PROCEDURE mooncake_duckdb.duckdb_secret_r2_check();

CREATE OR REPLACE FUNCTION pg_catalog.create_secret(
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
    allowed_keys TEXT[] := ARRAY['USE_SSL', 'REGION', 'END_POINT'];
    param_keys TEXT[];
    invalid_keys TEXT[];
BEGIN
    IF type = 'S3' THEN
        param_keys := ARRAY(SELECT jsonb_object_keys(extra_params));

        invalid_keys := ARRAY(SELECT unnest(param_keys) EXCEPT SELECT unnest(allowed_keys));

        -- If there are any invalid keys, raise an exception
        IF array_length(invalid_keys, 1) IS NOT NULL THEN
            RAISE EXCEPTION 'Invalid extra parameters: %', array_to_string(invalid_keys, ', ')
            USING HINT = 'Allowed parameters are USE_SSL, REGION, END_POINT.';
        END IF;

        INSERT INTO mooncake.secrets(secret_name, secret_type, property)
        VALUES (
            name,
            type,
            jsonb_build_object(
                'AWS_ACCESS_KEY_ID', key_id,
                'AWS_SECRET_ACCESS_KEY', secret
            ) ||
            jsonb_strip_nulls(jsonb_build_object(
                'ALLOW_HTTP', NOT((extra_params->>'USE_SSL')::boolean),
                'AWS_REGION', extra_params->>'region',
                'AWS_ENDPOINT', extra_params->>'end_point'
            ))  -- Merge required and optional parameters
        );
        INSERT INTO mooncake_duckdb.secrets
        (type, key_id, secret, session_token, region)
        VALUES (type, key_id, secret, NULL, extra_params->>'region');
    ELSE
        RAISE EXCEPTION 'Unsupported secret type: %', type
        USING HINT = 'Only secrets of type S3 are supported.';
    END IF;
END;
$create_secret$;

CREATE VIEW mooncake.columnstore_tables as
    select relname as table_name, path
    from pg_class p join mooncake.tables m
    on p.oid = m.oid;