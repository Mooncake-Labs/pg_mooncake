CREATE OR REPLACE FUNCTION mooncake.create_secret(
    name TEXT,
    type TEXT,
    key_id TEXT,
    secret TEXT,
    extra_params JSONB DEFAULT '{}'::JSONB
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $create_secret$
DECLARE
    allowed_keys TEXT[] := ARRAY['ENDPOINT', 'REGION', 'SCOPE', 'URL_STYLE', 'USE_SSL'];
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
            USING HINT = 'Allowed parameters are ENDPOINT, REGION, SCOPE, URL_STYLE, USE_SSL.';
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
                CASE WHEN extra_params->>'URL_STYLE' IS NULL THEN '' ELSE format(', URL_STYLE %L', extra_params->>'URL_STYLE') END ||
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
