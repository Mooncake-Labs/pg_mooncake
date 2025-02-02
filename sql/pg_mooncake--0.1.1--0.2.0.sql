DROP FUNCTION IF EXISTS mooncake.create_secret;

CREATE OR REPLACE FUNCTION mooncake.create_secret(
    name TEXT,
    type TEXT,
    key_id TEXT,
    secret TEXT,
    extra_params JSONB DEFAULT '{}'::JSONB
)
RETURNS VOID
AS 'MODULE_PATHNAME', 'mooncake_create_secret'
LANGUAGE C STRICT
SECURITY DEFINER;
