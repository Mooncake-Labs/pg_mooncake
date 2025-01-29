CREATE PROCEDURE mooncake.reset_duckdb()
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'mooncake_reset_duckdb';
