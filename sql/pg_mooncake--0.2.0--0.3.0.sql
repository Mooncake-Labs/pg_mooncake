-- Upgrade pg_mooncake from 0.2.0 to 0.3.0: ALTER EXTENSION pg_mooncake UPDATE TO '0.3.0';

-- mooncake.load_files was removed. 0.2.0 builds from before it was added don't have it.
DROP PROCEDURE IF EXISTS mooncake.load_files(text, text[]);

-- Mirrors created by 0.2.0 relied on moonlink's write-ahead log, which 0.3.0 no longer has, so they can be missing
-- changes after the upgrade. Point at the ones that still need to be re-created.
DO $$
DECLARE
    mirrors text;
BEGIN
    SELECT string_agg(c.oid::regclass::text, ', ' ORDER BY c.oid::regclass::text)
    INTO mirrors
    FROM pg_class c
    JOIN pg_am a ON a.oid = c.relam
    WHERE a.amname = 'mooncake';
    IF mirrors IS NOT NULL THEN
        RAISE WARNING 'mirrors created before pg_mooncake 0.3.0 must be dropped and re-created: %', mirrors
            USING HINT = 'For each mirror: DROP TABLE <mirror>; CALL mooncake.create_table(''<mirror>'', ''<source>'');';
    END IF;
END
$$;
