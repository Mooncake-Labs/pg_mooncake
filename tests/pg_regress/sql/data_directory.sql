-- moonlink's directory is set at server start; the default is relative to the data directory.
SELECT name, setting, context FROM pg_settings WHERE name = 'mooncake.data_directory';
SET mooncake.data_directory = '/tmp';
