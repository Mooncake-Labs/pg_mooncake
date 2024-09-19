git config --global --add safe.directory /workspaces/pg_mooncake
git config --global --add safe.directory /workspaces/pg_mooncake/third_party/duckdb

pg_ctl -D /usr/local/pgsql/data start
dropdb --if-exists mooncake
createdb mooncake
