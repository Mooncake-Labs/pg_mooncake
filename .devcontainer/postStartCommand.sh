git config --global --add safe.directory /workspaces/pg_mooncake
git config --global --add safe.directory /workspaces/pg_mooncake/third_party/duckdb

rm -rf /usr/local/pgsql/data
initdb -D /usr/local/pgsql/data
pg_ctl -D /usr/local/pgsql/data start
createdb mooncake
