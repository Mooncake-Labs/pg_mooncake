git config --global --add safe.directory /workspaces/pg_mooncake

pg_ctl -D /usr/local/pgsql/data start
dropdb --if-exists mooncake
createdb mooncake
