cat >> ~/.psqlrc <<EOF
\set PROMPT1 '%/ (pid: %p) %R%# '
\set PROMPT2 '  '
EOF

for version in 14 15 16 17; do
cat >> ~/.pgrx/data-${version}/postgresql.conf <<EOF
duckdb.allow_community_extensions = true
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
EOF
done

git config devcontainers-theme.show-dirty 1
