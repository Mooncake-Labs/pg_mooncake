FROM postgres:18 AS build

RUN apt update \
 && apt install -y \
    curl \
    gcc \
    make \
    pkg-config \
    postgresql-server-dev-18 \
 && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

RUN cargo install --locked cargo-pgrx@0.16.1 \
 && cargo pgrx init --pg18=$(which pg_config)

WORKDIR pg_mooncake

COPY Cargo.toml Makefile pg_mooncake.control .
COPY moonlink moonlink
COPY src src
# Extension upgrade scripts (pg_mooncake--<from>--<to>.sql); cargo pgrx package copies them from sql/.
COPY sql sql

RUN make package

FROM pgduckdb/pgduckdb:18-main

COPY --from=build /pg_mooncake/target/release/pg_mooncake-pg18/ /

USER root

RUN cat >> /usr/share/postgresql/postgresql.conf.sample <<EOF
duckdb.allow_community_extensions = true
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
EOF

USER postgres
