FROM postgres:17 AS build

RUN apt update \
 && apt install -y \
    curl \
    gcc \
    make \
    pkg-config \
    postgresql-server-dev-17 \
 && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

RUN cargo install --locked cargo-pgrx@0.16.0 \
 && cargo pgrx init --pg17=$(which pg_config)

WORKDIR pg_mooncake

COPY Cargo.toml Makefile pg_mooncake.control .
COPY moonlink moonlink
COPY src src

RUN make package

FROM pgduckdb/pgduckdb:17-v1.0.0

COPY --from=build /pg_mooncake/target/release/pg_mooncake-pg17/ /

USER root

RUN cat >> /usr/share/postgresql/postgresql.conf.sample <<EOF
duckdb.allow_community_extensions = true
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
EOF

USER postgres
