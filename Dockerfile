FROM postgres:18-bookworm AS build

ARG JOBS=4

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential clang-14 cmake curl git libclang-dev libcurl4-openssl-dev \
    liblz4-dev libssl-dev ninja-build pkg-config postgresql-server-dev-18 \
 && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.90.0
ENV PATH="/root/.cargo/bin:$PATH"

RUN cargo install --locked cargo-pgrx@0.16.1 \
 && cargo pgrx init --pg18=$(which pg_config)

WORKDIR /pg_mooncake
COPY Cargo.toml Cargo.lock rust-toolchain.toml Makefile pg_mooncake.control ./
COPY duckdb_mooncake duckdb_mooncake
COPY moonlink moonlink
COPY pg_duckdb pg_duckdb
COPY src src

# Docker contexts do not carry the parent repository's submodule Git metadata.
RUN make -C pg_duckdb -o .git/modules/third_party/duckdb/HEAD \
      PG_DUCKDB_VERSION=v1.2.0 DUCKDB_CC=clang-14 DUCKDB_CXX=clang++-14 \
      DESTDIR=/out -j${JOBS} install \
 && make package JOBS=${JOBS} PACKAGE_DIR=/out

FROM postgres:18-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libcurl4 liblz4-1 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /

RUN cat >> /usr/share/postgresql/postgresql.conf.sample <<EOF
duckdb.allow_community_extensions = true
duckdb.allow_unsigned_extensions = true
mooncake.extension_repository = '/usr/share/postgresql/18/extension/pg_mooncake'
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
EOF

USER postgres
