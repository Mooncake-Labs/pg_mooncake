# PostgreSQL 18 with pg_duckdb and pg_mooncake, built from this checkout as the README's "Build from source" describes.
# The build context needs the moonlink and pg_duckdb submodules: git submodule update --init --recursive moonlink pg_duckdb
FROM postgres:18 AS build

# The image sets PG_VERSION to the Debian package version, which would override the Makefile's PG_VERSION, so the
# make calls below pass it and PG_CONFIG explicitly.
ARG PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    curl \
    git \
    libclang-dev \
    libcurl4-openssl-dev \
    liblz4-dev \
    libssl-dev \
    ninja-build \
    pkg-config \
    postgresql-server-dev-18 \
 && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal

ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /pg_mooncake

# As in CI, cargo-pgrx (the version of the pgrx crate) is built with rustup's default toolchain, before
# rust-toolchain.toml is in the directory.
COPY Cargo.toml Makefile pg_mooncake.control ./
RUN pgrx_version=$(sed -n 's/^pgrx = "=\{0,1\}\(.*\)"/\1/p' Cargo.toml) \
 && cargo install --locked "cargo-pgrx@$pgrx_version" \
 && cargo pgrx init --pg18 "$PG_CONFIG"

# pg_mooncake itself builds with the toolchain rust-toolchain.toml pins.
COPY rust-toolchain.toml ./
COPY .cargo .cargo
RUN rustup toolchain install

COPY pg_duckdb pg_duckdb
# pg_duckdb's Makefile runs `git submodule update` unless this file exists; the context has the submodules checked out
# but no git metadata.
RUN mkdir -p pg_duckdb/.git/modules/third_party/duckdb \
 && touch pg_duckdb/.git/modules/third_party/duckdb/HEAD \
 && make pg_duckdb PG_VERSION=pg18 PG_CONFIG="$PG_CONFIG"

COPY moonlink moonlink
COPY src src
RUN make package PG_VERSION=pg18 PG_CONFIG="$PG_CONFIG"

FROM postgres:18

# pg_duckdb's built-in httpfs links libcurl; CREATE EXTENSION pg_mooncake downloads the mooncake DuckDB extension over
# HTTPS.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    libcurl4t64 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /usr/lib/postgresql/18/lib/pg_duckdb.so /usr/lib/postgresql/18/lib/libduckdb.so /usr/lib/postgresql/18/lib/
COPY --from=build /usr/share/postgresql/18/extension/pg_duckdb* /usr/share/postgresql/18/extension/
COPY --from=build /pg_mooncake/target/release/pg_mooncake-pg18/ /

# moonlink's files go next to PGDATA (/var/lib/postgresql/18/docker): in the volume, but outside the data directory, so
# that pg_basebackup works. A new volume gets the directory from the image, owned by postgres.
RUN install -d -o postgres -g postgres /var/lib/postgresql/18 \
 && install -d -o postgres -g postgres -m 0700 /var/lib/postgresql/18/mooncake \
 && cat >> /usr/share/postgresql/postgresql.conf.sample <<EOF
duckdb.allow_community_extensions = true
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
mooncake.data_directory = '/var/lib/postgresql/18/mooncake'
EOF
