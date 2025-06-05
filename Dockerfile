FROM postgres:17 AS base

FROM base AS pg_mooncake

RUN apt update \
 && apt install -y \
    cmake \
    curl \
    git \
    g++ \
    liblz4-dev \
    pkg-config \
    postgresql-server-dev-17 \
 && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

RUN cargo install --locked cargo-pgrx@0.14.3 \
 && cargo pgrx init --pg17=$(which pg_config)

COPY . /build/pg_mooncake

RUN cd /build/pg_mooncake \
 && make clean \
 && make package

FROM base

COPY --from=pg_mooncake /build/pg_mooncake/target/release/pg_mooncake-pg17/ /

RUN cat >> /usr/share/postgresql/postgresql.conf.sample <<EOF
shared_preload_libraries = 'pg_mooncake'
wal_level = logical
EOF

ENV RUST_BACKTRACE="1"
