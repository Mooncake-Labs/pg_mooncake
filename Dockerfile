FROM postgres:17 AS base

FROM base AS build

RUN apt update \
 && apt install -y \
    cmake \
    curl \
    git \
    g++ \
    liblz4-dev \
    ninja-build \
    pkg-config \
    postgresql-server-dev-17 \
 && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

RUN cargo install --locked cargo-pgrx@0.14.3 \
 && cargo pgrx init --pg17=$(which pg_config)

COPY . /pg_mooncake

RUN cd /pg_mooncake \
 && make clean \
 && make package -j$(nproc)

FROM base

COPY --from=build /pg_mooncake/target/release/pg_mooncake-pg17/ /

COPY .devcontainer /tmp/.devcontainer

RUN cat /tmp/.devcontainer/postgres.conf >> /usr/share/postgresql/postgresql.conf.sample

ENV RUST_BACKTRACE="1"
