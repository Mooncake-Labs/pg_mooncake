FROM postgres:16

RUN apt update \
 && apt install -y \
    curl \
    g++ \
    liblz4-dev \
    cmake \
    postgresql-server-dev-16 \
 && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:$PATH"

COPY . /tmp/pg_mooncake

RUN cd /tmp/pg_mooncake \
 && make clean \
 && make clean-duckdb \
 && make release \
 && make install
