FROM postgres:16

RUN apt update \
 && apt install -y \
    g++ \
    make \
    postgresql-server-dev-16 \
 && rm -rf /var/lib/apt/lists/*

COPY . /tmp/pg_mooncake

RUN cd /tmp/pg_mooncake \
 && make clean \
 && make BUILD_TYPE=release \
 && make install
