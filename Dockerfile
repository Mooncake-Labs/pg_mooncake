FROM postgres:16

RUN apt update \
 && apt install -y \
    curl \
    g++ \
    liblz4-dev \
    make \
    postgresql-server-dev-16 \
    unzip \
 && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/duckdb/duckdb/releases/download/v1.1.0/libduckdb-linux-aarch64.zip -o libduckdb-linux-aarch64.zip \
 && unzip libduckdb-linux-aarch64.zip libduckdb.so \
 && install -m 755 libduckdb.so $(pg_config --pkglibdir) \
 && rm libduckdb-linux-aarch64.zip libduckdb.so

COPY . /tmp/pg_mooncake

RUN cd /tmp/pg_mooncake \
 && make clean \
 && make BUILD_TYPE=release \
 && make install
