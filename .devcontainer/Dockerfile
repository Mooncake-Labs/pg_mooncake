FROM ubuntu:22.04

RUN yes | unminimize

RUN apt update \
 && apt install -y \
    bzip2 \
    clang-format \
    cmake \
    curl \
    g++ \
    gdb \
    git \
    libicu-dev \
    libreadline-dev \
    locales \
    man \
    pkg-config \
    sudo \
    vim \
    zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8

RUN useradd -ms /bin/bash postgres \
 && echo 'postgres ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER postgres
WORKDIR /home/postgres
RUN echo 'source /workspaces/pg_mooncake/.devcontainer/.bashrc' >> .bashrc

ENV PATH="/usr/local/pgsql/bin:${PATH}"
RUN curl https://ftp.postgresql.org/pub/source/v16.4/postgresql-16.4.tar.bz2 | bzip2 -d | tar x \
 && cd postgresql-16.4 \
 && ./configure --enable-cassert --enable-debug CFLAGS='-ggdb3' \
 && make -j$(nproc) \
 && sudo make install \
 && sudo chown -R postgres:postgres /usr/local/pgsql \
 && initdb -D /usr/local/pgsql/data
RUN echo '\\i /workspaces/pg_mooncake/.devcontainer/.psqlrc' > .psqlrc
