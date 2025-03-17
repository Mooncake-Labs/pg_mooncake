FROM mcr.microsoft.com/devcontainers/rust:latest AS base

FROM base AS mold

WORKDIR /build

RUN git clone --branch v2.40.0 --single-branch https://github.com/rui314/mold.git \
 && cd mold \
 && ./install-build-deps.sh \
 && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ -DCMAKE_INSTALL_PREFIX=/out -B build \
 && cmake --build build -j$(nproc) \
 && cmake --build build --target install

FROM base

RUN apt update \
 && apt install -y \
    bison \
    clang-format \
    cmake \
    flex \
    libclang-dev \
    liblz4-dev \
 && rm -rf /var/lib/apt/lists/*

COPY --from=mold /out /usr

RUN ln -fs /usr/bin/mold /usr/bin/ld

USER vscode

RUN cargo install --locked cargo-pgrx@0.14.3 \
 && cargo pgrx init

RUN echo '\\i /workspaces/pg_mooncake/.devcontainer/.psqlrc' > ~/.psqlrc

RUN cat >> ~/.pgrx/data-17/postgresql.conf <<EOF
shared_preload_libraries = 'pg_mooncake'
wal_level = logical
EOF

RUN wget -P ~ https://raw.githubusercontent.com/cmrschwarz/rust-prettifier-for-lldb/refs/heads/main/rust_prettifier_for_lldb.py
