FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update \
 && apt install -y \
    bison \
    clang-format-19 \
    cmake \
    flex \
    libclang-dev \
    mold \
    ninja-build \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/bin/clang-format-19 /usr/bin/clang-format

USER vscode

RUN cargo install --locked cargo-pgrx@0.14.3 \
 && cargo pgrx init

RUN wget -P ~ https://raw.githubusercontent.com/cmrschwarz/rust-prettifier-for-lldb/refs/heads/main/rust_prettifier_for_lldb.py

ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"
