FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update \
 && apt install -y \
    bison \
    cmake \
    flex \
    libclang-dev \
    liblz4-dev \
    mold \
    ninja-build \
 && rm -rf /var/lib/apt/lists/* \
 && ln -sf /usr/bin/mold /usr/bin/ld

USER vscode

RUN cargo install --locked cargo-pgrx@0.16.0 \
 && cargo pgrx init

RUN cd ~ \
 && git clone https://github.com/microsoft/vcpkg.git \
 && sh ./vcpkg/scripts/bootstrap.sh -disableMetrics

ENV VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake

RUN wget -P ~ https://raw.githubusercontent.com/cmrschwarz/rust-prettifier-for-lldb/refs/heads/main/rust_prettifier_for_lldb.py
