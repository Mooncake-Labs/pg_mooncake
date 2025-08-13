FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update \
 && apt install -y \
    bison \
    clang-format-19 \
    cmake \
    flex \
    libclang-dev \
    liblz4-dev \
    ninja-build \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/bin/clang-format-19 /usr/bin/clang-format

USER vscode

RUN cd ~ \
 && git clone --branch stable https://github.com/rui314/mold.git \
 && cd mold \
 && sudo ./install-build-deps.sh \
 && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ -B build \
 && cmake --build build -j$(nproc) \
 && sudo cmake --build build --target install \
 && sudo ln -fs /usr/local/bin/mold /usr/bin/ld

RUN cd ~ \
 && git clone https://github.com/microsoft/vcpkg.git \
 && sh ./vcpkg/scripts/bootstrap.sh -disableMetrics

ENV VCPKG_TOOLCHAIN_PATH=/home/vscode/vcpkg/scripts/buildsystems/vcpkg.cmake

RUN cargo install --locked cargo-pgrx@0.16.0 \
 && cargo pgrx init

RUN wget -P ~ https://raw.githubusercontent.com/cmrschwarz/rust-prettifier-for-lldb/refs/heads/main/rust_prettifier_for_lldb.py
