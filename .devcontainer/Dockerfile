FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt update \
 && apt install -y \
    bison \
    cmake \
    flex \
    libclang-dev \
    liblz4-dev \
 && rm -rf /var/lib/apt/lists/*

USER vscode

RUN cargo install --locked cargo-pgrx@0.14.3 \
 && cargo pgrx init

RUN echo '\\i /workspaces/pg_mooncake/.devcontainer/.psqlrc' > ~/.psqlrc

RUN wget -P ~ https://raw.githubusercontent.com/cmrschwarz/rust-prettifier-for-lldb/refs/heads/main/rust_prettifier_for_lldb.py
