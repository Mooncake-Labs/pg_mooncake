#!/usr/bin/env bash
# Records where a locally built duckdb_mooncake came from, for scripts/e2e-smoke.sh (scenario build).
#
# Usage: scripts/e2e-build-info.sh <installed mooncake.duckdb_extension> [duckdb_mooncake checkout]
#
# Run it right after building and copying the extension, against the checkout that built it
# (default: the duckdb_mooncake submodule of this repository). It writes
# <extension>.build-info with the keys e2e-smoke.sh checks: sha256 of the file, the
# duckdb_mooncake commit, whether that checkout had uncommitted changes, and the moonlink_rpc
# source from moonlink_ffi/Cargo.lock.
set -euo pipefail

ext=${1:?usage: $0 <mooncake.duckdb_extension> [duckdb_mooncake checkout]}
dm=${2:-$(cd "$(dirname "$0")/.." && pwd)/duckdb_mooncake}
[[ -f $ext ]] || { echo "$ext: no such file" >&2; exit 2; }
[[ $(git -C "$dm" rev-parse --show-toplevel 2>/dev/null) == "$(cd "$dm" && pwd -P)" ]] ||
  { echo "$dm is not a duckdb_mooncake checkout (submodule not initialized?)" >&2; exit 2; }

# A failing git status is "unknown", which e2e-smoke.sh does not accept as clean.
if changes=$(git -C "$dm" status --porcelain --untracked-files=no); then
  if [[ -n $changes ]]; then dirty=yes; else dirty=no; fi
else dirty=unknown; fi
lock=$(grep -A2 '^name = "moonlink_rpc"$' "$dm/moonlink_ffi/Cargo.lock" | sed -n 's/^source = "\(.*\)"$/\1/p') || lock=""

{
  echo "source=local-build"
  echo "duckdb_mooncake_commit=$(git -C "$dm" rev-parse HEAD)"
  echo "duckdb_mooncake_dirty=$dirty"
  echo "moonlink_rpc_lock=${lock:-unknown}"
  echo "sha256=$(sha256sum "$ext" | cut -d' ' -f1)"
  echo "built_at=$(date -u -r "$ext" +%Y-%m-%dT%H:%M:%SZ)"
} > "$ext.build-info"
echo "wrote $ext.build-info"
