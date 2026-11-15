#!/usr/bin/env bash
# Records where a locally built duckdb_mooncake came from, for scripts/e2e-smoke.sh (scenario build).
#
# Usage: scripts/e2e-build-info.sh <installed mooncake.duckdb_extension> [duckdb_mooncake checkout]
#
# Run it right after building and copying the extension, against the checkout that built it
# (default: the duckdb_mooncake submodule of this repository). It writes
# <extension>.build-info with the keys e2e-smoke.sh checks: sha256 of the file, the
# duckdb_mooncake commit, whether that checkout had uncommitted changes, and the moonlink_rpc
# source from moonlink_ffi/Cargo.lock. The record attests that checkout as it is when the script
# runs, not the build itself.
set -euo pipefail

ext=${1:?usage: $0 <mooncake.duckdb_extension> [duckdb_mooncake checkout]}
dm=${2:-$(cd "$(dirname "$0")/.." && pwd)/duckdb_mooncake}
[[ -f $ext ]] || { echo "$ext: no such file" >&2; exit 2; }
# Every probe runs before anything is written. One that fails records "unknown", which the gate
# never accepts, and makes this script exit 1; the file is replaced atomically.
ok=1
sha=$(sha256sum "$ext" | cut -d' ' -f1)
[[ $sha =~ ^[0-9a-f]{64}$ ]] || { echo "$ext: cannot hash" >&2; exit 2; }
top=$(git -C "$dm" rev-parse --show-toplevel 2>/dev/null) || top=""
if [[ -z $top || $top != "$(cd "$dm" 2>/dev/null && pwd -P)" ]]; then
  echo "error: $dm is not a duckdb_mooncake checkout (submodule not initialized?); recording unknown" >&2
  commit=unknown dirty=unknown lock=unknown ok=0
else
  commit=$(git -C "$dm" rev-parse HEAD) || { commit=unknown ok=0; }
  if changes=$(git -C "$dm" status --porcelain --untracked-files=no); then
    if [[ -n $changes ]]; then dirty=yes; else dirty=no; fi
  else
    echo "error: git status failed in $dm; recording dirty=unknown" >&2
    dirty=unknown ok=0
  fi
  lock=$(grep -A2 '^name = "moonlink_rpc"$' "$dm/moonlink_ffi/Cargo.lock" | sed -n 's/^source = "\(.*\)"$/\1/p') || lock=""
  [[ -n $lock ]] || { echo "error: no moonlink_rpc entry in $dm/moonlink_ffi/Cargo.lock" >&2; lock=unknown ok=0; }
fi

tmp=$(mktemp "$ext.build-info.XXXXXX")
trap 'rm -f "$tmp"' EXIT
{
  echo "source=local-build"
  echo "duckdb_mooncake_commit=$commit"
  echo "duckdb_mooncake_dirty=$dirty"
  echo "moonlink_rpc_lock=$lock"
  echo "sha256=$sha"
  echo "built_at=$(date -u -r "$ext" +%Y-%m-%dT%H:%M:%SZ)"
} > "$tmp"
mv -f "$tmp" "$ext.build-info"
trap - EXIT
echo "wrote $ext.build-info"
(( ok )) || { echo "some provenance is unknown; the gate will fail duckdb_mooncake_built_from_pins" >&2; exit 1; }
