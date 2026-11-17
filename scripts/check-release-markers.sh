#!/usr/bin/env bash
# Fails while release markers remain: HTML comments starting with "RC:" in Markdown, and SQL comment lines starting
# with "RC:". They mark statements to check or fill in before tagging a release (TESTING.md).
set -euo pipefail
cd "$(dirname "$0")/.."
if git grep -n -E -e '<!-- RC:' -e '^[[:space:]]*-- RC:' -- . ':!scripts/check-release-markers.sh'; then
  echo "release markers remain (above); resolve them before tagging" >&2
  exit 1
fi
echo "no release markers"
