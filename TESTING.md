# Testing

## Regression tests

`make test PG_VERSION=pg18` (and `pg17`, ...) runs `cargo pgrx regress` on the pgrx cluster. See `AGENTS.md`
section 4 for the build and cluster setup.

## End-to-end gate (`scripts/e2e-smoke.sh`)

`scripts/e2e-smoke.sh` drives an installed pg_mooncake, pg_duckdb and duckdb_mooncake through mirrors on throwaway
clusters: CDC bursts, every supported column type, unchanged TOAST values, identifiers, persistence after a snapshot
and after a restart, moonlink being down or hung (and re-creating a mirror whose DROP only warned), source DDL and
TRUNCATE, a clean restart after a mirror's source table was dropped, a clean restart after a large transaction, and a
crash (`pg_ctl stop -m immediate`). After the crash, the moonlink background worker must come back without a
crash loop, `mooncake.list_tables()` must answer, and dropping and re-creating the mirror must give the heap contents.
It also checks that the duckdb_mooncake that DuckDB loads is built from the submodule commit that the DuckDB
community extension for DuckDB v1.4.1 uses, with the same moonlink wire protocol.

Requirements: Linux, bash 4 or later, GNU coreutils, procps, and a PostgreSQL install (tested with 17; 18 has not
been run with this script yet) with pg_duckdb and pg_mooncake installed and the submodules checked out. The script
checks its tool requirements before it starts. It runs 20 to 35 minutes, creates clusters under `$E2E_DIR` on
`$PGPORT`, and stops them when done.

```bash
PG_PREFIX=/path/to/pg17 \
DUCKDB_EXTENSION_DIR=/path/to/duckdb_extensions \
E2E_DIR=/tmp/pg_mooncake-e2e \
  scripts/e2e-smoke.sh 17                  # all scenarios; or name some, e.g. scripts/e2e-smoke.sh 17 main crash
```

- `PG_PREFIX` is the PostgreSQL install root (`$PG_PREFIX/bin/pg_ctl`); by default the one whose `pg_config` is on
  `PATH`. Its major version must match the argument. For a pgrx-managed PostgreSQL that is
  `$(dirname "$(dirname "$(cargo pgrx info pg-config pg17)")")`.
- `DUCKDB_EXTENSION_DIR` (required) becomes `duckdb.extension_directory` of every test cluster. Put a locally built
  `mooncake.duckdb_extension` in `<dir>/v1.4.1/linux_amd64/` (see "Using a locally built duckdb_mooncake" in
  `AGENTS.md`). With an empty directory, `CREATE EXTENSION pg_mooncake` installs the community build instead; that
  path has not been run with this script yet.
- After building duckdb_mooncake and copying it there, run
  `scripts/e2e-build-info.sh <dir>/v1.4.1/linux_amd64/mooncake.duckdb_extension` from the checkout that built it. It
  writes `mooncake.duckdb_extension.build-info` (the file's sha256, the duckdb_mooncake commit, whether that checkout
  had local changes, and its moonlink_rpc source). The `build` scenario checks that record against the loaded file and
  the submodule pins. If the script can't establish one of these values, it records `unknown` (which the gate
  rejects) and exits 1. The record attests the checkout at the time the script runs, not the build itself, so run it
  right after the build. Without a record whose sha256 is the loaded file's, the two provenance checks are SKIP or
  FAIL, so the gate can't pass on an unrecorded binary. That includes an extension installed by DuckDB `INSTALL`.
- `E2E_DIR` defaults to `${TMPDIR:-/tmp}/pg_mooncake-e2e`.
- Other settings (port, timeouts) are listed at the top of the script.

Output is one line per item: `RESULT <scenario>.<check> PASS|FAIL|XFAIL(<bug>)|XPASS(<bug>)|SKIP [detail]`, `INFO`
and `BUILD` lines, `KNOWN <bug> FIXED|OPEN|UNKNOWN|NOT-RUN|NOT-SEEN` per known bug, `SUMMARY`, and a last line
`GATE e2e-smoke PASS|FAIL|INCOMPLETE`. Exit status: 0 for PASS, 1 for FAIL, 3 for INCOMPLETE (a subset of scenarios ran
and passed), 2 for a usage or setup error. Cluster logs and query outputs stay under `$E2E_DIR/out-<major>-<scenario>/`.

`scripts/e2e-known-bugs.txt` lists the failures known on the current submodules, each with a regex for its failure
signature. A check that fails that way reports `XFAIL(<bug>)`; one that fails in any other way is `FAIL`. XFAIL still
fails the gate. When a fix makes a listed check pass, it reports `XPASS(<bug>)`, and `KNOWN <bug> FIXED` means every
check listed for that bug passed: remove its lines in the same change. A bug whose description starts with
`Intermittent:` reports `NOT-SEEN` instead, since one clean run doesn't show it is gone.
