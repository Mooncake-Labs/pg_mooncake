# Testing

## Regression tests

`make test PG_VERSION=pg18` (and `pg17`, ...) runs `cargo pgrx regress` on the pgrx cluster. See `AGENTS.md`
section 4 for the build and cluster setup.

## End-to-end gate (`scripts/e2e-smoke.sh`)

`scripts/e2e-smoke.sh` drives an installed pg_mooncake, pg_duckdb and duckdb_mooncake through mirrors on throwaway
clusters: CDC bursts, every supported column type, unchanged TOAST values, identifiers, persistence after a snapshot
and after a restart, moonlink being down or hung (and re-creating a mirror whose DROP only warned), source DDL and
TRUNCATE, a clean restart after a mirror's source table was dropped, sessions that must survive a config reload and
TimeZone resets after a mirror read, writes to mirrors (which must fail with `cannot modify mirror`), who can read
mirrors (`duckdb.postgres_role`, the server-file roles, GRANT and REVOKE ALL), a clean restart after a large
transaction, and a crash (`pg_ctl stop -m immediate`). A statement that crashes a backend (and so restarts every
Postgres process) fails its check. After the crash, the moonlink background worker must come back without a
crash loop, `mooncake.list_tables()` must answer, and dropping and re-creating the mirror must give the heap contents.
It also checks that the duckdb_mooncake that DuckDB loads is built from the submodule commit that the DuckDB
community extension for DuckDB v1.4.1 uses, with the same moonlink wire protocol.

Requirements: Linux, bash 4 or later, GNU coreutils, procps, and a PostgreSQL install (tested with 17; 18 has not
been run with this script yet) with pg_duckdb and pg_mooncake installed and the submodules checked out. The script
checks its tool requirements before it starts. It runs 25 to 40 minutes, creates clusters under `$E2E_DIR` on
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

Output is one line per item: `RESULT <scenario>.<check> <status> [detail]` with status `PASS`, `FAIL`,
`XFAIL(<bugs>)`, `XFAIL(limitation:<bugs>)`, `XPASS(<bugs>)` or `SKIP`; `INFO` and `BUILD` lines (including
`BUILD known_bugs=<file> sha256=<hash>`); `KNOWN <bug> FIXED|OPEN|UNKNOWN|NOT-RUN|NOT-SEEN ... class=<class>` per
known bug; `SUMMARY`; `BLOCKING e2e-smoke <n> <bugs>`; and a last line `GATE e2e-smoke PASS|FAIL|BLOCKED|INCOMPLETE`.
Exit status: 0 for PASS, 1 for FAIL, 4 for BLOCKED, 3 for INCOMPLETE (a subset of scenarios ran and passed), 2 for a
usage or setup error. Cluster logs and query outputs stay under `$E2E_DIR/out-<major>-<scenario>/`.

`scripts/e2e-known-bugs.txt` lists the failures known on the current submodules, each with a regex for its failure
signature. A check that fails that way reports `XFAIL(<bugs>)`; one that fails in any other way is `FAIL`. When a fix
makes a listed check pass, it reports `XPASS(<bugs>)`, and `KNOWN <bug> FIXED` means every check listed for that bug
passed: remove its lines in the same change. For a bug whose description starts with `Intermittent:`, a run in which
every listed check passed reports `NOT-SEEN` instead of `FIXED`: passing runs don't show that an intermittent bug is
gone, so its entry stays until its fix is in. A known-bugs file that is missing, empty or has no `bug` entries is a
setup error (exit 2), and so is a bug declared twice or a name outside `[A-Za-z0-9_.-]`. `EMPTY_KNOWN_BUGS_OK=1`
allows an existing file without entries, for a release with no known bugs; a missing file is always an error.

After unsupported source DDL (`ddl`, `ddl_drop`), a mirror passes if it equals the source, or if its scans fail with
one ERROR line that names the mirror, says `schema changes are not supported` and gives the `drop and re-create`
remedy. Any other error, or a mirror that silently differs, fails.

Every scenario ends by stopping its cluster (`pg_ctl stop -m fast`) and then checking the whole server log:
`no_moonlink_panics`, `bgworker_no_abnormal_exits`, `no_postgres_crash_restarts` and
`no_worker_errors_during_final_stop`. A clean stop SIGTERMs the moonlink background worker, which may exit with code
1 so that Postgres restarts it when it is killed at other times. So each clean stop (and the SIGTERM test in
`moonlink_down`) excuses one `exited with exit code 1` in its part of the log, but only from a worker that logged no
ERROR or FATAL. Other exit codes, signal deaths, and the exits of an immediate stop (`-m immediate` sends SIGQUIT)
always count. `no_worker_errors_during_final_stop` fails if the worker logs an ERROR or FATAL while it shuts down.

### Release policy: blocking and limitation

Every known bug is declared once with a class, `bug <name> class=blocking|class=limitation <description>`:

- `class=blocking`: the bug can crash-loop moonlink, panic, lose or silently change mirrored data, or leave mirrors
  unusable. A release needs it fixed and its entry removed; while it is listed the gate can't pass. A check that
  fails with it reports `XFAIL(<bugs>)`, which fails the gate.
- `class=limitation`: a documented restriction that is loud (an error at `mooncake.create_table`) or harmless. It
  ships and goes into the release notes. A check that fails only with limitations reports
  `XFAIL(limitation:<bugs>)`, and it doesn't count against the gate.

The gate is:

- `FAIL` if any check is FAIL, SKIP or XFAIL of a blocking bug, or if no check passed;
- otherwise `BLOCKED` while the file lists any `class=blocking` bug, even one the run didn't hit (for example an
  intermittent one that reports `NOT-SEEN` or `UNKNOWN`); the `BLOCKING` line names them;
- otherwise `INCOMPLETE` if not every scenario ran;
- otherwise `PASS`, which is what a release needs.

A fix removes the bug's entry, which its `XPASS` and `FIXED` reports show is safe. Moving a bug from `blocking` to
`limitation` is a release decision, not a test change.
