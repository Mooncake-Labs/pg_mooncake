# Testing

## Regression tests

`make test PG_VERSION=pg18` (and `pg17`, ...) runs `cargo pgrx regress` on the pgrx cluster and fails if any test's
output differs from `tests/pg_regress/expected` (it prints `tests/pg_regress/regression.diffs`). See `AGENTS.md`
section 4 for the build and cluster setup.

## CI

`.github/workflows/ci.yml` runs on pull requests, pushes to the development branches, and `v*` tags, with PostgreSQL
from the PostgreSQL apt repository and the Rust toolchain from `rust-toolchain.toml`:

- `fmt` runs `cargo fmt --check`, and `make check-rust-postgres` (rust-postgres pinned as in moonlink).
- `duckdb_mooncake` builds `mooncake.duckdb_extension` from the submodule (cached per submodule commit) for the other
  jobs, and checks that it was built for `DUCKDB_VERSION` in the `Makefile`. That version must equal pg_duckdb's
  (`pg_duckdb/Makefile`); the setup action fails otherwise.
- `test` builds pg_duckdb and runs `make test` on PostgreSQL 14 to 18, with that extension in `duckdb.extension_directory`
  as described below. The PostgreSQL 18 job also runs `cargo clippy -- -D warnings`.
- `e2e` runs `scripts/e2e-build-info.sh` for the downloaded extension and then `scripts/e2e-smoke.sh` on PostgreSQL 17
  and 18, on pushes and manual runs only (about 25 minutes each). It fails on unexpected failures (`fail=` > 0 in
  `SUMMARY`), on more skipped checks than `E2E_MAX_SKIP`, or if no check passed. The run summary lists the failures and
  fixed bugs; logs are attached as artifacts.
- `release-markers` runs on `v*` tags only and fails while release markers remain (`scripts/check-release-markers.sh`).

## Release markers

Statements in the release notes and docs that must be checked or filled in before a release carry a marker: an HTML
comment whose text starts with `RC:` in Markdown, and a comment line starting with `RC:` in SQL. Resolve each one and
delete it before tagging; the `release-markers` job fails a tag that still has any.

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

Requirements: Linux, bash 4 or later, GNU coreutils, GNU find, procps, and a PostgreSQL install (tested with 17 and 18)
with pg_duckdb and pg_mooncake installed and the submodules checked out. The script checks its tool requirements before
it starts. It runs about 25 minutes, creates clusters under `$E2E_DIR` on `$PGPORT`, and stops them when done.

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
  `AGENTS.md`). With an empty directory, `CREATE EXTENSION pg_mooncake` installs the community build instead, which
  has no build record (next item), so the gate can't pass on it.
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
`XFAIL(limitation:<bugs>)`, `XPASS(<bugs>)` or `SKIP`; `INFO` and `BUILD` lines (including
`BUILD known_bugs=<file> sha256=<hash>`); a `KNOWN <bug> ...` line per known bug; `SUMMARY`; and a last line
`GATE e2e-smoke PASS|FAIL|INCOMPLETE`. Exit status: 0 for PASS, 1 for FAIL, 3 for INCOMPLETE (a subset of scenarios
ran and passed), 2 for a usage or setup error. Cluster logs and query outputs stay under `$E2E_DIR/out-<major>-<scenario>/`.

### Known limitations and the gate

`scripts/e2e-known-bugs.txt` lists the documented limitations the gate expects at the submodule pins, each with a regex
for its failure signature; these limitations are among the CHANGELOG's "Known limitations". A check that fails that
way reports `XFAIL(limitation:<bugs>)` and doesn't count against the gate; one that fails in any other way is `FAIL`.
When a fix makes a listed check pass, it reports `XPASS(<bugs>)`, and `KNOWN <bug> FIXED` means every check listed for
that bug passed: remove its lines in the same change. A missing, empty or malformed known-bugs file is a setup error
(exit 2).

`GATE e2e-smoke PASS` (exit 0) means every check passed or failed only with a listed limitation, and none was skipped.
That is what a release needs. A new failure is fixed, not added to the file: adding a limitation is a release decision.

### What some checks require

After unsupported source DDL (`ddl`, `ddl_drop`), a mirror passes if it equals the source, or if its scans fail with
one ERROR line that names the mirror, says `schema changes are not supported` and gives the `drop and re-create`
remedy. Any other error, or a mirror that silently differs, fails.

Every scenario ends by stopping its cluster (`pg_ctl stop -m fast`) and then checking the whole server log:
`no_moonlink_panics`, `bgworker_no_abnormal_exits`, `no_postgres_crash_restarts` and
`no_worker_errors_during_final_stop`. A clean stop SIGTERMs the moonlink background worker, which may exit with code
1 so that Postgres restarts it when it is killed at other times. So each clean stop (and the SIGTERM test in
`moonlink_down`) excuses one `exited with exit code 1` in its part of the log, but only from a worker that logged no
ERROR or FATAL. The one exception is the FATAL Postgres itself logs when it stops a background worker at shutdown
(`terminating background worker ... due to administrator command`), which never counts. Other exit codes, signal
deaths, and the exits of an immediate stop (`-m immediate` sends SIGQUIT) always count.
`no_worker_errors_during_final_stop` covers only the final stop: it fails if the worker logs an ERROR or FATAL while it
shuts down. Errors before that are checked where they happen (for example `no_worker_errors_after_<restart|crash>`).
A clean stop that doesn't finish within 30 s fails. The failure names the processes still running (walsenders are
only counted, since Postgres keeps them until everything else has exited), whether moonlink logged that it got the
SIGTERM (`moonlink saw SIGTERM: yes|no`), and the panics during the stop.

Session checks (`config_reload`, `mirror_writes`, `roles`) run one psql session per case. A crash of any Postgres
process other than a background worker, seen in the server log or as a lost connection, fails the case as `backend
crashed (<kind>, <signal n | exit code n>, cassert=<yes|no|unknown>) running [<statement>]; TRAP: <...>`. The process
kind is given in PostgreSQL 17 terms: PostgreSQL 18 names a backend by its type (`client backend`, `walsender`, ...),
and those are reported as `server process`, so the known-bug rules match on every major. The log is looked at again 1.5
s after a passing case, so a crash right after the session's last output is caught the same way. A session that lost its
connection without a crash in the log is reported as `session lost its connection ...`. `no_postgres_crash_restarts`
also counts the crashes that no session check reported, so a crash elsewhere in the scenario isn't hidden behind a known
one. A crash during a stop ends it with `abnormal database system shutdown` instead of a restart, and `pg_ctl stop`
still succeeds; that line counts as a crash too. The `config_reload` cases wait for the mirrors before each case, must
read the mirror first (pg_duckdb's hook only runs once DuckDB is initialised) and then run without any error; the reload
case also requires the backend's `pg_conf_load_time()` to advance. In `mirror_writes`, writes to a mirror, including
the multiple-assignment `UPDATE` and `INSERT ... ON CONFLICT DO UPDATE`, must fail with an ordinary
`ERROR: cannot modify mirror ...` and no crash.

## Differential tests (`tests/mirror_diff`), advisory

`tests/mirror_diff/mirror_diff.py` uses the PostgreSQL regression suite as a corpus: it mirrors the suite's tables and
checks the mirrors and queries on them against the heap tables. A full run takes one to three hours. It is advisory,
not a release gate; [tests/mirror_diff/README.md](tests/mirror_diff/README.md) describes how to run it and its checks.
