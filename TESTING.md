# Testing

## Regression tests

`make test PG_VERSION=pg18` (and `pg17`, ...) runs `cargo pgrx regress` on the pgrx cluster and fails if any test's
output differs from `tests/pg_regress/expected` (it prints `tests/pg_regress/regression.diffs`). See `AGENTS.md`
section 4 for the build and cluster setup.

## CI

`.github/workflows/ci.yml` runs on pull requests, pushes to `main` and `v0.3-dev`, and `v*` tags, with PostgreSQL from
the PostgreSQL apt repository and the Rust toolchain from `rust-toolchain.toml`:

- `fmt` runs `cargo fmt --check`.
- `duckdb_mooncake` builds `mooncake.duckdb_extension` from the submodule (cached per submodule commit) for the other
  jobs, and checks that it was built for `DUCKDB_VERSION` in the `Makefile`. That version must equal pg_duckdb's
  (`pg_duckdb/Makefile`); the setup action fails otherwise.
- `test` builds pg_duckdb and runs `make test` on PostgreSQL 14 to 18, with that extension in `duckdb.extension_directory`
  as described below. The PostgreSQL 18 job also runs `cargo clippy -- -D warnings`.
- `e2e` runs `scripts/e2e-build-info.sh` for the downloaded extension and then `scripts/e2e-smoke.sh` on PostgreSQL 17
  and 18, on pushes and manual runs only (about 35 minutes each). It fails on unexpected failures (`fail=` > 0 in
  `SUMMARY`), on more skipped checks than `E2E_MAX_SKIP`, or if no check passed, but not on known bugs (so
  `GATE BLOCKED` alone does not fail it). The run summary lists the failures and fixed bugs; logs are attached as
  artifacts.
- `release-markers` runs on `v*` tags only and fails while release markers remain (`scripts/check-release-markers.sh`).

## Release markers

Statements in the release notes and docs that must be checked or filled in before a release carry a marker: an HTML
comment whose text starts with `RC:` in Markdown, and a comment line starting with `RC:` in SQL. This includes every
statement that is true only after the moonlink submodule bump. Resolve each one and delete it before tagging; the
`release-markers` job fails a tag that still has any.

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
it starts. It runs about 20 minutes, creates clusters under `$E2E_DIR` on `$PGPORT`, and stops them when done.

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
gone, so its entry stays until its fix is in. The known-bugs file must be a readable regular file. These are setup
errors (exit 2): a missing file (or `KNOWN_BUGS` set to an empty value), a file that is empty or has no `bug` entries, a
regex that matches the empty string or arbitrary text, a rule that accepts any crash restart without a `requires` line,
a bug declared twice, and a bug name outside `[A-Za-z0-9_.-]`. `EMPTY_KNOWN_BUGS_OK=1` allows an existing file without
entries, for a release with no known bugs; a missing file is always an error.

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
case also requires the backend's `pg_conf_load_time()` to advance. In `mirror_writes`, the contract a fix must meet for
the multiple-assignment `UPDATE` and `INSERT ... ON CONFLICT DO UPDATE` is an ordinary `ERROR: cannot modify mirror ...`
and no crash. No code emits that message yet; today plain writes fail with pg_duckdb's "not implemented" errors, which
the plain-write controls accept as long as there is no crash.

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

## Differential tests from the PostgreSQL regression suite (`tests/mirror_diff`)

`tests/mirror_diff/mirror_diff.py` uses the PostgreSQL regression suite as a corpus. It mirrors the suite's tables,
replays its files against the heap tables while they are mirrored, and checks that every column type survives the
initial copy, CDC and `mooncake.create_snapshot`, that mirrors equal their heap tables after each DML statement or
COMMIT, and that each SELECT on a mirror (run by DuckDB) returns what Postgres returns. It needs Python 3.8 or later,
an installed PostgreSQL with pg_duckdb and pg_mooncake, and a PostgreSQL source tree of the same major version, whose
`src/test/regress` it reads at run time (no PostgreSQL files are copied here).

```bash
python3 tests/mirror_diff/mirror_diff.py --pg-src /path/to/postgresql-17.x --bindir /path/to/pg17/bin \
    --duckdb-extension-dir /path/to/duckdb_extensions --workdir /tmp/mirror_diff | tee mirror_diff.log
python3 tests/mirror_diff/test_mirror_diff.py   # unit tests, no database
```

A full run takes one to three hours; `--files numeric,join` runs a subset (the gate then reports `INCOMPLETE`) and
`--resume` continues an interrupted run. The output lines, the known-differences file
(`tests/mirror_diff/expected.txt`, `bug <name> class=blocking|class=limitation ...`) and the `GATE mirror_diff` line
and exit status follow the [end-to-end gate](#end-to-end-gate-scriptse2e-smokesh) and its
[release policy](#release-policy-blocking-and-limitation), with one difference: a plain SKIP (a query DuckDB can't
plan, a volatile or side-effecting statement) doesn't fail the mirror_diff gate; only an
unverified SKIP (a mirror that couldn't be checked) does. `tests/mirror_diff/README.md` describes the checks, the
comparison rules, the SKIPs and the rule syntax.
