# mirror_diff: differential tests of mirrors against the PostgreSQL regression suite

`mirror_diff.py` uses the PostgreSQL regression suite (`src/test/regress` of a PostgreSQL source
tree) as a corpus of tables, values, queries and DML. It mirrors those tables with
`mooncake.create_table` and checks that the mirror always holds the same data as the heap
table and that queries on the mirror (executed by DuckDB) return what Postgres returns.

The PostgreSQL files are read from `--pg-src` at run time. None are copied into this repository.

## Requirements

- An installed Postgres with pg_duckdb and pg_mooncake, plus the DuckDB `mooncake` extension.
  By default the harness creates a throwaway cluster with `initdb` from `--bindir`.
- A PostgreSQL source tree of the same major version. Optionally build `regress.so`
  (`make -C src/test/regress` in a configured tree): a few fixture functions in `test_setup.sql`
  need it, and without it those statements are skipped (an `INFO` line says so).
- Python 3.8+ (standard library only) and `psql`.

## Running

```sh
python3 tests/mirror_diff/mirror_diff.py \
    --pg-src ~/src/postgresql-17.6 \
    --bindir /usr/local/pgsql/bin \
    --duckdb-extension-dir ~/duckdb_extensions \
    --workdir /tmp/mirror_diff | tee mirror_diff.log
```

Useful options:

| Option | Meaning |
|---|---|
| `--layers data,cdc,query` | which layers to run (default: all) |
| `--files numeric,join` | only these files (regression file names, or `extra_cdc`, `extra_tables`) |
| `--host /tmp --port 5432` | use a running server instead of a throwaway cluster |
| `--server-log FILE` | with `--host`: the server's log, so moonlink panics and backend crashes are seen |
| `--keep` | keep the databases and leave the server running, for debugging |
| `--expected FILE` | known-differences list (default `expected.txt` next to the script) |
| `--bug-ids FILE` | optional bug-id map: `<bug> <ids>` lines, shown as `ids=` on `KNOWN` lines |
| `--converge-secs N` | how long to keep reading a mirror that differs (default 10) |
| `--max-rejected-values N` | values a type probe may drop after `create_table` refuses them by name (default 40) |
| `--trace` | log every statement to `<workdir>/trace.sql` |
| `--resume` | continue an interrupted run in the same `--workdir` (finished units are listed in `done.txt`) |
| `--exit-regression` | exit with the regression rule instead of the gate (see Output) |

A full run (all layers, about 60 regress files, each in its own throwaway cluster) takes 1–3
hours, most of it spent waiting out moonlink hangs after panics. Artifacts in `<workdir>`:

- `results.jsonl`: one JSON record per check with the full evidence (statement, events, sample
  rows that differ, errors). `python3 summarize.py <workdir>/results.jsonl` renders it as markdown.
- `logs/<layer>-<file>.log`: the server log of each file's cluster (moonlink panics, backtraces).
- `trace.sql`: with `--trace`, every statement each session sent, with outcome and duration.

The harness only deletes a `<workdir>/data` directory that carries its `mirror_diff.cluster`
marker, so pointing `--workdir` at the wrong place cannot remove an unrelated cluster.

## What is checked

| Layer | Check name | Meaning |
|---|---|---|
| data | `data.copy.<file>.<table>` | Each table of the data-type files is created and filled without a mirror, then mirrored by initial copy and compared. The file is replayed statement by statement and each table is checked again whenever its content changed before the file drops or alters it (`.s2`, `.s3`, ...). |
| data | `data.type.<type>` | All values of a built-in column type, pooled from every file, in one table: initial copy, CDC INSERT followed by `create_snapshot`, CDC UPDATE that leaves the value unchanged, CDC DELETE followed by `create_snapshot`. |
| data | `data.type.<file>.<type>` | The same for types a file defines itself (enum, domain, composite, range). |
| cdc | `cdc.<file>.<table>` | The file runs statement by statement on heap tables while every table in `public` is mirrored into schema `mc`. Mirrors are created for fixtures up front and for the file's own tables right after `CREATE TABLE`. After each DML statement or COMMIT, the mirrors of the tables it names must equal the heap. |
| query | `query.<file>.L<line>` | Each SELECT that names a mirrored table runs again with `search_path = mc, ...` and is compared with the heap result. |
| – | `panic.<file>.L<line>` | moonlink panicked while that statement ran (detail: the panic sites). |
| – | `backend.<file>.L<line>` | a Postgres backend died and the server went through crash recovery (detail: the `TRAP` line). |
| – | `panic.data.<file>`, `panic.run.<file>` | moonlink panics in the unit's server log that no other check reported (end-of-unit sweep). |
| – | `backend.data.<file>`, `backend.run.<file>` | Postgres crash restarts in the unit's server log that no other check reported (end-of-unit sweep). |
| – | `service.<file>.wedged` | mooncake RPCs kept timing out; the file's remaining mirrors are unverified. |
| – | `service.data.<file>`, `service.run.<file>` | moonlink still answers `mooncake.list_tables()` after the data or run unit of the file. |
| – | `stmt.<file>.<name>` | a `md:check` directive in the extra files (see below). |
| – | `infra.data.<file>`, `infra.run.<file>`, `infra.<file>.file_timeout`, `infra.fixtures`, `infra.panic_detection` | the harness itself could not do its job: a unit raised, a file hit `--file-timeout`, fixtures failed to load, or `--host` ran without `--server-log`. |

### Comparison rules

- Rows are compared as Postgres prints them, typed by the query's result columns (psql
  `\gdesc`). Only `real` and `double precision` columns get a tolerance: they are compared after
  rounding to 6 and 12 significant digits. `numeric`, text, dates and everything else must match
  exactly, so `2.5` vs `2.5000000000000000` is a difference.
- Row order is checked as far as the statement's top-level `ORDER BY` decides it: rows must come
  in the same sequence of sort keys, and rows with equal keys may come in any order. The last
  group of equal keys may hold different rows only when a literal `LIMIT n` (or `FETCH FIRST n`)
  actually cut the result to n rows. Without a top-level `ORDER BY`, rows are compared as
  multisets. Sort keys are mapped to output columns by position or name; when the leading
  `ORDER BY` item is an expression that isn't an output column, or `\gdesc` fails, only the
  multiset is compared (the check passes as `order-unverified`).
- When a `LIMIT` may legitimately pick different rows (no usable sort key, or a non-literal
  limit) and the rows differ, the query is a SKIP: neither result can be called wrong.
- If both the heap and the mirror query fail, they match only with the same SQLSTATE and the
  same message (after removing the pg_duckdb prefix and schema qualifiers). A timeout or a
  dead session never matches.
- In the cdc layer, the mirror is read in the session that wrote, right after a fence commit
  (`SELECT pg_current_xact_id()`), so read-your-writes must already show every write. A mirror
  that matches only when read again later counts as a stale read. A failed heap read, a failed
  mirror read, a rejected or crashed `create_table` and a failed mirror drop are all recorded as
  events of the table's `cdc` check, so none of them can pass silently.
- `json` and `jsonb` cells are compared after JSON normalisation in the cdc layer: whitespace is
  removed and object keys are sorted, while numbers keep their text and duplicate keys are kept.
  The mirror's reformatting of JSON text is reported by the data layer (verdict `equivalent`).
- In the data layer, every CDC step is read the same way (fence commit, then the mirror), so a
  stale read is the verdict `stale`. A moonlink panic during a probe makes it `crashed` even if
  the mirror still matches. `data.copy` and `data.type` checks carry one event per diverging
  column, step or refused value, matched one by one like cdc events.
- After every unit the server log is swept: a panic or crash restart that no check reported
  fails `panic.<data|run>.<file>` or `backend.<data|run>.<file>`.
- Each query difference is classified by a third run on the heap tables with
  `duckdb.force_execution = true`, used only when `EXPLAIN` shows DuckDB planned it:
  1. a mirror query that timed out or killed its session is a FAIL;
  2. if DuckDB-on-heap returns the heap's answer, the mirror is wrong: FAIL;
  3. if it returns the mirror's answer, the difference is DuckDB semantics and is attributed to
     the `engine` bug of `expected.txt`; a DuckDB planning error (Binder, Catalog, Not
     implemented, ...) on both is a SKIP, and an error of the mirror read path itself (moonlink,
     its RPC, IO) is a FAIL;
  4. a window query whose result depends on physical row order (an order-sensitive function such
     as `row_number()` or `lag()` over a window without `ORDER BY`, or a `ROWS`/`GROUPS` frame or
     `EXCLUDE` clause) counts as DuckDB semantics when DuckDB-on-heap differs from the heap as well;
  5. anything else is a FAIL.
- After a divergence the mirror is dropped and re-created, so later statements are checked
  independently. If the initial copy itself differs only in some columns, those columns are
  left out of later CDC comparisons of that table, and queries reading them inherit the table's
  verdict instead of failing again.
- Before `ALTER TABLE`, `TRUNCATE` or `DROP TABLE` on a mirrored source, the harness drops the
  mirror and re-creates it afterwards. A statement can opt out with a `-- md:unprotected`
  comment (see `extra_cdc.sql`).

### Data-type verdicts

One `TYPE` line per type at the end of the run:

| verdict | meaning | status |
|---|---|---|
| exact | identical in every step | PASS |
| rejected | `create_table` refused the table with an error | FAIL (the column type is in the detail) |
| rejected-values | `create_table` refused some values by name (for example `NaN`, an infinite timestamp or a multi-dimensional array); they are removed and listed, and the remaining values are exact | FAIL |
| equivalent | only JSON whitespace or key order differs | FAIL |
| stale | the fenced read missed a write; the mirror matched only when read again | FAIL |
| errored | the mirror errors on scan or snapshot | FAIL |
| lossy | the mirror returns different data without an error (silent divergence) | FAIL |
| crashed | moonlink panicked, or `create_table` hung | FAIL |

A clean rejection is what a release wants for a type or value mirrors don't support; `expected.txt`
lists those as limitations, so they report `XFAIL(limitation:...)` (see below).

## Output

The line format is the one of `scripts/e2e-smoke.sh` (see the
[end-to-end gate section of TESTING.md](../../TESTING.md#end-to-end-gate-scriptse2e-smokesh)):

```
RESULT <check> PASS|FAIL|XFAIL(<bugs>)|XFAIL(limitation:<bugs>)|XPASS(<bugs>)|SKIP [detail]
BUILD <k>=<v>     what was tested (commits, library hashes, server version, cassert)
TYPE <type> <verdict> probes=<n> worst=<check>
STAT <counter> <n>
KNOWN <bug> OPEN|FIXED|NOT-SEEN <check>=<status>,... class=blocking|limitation [ids=<ids>]
SUMMARY mirror_diff pg<major> layers=<..> pass=N xpass=N fail=N xfail=N xfail_limitation=N skip=N unverified=N
BLOCKING mirror_diff <n> [<bug>,...]
GATE mirror_diff PASS|FAIL|BLOCKED|INCOMPLETE
```

- `GATE` and the exit status follow the
  [release policy](../../TESTING.md#release-policy-blocking-and-limitation): FAIL (exit 1) >
  BLOCKED (exit 4) > INCOMPLETE (exit 3, the run was limited by `--files` or `--layers`) >
  PASS (exit 0). `XFAIL(limitation:...)` does not count against the gate.
- Unlike the end-to-end gate, plain SKIPs don't count against this gate. They are queries that
  can't be compared: DuckDB can't plan them, the session's role may not run DuckDB, they are
  volatile or side-effecting, they ran inside a transaction with uncommitted writes, they read a
  mirror whose initial copy is already known to be wrong (`reads lossy mirror`, see the table's
  cdc check), or a `LIMIT` may pick different rows. The exception is an `unverified` SKIP: a
  mirror that could not be checked because moonlink was wedged or the file timed out, or a
  `--host` run without `--server-log`. Unless `expected.txt` lists it, it fails the gate like a
  FAIL.
- Known gaps: after `service.<file>.wedged`, later DML of the file is no longer compared, and a
  mirrored table that the file drops after the wedge reports PASS instead of unverified (the
  wedge check itself fails).
- `--exit-regression` exits 0 iff no check FAILs and no check is unverified in a way
  `expected.txt` doesn't list, whatever bugs are still listed. Use it to check a change against
  the known differences.

To re-apply an edited known-differences list to a finished run without a database:

```sh
python3 tests/mirror_diff/mirror_diff.py --rescore <workdir>/results.jsonl [expected.txt] [--exit-regression]
```

It writes `<workdir>/results.rescored.jsonl` and prints the RESULT/SUMMARY/GATE lines again.

## Known differences: `expected.txt`

The model is the one of `scripts/e2e-known-bugs.txt`. Bugs are declared once with their class
(`bug <name> class=blocking|class=limitation <description>`); the classes mean what TESTING.md
says. Rules map failures to bugs:

```
check <check-glob> <bug>[,<bug>...] [<regex>]
engine <bug>
```

- `<check-glob>` is an exact check name or an `fnmatch` glob. A `.vN` (repeated check) or `.sN`
  (later table state) suffix also matches the rule for the name without it.
- The optional `<regex>` (Python syntax) is searched in the failure signature. For most checks
  that is the RESULT detail. A `cdc`, `data.copy` or `data.type` check is a list of events, one
  signature each (for cdc
  `<stage>: <detail> :: <statement>`), for example
  `dml: value col=c type=character(3) heap-only: ab  || mirror-only: ab :: UPDATE ...`; the
  check reports XFAIL only if every event matches a rule, with the union of their bugs. Pin rules
  to what identifies the bug (the column type, the panic site `file.rs:line:col`, the TRAP
  text, the statement) rather than to line numbers.
- A FAIL that matches becomes `XFAIL(<bugs>)`, or `XFAIL(limitation:<bugs>)` when all its bugs are
  limitations. A PASS matched by a rule without a regex, or by a rule naming the check exactly,
  becomes `XPASS(<bugs>)`: the bug looks fixed there, so remove or narrow the rule.
- `engine <bug>` is the bug attached to query differences that DuckDB also produces on the heap.

## Unit tests

`python3 tests/mirror_diff/test_mirror_diff.py` tests the comparator, the query classifier, the
known-differences matching and gate codes, and the SQL splitter. It needs no database.

## Directives in SQL files

Comments at the end of a statement in the extra files:

- `-- md:check=<name>`: report `stmt.<file>.<name>`, PASS iff the statement succeeds.
- `-- md:check-error=<name>`: PASS iff the statement fails with an ERROR (not a crash or hang).
- `-- md:no-compare`: skip the mirror comparison after this DML statement.
- `-- md:unprotected`: run `TRUNCATE`/`ALTER`/`DROP` without dropping the mirror first.

## Extra files

- `extra_cdc.sql`: CDC cases the regression suite does not cover well: NULL transitions,
  primary-key updates, `UPDATE ... FROM` and `DELETE ... USING`, keyless duplicate rows,
  savepoints, rollbacks, TOASTed values with updates that leave them unchanged, a 150k-row
  transaction, `create_snapshot` after a write, and TRUNCATE.
- `extra_tables.sql`: source-table shapes: dropped columns, generated and identity columns,
  unlogged, inheritance, `REPLICA IDENTITY USING INDEX`, unusual column names, partitioned.

Both run like regression files.
