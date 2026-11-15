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
  need it, and without it those statements fail harmlessly.
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
| `--keep` | keep the databases and leave the server running, for debugging |
| `--expected FILE` | known-failure list (default `expected.txt` next to the script) |
| `--converge-secs N` | how long a mirror may lag before it counts as diverged (default 10) |
| `--trace` | log every statement to `<workdir>/trace.sql` |
| `--resume` | continue an interrupted run in the same `--workdir` (finished files are listed in `done.txt`) |

A full run (all layers, about 60 regress files, each in its own throwaway cluster) takes 1–3
hours, most of it spent waiting out moonlink hangs after panics. Artifacts in `<workdir>`:

- `results.jsonl`: one JSON record per check with the full evidence (statement, sample rows that
  differ, errors). `python3 summarize.py <workdir>/results.jsonl` renders it as markdown tables.
- `logs/<layer>-<file>.log`: the server log of each file's cluster (moonlink panics, backtraces).
- `trace.sql`: with `--trace`, every statement each session sent, with outcome and duration.

## What is checked

| Layer | Check name | Meaning |
|---|---|---|
| data | `data.copy.<file>.<table>` | Each table of the data-type files is created and filled without a mirror, then mirrored by initial copy and compared, before and after `create_snapshot`. |
| data | `data.type.<type>` | All values of a built-in column type, pooled from every file, in one table: initial copy, CDC INSERT, CDC UPDATE that leaves the value unchanged, CDC DELETE, and after `create_snapshot`. |
| data | `data.type.<file>.<type>` | The same for types a file defines itself (enum, domain, composite, range). |
| cdc | `cdc.<file>.<table>` | The file runs statement by statement on heap tables while every table in `public` is mirrored into schema `mc`. Mirrors are created for fixtures up front and for the file's own tables right after `CREATE TABLE`. After each DML statement or COMMIT, the mirrors of the tables it names must equal the heap. |
| query | `query.<file>.L<line>` | Each SELECT that names a mirrored table runs again with `search_path = mc, ...` and is compared with the heap result. |
| – | `panic.<file>.L<line>` | moonlink panicked while that statement ran. |

Comparison rules:

- Rows are compared as Postgres prints them. Order is ignored unless that is the only
  difference, which still passes as `order`. For query checks, numbers that differ only in
  formatting or scale (`2.5` vs `2.5000000000000000`) pass as `tolerant`. Data and CDC checks
  require exact equality.
- In the cdc layer, `json` and `jsonb` cells are compared after JSON normalisation (compact,
  sorted keys). The mirror's reformatting of JSON text is reported once per type by
  `data.type.json` / `data.type.jsonb` (verdict `equivalent`).
- Each query diff is classified by a third run on the heap tables with
  `duckdb.force_execution = true`. If DuckDB-on-heap returns the mirror's answer, the diff is a
  DuckDB semantics difference: XFAIL, attributed to the bug named by `engine` in
  `expected.txt`. If it returns the heap's answer, the mirror is wrong (FAIL). If neither DuckDB
  path can run the query, it is SKIP (counted, not a failure). Window frames whose answer
  depends on physical row order count as DuckDB semantics.
- After a divergence, the mirror is dropped and re-created so later statements are checked
  independently. If the initial copy itself is lossy, the affected columns are excluded from
  later CDC comparisons. Queries reading those columns inherit the table's verdict instead of
  failing again.
- Before `ALTER TABLE`, `TRUNCATE` or `DROP TABLE` on a mirrored source, the harness drops the
  mirror and re-creates it afterwards. A statement can opt out with a `-- md:unprotected`
  comment (see `extra_cdc.sql`).

Data-type verdicts, one `TYPE` line per type at the end:

| verdict | meaning | status |
|---|---|---|
| exact | identical in every step | PASS |
| rejected | `create_table` refused the table with an error | SKIP |
| equivalent | only JSON formatting differs | FAIL |
| errored | the mirror errors on scan or snapshot | FAIL |
| lossy | the mirror returns different data without an error (silent divergence) | FAIL |
| crashed | moonlink panicked | FAIL |

## Output

The format is shared with `scripts/e2e-smoke.sh`:

```
RESULT <check> PASS|FAIL|XFAIL(<bugs>)|XPASS(<bugs>)|SKIP [detail]
BUILD <k>=<v>     what was tested (commits, library hashes, server version)
TYPE <type> <verdict> probes=<n> worst=<check>
STAT <counter> <n>
KNOWN <bug> OPEN|FIXED|NOT-SEEN <check>=<status>,... xfail=<n> xpass=<n> class=blocking|limitation [ids=<ids>]
SUMMARY mirror_diff pg<major> layers=<..> pass=N xpass=N fail=N xfail=N skip=N
BLOCKING mirror_diff <n> [<bug>,...]      blocking bugs listed in expected.txt (seen or not)
GATE mirror_diff PASS|FAIL|BLOCKED
```

- The exit status is 0 iff there is no unexpected FAIL (regression rule).
- `GATE` applies the release policy of `scripts/e2e-known-bugs.txt` (see TESTING.md): `FAIL` if any
  check FAILs or XFAILs on a `blocking` bug; `BLOCKED` if none did but `expected.txt` still lists a
  `blocking` bug; `PASS` otherwise. XFAILs on `limitation` bugs (DuckDB semantics, JSON text
  form) and SKIPs (types rejected cleanly at `create_table`, queries DuckDB cannot run) don't
  count against it.
- `KNOWN ... ids=` shows tracker ids when `--bug-ids FILE` (or `MIRROR_DIFF_BUG_IDS`) points to a
  file of `<bug> <ids>` lines; that file is not part of the repository.

To re-apply an edited known-differences list to a finished run without a database:

```sh
python3 tests/mirror_diff/mirror_diff.py --rescore <workdir>/results.jsonl [expected.txt]
```

It writes `<workdir>/results.rescored.jsonl` and prints the RESULT/SUMMARY/GATE lines again.

## Known differences: `expected.txt`

Same model as `scripts/e2e-known-bugs.txt`:

```
bug <name> blocking|limitation <description>
check <check-glob> <bug>[,<bug>...] [<regex>]
engine <bug>
```

- `<check-glob>` is an exact name or an `fnmatch` glob; the first matching `check` line wins.
- The optional `<regex>` (Python syntax) must be found in the failure detail, e.g.
  `check cdc.* numeric-digits-shifted ^types=\[numeric\]` matches only CDC failures whose diverged
  columns are all plain `numeric`.
- A FAIL that matches becomes `XFAIL(<bugs>)`. A PASS that matches a line without a regex becomes
  `XPASS(<bugs>)`: the bug looks fixed, so remove its lines in the same change.
- `engine <bug>` is the bug attached to query differences that DuckDB also produces on the heap.
- Moving a bug from `blocking` to `limitation` is a release decision, not a test change.

## Directives in SQL files

Comments at the end of a statement in the extra files:

- `-- md:check=<name>`: report `stmt.<file>.<name>`, PASS iff the statement succeeds.
- `-- md:check-error=<name>`: PASS iff the statement fails with an ERROR (not a crash or hang).
- `-- md:no-compare`: skip the mirror comparison after this DML statement.
- `-- md:unprotected`: run `TRUNCATE`/`ALTER`/`DROP` without dropping the mirror first.

Other checks: `backend.<file>.L<line>` (a Postgres backend died and the server went through
crash recovery), `service.<file>.wedged` (mooncake RPCs kept timing out, so the file continues
without mirrors), `service.<layer>.<file>` (moonlink answers `list_tables` after the file).

## Extra files

- `extra_cdc.sql`: CDC cases the regression suite does not cover well: NULL transitions,
  primary-key updates, `UPDATE ... FROM` and `DELETE ... USING`, keyless duplicate rows,
  savepoints, rollbacks, TOASTed values with updates that leave them unchanged, a 150k-row
  transaction, and TRUNCATE.
- `extra_tables.sql`: source-table shapes: dropped columns, generated and identity columns,
  unlogged, inheritance, `REPLICA IDENTITY USING INDEX`, unusual column names, partitioned.

Both run like regression files.
