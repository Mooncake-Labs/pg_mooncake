#!/usr/bin/env python3
"""Differential mirror testing for pg_mooncake, driven by the PostgreSQL regression suite.

The harness reads the regression files from a PostgreSQL source tree at run time (nothing is
copied into this repository) and checks three things:

  data   Fidelity of every column type. Each data-type file is replayed without mirrors; every
         table is then mirrored (initial copy) and compared row by row with the heap
         (data.copy.<file>.<table>). All values of each built-in column type are pooled across
         files and probed in isolation: initial copy, CDC insert, CDC update that leaves the value
         unchanged, CDC delete, and after create_snapshot (data.type.<type>). Types defined by a
         file (enum, domain, composite, range) are probed in that file's database
         (data.type.<file>.<type>). Verdicts: exact, rejected (create_table refused the table),
         equivalent (only JSON formatting differs), lossy (silent divergence), errored, crashed.

  cdc    Each regress file is run statement by statement against heap tables while every table
         in schema public is mirrored into schema "mc" (fixtures up front, the file's own tables
         right after CREATE TABLE). After each DML statement or COMMIT, the mirrors of the tables
         it names must equal the heap (same session, so scans wait for the session's last
         commit), and again after mooncake.create_snapshot at the end (cdc.<file>.<table>).

  query  Every SELECT of the file that names a mirrored table is run a second time with
         search_path = mc, ... so it reads the mirrors (DuckDB executes it), and compared with
         the heap result (query.<file>.L<line>). A difference is classified by a third run on the
         heap tables with duckdb.force_execution: if DuckDB-on-heap gives the mirror's answer it
         is DuckDB semantics (XFAIL with the engine-semantics bug), if it gives the heap's
         answer the mirror is wrong (FAIL). Queries neither DuckDB path can run are SKIPs.

Output (shared with scripts/e2e-smoke.sh): one line per check,
  RESULT <check> PASS|FAIL|XFAIL(<bugs>)|XPASS(<bugs>)|SKIP [detail]
then BUILD / TYPE / STAT / KNOWN information lines, and
  SUMMARY mirror_diff pg<major> layers=<..> pass=N xpass=N fail=N xfail=N skip=N
  BLOCKING mirror_diff <n> [<bug>,...]
  GATE mirror_diff PASS|FAIL|BLOCKED
Exit status: 0 iff there is no unexpected FAIL (regression rule). See README.md for the GATE rule.
"""

import argparse
import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import sqlsplit  # noqa: E402
from psqlsession import NULL_TOKEN, PsqlSession  # noqa: E402

# ------------------------------------------------------------------------------------------------
FIXTURES = "test_setup"
DATA_FILES = [
    "boolean", "int2", "int4", "int8", "oid", "float4", "float8", "numeric", "money",
    "char", "varchar", "text", "name", "strings", "date", "time", "timetz", "timestamp",
    "timestamptz", "interval", "uuid", "json", "jsonb", "arrays", "enum", "domain", "bit",
    "inet", "macaddr", "macaddr8", "point", "tstypes", "rangetypes", "xml", "pg_lsn", "rowtypes",
]
QUERY_FILES = [
    "select", "select_distinct", "select_distinct_on", "select_having", "select_implicit",
    "aggregates", "join", "subselect", "union", "window", "groupingsets", "with", "case",
    "limit", "select_into", "expressions",
]
CDC_FILES = ["insert", "update", "delete", "returning", "insert_conflict", "merge",
             "transactions", "copydml"]
LOCAL_FILES = {"extra_cdc": os.path.join(HERE, "extra_cdc.sql"),
               "extra_tables": os.path.join(HERE, "extra_tables.sql")}
# Files of the parallel schedule that create objects these files use (replayed first, unchecked).
PREREQS = {
    "select": ["create_index"], "join": ["create_index"], "subselect": ["create_index"],
    "union": ["create_index"], "aggregates": ["create_index", "create_aggregate"],
}

# psql meta-commands that would change output format, connection, or the client itself.
SKIP_META = re.compile(
    r"^\\(c|connect|o|out|pset|a|t|x|f|H|html|C|T|!|i|ir|include|include_relative|lo_\w+|"
    r"watch|timing|q|quit|password|encoding|prompt|w|write|e|edit|ef|ev|s|cd|errverbose|"
    r"crosstabview|gdesc|startpipeline|endpipeline|syncpipeline|sendpipeline|flushrequest|"
    r"getresults|flush|bind|bind_named|parse|close_prepared)\b")
SKIP_SET_VAR = re.compile(r"^\\(set|unset)\s+(ON_ERROR_STOP|ECHO|QUIET|ON_ERROR_ROLLBACK|"
                          r"FETCH_COUNT|SHOW_ALL_RESULTS|AUTOCOMMIT|SINGLELINE|SINGLESTEP)\b", re.I)

QUERY_START = re.compile(r"^\(*\s*(select|with|values|table)\b", re.I)
DML_START = re.compile(r"^(insert|update|delete|merge|copy)\b", re.I)
DML_CTE = re.compile(r"\bas\s*(not\s+)?(materialized\s*)?\(\s*(insert|update|delete|merge)\b", re.I)
SELECT_INTO = re.compile(r"^\(*\s*select\b(?:(?!\bfrom\b).)*?\binto\b", re.I)
TXN_START = re.compile(r"^(begin|start\s+transaction)\b", re.I)
TXN_END = re.compile(r"^(commit|end|rollback|abort)\b(?!\s+prepared)", re.I)
PRE_DDL = re.compile(r"^(drop\s+table|truncate|alter\s+table)\b", re.I)
DESTRUCTIVE = re.compile(r"^(drop\s+(table|type|domain|schema|extension)|truncate|alter\s+(table|type|domain))\b", re.I)
SELECT_STAR = re.compile(r"(\bselect|,)\s*(distinct\s+)?([\w\"]+\.)?\*", re.I)
VOLATILE = re.compile(
    r"\b(nextval|setval|currval|lastval|random|random_normal|setseed|now|current_timestamp|"
    r"clock_timestamp|statement_timestamp|transaction_timestamp|timeofday|localtime|"
    r"localtimestamp|current_time|current_date|txid_\w+|pg_current_xact\w*|pg_backend_pid|"
    r"gen_random_uuid|uuidv\d|pg_sleep\w*|pg_advisory\w*|pg_try_advisory\w*|pg_stat\w*|"
    r"pg_relation_size|pg_table_size|pg_total_relation_size|ctid|xmin|xmax|cmin|cmax|tableoid|"
    r"lo_\w+|pg_notify|set_config|pg_read\w*|pg_ls\w*|pg_current_wal\w*|inet_client\w*|"
    r"pg_column_size|pg_column_compression|explain|for\s+(update|share|no\s+key|key))\b", re.I)
NUMBER = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
SCHEMA_MIRROR = "mc"
VERDICT_ORDER = ["crashed", "lossy", "errored", "equivalent", "rejected", "exact"]


def ident_re(name):
    return re.compile(r"(?<![\w.\"$])" + re.escape(name) + r"(?![\w\"$])", re.I)


def strip_literals(sql):
    """SQL with string literals blanked, so 'tbl'::regclass in catalog queries is not a table reference."""
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


def order_dependent(sql):
    """Window frames whose ORDER BY is missing or not unique: the result depends on physical row
    order, which differs between a heap and its mirror's parquet files."""
    return bool(re.search(r"\bover\s*\((?:(?!order\s+by)[^)])*\b(rows|groups)\s+between", sql, re.I)
                or re.search(r"\b(rows|groups|range)\s+between\b.*\bexclude\b", sql, re.I)
                or re.search(r"\b(first_value|last_value|nth_value)\s*\(", sql, re.I))


def main_verb(sql):
    """First keyword of the statement outside parentheses: WITH x AS (...) INSERT ... is an INSERT."""
    flat, prev = strip_literals(sql), None
    while prev != flat:
        prev, flat = flat, re.sub(r"\([^()]*\)", " ", flat)
    m = re.search(r"\b(select|values|table|insert|update|delete|merge)\b", re.sub(r"^\W*with\s+(recursive\s+)?", "", flat, flags=re.I), re.I)
    return m.group(1).lower() if m else ""


def one_line(s, n=300):
    s = " ".join(str(s).split())
    return s if len(s) <= n else s[:n] + "..."


def qlit(s):
    return "'" + s.replace("'", "''") + "'"


def qident(s):
    return '"' + s.replace('"', '""') + '"'


def type_key(typ):
    return re.sub(r"[^A-Za-z0-9]+", "_", typ.replace("[]", "_array")).strip("_")


def type_label(typ):
    return re.sub(r"\(\d+(,\d+)?\)", lambda m: "(p,s)" if "," in m.group(0) else "(n)", typ)


# ------------------------------------------------------------------------------------------------
class Expectations:
    """Known differences (expected.txt), same model as scripts/e2e-known-bugs.txt:

      bug <key> blocking|limitation <description>
      check <check-glob> <key>[,<key>...] [<regex>]
      engine <key>

    A failing check matching a check line (fnmatch glob on the name; the optional Python regex is
    searched in the failure detail) reports XFAIL(<keys>); the first matching line wins. A passing
    check matching a line without a regex reports XPASS(<keys>). "engine" names the bug that the
    classifier attaches to DuckDB-semantics differences of query checks."""

    def __init__(self, path, ids_path=None):
        self.rules = []
        self.bugs = {}       # key -> (class, description)
        self.engine = []
        self.ids = {}        # key -> tracker ids, from a private file outside the repository
        if path and os.path.exists(path):
            for n, raw in enumerate(open(path), 1):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(None, 3)
                kind = parts[0]
                if kind == "bug" and len(parts) >= 3 and parts[2] in ("blocking", "limitation"):
                    self.bugs[parts[1]] = (parts[2], parts[3] if len(parts) > 3 else "")
                elif kind == "check" and len(parts) >= 3:
                    rx = re.compile(parts[3]) if len(parts) > 3 else None
                    self.rules.append((parts[1], parts[2].split(","), rx))
                elif kind == "engine" and len(parts) == 2:
                    self.engine = [parts[1]]
                else:
                    sys.exit(f"{path}:{n}: cannot parse: {line}")
            for _, keys, _ in self.rules:
                for k in keys:
                    if k not in self.bugs:
                        sys.exit(f"{path}: check refers to undeclared bug {k}")
        if ids_path and os.path.exists(ids_path):
            for raw in open(ids_path):
                p = raw.split()
                if len(p) >= 2 and not raw.startswith("#"):
                    self.ids[p[0]] = p[1]

    def match(self, check, detail, passed=False):
        for k, (pat, keys, rx) in enumerate(self.rules):
            if not fnmatch.fnmatchcase(check, pat):
                continue
            if rx is not None and (passed or not rx.search(detail or "")):
                continue
            return k, keys
        return None, None

    def blocking(self, keys):
        return [k for k in keys if self.bugs.get(k, ("blocking",))[0] == "blocking"]


class Reporter:
    def __init__(self, exp, log_path, resume=False):
        self.exp = exp
        self.counts = {"PASS": 0, "XPASS": 0, "FAIL": 0, "XFAIL": 0, "SKIP": 0}
        self.xfail_ids = set()
        prior = []
        if resume and os.path.exists(log_path):
            with open(log_path) as f:
                prior = [json.loads(line) for line in f if line.strip()]
        self.log = open(log_path, "a" if resume else "w")
        self.stats = {}
        self.known = {}  # bug key -> {"XFAIL": n, "XPASS": n, "checks": [...]}
        self.blocking_xfails = 0
        self.names = {}
        for rec in prior:  # results of files completed before a --resume
            self.account(rec["check"], rec["status"], rec.get("ids") or [])
            self.names[rec["check"]] = self.names.get(rec["check"], 0) + 1

    def account(self, check, status, ids):
        self.counts[status] += 1
        if status == "XFAIL":
            self.xfail_ids.update(ids or ["?"])
            if not ids or self.exp.blocking(ids):
                self.blocking_xfails += 1
        for i in ids if status in ("XFAIL", "XPASS") else []:
            kn = self.known.setdefault(i, {"XFAIL": 0, "XPASS": 0, "checks": []})
            kn[status] += 1
            if len(kn["checks"]) < 3:
                kn["checks"].append(f"{check}={status}")

    def result(self, check, status, detail="", record=None, ids=None):
        ids = list(ids or [])
        n = self.names.get(check, 0) + 1
        self.names[check] = n
        if n > 1:
            check = f"{check}.v{n}"
        if status == "FAIL" and not ids:
            k, rule_ids = self.exp.match(check, detail)
            if k is not None:
                status, ids = "XFAIL", rule_ids
        elif status == "FAIL":
            status = "XFAIL"
        elif status == "PASS":
            k, rule_ids = self.exp.match(check, detail, passed=True)
            if k is not None:
                status, ids = "XPASS", rule_ids
        label = f"{status}({','.join(map(str, ids)) or '?'})" if status in ("XFAIL", "XPASS") else status
        self.account(check, status, ids)
        print(f"RESULT {check} {label}" + (f" {one_line(detail)}" if detail else ""), flush=True)
        rec = dict(record or {})
        rec.update(check=check, status=status, ids=ids, detail=detail)
        self.log.write(json.dumps(rec, default=str) + "\n")
        self.log.flush()

    def stat(self, key, n=1):
        self.stats[key] = self.stats.get(key, 0) + n

    def summary(self, major, layers):
        for k in sorted(self.stats):
            print(f"STAT {k} {self.stats[k]}", flush=True)
        for key in sorted(set(self.exp.bugs) | set(self.known)):
            kn = self.known.get(key, {"XFAIL": 0, "XPASS": 0, "checks": []})
            state = "OPEN" if kn["XFAIL"] else ("FIXED" if kn["XPASS"] else "NOT-SEEN")
            cls = self.exp.bugs.get(key, ("blocking", ""))[0]
            ids = self.exp.ids.get(key)
            print(f"KNOWN {key} {state} {','.join(kn['checks']) or '-'} xfail={kn['XFAIL']} xpass={kn['XPASS']} "
                  f"class={cls}" + (f" ids={ids}" if ids else ""), flush=True)
        c = self.counts
        print(f"SUMMARY mirror_diff pg{major} layers={layers} pass={c['PASS']} xpass={c['XPASS']} "
              f"fail={c['FAIL']} xfail={c['XFAIL']} skip={c['SKIP']}", flush=True)
        listed = sorted(k for k, (cls, _) in self.exp.bugs.items() if cls == "blocking")
        print(f"BLOCKING mirror_diff {len(listed)}" + (f" {','.join(listed)}" if listed else ""), flush=True)
        if c["FAIL"] or self.blocking_xfails:
            print("GATE mirror_diff FAIL", flush=True)
        elif listed:
            print("GATE mirror_diff BLOCKED", flush=True)
        else:
            print("GATE mirror_diff PASS", flush=True)
        return c["FAIL"] == 0


# ------------------------------------------------------------------------------------------------
class Server:
    """A throwaway cluster (default) or an existing server (--host)."""

    def __init__(self, args):
        self.args = args
        self.bindir = args.bindir
        self.psql = os.path.join(self.bindir, "psql")
        self.datadir = os.path.join(args.workdir, "data")
        self.logfile = os.path.join(args.workdir, "postgres.log")
        self.host = args.host or os.path.join(args.workdir, "sock")
        self.port = args.port
        self.user = args.user or os.environ.get("USER", "postgres")
        self.managed = args.host is None

    def conninfo(self, db):
        return f"host={self.host} port={self.port} user={self.user} dbname={db}"

    def setup(self, label="cluster"):
        """Fresh throwaway cluster (one per file, so a crash loop or leaked state in one file
        cannot affect the next). Logs go to <workdir>/logs/<label>.log."""
        if not self.managed:
            return
        os.makedirs(os.path.join(self.args.workdir, "logs"), exist_ok=True)
        if os.path.exists(self.datadir):
            self.pg_ctl("stop", "-m", "immediate", check=False)
        self.logfile = os.path.join(self.args.workdir, "logs", f"{label}.log")
        os.makedirs(self.host, exist_ok=True)
        if os.path.exists(self.datadir):
            self.pg_ctl("stop", "-m", "immediate", check=False)
            shutil.rmtree(self.datadir)
        subprocess.run([os.path.join(self.bindir, "initdb"), "-D", self.datadir, "-U", self.user,
                        "--locale=C", "--encoding=UTF8"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(os.path.join(self.datadir, "postgresql.conf"), "a") as f:
            f.write(f"""
port = {self.port}
unix_socket_directories = '{self.host}'
listen_addresses = ''
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
max_replication_slots = 64
max_wal_senders = 64
max_connections = 200
fsync = off
duckdb.allow_community_extensions = true
duckdb.allow_unsigned_extensions = true
duckdb.autoinstall_known_extensions = false
""")
            if self.args.duckdb_extension_dir:
                f.write(f"duckdb.extension_directory = '{self.args.duckdb_extension_dir}'\n")
            for extra in self.args.conf or []:
                f.write(extra + "\n")
        self.pg_ctl("start")

    def pg_ctl(self, *a, check=True):
        return subprocess.run([os.path.join(self.bindir, "pg_ctl"), "-D", self.datadir,
                               "-l", self.logfile, "-w", *a], check=check,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def restart(self):
        if self.managed:
            self.pg_ctl("restart", "-m", "fast", check=False)

    def stop(self):
        if self.managed:
            self.pg_ctl("stop", "-m", "fast", check=False)

    def admin(self, db, sql, timeout=60):
        try:
            p = subprocess.run([self.psql, "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", self.conninfo(db),
                                "-c", sql], capture_output=True, text=True, timeout=timeout)
            return p.returncode, p.stdout.strip(), p.stderr.strip()
        except subprocess.TimeoutExpired:
            return -1, "", "timeout"

    def cancel(self, pid):
        self.admin("postgres", f"SELECT pg_cancel_backend({pid})", timeout=10)
        time.sleep(2)
        self.admin("postgres", f"SELECT pg_terminate_backend({pid})", timeout=10)

    def panic_lines(self):
        if not os.path.exists(self.logfile):
            return []
        with open(self.logfile, errors="replace") as f:
            return [line.strip() for line in f if "panicked at" in line]

    def panics(self):
        return len(self.panic_lines())

    def backend_crashes(self):
        """Postgres backends killed by a signal (server-wide crash recovery follows)."""
        if not os.path.exists(self.logfile):
            return []
        with open(self.logfile, errors="replace") as f:
            lines = f.readlines()
        out = []
        for i, line in enumerate(lines):
            if "was terminated by signal" in line:
                trap = next((ln.strip() for ln in reversed(lines[max(0, i - 60):i]) if "TRAP:" in ln), "")
                out.append((line.strip(), trap))
        return out

    def healthy(self, db, why=None):
        rc, _, err = self.admin(db, "SELECT count(*) FROM mooncake.list_tables()", timeout=30)
        if why is not None:
            why.append(err or f"rc={rc}")
        return rc == 0

    def recover(self, db):
        """Bring the moonlink service back; returns True if it answers list_tables."""
        for _ in range(10):  # the bgworker restarts by itself after a crash
            if self.healthy(db):
                return True
            time.sleep(3)
        self.restart()
        for _ in range(30):
            if self.healthy(db):
                return True
            time.sleep(2)
        return False


# ------------------------------------------------------------------------------------------------
def _tol(c):
    if NUMBER.match(c):
        try:
            f = float(c)
        except ValueError:
            return c
        if f == int(f) and abs(f) < 1e15:
            return str(int(f))
        return "%.10g" % f
    return c


def json_norm(rows, idx):
    """Rows with the cells at positions idx re-serialised as compact JSON (mirrors print
    json/jsonb in DuckDB's compact form; the data layer reports that once per type)."""
    if not idx:
        return rows
    out = []
    for r in rows:
        r = list(r)
        for i in idx:
            if i < len(r) and r[i] != NULL_TOKEN:
                try:
                    r[i] = json.dumps(json.loads(r[i]), separators=(",", ":"), ensure_ascii=False, sort_keys=True)
                except ValueError:
                    pass
        out.append(r)
    return out


def same(a, b):
    """Compare two Results: (equal, how) with how in exact|order|tolerant|both-error|..."""
    if not a.ok or not b.ok:
        if not a.ok and not b.ok:
            return True, "both-error"
        return False, "error-vs-rows"
    if a.rows == b.rows:
        return True, "exact"
    if sorted(map(tuple, a.rows)) == sorted(map(tuple, b.rows)):
        return True, "order"
    if sorted(tuple(map(_tol, r)) for r in a.rows) == sorted(tuple(map(_tol, r)) for r in b.rows):
        return True, "tolerant"
    return False, "rows"


def diff_sample(a, b, n=3):
    if not a.ok or not b.ok:
        return f"heap={a.outcome()}:{a.message[:120]} other={b.outcome()}:{b.message[:120]}"
    ca, cb = {}, {}
    for r in a.rows:
        ca[tuple(r)] = ca.get(tuple(r), 0) + 1
    for r in b.rows:
        cb[tuple(r)] = cb.get(tuple(r), 0) + 1
    only_a = [r for r in ca if ca[r] > cb.get(r, 0)][:n]
    only_b = [r for r in cb if cb[r] > ca.get(r, 0)][:n]

    def fmt(rs):
        return "; ".join("|".join(c[:60] for c in r) for r in rs)[:240]
    return f"{len(a.rows)} vs {len(b.rows)} rows; heap-only: {fmt(only_a)} || mirror-only: {fmt(only_b)}"


def json_equivalent(a, b):
    def norm(rows):
        out = []
        for r in rows:
            cells = []
            for c in r:
                try:
                    cells.append(json.dumps(json.loads(c), sort_keys=True))
                except (ValueError, TypeError):
                    cells.append(c)
            out.append(tuple(cells))
        return sorted(out)
    return a.ok and b.ok and norm(a.rows) == norm(b.rows)


TABLES_SQL = (
    "SELECT c.relname, c.relkind, c.relispartition, "
    " coalesce((SELECT json_agg(json_build_array(a.attname, format_type(a.atttypid, a.atttypmod), t.typtype,"
    "     t.typnamespace = 'pg_catalog'::regnamespace, t.typcategory) ORDER BY a.attnum)"
    "   FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid"
    "   WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped), '[]') "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND c.relpersistence IN ('p', 'u') "
    "  AND c.relname NOT LIKE 'md\\_%';")


def read_tables(session):
    r = session.run(TABLES_SQL)
    out = {}
    if r.ok:
        for name, kind, ispart, cols in r.rows:
            out[name] = {"kind": kind, "partition": ispart == "t",
                         "cols": [tuple(c) for c in json.loads(cols)]}
    return out


def select_list(cols):
    """Column list for comparisons."""
    return ", ".join(qident(c[0]) for c in cols)


def json_cols(cols):
    return [i for i, c in enumerate(cols) if c[1] in ("json", "jsonb")]


# ------------------------------------------------------------------------------------------------
class FileRun:
    """Run one regress file in its own database with mirrors attached (cdc + query layers)."""

    def __init__(self, h, name):
        self.h = h
        self.srv = h.srv
        self.rep = h.rep
        self.name = name
        self.db = h.dbname(name)
        self.s = None
        self.c = None          # control session for mooncake RPCs (a failed mooncake RPC poisons the session's RPC client)
        self.tables = {}       # heap table -> state
        self.queries = []      # query outcomes, reported at end of file
        self.events = {}       # table -> divergence records
        self.ever = {}         # table -> "" once mirrored, else the create_table error
        self.catalog_dirty = False
        self.pending_cmp = set()
        self.unit_line = 0
        self.wedged = ""        # set when mooncake RPCs keep timing out (the replication task died)
        self.ctl_timeouts = 0

    # ---- sessions ---------------------------------------------------------------------------
    def new_session(self):
        if self.s:
            self.s.kill()
        self.s = self.h.session(self.db)

    def run(self, text, timeout=None):
        before = self.srv.panics()
        crashes = len(self.srv.backend_crashes())
        r = self.s.run(text, timeout=timeout or self.h.args.stmt_timeout, on_timeout=self.srv.cancel)
        m = re.search(r"md:check(-error)?=([\w.]+)", text)
        if m and not text.startswith(("SAVEPOINT md_", "SELECT set_config", "SET duckdb")):
            want_error = bool(m.group(1))
            good = (r.error and not r.died and not r.timed_out) if want_error else r.ok
            self.rep.result(f"stmt.{self.name}.{m.group(2)}", "PASS" if good else "FAIL",
                            "" if good else f"{r.outcome()}: {r.message[:200]} :: {one_line(text, 160)}")
        if r.died and len(self.srv.backend_crashes()) > crashes:
            line, trap = self.srv.backend_crashes()[-1]
            self.rep.result(f"backend.{self.name}.L{self.unit_line}", "FAIL",
                            f"Postgres backend crashed (server crash recovery): {trap or line} :: {one_line(text, 160)}")
        if r.died:
            self.rep.stat("session.restarted")
            self.new_session()
        if self.srv.panics() > before:
            self.on_panic(text)
        return r

    def ctl(self, sql, timeout=None):
        """Run a mooncake procedure / mirror DDL on the control session; recycle it after any error."""
        if self.c is None or not self.c.alive():
            self.c = self.h.session(self.db)
        before = self.srv.panics()
        r = self.c.run(sql, timeout=timeout or self.h.args.ddl_timeout, on_timeout=self.srv.cancel)
        if not r.ok:
            self.c.kill()
            self.c = None
        if r.timed_out or r.died:
            self.ctl_timeouts += 1
            if self.ctl_timeouts >= 2 and not self.wedged:
                self.wedged = f"line {self.unit_line}: {one_line(sql, 100)}"
                self.rep.result(f"service.{self.name}.wedged", "FAIL",
                                f"mooncake RPCs time out repeatedly from {self.wedged}; no further mirrors in this file")
        if self.srv.panics() > before:
            self.on_panic(sql)
        return r

    def txn_state(self):
        """(in transaction block, has written in it). An aborted block counts as both."""
        r = self.s.run("SELECT now() <> statement_timestamp(), pg_current_xact_id_if_assigned() IS NOT NULL;")
        if not r.ok or not r.rows:
            return True, True
        return r.rows[-1][0] == "t", r.rows[-1][1] == "t"

    def on_panic(self, text):
        lines = self.srv.panic_lines()
        new = lines[self.h.panics_seen:]
        self.h.panics_seen = len(lines)
        if not new:
            return
        where = re.sub(r"^.*panicked at ", "", new[0])
        self.rep.result(f"panic.{self.name}.L{self.unit_line}", "FAIL",
                        f"{len(new)} moonlink panic(s) at {where[:200]} during: {one_line(text, 160)}",
                        record={"panics": new[:5]})
        if not self.srv.recover(self.db):
            raise RuntimeError("moonlink service down and did not recover after panic")
        if not self.s.alive():
            self.new_session()
        # Mirrors are not re-created here: dropping a mirror whose table handler died can hang.
        # Divergences that follow from the panic are re-baselined one by one.

    def remirror_pending(self):
        for name, t in list(self.tables.items()):
            if t.pop("needs_remirror", False):
                self.mirror(name, "re-create after panic")

    # ---- catalog -------------------------------------------------------------------------------
    def sync_catalog(self):
        now = read_tables(self.s)
        for name, info in now.items():
            if info["partition"]:
                continue
            old = self.tables.get(name)
            if old is None:
                self.tables[name] = dict(info, mirrored=False)
                self.mirror(name, "created")
            elif old["cols"] != info["cols"] or old.get("stale"):
                self.drop_mirror(name)
                old.update(info, stale=False)
                self.mirror(name, "schema changed")
        for name in list(self.tables):
            if name not in now:
                self.drop_mirror(name)
                del self.tables[name]
        self.catalog_dirty = False

    def mirror(self, name, reason):
        t = self.tables[name]
        t.update(bad_cols=set(), lossy_all=False)
        if not t["cols"]:
            return
        if self.wedged:
            t["mirrored"] = False
            self.ever.setdefault(name, f"not attempted: moonlink wedged since {self.wedged}")
            return
        crash_type = next((c[1] for c in t["cols"] if c[1] in self.h.crash_types), None)
        if crash_type:
            err = f"not attempted: type {crash_type} crashed moonlink at {self.h.crash_types[crash_type]}"
            t["mirrored"] = False
            self.ever.setdefault(name, err)
            return
        before = self.srv.panics()
        r = self.ctl(f"CALL mooncake.create_table({qlit(SCHEMA_MIRROR + '.' + name)}, {qlit('public.' + name)});")
        if self.srv.panics() > before:
            suspects = [c[1] for c in t["cols"] if c[1] not in self.h.good_types]
            for typ in suspects:
                self.h.crash_types.setdefault(typ, f"{self.name}.{name}")
            t["mirrored"] = False
            self.ever.setdefault(name, f"create_table crashed moonlink (suspect types: {suspects})")
            self.remirror_pending()
            return
        if not r.ok:
            t["mirrored"] = False
            if self.ever.get(name) != "":
                self.ever[name] = one_line(r.message or r.outcome(), 200)
            self.rep.stat("mirror.rejected")
            return
        t["mirrored"] = True
        self.ever[name] = ""
        self.h.good_types.update(c[1] for c in t["cols"])
        self.rep.stat("mirror.created")
        self.compare(name, f"<create_table: {reason}>", stage="copy")

    def drop_mirror(self, name):
        t = self.tables.get(name)
        if not t or not t.get("mirrored"):
            return
        t["mirrored"] = False
        if self.wedged:
            return
        r = self.ctl(f"DROP TABLE IF EXISTS {SCHEMA_MIRROR}.{qident(name)};")
        if not r.ok:
            self.rep.stat("mirror.drop_failed")
            self.events.setdefault(name, []).append(
                {"stage": "drop", "line": self.unit_line, "stmt": "DROP mirror", "error": r.message[:300],
                 "cols": [], "types": []})

    # ---- comparison ----------------------------------------------------------------------------
    def compare(self, name, stmt, stage):
        """Mirror vs heap for one table; records a divergence and re-baselines the mirror."""
        t = self.tables[name]
        if self.wedged:
            return True
        if not t.get("mirrored") or (t["lossy_all"] and stage != "final"):
            return True
        cols = [c for c in t["cols"] if c[0] not in t["bad_cols"]] if stage != "final" else t["cols"]
        if not cols:
            return True
        sel = select_list(cols)
        heap = self.run(f"SELECT {sel} FROM public.{qident(name)};")
        if not heap.ok:
            return True
        jidx = json_cols(cols)
        heap.rows = json_norm(heap.rows, jidx)
        deadline = time.monotonic() + self.h.args.converge_secs
        polls = 0
        reader = self.run
        while True:
            mir = reader(f"SELECT {sel} FROM {SCHEMA_MIRROR}.{qident(name)};")
            if not mir.ok and "duckdb.postgres_role" in mir.message:
                # The file switched to a role without DuckDB rights (SET SESSION AUTHORIZATION);
                # read the mirror as the harness user instead (polling replaces read-your-writes).
                reader = self.ctl
                mir = reader(f"SELECT {sel} FROM {SCHEMA_MIRROR}.{qident(name)};")
            mir.rows = json_norm(mir.rows, jidx)
            eq, how = same(heap, mir)
            if eq and how in ("exact", "order"):
                if polls:
                    self.rep.stat("cdc.converged_after_poll")
                return True
            if mir.died or mir.timed_out or time.monotonic() >= deadline:
                break
            polls += 1
            time.sleep(0.5)
        dcols = self.diverged_columns(cols, heap, mir)
        rec = {"stage": stage, "line": self.unit_line, "stmt": one_line(stmt, 300),
               "cols": sorted(c[0] for c in dcols), "types": sorted({c[1] for c in dcols}),
               "sample": diff_sample(heap, mir), "mirror_error": mir.message[:300]}
        self.events.setdefault(name, []).append(rec)
        self.h.diverged_cols.setdefault((self.name, name), set()).update(rec["cols"] or ["*"])
        self.rep.stat("cdc.divergences")
        if stage == "copy":
            if dcols and len(heap.rows) == len(mir.rows) and mir.ok:
                t["bad_cols"] |= {c[0] for c in dcols}
            else:
                t["lossy_all"] = True
            return False
        if stage != "final":
            self.drop_mirror(name)
            self.mirror(name, "re-baseline after divergence")
        return False

    @staticmethod
    def diverged_columns(cols, heap, mir):
        if not heap.ok or not mir.ok or len(heap.rows) != len(mir.rows):
            return []
        out = []
        for i, col in enumerate(cols):
            if sorted(r[i] for r in heap.rows) != sorted(r[i] for r in mir.rows):
                out.append(col)
        return out

    # ---- statements --------------------------------------------------------------------------
    def mirrored_refs(self, sql):
        if self.wedged:
            return []
        bare = strip_literals(sql)
        return [n for n, t in self.tables.items() if t.get("mirrored") and ident_re(n).search(bare)]

    def masking(self, refs, sql):
        """Mirrors this query reads that are known to be wrong at this moment (lossy initial copy)."""
        out = []
        for name in refs:
            t = self.tables[name]
            if t["lossy_all"]:
                out.append(name)
            elif t["bad_cols"] and (SELECT_STAR.search(sql) or any(ident_re(c).search(sql) for c in t["bad_cols"])):
                out.append(name)
        return out

    def handle_query(self, unit):
        refs = self.mirrored_refs(unit.sql)
        if not refs:
            self.run(unit.text)
            return
        q = {"line": unit.line, "sql": unit.sql, "refs": refs}
        if VOLATILE.search(unit.sql) or unit.meta not in ("", "\\g"):
            self.run(unit.text)
            self.queries.append(dict(q, status="SKIP", reason="volatile or side-effecting"))
            return
        heap = self.run(unit.text)
        in_txn, has_writes = self.txn_state()
        if has_writes:
            self.queries.append(dict(q, status="SKIP", reason="uncommitted writes in the transaction"))
            return
        q["masked_by"] = self.masking(refs, unit.sql)
        mir = self.shadow(unit, mirror=True, in_txn=in_txn)
        eq, how = same(heap, mir)
        q["how"] = how
        if eq and how != "both-error":
            q["status"] = "PASS"
            self.rep.stat(f"query.pass_{how}")
            self.queries.append(q)
            return
        if eq:
            self.rep.stat("query.both_error")
            self.queries.append(dict(q, status="SKIP", reason="errors on heap and on mirror",
                                     heap_error=heap.message[:200], mirror_error=mir.message[:200]))
            return
        duck = self.shadow(unit, mirror=False, in_txn=in_txn)
        if not self.ran_in_duckdb(unit, in_txn):
            duck.error, duck.message = True, "not executed by DuckDB (pg_duckdb fell back to Postgres)"
            duck.rows = []
        q.update(heap=heap.outcome(), mirror=mir.outcome(), duck=duck.outcome(), sample=diff_sample(heap, mir),
                 heap_error=heap.message[:300], mirror_error=mir.message[:300], duck_error=duck.message[:300],
                 duck_vs_mirror=same(duck, mir)[1], duck_vs_heap=same(duck, heap)[1])
        if mir.timed_out or mir.died:
            q.update(status="FAIL", verdict="mooncake", reason=f"mirror query {mir.outcome()}")
        elif same(duck, mir)[0]:
            if mir.ok:
                q.update(status="ENGINE", verdict="engine", reason="DuckDB semantics")
            else:
                q.update(status="SKIP", verdict="unsupported", reason="DuckDB cannot run it")
                self.rep.stat("query.duckdb_unsupported")
        elif order_dependent(unit.sql) and mir.ok and heap.ok and len(mir.rows) == len(heap.rows):
            q.update(status="ENGINE", verdict="engine",
                     reason="result depends on physical row order (window frame without a unique ORDER BY)")
        elif duck.ok and same(duck, heap)[0]:
            q.update(status="FAIL", verdict="mooncake", reason="mirror differs; DuckDB on heap agrees with heap")
        elif not duck.ok and not mir.ok:
            q.update(status="SKIP", verdict="unsupported", reason="DuckDB cannot run it")
            self.rep.stat("query.duckdb_unsupported")
        else:
            q.update(status="FAIL", verdict="unclassified", reason="heap, mirror and DuckDB-on-heap all differ")
        self.queries.append(q)

    def ran_in_duckdb(self, unit, in_txn):
        """duckdb.force_execution silently falls back to Postgres for queries DuckDB cannot plan."""
        sql = unit.sql.rstrip(";")
        pre = ("SAVEPOINT md_explain;\n" if in_txn else "") + "SET duckdb.force_execution = true;"
        post = ("ROLLBACK TO SAVEPOINT md_explain;\nRELEASE SAVEPOINT md_explain;\n" if in_txn else "") + \
            "RESET duckdb.force_execution;"
        self.s.run(pre)
        r = self.run(f"EXPLAIN (COSTS OFF) {sql};")
        if self.s.alive():
            self.s.run(post)
        return r.ok and any("duckdb" in cell.lower() for row in r.rows for cell in row)

    def shadow(self, unit, mirror, in_txn):
        pre, post = [], []
        if in_txn:
            pre.append("SAVEPOINT md_shadow;")
        if mirror:
            pre.append(f"SELECT set_config('search_path', '{SCHEMA_MIRROR}, ' || current_setting('search_path'), false) \\g /dev/null")
            post.append(f"SELECT set_config('search_path', regexp_replace(current_setting('search_path'), '^{SCHEMA_MIRROR}, ', ''), false) \\g /dev/null")
        else:
            pre.append("SET duckdb.force_execution = true;")
            post.append("RESET duckdb.force_execution;")
        if in_txn:
            post = ["ROLLBACK TO SAVEPOINT md_shadow;", "RELEASE SAVEPOINT md_shadow;"] + post
        self.s.run("\n".join(pre))
        r = self.run(unit.text)
        if self.s.alive():
            self.s.run("\n".join(post))
        return r

    def handle_dml(self, unit):
        r = self.run(unit.text)
        if not r.ok:
            return
        refs = self.mirrored_refs(unit.sql)
        in_txn, _ = self.txn_state()
        if in_txn:
            self.pending_cmp.update(refs)
            return
        if "md:no-compare" in unit.text:
            return
        for name in refs:
            self.compare(name, unit.sql, stage="dml")

    def pre_ddl(self, unit):
        """Drop mirrors of tables about to be dropped, truncated or altered (those statements break CDC for the mirror)."""
        if not PRE_DDL.match(unit.sql) or "md:unprotected" in unit.text:
            return
        if self.txn_state()[0]:
            return
        for name in self.mirrored_refs(unit.sql):
            self.drop_mirror(name)
            self.tables[name]["stale"] = True
            self.rep.stat("mirror.dropped_for_ddl")

    def execute(self, unit):
        if unit.kind == "meta":
            if not (SKIP_META.match(unit.meta) or SKIP_SET_VAR.match(unit.meta)):
                self.run(unit.text)
            return
        if unit.kind == "block":
            self.run(unit.text)
            self.catalog_dirty = True
            return
        sql, meta = unit.sql, unit.meta
        if meta.startswith("\\gx"):
            unit = sqlsplit.Unit("sql", unit.text.replace("\\gx", "\\g", 1), sql, "\\g", unit.line)
        verb = main_verb(sql)
        if QUERY_START.match(sql) and verb in ("select", "values", "table") and not DML_CTE.search(sql) \
                and not SELECT_INTO.match(sql) and not meta.startswith("\\gexec"):
            self.handle_query(unit)
            return
        if DML_START.match(sql) or (sql.lower().startswith("with") and (DML_CTE.search(sql) or verb in ("insert", "update", "delete", "merge"))):
            if re.match(r"^copy\b", sql, re.I) and not re.search(r"\bfrom\b", sql, re.I):
                self.run(unit.text)  # COPY ... TO
                return
            self.handle_dml(unit)
            return
        if TXN_START.match(sql):
            self.run(unit.text)
            return
        if TXN_END.match(sql):
            self.run(unit.text)
            if not self.txn_state()[0]:
                if self.catalog_dirty:
                    self.sync_catalog()
                for name in sorted(self.pending_cmp):
                    if name in self.tables:
                        self.compare(name, f"<COMMIT at line {unit.line}>", stage="commit")
                self.pending_cmp.clear()
            return
        self.pre_ddl(unit)
        self.run(unit.text)
        if self.txn_state()[0]:
            self.catalog_dirty = True
        else:
            self.sync_catalog()

    # ---- main ------------------------------------------------------------------------------
    def go(self):
        self.h.create_db(self.db)
        self.new_session()
        for pre in PREREQS.get(self.name, []):
            for unit in sqlsplit.split(open(self.h.sql_path(pre)).read()):
                if not (unit.kind == "meta" and (SKIP_META.match(unit.meta) or SKIP_SET_VAR.match(unit.meta))):
                    self.s.run(unit.text, timeout=self.h.args.stmt_timeout, on_timeout=self.srv.cancel)
                    if not self.s.alive():
                        self.new_session()
            self.s.run("ROLLBACK;\nRESET ALL;")
        # Files switch roles (SET SESSION AUTHORIZATION); mirrors must stay readable for them.
        self.s.run(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_MIRROR};\n"
                   f"GRANT USAGE ON SCHEMA {SCHEMA_MIRROR} TO PUBLIC;\n"
                   f"ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA_MIRROR} GRANT SELECT ON TABLES TO PUBLIC;")
        text = open(self.h.sql_path(self.name)).read()
        for name, info in read_tables(self.s).items():
            if info["partition"]:
                continue
            self.tables[name] = dict(info, mirrored=False, bad_cols=set(), lossy_all=False)
            if ident_re(name).search(text):  # mirror only the fixtures this file names
                self.mirror(name, "fixture")
        t0 = time.monotonic()
        try:
            for unit in sqlsplit.split(text):
                if time.monotonic() - t0 > self.h.args.file_timeout:
                    self.rep.result(f"infra.{self.name}.file_timeout", "FAIL",
                                    f"stopped at line {unit.line} after {self.h.args.file_timeout:.0f}s")
                    break
                self.unit_line = unit.line
                self.execute(unit)
                self.remirror_pending()
            self.unit_line = 0
            self.run("ROLLBACK;")
            if self.catalog_dirty:
                self.sync_catalog()
            self.final_snapshot()
        finally:
            self.report()
            for sess in (self.s, self.c):
                if sess:
                    sess.kill()

    def final_snapshot(self):
        """Final convergence of every mirror. Iceberg persistence (create_snapshot) is exercised by
        the data layer and extra_cdc.sql, right after a write: create_snapshot waits for the
        session's XactLastCommitEnd, which a later SELECT doing HOT pruning moves to xid-less WAL
        that never commits, so it waits forever; and a fresh session's LSN 0 does not wait at all."""
        for name, t in sorted(self.tables.items()):
            if t.get("mirrored"):
                self.compare(name, "<end of file>", stage="final")

    def cdc_detail(self, name):
        ev = self.events.get(name, [])
        types = sorted({ty for e in ev for ty in e.get("types", [])})
        first = ev[0]
        return (f"types=[{','.join(types)}] events={len(ev)} first={first['stage']}@L{first['line']} "
                f"cols={first.get('cols')} {first.get('sample') or first.get('error', '')} :: {first['stmt'][:160]}")

    def report(self):
        for name in sorted(self.ever):
            check = f"cdc.{self.name}.{name}"
            if self.events.get(name):
                self.rep.result(check, "FAIL", self.cdc_detail(name), record={"events": self.events[name]})
            elif self.ever[name]:
                self.rep.result(check, "SKIP", f"no mirror: {self.ever[name]}")
            else:
                self.rep.result(check, "PASS")
        for q in self.queries:
            check = f"query.{self.name}.L{q['line']}"
            st = q["status"]
            sql = one_line(q["sql"], 160)
            if st == "PASS":
                self.rep.result(check, "PASS", "" if q.get("how") == "exact" else q["how"])
            elif st == "SKIP":
                self.rep.result(check, "SKIP", f"{q['reason']}: {sql}", record=q)
            elif st == "ENGINE":
                self.rep.stat("query.engine")
                self.rep.result(check, "FAIL", f"engine: {q.get('sample') or q.get('heap_error')} :: {sql}",
                                record=q, ids=self.h.exp.engine or ["duckdb-semantics"])
            elif q.get("masked_by"):
                names = q["masked_by"]
                ids = sorted({i for n in names
                              for i in (self.h.exp.match(f"cdc.{self.name}.{n}", self.cdc_detail(n))[1] or [])
                              if self.events.get(n)})
                if ids:
                    self.rep.result(check, "FAIL", f"reads lossy mirror {names}: {sql}", record=q, ids=ids)
                else:
                    self.rep.result(check, "SKIP", f"reads lossy mirror {names} (see cdc.{self.name}.*): {sql}",
                                    record=q)
            else:
                self.rep.result(check, "FAIL", f"{q['reason']}: {q.get('sample', '')} :: {sql}", record=q)


# ------------------------------------------------------------------------------------------------
class ProbeRun:
    """Data layer for one file: replay it without mirrors, check initial-copy fidelity of each
    table, probe the file's own types, and harvest built-in type values into the shared pool."""

    def __init__(self, h, name):
        self.h = h
        self.srv = h.srv
        self.rep = h.rep
        self.name = name
        self.db = None
        self.s = None
        self.attempt = 0

    def fresh_db(self, replay=True):
        """New database; replay the file. Regress files drop their tables as they go, so the
        tables are collected (copy check + type values) before every destructive statement."""
        self.attempt += 1
        self.db = self.h.dbname(self.name, suffix=f"d{self.attempt}")
        self.h.create_db(self.db)
        if self.s:
            self.s.kill()
        self.s = self.h.session(self.db)
        self.s.run("CREATE SCHEMA IF NOT EXISTS md;")
        if not replay:
            return
        if self.name != FIXTURES:
            for pre in PREREQS.get(self.name, []) + [self.name]:
                for unit in sqlsplit.split(open(self.h.sql_path(pre)).read()):
                    if unit.kind == "meta" and (SKIP_META.match(unit.meta) or SKIP_SET_VAR.match(unit.meta)):
                        continue
                    if pre == self.name and DESTRUCTIVE.match(unit.sql) and not self.in_txn():
                        self.collect()
                    self.s.run(unit.text, timeout=self.h.args.stmt_timeout, on_timeout=self.srv.cancel)
                    if not self.s.alive():
                        self.s = self.h.session(self.db)
                self.s.run("ROLLBACK;\nRESET ALL;")
        self.collect()

    def in_txn(self):
        r = self.s.run("SELECT now() <> statement_timestamp();")
        return not r.ok or not r.rows or r.rows[-1][0] == "t"

    def q(self, sql, timeout=None):
        r = self.s.run(sql, timeout=timeout or self.h.args.stmt_timeout, on_timeout=self.srv.cancel)
        if r.died or (not r.ok and ("mooncake" in sql.lower() or " md." in sql)):
            # A failed mooncake RPC poisons the session's RPC client: reconnect.
            self.s.kill()
            self.s = self.h.session(self.db)
        return r

    def recover(self):
        if not self.srv.recover(self.db):
            raise RuntimeError("moonlink service down and did not recover")
        if not self.s or not self.s.alive():
            self.s = self.h.session(self.db)

    def go(self):
        self.fresh_db()

    def collect(self):
        tables = {n: i for n, i in read_tables(self.s).items() if not i["partition"]}
        for rel in sorted(tables):
            self.copy_check(rel, tables[rel]["cols"])
        by_type = {}
        for rel, info in tables.items():
            for col in info["cols"]:
                by_type.setdefault(col, []).append((rel, col[0]))
        types = {}
        for col, uses in by_type.items():
            _, typ, typtype, builtin, _ = col
            types.setdefault((typ, typtype, builtin), []).extend(uses)
        for (typ, typtype, builtin), uses in sorted(types.items()):
            union = " UNION ALL ".join(f"SELECT {qident(att)}::{typ} AS v FROM public.{qident(rel)}"
                                       for rel, att in uses)
            if builtin:
                self.harvest(typ, union)
            else:
                self.h.type_probe(self, f"data.type.{self.name}.{type_key(typ)}", typ,
                                  f"SELECT v FROM ({union}) s", origin=self.name)

    def harvest(self, typ, union):
        r = self.q(f"SELECT DISTINCT v::text FROM ({union}) s LIMIT {self.h.args.pool_limit};")
        if r.ok:
            pool = self.h.pool.setdefault(typ, {"values": set(), "files": set()})
            pool["values"].update(row[0] for row in r.rows if row)
            pool["files"].add(self.name)

    def copy_check(self, rel, cols):
        if not cols:
            return
        sel = select_list(cols)
        h = self.q(f"SELECT md5(string_agg(t::text, E'\\n' ORDER BY t::text)), count(*) FROM (SELECT {sel} FROM public.{qident(rel)}) t;")
        key = ("table", rel, h.rows[0][0] if h.ok and h.rows else self.name)
        if key in self.h.seen or ("file-table", self.name, rel) in self.h.seen:
            return
        self.h.seen.add(key)
        self.h.seen.add(("file-table", self.name, rel))
        verdict, detail = self.h.mirror_and_compare(self, f"public.{qident(rel)}", rel, f"md.{rel}", sel)
        check = f"data.copy.{self.name}.{rel}"
        types = sorted({c[1] for c in cols})
        rec = {"verdict": verdict, "types": types}
        if verdict == "rejected":
            self.rep.result(check, "SKIP", f"rejected: {detail}", record=rec)
        else:
            self.rep.result(check, "PASS" if verdict == "exact" else "FAIL",
                            "" if verdict == "exact" else f"{verdict}: types=[{','.join(types)}] {detail}", record=rec)
        self.q(f"DROP TABLE IF EXISTS md.{qident(rel)};", timeout=self.h.args.ddl_timeout)


class PoolRun(ProbeRun):
    """Probes every pooled built-in type in one database."""

    def go(self):
        self.fresh_db(replay=False)
        for k, typ in enumerate(sorted(self.h.pool)):
            last = self.h.last_verdict
            if last in ("crashed", "errored") and k:
                # A panic can leave a zombie table that breaks later probes or recovery.
                self.h.end("data", f"pooled_types_until_{type_key(typ)}")
                self.h.begin(f"data-pooled-types-from-{type_key(typ)}")
                self.fresh_db(replay=False)
            vals = sorted(v for v in self.h.pool[typ]["values"] if v != NULL_TOKEN)
            arr = "ARRAY[" + ", ".join(qlit(v) for v in vals) + "]::text[]" if vals else "ARRAY[]::text[]"
            # Values were printed in the source file's session (its TimeZone, DateStyle, ...); keep the
            # ones this session can read back, so one unparsable value cannot void the whole probe.
            valid = f"WHERE pg_input_is_valid(t, {qlit(typ)})" if self.h.server_num >= 160000 else ""
            values_sql = (f"SELECT t::{typ} AS v FROM unnest({arr}) t {valid} "
                          f"UNION ALL SELECT NULL::{typ}")
            self.h.type_probe(self, f"data.type.{type_key(typ)}", typ, values_sql,
                              origin=",".join(sorted(self.h.pool[typ]["files"])))


# ------------------------------------------------------------------------------------------------
class Harness:
    def __init__(self, args):
        self.args = args
        self.srv = Server(args)
        self.exp = Expectations(args.expected, args.bug_ids)
        os.makedirs(args.workdir, exist_ok=True)
        os.makedirs(os.path.join(args.workdir, "results"), exist_ok=True)
        self.rep = Reporter(self.exp, os.path.join(args.workdir, "results.jsonl"), resume=args.resume)
        self.done_path = os.path.join(args.workdir, "done.txt")
        self.pool_path = os.path.join(args.workdir, "pool.json")
        self.done = set()
        if args.resume and os.path.exists(self.done_path):
            self.done = {line.strip() for line in open(self.done_path) if line.strip()}
        self.seen = set()
        self.pool = {}
        self.type_verdicts = {}
        self.diverged_cols = {}
        self.crash_types = {}  # column type -> where it crashed moonlink at create_table
        self.good_types = set()
        self.panics_seen = 0
        self.probe_seq = 0
        self.last_verdict = None
        self.server_num = 0
        regress = os.path.join(args.pg_src, "src", "test", "regress")
        self.regress = regress if os.path.isdir(regress) else args.pg_src
        if not os.path.isfile(os.path.join(self.regress, "sql", FIXTURES + ".sql")):
            sys.exit(f"no {FIXTURES}.sql under {self.regress}/sql: --pg-src must be a PostgreSQL source tree")
        self.env = dict(os.environ, PG_ABS_SRCDIR=self.regress, PG_ABS_BUILDDIR=args.workdir,
                        PG_LIBDIR=self.regress, PG_DLSUFFIX=".so", PGTZ="PST8PDT",
                        PGDATESTYLE="Postgres, MDY", LC_MESSAGES="C", PGOPTIONS="")
        self.dbs = []
        self.session_seq = 0
        self.trace = open(os.path.join(args.workdir, "trace.sql"), "a" if args.resume else "w") if args.trace else None
        if args.resume and os.path.exists(self.pool_path):
            for typ, v in json.load(open(self.pool_path)).items():
                if typ != "_done":
                    self.pool[typ] = {"values": set(v["values"]), "files": set(v["files"])}
            if "data pooled-types" in self.done:
                self.pool["_done"] = True

    def sql_path(self, name):
        return LOCAL_FILES.get(name) or os.path.join(self.regress, "sql", name + ".sql")

    def dbname(self, name, suffix=""):
        return ("md_" + re.sub(r"[^a-z0-9_]", "_", name.lower()) + ("_" + suffix if suffix else ""))[:50]

    def session(self, db):
        """New psql session; waits out server crash recovery (a backend crash restarts all)."""
        for attempt in range(30):
            self.session_seq += 1
            try:
                return PsqlSession(self.srv.psql, self.srv.conninfo(db), timeout=self.args.stmt_timeout,
                                   env=self.env, trace=self.trace, name=f"{db}#{self.session_seq}").start()
            except RuntimeError as e:
                if attempt == 29 or not re.search(r"recovery mode|starting up|shutting down|Connection refused|No such file", str(e)):
                    raise
                time.sleep(2)

    def create_db(self, db):
        self.drop_db(db)
        rc, _, err = self.srv.admin("postgres", f"CREATE DATABASE {db} TEMPLATE md_template")
        if rc != 0:
            raise RuntimeError(f"create database {db}: {err}")
        rc, _, err = self.srv.admin(db, "CREATE EXTENSION IF NOT EXISTS pg_mooncake CASCADE")
        if rc != 0:
            raise RuntimeError(f"create extension in {db}: {err}")
        self.dbs.append(db)

    def drop_db(self, db):
        rc, out, _ = self.srv.admin(db, "SELECT string_agg(format('%I.%I', n.nspname, c.relname), ' ') "
                                        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                                        "JOIN pg_am am ON am.oid = c.relam WHERE am.amname = 'mooncake'")
        if rc == 0 and out:
            for t in out.split():
                self.srv.admin(db, f"DROP TABLE IF EXISTS {t}", timeout=60)
        rc, _, err = self.srv.admin("postgres", f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
        if rc != 0:
            # A leftover replication slot (mirror drop failed) blocks DROP DATABASE.
            self.srv.admin("postgres", "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                                       f"WHERE database = {qlit(db)} AND active_pid IS NOT NULL")
            time.sleep(1)
            self.srv.admin("postgres", "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                                       f"WHERE database = {qlit(db)}")
            rc, _, err = self.srv.admin("postgres", f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
            self.rep.stat("infra.slot_leaked")
        if db in self.dbs:
            self.dbs.remove(db)

    def template(self):
        self.srv.admin("postgres", "DROP DATABASE IF EXISTS md_template WITH (FORCE)")
        rc, _, err = self.srv.admin("postgres", "CREATE DATABASE md_template")
        if rc != 0:
            raise RuntimeError(err)
        s = self.session("md_template")
        failed = []
        for unit in sqlsplit.split(open(self.sql_path(FIXTURES)).read()):
            r = s.run(unit.text, timeout=300)
            if not r.ok:
                failed.append(f"L{unit.line}: {r.message[:80]}")
        s.close()
        print(f"INFO fixtures loaded into md_template; {len(failed)} statement(s) failed: {'; '.join(failed)[:400]}",
              flush=True)

    # ---- shared probe logic ------------------------------------------------------------------
    def mirror_and_compare(self, run, heap_rel, src_name, dst, sel):
        """Mirror public.<src_name> into <dst> by initial copy; return (verdict, detail)."""
        before = self.srv.panics()
        r = run.q(f"CALL mooncake.create_table({qlit(dst)}, {qlit('public.' + src_name)});", timeout=self.args.ddl_timeout)
        if self.srv.panics() > before:
            p = self.srv.panic_lines()
            self.panics_seen = len(p)
            run.recover()
            return "crashed", f"create_table panicked moonlink: {re.sub(r'^.*panicked at ', '', p[-1])[:200]}"
        if not r.ok:
            if r.outcome() != "error":
                run.recover()
                return "crashed", f"create_table {r.outcome()}"
            return "rejected", one_line(r.message, 200)
        return self.converge(run, f"SELECT {sel} FROM {heap_rel}", f"SELECT {sel} FROM {dst}")

    def converge(self, run, heap_sql, mirror_sql):
        heap = run.q(heap_sql + ";")
        if not heap.ok:
            return "errored", f"heap query failed: {heap.message[:200]}"
        deadline = time.monotonic() + self.args.converge_secs
        before = self.srv.panics()
        while True:
            mir = run.q(mirror_sql + ";")
            eq, how = same(heap, mir)
            if eq and how in ("exact", "order"):
                return "exact", ""
            if self.srv.panics() > before or mir.died:
                self.panics_seen = self.srv.panics()
                run.recover()
                return "crashed", f"mirror scan {mir.outcome()}: {mir.message[:200]}"
            if mir.timed_out or time.monotonic() >= deadline:
                break
            time.sleep(0.5)
        if not mir.ok:
            return "errored", f"mirror scan {mir.outcome()}: {one_line(mir.message, 200)}"
        if json_equivalent(heap, mir):
            return "equivalent", diff_sample(heap, mir)
        return "lossy", diff_sample(heap, mir)

    def type_probe(self, run, check, typ, values_sql, origin):
        """One column type in isolation: initial copy, CDC insert/update/delete, snapshot."""
        self.probe_seq += 1
        k = self.probe_seq
        src, cdc = f"md_ps{k}", f"md_pc{k}"
        r = run.q(f"CREATE TABLE public.{src} AS SELECT row_number() OVER () AS id, v FROM ({values_sql}) s;")
        if r.ok:
            r = run.q(f"ALTER TABLE public.{src} ADD PRIMARY KEY (id);")
        if not r.ok:
            self.rep.result(check, "SKIP", f"could not build probe table: {one_line(r.message, 200)}")
            return
        n = run.q(f"SELECT count(*) FROM public.{src};").rows
        n = n[0][0] if n else "?"
        steps = {}
        steps["copy"] = self.mirror_and_compare(run, f"public.{src}", src, f"md.{src}", "id, v")
        if steps["copy"][0] not in ("rejected", "crashed"):
            run.q(f"CREATE TABLE public.{cdc} (id bigint PRIMARY KEY, v {typ});")
            r = run.q(f"CALL mooncake.create_table({qlit('md.' + cdc)}, {qlit('public.' + cdc)});",
                      timeout=self.args.ddl_timeout)
            if r.ok:
                # create_snapshot runs right after the write, in the writing session: it waits for
                # that commit (a SELECT in between could move the session's LSN to WAL that never commits).
                for step, sql, snap in (
                        ("cdc_insert", f"INSERT INTO public.{cdc} SELECT id, v FROM public.{src};", True),
                        ("cdc_update", f"UPDATE public.{cdc} SET id = -id;", False),
                        ("cdc_delete", f"DELETE FROM public.{cdc} WHERE id % 2 = 0 AND id <> 0;", n not in ("0", "1"))):
                    w = run.q(sql)
                    if not w.ok:
                        steps[step] = ("errored", f"source DML failed: {one_line(w.message, 160)}")
                        break
                    if snap:
                        sr = run.q(f"CALL mooncake.create_snapshot({qlit('md.' + cdc)});", timeout=self.args.ddl_timeout)
                        if not sr.ok:
                            steps[step] = ("errored", f"create_snapshot after {step}: {sr.outcome()} {one_line(sr.message, 160)}")
                            break
                    steps[step] = self.converge(run, f"SELECT id, v FROM public.{cdc}", f"SELECT id, v FROM md.{cdc}")
                    if steps[step][0] != "exact":
                        break
            else:
                steps["cdc_insert"] = ("rejected" if r.outcome() == "error" else "crashed", one_line(r.message, 160))
        for t in (cdc, src):
            run.q(f"DROP TABLE IF EXISTS md.{t};", timeout=self.args.ddl_timeout)
            run.q(f"DROP TABLE IF EXISTS public.{t};")
        seen = [v for v, _ in steps.values()]
        overall = next(o for o in VERDICT_ORDER if o in seen or o == "exact")
        if overall == "rejected" and len(set(seen)) > 1:
            overall = next(o for o in VERDICT_ORDER if o in seen and o != "rejected")
        detail = "; ".join(f"{s}={v}" + (f" ({d})" if d and v != "exact" else "") for s, (v, d) in steps.items())
        self.type_verdicts.setdefault(type_label(typ), []).append((typ, overall, check))
        self.last_verdict = overall
        status = {"exact": "PASS", "rejected": "SKIP"}.get(overall, "FAIL")
        self.rep.result(check, status, f"{overall}: {n} values ({origin}); {detail}",
                        record={"type": typ, "verdict": overall, "steps": steps, "origin": origin})

    # ---- main ------------------------------------------------------------------------------
    def selection(self):
        layers = set(self.args.layers.split(","))
        sel = []
        if "data" in layers:
            sel += [("data", f) for f in [FIXTURES] + DATA_FILES]
        if layers & {"query", "cdc"}:
            sel += [("run", f) for f in DATA_FILES + QUERY_FILES + CDC_FILES + list(LOCAL_FILES)]
        if self.args.files:
            want = self.args.files.split(",")
            sel = [(k, f) for k, f in sel if f in want]
        return [(k, f) for k, f in sel if os.path.exists(self.sql_path(f))]

    def build_info(self):
        def git(*a):
            try:
                return subprocess.run(["git", "-C", HERE, *a], capture_output=True, text=True, timeout=10).stdout.strip()
            except (OSError, subprocess.TimeoutExpired):
                return ""
        print(f"BUILD harness_commit={git('rev-parse', '--short=12', 'HEAD') or 'unknown'}", flush=True)
        for line in git("submodule", "status").splitlines():
            parts = line.strip().lstrip("+-U").split()
            if len(parts) >= 2:
                print(f"BUILD submodule_{parts[1]}={parts[0][:12]}", flush=True)
        rc, out, _ = self.srv.admin("postgres", "SELECT current_setting('server_version')")
        print(f"BUILD server_version={out}", flush=True)
        rc, num, _ = self.srv.admin("postgres", "SELECT current_setting('server_version_num')")
        self.server_num = int(num) if rc == 0 and num.isdigit() else 0
        rc, pkglib, _ = self.srv.admin("postgres", "SELECT setting FROM pg_config WHERE name = 'PKGLIBDIR'")
        for lib in ("pg_mooncake.so", "pg_duckdb.so"):
            p = os.path.join(pkglib, lib)
            if os.path.exists(p):
                print(f"BUILD {lib}_sha256={hashlib.sha256(open(p, 'rb').read()).hexdigest()[:16]}", flush=True)
        print(f"BUILD pg_src={self.regress}", flush=True)
        if self.args.build_info and os.path.exists(self.args.build_info):
            for line in open(self.args.build_info):
                if line.strip():
                    print(f"BUILD {line.strip()}", flush=True)

    def begin(self, label):
        self.srv.setup(label)
        self.dbs = []
        self.template()
        self.panics_seen = self.srv.panics()

    def end(self, kind, name):
        """moonlink must still answer after the file (no crash loop), then tear down."""
        db = self.dbs[-1] if self.dbs else None
        if db is not None:
            why = []
            ok = self.srv.healthy(db) or (time.sleep(20) or self.srv.healthy(db, why))
            self.rep.result(f"service.{kind}.{name}", "PASS" if ok else "FAIL",
                            "" if ok else f"mooncake.list_tables() fails after the file: {one_line(why[-1], 240)}")
        if self.srv.managed:
            if not self.args.keep:
                self.srv.stop()
        else:
            for db in list(self.dbs):
                self.drop_db(db)

    def checkpoint(self, key):
        """Record a finished unit of work so --resume skips it (results are already in results.jsonl)."""
        self.done.add(key)
        with open(self.done_path, "a") as f:
            f.write(key + "\n")
        pool = {t: ({"values": sorted(v["values"]), "files": sorted(v["files"])} if t != "_done" else v)
                for t, v in self.pool.items()}
        with open(self.pool_path + ".tmp", "w") as f:
            json.dump(pool, f)
        os.replace(self.pool_path + ".tmp", self.pool_path)

    def main(self):
        sel = self.selection()
        self.begin("build-info")
        self.build_info()
        self.srv.stop()
        try:
            for kind, f in sel:
                if kind == "run" and "data" in self.args.layers.split(",") and self.pool and not self.pool.get("_done"):
                    self.run_pool()
                if f"{kind} {f}" in self.done:
                    print(f"INFO skip {kind} {f} (done in an earlier run, --resume)", flush=True)
                    continue
                print(f"INFO begin {kind} {f}", flush=True)
                t0 = time.monotonic()
                self.begin(f"{kind}-{f}")
                try:
                    (ProbeRun if kind == "data" else FileRun)(self, f).go()
                except Exception as e:  # one broken file must not end the run
                    import traceback
                    self.rep.result(f"infra.{kind}.{f}", "FAIL", f"{type(e).__name__}: {e}",
                                    record={"traceback": traceback.format_exc()})
                self.end(kind, f)
                self.checkpoint(f"{kind} {f}")
                print(f"INFO end {kind} {f} {time.monotonic() - t0:.0f}s", flush=True)
            if self.pool and not self.pool.get("_done"):
                self.run_pool()
            self.type_summary()
        finally:
            if self.srv.managed and not self.args.keep:
                self.srv.stop()
        layers = self.args.layers.replace(",", "+")
        return 0 if self.rep.summary(self.major(), layers) else 1

    def major(self):
        try:
            out = subprocess.run([os.path.join(self.srv.bindir, "pg_config"), "--version"],
                                 capture_output=True, text=True).stdout
            return re.search(r"(\d+)", out).group(1)
        except (OSError, AttributeError):
            return "?"

    def run_pool(self):
        if "data pooled-types" in self.done:
            self.pool["_done"] = True
            return
        print("INFO begin data pooled-types", flush=True)
        t0 = time.monotonic()
        self.pool = {k: v for k, v in self.pool.items() if k != "_done"}
        self.begin("data-pooled-types")
        try:
            PoolRun(self, "pool").go()
        except Exception as e:
            self.rep.result("infra.data.pool", "FAIL", f"{type(e).__name__}: {e}")
        self.end("data", "pooled_types")
        self.pool["_done"] = True
        self.checkpoint("data pooled-types")
        print(f"INFO end data pooled-types {time.monotonic() - t0:.0f}s", flush=True)

    def type_summary(self):
        verdicts = {}  # from results.jsonl, so verdicts of files done before a --resume count
        with open(os.path.join(self.args.workdir, "results.jsonl")) as f:
            for line in f:
                rec = json.loads(line)
                if rec["check"].startswith("data.type.") and rec.get("verdict"):
                    verdicts.setdefault(type_label(rec["type"]), []).append((rec["type"], rec["verdict"], rec["check"]))
        for label in sorted(verdicts):
            vs = verdicts[label]
            worst = min(vs, key=lambda v: VERDICT_ORDER.index(v[1]))
            print(f"TYPE {type_key(label)} {worst[1]} probes={len(vs)} worst={worst[2]}", flush=True)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pg-src", required=True, help="PostgreSQL source tree (or its src/test/regress)")
    p.add_argument("--bindir", help="bin directory of the Postgres install with pg_mooncake "
                                    "(default: pg_config --bindir)")
    p.add_argument("--workdir", default=os.path.join(os.getcwd(), "mirror_diff_work"))
    p.add_argument("--host", help="use a running server (socket dir or host) instead of a throwaway cluster")
    p.add_argument("--port", type=int, default=5491)
    p.add_argument("--user")
    p.add_argument("--duckdb-extension-dir", default=os.environ.get("DUCKDB_EXTENSION_DIR"),
                   help="duckdb.extension_directory for the throwaway cluster")
    p.add_argument("--conf", action="append", help="extra postgresql.conf line (repeatable)")
    p.add_argument("--layers", default="data,cdc,query")
    p.add_argument("--files", help="comma-separated subset of file names (regress or extra_*)")
    p.add_argument("--expected", default=os.path.join(HERE, "expected.txt"), help="known-failure list")
    p.add_argument("--bug-ids", default=os.environ.get("MIRROR_DIFF_BUG_IDS"),
                   help='optional file of "<bug> <tracker ids>" lines, appended to KNOWN lines')
    p.add_argument("--build-info", help="file of k=v lines to echo as BUILD lines")
    p.add_argument("--stmt-timeout", type=float, default=120)
    p.add_argument("--ddl-timeout", type=float, default=60)
    p.add_argument("--file-timeout", type=float, default=2400)
    p.add_argument("--converge-secs", type=float, default=10)
    p.add_argument("--pool-limit", type=int, default=5000, help="max distinct values per type per file")
    p.add_argument("--trace", action="store_true", help="log every statement sent to <workdir>/trace.sql")
    p.add_argument("--resume", action="store_true",
                   help="continue a run in the same --workdir: skip files listed in done.txt")
    p.add_argument("--keep", action="store_true", help="keep databases and leave the server running")
    a = p.parse_args(argv)
    if not a.bindir:
        try:
            a.bindir = subprocess.run(["pg_config", "--bindir"], capture_output=True, text=True, check=True).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            p.error("--bindir is required when pg_config is not on PATH")
    a.workdir = os.path.abspath(a.workdir)
    return a


def rescore(results_path, expected_path, major="?"):
    """Re-apply the known-failure list to a finished run's results.jsonl (no database needed).
    The raw outcome of each check is its status with XFAIL/XPASS undone; engine-semantics XFAILs
    (from the classifier, not from the list) are kept."""
    exp = Expectations(expected_path, os.environ.get("MIRROR_DIFF_BUG_IDS"))
    out_path = results_path.replace(".jsonl", ".rescored.jsonl")
    rep = Reporter(exp, out_path)
    with open(results_path) as f:
        recs = [json.loads(line) for line in f if line.strip()]
    for rec in recs:
        status, detail = rec["status"], rec.get("detail", "")
        ids = None
        if status == "XFAIL" and detail.startswith("engine:"):
            ids = exp.engine or rec.get("ids") or ["duckdb-semantics"]
            status = "FAIL"
        elif status == "XFAIL":
            status = "FAIL"
        elif status == "XPASS":
            status = "PASS"
        extra = {k: v for k, v in rec.items() if k not in ("check", "status", "ids", "detail")}
        rep.names[rec["check"]] = 0  # keep the original check name (with any .vN suffix)
        rep.result(rec["check"], status, detail, record=extra, ids=ids)
    return 0 if rep.summary(major, "rescore") else 1


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--rescore":
        # mirror_diff.py --rescore <results.jsonl> [expected.txt]
        sys.exit(rescore(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else os.path.join(HERE, "expected.txt")))
    sys.exit(Harness(parse_args(sys.argv[1:])).main())
