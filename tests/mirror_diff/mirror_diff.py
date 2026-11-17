#!/usr/bin/env python3
"""Differential mirror testing for pg_mooncake, driven by the PostgreSQL regression suite.

The harness reads the regression files from a PostgreSQL source tree at run time (nothing is
copied into this repository) and checks three things:

  data   Fidelity of every column type. Each data-type file is replayed without mirrors; every
         table is mirrored (initial copy) and compared row by row with the heap each time the file
         is about to drop or alter it (data.copy.<file>.<table>, .sN for later states). All values
         of each built-in column type are pooled across files and probed in isolation: initial
         copy, CDC insert, CDC update that leaves the value unchanged, CDC delete, and after
         create_snapshot (data.type.<type>). Types defined by a file (enum, domain, composite,
         range) are probed in that file's database (data.type.<file>.<type>). Verdicts: exact,
         rejected-values (values create_table refused by name are dropped; the rest is exact),
         rejected (create_table refused the table), equivalent (only JSON whitespace or key
         order differs), stale (a fenced read missed a write), lossy (silent divergence),
         errored, crashed. Only exact passes.

  cdc    Each regress file is run statement by statement against heap tables while every table
         in schema public is mirrored into schema "mc" (fixtures up front, the file's own tables
         right after CREATE TABLE). After each DML statement or COMMIT, the mirrors of the tables
         it names must equal the heap. The mirror is read right after a fence commit in the same
         session, so read-your-writes must make it current: a mirror that only catches up later
         is a stale read (cdc.<file>.<table>, one event per divergence).

  query  Every SELECT of the file that names a mirrored table is run a second time with
         search_path = mc, ... so it reads the mirrors (DuckDB executes it), and compared with
         the heap result as typed and ordered by the statement (query.<file>.L<line>). A
         difference is classified by a third run on the heap tables with duckdb.force_execution:
         if DuckDB-on-heap gives the heap's answer the mirror is wrong (FAIL); if it gives the
         mirror's answer it is DuckDB semantics (the engine bug of expected.txt). Queries DuckDB
         cannot plan are SKIPs.

Output (shared with scripts/e2e-smoke.sh): one line per check,
  RESULT <check> PASS|FAIL|XFAIL(<bugs>)|XFAIL(limitation:<bugs>)|XPASS(<bugs>)|SKIP [detail]
then BUILD / TYPE / STAT / KNOWN information lines, and
  SUMMARY mirror_diff pg<major> layers=<..> pass=N xpass=N fail=N xfail=N xfail_limitation=N skip=N unverified=N
  BLOCKING mirror_diff <n> [<bug>,...]
  GATE mirror_diff PASS|FAIL|BLOCKED|INCOMPLETE
Exit status: the GATE (0 PASS, 1 FAIL, 4 BLOCKED, 3 INCOMPLETE), or with --exit-regression 0 iff
no check failed or went unverified in a way expected.txt does not list. See README.md.
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

import compare  # noqa: E402
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
    r"^\\!|^\\(c|connect|o|out|pset|a|t|x|f|H|html|C|T|!|i|ir|include|include_relative|lo_\w+|"
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
SCHEMA_MIRROR = "mc"
CLUSTER_MARKER = "mirror_diff.cluster"  # written into data directories this harness created
VERDICT_ORDER = ["crashed", "lossy", "stale", "errored", "equivalent", "rejected", "rejected-values", "exact"]


def ident_re(name):
    return re.compile(r"(?<![\w.\"$])" + re.escape(name) + r"(?![\w\"$])", re.I)


def strip_literals(sql):
    """SQL with string literals blanked, so 'tbl'::regclass in catalog queries is not a table reference."""
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


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
    return re.sub(r"[^A-Za-z0-9]+", "_", typ.replace("[]", "_array").replace("-", "m")).strip("_")


def type_label(typ):
    return re.sub(r"\(\d+(,\d+)?\)", lambda m: "(p,s)" if "," in m.group(0) else "(n)", typ)


# ------------------------------------------------------------------------------------------------
class Expectations:
    """Known differences (expected.txt), same model as scripts/e2e-known-bugs.txt:

      bug <key> class=blocking|class=limitation <description>
      check <check-glob> <key>[,<key>...] [<regex>]
      engine <key>

    A failing check is matched against the check lines whose glob (fnmatch on the check name,
    also tried without a .vN/.sN suffix) matches; the optional Python regex must be found in the
    failure signature. Checks that carry events (cdc tables) are XFAIL only if every event
    matches some line; they report the union of the matched keys. A passing check matched by a
    line without a regex, or by a line naming it exactly, reports XPASS. "engine" names the bug
    the classifier attaches to DuckDB-semantics differences of query checks."""

    def __init__(self, path, ids_path=None):
        self.rules = []
        self.bugs = {}       # key -> (class, description)
        self.engine = []
        self.ids = {}        # key -> tracker ids, from a private file outside the repository
        if path and os.path.exists(path):
            with open(path) as f:
                lines = f.readlines()
            for n, raw in enumerate(lines, 1):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(None, 3)
                kind = parts[0]
                if kind == "bug" and len(parts) >= 4 and parts[2] in ("class=blocking", "class=limitation"):
                    if not re.fullmatch(r"[A-Za-z0-9_.-]+", parts[1]):
                        sys.exit(f"{path}:{n}: bug name {parts[1]!r} must match [A-Za-z0-9_.-]+")
                    if parts[1] in self.bugs:
                        sys.exit(f"{path}:{n}: bug {parts[1]} is declared twice")
                    self.bugs[parts[1]] = (parts[2].split("=", 1)[1], parts[3])
                elif kind == "check" and len(parts) >= 3:
                    try:
                        rx = re.compile(parts[3]) if len(parts) > 3 else None
                    except re.error as e:
                        sys.exit(f"{path}:{n}: invalid regex: {e}")
                    self.rules.append((parts[1], parts[2].split(","), rx, n))
                elif kind == "engine" and len(parts) == 2:
                    self.engine = [parts[1]]
                else:
                    sys.exit(f"{path}:{n}: cannot parse: {line}")
            for _, keys, _, n in self.rules:
                for k in keys:
                    if k not in self.bugs:
                        sys.exit(f"{path}:{n}: check refers to undeclared bug {k}")
            for k in self.engine:
                if k not in self.bugs:
                    sys.exit(f"{path}: engine refers to undeclared bug {k}")
        if ids_path and os.path.exists(ids_path):
            with open(ids_path) as f:
                id_lines = f.readlines()
            for raw in id_lines:
                p = raw.split()
                if len(p) >= 2 and not raw.startswith("#"):
                    self.ids[p[0]] = p[1]

    @staticmethod
    def _names(check):
        base = re.sub(r"\.[vs]\d+$", "", check)
        return [check] if base == check else [check, base]

    def match(self, check, detail, passed=False):
        """Keys of the first rule matching a failure (or, with passed, a rule that would XPASS)."""
        for pat, keys, rx, _ in self.rules:
            if not any(fnmatch.fnmatchcase(c, pat) for c in self._names(check)):
                continue
            if passed:
                if rx is None or not re.search(r"[*?\[]", pat):
                    return keys
                continue
            if rx is not None and not rx.search(detail or ""):
                continue
            return keys
        return None

    def match_events(self, check, events):
        """Union of keys if every event's signature matches a rule for this check, else None."""
        if not events:
            return None
        keys = []
        for ev in events:
            k = self.match(check, ev["text"])
            if k is None:
                return None
            keys += [x for x in k if x not in keys]
        return keys

    def is_limitation(self, keys):
        return bool(keys) and all(self.bugs.get(k, ("blocking",))[0] == "limitation" for k in keys)


class Reporter:
    STATUSES = ("PASS", "XPASS", "FAIL", "XFAIL", "XFAIL_LIMITATION", "SKIP")

    def __init__(self, exp, log_path, resume=False, done=()):
        self.exp = exp
        self.counts = {k: 0 for k in self.STATUSES}
        self.unverified = 0
        self.unexpected = []   # checks that make --exit-regression fail
        self.stats = {}
        self.known = {}        # bug key -> {"XFAIL": n, "XPASS": n, "checks": [...]}
        self.blocking_xfails = 0
        self.names = {}
        self.unit = ""
        prior = []
        if resume and os.path.exists(log_path):
            with open(log_path) as f:
                prior = [json.loads(line) for line in f if line.strip()]
            # Records of a unit that was interrupted are dropped; the unit runs again.
            prior = [r for r in prior if r.get("unit") in done]
            with open(log_path + ".tmp", "w") as f:
                for rec in prior:
                    f.write(json.dumps(rec, default=str) + "\n")
            os.replace(log_path + ".tmp", log_path)
        self.log = open(log_path, "a" if resume else "w")
        for rec in prior:
            self.account(rec["check"], rec["status"], rec.get("ids") or [], rec.get("unverified"))
            base = re.sub(r"\.v\d+$", "", rec["check"])
            self.names[base] = self.names.get(base, 0) + 1

    def account(self, check, status, ids, unverified=False):
        self.counts[status] += 1
        if status == "SKIP" and unverified:
            self.unverified += 1
        if status == "FAIL" or (status == "SKIP" and unverified):
            self.unexpected.append(check)
        if status == "XFAIL":
            self.blocking_xfails += 1
        if status in ("XFAIL", "XFAIL_LIMITATION", "XPASS"):
            col = "XPASS" if status == "XPASS" else "XFAIL"
            for i in ids:
                kn = self.known.setdefault(i, {"XFAIL": 0, "XPASS": 0, "checks": []})
                kn[col] += 1
                if len(kn["checks"]) < 3:
                    kn["checks"].append(f"{check}={col}")

    def result(self, check, status, detail="", record=None, ids=None, events=None, unverified=False):
        """Report one check. FAIL is turned into XFAIL by expected.txt (ids: keys decided by the
        caller, e.g. the engine bug); unverified marks a SKIP that hides a check that could not run
        (it fails the regression rule unless expected.txt lists it)."""
        ids = list(ids or [])
        n = self.names.get(check, 0) + 1
        self.names[check] = n
        if n > 1:
            check = f"{check}.v{n}"
        if status == "FAIL":
            if not ids:
                ids = (self.exp.match_events(check, events) if events else self.exp.match(check, detail)) or []
            if ids:
                status = "XFAIL_LIMITATION" if self.exp.is_limitation(ids) else "XFAIL"
        elif status == "SKIP" and unverified:
            ids = self.exp.match(check, detail) or []
            if ids:
                status = "XFAIL_LIMITATION" if self.exp.is_limitation(ids) else "XFAIL"
                unverified = False
        elif status == "PASS":
            ids = self.exp.match(check, "", passed=True) or []
            if ids:
                status = "XPASS"
        if status == "XFAIL_LIMITATION":
            label = f"XFAIL(limitation:{','.join(ids)})"
        elif status in ("XFAIL", "XPASS"):
            label = f"{status}({','.join(ids)})"
        else:
            label = status
        self.account(check, status, ids, unverified)
        print(f"RESULT {check} {label}" + (f" {one_line(detail)}" if detail else ""), flush=True)
        rec = dict(record or {})
        rec.update(check=check, status=status, ids=ids, detail=detail, unit=self.unit)
        if events:
            rec["events"] = events
        if unverified:
            rec["unverified"] = True
        self.log.write(json.dumps(rec, default=str) + "\n")
        self.log.flush()

    def stat(self, key, n=1):
        self.stats[key] = self.stats.get(key, 0) + n

    def summary(self, major, layers, complete):
        """Print the trailer; return (gate exit code, regression exit code)."""
        for k in sorted(self.stats):
            print(f"STAT {k} {self.stats[k]}", flush=True)
        for key in sorted(set(self.exp.bugs) | set(self.known)):
            kn = self.known.get(key, {"XFAIL": 0, "XPASS": 0, "checks": []})
            state = "OPEN" if kn["XFAIL"] else ("FIXED" if kn["XPASS"] else "NOT-SEEN")
            cls = self.exp.bugs.get(key, ("blocking", ""))[0]
            ids = self.exp.ids.get(key)
            print(f"KNOWN {key} {state} {','.join(kn['checks']) or '-'} class={cls}" + (f" ids={ids}" if ids else ""),
                  flush=True)
        c = self.counts
        print(f"SUMMARY mirror_diff pg{major} layers={layers} pass={c['PASS']} xpass={c['XPASS']} "
              f"fail={c['FAIL']} xfail={c['XFAIL']} xfail_limitation={c['XFAIL_LIMITATION']} skip={c['SKIP']} "
              f"unverified={self.unverified}", flush=True)
        listed = sorted(k for k, (cls, _) in self.exp.bugs.items() if cls == "blocking")
        print(f"BLOCKING mirror_diff {len(listed)}" + (f" {','.join(listed)}" if listed else ""), flush=True)
        if c["FAIL"] or self.blocking_xfails or self.unverified or not c["PASS"]:
            gate, code = "FAIL", 1
        elif listed:
            gate, code = "BLOCKED", 4
        elif not complete:
            gate, code = "INCOMPLETE", 3
        else:
            gate, code = "PASS", 0
        print(f"GATE mirror_diff {gate}", flush=True)
        return code, (1 if self.unexpected else 0)


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
        self.logfile = os.path.join(self.args.workdir, "logs", f"{label}.log")
        os.makedirs(self.host, exist_ok=True)
        if os.path.exists(self.datadir):
            if not os.path.exists(os.path.join(self.datadir, CLUSTER_MARKER)):
                sys.exit(f"{self.datadir} exists but was not created by mirror_diff ({CLUSTER_MARKER} missing); "
                         "refusing to delete it: choose another --workdir")
            self.pg_ctl("stop", "-m", "immediate", check=False)
            shutil.rmtree(self.datadir)
        subprocess.run([os.path.join(self.bindir, "initdb"), "-D", self.datadir, "-U", self.user,
                        "--locale=C", "--encoding=UTF8"], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        open(os.path.join(self.datadir, CLUSTER_MARKER), "w").close()
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
        """Cancel the statement of backend pid; terminate the backend only if it is still running
        that statement after 10 s (a cancelled statement leaves the session usable)."""
        self.admin("postgres", f"SELECT pg_cancel_backend({pid})", timeout=10)
        for _ in range(20):
            time.sleep(0.5)
            rc, state, _ = self.admin("postgres", f"SELECT state FROM pg_stat_activity WHERE pid = {pid}", timeout=10)
            if rc == 0 and state != "active":
                return
        self.admin("postgres", f"SELECT pg_terminate_backend({pid})", timeout=10)

    def panic_lines(self):
        if not os.path.exists(self.logfile):
            return []
        with open(self.logfile, errors="replace") as f:
            return [line.strip() for line in f if "panicked at" in line]

    def panics(self):
        return len(self.panic_lines())

    def can_see_panics(self):
        return os.path.exists(self.logfile)

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
                if not trap and i + 1 < len(lines) and "Failed process was running:" in lines[i + 1]:
                    # Release builds have no TRAP line; name the statement instead.
                    trap = (re.sub(r"^.*?LOG:\s+", "", line.strip()) + "; "
                            + re.sub(r"^.*?DETAIL:\s+", "", lines[i + 1].strip()))
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
def panic_site(line):
    """Location of a Rust panic, independent of where the crates were built: moonlink sources as
    moonlink/src/<crate>/src/...rs:L:C, registry crates as <crate>-<version>/src/...rs:L:C."""
    m = re.search(r"panicked at (\S+?\.rs:\d+:\d+)", line)
    if not m:
        return one_line(re.sub(r"^.*panicked at ", "", line), 120)
    path = m.group(1)
    reg = re.search(r"/registry/src/[^/]+/(.+)$", path)
    if reg:
        return reg.group(1)
    ml = re.search(r"(?:^|/)(moonlink/src/.+)$", path)
    return ml.group(1) if ml else path


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
                    r[i] = compare.canonical_json(r[i])
                except ValueError:
                    pass
        out.append(r)
    return out


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
                    cells.append(compare.canonical_json(c))
                except (ValueError, TypeError):
                    cells.append(c)
            out.append(tuple(cells))
        return sorted(out)
    return a.ok and b.ok and norm(a.rows) == norm(b.rows)


TABLES_SQL = (
    "SELECT c.relname, c.relkind, c.relispartition, "
    " coalesce((SELECT json_agg(json_build_array(a.attname, format_type(a.atttypid, a.atttypmod), t.typtype,"
    "     t.typnamespace = 'pg_catalog'::regnamespace, t.typcategory, a.atttypid) ORDER BY a.attnum)"
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
ROLE_PROBLEM = re.compile(r"duckdb\.postgres_role|pg_read_server_files|permission denied", re.I)


def rejection_core(message):
    """The innermost cause of a create_table error (moonlink wraps it in several layers)."""
    msg = message or ""
    for rx in (r"source table \S+ cannot be mirrored: .*?(?=\s+\((?:permanent|temporary)\) at |$)",
               r"conversion error: .*?(?=\s+\((?:permanent|temporary)\) at |$)",
               r"type with oid \d+[^\"]*?not supported",
               r"Decimals with precision larger than \d+ are not supported",
               r"[Uu]nsupported[^\"]{0,160}", r"not supported[^\"]{0,160}", r"[Ii]nvalid[^\"]{0,160}"):
        m = re.search(rx, msg)
        if m:
            return one_line(m.group(0), 240)
    inner = re.sub(r"\s+\((?:permanent|temporary)\) at \S+$", "", msg.split("source: ")[-1].strip())
    return one_line(inner, 240)


def describe_rejection(cols, message):
    """create_table error with the column it is about: the column whose type oid the error names,
    else the first column of a non-builtin or non-base type."""
    m = re.search(r"oid (\d+)", message or "")
    hit = [c for c in cols if m and len(c) > 5 and str(c[5]) == m.group(1)]
    named = re.search(r"column (\S+) \(", message or "")
    if not hit and named:
        hit = [c for c in cols if c[0] == named.group(1)]
    wide = re.search(r"precision larger than (\d+)", message or "")
    if not hit and wide:
        hit = [c for c in cols if re.match(r"numeric\((\d+)", c[1])
               and int(re.match(r"numeric\((\d+)", c[1]).group(1)) > int(wide.group(1))]
    if not hit:
        hit = [c for c in cols if not c[3] or c[2] != "b"]
    core = rejection_core(message)
    if hit:
        c = hit[0]
        return f"create_table rejected: column {c[0]} type={c[1]} typtype={c[2]}: {core}"
    return f"create_table rejected: {core}"


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
        self.events = {}       # table -> events (divergences, failed create_table, failed reads, ...)
        self.ever = set()      # tables a mirror was attempted for
        self.unverified = {}   # table -> why its mirror could not be checked
        self.catalog_dirty = False
        self.pending_cmp = set()
        self.unit_line = 0
        self.cur_stmt = ""
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
            # Every crash since the last one reported, not only this statement's.
            new = self.srv.backend_crashes()[self.h.crashes_seen:]
            self.h.crashes_seen += len(new)
            self.rep.result(f"backend.{self.name}.L{self.unit_line}", "FAIL",
                            "Postgres backend crashed (server crash recovery): "
                            + " | ".join(trap or line for line, trap in new) + f" during: {one_line(text, 160)}")
        if r.died:
            self.rep.stat("session.restarted")
            self.new_session()
        if self.srv.panics() > before:
            self.on_panic(text)
        return r

    def last_panic_site(self):
        lines = self.srv.panic_lines()
        return panic_site(lines[-1]) if lines else "none"

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
                                f"mooncake RPCs time out repeatedly from {self.wedged}; last moonlink panic at "
                                f"{self.last_panic_site()}; the file's remaining mirrors are unverified")
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
        sites = sorted({panic_site(ln) for ln in new})
        self.rep.result(f"panic.{self.name}.L{self.unit_line}", "FAIL",
                        f"{len(new)} moonlink panic(s) at {', '.join(sites)} during: {one_line(text, 160)}",
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

    def event(self, name, kind, detail, stmt="", stage="", **extra):
        """Record what went wrong with a table's mirror; the cdc check is XFAIL only if
        expected.txt explains every event."""
        stmt = one_line(stmt or self.cur_stmt, 300)
        ev = {"kind": kind, "stage": stage, "line": self.unit_line, "stmt": stmt, "detail": detail}
        ev.update({k: v for k, v in extra.items() if v is not None})
        ev["text"] = f"{stage or kind}: {detail} :: {stmt}"
        self.events.setdefault(name, []).append(ev)
        self.rep.stat(f"cdc.event.{kind}")

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
        self.ever.add(name)
        if self.wedged:
            t["mirrored"] = False
            self.unverified.setdefault(name, f"no mirror: moonlink wedged since {self.wedged}")
            return
        crash = next(((c, self.h.crash_types[c[1]]) for c in t["cols"] if c[1] in self.h.crash_types), None)
        if crash:
            (col, (where, site)) = crash
            t["mirrored"] = False
            self.event(name, "create-table", f"create_table not attempted: column {col[0]} type={col[1]} "
                                             f"crashed moonlink at {site} ({where})", stage="create")
            return
        before = self.srv.panics()
        r = self.ctl(f"CALL mooncake.create_table({qlit(SCHEMA_MIRROR + '.' + name)}, {qlit('public.' + name)});")
        if self.srv.panics() > before:
            site = self.last_panic_site()
            suspects = [c for c in t["cols"] if c[1] not in self.h.good_types]
            for c in suspects:
                self.h.crash_types.setdefault(c[1], (f"{self.name}.{name}", site))
            t["mirrored"] = False
            self.event(name, "create-table", f"create_table crashed moonlink at {site} "
                                             f"(suspect types: {','.join(c[1] for c in suspects)})", stage="create")
            self.remirror_pending()
            return
        if not r.ok:
            t["mirrored"] = False
            detail = (f"create_table {r.outcome()}" if r.timed_out or r.died
                      else describe_rejection(t["cols"], r.message))
            self.event(name, "create-table", detail, stage="create")
            self.rep.stat("mirror.rejected")
            return
        t["mirrored"] = True
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
            self.event(name, "drop", f"drop mirror failed: {r.outcome()} {one_line(r.message, 200)}", stage="drop")

    # ---- comparison ----------------------------------------------------------------------------
    def fenced_read(self, sql):
        """Read a mirror right after a fence commit in the reading session: pg_mooncake makes the
        scan wait for that session's last commit, which is after every write so far."""
        fence = self.s.run("SELECT pg_current_xact_id() \\g /dev/null")
        if fence.ok:
            r = self.run(sql)
            if r.ok or not ROLE_PROBLEM.search(r.message):
                return r
        # The file switched to a role that can't read mirrors (or can't commit): read as the
        # harness user. Its fence commit comes after the file session's commits as well.
        self.ctl("SELECT pg_current_xact_id() \\g /dev/null")
        return self.ctl(sql)

    def compare(self, name, stmt, stage):
        """Mirror vs heap for one table, outside a transaction block. Records events; after a
        divergence the mirror is re-created so later statements are checked independently."""
        t = self.tables[name]
        if not t.get("mirrored") or (t["lossy_all"] and stage != "final"):
            return True
        if self.wedged:
            self.unverified.setdefault(name, f"moonlink wedged since {self.wedged}")
            return True
        cols = [c for c in t["cols"] if c[0] not in t["bad_cols"]] if stage != "final" else t["cols"]
        if not cols:
            return True
        sel = select_list(cols)
        heap = self.run(f"SELECT {sel} FROM public.{qident(name)};")
        if not heap.ok:
            self.event(name, "heap-read-error", f"heap read failed: {heap.outcome()} {heap.sqlstate} "
                                                f"{one_line(heap.message, 200)}", stmt, stage)
            return False
        jidx = json_cols(cols)
        heap.rows = json_norm(heap.rows, jidx)
        first = mir = self.fenced_read(f"SELECT {sel} FROM {SCHEMA_MIRROR}.{qident(name)};")
        mir.rows = json_norm(mir.rows, jidx)
        if mir.ok and compare.rows_match(heap.rows, mir.rows)[0]:
            return True
        deadline = time.monotonic() + self.h.args.converge_secs
        t0 = time.monotonic()
        while not (mir.died or mir.timed_out) and time.monotonic() < deadline:
            time.sleep(0.5)
            mir = self.fenced_read(f"SELECT {sel} FROM {SCHEMA_MIRROR}.{qident(name)};")
            mir.rows = json_norm(mir.rows, jidx)
            if mir.ok and compare.rows_match(heap.rows, mir.rows)[0]:
                was = (f"error {one_line(first.message, 120)}" if not first.ok
                       else compare.divergence_events([(c[0], c[1]) for c in cols], heap.rows, first.rows)[0]["detail"])
                self.event(name, "stale-read", f"stale read: the mirror matched only {time.monotonic() - t0:.1f}s "
                                               f"after the fenced read, which gave {one_line(was, 200)}", stmt, stage)
                return False
        if not mir.ok:
            evs = [{"kind": "mirror-error", "detail": f"mirror read failed: {mir.outcome()} {mir.sqlstate} "
                                                      f"{one_line(mir.message, 500)}"}]
        else:
            evs = compare.divergence_events([(c[0], c[1]) for c in cols], heap.rows, mir.rows)
        for e in evs:
            self.event(name, e["kind"], e["detail"], stmt, stage, col=e.get("col"), type=e.get("type"))
        self.h.diverged_cols.setdefault((self.name, name), set()).update(e.get("col") or "*" for e in evs)
        self.rep.stat("cdc.divergences")
        if stage == "copy":
            if mir.ok and evs and all(e["kind"] == "value" for e in evs):
                t["bad_cols"] |= {e["col"] for e in evs}
            else:
                t["lossy_all"] = True
            return False
        if stage != "final":
            self.drop_mirror(name)
            self.mirror(name, "re-baseline after divergence")
        return False

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

    def describe(self, unit, in_txn):
        """(result column names, types) of a query, from psql's \\gdesc (nothing is executed)."""
        sql = unit.sql.rstrip().rstrip(";")
        text = ("SAVEPOINT md_desc;\n" if in_txn else "") + sql + "\n\\gdesc\n"
        r = self.s.run(text, timeout=self.h.args.stmt_timeout, on_timeout=self.srv.cancel)
        if in_txn and self.s.alive():
            self.s.run("ROLLBACK TO SAVEPOINT md_desc;\nRELEASE SAVEPOINT md_desc;")
        if not r.ok:
            return [], []
        rows = [row for row in r.rows if len(row) == 2]
        return [row[0] for row in rows], [row[1] for row in rows]

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
        names, types = self.describe(unit, in_txn) if heap.ok else ([], [])
        order_items, limit = compare.top_level_order_by(unit.sql)
        q.update(types=types, order_by=order_items, limit=limit)

        def match(a, b):
            return compare.results_match(a, b, types, order_items, names, limit)
        mir = self.shadow(unit, mirror=True, in_txn=in_txn)
        eq, how = match(heap, mir)
        q["how"] = how
        if eq:
            q["status"] = "PASS"
            self.rep.stat(f"query.pass_{how}")
            self.queries.append(q)
            return
        if how == "limit-unverifiable" and self.limit_check(unit, q, heap, mir, types, in_txn):
            self.queries.append(q)
            return
        duck = self.shadow(unit, mirror=False, in_txn=in_txn)
        duck_ran = self.ran_in_duckdb(unit, in_txn)
        status, verdict, reason = compare.classify(heap, mir, duck, duck_ran, match, compare.order_dependent(unit.sql))
        if status == "SKIP":
            self.rep.stat("query.duckdb_unsupported")
        q.update(status=status, verdict=verdict, reason=reason, heap=heap.outcome(), mirror=mir.outcome(),
                 duck=duck.outcome() if duck_ran else "not-run-by-duckdb", sample=diff_sample(heap, mir),
                 heap_error=heap.message[:300], mirror_error=mir.message[:300], duck_error=duck.message[:300],
                 duck_vs_mirror=match(duck, mir)[1], duck_vs_heap=match(duck, heap)[1])
        self.queries.append(q)

    def limit_check(self, unit, q, heap, mir, types, in_txn):
        """Rows a LIMIT may legitimately pick differently: any rows of the unlimited heap result
        (with the same sort keys where they differ) are a correct answer, so the mirror's must
        be in it. Returns False when the unlimited result can't be read; the caller classifies
        the difference as usual then."""
        full_sql = compare.strip_top_level_limit(unit.sql)
        if not full_sql:
            return False
        if in_txn:
            self.s.run("SAVEPOINT md_full;")
        full = self.run(full_sql + ";")
        if in_txn and self.s.alive():
            self.s.run("ROLLBACK TO SAVEPOINT md_full;\nRELEASE SAVEPOINT md_full;")
        if not full.ok:
            q["full_error"] = f"{full.outcome()} {full.message[:200]}"
            return False
        if mir.ok and compare.rows_subset(mir.rows, full.rows, types):
            q.update(status="PASS", how="limit-subset")
            self.rep.stat("query.pass_limit-subset")
            return True
        # Classify like any other difference, with "is in the unlimited result" standing in for
        # "equals the heap result".
        duck = self.shadow(unit, mirror=False, in_txn=in_txn)
        duck_ran = self.ran_in_duckdb(unit, in_txn)
        if duck_ran and duck.ok and compare.rows_subset(duck.rows, full.rows, types):
            q.update(status="FAIL", verdict="mooncake",
                     reason="mirror rows are not in the heap's result without LIMIT; DuckDB on the heap tables' are")
        elif duck_ran and duck.ok and mir.ok and compare.rows_match(duck.rows, mir.rows, types)[0]:
            q.update(status="ENGINE", verdict="engine",
                     reason="DuckDB semantics (DuckDB on the heap tables gives the mirror's rows, outside the heap's result)")
        else:
            q.update(status="FAIL", verdict="unclassified", reason="mirror rows are not in the heap's result without LIMIT")
        q.update(sample=diff_sample(heap, mir), heap=heap.outcome(), mirror=mir.outcome(),
                 duck=duck.outcome() if duck_ran else "not-run-by-duckdb")
        return True

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

    def handle_multi(self, unit):
        """Statements joined with \\; run as one query string: compare everything they name."""
        self.run(unit.text)
        refs = self.mirrored_refs(unit.sql)
        if self.txn_state()[0]:
            self.catalog_dirty = True
            self.pending_cmp.update(refs)
            return
        self.sync_catalog()
        for name in self.mirrored_refs(unit.sql):
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
        self.cur_stmt = unit.sql or unit.meta
        if unit.kind == "meta":
            if not (SKIP_META.match(unit.meta) or SKIP_SET_VAR.match(unit.meta)):
                self.run(unit.text)
            return
        if unit.kind == "block":
            self.run(unit.text)
            self.catalog_dirty = True
            return
        if unit.multi:
            self.handle_multi(unit)
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
        self.cur_stmt = "<fixture>"
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
                                    f"stopped at line {unit.line} after {self.h.args.file_timeout:.0f}s; last moonlink "
                                    f"panic at {self.last_panic_site()}")
                    for name in self.tables:
                        if self.tables[name].get("mirrored"):
                            self.unverified.setdefault(name, f"file stopped at line {unit.line} (file timeout)")
                    break
                self.unit_line = unit.line
                self.execute(unit)
                self.remirror_pending()
            self.unit_line = 0
            self.cur_stmt = "<end of file>"
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
        kinds = sorted({e["kind"] for e in ev})
        return f"events={len(ev)} kinds={','.join(kinds)} first@L{ev[0]['line']} {ev[0]['text']}"

    def report(self):
        for name in sorted(self.ever):
            check = f"cdc.{self.name}.{name}"
            if self.events.get(name):
                self.rep.result(check, "FAIL", self.cdc_detail(name), events=self.events[name])
            elif name in self.unverified:
                self.rep.result(check, "SKIP", f"unverified: {self.unverified[name]}", unverified=True)
            else:
                self.rep.result(check, "PASS")
        for q in self.queries:
            check = f"query.{self.name}.L{q['line']}"
            st = q["status"]
            sql = one_line(q["sql"], 160)
            rec = {k: v for k, v in q.items() if k not in ("status",)}
            if st == "PASS":
                self.rep.result(check, "PASS", "" if q.get("how") == "exact" else q["how"])
            elif st == "SKIP":
                self.rep.result(check, "SKIP", f"{q['reason']}: {sql}", record=rec)
            elif st == "ENGINE":
                self.rep.stat("query.engine")
                self.rep.result(check, "FAIL", f"engine: {q['reason']}: {q.get('sample') or q.get('heap_error')} :: {sql}",
                                record=rec, ids=self.h.exp.engine or ["duckdb-semantics"])
            elif q.get("masked_by"):
                names = q["masked_by"]
                ids = []
                for n in names:
                    k = self.h.exp.match_events(f"cdc.{self.name}.{n}", self.events.get(n, []))
                    if k is None:
                        ids = None
                        break
                    ids += [x for x in k if x not in ids]
                if ids:
                    self.rep.result(check, "FAIL", f"reads lossy mirror {names}: {sql}", record=rec, ids=ids)
                else:
                    self.rep.result(check, "SKIP", f"reads lossy mirror {names} (see cdc.{self.name}.*): {sql}",
                                    record=rec)
            else:
                self.rep.result(check, "FAIL", f"{q['reason']}: {q.get('sample', '')} :: {sql}", record=rec)


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
        self.states = {}  # table -> content hashes already checked in this file

    def fresh_db(self, replay=True):
        """New database; replay the file. Regress files drop and alter their tables as they go,
        so the tables are collected (copy check + type values) before every destructive statement."""
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
        try:
            self.fresh_db()
        finally:
            if self.s:
                self.s.kill()

    def collect(self):
        tables = {n: i for n, i in read_tables(self.s).items() if not i["partition"]}
        for rel in sorted(tables):
            self.copy_check(rel, tables[rel]["cols"])
        by_type = {}
        for rel, info in tables.items():
            for col in info["cols"]:
                by_type.setdefault((col[1], col[2], col[3]), []).append((rel, col[0]))
        for (typ, typtype, builtin), uses in sorted(by_type.items()):
            union = " UNION ALL ".join(f"SELECT {qident(att)}::{typ} AS v FROM public.{qident(rel)}"
                                       for rel, att in uses)
            if builtin:
                self.harvest(typ, union, typtype)
            else:
                self.h.type_probe(self, f"data.type.{self.name}.{type_key(typ)}", typ,
                                  f"SELECT v FROM ({union}) s", origin=self.name, typtype=typtype)

    def harvest(self, typ, union, typtype):
        r = self.q(f"SELECT DISTINCT v::text FROM ({union}) s LIMIT {self.h.args.pool_limit};")
        if r.ok:
            pool = self.h.pool.setdefault(typ, {"values": set(), "files": set(), "typtype": typtype})
            pool["values"].update(row[0] for row in r.rows if row)
            pool["files"].add(self.name)

    def copy_check(self, rel, cols):
        """Initial-copy check of one state of a table: each distinct content of a table is checked
        once per file (data.copy.<file>.<table>, then .s2, .s3, ...), and a content already checked
        in another file is not checked again."""
        if not cols:
            return
        sel = select_list(cols)
        h = self.q(f"SELECT md5(string_agg(t::text, E'\\n' ORDER BY t::text)), count(*) FROM (SELECT {sel} FROM public.{qident(rel)}) t;")
        digest = h.rows[0][0] if h.ok and h.rows else f"unreadable:{self.attempt}"
        seen = self.states.setdefault(rel, [])
        if digest in seen or ("table", rel, digest) in self.h.seen:
            return
        seen.append(digest)
        self.h.seen.add(("table", rel, digest))
        self.h.last_events = []
        verdict, detail = self.h.mirror_and_compare(self, f"public.{qident(rel)}", rel, f"md.{rel}", sel, cols)
        check = f"data.copy.{self.name}.{rel}" + (f".s{len(seen)}" if len(seen) > 1 else "")
        types = sorted({c[1] for c in cols})
        rec = {"verdict": verdict, "types": types}
        base = f"{verdict}: types=[{','.join(types)}]"
        # One event per diverging column, so a listed bug on one column can't hide another.
        parts = self.h.last_events if verdict in ("lossy", "equivalent") and self.h.last_events else [detail]
        events = [{"kind": verdict, "text": f"{base} {d}"} for d in parts]
        self.rep.result(check, "PASS" if verdict == "exact" else "FAIL",
                        "" if verdict == "exact" else f"{base} {detail}", record=rec,
                        events=None if verdict == "exact" else events)
        self.q(f"DROP TABLE IF EXISTS md.{qident(rel)};", timeout=self.h.args.ddl_timeout)


class PoolRun(ProbeRun):
    """Probes every pooled built-in type in one database."""

    def go(self):
        try:
            self.probe_all()
        finally:
            if self.s:
                self.s.kill()

    def probe_all(self):
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
                              origin=",".join(sorted(self.h.pool[typ]["files"])),
                              typtype=self.h.pool[typ].get("typtype", "b"))


class FixtureError(RuntimeError):
    pass


# ------------------------------------------------------------------------------------------------
class Harness:
    def __init__(self, args):
        self.args = args
        self.srv = Server(args)
        if args.host and args.server_log:
            self.srv.logfile = args.server_log
        self.exp = Expectations(args.expected, args.bug_ids)
        os.makedirs(args.workdir, exist_ok=True)
        os.makedirs(os.path.join(args.workdir, "results"), exist_ok=True)
        self.done_path = os.path.join(args.workdir, "done.txt")
        self.pool_path = os.path.join(args.workdir, "pool.json")
        self.done = set()
        if args.resume and os.path.exists(self.done_path):
            self.done = {line.strip() for line in open(self.done_path) if line.strip()}
        self.rep = Reporter(self.exp, os.path.join(args.workdir, "results.jsonl"), resume=args.resume,
                            done=self.done)
        self.seen = set()
        self.pool = {}
        self.diverged_cols = {}
        self.crash_types = {}  # column type -> (where, panic site) of a create_table that crashed moonlink
        self.good_types = set()
        self.panics_seen = 0
        self.crashes_seen = 0
        self.last_events = []   # divergence details of the last lossy/equivalent converge()
        self.probe_seq = 0
        self.last_verdict = None
        self.server_num = 0
        self.regresslib_note = False
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
                    self.pool[typ] = {"values": set(v["values"]), "files": set(v["files"]),
                                      "typtype": v.get("typtype", "b")}
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
        """md_template: the fixtures of test_setup.sql, copied into every database. A fixture
        statement that fails voids every check built on it, so it stops the run (FixtureError).
        The few that need the regress.so test library are skipped when it isn't built."""
        self.srv.admin("postgres", "DROP DATABASE IF EXISTS md_template WITH (FORCE)")
        rc, _, err = self.srv.admin("postgres", "CREATE DATABASE md_template")
        if rc != 0:
            raise FixtureError(f"create database md_template: {err}")
        have_lib = os.path.exists(os.path.join(self.regress, "regress.so"))
        s = self.session("md_template")
        failed, skipped = [], 0
        try:
            for unit in sqlsplit.split(open(self.sql_path(FIXTURES)).read()):
                if not have_lib and "regresslib" in unit.text and unit.kind == "sql":
                    skipped += 1
                    continue
                r = s.run(unit.text, timeout=300)
                if not r.ok:
                    failed.append(f"L{unit.line}: {r.outcome()} {r.message[:160]}")
        finally:
            s.close()
        if skipped and not self.regresslib_note:
            self.regresslib_note = True
            print(f"INFO {skipped} fixture statement(s) skipped: {self.regress}/regress.so is not built", flush=True)
        if failed:
            raise FixtureError(f"{len(failed)} fixture statement(s) of {FIXTURES}.sql failed: {'; '.join(failed)[:600]}")

    # ---- shared probe logic ------------------------------------------------------------------
    def mirror_and_compare(self, run, heap_rel, src_name, dst, sel, cols):
        """Mirror public.<src_name> into <dst> by initial copy; return (verdict, detail)."""
        before = self.srv.panics()
        r = run.q(f"CALL mooncake.create_table({qlit(dst)}, {qlit('public.' + src_name)});", timeout=self.args.ddl_timeout)
        if self.srv.panics() > before:
            p = self.srv.panic_lines()
            sites = ", ".join(sorted({panic_site(ln) for ln in p[self.panics_seen:]}))
            self.panics_seen = len(p)
            run.recover()
            return "crashed", f"create_table panicked moonlink at {sites}"
        if not r.ok:
            if r.outcome() != "error":
                run.recover()
                return "crashed", f"create_table {r.outcome()}"
            return "rejected", describe_rejection(cols, r.message).replace("create_table rejected: ", "", 1)
        return self.converge(run, f"SELECT {sel} FROM {heap_rel}", f"SELECT {sel} FROM {dst}",
                             [(c[0], c[1]) for c in cols])

    def converge(self, run, heap_sql, mirror_sql, cols):
        """Verdict of one mirror read. The mirror is read right after a fence commit in the session
        that wrote, so read-your-writes must already show the heap's rows; one that matches only
        when read again is a stale read. A moonlink panic since the heap read is a crash even if
        the rows match."""
        before = self.srv.panics()
        heap = run.q(heap_sql + ";")
        if not heap.ok:
            return "errored", f"heap query failed: {heap.message[:200]}"
        deadline = time.monotonic() + self.args.converge_secs
        t0, first = time.monotonic(), None
        while True:
            run.q("SELECT pg_current_xact_id() \\g /dev/null")
            mir = run.q(mirror_sql + ";")
            first = first or mir
            if self.srv.panics() > before or mir.died:
                p = self.srv.panic_lines()
                sites = ", ".join(sorted({panic_site(ln) for ln in p[self.panics_seen:]})) or "none"
                self.panics_seen = len(p)
                run.recover()
                return "crashed", f"mirror scan {mir.outcome()}; moonlink panicked at {sites}: {one_line(mir.message, 160)}"
            if mir.ok and compare.rows_match(heap.rows, mir.rows)[0]:
                if mir is first:
                    return "exact", ""
                was = (f"error {one_line(first.message, 120)}" if not first.ok else
                       "; ".join(e["detail"] for e in compare.divergence_events(cols, heap.rows, first.rows)))
                return "stale", (f"stale read: matched only {time.monotonic() - t0:.1f}s after the fenced read, "
                                 f"which gave {one_line(was, 200)}")
            if mir.timed_out or time.monotonic() >= deadline:
                break
            time.sleep(0.5)
        if not mir.ok:
            return "errored", f"mirror scan {mir.outcome()}: {one_line(mir.message, 400)}"
        events = compare.divergence_events(cols, heap.rows, mir.rows)
        self.last_events = [e["detail"] for e in events]
        details = "; ".join(self.last_events)
        if all(e.get("type") in ("json", "jsonb") for e in events) and json_equivalent(heap, mir):
            return "equivalent", details
        return "lossy", details

    def type_probe(self, run, check, typ, values_sql, origin, typtype="b"):
        """One column type in isolation: initial copy, CDC insert/update/delete, snapshot."""
        self.probe_seq += 1
        k = self.probe_seq
        src, cdc = f"md_ps{k}", f"md_pc{k}"
        panics_before = self.srv.panics()
        what = f"type={typ} typtype={typtype}"
        r = run.q(f"CREATE TABLE public.{src} AS SELECT row_number() OVER () AS id, v FROM ({values_sql}) s;")
        if r.ok:
            r = run.q(f"ALTER TABLE public.{src} ADD PRIMARY KEY (id);")
        if not r.ok:
            self.rep.result(check, "FAIL", f"probe table not built: {what} ({origin}): {one_line(r.message, 200)}",
                            record={"type": typ, "verdict": "harness", "origin": origin})
            return
        n = run.q(f"SELECT count(*) FROM public.{src};").rows
        n = n[0][0] if n else "?"
        oid = run.q(f"SELECT atttypid FROM pg_attribute WHERE attrelid = 'public.{src}'::regclass AND attname = 'v';").rows
        oid = int(oid[0][0]) if oid and oid[0] and oid[0][0].isdigit() else 0
        cols = [("id", "bigint", "b", True, "N", 20), ("v", typ, typtype, True, "", oid)]
        steps = {}
        # create_table rejects a table holding a value the mirror can't store (and names it). Each such
        # value is recorded and removed, so the type's other values are still checked.
        refused = []
        step_events = {}
        for _ in range(self.args.max_rejected_values):
            self.last_events = []
            steps["copy"] = self.mirror_and_compare(run, f"public.{src}", src, f"md.{src}", "id, v", cols)
            step_events["copy"] = list(self.last_events)
            bad = re.search(r"cannot convert '(.*?)': (.*)$", steps["copy"][1]) if steps["copy"][0] == "rejected" else None
            if not bad:
                break
            value = bad.group(1)
            if value.endswith("..."):  # moonlink shortens long values in the message
                cond = "v::text LIKE " + qlit(re.sub(r"([\\%_])", r"\\\1", value[:-3]) + "%")
            else:
                cond = f"v::text = {qlit(value)}"
            d = run.q(f"WITH d AS (DELETE FROM public.{src} WHERE {cond} RETURNING 1) SELECT count(*) FROM d;")
            if not d.ok or not d.rows or d.rows[-1][0] == "0":
                break
            refused.append((bad.group(1), bad.group(2)))
        if refused:
            n = (run.q(f"SELECT count(*) FROM public.{src};").rows or [["?"]])[0][0]
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
                    self.last_events = []
                    steps[step] = self.converge(run, f"SELECT id, v FROM public.{cdc}", f"SELECT id, v FROM md.{cdc}",
                                                [("id", "bigint"), ("v", typ)])
                    step_events[step] = list(self.last_events)
                    if steps[step][0] != "exact":
                        break
            else:
                steps["cdc_insert"] = ("rejected" if r.outcome() == "error" else "crashed",
                                       one_line(r.message or r.outcome(), 160))
        for t in (cdc, src):
            run.q(f"DROP TABLE IF EXISTS md.{t};", timeout=self.args.ddl_timeout)
            run.q(f"DROP TABLE IF EXISTS public.{t};")
        seen = [v for v, _ in steps.values()]
        overall = next(o for o in VERDICT_ORDER if o in seen or o == "exact")
        if overall == "rejected" and len(set(seen)) > 1:
            overall = next(o for o in VERDICT_ORDER if o in seen and o != "rejected")
        if refused and overall == "exact":
            overall = "rejected-values"
        if self.srv.panics() > panics_before and overall != "crashed":
            # A panic in the source DML or create_snapshot steps, with a mirror that still matched.
            lines = self.srv.panic_lines()
            sites = ", ".join(sorted({panic_site(ln) for ln in lines[min(self.panics_seen, panics_before):]}))
            self.panics_seen = len(lines)
            overall = "crashed"
            steps["probe"] = ("crashed", "moonlink panicked at " + sites)
        detail = "; ".join(f"{s}={v}" + (f" ({d})" if d and v != "exact" else "") for s, (v, d) in steps.items())
        if refused:
            reasons = sorted({r for _, r in refused})
            detail += (f"; {len(refused)} value(s) rejected at create_table: "
                       + "; ".join(reasons)[:300] + " [" + ", ".join(v[:40] for v, _ in refused[:5]) + "]")
        self.last_verdict = overall
        base = f"{overall}: {what} {n} values ({origin})"
        # One event per step that isn't exact (per diverging column) and per refused value, matched
        # separately against expected.txt.
        events = []
        for step, (v, d) in steps.items():
            if v == "exact":
                continue
            parts = step_events.get(step) if v in ("lossy", "equivalent") and step_events.get(step) else [d]
            events += [{"kind": v, "text": f"{base}; {step}={v} ({p})"} for p in parts]
        events += [{"kind": "rejected-value", "text": f"{base}; value rejected at create_table: {r} [{v[:40]}]"}
                   for v, r in refused]
        self.rep.result(check, "PASS" if overall == "exact" else "FAIL", f"{base}; {detail}",
                        record={"type": typ, "typtype": typtype, "verdict": overall, "steps": steps, "origin": origin},
                        events=events or None)

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

    def complete(self):
        return not self.args.files and set(self.args.layers.split(",")) >= {"data", "cdc", "query"}

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
        rc, cassert, _ = self.srv.admin("postgres", "SELECT current_setting('debug_assertions')")
        print(f"BUILD cassert={cassert or 'unknown'}", flush=True)
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
        # A throwaway cluster has a fresh log: everything in it belongs to this unit.
        self.panics_seen = 0 if self.srv.managed else self.srv.panics()
        self.crashes_seen = 0 if self.srv.managed else len(self.srv.backend_crashes())

    def sweep(self, kind, name):
        """Panics and crash restarts in the unit's server log that no check reported (they happened
        outside a statement the harness watches: a fence, a txn-state probe, between statements)."""
        lines = self.srv.panic_lines()
        if len(lines) > self.panics_seen:
            new = lines[self.panics_seen:]
            self.rep.result(f"panic.{kind}.{name}", "FAIL",
                            f"{len(new)} moonlink panic(s) at {', '.join(sorted({panic_site(ln) for ln in new}))} "
                            "not attributed to a check", record={"panics": new[:5]})
            self.panics_seen = len(lines)
        crashes = self.srv.backend_crashes()
        if len(crashes) > self.crashes_seen:
            new = crashes[self.crashes_seen:]
            self.rep.result(f"backend.{kind}.{name}", "FAIL",
                            f"{len(new)} Postgres crash restart(s) not attributed to a statement: "
                            + "; ".join(sorted({one_line(trap or re.sub(r'^.*?LOG:\s+', '', line), 200) for line, trap in new})))
            self.crashes_seen = len(crashes)

    def end(self, kind, name):
        """No unreported panic or crash in the unit's log, and moonlink still answers after the
        file (no crash loop); then tear down. The log is swept last, so panics logged by the health
        check or the shutdown are read too."""
        db = self.dbs[-1] if self.dbs else None
        if db is not None:
            why = []
            ok = self.srv.healthy(db) or (time.sleep(20) or self.srv.healthy(db, why))
            self.rep.result(f"service.{kind}.{name}", "PASS" if ok else "FAIL",
                            "" if ok else f"mooncake.list_tables() fails after the file: {one_line(why[-1], 240)}; "
                                          f"last moonlink panic at {panic_site(self.srv.panic_lines()[-1]) if self.srv.panics() else 'none'}")
        if self.srv.managed:
            if not self.args.keep:
                self.srv.stop()
        else:
            for db in list(self.dbs):
                self.drop_db(db)
        self.sweep(kind, name)

    def checkpoint(self, key):
        """Record a finished unit of work so --resume skips it (results are already in results.jsonl)."""
        self.done.add(key)
        with open(self.done_path, "a") as f:
            f.write(key + "\n")
        pool = {t: ({"values": sorted(v["values"]), "files": sorted(v["files"]), "typtype": v.get("typtype", "b")}
                    if t != "_done" else v) for t, v in self.pool.items()}
        with open(self.pool_path + ".tmp", "w") as f:
            json.dump(pool, f)
        os.replace(self.pool_path + ".tmp", self.pool_path)

    def run_unit(self, kind, f):
        key = f"{kind} {f}"
        self.rep.unit = key
        print(f"INFO begin {key}", flush=True)
        t0 = time.monotonic()
        self.begin(f"{kind}-{f}")
        try:
            (ProbeRun if kind == "data" else FileRun)(self, f).go()
        except FixtureError:
            raise
        except Exception as e:  # one broken file must not end the run
            import traceback
            self.rep.result(f"infra.{kind}.{f}", "FAIL", f"{type(e).__name__}: {e}",
                            record={"traceback": traceback.format_exc()})
        self.end(kind, f)
        self.checkpoint(key)
        print(f"INFO end {key} {time.monotonic() - t0:.0f}s", flush=True)

    def main(self):
        sel = self.selection()
        self.rep.unit = "build-info"
        try:
            self.begin("build-info")
            self.build_info()
            self.srv.stop()
            if not self.srv.managed and not self.args.server_log:
                self.rep.result("infra.panic_detection", "SKIP", "--host without --server-log: moonlink panics "
                                "and backend crashes in the server log are not detected", unverified=True)
            for kind, f in sel:
                if kind == "run" and "data" in self.args.layers.split(",") and self.pool and not self.pool.get("_done"):
                    self.run_pool()
                if f"{kind} {f}" in self.done:
                    print(f"INFO skip {kind} {f} (done in an earlier run, --resume)", flush=True)
                    continue
                self.run_unit(kind, f)
            if self.pool and not self.pool.get("_done"):
                self.run_pool()
            self.type_summary()
        except FixtureError as e:
            self.rep.result("infra.fixtures", "FAIL", str(e))
        finally:
            if self.srv.managed and not self.args.keep:
                self.srv.stop()
        layers = self.args.layers.replace(",", "+")
        gate, regression = self.rep.summary(self.major(), layers, self.complete())
        return regression if self.args.exit_regression else gate

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
        self.rep.unit = "data pooled-types"
        print("INFO begin data pooled-types", flush=True)
        t0 = time.monotonic()
        self.pool = {k: v for k, v in self.pool.items() if k != "_done"}
        self.begin("data-pooled-types")
        try:
            PoolRun(self, "pool").go()
        except FixtureError:
            raise
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
                if rec["check"].startswith("data.type.") and rec.get("verdict") in VERDICT_ORDER:
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
    p.add_argument("--server-log", help="with --host: the server's log file, for panic and crash detection")
    p.add_argument("--port", type=int, default=5491)
    p.add_argument("--user")
    p.add_argument("--duckdb-extension-dir", default=os.environ.get("DUCKDB_EXTENSION_DIR"),
                   help="duckdb.extension_directory for the throwaway cluster")
    p.add_argument("--conf", action="append", help="extra postgresql.conf line (repeatable)")
    p.add_argument("--layers", default="data,cdc,query")
    p.add_argument("--files", help="comma-separated subset of file names (regress or extra_*)")
    p.add_argument("--expected", default=os.path.join(HERE, "expected.txt"), help="known-failure list")
    p.add_argument("--bug-ids", default=os.environ.get("MIRROR_DIFF_BUG_IDS"),
                   help='optional bug-id map: a file of "<bug> <ids>" lines, shown on KNOWN lines')
    p.add_argument("--build-info", help="file of k=v lines to echo as BUILD lines")
    p.add_argument("--stmt-timeout", type=float, default=120)
    p.add_argument("--ddl-timeout", type=float, default=60)
    p.add_argument("--file-timeout", type=float, default=2400)
    p.add_argument("--converge-secs", type=float, default=10)
    p.add_argument("--pool-limit", type=int, default=5000, help="max distinct values per type per file")
    p.add_argument("--max-rejected-values", type=int, default=40,
                   help="values create_table may reject (by name) before a type probe gives up")
    p.add_argument("--trace", action="store_true", help="log every statement sent to <workdir>/trace.sql")
    p.add_argument("--resume", action="store_true",
                   help="continue a run in the same --workdir: skip units listed in done.txt")
    p.add_argument("--keep", action="store_true", help="keep databases and leave the server running")
    p.add_argument("--exit-regression", action="store_true",
                   help="exit 0 iff no check fails or goes unverified unlisted (instead of the GATE code)")
    a = p.parse_args(argv)
    if not a.bindir:
        try:
            a.bindir = subprocess.run(["pg_config", "--bindir"], capture_output=True, text=True, check=True).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            p.error("--bindir is required when pg_config is not on PATH")
    a.workdir = os.path.abspath(a.workdir)
    return a


def requalify(rec, status, detail):
    """Re-apply the classifier's SKIP rules for a mirror query that errored (a finished run's
    record keeps the outcomes and error texts, not the rows). A FAIL the classifier attributed to
    the mirror (DuckDB on the heap tables agreed with the heap) stays a FAIL."""
    if rec.get("verdict") == "mooncake":
        return status, detail
    msg = rec.get("mirror_error") or ""
    if compare.DUCKDB_ROLE_ERROR.search(msg):
        return "SKIP", f"the session's role may not run DuckDB: {rec.get('sql', '')[:160]}"
    duck_ran = rec.get("duck") not in (None, "not-run-by-duckdb")
    if compare.is_planning_error(msg) and (not duck_ran or rec.get("duck") == "error"):
        return "SKIP", f"DuckDB cannot run it: {rec.get('sql', '')[:160]}"
    return status, detail


def rescore(results_path, expected_path, major="?", exit_regression=False):
    """Re-apply the known-failure list to a finished run's results.jsonl (no database needed).
    The raw outcome of each check is its status with XFAIL/XPASS undone; engine-semantics XFAILs
    (from the classifier, not from the list) are kept."""
    exp = Expectations(expected_path, os.environ.get("MIRROR_DIFF_BUG_IDS"))
    out_path = results_path.replace(".jsonl", ".rescored.jsonl")
    rep = Reporter(exp, out_path)
    with open(results_path) as f:
        recs = [json.loads(line) for line in f if line.strip()]
    events = {r["check"]: r.get("events") or [] for r in recs if r["check"].startswith("cdc.")}
    for rec in recs:
        status, detail = rec["status"], rec.get("detail", "")
        ids = None
        if status in ("XFAIL", "XFAIL_LIMITATION") and detail.startswith("engine:"):
            ids = exp.engine or rec.get("ids") or ["duckdb-semantics"]
            status = "FAIL"
        elif status in ("XFAIL", "XFAIL_LIMITATION") and rec.get("masked_by"):
            fname = rec["check"].split(".")[1]
            ids, status = [], "SKIP"
            for n in rec["masked_by"]:
                k = exp.match_events(f"cdc.{fname}.{n}", events.get(f"cdc.{fname}.{n}", []))
                if k is None:
                    ids = []
                    break
                ids += [x for x in k if x not in ids]
            if ids:
                status = "FAIL"
        elif status in ("XFAIL", "XFAIL_LIMITATION"):
            status = "FAIL"
        elif status == "XPASS":
            status = "PASS"
        if status == "FAIL" and rec["check"].startswith("query.") and rec.get("mirror") == "error":
            status, detail = requalify(rec, status, detail)
        unverified = bool(rec.get("unverified")) or (status == "SKIP" and detail.startswith("unverified:"))
        extra = {k: v for k, v in rec.items() if k not in ("check", "status", "ids", "detail", "events", "unverified", "unit")}
        rep.unit = rec.get("unit", "")
        rep.names[rec["check"]] = 0  # keep the original check name (with any .vN suffix)
        rep.result(rec["check"], status, detail, record=extra, ids=ids, events=rec.get("events"), unverified=unverified)
    gate, regression = rep.summary(major, "rescore", True)
    return regression if exit_regression else gate


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--rescore":
        # mirror_diff.py --rescore <results.jsonl> [expected.txt] [--exit-regression]
        rest = [a for a in sys.argv[2:] if a != "--exit-regression"]
        sys.exit(rescore(rest[0], rest[1] if len(rest) > 1 else os.path.join(HERE, "expected.txt"),
                         exit_regression="--exit-regression" in sys.argv))
    sys.exit(Harness(parse_args(sys.argv[1:])).main())
