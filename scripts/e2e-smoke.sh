#!/usr/bin/env bash
# End-to-end release gate for an installed pg_mooncake + pg_duckdb + duckdb_mooncake.
#
# Usage: scripts/e2e-smoke.sh <pg_major> [scenario ...]
#   scenarios: build main schema restart_after_drop persist moonlink_down ddl ddl_drop truncate
#              big_txn_restart crash
#              (default: all; each on a fresh cluster)
# Requires Linux, bash >= 4, GNU coreutils (timeout, date), procps (pgrep), diff and find; checked
# before anything else runs.
#
# Output (one line each, machine-readable; <check> never contains spaces):
#   BUILD <key>=<value>                        what is under test (binaries, hashes, commits)
#   RESULT <scenario.check> <status> [detail]  status: PASS | FAIL | XFAIL(<bugs>) | XPASS(<bugs>) | SKIP
#   INFO <scenario.check> ...                  reported only, never part of the gate
#   KNOWN <bug> FIXED|OPEN|UNKNOWN|NOT-RUN <check>=<status>,... [ids=<ids>]
#   SUMMARY e2e-smoke pg<major> scenarios=<a,b,...> pass=N xpass=N fail=N xfail=N skip=N
#   GATE e2e-smoke PASS|FAIL|INCOMPLETE        (last line)
# Every RESULT line is a gate check. GATE is FAIL (exit 1) if any RESULT is FAIL, XFAIL or SKIP,
# INCOMPLETE (exit 3) if everything else passed but not every scenario ran, and PASS (exit 0)
# otherwise. SKIP means a check could not run, e.g. because postgres died.
#
# Known bugs are an optional overlay (KNOWN_BUGS=<file>, default scripts/e2e-known-bugs.txt if it
# exists) that lists, per check, a bug and a regex for that bug's failure signature. A listed check
# whose failure signatures all match listed bugs reports XFAIL(<bugs>). Any other failure stays FAIL,
# so a new problem in a check that is already known to fail is not hidden. A listed check that
# passes reports XPASS(<bugs>), and KNOWN <bug> FIXED means every check listed for the bug passed.
# KNOWN_BUG_IDS=<file> (lines "<bug> <ids>") appends tracker ids to the KNOWN lines.
#
# Mirror comparisons wait for an LSN, not a fixed time: commit a row to a mirrored fence table, poll
# mooncake.list_tables() until the fence mirror's commit_lsn reaches that commit (the replication
# stream is ordered, so every earlier commit has reached moonlink), then read the mirror from a fresh
# session. A fence that never arrives is reported as lag; a mismatch after it is wrong data. Reads
# never block inside moonlink waiting for an LSN, so one stuck mirror can't wedge the rest of the run.
# Read-your-writes waits are tested separately (ryw_* checks).
#
# Env: WORK (default ~/work), PG_PREFIX (default $WORK/pg/<major>), DUCKDB_EXTENSION_DIR,
#      E2E_DIR (clusters and outputs, default $WORK/e2e), BUILD_SRC (checkout the binaries were
#      built from; default the harness checkout), PGPORT (default 5400+major),
#      CONVERGE_SECS (fence timeout, 30), GRACE_SECS (apply grace after the fence, 5),
#      PSQL_TIMEOUT (per psql call, 60), KNOWN_BUGS, KNOWN_BUG_IDS,
#      WIRE_MOONLINK_RPC_REV and COMMUNITY_DUCKDB_MOONCAKE_REF (expected pins, see scenario build).
set -uo pipefail

preflight() {
  local missing=() c
  (( BASH_VERSINFO[0] >= 4 )) || missing+=("bash >= 4 (this is $BASH_VERSION)")
  for c in timeout pgrep diff find sed awk; do command -v "$c" > /dev/null || missing+=("$c"); done
  [[ $(date +%N 2>/dev/null) =~ ^[0-9]+$ ]] || missing+=("GNU date (date +%N)")
  [[ $(uname -s) == Linux ]] || missing+=("Linux (the moonlink bgworker is found by its process title)")
  if (( ${#missing[@]} > 0 )); then
    echo "e2e-smoke.sh: missing requirements:" >&2
    printf '  - %s\n' "${missing[@]}" >&2
    exit 2
  fi
}
preflight
# Several checks read Postgres messages (server log, psql errors), so they must be in English. The
# server side is set in new_cluster; here libpq/psql/initdb. LC_ALL would override LC_MESSAGES, and
# LANGUAGE is ignored once LC_MESSAGES is C.
if [[ -n ${LC_ALL:-} ]]; then export LANG=$LC_ALL; unset LC_ALL; fi
export LC_MESSAGES=C
unset LANGUAGE

MAJOR=${1:?pg major}; shift
ALL_SCENARIOS=(build main schema restart_after_drop persist moonlink_down ddl ddl_drop truncate big_txn_restart crash)
SCENARIOS=("$@"); [[ ${#SCENARIOS[@]} -eq 0 ]] && SCENARIOS=("${ALL_SCENARIOS[@]}")
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
WORK=${WORK:-$HOME/work}
PG_PREFIX=${PG_PREFIX:-$WORK/pg/$MAJOR}
BIN=$PG_PREFIX/bin
EXTDIR=${DUCKDB_EXTENSION_DIR:-$WORK/duckdb_extensions}
E2E_DIR=${E2E_DIR:-$WORK/e2e}
BUILD_SRC=${BUILD_SRC:-$SCRIPT_DIR/..}
CONVERGE_SECS=${CONVERGE_SECS:-30}
GRACE_SECS=${GRACE_SECS:-5}
PSQL_TIMEOUT=${PSQL_TIMEOUT:-60}
KNOWN_BUGS=${KNOWN_BUGS:-$SCRIPT_DIR/e2e-known-bugs.txt}
KNOWN_BUG_IDS=${KNOWN_BUG_IDS:-}
DB_USER=${USER:-$(id -un)}
export PGHOST=/tmp PGDATABASE=postgres PGPORT=${PGPORT:-$((5400 + MAJOR))}
SCEN=""; DATA=""; OUT=""; FENCE_SRC=""; FENCE_MC=""; FENCE_N=0; RESNAPSHOT_OK=0; ALLOWED_EXIT_LINES=""; WAIT_ERR=""

[[ -x $BIN/psql ]] || { echo "no psql in $BIN (set PG_PREFIX)" >&2; exit 2; }

# ---- known bugs ----------------------------------------------------------------------------------
# File lines: "bug <name> <description>", "check <scenario.check> <bug> <regex>" and
# "requires <scenario.check> <bug> <earlier scenario.check> <bug2>". The regex (bash ERE) is matched
# against each failure signature: the check's detail, or one line per distinct cause where a check
# reports several (panic locations, process exits). A "requires" line makes the check's rule for
# <bug> count only if the earlier check failed with <bug2>, for failures whose text alone doesn't
# identify the cause (a hung DROP, a missing fence mirror).
declare -A BUG_DESC=() KNOWN_RULES=() BUG_IDS=() RULE_REQUIRES=()
load_known_bugs() {
  local line kind a b rest other other_bug extra n=0
  if [[ -f $KNOWN_BUGS ]]; then
    while IFS= read -r line || [[ -n $line ]]; do
      n=$((n + 1))
      [[ $line =~ ^[[:space:]]*(#|$) ]] && continue
      read -r kind a b rest <<< "$line"
      case $kind in
        bug) BUG_DESC[$a]="$b${rest:+ $rest}" ;;
        check)
          if [[ -z $rest || -z ${BUG_DESC[$b]:-} ]]; then
            echo "$KNOWN_BUGS:$n: expected 'check <check> <declared bug> <regex>'" >&2; exit 2
          fi
          [[ "" =~ $rest ]]
          if (( $? == 2 )); then echo "$KNOWN_BUGS:$n: invalid regex: $rest" >&2; exit 2; fi
          KNOWN_RULES[$a]+="$b"$'\t'"$rest"$'\n' ;;
        requires)
          read -r kind a b other other_bug extra <<< "$line"
          if [[ -z $other_bug || -n $extra || -z ${BUG_DESC[$b]:-} || -z ${BUG_DESC[$other_bug]:-} ]]; then
            echo "$KNOWN_BUGS:$n: expected 'requires <check> <bug> <earlier check> <bug>'" >&2; exit 2
          fi
          RULE_REQUIRES[$a|$b]+="$other|$other_bug " ;;
        *) echo "$KNOWN_BUGS:$n: unknown entry '$kind'" >&2; exit 2 ;;
      esac
    done < "$KNOWN_BUGS"
  fi
  if [[ -f $KNOWN_BUG_IDS ]]; then
    while read -r a b; do [[ -z $a || $a == \#* ]] || BUG_IDS[$a]=$b; done < "$KNOWN_BUG_IDS"
  fi
}
load_known_bugs

# known_bugs_of <check>: every bug listed for the check, comma-separated.
known_bugs_of() {
  local bug re bugs=""
  while IFS=$'\t' read -r bug re; do
    [[ -n $bug && ,$bugs, != *",$bug,"* ]] && bugs+="${bugs:+,}$bug"
  done <<< "${KNOWN_RULES[$1]:-}"
  echo "$bugs"
}
# rule_requirements_met <check> <bug>: every "requires" for the check's <bug> rule is satisfied.
rule_requirements_met() {
  local req
  for req in ${RULE_REQUIRES[$1|$2]:-}; do
    [[ ,${MATCHED_OF[${req%|*}]:-}, == *",${req#*|},"* ]] || return 1
  done
}
# known_bugs_matching <check> <signatures>: the listed bugs that explain the failure, or nothing
# unless every signature line matches at least one of the check's bug regexes.
known_bugs_matching() {
  local rules=${KNOWN_RULES[$1]:-} sig bug re hit bugs=""
  [[ -n $rules ]] || return 0
  while IFS= read -r sig; do
    hit=""
    while IFS=$'\t' read -r bug re; do
      [[ -n $bug ]] && [[ $sig =~ $re ]] && rule_requirements_met "$1" "$bug" || continue
      hit=1
      [[ ,$bugs, == *",$bug,"* ]] || bugs+="${bugs:+,}$bug"
    done <<< "$rules"
    [[ -n $hit ]] || return 0
  done <<< "$2"
  echo "$bugs"
}

# ---- results -------------------------------------------------------------------------------------
declare -A STATUS_OF=() MATCHED_OF=()
declare -A COUNT=([pass]=0 [xpass]=0 [fail]=0 [xfail]=0 [skip]=0)
one_line() { tr '\n\t' '  ' | sed 's/ *$//' | cut -c1-"${1:-300}"; }
pg_alive() { "$BIN/pg_ctl" -D "$DATA" status > /dev/null 2>&1; }

# result <check> <0|1|skip> [detail] [signatures]: signatures (one per line, default the detail)
# decide whether a failure is a known bug; see the header.
result() {
  local check=$SCEN.$1 ok=$2 detail=${3:-} sigs=${4:-} all bugs status
  all=$(known_bugs_of "$check")
  if [[ $ok == 1 ]] && ! pg_alive; then ok=skip; detail="postgres not running; $detail"; fi
  case $ok in
    0)
      if [[ -n $all ]]; then status="XPASS($all)"; COUNT[xpass]=$((COUNT[xpass] + 1))
      else status=PASS; COUNT[pass]=$((COUNT[pass] + 1)); fi ;;
    skip) status=SKIP; COUNT[skip]=$((COUNT[skip] + 1)) ;;
    *)
      bugs=$(known_bugs_matching "$check" "${sigs:-$detail}")
      if [[ -n $bugs ]]; then
        status="XFAIL($bugs)"; MATCHED_OF[$check]=$bugs; COUNT[xfail]=$((COUNT[xfail] + 1))
      else
        status=FAIL; COUNT[fail]=$((COUNT[fail] + 1))
        [[ -z $all ]] || detail="not the known failure of $all: $detail"
      fi ;;
  esac
  STATUS_OF[$check]=${status%%(*}
  echo "RESULT $check $status${detail:+ $(echo "$detail" | one_line)}"
}
info() { echo "INFO $SCEN.$1 $(echo "${*:2}" | one_line)"; }

# ---- psql helpers --------------------------------------------------------------------------------
# Every psql call is time-limited: a scan waiting on a stuck mirror never returns on its own.
psqlq() { timeout -k 5 "$PSQL_TIMEOUT" "$BIN/psql" -X -v ON_ERROR_STOP=1 -qAt "$@"; }

# run <check> <psql args...>: PASS iff psql exits 0 (stdin is passed through for heredocs).
run() {
  local name=$1; shift
  if psqlq "$@" > "$OUT/$name.out" 2>&1; then result "$name" 0
  else result "$name" 1 "rc=$? $(head -3 "$OUT/$name.out")"; fi
}

# check_eq <check> <actual> <expected>
check_eq() {
  if [[ "$2" == "$3" ]]; then result "$1" 0; else result "$1" 1 "expected [$3] got [$(echo "$2" | head -3)]"; fi
}

# fence_wait <secs>: commit a row to the mirrored fence table, then wait (polling
# mooncake.list_tables(), never blocking inside a scan) until moonlink has applied that commit.
# The database's replication stream is ordered, so every earlier commit has then been handed to
# its table. Sets FENCE_WAL (a lower bound of the fence's commit LSN) and FENCE_ERR.
fence_present() {
  local out rc
  out=$(psqlq -c "SELECT commit_lsn FROM mooncake.list_tables() WHERE \"table\" = 'public.$FENCE_MC'" 2>&1); rc=$?
  if (( rc != 0 )); then FENCE_ERR="mooncake.list_tables() failed (rc=$rc): $(echo "$out" | one_line 160)"; return 1; fi
  [[ -n $out ]] || { FENCE_ERR="fence mirror $FENCE_MC missing from mooncake.list_tables()"; return 1; }
}
fence_wait() {
  local secs=$1 out
  FENCE_ERR=""; FENCE_WAL=""
  fence_present || return 1
  # Insert-only: UPDATEs of rows persisted before a restart exercise separate bugs. The insert LSN
  # read inside the fence transaction is a lower bound of its commit LSN, so a late earlier fence
  # commit can't satisfy the wait.
  if ! out=$(psqlq -c "BEGIN" -c "INSERT INTO $FENCE_SRC DEFAULT VALUES" -c "SELECT pg_current_wal_insert_lsn()" -c "COMMIT" 2>&1); then
    FENCE_ERR="fence write failed: $(echo "$out" | one_line 160)"; return 1
  fi
  FENCE_WAL=$out
  poll_until "$secs" sql_is "SELECT commit_lsn >= '$FENCE_WAL' FROM mooncake.list_tables() WHERE \"table\" = 'public.$FENCE_MC'" t ||
    { FENCE_ERR="moonlink did not apply the fence commit (WAL >= $FENCE_WAL) within ${secs}s: replication lag or stuck stream; moonlink panics so far: $(panics_inline)"; return 1; }
}

# fresh_fence <check>: switch to a new fence pair, so checks after a crash don't depend on the old
# fence mirror having survived it. Creating it is itself gated: create_table must work.
fresh_fence() {
  FENCE_N=$((FENCE_N + 1)); FENCE_SRC=e2e_fence_$FENCE_N; FENCE_MC=${FENCE_SRC}_mc
  run "$1" <<SQL
CREATE TABLE $FENCE_SRC (id bigserial PRIMARY KEY, at timestamptz DEFAULT now());
INSERT INTO $FENCE_SRC DEFAULT VALUES;
CALL mooncake.create_table('$FENCE_MC', '$FENCE_SRC');
SQL
}

# check_mirror_scalar <check> <sql> <expected>: fence, then a fresh-session read (no read-your-writes
# wait) polled for up to GRACE_SECS while the table applies the events it was handed.
check_mirror_scalar() {
  local got t0
  if ! fence_wait "$CONVERGE_SECS"; then result "$1" 1 "$FENCE_ERR"; return; fi
  t0=$(date +%s)
  while got=$(psqlq -c "$2" 2>&1); [[ $got != "$3" ]] && (( $(date +%s) - t0 < GRACE_SECS )); do sleep 0.3; done
  check_eq "$1" "$got" "$3"
}

# compare_core <name> <heap> <mirror> <cols> <secs>: sets CMP_OK (0/1) and CMP_DETAIL. A failure after
# the fence was applied is wrong data (or a failing mirror query), not lag.
compare_core() {
  local name=$1 heap=$2 mirror=$3 cols=$4 secs=$5 t0 rc ms
  local h=$OUT/$name.heap m=$OUT/$name.mirror
  CMP_OK=1
  if ! psqlq -c "SELECT $cols FROM $heap ORDER BY 1" > "$h" 2> "$h.err"; then
    CMP_DETAIL="heap query failed: $(one_line 200 < "$h.err")"; return
  fi
  t0=$(date +%s%N)
  if ! fence_wait "$secs"; then CMP_DETAIL=$FENCE_ERR; return; fi
  while :; do
    psqlq -c "SELECT $cols FROM $mirror ORDER BY 1" > "$m" 2> "$m.err"; rc=$?
    (( rc == 0 )) && diff -q "$h" "$m" > /dev/null && break
    (( ($(date +%s%N) - t0) / 1000000000 >= secs + GRACE_SECS )) && break
    sleep 0.3
  done
  ms=$(( ($(date +%s%N) - t0) / 1000000 ))
  if (( rc >= 124 )); then
    CMP_DETAIL="mirror query timed out after the fence (WAL $FENCE_WAL) was applied; moonlink panics so far: $(panics_inline)"
  elif (( rc != 0 )); then
    CMP_DETAIL="mirror query failed (rc=$rc): $(one_line 200 < "$m.err")"
  elif ! diff -q "$h" "$m" > /dev/null; then
    CMP_DETAIL="data mismatch after the fence (WAL $FENCE_WAL) was applied: $(diff "$h" "$m" | head -4 | cut -c1-120 | one_line)"
  else
    CMP_OK=0; CMP_DETAIL="$(wc -l < "$h") rows, fence WAL $FENCE_WAL, ${ms}ms"
  fi
}
compare() { compare_core "$1" "$2" "$3" "${4:-*}" "$CONVERGE_SECS"; result "$1" "$CMP_OK" "$CMP_DETAIL"; }
info_compare() {
  compare_core "$1" "$2" "$3" "${4:-*}" "${INFO_CONVERGE_SECS:-10}"
  info "$1" "$( ((CMP_OK == 0)) && echo equal || echo different): $CMP_DETAIL"
}

# ryw_cross_table <check>: in one session, commit to the fence table and then scan the (otherwise
# idle) probe mirror. The scan waits for that commit's LSN, as an application session would.
ryw_cross_table() {
  local want got rc mark
  want=$(psqlq -c "SELECT count(*) FROM e2e_probe" 2>&1); mark=$(panic_count)
  got=$(timeout -k 5 20 "$BIN/psql" -X -v ON_ERROR_STOP=1 -qAt -c "INSERT INTO $FENCE_SRC DEFAULT VALUES" \
    -c "SELECT count(*) FROM e2e_probe_mc" 2>&1); rc=$?
  # A commit to the probe table releases a scan still waiting there, so it can't wedge later DDL.
  psqlq -c "INSERT INTO e2e_probe DEFAULT VALUES" > /dev/null 2>&1
  if (( rc >= 124 )); then result "$1" 1 "read-your-writes scan of an idle mirror did not return within 20s; moonlink panics during the check: $(panics_inline "$mark")"
  elif (( rc != 0 )); then result "$1" 1 "$(echo "$got" | one_line 200); moonlink panics during the check: $(panics_inline "$mark")"
  else check_eq "$1" "$got" "$want"; fi
}

# poll_until <secs> <command...>: re-run until it succeeds (waits for asynchronous cleanup).
poll_until() {
  local secs=$1 t0; shift; t0=$(date +%s)
  until "$@"; do (( $(date +%s) - t0 >= secs )) && return 1; sleep 0.5; done
}
sql_is() { [[ "$(psqlq -c "$1" 2>&1)" == "$2" ]]; }

# ---- moonlink bgworker ---------------------------------------------------------------------------
# The moonlink bgworker never connects to a database, so it has no pg_stat_activity row on any PG
# version; find it by process title among the postmaster's children instead.
moonlink_pid() {
  local pm; pm=$(head -1 "$DATA/postmaster.pid" 2>/dev/null) || return 0
  [[ -n $pm ]] && pgrep -P "$pm" -f '^postgres: (.*: )?moonlink( |$)' | head -1
}
moonlink_up() { [[ -n $(moonlink_pid) ]]; }
moonlink_gone() { [[ -z $(moonlink_pid) ]]; }
moonlink_restarted() { local p; p=$(moonlink_pid); [[ -n $p && $p != "$1" ]]; }
# panic_lines: one line per "panicked at" line in the postgres log: "<path>:<line>" with registry and
# checkout prefixes stripped (so locations compare across machines), or "unparsed panic: <text>" for
# any other form, so that no panic is dropped and marks (panic_count) and sites count the same lines.
panic_lines() {
  grep 'panicked at' "$OUT/postgres.log" 2>/dev/null | sed -E '
    s/.*panicked at ([^ ]+):([0-9]+):[0-9]+:.*/\1:\2/
    t parsed
    s/.*panicked at (.{0,80}).*/unparsed panic: \1/
    b
    :parsed
    s#^.*/registry/src/[^/]+/##
    s#^.*/git/checkouts/[^/]+/[^/]+/##
    s#^/([^/]+/)*(moonlink/src/(moonlink|moonlink_[a-z_]+)/src/)#\2#'
}
panic_count() { panic_lines | wc -l; }
# panic_sites [n]: one "<site> x<count>" per distinct panic site, counting only panics after the first n.
panic_sites() {
  panic_lines | tail -n +$(( ${1:-0} + 1 )) | sort | uniq -c | awk '{c = $1; $1 = ""; print substr($0, 2) " x" c}'
}
# panics_inline [n]: panic sites after the first n on one line, or "none".
panics_inline() { local p; p=$(panic_sites "${1:-0}" | paste -sd, - | sed 's/,/, /g'); echo "${p:-none}"; }
# worker_exits: one "<how> x<count>" per kind of abnormal moonlink bgworker exit the postmaster logged,
# leaving out one exit logged within the postgres.log line range ALLOWED_EXIT_LINES ("<from> <to>").
worker_exits() {
  awk -v range="$ALLOWED_EXIT_LINES" '
    BEGIN { split(range, r, " ") }
    match($0, /background worker "moonlink" \(PID [0-9]+\) (exited with exit code [1-9][0-9]*|was terminated by signal [0-9]+)/) {
      how = substr($0, RSTART, RLENGTH); sub(/.*\) /, "", how)
      if (!allowed_used && r[1] != "" && NR > r[1] && NR <= r[2] && how ~ /^exited/) { allowed_used = 1; next }
      print how
    }' "$OUT/postgres.log" 2>/dev/null | sort | uniq -c | awk '{c = $1; $1 = ""; print substr($0, 2) " x" c}'
}
# crash_restarts: one "<process kind> terminated by signal <n> x<count>" per kind of Postgres process
# crash, plus "reinitializing x<count>" for each crash-restart of the whole server.
crash_restarts() {
  sed -nE 's/.*LOG: +(.*) \(PID [0-9]+\) was terminated by signal ([0-9]+).*/\1 terminated by signal \2/p;
           s/.*all server processes terminated; reinitializing.*/reinitializing/p' "$OUT/postgres.log" 2>/dev/null |
    sort | uniq -c | awk '{c = $1; $1 = ""; print substr($0, 2) " x" c}'
}

# no_crash_loop <check> [panic mark]: over a 20s window the bgworker pid is stable and
# mooncake.list_tables() still answers (the user's "never crash-loop" criterion). Failures name the
# panics after the mark, so a known bug can be told apart from a new cause.
no_crash_loop() {
  local p1 p2 c1 c2 lt mark=${2:-0}
  p1=$(moonlink_pid); c1=$(panic_count)
  sleep 20
  p2=$(moonlink_pid); c2=$(panic_count)
  lt=$(psqlq -c "SELECT count(*) FROM mooncake.list_tables()" 2>&1)
  if [[ -z $p1 || $p1 != "$p2" ]]; then
    result "$1" 1 "crash loop: bgworker pid [$p1] -> [$p2] in 20s; panics since restart: $(panics_inline "$mark"); list_tables: $(echo "$lt" | one_line 120)"
  elif [[ ! $lt =~ ^[0-9]+$ ]]; then
    result "$1" 1 "bgworker up (pid $p1) but list_tables failed; panics since restart: $(panics_inline "$mark"); list_tables: $(echo "$lt" | one_line 160)"
  else result "$1" 0 "pid $p1, panics $c1 -> $c2 in 20s"; fi
}

# wait_ready: mooncake.list_tables() answers within 60s; WAIT_ERR holds the last error otherwise.
wait_ready() {
  local t0; t0=$(date +%s)
  until WAIT_ERR=$(psqlq -c "SELECT count(*) FROM mooncake.list_tables()" 2>&1); do
    (( $(date +%s) - t0 >= 60 )) && return 1
    sleep 0.5
  done
  WAIT_ERR=""
}

# ---- cluster lifecycle ---------------------------------------------------------------------------
start() { "$BIN/pg_ctl" -D "$DATA" -l "$OUT/postgres.log" -w -t 60 start > /dev/null 2>&1; }
# stop <mode> [check]: with a check name, a stop that doesn't finish within 30s fails the check: a
# hung shutdown is a bug in itself (a service manager would SIGKILL Postgres), e.g. a backend blocked
# in a moonlink RPC ignores SIGTERM. The run then goes on after an immediate stop.
stop() {
  local mode=${1:-fast} check=${2:-}
  if ! pg_alive; then [[ -z $check ]] || result "$check" skip "postgres not running"; return 0; fi
  if "$BIN/pg_ctl" -D "$DATA" -m "$mode" -w -t 30 stop > /dev/null 2>&1; then
    [[ -z $check ]] || result "$check" 0
    return 0
  fi
  [[ -z $check ]] || result "$check" 1 "pg_ctl stop -m $mode did not finish within 30s (continuing after -m immediate)"
  "$BIN/pg_ctl" -D "$DATA" -m immediate -w stop > /dev/null 2>&1
}

new_cluster() {
  SCEN=$1; DATA=$E2E_DIR/data-$MAJOR-$SCEN; OUT=$E2E_DIR/out-$MAJOR-$SCEN
  FENCE_SRC=e2e_fence; FENCE_MC=e2e_fence_mc; FENCE_N=0; ALLOWED_EXIT_LINES=""
  [[ -f $DATA/postmaster.pid ]] && "$BIN/pg_ctl" -D "$DATA" -m immediate -w stop > /dev/null 2>&1
  rm -rf -- "$DATA" "$OUT"; mkdir -p "$OUT"
  "$BIN/initdb" -D "$DATA" -U "$DB_USER" > "$OUT/initdb.log" 2>&1 || { result initdb 1 "$(tail -2 "$OUT/initdb.log")"; return 1; }
  cat >> "$DATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '/tmp'
listen_addresses = ''
shared_preload_libraries = 'pg_duckdb,pg_mooncake'
wal_level = logical
duckdb.allow_community_extensions = true
duckdb.allow_unsigned_extensions = true
duckdb.autoinstall_known_extensions = false
duckdb.extension_directory = '$EXTDIR'
lc_messages = 'C'
EOF
  start || { result start 1 "$(tail -3 "$OUT/postgres.log")"; return 1; }
  run create_extension -c "CREATE EXTENSION pg_mooncake CASCADE"
  run fence_setup <<'SQL'
CREATE TABLE e2e_fence (id bigserial PRIMARY KEY, at timestamptz DEFAULT now());
INSERT INTO e2e_fence DEFAULT VALUES;
CALL mooncake.create_table('e2e_fence_mc', 'e2e_fence');
CREATE TABLE e2e_probe (id bigserial PRIMARY KEY, at timestamptz DEFAULT now());
INSERT INTO e2e_probe DEFAULT VALUES;
CALL mooncake.create_table('e2e_probe_mc', 'e2e_probe');
SQL
}

end_cluster() {
  local sites exits crashes
  sites=$(panic_sites)
  if [[ -n $sites ]]; then
    result no_moonlink_panics 1 "$(panic_count) panic(s) at: $(echo "$sites" | paste -sd, - | sed 's/,/, /g')" "$sites"
  else result no_moonlink_panics 0; fi
  # Catches crash loops slower than no_crash_loop's 20s sample: every abnormal exit is logged.
  exits=$(worker_exits)
  if [[ -n $exits ]]; then
    result bgworker_no_abnormal_exits 1 "moonlink bgworker $(echo "$exits" | paste -sd, - | sed 's/,/, /g')" "$exits"
  else result bgworker_no_abnormal_exits 0; fi
  # A crashed backend makes the postmaster restart every process; it then stays up, so check the log.
  crashes=$(crash_restarts)
  if [[ -n $crashes ]]; then
    result no_postgres_crash_restarts 1 "$(echo "$crashes" | paste -sd, - | sed 's/,/, /g')" "$crashes"
  else result no_postgres_crash_restarts 0; fi
  if pg_alive; then result postgres_alive 0; else result postgres_alive skip "postgres died during the scenario"; fi
  stop fast clean_stop_at_end
}

# ---- build provenance ----------------------------------------------------------------------------
sha256() { if command -v sha256sum > /dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
build_info() {
  local libdir f
  local dirty=""
  [[ -n $(git -C "$SCRIPT_DIR" status --porcelain -- . 2>/dev/null) ]] && dirty=" (scripts/ has uncommitted changes)"
  echo "BUILD harness_commit=$(git -C "$SCRIPT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)$dirty"
  local sub pin head
  echo "BUILD build_src=$BUILD_SRC commit=$(git -C "$BUILD_SRC" rev-parse HEAD 2>/dev/null || echo unknown)$([[ -n $(git -C "$BUILD_SRC" status --porcelain --untracked-files=no 2>/dev/null) ]] && echo ' (uncommitted changes)')"
  for sub in moonlink duckdb_mooncake pg_duckdb; do
    pin=$(git -C "$BUILD_SRC" ls-tree HEAD "$sub" 2>/dev/null | awk '{print $3}')
    head=$(git -C "$BUILD_SRC/$sub" rev-parse HEAD 2>/dev/null)
    echo "BUILD checkout_${sub}_commit=${head:-unknown}${pin:+ pinned=$pin}$([[ -n $pin && $pin != "$head" ]] && echo ' (differs from pin)')"
  done
  echo "BUILD pg_version=$("$BIN/postgres" --version | one_line)"
  echo "BUILD pg_cassert=$("$BIN/pg_config" --configure | grep -q enable-cassert && echo yes || echo no)"
  libdir=$("$BIN/pg_config" --pkglibdir)
  for f in pg_mooncake pg_duckdb; do
    if [[ -f $libdir/$f.so ]]; then
      echo "BUILD ${f}_so_sha256=$(sha256 "$libdir/$f.so") mtime=$(date -u -r "$libdir/$f.so" +%Y-%m-%dT%H:%M:%SZ)"
    else echo "BUILD ${f}_so=missing"; fi
  done
  while IFS= read -r f; do
    echo "BUILD duckdb_mooncake_file=$f"
    echo "BUILD duckdb_mooncake_sha256=$(sha256 "$f")"
    # DuckDB extension footer: zero-padded metadata fields (platform, DuckDB version, extension
    # version, ABI) ahead of the 256-byte signature.
    echo "BUILD duckdb_mooncake_footer=$(tail -c 512 "$f" | head -c 256 | tr -c '[:print:]' '\n' | grep -v '^$' | paste -sd, -)"
    [[ -f $f.info ]] && echo "BUILD duckdb_mooncake_install_info=present (installed by DuckDB INSTALL)"
    [[ -f $f.build-info ]] && sed 's/^/BUILD duckdb_mooncake_/' "$f.build-info"
  done < <(find "$EXTDIR" -name mooncake.duckdb_extension 2>/dev/null | sort)
}

# ================================================================================================
# What runs, and whether DuckDB loads a duckdb_mooncake that speaks the same wire protocol as the one
# users install: the DuckDB community extension for DuckDB v1.4.1, built from
# COMMUNITY_DUCKDB_MOONCAKE_REF with moonlink_rpc at WIRE_MOONLINK_RPC_REV and no version check.
WIRE_MOONLINK_RPC_REV=${WIRE_MOONLINK_RPC_REV:-c1a99ef01af878a5ffa77d1019ef4833a7907410}
COMMUNITY_DUCKDB_MOONCAKE_REF=${COMMUNITY_DUCKDB_MOONCAKE_REF:-bd9e5f3ade4ae41d5d13d9cfd4b37b09b39996ab}
scenario_build() {
  new_cluster build || return
  local r ver="" path="" pin head lock changes rc sha info
  local dm=$BUILD_SRC/duckdb_mooncake
  r=$(psqlq -c "SELECT r['extension_version']::text || ' ' || r['install_path']::text FROM duckdb.query(\$\$ SELECT extension_version, install_path FROM duckdb_extensions() WHERE extension_name = 'mooncake' AND loaded \$\$) r" 2>&1)
  echo "BUILD duckdb_mooncake_loaded=$(echo "$r" | one_line 200)"
  echo "BUILD pg_mooncake_extversion=$(psqlq -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_mooncake'" 2>&1 | one_line 80)"
  if [[ $r =~ ^([^ ]+)\ (/.+)$ ]]; then
    ver=${BASH_REMATCH[1]}; path=${BASH_REMATCH[2]}
    result duckdb_mooncake_loaded 0 "$ver from $path"
  else result duckdb_mooncake_loaded 1 "$(echo "$r" | one_line 200)"; fi

  pin=$(git -C "$BUILD_SRC" ls-tree HEAD duckdb_mooncake 2>/dev/null | awk '{print $3}')
  if [[ $pin == "$COMMUNITY_DUCKDB_MOONCAKE_REF" ]]; then result duckdb_mooncake_pin_is_community_build 0
  else result duckdb_mooncake_pin_is_community_build 1 "pin [$pin], community build for DuckDB v1.4.1 is $COMMUNITY_DUCKDB_MOONCAKE_REF"; fi
  # The next checks read the duckdb_mooncake checkout. An uninitialized submodule would resolve to
  # the superproject, so it is detected and the checks are skipped with a hint.
  if [[ $(git -C "$dm" rev-parse --show-toplevel 2>/dev/null) != "$(cd "$dm" 2>/dev/null && pwd -P)" ]]; then
    local hint="duckdb_mooncake submodule not checked out in $BUILD_SRC; run: git -C $BUILD_SRC submodule update --init duckdb_mooncake"
    result duckdb_mooncake_is_pinned_commit skip "$hint"
    result duckdb_mooncake_moonlink_rpc_rev skip "$hint"
    result duckdb_mooncake_checkout_unmodified skip "$hint"
  else
    head=$(git -C "$dm" rev-parse HEAD 2>/dev/null)
    if [[ -n $ver && -n $pin && $pin == "$ver"* && $head == "$pin" ]]; then
      result duckdb_mooncake_is_pinned_commit 0 "$ver = submodule pin"
    else result duckdb_mooncake_is_pinned_commit 1 "loaded [$ver], submodule pin [$pin], checked out [$head] in $dm"; fi
    lock=$(grep -A2 '^name = "moonlink_rpc"$' "$dm/moonlink_ffi/Cargo.lock" 2>/dev/null | sed -n 's/^source = "\(.*\)"$/\1/p')
    if [[ $lock == *"#$WIRE_MOONLINK_RPC_REV" ]]; then result duckdb_mooncake_moonlink_rpc_rev 0 "$lock"
    else result duckdb_mooncake_moonlink_rpc_rev 1 "moonlink_ffi/Cargo.lock moonlink_rpc source [$lock], expected ...#$WIRE_MOONLINK_RPC_REV"; fi
    # Local edits (cargo update, a path override for moonlink_rpc, ...) don't change the version string.
    changes=$(git -C "$dm" status --porcelain --untracked-files=no 2>&1); rc=$?
    if (( rc == 0 )) && [[ -z $changes ]]; then result duckdb_mooncake_checkout_unmodified 0
    else result duckdb_mooncake_checkout_unmodified 1 "rc=$rc $(echo "$changes" | one_line 200)"; fi
  fi
  # The checks above see today's checkout; a build-info records the checkout the binary was built from.
  if [[ -n $path && -f $path.build-info ]]; then
    sha=$(sha256 "$path"); info=$(sed -n 's/^sha256=//p' "$path.build-info")
    if [[ $sha == "$info" ]]; then result duckdb_mooncake_build_info_current 0
    else result duckdb_mooncake_build_info_current 1 "stale: build-info says sha256 $info, loaded file is $sha"; fi
    local b_commit b_dirty b_lock
    b_commit=$(sed -n 's/^duckdb_mooncake_commit=//p' "$path.build-info")
    b_dirty=$(sed -n 's/^duckdb_mooncake_dirty=//p' "$path.build-info")
    b_lock=$(sed -n 's/^moonlink_rpc_lock=//p' "$path.build-info")
    if [[ $b_commit == "$pin" && $b_dirty == no && $b_lock == *"#$WIRE_MOONLINK_RPC_REV" ]]; then
      result duckdb_mooncake_built_from_pins 0
    else
      result duckdb_mooncake_built_from_pins 1 "built from commit [$b_commit] (pin [$pin]), dirty [$b_dirty], moonlink_rpc [$b_lock] (expected #$WIRE_MOONLINK_RPC_REV)"
    fi
  else
    info duckdb_mooncake_build_info "none next to ${path:-the loaded extension}: the loaded binary's build provenance is unverified"
  fi
  end_cluster
}

# ---- type matrix (scenarios main and persist) ----------------------------------------------------
# Each entry: "<type>|<comma-separated SQL literals>"; NULL is always added.
TYPES=(
  "smallint|-32768, 0, 32767" "integer|-2147483648, 42" "bigint|-9223372036854775808, 9223372036854775807"
  "real|1.5, -0.25" "double precision|3.141592653589793, -1e300" "numeric(20,5)|123456789012345.12345, -0.00001"
  "numeric|1.23456789012345678901234567890" "boolean|true, false" "text|'', 'hello', 'ünïcødé ✓'"
  "varchar(10)|'abc'" "char(3)|'ab'" "bytea|'\\xdeadbeef'::bytea, ''::bytea" "date|'2024-02-29', '1970-01-01'"
  "time|'23:59:59.999999'" "timestamp|'2024-06-05 10:00:00.123456'" "timestamptz|'2024-06-05 10:00:00+02'"
  "interval|'1 year 2 days 03:04:05'" "uuid|'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'" "json|'{\"a\":1}'"
  "jsonb|'{\"a\":[1,2]}'" "integer[]|'{1,2,3}'" "text[]|'{a,b}'" "inet|'10.0.0.1/32'"
)
# type_check_name <type>: "double precision" -> double_precision, "integer[]" -> integer_array.
type_check_name() { echo "${1//\[\]/_array}" | tr -c 'a-z0-9_\n' '_' | sed 's/_*$//'; }
# type_cols <type>: columns to compare; DuckDB renders json compactly, PG jsonb::text adds spaces.
type_cols() { if [[ $1 == json* ]]; then echo "id, replace(c::text, ' ', '')"; else echo '*'; fi; }
# create_type_table <index>: source ty_<i> with the entry's values + NULL, mirrored as ty_<i>_mc.
create_type_table() {
  local ty=${TYPES[$1]%%|*} vals=${TYPES[$1]#*|}
  psqlq > "$OUT/type_$1.out" 2>&1 <<SQL
CREATE TABLE ty_$1 (id serial PRIMARY KEY, c $ty);
CALL mooncake.create_table('ty_$1_mc', 'ty_$1');
INSERT INTO ty_$1 (c) SELECT x::$ty FROM (VALUES ($(echo "$vals" | sed 's/, /), (/g')), (NULL)) v(x);
SQL
}

# ================================================================================================
scenario_main() {
  new_cluster main || return
  local p; p=$(moonlink_pid)
  if [[ -n $p ]]; then result bgworker_detected 0 "pid $p"; else result bgworker_detected 1 "no 'postgres: moonlink' child of the postmaster"; fi

  # ---- README quickstart -----------------------------------------------------------------------
  run quickstart_setup <<'SQL'
CREATE TABLE trades(id bigint PRIMARY KEY, symbol text, time timestamp, price real);
CALL mooncake.create_table('trades_iceberg', 'trades');
INSERT INTO trades VALUES
  (1,  'AMD', '2024-06-05 10:00:00', 119),
  (2, 'AMZN', '2024-06-05 10:05:00', 207),
  (3, 'AAPL', '2024-06-05 10:10:00', 203),
  (4, 'AMZN', '2024-06-05 10:15:00', 210);
SQL
  check_mirror_scalar quickstart_avg "SELECT avg(price) FROM trades_iceberg WHERE symbol = 'AMZN'" "208.5"

  # ---- CDC bursts (small transactions) ---------------------------------------------------------
  run cdc_setup <<'SQL'
CREATE TABLE t (id bigint PRIMARY KEY, v text, n int, ts timestamptz DEFAULT now(), j jsonb, d numeric(12,3));
CALL mooncake.create_table('t_mc', 't');
SQL
  local b
  {
    for b in $(seq 0 9); do
      echo "INSERT INTO t (id, v, n, j, d) SELECT g, md5(g::text), g % 97, jsonb_build_object('g', g), g / 7.0 FROM generate_series($((b * 1000)), $((b * 1000 + 999))) g;"
    done
    echo "UPDATE t SET v = v || '-u', n = n + 1 WHERE id % 5 = 0;"
    echo "DELETE FROM t WHERE id % 11 = 0;"
    echo "BEGIN; INSERT INTO t (id, v, n) VALUES (100001, 'x', 1); UPDATE t SET n = -1 WHERE id = 100001; DELETE FROM t WHERE id = 3; COMMIT;"
    echo "BEGIN; INSERT INTO t (id, v, n) VALUES (100002, 'rolled back', 1); ROLLBACK;"
  } > "$OUT/cdc_workload.sql"
  run cdc_workload -f "$OUT/cdc_workload.sql"
  # Same-session read-your-writes (the scan waits for XactLastCommitEnd).
  check_eq ryw_same_session "$(psqlq -c "INSERT INTO t (id, v, n) VALUES (100003, 'ryw', 7)" -c "SELECT count(*) FROM t_mc WHERE id = 100003" 2>&1)" "1"
  check_eq cdc_heap_rows "$(psqlq -c "SELECT count(*) FROM t" 2>&1)" "9091"
  # DuckDB renders json compactly ({"g":1}) while PG jsonb::text adds spaces; compare without spaces.
  local COLS="id, v, n, ts, replace(j::text, ' ', ''), d"
  compare cdc_bursts t t_mc "$COLS"

  # ---- Cross-table read-your-writes (positive control for the after-restart check) ------------
  ryw_cross_table ryw_cross_table

  # ---- TOAST -----------------------------------------------------------------------------------
  # toast_values: large values that the UPDATE itself rewrites (the new value travels in the WAL).
  run toast_setup <<'SQL'
CREATE TABLE big_vals (id int PRIMARY KEY, v text, b bytea);
CALL mooncake.create_table('big_vals_mc', 'big_vals');
INSERT INTO big_vals SELECT g, repeat(md5(g::text), 32768), decode(repeat('ab', 100000), 'hex') FROM generate_series(1, 20) g;
INSERT INTO big_vals VALUES (21, NULL, NULL);
UPDATE big_vals SET v = v || 'x' WHERE id % 2 = 0;
SQL
  compare toast_values big_vals big_vals_mc "id, md5(v), length(v), octet_length(b)"
  # toast_unchanged_update: UPDATE of a small column while the large ones stay out of line; pgoutput
  # then sends the large columns as "unchanged TOAST" and the mirror must keep their values.
  run toast_unchanged_setup <<'SQL'
CREATE TABLE toast_u (id int PRIMARY KEY, n int, t text, b bytea, j jsonb);
ALTER TABLE toast_u ALTER COLUMN t SET STORAGE EXTERNAL, ALTER COLUMN b SET STORAGE EXTERNAL,
  ALTER COLUMN j SET STORAGE EXTERNAL;
CALL mooncake.create_table('toast_u_mc', 'toast_u');
INSERT INTO toast_u
SELECT g, 0, s, decode(s, 'hex'), to_jsonb(ARRAY[s])
FROM generate_series(1, 20) g,
     LATERAL (SELECT string_agg(md5(g::text || ':' || x::text), '') AS s FROM generate_series(1, 200) x) l;
SQL
  # Precondition: every large value really is stored out of line (else the test proves nothing).
  local toastrel
  toastrel=$(psqlq -c "SELECT reltoastrelid::regclass FROM pg_class WHERE oid = 'toast_u'::regclass" 2>&1)
  check_eq toast_unchanged_precondition "$(psqlq -c "SELECT count(DISTINCT chunk_id) FROM $toastrel" 2>&1)" "60"
  run toast_unchanged_workload -c "UPDATE toast_u SET n = n + 1 WHERE id % 2 = 0"
  compare toast_unchanged_update toast_u toast_u_mc "id, n, md5(t), b, replace(j::text, ' ', '')"

  # ---- RPC-backed procedures -------------------------------------------------------------------
  run create_snapshot -c "CALL mooncake.create_snapshot('t_mc')"
  local mode
  for mode in data index full; do run "optimize_$mode" -c "CALL mooncake.optimize_table('t_mc', '$mode')"; done
  run optimize_bad_mode_errors -c "DO \$\$ BEGIN CALL mooncake.optimize_table('t_mc', 'bogus'); RAISE EXCEPTION 'no error'; EXCEPTION WHEN OTHERS THEN IF SQLERRM = 'no error' THEN RAISE; END IF; END \$\$"
  compare cdc_after_optimize t t_mc "$COLS"
  check_eq list_tables_has_t_mc "$(psqlq -c "SELECT count(*) FROM mooncake.list_tables() WHERE \"table\" = 'public.t_mc'" 2>&1)" "1"
  psqlq -c "SELECT * FROM mooncake.list_tables()" > "$OUT/list_tables.txt" 2>&1

  # ---- Clean restart (small txns only) ---------------------------------------------------------
  stop fast clean_stop_before_restart; start
  if wait_ready; then result restart_clean_ready 0; else result restart_clean_ready 1 "mooncake.list_tables() not answering within 60s"; fi
  # Also the positive control for the crash-loop probe used by the restart/crash gate.
  no_crash_loop no_crash_loop_after_clean_restart
  ryw_cross_table ryw_cross_table_after_restart
  compare after_clean_restart t t_mc "$COLS"
  run cdc_post_restart_workload -c "INSERT INTO t (id, v, n) SELECT g, 'post-restart', 2 FROM generate_series(600000, 600999) g" \
    -c "UPDATE t SET n = 42 WHERE id BETWEEN 1 AND 500"
  compare cdc_after_clean_restart t t_mc "$COLS"
  compare toast_after_clean_restart big_vals big_vals_mc "id, md5(v), length(v), octet_length(b)"

  # ---- Drop + cleanup --------------------------------------------------------------------------
  run drop_mirror -c "DROP TABLE t_mc"
  check_eq drop_list_tables "$(psqlq -c "SELECT count(*) FROM mooncake.list_tables() WHERE \"table\" = 'public.t_mc'" 2>&1)" "0"
  if poll_until 30 no_files_matching '*t_mc*'; then result drop_files_removed 0
  else result drop_files_removed 1 "after 30s: $(find "$DATA/pg_mooncake" -path '*t_mc*' | head -5 | one_line)"; fi
  local pubq="SELECT count(*) FROM pg_publication_tables WHERE schemaname = 'public' AND tablename = 't'"
  if poll_until 30 sql_is "$pubq" 0; then result drop_source_removed_from_publication 0
  else result drop_source_removed_from_publication 1 "count=$(psqlq -c "$pubq" 2>&1) after 30s"; fi
  run drop_source_with_mirror -c "DROP TABLE trades"
  check_mirror_scalar mirror_survives_source_drop "SELECT count(*) FROM trades_iceberg" "4"
  # Drop every remaining mirror (incl. the fence); the database should then hold no slot or
  # publication (an orphaned inactive logical slot retains WAL forever).
  local m rc mirrors failed=""
  mirrors=$(psqlq -c "SELECT c.oid::regclass FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE a.amname = 'mooncake'" 2>&1); rc=$?
  if (( rc != 0 )) || [[ -z $mirrors ]]; then
    # The fence and probe mirrors always exist here, so an empty list is an error too.
    result drop_all_mirrors 1 "listing mirrors failed (rc=$rc): $(echo "$mirrors" | one_line 160)"
  else
    for m in $mirrors; do
      psqlq -c "DROP TABLE $m" > "$OUT/drop_all.out" 2>&1; rc=$?
      (( rc == 0 )) && continue
      failed+="$m (rc=$rc): $(head -1 "$OUT/drop_all.out"); "
      (( rc >= 124 )) && { failed+="stopped after a hung DROP"; break; }
    done
    if [[ -z $failed ]]; then result drop_all_mirrors 0; else result drop_all_mirrors 1 "$failed"; fi
  fi
  check_eq drop_all_list_tables "$(psqlq -c "SELECT count(*) FROM mooncake.list_tables()" 2>&1)" "0"
  if poll_until 30 sql_is "SELECT count(*) FROM pg_publication_tables" 0; then result drop_all_publication_tables 0
  else result drop_all_publication_tables 1 "$(psqlq -c "SELECT schemaname || '.' || tablename FROM pg_publication_tables" 2>&1 | one_line)"; fi
  if poll_until 30 sql_is "SELECT count(*) FROM pg_replication_slots" 0; then result drop_all_slots 0
  else result drop_all_slots 1 "$(psqlq -c "SELECT slot_name || ' active=' || active FROM pg_replication_slots" 2>&1 | one_line)"; fi
  find "$DATA/pg_mooncake" -maxdepth 3 > "$OUT/pg_mooncake_tree_after_drop_all.txt" 2>&1
  if poll_until 30 no_leftover_files; then result drop_all_files_removed 0
  else result drop_all_files_removed 1 "$(leftover_files | head -5 | one_line)"; fi

  end_cluster
}
no_files_matching() { [[ -z $(find "$DATA/pg_mooncake" -path "$1" 2>/dev/null | head -1) ]]; }
leftover_files() {
  find "$DATA/pg_mooncake" -mindepth 1 -maxdepth 2 -not -name moonlink.sock -not -name 'moonlink_metadata_store.sqlite*' \
    -not -path '*/temp' -not -path '*/read_through_cache' -not -path '*/_wal' -not -path '*/postgres' -not -name indicator.text 2>/dev/null
}
no_leftover_files() { [[ -z $(leftover_files | head -1) ]]; }

# ================================================================================================
# Types and identifiers: each case mirrors its own table and is compared after an LSN fence.
scenario_schema() {
  new_cluster schema || return
  # ---- Wide types / NULLs: one table per type --------------------------------------------------
  local i ty cname
  for i in "${!TYPES[@]}"; do
    ty=${TYPES[$i]%%|*}; cname=type.$(type_check_name "$ty")
    if ! create_type_table "$i"; then result "$cname" 1 "setup: $(head -2 "$OUT/type_$i.out")"; continue; fi
    compare "$cname" "ty_$i" "ty_${i}_mc" "$(type_cols "$ty")"
  done

  # ---- Identifiers -----------------------------------------------------------------------------
  # Postgres identifier semantics end to end: quoted names keep case, spaces and embedded quotes;
  # unquoted names fold to lower case; moonlink and list_tables use the unquoted schema.table text.
  run quoted_setup <<'SQL'
CREATE SCHEMA "Mixed Schema";
CREATE TABLE "Mixed Schema"."Src Table" (id int PRIMARY KEY, "Val" text);
CALL mooncake.create_table('"Mixed Schema"."Mirror ""Q"""', '"Mixed Schema"."Src Table"');
INSERT INTO "Mixed Schema"."Src Table" VALUES (1, 'a'), (2, 'b'), (3, 'c');
DELETE FROM "Mixed Schema"."Src Table" WHERE id = 1;
SQL
  compare quoted_identifiers '"Mixed Schema"."Src Table"' '"Mixed Schema"."Mirror ""Q"""'
  check_eq quoted_list_tables_name "$(psqlq -c "SELECT \"table\" FROM mooncake.list_tables() WHERE \"table\" LIKE 'Mixed Schema.%'" 2>&1)" 'Mixed Schema.Mirror "Q"'
  # A name that needs quoting but is valid unquoted ($), given in mixed case: folds to mc$1 / src$1.
  run dollar_ident_setup <<'SQL'
CREATE TABLE src$1 (id int PRIMARY KEY, v text);
CALL mooncake.create_table('Mc$1', 'SRC$1');
INSERT INTO src$1 VALUES (1, 'a'), (2, 'b');
SQL
  compare dollar_identifier 'src$1' 'mc$1'
  check_eq dollar_list_tables_name "$(psqlq -c "SELECT \"table\" FROM mooncake.list_tables() WHERE \"table\" LIKE '%\$1'" 2>&1)" 'public.mc$1'
  run identifiers_drop -c 'DROP TABLE IF EXISTS "Mixed Schema"."Mirror ""Q""", mc$1'
  check_eq identifiers_removed_from_list_tables "$(psqlq -c "SELECT count(*) FROM mooncake.list_tables() WHERE \"table\" LIKE 'Mixed Schema.%' OR \"table\" LIKE '%\$1'" 2>&1)" "0"
  run other_schema_setup <<'SQL'
CREATE SCHEMA s2;
CREATE TABLE s2.src (id int PRIMARY KEY, v text);
CALL mooncake.create_table('s2.mirror', 's2.src');
INSERT INTO s2.src VALUES (1, 'a');
SQL
  compare non_public_schema s2.src s2.mirror
  end_cluster
}

# Clean restart after other mirrors of the same database were created and dropped: the surviving
# mirror must keep its rows and keep ingesting.
scenario_restart_after_drop() {
  new_cluster restart_after_drop || return
  run setup <<'SQL'
CREATE TABLE k (id bigint PRIMARY KEY, v text);
CALL mooncake.create_table('k_mc', 'k');
INSERT INTO k SELECT g, md5(g::text) FROM generate_series(1, 5000) g;
CREATE TABLE x1 (id int PRIMARY KEY, v text);
CALL mooncake.create_table('x1_mc', 'x1');
INSERT INTO x1 VALUES (1, 'a');
CREATE TABLE x2 (id int PRIMARY KEY, v text);
CALL mooncake.create_table('x2_mc', 'x2');
INSERT INTO x2 VALUES (1, 'a');
SQL
  compare before_restart k k_mc
  run drop_others -c "DROP TABLE x1_mc" -c "DROP TABLE x2_mc"
  compare after_drop_others k k_mc
  stop fast clean_stop_before_restart; start
  if wait_ready; then result restart_ready 0; else result restart_ready 1 "mooncake.list_tables() not answering within 60s"; fi
  compare survivor_after_restart k k_mc
  run cdc_workload -c "UPDATE k SET v = 'u' WHERE id % 10 = 0" -c "DELETE FROM k WHERE id % 7 = 0" -c "INSERT INTO k VALUES (100000, 'new')"
  compare survivor_cdc_after_restart k k_mc
  end_cluster
}

# ================================================================================================
# Iceberg persistence per type: create every mirror first, then force a snapshot of each. Snapshots
# only take shared locks in moonlink, so one that hangs can't block the others.
scenario_persist() {
  new_cluster persist || return
  local i ty out rc c0 c1
  declare -A created=()
  for i in "${!TYPES[@]}"; do
    if create_type_table "$i"; then created[$i]=1
    else result "type.$(type_check_name "${TYPES[$i]%%|*}")" 1 "setup: $(head -2 "$OUT/type_$i.out")"; fi
  done
  for i in "${!TYPES[@]}"; do
    [[ -n ${created[$i]:-} ]] || continue
    ty=${TYPES[$i]%%|*}; c0=$(panic_count)
    # Same session: create_snapshot waits for an Iceberg snapshot covering this session's last commit.
    out=$(psqlq -c "INSERT INTO ty_$i (c) VALUES (NULL)" -c "CALL mooncake.create_snapshot('ty_${i}_mc')" 2>&1); rc=$?
    c1=$(panic_count)
    if (( rc == 0 && c0 == c1 )); then result "type.$(type_check_name "$ty")" 0
    elif (( rc >= 124 )); then result "type.$(type_check_name "$ty")" 1 "create_snapshot did not return within ${PSQL_TIMEOUT}s; panics during it: $(panics_inline "$c0")"
    else result "type.$(type_check_name "$ty")" 1 "rc=$rc, panics during it: $(panics_inline "$c0"): $(echo "$out" | one_line 160)"; fi
  done
  for i in "${!TYPES[@]}"; do
    [[ -n ${created[$i]:-} ]] || continue
    ty=${TYPES[$i]%%|*}
    compare "type.$(type_check_name "$ty").after_snapshot" "ty_$i" "ty_${i}_mc" "$(type_cols "$ty")"
  done
  # After a restart moonlink rebuilds every mirror from its persisted state.
  stop fast clean_stop_before_restart; start
  if wait_ready; then result restart_ready 0; else result restart_ready 1 "mooncake.list_tables() not answering within 60s"; fi
  for i in "${!TYPES[@]}"; do
    [[ -n ${created[$i]:-} ]] || continue
    ty=${TYPES[$i]%%|*}
    compare "type.$(type_check_name "$ty").after_restart" "ty_$i" "ty_${i}_mc" "$(type_cols "$ty")"
  done
  end_cluster
}

# ================================================================================================
# moonlink unhealthy: what Postgres sessions see when the bgworker errors, dies or hangs. Mirrors
# must always be droppable ("drop and re-create" is the user's recovery path), and errors must be
# ordinary ERRORs that leave the session usable.

# raw_psql <secs> <out> <err> <psql args...>: psql without ON_ERROR_STOP, so later statements still
# run after an error; returns psql's (or timeout's) exit code.
raw_psql() {
  local secs=$1 out=$2 err=$3; shift 3
  timeout -k 5 "$secs" "$BIN/psql" -X -qAt -v ON_ERROR_STOP=0 "$@" > "$out" 2> "$err"
}

scenario_moonlink_down() {
  new_cluster moonlink_down || return
  run setup <<'SQL'
CREATE TABLE h (id int PRIMARY KEY);
INSERT INTO h VALUES (1);
CREATE TABLE s (id int PRIMARY KEY, v text);
INSERT INTO s VALUES (1, 'a');
CALL mooncake.create_table('s_mc', 's');
CREATE TABLE s2 (id int PRIMARY KEY, v text);
CALL mooncake.create_table('s2_mc', 's2');
CREATE TABLE s3 (id int PRIMARY KEY, v text);
CALL mooncake.create_table('s3_mc', 's3');
SQL
  local o=$OUT/down rc errs pid term_pid="" t0 secs log_before

  # An RPC error must not break later mooncake calls (or DROP) in the same session.
  raw_psql 60 "$o.ss.out" "$o.ss.err" -c "CALL mooncake.optimize_table('s2_mc', 'bogus')" \
    -c "DROP TABLE s2_mc" -c "SELECT 'dropped=' || (to_regclass('s2_mc') IS NULL)"; rc=$?
  errs=$(grep -c 'ERROR:' "$o.ss.err")
  if (( rc == 0 )) && [[ $errs == 1 ]] && grep -qx 'dropped=true' "$o.ss.out"; then result rpc_error_then_drop_same_session 0
  else
    result rpc_error_then_drop_same_session 1 "rc=$rc errors=$errs, last error: $(grep 'ERROR:' "$o.ss.err" | tail -1 | cut -c1-200); output: $(one_line 40 < "$o.ss.out")"
  fi

  # Stale stream: the session's connection dies with moonlink. Its next call must detect that and
  # reconnect (reporting that moonlink is unreachable) instead of failing on the dead connection, and
  # so must the call after it.
  pid=$(moonlink_pid); term_pid=$pid; log_before=$(wc -l < "$OUT/postgres.log")
  if [[ -z $pid ]]; then
    result stale_stream_first_call_reconnects skip "no moonlink bgworker found"; result stale_stream_reconnects skip "no moonlink bgworker found"
  else
    raw_psql 60 "$o.st.out" "$o.st.err" <<SQL
SELECT count(*) FROM mooncake.list_tables();
\! kill -TERM $pid; sleep 2
SELECT count(*) FROM mooncake.list_tables();
SELECT count(*) FROM mooncake.list_tables();
SQL
    local e1 e2
    e1=$(grep 'ERROR:' "$o.st.err" | sed -n 1p); e2=$(grep 'ERROR:' "$o.st.err" | sed -n 2p)
    if [[ ! $(head -1 "$o.st.out") =~ ^[0-9]+$ ]]; then
      result stale_stream_first_call_reconnects 1 "list_tables failed before moonlink was stopped: [$(head -1 "$o.st.out")] $(echo "$e1" | one_line 160)"
    elif [[ $e1 =~ [Ff]ailed\ to\ connect ]]; then result stale_stream_first_call_reconnects 0 "$e1"
    else result stale_stream_first_call_reconnects 1 "first call after moonlink stopped: [$e1]"; fi
    if [[ $e2 =~ [Ff]ailed\ to\ connect ]]; then result stale_stream_reconnects 0 "$e2"
    else result stale_stream_reconnects 1 "[$e2]"; fi
  fi

  # moonlink down (the TERM above): heap DROPs are unaffected, mirror DROPs succeed with a WARNING,
  # and mooncake.* calls raise ordinary ERRORs.
  if [[ -z $term_pid ]]; then result bgworker_exited_after_term skip "no moonlink bgworker found"
  elif poll_until 10 moonlink_gone; then result bgworker_exited_after_term 0
  else result bgworker_exited_after_term 1 "bgworker pid $term_pid still running 10s after SIGTERM"; fi
  # The SIGTERMed worker's own exit (non-zero is how a fixed worker gets restarted) is not abnormal.
  ALLOWED_EXIT_LINES="$log_before $(wc -l < "$OUT/postgres.log")"
  raw_psql 30 "$o.dh.out" "$o.dh.err" -c "DROP TABLE h"; rc=$?
  if (( rc == 0 )) && ! grep -q 'ERROR:\|WARNING:' "$o.dh.err"; then result drop_heap_while_down 0
  else result drop_heap_while_down 1 "rc=$rc $(one_line 240 < "$o.dh.err")"; fi
  raw_psql 30 "$o.dm.out" "$o.dm.err" -c "DROP TABLE s_mc" -c "SELECT 'dropped=' || (to_regclass('s_mc') IS NULL)"; rc=$?
  if (( rc == 0 )) && ! grep -q 'ERROR:' "$o.dm.err" && grep -q 'WARNING:.*did not drop' "$o.dm.err" && grep -qx 'dropped=true' "$o.dm.out"; then
    result drop_mirror_while_down 0 "$(grep -m1 'WARNING:' "$o.dm.err" | cut -c1-160)"
  else result drop_mirror_while_down 1 "rc=$rc $(one_line 240 < "$o.dm.err") $(one_line 40 < "$o.dm.out")"; fi
  raw_psql 30 "$o.lt.out" "$o.lt.err" -c "SELECT count(*) FROM mooncake.list_tables()"; rc=$?
  if grep -q 'ERROR:' "$o.lt.err" && ! grep -qi 'server closed the connection\|terminated abnormally' "$o.lt.err" && pg_alive; then
    result list_tables_errors_while_down 0 "$(grep -m1 'ERROR:' "$o.lt.err" | cut -c1-120)"
  else result list_tables_errors_while_down 1 "rc=$rc $(one_line 240 < "$o.lt.err") $(one_line 40 < "$o.lt.out")"; fi

  # A bgworker that exited after SIGTERM must come back (restart_time is 15s).
  if [[ -z $term_pid ]]; then result bgworker_restarted_after_term skip "no moonlink bgworker found"
  elif poll_until 30 moonlink_restarted "$term_pid" && wait_ready; then
    result bgworker_restarted_after_term 0 "pid $term_pid -> $(moonlink_pid)"
  else result bgworker_restarted_after_term 1 "no new moonlink bgworker 30s after SIGTERM (pid before: $term_pid, now: [$(moonlink_pid)])"; fi

  stop fast clean_stop_before_restart; start
  if wait_ready; then result restart_ready 0; else result restart_ready 1 "mooncake.list_tables() not answering within 60s"; fi

  # moonlink hung: DROP of a mirror returns (bounded RPC) with a WARNING instead of blocking forever.
  pid=$(moonlink_pid)
  if [[ -z $pid ]]; then result drop_mirror_while_hung skip "no moonlink bgworker found"
  else
    trap 'kill -CONT '"$pid"' 2> /dev/null' EXIT
    kill -STOP "$pid"
    t0=$(date +%s)
    raw_psql 60 "$o.hg.out" "$o.hg.err" -c "DROP TABLE s3_mc" -c "SELECT 'dropped=' || (to_regclass('s3_mc') IS NULL)"; rc=$?
    secs=$(( $(date +%s) - t0 ))
    kill -CONT "$pid"; trap - EXIT
    if (( rc == 0 && secs <= 40 )) && grep -q 'WARNING:.*no response from moonlink' "$o.hg.err" && grep -qx 'dropped=true' "$o.hg.out"; then
      result drop_mirror_while_hung 0 "${secs}s: $(grep -m1 'WARNING:' "$o.hg.err" | cut -c1-120)"
    else result drop_mirror_while_hung 1 "rc=$rc after ${secs}s: $(one_line 200 < "$o.hg.err")"; fi
  fi

  # Orphan cleanup (INFO): s_mc was dropped while moonlink was down, so moonlink still has it. The
  # documented cleanup is CREATE TABLE <same name> (id int) USING mooncake; DROP TABLE <same name>.
  local m
  for m in e2e_fence_mc e2e_probe_mc s3_mc; do raw_psql 40 "$o.cl.out" "$o.cl.err" -c "DROP TABLE IF EXISTS $m"; done
  info orphan_before "list_tables: $(psqlq -c "SELECT string_agg(\"table\", ',') FROM mooncake.list_tables()" 2>&1 | one_line 160)"
  if [[ $(psqlq -c "SELECT to_regclass('s_mc') IS NULL" 2>&1) != t ]]; then
    info orphan_cleanup "skipped: s_mc still exists in Postgres (its DROP failed while moonlink was down)"
  fi
  raw_psql 40 "$o.or.out" "$o.or.err" -c "DROP TABLE IF EXISTS s_mc" -c "CREATE TABLE s_mc (id int) USING mooncake" -c "DROP TABLE s_mc"; rc=$?
  info orphan_cleanup "rc=$rc $(one_line 160 < "$o.or.err") public.s_mc still listed: $(psqlq -c "SELECT count(*) FROM mooncake.list_tables() WHERE \"table\" = 'public.s_mc'" 2>&1 | one_line 80)"
  poll_until 30 sql_is "SELECT count(*) FROM pg_replication_slots" 0
  info orphan_last_mirror_slot "slots after the last mirror was cleaned up: $(psqlq -c "SELECT count(*) || ' ' || coalesce(string_agg(slot_name, ','), '') FROM pg_replication_slots" 2>&1 | one_line 120)"

  end_cluster
}

# ================================================================================================
# Restart / crash acceptance (per user): crash recovery need not be exact. After a restart the
# moonlink bgworker must be up and serving RPCs (no crash-loop), and drop + re-create of the mirror
# ("resnapshot") must yield the heap contents. Exact post-restart equality is reported as INFO.

# resnapshot_check <prefix> <heap> <mirror>: the user-facing recovery path is "drop and re-create".
# A step whose prerequisite failed is SKIP (it could not run), so its result isn't blamed on a bug.
# Sets RESNAPSHOT_OK=1 once the mirror was re-created.
resnapshot_check() {
  RESNAPSHOT_OK=0
  run "${1}_drop" -c "DROP TABLE $3"
  if [[ ${STATUS_OF[$SCEN.${1}_drop]} != *PASS ]]; then
    result "${1}_recreate" skip "not attempted: the DROP before it failed"
    result "${1}_matches_heap" skip "not attempted: the DROP before it failed"
    return
  fi
  run "${1}_recreate" -c "CALL mooncake.create_table('$3', '$2')"
  if [[ ${STATUS_OF[$SCEN.${1}_recreate]} != *PASS ]]; then
    result "${1}_matches_heap" skip "not attempted: create_table failed"
    return
  fi
  RESNAPSHOT_OK=1
  compare "${1}_matches_heap" "$2" "$3"
}
# cdc_after_resnapshot <heap> <mirror> <workload sql>: CDC into the re-created mirror.
cdc_after_resnapshot() {
  if (( ! RESNAPSHOT_OK )); then
    result cdc_after_resnapshot_workload skip "not attempted: drop + re-create failed"
    result cdc_after_resnapshot skip "not attempted: drop + re-create failed"
    return
  fi
  run cdc_after_resnapshot_workload -c "$3"
  compare cdc_after_resnapshot "$1" "$2"
}

# restart_gate <suffix> <heap> <mirror> <stop mode>
restart_gate() {
  local mark sites
  if [[ $4 == fast ]]; then stop fast clean_stop_before_restart; else stop "$4"; fi
  mark=$(panic_count)
  start
  if wait_ready; then result "service_up_after_$1" 0
  else
    result "service_up_after_$1" 1 "mooncake.list_tables() not answering within 60s; panics since restart: $(panics_inline "$mark"); last error: $(echo "$WAIT_ERR" | one_line 160)"
  fi
  no_crash_loop "no_crash_loop_after_$1" "$mark"
  # Recovery itself must not panic; later checks of this restart can require this one's cause.
  sites=$(panic_sites "$mark")
  if [[ -n $sites ]]; then result "no_panics_after_$1" 1 "panics since restart: $(panics_inline "$mark")" "$sites"
  else result "no_panics_after_$1" 0; fi
  info_compare "exact_after_$1" "$2" "$3"
  fresh_fence fresh_fence_setup
  resnapshot_check resnapshot "$2" "$3"
}

scenario_big_txn_restart() {
  new_cluster big_txn_restart || return
  run setup <<'SQL'
CREATE TABLE b (id bigint PRIMARY KEY, v text);
CALL mooncake.create_table('b_mc', 'b');
INSERT INTO b SELECT g, 'small' FROM generate_series(1, 300) g;
SQL
  run big_txn_workload -c "INSERT INTO b SELECT g, repeat('z', 200) FROM generate_series(100000, 299999) g" \
    -c "INSERT INTO b VALUES (1000000, 'last-before-restart')"
  check_eq heap_rows "$(psqlq -c "SELECT count(*) FROM b" 2>&1)" "200301"
  compare before_restart b b_mc
  restart_gate restart b b_mc fast
  cdc_after_resnapshot b b_mc "INSERT INTO b VALUES (1000001, 'after-resnapshot')"
  end_cluster
}

scenario_crash() {
  new_cluster crash || return
  run setup <<'SQL'
CREATE TABLE c (id bigint PRIMARY KEY, v text);
CALL mooncake.create_table('c_mc', 'c');
SQL
  local i
  {
    for i in $(seq 0 9); do echo "INSERT INTO c SELECT g, md5(g::text) FROM generate_series($((i * 1000 + 1)), $((i * 1000 + 1000))) g;"; done
    echo "UPDATE c SET v = 'u' WHERE id % 3 = 0;"
    echo "INSERT INTO c SELECT g, repeat('z', 200) FROM generate_series(100000, 299999) g;"
    echo "INSERT INTO c VALUES (1000000, 'pre-crash');"
  } > "$OUT/crash_workload.sql"
  run crash_workload -f "$OUT/crash_workload.sql"
  check_eq heap_rows "$(psqlq -c "SELECT count(*) FROM c" 2>&1)" "210001"
  compare before_crash c c_mc
  restart_gate crash c c_mc immediate
  cdc_after_resnapshot c c_mc "DELETE FROM c WHERE id BETWEEN 1 AND 500"
  end_cluster
}

# ================================================================================================
# Source DDL, one kind per cluster (a failure in one must not mask the others). A bystander mirror
# in the same database checks the blast radius.

# ddl_cluster <scenario>: fresh cluster with bystander "other" and source "d"
ddl_cluster() {
  new_cluster "$1" || return 1
  run bystander_setup <<'SQL'
CREATE TABLE other (id int PRIMARY KEY, v text);
CALL mooncake.create_table('other_mc', 'other');
INSERT INTO other VALUES (1, 'a');
SQL
  run ddl_setup <<'SQL'
CREATE TABLE d (id int PRIMARY KEY, a int, z text);
CALL mooncake.create_table('d_mc', 'd');
INSERT INTO d SELECT g, g, 'z' FROM generate_series(1, 100) g;
SQL
  compare ddl_before d d_mc "id, a, z"
}
ddl_finish() {
  run bystander_workload -c "INSERT INTO other VALUES (2, 'b')"
  compare bystander_after_ddl other other_mc
  run drop_mirror_after_ddl -c "DROP TABLE d_mc"
  run drop_bystander_after_ddl -c "DROP TABLE other_mc"
  end_cluster
}

scenario_ddl() {
  ddl_cluster ddl || return
  run ddl_add_column -c "ALTER TABLE d ADD COLUMN b int DEFAULT 5" -c "INSERT INTO d SELECT g, g, 'z', 7 FROM generate_series(101, 200) g"
  compare ddl_after_add_column d d_mc "id, a, z"
  psqlq -c "\d d_mc" > "$OUT/ddl_mirror_schema.txt" 2>&1
  ddl_finish
}

scenario_ddl_drop() {
  ddl_cluster ddl_drop || return
  run ddl_drop_column -c "ALTER TABLE d DROP COLUMN z" -c "INSERT INTO d (id, a) SELECT g, g FROM generate_series(201, 300) g" -c "UPDATE d SET a = -a WHERE id <= 10"
  compare ddl_after_drop_column d d_mc "id, a"
  ddl_finish
}

scenario_truncate() {
  ddl_cluster truncate || return
  run truncate_workload -c "TRUNCATE d" -c "INSERT INTO d (id, a, z) VALUES (1, 1, 'z')"
  # Acceptable: the mirror equals the heap, or scans fail loudly naming the truncate (errored-table
  # design). Silently serving the pre-truncate rows is the bug.
  compare_core truncate_mirror d d_mc "id, a, z" "$CONVERGE_SECS"
  if (( CMP_OK == 0 )); then result truncate_not_silent 0 "$CMP_DETAIL"
  elif grep -qi 'truncat' "$OUT/truncate_mirror.mirror.err" 2>/dev/null; then
    result truncate_not_silent 0 "mirror errors: $(one_line 160 < "$OUT/truncate_mirror.mirror.err")"
  else result truncate_not_silent 1 "$CMP_DETAIL"; fi
  resnapshot_check resnapshot_after_truncate d d_mc
  ddl_finish
}

# ================================================================================================
# Known-bug summary, per bug: FIXED only when every check listed for it passed (XPASS); OPEN when
# one of them failed with the bug's signature; UNKNOWN otherwise (e.g. another bug masked it).
known_summary() {
  local check st bug re list verdict seen e entries n_all n_xpass n_xfail n_notrun
  declare -A by_bug=()
  for check in $(printf '%s\n' "${!KNOWN_RULES[@]}" | sort); do
    [[ " ${SCENARIOS[*]} " == *" ${check%%.*} "* ]] || continue
    st=${STATUS_OF[$check]:-NOT-RUN}; seen=","
    while IFS=$'\t' read -r bug re; do
      [[ -n $bug && $seen != *",$bug,"* ]] || continue
      seen+="$bug,"
      if [[ $st == XFAIL && ,${MATCHED_OF[$check]:-}, != *",$bug,"* ]]; then by_bug[$bug]+="$check=MASKED,"
      else by_bug[$bug]+="$check=$st,"; fi
    done <<< "${KNOWN_RULES[$check]}"
  done
  for bug in $(printf '%s\n' "${!by_bug[@]}" | sort); do
    list=${by_bug[$bug]%,}; n_all=0; n_xpass=0; n_xfail=0; n_notrun=0
    IFS=, read -ra entries <<< "$list"
    for e in "${entries[@]}"; do
      n_all=$((n_all + 1))
      case ${e##*=} in XPASS) n_xpass=$((n_xpass + 1)) ;; XFAIL) n_xfail=$((n_xfail + 1)) ;; NOT-RUN) n_notrun=$((n_notrun + 1)) ;; esac
    done
    if (( n_xfail > 0 )); then verdict=OPEN
    elif (( n_xpass == n_all )); then verdict=FIXED
    elif (( n_notrun == n_all )); then verdict=NOT-RUN
    else verdict=UNKNOWN; fi
    echo "KNOWN $bug $verdict $list${BUG_IDS[$bug]:+ ids=${BUG_IDS[$bug]}}"
  done
}

main() {
  local s scen_list counts complete=1
  build_info
  for s in "${SCENARIOS[@]}"; do
    if declare -F "scenario_$s" > /dev/null; then "scenario_$s"; else echo "unknown scenario: $s" >&2; return 2; fi
  done
  for s in "${ALL_SCENARIOS[@]}"; do [[ " ${SCENARIOS[*]} " == *" $s "* ]] || complete=0; done
  known_summary
  scen_list=$(IFS=,; echo "${SCENARIOS[*]}")
  counts="pass=${COUNT[pass]} xpass=${COUNT[xpass]} fail=${COUNT[fail]} xfail=${COUNT[xfail]} skip=${COUNT[skip]}"
  echo "SUMMARY e2e-smoke pg$MAJOR scenarios=$scen_list $counts"
  if (( COUNT[fail] + COUNT[xfail] + COUNT[skip] > 0 || COUNT[pass] + COUNT[xpass] == 0 )); then
    echo "GATE e2e-smoke FAIL"; return 1
  fi
  if (( ! complete )); then echo "GATE e2e-smoke INCOMPLETE"; return 3; fi
  echo "GATE e2e-smoke PASS"
}
# Parsed in full before it runs, so editing this file during a run is safe.
main; exit
