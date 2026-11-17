"""Result comparison and query classification for mirror_diff (no database access, unit-tested).

Results are compared as Postgres prints them. The only tolerance is for float4/float8 result
columns, rounded to the digits the type guarantees, because DuckDB prints a few last digits
differently. Everything else (numeric scale, text, dates) must match exactly. Row order is
compared as far as the statement's top-level ORDER BY determines it.
"""

import json
import re
from decimal import Decimal, InvalidOperation

from psqlsession import NULL_TOKEN

# Significant digits that survive a round trip through each binary float type.
FLOAT_DIGITS = {"double precision": 12, "real": 6}

# Errors DuckDB raises while binding or planning a statement it does not support. pg_duckdb
# prefixes errors of DuckDB execution with "(PGDuckDB/...)".
DUCKDB_PLANNING_ERROR = re.compile(
    r"\(PGDuckDB/CreatePlan\)|"
    r"\(PGDuckDB/[^)]*\).*\b(Catalog Error|Binder Error|Parser Error|Not implemented Error|"
    r"Unsupported|Conversion Error: Unimplemented|is not supported|Not implemented)|"
    r"^(ERROR:\s+)?DuckDB execution is not supported inside functions", re.I | re.S)
# The session's role may not run DuckDB at all (files switch roles): not a property of the mirror.
DUCKDB_ROLE_ERROR = re.compile(r"not been granted the duckdb\.postgres_role", re.I)
# Errors of the mirror read path itself (moonlink, the mooncake DuckDB extension, its RPC): never
# a DuckDB semantics difference, even when DuckDB phrases them.
NOT_DUCKDB_ERROR = re.compile(
    r"moonlink|mooncake_duckdb_cpp_init|backend error|Failed to connect|IO Error|early eof|\bRPC\b",
    re.I)


def is_planning_error(message):
    return bool(DUCKDB_PLANNING_ERROR.search(message or "")) and not NOT_DUCKDB_ERROR.search(message or "")


# ---- cells and rows ------------------------------------------------------------------------------
def canon_cell(cell, typ):
    digits = FLOAT_DIGITS.get(typ or "")
    if digits is None or cell == NULL_TOKEN:
        return cell
    try:
        f = float(cell)
    except ValueError:
        return cell
    if f != f or f in (float("inf"), float("-inf")):
        return cell.lower().lstrip("+")
    if f == 0:
        return "0"
    return format(f, f".{digits}g")


def canon_rows(rows, types):
    if not types or not any(t in FLOAT_DIGITS for t in types):
        return [tuple(r) for r in rows]
    return [tuple(canon_cell(c, types[i] if i < len(types) else None) for i, c in enumerate(r)) for r in rows]


def norm_error(message):
    """Error text without the parts that differ by engine path: the pg_duckdb prefix, the
    'Prepared query returned an error' wrapper, catalog qualifiers and the LINE excerpt."""
    m = (message or "").split("\nLINE ")[0]
    m = re.sub(r"\(PGDuckDB/[^)]*\)\s*", "", m)
    m = re.sub(r"Prepared query returned an error:\s*'?", "", m)
    m = re.sub(r"\b(mc|public|pg_catalog|memory|main)\.", "", m)
    m = re.sub(r"^(ERROR|WARNING):\s*", "", m.strip())
    return " ".join(m.split()).rstrip("'").strip()


# ---- JSON ----------------------------------------------------------------------------------------
class _JsonNumber(str):
    """A JSON number kept as its text, so 0.1234567890123456789 and 0.12345678901234568 differ."""


class _JsonObject(list):
    """A JSON object as its (key, value) pairs, so duplicate keys (json, not jsonb) are kept."""


def canonical_json(text):
    """JSON text with insignificant whitespace removed and object keys sorted (stable for duplicate
    keys); numbers keep their text. Raises ValueError for text that isn't JSON."""
    value = json.loads(text, parse_float=_JsonNumber, parse_int=_JsonNumber, parse_constant=_JsonNumber,
                       object_pairs_hook=_JsonObject)
    return _dump_json(value)


def _dump_json(v):
    if isinstance(v, _JsonNumber):
        return str(v)
    if isinstance(v, _JsonObject):
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + _dump_json(x)
                              for k, x in sorted(v, key=lambda kv: kv[0])) + "}"
    if isinstance(v, list):
        return "[" + ",".join(_dump_json(x) for x in v) + "]"
    return json.dumps(v, ensure_ascii=False)


# ---- ORDER BY ------------------------------------------------------------------------------------
def mask(sql):
    """sql with the inside of string literals, quoted identifiers and parentheses replaced by
    underscores (same length), so keyword and comma positions found in it are top-level ones."""
    out, depth, i, n = list(sql), 0, 0, len(sql)
    while i < n:
        c = sql[i]
        if c in "'\"":
            j = i + 1
            while j < n and not (sql[j] == c and (j + 1 >= n or sql[j + 1] != c)):
                j += 2 if sql[j] == c else 1
            for k in range(i + 1, min(j, n)):
                out[k] = "_"
            i = j + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth = max(0, depth - 1)
        elif depth:
            out[i] = "_"
        i += 1
    return "".join(out)


_CLAUSE_END = re.compile(r"\b(limit|offset|fetch|for\s+(update|share|no\s+key|key)|window)\b", re.I)


def top_level_order_by(sql):
    """(ORDER BY items of the outermost query or None, limit). limit is None without LIMIT/FETCH,
    the row count of a literal LIMIT n / FETCH FIRST n ROWS ONLY, or -1 when the count isn't a
    literal (an expression, a parameter, WITH TIES)."""
    sql = sql.rstrip().rstrip(";")
    m = mask(sql)
    limit = None
    lim = re.search(r"\blimit\s+(?!all\b)(\S+)", m, re.I)
    fetch = re.search(r"\bfetch\s+(first|next)\b(.*?)\brows?\b\s*(only|with\s+ties)", m, re.I | re.S)
    if lim:
        limit = int(lim.group(1)) if re.fullmatch(r"\d+", lim.group(1)) else -1
    elif fetch:
        n = fetch.group(2).strip()
        if re.search(r"with\s+ties", fetch.group(3), re.I):
            limit = -1
        else:
            limit = 1 if not n else (int(n) if re.fullmatch(r"\d+", n) else -1)
    found = list(re.finditer(r"\border\s+by\b", m, re.I))
    if not found:
        return None, limit
    start = found[-1].end()
    end = _CLAUSE_END.search(m, start)
    stop = end.start() if end else len(sql)
    items, k = [], start
    for c in re.finditer(",", m[start:stop]):
        items.append(sql[k:start + c.start()].strip())
        k = start + c.end()
    items.append(sql[k:stop].strip())
    return [i for i in items if i], limit


def strip_top_level_limit(sql):
    """sql without its top-level LIMIT / OFFSET / FETCH clauses (None if it has none)."""
    sql = sql.rstrip().rstrip(";")
    m = mask(sql)
    hits = [x.start() for x in re.finditer(r"\b(limit|offset|fetch)\b", m, re.I)]
    return sql[:min(hits)].rstrip() if hits else None


def rows_subset(sub_rows, full_rows, types=None):
    """True if every row of sub_rows (with multiplicity) is in full_rows."""
    full = _multiset(canon_rows([tuple(r) for r in full_rows], types))
    for r, n in _multiset(canon_rows([tuple(r) for r in sub_rows], types)).items():
        if full.get(r, 0) < n:
            return False
    return True


def _item_expr(item):
    e = re.sub(r"\s+nulls\s+(first|last)\s*$", "", item.strip(), flags=re.I)
    e = re.sub(r"\s+using\s+\S+\s*$", "", e, flags=re.I)
    return re.sub(r"\s+(asc|desc)\s*$", "", e, flags=re.I).strip()


def order_key_columns(items, names):
    """Result column indexes of the leading ORDER BY items that name an output column (by
    position, name or qualified name). Stops at the first item that can't be mapped."""
    lower = [n.lower() for n in names]
    out = []
    for item in items or []:
        e = _item_expr(item)
        if re.fullmatch(r"\d+", e):
            k = int(e) - 1
            if not 0 <= k < len(names):
                break
            out.append(k)
            continue
        e = e.split(".")[-1].strip()
        e = e[1:-1] if e.startswith('"') and e.endswith('"') else e.lower()
        if e.lower() not in lower or lower.count(e.lower()) > 1:
            break
        out.append(lower.index(e.lower()))
    return out


# ---- comparison ----------------------------------------------------------------------------------
def _multiset(rows):
    d = {}
    for r in rows:
        d[r] = d.get(r, 0) + 1
    return d


def rows_match(a_rows, b_rows, types=None, order_items=None, names=None, limit=None):
    """(equal, how). how: exact | float | multiset | ties | order-unverified | order | rows |
    limit-unverifiable. limit is as returned by top_level_order_by. limit-unverifiable means the
    rows differ only where a LIMIT may legitimately pick different rows (no ORDER BY at all, or
    the last group of equal sort keys of a result the LIMIT cut): the caller must check the rows
    against the unlimited result."""
    ra, rb = [tuple(r) for r in a_rows], [tuple(r) for r in b_rows]
    if ra == rb:
        return True, "exact"
    ca, cb = canon_rows(ra, types), canon_rows(rb, types)
    via = "float" if ca != ra or cb != rb else None
    if len(ca) != len(cb):
        return False, "rows"
    keys = order_key_columns(order_items, names or []) if order_items else []
    if not order_items or not keys:
        if _multiset(ca) == _multiset(cb):
            # ORDER BY whose leading item isn't an output column: only the multiset is checked.
            return True, via or ("order-unverified" if order_items else "multiset")
        # Without ORDER BY, LIMIT may keep any subset of that size. An ORDER BY that can't be
        # mapped to output columns still fixes which rows LIMIT keeps: a plain difference.
        return False, "limit-unverifiable" if limit is not None and order_items is None else "rows"
    if ca == cb:
        return True, via or "exact"
    # Rows must come in the same ORDER BY key sequence; rows with equal keys may be permuted.
    def key(r):
        return tuple(r[k] for k in keys)
    if [key(r) for r in ca] != [key(r) for r in cb]:
        return False, "order" if _multiset(ca) == _multiset(cb) else "rows"
    i = 0
    while i < len(ca):
        j = i
        while j < len(ca) and key(ca[j]) == key(ca[i]):
            j += 1
        if _multiset(ca[i:j]) != _multiset(cb[i:j]):
            if j < len(ca) or limit is None:
                return False, "rows"
            # The last group of equal keys may be cut by LIMIT, but only if LIMIT cut the result;
            # its rows must then come from the unlimited result.
            if limit == -1 or len(ca) == limit:
                return False, "limit-unverifiable"
            return False, "rows"
        i = j
    return True, "ties"


def results_match(a, b, types=None, order_items=None, names=None, limit=None):
    """Compare two psqlsession Results: (equal, how). Both erroring is equal only with the same
    SQLSTATE and normalised message; a timeout or a dead session never matches."""
    for r in (a, b):
        if r.timed_out:
            return False, "timeout"
        if r.died:
            return False, "died"
    if a.error or b.error:
        if a.error and b.error:
            if a.sqlstate == b.sqlstate and norm_error(a.message) == norm_error(b.message):
                return True, "same-error"
            return False, "different-error"
        return False, "error-vs-rows"
    return rows_match(a.rows, b.rows, types, order_items, names, limit)


# ---- window frames -------------------------------------------------------------------------------
ORDER_SENSITIVE = re.compile(
    r"\b(row_number|ntile|lag|lead|first_value|last_value|nth_value|array_agg|string_agg|json_agg|"
    r"jsonb_agg|json_object_agg|jsonb_object_agg|xmlagg)\s*\(", re.I)


def _windows(sql):
    """(function text before OVER, window spec text) for every window call; named windows resolved."""
    s = re.sub(r"'(?:[^']|'')*'", "''", sql)
    named = {}
    for m in re.finditer(r"\bwindow\s+", s, re.I):
        rest = s[m.end():]
        for wm in re.finditer(r"(\w+)\s+as\s*\(", rest, re.I):
            named[wm.group(1).lower()] = _paren(rest, wm.end() - 1)
    out = []
    for m in re.finditer(r"\bover\s*(\(|(\w+))", s, re.I):
        spec = _paren(s, m.start(1)) if m.group(1) == "(" else named.get(m.group(2).lower(), "")
        base = re.match(r"\s*(\w+)", spec)
        if base and base.group(1).lower() in named:
            spec = named[base.group(1).lower()] + " " + spec
        func = s[max(0, m.start() - 200):m.start()]
        out.append((func, spec))
    return out


def _paren(s, i):  # text inside the parenthesis that opens at s[i]
    depth, j = 0, i
    while j < len(s):
        depth += {"(": 1, ")": -1}.get(s[j], 0)
        j += 1
        if depth == 0:
            break
    return s[i + 1:j - 1]


def order_dependent(sql):
    """True if a window function's result depends on the physical order of rows, which differs
    between a heap and its mirror: an order-sensitive function over a window without ORDER BY,
    or a ROWS/GROUPS frame or EXCLUDE clause (ties are split by physical order)."""
    for func, spec in _windows(sql):
        tail = re.split(r"\)\s*$", func)[0]
        calls = list(re.finditer(r"(\w+)\s*\(", tail))
        fname = calls[-1].group(0) if calls else ""
        if not re.search(r"\border\s+by\b", spec, re.I) and ORDER_SENSITIVE.match(fname):
            return True
        if re.search(r"\b(rows|groups)\s+(between|unbounded|current|\d)", spec, re.I) or \
                re.search(r"\bexclude\b", spec, re.I):
            return True
    return False


# ---- classification ------------------------------------------------------------------------------
def classify(heap, mirror, duck, duck_ran, match, order_dep):
    """Verdict for a query whose mirror result differs from the heap result.

    duck is the same query on the heap tables with duckdb.force_execution; duck_ran says EXPLAIN
    showed DuckDB planned it. match(a, b) -> (equal, how). Returns (status, verdict, reason) with
    status FAIL | ENGINE | SKIP."""
    if mirror.timed_out or mirror.died:
        return "FAIL", "mooncake", f"mirror query {'timed out' if mirror.timed_out else 'died'}"
    if mirror.error and DUCKDB_ROLE_ERROR.search(mirror.message or ""):
        return "SKIP", "unsupported", "the session's role may not run DuckDB"
    if duck_ran and match(duck, heap)[0]:
        return "FAIL", "mooncake", "mirror differs; DuckDB on the heap tables agrees with the heap"
    if duck_ran and match(duck, mirror)[0]:
        if mirror.ok:
            return "ENGINE", "engine", "DuckDB semantics (DuckDB on the heap tables gives the mirror's answer)"
        if is_planning_error(mirror.message):
            return "SKIP", "unsupported", "DuckDB cannot run it"
        if NOT_DUCKDB_ERROR.search(mirror.message or ""):
            return "FAIL", "mooncake", "mirror read path error"
        return "ENGINE", "engine", "error raised by DuckDB only (also on the heap tables)"
    if order_dep and duck_ran and duck.ok and heap.ok and mirror.ok and len(mirror.rows) == len(heap.rows):
        return "ENGINE", "engine", "result depends on physical row order (window frame or order-sensitive function)"
    if mirror.error and is_planning_error(mirror.message) and (not duck_ran or duck.error):
        return "SKIP", "unsupported", "DuckDB cannot run it"
    if not duck_ran:
        return "FAIL", "unclassified", "mirror differs and DuckDB did not run the query on the heap tables"
    return "FAIL", "unclassified", "heap, mirror and DuckDB-on-heap all differ"


# ---- divergence events ---------------------------------------------------------------------------
def _fmt_rows(rows, n):
    return "; ".join("|".join(str(c)[:60] for c in r) for r in rows[:n])[:240]


# ---- DuckDB STRUCT text ------------------------------------------------------------------------
def _parse_duck(s, i):
    """Parse a DuckDB value printed as text at s[i]: {'k': v, ...} (STRUCT), [a, b] (LIST), NULL
    or a bare scalar. Returns (value, next index); STRUCT -> list of values, LIST -> tuple."""
    if s.startswith("{'", i):
        i += 1
        fields = []
        while True:
            if s[i] != "'":
                raise ValueError("struct key")
            j = s.index("': ", i + 1)
            v, i = _parse_duck(s, j + 3)
            fields.append(v)
            if s.startswith(", '", i):
                i += 2
                continue
            if s[i] == "}":
                return fields, i + 1
            raise ValueError("struct end")
    if s.startswith("[", i):
        i += 1
        items = []
        if s[i] == "]":
            return tuple(items), i + 1
        while True:
            v, i = _parse_duck(s, i)
            items.append(v)
            if s.startswith(", ", i):
                i += 2
                continue
            if s[i] == "]":
                return tuple(items), i + 1
            raise ValueError("list end")
    j = i
    while j < len(s) and s[j] not in ",}]":
        j += 1
    text = s[i:j]
    return (None if text == "NULL" else text), j


def _pg_scalar(v):
    # DuckDB prints integral DOUBLE values as 11.0 where Postgres prints 11.
    return v[:-2] if re.fullmatch(r"-?\d+\.0", v) else v


def _pg_text(v, in_array):
    """Postgres output text of a parsed DuckDB value (record_out / array_out conventions)."""
    if v is None:
        return None
    if isinstance(v, list):
        out = []
        for f in v:
            t = _pg_text(f, False)
            if t is None:
                out.append("")
            elif t == "" or re.search(r'[",()\\\s]', t):
                out.append('"' + t.replace("\\", "\\\\").replace('"', '""') + '"')
            else:
                out.append(t)
        return "(" + ",".join(out) + ")"
    if isinstance(v, tuple):
        return "{" + ",".join(_pg_array_elem(_pg_text(x, True)) for x in v) + "}"
    return _pg_scalar(v)


def _pg_array_elem(t):
    if t is None:
        return "NULL"
    if t == "" or t.upper() == "NULL" or re.search(r'[{},"\\\s]', t):
        return '"' + t.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return t


def _parse_pg_array(text):
    """Elements of a one-dimensional Postgres array literal (None for NULL), or None."""
    if not (text.startswith("{") and text.endswith("}")):
        return None
    body, out, i = text[1:-1], [], 0
    if not body:
        return out
    while i <= len(body):
        if i < len(body) and body[i] == '"':
            j, buf = i + 1, []
            while body[j] != '"':
                if body[j] == "\\":
                    j += 1
                buf.append(body[j])
                j += 1
            out.append("".join(buf))
            i = j + 1
        else:
            j = body.find(",", i)
            j = len(body) if j < 0 else j
            elem = body[i:j]
            out.append(None if elem == "NULL" else elem)
            i = j
        if i < len(body) and body[i] != ",":
            return None
        i += 1
    return out


def struct_to_row(cell):
    """The Postgres text of a composite (or array of composites) value that the mirror printed
    as DuckDB STRUCT text, or None if the cell isn't STRUCT text that parses completely."""
    try:
        if cell.startswith("{'"):
            v, i = _parse_duck(cell, 0)
            return _pg_text(v, False) if i == len(cell) else None
        elems = _parse_pg_array(cell)
        if elems and all(e is None or e.startswith("{'") for e in elems):
            rows = [None if e is None else struct_to_row(e) for e in elems]
            if any(e is not None and r is None for e, r in zip(elems, rows)):
                return None
            return "{" + ",".join(_pg_array_elem(r) for r in rows) + "}"
    except (ValueError, IndexError):
        return None
    return None


_NUMERIC_SPECIALS = ("NaN", "Infinity", "-Infinity")


def _numeric(text):
    if text in _NUMERIC_SPECIALS:
        return text
    return Decimal(text).normalize()


def numeric_key(v):
    """A numeric cell, or a one-dimensional array of them, as numbers, so that 1 and 1.0000000000
    compare equal. None if it isn't one."""
    if v is None or v == NULL_TOKEN:
        return v
    s = str(v)
    try:
        if s.startswith("{") and s.endswith("}"):
            inner = s[1:-1]
            if "{" in inner or '"' in inner:
                return None
            return tuple(None if p == "NULL" else _numeric(p) for p in (inner.split(",") if inner else []))
        return _numeric(s)
    except InvalidOperation:
        return None


def divergence_events(cols, heap_rows, mirror_rows, sample=3):
    """Events describing how a mirror's rows differ from the heap's. cols: [(name, type), ...].
    Each event: {"kind": row-count|value|rows, "detail": text, ["col", "type"]}."""
    ha, mb = [tuple(r) for r in heap_rows], [tuple(r) for r in mirror_rows]
    ca, cb = _multiset(ha), _multiset(mb)
    only_h = [r for r in ca if ca[r] > cb.get(r, 0)]
    only_m = [r for r in cb if cb[r] > ca.get(r, 0)]
    if len(ha) != len(mb):
        return [{"kind": "row-count",
                 "detail": f"row-count heap={len(ha)} mirror={len(mb)} heap-only: {_fmt_rows(only_h, sample)} "
                           f"|| mirror-only: {_fmt_rows(only_m, sample)}"}]
    out = []
    for i, (name, typ) in enumerate(cols):
        hv, mv = _multiset([r[i] for r in ha if i < len(r)]), _multiset([r[i] for r in mb if i < len(r)])
        if hv == mv:
            continue
        h_only = [v for v in hv if hv[v] > mv.get(v, 0)]
        m_only = [v for v in mv if mv[v] > hv.get(v, 0)]
        converted = {}
        for v, k in mv.items():
            c = struct_to_row(v) if isinstance(v, str) and "{'" in v else v
            converted[c] = converted.get(c, 0) + k
        if None not in converted and converted == hv:
            # Only the text form differs: the mirror prints the composite as DuckDB STRUCT text.
            out.append({"kind": "struct-text", "col": name, "type": typ,
                        "detail": f"struct-text col={name} type={typ}: {sum(k for v, k in mv.items() if v not in hv)} "
                                  f"value(s) equal the heap's after STRUCT-to-row conversion, e.g. heap "
                                  f"{str(h_only[0])[:60]} mirror {str(m_only[0])[:60]}"})
            continue
        if typ.startswith("numeric"):
            hn, mn = {}, {}
            for src, dst in ((hv, hn), (mv, mn)):
                for v, k in src.items():
                    key = numeric_key(v)
                    dst[key] = dst.get(key, 0) + k
            if None not in hn and None not in mn and hn == mn:
                # Only the text form differs: the mirror prints the values with another scale.
                out.append({"kind": "numeric-scale", "col": name, "type": typ,
                            "detail": f"numeric-scale col={name} type={typ}: {sum(k for v, k in mv.items() if v not in hv)} "
                                      f"value(s) equal the heap's as numbers, e.g. heap {str(h_only[0])[:60]} "
                                      f"mirror {str(m_only[0])[:60]}"})
                continue
        out.append({"kind": "value", "col": name, "type": typ,
                    "detail": f"value col={name} type={typ} heap-only: {'; '.join(str(v)[:80] for v in h_only[:sample])} "
                              f"|| mirror-only: {'; '.join(str(v)[:80] for v in m_only[:sample])}"})
    if not out and only_h:
        out.append({"kind": "rows", "detail": f"rows heap-only: {_fmt_rows(only_h, sample)} "
                                              f"|| mirror-only: {_fmt_rows(only_m, sample)}"})
    return out
