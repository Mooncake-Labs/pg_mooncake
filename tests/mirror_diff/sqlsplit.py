"""Split a pg_regress .sql file into execution units the way psql would.

A unit is one SQL statement (with the meta-command that terminated it, e.g. \\gset), one
meta-command line, a COPY ... FROM stdin statement plus its data, or a whole \\if ... \\endif block
(executed as one unit, because per-unit markers would be swallowed by an inactive branch).
Statements joined with \\; are one unit (psql sends them as one query string). Like psql, the
splitter knows SQL-standard function bodies (BEGIN ATOMIC ... END) and follows
SET standard_conforming_strings, which decides whether a backslash escapes a quote.
"""

import bisect
import re

_COPY_STDIN = re.compile(r"^\s*copy\b.*\bfrom\s+(stdin|stdout)\b", re.I | re.S)
_META_COPY_STDIN = re.compile(r"^\s*\\copy\b.*\bfrom\s+(stdin|pstdin)\b", re.I)
_DOLLAR = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")
_ROUTINE = re.compile(r"^\s*create\s+(or\s+replace\s+)?(function|procedure)\b", re.I)
_SCS = re.compile(r"^(set\s+(session\s+|local\s+)?standard_conforming_strings\s*(=|to)\s*'?(\w+)'?|"
                  r"reset\s+(standard_conforming_strings|all))\s*;?$", re.I)


class Unit:
    __slots__ = ("kind", "text", "sql", "meta", "line", "multi")

    def __init__(self, kind, text, sql, meta, line, multi=False):
        self.kind = kind    # "sql" | "meta" | "block"
        self.text = text    # exact text to send to psql (ends with a newline)
        self.sql = sql      # the SQL part with comments stripped and whitespace collapsed
        self.meta = meta    # terminating or standalone meta-command, or ""
        self.line = line    # 1-based line number where the unit starts
        self.multi = multi  # several statements joined with \\;

    def __repr__(self):
        return f"Unit({self.kind}@{self.line}: {self.text[:60]!r})"


def _skip_quoted(s, i, scs=True):
    """If s[i] starts a quoted token, return the index just after it; else return i. With
    scs (standard_conforming_strings) off, a backslash escapes in every '...' literal."""
    n = len(s)
    c = s[i]
    if c == "'":
        esc = not scs or (i > 0 and s[i - 1] in "eE" and (i < 2 or not (s[i - 2].isalnum() or s[i - 2] == "_")))
        j = i + 1
        while j < n:
            if esc and s[j] == "\\":
                j += 2
                continue
            if s[j] == "'":
                if j + 1 < n and s[j + 1] == "'":
                    j += 2
                    continue
                return j + 1
            j += 1
        return n
    if c == '"':
        j = i + 1
        while j < n:
            if s[j] == '"':
                if j + 1 < n and s[j + 1] == '"':
                    j += 2
                    continue
                return j + 1
            j += 1
        return n
    if c == "$" and (i == 0 or not (s[i - 1].isalnum() or s[i - 1] == "_")):
        m = _DOLLAR.match(s, i)
        if m:
            k = s.find(m.group(0), m.end())
            return n if k < 0 else k + len(m.group(0))
    return i


def _skip_comment(s, i):
    if s.startswith("--", i):
        j = s.find("\n", i)
        return len(s) if j < 0 else j
    if s.startswith("/*", i):
        depth, j, n = 1, i + 2, len(s)
        while j < n and depth:
            if s.startswith("/*", j):
                depth, j = depth + 1, j + 2
            elif s.startswith("*/", j):
                depth, j = depth - 1, j + 2
            else:
                j += 1
        return j
    return i


def strip_comments(sql, scs=True):
    """Remove comments outside quotes and collapse whitespace."""
    out, i, n = [], 0, len(sql)
    while i < n:
        j = _skip_comment(sql, i)
        if j > i:
            out.append(" ")
            i = j
            continue
        j = _skip_quoted(sql, i, scs)
        if j > i:
            out.append(sql[i:j])
            i = j
            continue
        out.append(sql[i])
        i += 1
    return " ".join("".join(out).split())


def _eol(s, i):
    j = s.find("\n", i)
    return len(s) if j < 0 else j


def _copy_data(s, i):
    """COPY data lines start at index i; return the index after the terminating '\\.' line."""
    n = len(s)
    while i < n:
        e = _eol(s, i)
        if s[i:e].strip() == "\\.":
            return min(e + 1, n)
        i = e + 1
    return n


def _first_nonspace(s, i):
    while i < len(s) and s[i] in " \t\r\n":
        i += 1
    return min(i, len(s) - 1)


def _if_block_end(s, i):
    depth, n = 1, len(s)
    while i < n:
        e = _eol(s, i)
        cmd = s[i:e].strip()
        if re.match(r"\\if\b", cmd):
            depth += 1
        elif re.match(r"\\endif\b", cmd):
            depth -= 1
            if depth == 0:
                return min(e + 1, n)
        i = e + 1
    return n


def split(text):
    if not text.endswith("\n"):
        text += "\n"
    starts = [0] + [k + 1 for k, ch in enumerate(text) if ch == "\n"]

    def line_of(pos):
        return bisect.bisect_right(starts, pos)

    units = []
    n = len(text)
    start = 0       # start of the pending unit
    depth = 0       # parentheses
    atomic = 0      # BEGIN ATOMIC ... END nesting (CASE ... END counts inside it)
    multi = False   # the pending unit already contains a \; separator
    scs = True      # standard_conforming_strings

    def emit(unit):
        nonlocal scs
        units.append(unit)
        if unit.kind == "sql":
            m = _SCS.match(unit.sql)
            if m:
                scs = bool(m.group(5)) or m.group(4).lower() in ("on", "true", "1", "yes")

    def pending_sql(end):
        return strip_comments(text[start:end], scs)

    i = 0
    while i < n:
        if (i == 0 or text[i - 1] == "\n") and not pending_sql(i):
            ls = i
            while ls < n and text[ls] in " \t":
                ls += 1
            if ls < n and text[ls] == "\\" and not text.startswith("\\;", ls):
                e = _eol(text, ls)
                cmd = text[ls:e].strip()
                if re.match(r"\\if\b", cmd):
                    end = _if_block_end(text, e + 1)
                    emit(Unit("block", text[ls:end], "", "\\if", line_of(ls)))
                else:
                    end = e + 1
                    if _META_COPY_STDIN.match(cmd):
                        end = _copy_data(text, end)
                    emit(Unit("meta", text[ls:end], "", cmd, line_of(ls)))
                i = start = min(end, n)
                depth = atomic = 0
                multi = False
                continue
        c = text[i]
        j = _skip_comment(text, i)
        if j > i:
            i = j
            continue
        j = _skip_quoted(text, i, scs)
        if j > i:
            i = j
            continue
        if (c.isalpha() or c == "_") and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] in "_$")):
            m = re.match(r"[A-Za-z_][A-Za-z0-9_$]*", text[i:i + 64])
            word = m.group(0).lower()
            if word == "begin" and depth == 0 and (atomic or _ROUTINE.match(pending_sql(i))):
                if atomic or re.match(r"begin\s+atomic\b", text[i:i + 40], re.I):
                    atomic += 1
            elif word == "case" and atomic:
                atomic += 1
            elif word == "end" and atomic:
                atomic -= 1
            i += len(word)
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth = max(0, depth - 1)
        elif c == ";" and depth == 0 and not atomic:
            sql = text[start:i + 1]
            end = i + 1
            e = _eol(text, i + 1)
            if text[i + 1:e].strip().startswith("--") or not text[i + 1:e].strip():
                # A comment on the rest of the line belongs to this statement (md: directives).
                sql_with_comment = text[start:e]
                end = min(e + 1, n)
            else:
                sql_with_comment = sql
            unit_text = sql_with_comment.lstrip("\n") + "\n"
            if _COPY_STDIN.match(strip_comments(sql, scs)):
                end = _copy_data(text, _eol(text, i + 1) + 1)
                unit_text = sql.lstrip("\n") + text[i + 1:end]
            emit(Unit("sql", unit_text, strip_comments(sql, scs), "", line_of(_first_nonspace(text, start)), multi))
            i = start = end
            multi = False
            continue
        elif c == "\\":
            if text.startswith("\\;", i):
                multi = True
                i += 2
                continue
            e = _eol(text, i)
            sql = text[start:i]
            meta = text[i:e].strip()
            if strip_comments(sql, scs):
                emit(Unit("sql", sql.lstrip("\n") + text[i:e] + "\n", strip_comments(sql, scs), meta,
                          line_of(_first_nonspace(text, start)), multi))
            else:
                emit(Unit("meta", text[i:e] + "\n", "", meta, line_of(i)))
            i = start = e + 1
            depth = atomic = 0
            multi = False
            continue
        i += 1
    return units
