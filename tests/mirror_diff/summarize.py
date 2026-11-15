#!/usr/bin/env python3
"""Markdown summary of a mirror_diff run: python3 summarize.py <workdir>/results.jsonl [run.log]

Sections: status counts per layer, data-type verdicts, query differential by outcome and
DuckDB-semantics categories, CDC results per file, and failures grouped by signature
(bug names for XFAILs, panic location or first detail words for FAILs)."""

import collections
import json
import re
import sys


def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def layer(check):
    return check.split(".", 1)[0]


ENGINE_CATEGORIES = [
    ("integer division / result type", r"/ int|/ \(?-?\d|div\b|\.5\b|/ 2"),
    ("whole-row reference in aggregate (t.*)", r"count\(\w+\.\*\)|\w+\.\* IS"),
    ("error raised by Postgres, not by DuckDB (or vice versa)", r"^heap=error|other=error"),
    ("NULL ordering / sort", r"order by .*nulls|nulls (first|last)"),
    ("collation / string comparison", r"collate|ilike|~\*|upper\(|lower\("),
    ("floating point formatting / precision", r"float|real|double|sqrt|exp\(|ln\(|log\(|power\("),
    ("numeric scale of results", r"numeric|avg\(|sum\(|round\("),
    ("date / time semantics", r"interval|timestamp|date|time\b|extract|date_part|age\("),
]


def engine_category(rec):
    text = (rec.get("sql") or "") + " " + (rec.get("sample") or "")
    if rec.get("heap") != rec.get("mirror"):
        return "error raised by only one engine"
    for name, rx in ENGINE_CATEGORIES:
        if re.search(rx, text, re.I):
            return name
    return "other"


def signature(rec):
    d = rec.get("detail", "")
    m = re.search(r"panic(?:s|ked)?(?: moonlink)?[^:]*at ([^\s:]+:\d+)", d)
    if m:
        return "panic " + re.sub(r"^.*/(src/)", r"\1", m.group(1))
    m = re.match(r"(\w+): types=\[([^\]]*)\]", d)
    if m:
        return f"{m.group(1)} types=[{m.group(2)}]"
    m = re.match(r"types=\[([^\]]*)\] events=\d+ first=(\w+)", d)
    if m:
        return f"cdc {m.group(2)} types=[{m.group(1)}]"
    return " ".join(d.split()[:6])[:90]


def main():
    recs = load(sys.argv[1])
    out = []
    by_layer = collections.defaultdict(collections.Counter)
    for r in recs:
        by_layer[layer(r["check"])][r["status"]] += 1
    out.append("## Status by layer\n")
    out.append("| layer | PASS | XPASS | FAIL | XFAIL | SKIP |")
    out.append("|---|---|---|---|---|---|")
    for lay in sorted(by_layer):
        c = by_layer[lay]
        out.append(f"| {lay} | {c['PASS']} | {c['XPASS']} | {c['FAIL']} | {c['XFAIL']} | {c['SKIP']} |")

    out.append("\n## Data-type verdicts\n")
    out.append("| type | verdict | check | detail |")
    out.append("|---|---|---|---|")
    for r in sorted((r for r in recs if r["check"].startswith("data.type.")), key=lambda r: r["check"]):
        det = r["detail"].replace("|", "\\|")[:160]
        ids = f" ({','.join(map(str, r['ids']))})" if r.get("ids") else ""
        out.append(f"| `{r.get('type', '?')}` | {r.get('verdict', '?')}{ids} | {r['check']} | {det} |")

    q = [r for r in recs if r["check"].startswith("query.")]
    out.append("\n## Query differential\n")
    qc = collections.Counter()
    for r in q:
        if r["status"] == "PASS":
            qc["pass (" + (r["detail"] or "exact") + ")"] += 1
        elif r["status"] == "SKIP":
            qc["skip: " + r["detail"].split(":")[0]] += 1
        elif r["status"] == "XFAIL" and r["detail"].startswith("engine"):
            qc["DuckDB semantics (XFAIL)"] += 1
        else:
            qc[r["status"].lower() + ": " + (r.get("verdict") or r["detail"].split(":")[0])] += 1
    out.append("| outcome | queries |")
    out.append("|---|---|")
    for k, v in qc.most_common():
        out.append(f"| {k} | {v} |")
    cats = collections.defaultdict(list)
    for r in q:
        if r["status"] == "XFAIL" and r["detail"].startswith("engine"):
            cats[engine_category(r)].append(r)
    if cats:
        out.append("\n### DuckDB-semantics categories\n")
        out.append("| category | count | example |")
        out.append("|---|---|---|")
        for k, rs in sorted(cats.items(), key=lambda kv: -len(kv[1])):
            ex = rs[0]
            out.append(f"| {k} | {len(rs)} | {ex['check']}: `{(ex.get('sql') or '')[:100]}` |")

    out.append("\n## CDC convergence per file\n")
    files = collections.defaultdict(collections.Counter)
    for r in recs:
        if r["check"].startswith("cdc."):
            files[r["check"].split(".")[1]][r["status"]] += 1
    out.append("| file | PASS | FAIL | XFAIL | SKIP |")
    out.append("|---|---|---|---|---|")
    for f in sorted(files):
        c = files[f]
        out.append(f"| {f} | {c['PASS']} | {c['FAIL']} | {c['XFAIL']} | {c['SKIP']} |")

    out.append("\n## Failures grouped by signature\n")
    groups = collections.defaultdict(list)
    for r in recs:
        if r["status"] in ("FAIL", "XFAIL") and not r["detail"].startswith("engine"):
            key = ("XFAIL(" + ",".join(map(str, r["ids"])) + ") " if r["status"] == "XFAIL" else "FAIL ") + signature(r)
            groups[key].append(r["check"])
    out.append("| group | count | checks |")
    out.append("|---|---|---|")
    for k, cs in sorted(groups.items(), key=lambda kv: (not kv[0].startswith("FAIL"), -len(kv[1]))):
        out.append(f"| {k.replace('|', '/')} | {len(cs)} | {', '.join(cs[:6])}{' ...' if len(cs) > 6 else ''} |")
    print("\n".join(out))


if __name__ == "__main__":
    main()
