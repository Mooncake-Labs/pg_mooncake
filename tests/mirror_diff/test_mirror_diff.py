#!/usr/bin/env python3
"""Unit tests of the comparison, classification, known-difference and splitting logic (no
database needed): python3 tests/mirror_diff/test_mirror_diff.py"""

import contextlib
import io
import json
import os
import sys
import tempfile
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import compare  # noqa: E402
import mirror_diff  # noqa: E402
import sqlsplit  # noqa: E402


class R:
    """Stand-in for psqlsession.Result."""

    def __init__(self, rows=None, error=False, sqlstate="", message="", timed_out=False, died=False):
        self.rows, self.error, self.sqlstate, self.message = rows or [], error, sqlstate, message
        self.timed_out, self.died = timed_out, died

    @property
    def ok(self):
        return not (self.error or self.timed_out or self.died)

    def outcome(self):
        return "died" if self.died else "timeout" if self.timed_out else "error" if self.error else "ok"


def match(a, b):
    return compare.results_match(a, b)


class Comparator(unittest.TestCase):
    def test_float_tolerance_only_for_float_types(self):
        self.assertTrue(compare.rows_match([["0.30000000000000004"]], [["0.3"]], ["double precision"])[0])
        self.assertFalse(compare.rows_match([["2.5"]], [["2.5000000000000000"]], ["numeric"])[0])
        self.assertFalse(compare.rows_match([["2.5"]], [["2.50"]], None)[0])
        self.assertFalse(compare.rows_match([["1.5"]], [["1.6"]], ["real"])[0])
        self.assertTrue(compare.rows_match([["-0"]], [["0"]], ["double precision"])[0])

    def test_order_by_enforced(self):
        items, limit = compare.top_level_order_by("SELECT a, b FROM t ORDER BY a")
        self.assertEqual((items, limit), (["a"], None))
        eq, how = compare.rows_match([["1", "x"], ["2", "y"]], [["2", "y"], ["1", "x"]], None, items, ["a", "b"])
        self.assertEqual((eq, how), (False, "order"))
        self.assertTrue(compare.rows_match([["1", "x"], ["2", "y"]], [["2", "y"], ["1", "x"]])[0])

    def test_ties_may_be_permuted(self):
        rows_a = [["1", "x"], ["1", "y"], ["2", "z"]]
        rows_b = [["1", "y"], ["1", "x"], ["2", "z"]]
        self.assertEqual(compare.rows_match(rows_a, rows_b, None, ["a"], ["a", "b"]), (True, "ties"))
        self.assertFalse(compare.rows_match(rows_a, [["1", "y"], ["1", "q"], ["2", "z"]], None, ["a"], ["a", "b"])[0])

    def test_limit_last_tie_group_lenient_only_when_cut(self):
        a = [["1", "x"], ["2", "y"]]
        b = [["1", "x"], ["2", "w"]]
        # The last group of a result LIMIT cut must be checked against the unlimited result.
        self.assertEqual(compare.rows_match(a, b, None, ["a"], ["a", "b"], limit=2), (False, "limit-unverifiable"))
        self.assertFalse(compare.rows_match(a, b, None, ["a"], ["a", "b"], limit=5)[0])
        self.assertFalse(compare.rows_match(a, b, None, ["a"], ["a", "b"], limit=None)[0])
        self.assertEqual(compare.rows_match(a, b, None, ["a"], ["a", "b"], limit=-1), (False, "limit-unverifiable"))

    def test_limit_without_order_needs_equal_rows(self):
        self.assertEqual(compare.rows_match([["7"]], [["100"]], None, None, ["c"], limit=1), (False, "limit-unverifiable"))
        self.assertTrue(compare.rows_match([["1"], ["2"]], [["2"], ["1"]], None, None, ["c"], limit=2)[0])
        self.assertEqual(compare.top_level_order_by("SELECT * FROM t FETCH FIRST 3 ROWS ONLY"), (None, 3))
        self.assertEqual(compare.top_level_order_by("SELECT * FROM t ORDER BY a LIMIT (SELECT 1)"), (["a"], -1))

    def test_order_by_only_top_level(self):
        sql = "SELECT * FROM (SELECT a FROM t ORDER BY a) s"
        self.assertEqual(compare.top_level_order_by(sql), (None, None))
        sql = "SELECT a, 'order by x' FROM t ORDER BY 2 DESC NULLS LAST, a LIMIT 5"
        self.assertEqual(compare.top_level_order_by(sql), (["2 DESC NULLS LAST", "a"], 5))
        self.assertEqual(compare.order_key_columns(["2 DESC NULLS LAST", "t.a"], ["a", "b"]), [1, 0])
        self.assertEqual(compare.order_key_columns(["f(a)", "a"], ["a"]), [])

    def test_both_error_needs_same_error(self):
        same = R(error=True, sqlstate="22012", message="ERROR:  division by zero")
        duck = R(error=True, sqlstate="22012", message="ERROR:  (PGDuckDB/Duckdb_ExecCustomScan) division by zero")
        other = R(error=True, sqlstate="42883", message="ERROR:  operator does not exist")
        self.assertEqual(match(same, duck), (True, "same-error"))
        self.assertEqual(match(same, other), (False, "different-error"))
        self.assertFalse(match(R(timed_out=True), R(timed_out=True))[0])
        self.assertFalse(match(R(died=True), R(died=True))[0])

    def test_limit_subset_helpers(self):
        self.assertEqual(compare.strip_top_level_limit("SELECT * FROM (SELECT a FROM t LIMIT 2) s LIMIT 3 OFFSET 1;"),
                         "SELECT * FROM (SELECT a FROM t LIMIT 2) s")
        self.assertIsNone(compare.strip_top_level_limit("SELECT a FROM t"))
        self.assertTrue(compare.rows_subset([["2"]], [["1"], ["2"]]))
        self.assertFalse(compare.rows_subset([["7"]], [["100"]]))

    def test_json_keeps_numbers_and_duplicate_keys(self):
        c = compare.canonical_json
        self.assertEqual(c('{"b": 1, "a": [1, 2]}'), c('{"a":[1,2],"b":1}'))
        self.assertNotEqual(c('{"price": 0.1234567890123456789012345}'), c('{"price":0.12345678901234568}'))
        self.assertNotEqual(c('[1e400]'), c('[1e999]'))
        self.assertNotEqual(c('{"a": 1, "a": 2}'), c('{"a": 2}'))
        self.assertEqual(c('{"a": 1, "a": 2}'), c('{"a":1,"a":2}'))

    def test_struct_text_is_converted_and_compared(self):
        c = compare.struct_to_row
        self.assertEqual(c("{'f1': 1, 'f2': foo}"), "(1,foo)")
        self.assertEqual(c("{'r': 11.0, 'i': NULL}"), "(11,)")
        self.assertEqual(c('{"{\'if1\': NULL, \'if2\': [foo, bar]}"}'), '{"(,\\"{foo,bar}\\")"}')
        self.assertIsNone(c("{'f1': 1, 'f2'"))
        same = compare.divergence_events([("c", "comptype")], [["(1,foo)"]], [["{'f1': 1, 'f2': foo}"]])
        self.assertEqual([e["kind"] for e in same], ["struct-text"])
        wrong = compare.divergence_events([("c", "comptype")], [["(1,foo)"]], [["{'f1': 2, 'f2': foo}"]])
        self.assertEqual([e["kind"] for e in wrong], ["value"])

    def test_numeric_scale_is_compared_as_numbers(self):
        same = compare.divergence_events([("n", "numeric"), ("a", "numeric[]")],
                                         [["1", "{0,1}"], ["-0.000001", "{NULL,2.5}"]],
                                         [["1.0000000000", "{0.0000000000,1.0000000000}"],
                                          ["-0.0000010000", "{NULL,2.5000000000}"]])
        self.assertEqual([e["kind"] for e in same], ["numeric-scale", "numeric-scale"])
        self.assertIn("numeric-scale col=n type=numeric: 2 value(s) equal the heap's as numbers", same[0]["detail"])
        wrong = compare.divergence_events([("n", "numeric")], [["1"], ["2"]], [["1.0000000000"], ["3.0000000000"]])
        self.assertEqual([e["kind"] for e in wrong], ["value"])
        nan = compare.divergence_events([("n", "numeric")], [["NaN"]], [["0.0000000000"]])
        self.assertEqual([e["kind"] for e in nan], ["value"])

    def test_numeric_scale_is_exact(self):
        # 30 significant digits: Decimal's 28-digit context would round both to the same value.
        heap, mirror = "12345678901234567890123456789.1234567891", "12345678901234567890123456789.1234567892"
        ev = compare.divergence_events([("n", "numeric")], [[heap]], [[mirror]])
        self.assertEqual([e["kind"] for e in ev], ["value"])
        ev = compare.divergence_events([("n", "numeric")], [[heap[:-1]]], [[heap[:-1] + "0"]])
        self.assertEqual([e["kind"] for e in ev], ["numeric-scale"])

    def test_numeric_scale_only_for_unconstrained_numeric_printed_as_decimal_38_10(self):
        def kinds(typ, heap, mirror):
            return [e["kind"] for e in compare.divergence_events([("n", typ)], [[heap]], [[mirror]])]
        self.assertEqual(kinds("numeric(10,2)", "1.00", "1.0000000000"), ["value"])
        self.assertEqual(kinds("numeric", "1", "1.000"), ["value"])
        self.assertEqual(kinds("numeric", "0", "-0.0000000000"), ["value"])
        self.assertEqual(kinds("numeric", "-0.5", "-0.5000000000"), ["numeric-scale"])
        self.assertEqual(kinds("numeric[]", "{1,NULL}", "{1.0000000000,NULL}"), ["numeric-scale"])
        self.assertEqual(kinds("numeric[]", "{1,2}", "{1.0000000000,2.00}"), ["value"])
        self.assertEqual(kinds("numeric[]", "{{1},{2}}", "{{1.0000000000},{2.0000000000}}"), ["value"])

    def test_char_padding_only_when_the_blanks_are_all_that_is_missing(self):
        def kinds(typ, heap, mirror):
            return [e["kind"] for e in compare.divergence_events([("c", typ)], heap, mirror)]
        self.assertEqual(kinds("character(4)", [["a   "], ["ab  "]], [["a"], ["ab"]]), ["char-padding"])
        self.assertEqual(kinds("character(4)", [["ab  "]], [["xy"]]), ["value"])
        self.assertEqual(kinds("character(4)", [["ab  "]], [["ab "]]), ["value"])
        self.assertEqual(kinds("character(4)", [["ab"]], [["ab  "]]), ["value"])
        self.assertEqual(kinds("character varying(4)", [["ab  "]], [["ab"]]), ["value"])
        self.assertEqual(kinds("character(5)[]", [['{"abc  ",abcde}']], [["{abc,abcde}"]]), ["char-padding"])
        self.assertEqual(kinds("character(5)[]", [['{"abc  ",abcde}']], [["{abd,abcde}"]]), ["value"])

    def test_divergence_events(self):
        ev = compare.divergence_events([("id", "integer"), ("c", "character(3)")],
                                       [["1", "ab "], ["2", "x  "]], [["1", "ab"], ["2", "x  "]])
        self.assertEqual([e["kind"] for e in ev], ["char-padding"])
        self.assertIn("char-padding col=c type=character(3): 1 value(s) equal the heap's without trailing blanks",
                      ev[0]["detail"])
        ev = compare.divergence_events([("id", "integer"), ("c", "text")], [["1", "ab "]], [["1", "ab"]])
        self.assertEqual([e["kind"] for e in ev], ["value"])
        self.assertIn("col=c type=text heap-only: ab ", ev[0]["detail"])
        ev = compare.divergence_events([("id", "integer")], [["1"], ["2"]], [["1"]])
        self.assertTrue(ev[0]["detail"].startswith("row-count heap=2 mirror=1"))


class Classifier(unittest.TestCase):
    def test_duckdb_on_heap_equal_heap_is_mirror_bug_even_if_order_dependent(self):
        heap, mirror, duck = R([["1"], ["2"]]), R([["1"], ["3"]]), R([["1"], ["2"]])
        self.assertEqual(compare.classify(heap, mirror, duck, True, match, True)[0], "FAIL")

    def test_engine_semantics(self):
        heap, mirror, duck = R([["2.5"]]), R([["2"]]), R([["2"]])
        self.assertEqual(compare.classify(heap, mirror, duck, True, match, False)[0], "ENGINE")

    def test_order_dependent_only_when_duckdb_also_differs(self):
        heap, mirror, duck = R([["1"], ["2"]]), R([["2"], ["9"]]), R([["2"], ["7"]])
        self.assertEqual(compare.classify(heap, mirror, duck, True, match, True)[0], "ENGINE")
        self.assertEqual(compare.classify(heap, mirror, duck, True, match, False)[0], "FAIL")

    def test_planning_error_skips_but_read_path_error_fails(self):
        heap = R([["1"]])
        plan = R(error=True, sqlstate="XX000", message="(PGDuckDB/CreatePlan) Binder Error: no function foo")
        self.assertEqual(compare.classify(heap, plan, plan, True, match, False)[0], "SKIP")
        io_err = R(error=True, sqlstate="XX000", message="(PGDuckDB/Duckdb_ExecCustomScan) IO Error: moonlink RPC early eof")
        self.assertEqual(compare.classify(heap, io_err, R([["1"]]), True, match, False)[0], "FAIL")
        self.assertEqual(compare.classify(heap, io_err, R(error=True, message="x"), False, match, False)[0], "FAIL")

    def test_role_and_createplan_errors_skip(self):
        heap = R([["1"]])
        role = R(error=True, message="DuckDB execution is not allowed because you have not been granted the duckdb.postgres_role")
        self.assertEqual(compare.classify(heap, role, R([["1"]]), False, match, False)[0], "SKIP")
        plan = R(error=True, message="(PGDuckDB/CreatePlan) Prepared query returned an error: Invalid Input Error: x")
        self.assertEqual(compare.classify(heap, plan, R([["1"]]), False, match, False)[0], "SKIP")

    def test_order_dependent_is_narrow(self):
        self.assertFalse(compare.order_dependent("SELECT sum(x) OVER () FROM t"))
        self.assertFalse(compare.order_dependent("SELECT sum(x) OVER (PARTITION BY a ORDER BY b) FROM t"))
        self.assertTrue(compare.order_dependent("SELECT row_number() OVER (PARTITION BY a) FROM t"))
        self.assertTrue(compare.order_dependent("SELECT sum(x) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t"))
        self.assertTrue(compare.order_dependent("SELECT sum(x) OVER w FROM t WINDOW w AS (ORDER BY a RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES)"))
        self.assertFalse(compare.order_dependent("SELECT first_value(x) OVER w FROM t WINDOW w AS (ORDER BY a)"))


EXPECTED = """
engine duckdb-semantics
bug duckdb-semantics class=limitation DuckDB semantics
bug unsupported-types class=limitation rejected cleanly
bug char-padding-lost class=blocking char(n) loses padding
check cdc.* char-padding-lost ^(copy|dml): value col=\\w+ type=character\\(\\d+\\)
check data.copy.* unsupported-types ^rejected: column \\w+ type=point
check data.type.boolean char-padding-lost
"""


class Expectations(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "expected.txt")
        with open(self.path, "w") as f:
            f.write(EXPECTED)
        self.exp = mirror_diff.Expectations(self.path)

    def rep(self):
        rep = mirror_diff.Reporter(self.exp, os.path.join(self.dir, "r.jsonl"))
        self.addCleanup(rep.log.close)
        return rep

    def quiet(self, fn, *a, **kw):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            res = fn(*a, **kw)
        return res, out.getvalue()

    def test_every_event_must_match(self):
        good = [{"text": "copy: value col=c type=character(3) heap-only: a || mirror-only: b :: x"}]
        bad = good + [{"text": "dml: row-count heap=2 mirror=1 :: DELETE FROM t"}]
        self.assertEqual(self.exp.match_events("cdc.f.t", good), ["char-padding-lost"])
        self.assertIsNone(self.exp.match_events("cdc.f.t", bad))

    def test_limitation_xfail_does_not_fail_gate(self):
        rep = self.rep()
        _, out = self.quiet(rep.result, "data.copy.x.t", "FAIL", "rejected: column p type=point typtype=b: unsupported")
        self.quiet(rep.result, "data.copy.x.u", "PASS")
        self.assertIn("XFAIL(limitation:unsupported-types)", out)
        (gate, regression), out = self.quiet(rep.summary, "17", "data", True)
        self.assertEqual((gate, regression), (4, 0))  # a blocking bug is listed: BLOCKED
        self.assertIn("xfail_limitation=1", out)

    def test_blocking_xfail_fails_gate_not_regression(self):
        rep = self.rep()
        self.quiet(rep.result, "cdc.f.t", "FAIL", "x", events=[{"text": "dml: value col=c type=character(1) :: u"}])
        (gate, regression), _ = self.quiet(rep.summary, "17", "cdc", True)
        self.assertEqual((gate, regression), (1, 0))

    def test_unverified_skip_fails(self):
        rep = self.rep()
        self.quiet(rep.result, "cdc.f.t", "SKIP", "unverified: moonlink wedged", unverified=True)
        self.quiet(rep.result, "cdc.f.u", "PASS")
        (gate, regression), out = self.quiet(rep.summary, "17", "cdc", True)
        self.assertEqual((gate, regression), (1, 1))
        self.assertIn("unverified=1", out)

    def test_exact_name_rule_reports_xpass(self):
        rep = self.rep()
        _, out = self.quiet(rep.result, "data.type.boolean", "PASS")
        self.assertIn("XPASS(char-padding-lost)", out)

    def test_versioned_names_match_base_rule(self):
        self.assertEqual(self.exp.match("data.copy.x.t.s2", "rejected: column p type=point x"), ["unsupported-types"])

    def test_duplicate_bug_is_an_error(self):
        with open(self.path, "a") as f:
            f.write("bug duckdb-semantics class=limitation again\n")
        with self.assertRaises(SystemExit):
            mirror_diff.Expectations(self.path)


class Splitter(unittest.TestCase):
    def kinds(self, text):
        return [(u.kind, u.sql or u.meta) for u in sqlsplit.split(text)]

    def test_begin_atomic(self):
        text = ("CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql\nBEGIN ATOMIC\n  SELECT CASE WHEN a > 0 THEN 1 END;\n"
                "  SELECT 2;\nEND;\nSELECT f(1);\n")
        units = sqlsplit.split(text)
        self.assertEqual(len(units), 2)
        self.assertTrue(units[0].sql.endswith("END;"))
        self.assertEqual(units[1].line, 6)

    def test_backslash_semicolon_joins_statements(self):
        units = sqlsplit.split("SELECT 1 \\; SELECT 2;\nSELECT 3;\n")
        self.assertEqual([u.multi for u in units], [True, False])

    def test_shell_escape_is_meta_and_skipped(self):
        units = sqlsplit.split("\\! echo hi\nSELECT 1;\n")
        self.assertEqual(units[0].kind, "meta")
        self.assertTrue(mirror_diff.SKIP_META.match(units[0].meta))

    def test_standard_conforming_strings_off(self):
        text = "SET standard_conforming_strings = off;\nSELECT 'a\\';b';\nRESET standard_conforming_strings;\nSELECT 'c\\'; SELECT 4;\n"
        sqls = [u.sql for u in sqlsplit.split(text)]
        self.assertEqual(sqls[1], "SELECT 'a\\';b';")
        self.assertEqual(sqls[3:], ["SELECT 'c\\';", "SELECT 4;"])


# ---- scenarios that passed wrongly at an earlier head; each must now fail the check ----------------
HERE = os.path.dirname(os.path.abspath(__file__))


class Srv:
    def __init__(self, managed=False):
        self.lines, self.crashes, self.managed = [], [], managed
        self.on_health = None

    def panics(self):
        return len(self.lines)

    def panic_lines(self):
        return list(self.lines)

    def backend_crashes(self):
        return list(self.crashes)

    def cancel(self, pid):
        pass

    def recover(self, db):
        return True

    def healthy(self, db, why=None):
        if self.on_health:
            self.on_health()
        return True

    def stop(self):
        pass

    def setup(self, label):
        pass


_REPORTERS = []


def tearDownModule():
    for rep in _REPORTERS:
        rep.log.close()


def stub_harness(managed=False):
    d = tempfile.mkdtemp()
    exp = mirror_diff.Expectations(os.path.join(HERE, "expected.txt"))
    rep = mirror_diff.Reporter(exp, os.path.join(d, "r.jsonl"))
    _REPORTERS.append(rep)
    h = object.__new__(mirror_diff.Harness)
    h.__dict__.update(
        args=types.SimpleNamespace(converge_secs=0.6, stmt_timeout=5, ddl_timeout=5, file_timeout=999,
                                   max_rejected_values=40, keep=False),
        exp=exp, rep=rep, srv=Srv(managed), diverged_cols={}, crash_types={}, good_types=set(), panics_seen=0,
        crashes_seen=0, last_events=[], dbs=["md_f"], probe_seq=0, last_verdict=None, seen=set())
    h.dbname = lambda n, suffix="": "md_" + n
    h.drop_db = lambda db: None
    h.template = lambda: None
    return h


def statuses(h):
    h.rep.log.flush()
    with open(h.rep.log.name) as f:
        return {json.loads(line)["check"]: json.loads(line)["status"] for line in f}


def quiet(fn, *a, **kw):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **kw)


def query_run(sql, heap, mirror, duck, full, names, typs):
    """Run handle_query on stub sessions: heap/mirror/DuckDB-on-heap results and the heap result of
    the statement without its LIMIT (None: that query errors)."""
    h = stub_harness()
    fr = mirror_diff.FileRun(h, "f")
    fr.s = types.SimpleNamespace(run=lambda *a, **k: R(), alive=lambda: True, kill=lambda: None)
    for name in ("t", "onek"):
        fr.tables[name] = {"cols": [("a", "integer", "b", True, "N", 23)], "mirrored": True, "bad_cols": set(),
                           "lossy_all": False}
    fr.txn_state = lambda: (False, False)
    unlimited = full if full is not None else R(error=True, sqlstate="57014", message="canceling statement")
    fr.run = lambda text, timeout=None: heap if "LIMIT" in text.upper() else unlimited
    fr.shadow = lambda unit, mirror=None, in_txn=None, _m=mirror: _m if mirror else duck
    fr.ran_in_duckdb = lambda unit, in_txn: True
    fr.describe = lambda unit, in_txn: (names, typs)
    unit = sqlsplit.split(sql + "\n")[0]
    quiet(fr.handle_query, unit)
    quiet(fr.report)
    return statuses(h)["query.f.L1"]


class FalsePassScenarios(unittest.TestCase):
    def test_unmapped_order_by_limit_is_not_a_subset_pass(self):
        full = R([[str(i)] for i in range(1, 101)])
        for sql in ("SELECT b FROM t ORDER BY a LIMIT 3;", "SELECT b FROM t ORDER BY -a LIMIT 3;"):
            st = query_run(sql, R([["1"], ["2"], ["3"]]), R([["98"], ["99"], ["100"]]), R([["1"], ["2"], ["3"]]),
                           full, ["b"], ["integer"])
            self.assertEqual(st, "FAIL", sql)

    def test_last_sort_group_under_limit_is_checked(self):
        heap = R([["1", "a"], ["2", "b"], ["3", "c"]])
        st = query_run("SELECT unique1, stringu1 FROM onek ORDER BY unique1 LIMIT 3;", heap,
                       R([["1", "a"], ["2", "b"], ["3", "ZZ"]]), heap, R(heap.rows + [["4", "d"]]),
                       ["unique1", "stringu1"], ["integer", "name"])
        self.assertEqual(st, "FAIL")

    def test_unreadable_unlimited_result_falls_back_to_classify(self):
        st = query_run("SELECT a FROM t LIMIT 1;", R([["100"]]), R([["7"]]), R([["100"]]), None, ["a"], ["integer"])
        self.assertEqual(st, "FAIL")

    def test_composite_rejection_is_not_a_limitation(self):
        # A composite of supported fields can be stored, so refusing it is a bug.
        refused = "source table public.t cannot be mirrored: column c (mycomp) has a type mirrors cannot store"
        h = stub_harness()
        quiet(h.rep.result, "data.copy.f.t", "FAIL", f"rejected: types=[mycomp] column c type=mycomp typtype=c: {refused}")
        quiet(h.rep.result, "cdc.f.t", "FAIL", "x", events=[{"text": "create: create_table rejected: column c type=mycomp "
              f"typtype=c: {refused} :: CREATE TABLE t (c mycomp);"}])
        quiet(h.rep.result, "data.type.f.mycomp_array", "FAIL", "x", events=[{"text": "rejected: type=mycomp[] typtype=b "
              "2 values (f); copy=rejected (column v type=mycomp[] typtype=b: source table public.md_ps1 cannot be "
              "mirrored: column v (mycomp[]) has a type mirrors cannot store)"}])
        quiet(h.rep.result, "data.copy.f.u", "FAIL", f"rejected: types=[mycomp] {refused}")
        self.assertEqual(set(statuses(h).values()), {"FAIL"})

    def test_new_panic_site_not_hidden_by_a_listed_one(self):
        h = stub_harness()
        run = types.SimpleNamespace(recover=lambda: None)

        def q(sql, timeout=None):
            if "create_table" in sql:
                h.srv.lines += ["thread 'x' panicked at moonlink/src/moonlink/src/new_site.rs:1:1:",
                                "thread 'x' panicked at moonlink/src/moonlink/src/row/arrow_converter.rs:108:13:"]
            return R()
        run.q = q
        verdict, detail = h.mirror_and_compare(run, "public.t", "t", "md.t", "a",
                                               [("a", "integer[]", "b", True, "A", 1007)])
        quiet(h.rep.result, "data.copy.arrays.t", "FAIL", f"{verdict}: types=[integer[]] {detail}")
        self.assertEqual(statuses(h)["data.copy.arrays.t"], "FAIL")

    def test_earlier_crash_not_hidden_by_a_listed_one(self):
        h = stub_harness()
        fr = mirror_diff.FileRun(h, "timestamptz")
        listed = ('LOG:  server process (PID 2) was terminated by signal 6: Aborted',
                  'TRAP: failed Assert("IsTransactionState()"), File: "relcache.c", Line: 2052, PID: 2')

        def died(text, timeout=None, on_timeout=None):
            h.srv.crashes += [("LOG:  server process (PID 1) was terminated by signal 11: Segmentation fault", ""), listed]
            return R(died=True)
        fr.s = types.SimpleNamespace(run=died, alive=lambda: True, kill=lambda: None)
        fr.new_session = lambda: None
        quiet(fr.run, "COMMIT;")
        self.assertEqual(statuses(h)["backend.timestamptz.L0"], "FAIL")

    def test_panic_during_health_check_or_shutdown_is_swept(self):
        h = stub_harness()
        h.srv.on_health = lambda: h.srv.lines.append("thread 'x' panicked at moonlink/src/moonlink/src/late.rs:1:1:")
        quiet(h.end, "run", "f")
        self.assertEqual(statuses(h).get("panic.run.f"), "FAIL")

    def test_fresh_log_baseline_is_zero(self):
        h = stub_harness(managed=True)
        h.srv.lines.append("thread 'x' panicked at moonlink/src/moonlink/src/fixture_time.rs:1:1:")
        quiet(h.begin, "run-f")
        quiet(h.end, "run", "f")
        self.assertEqual(statuses(h).get("panic.run.f"), "FAIL")

    def test_composite_value_diff_is_not_the_struct_limitation(self):
        h = stub_harness()
        for heap, mirror in (("(1,foo)", "{'f1': 1, 'f2': foo}"), ("(1,foo)", "{'f1': 9, 'f2': foo}")):
            ev = compare.divergence_events([("c1", "comptype")], [[heap]], [[mirror]])
            quiet(h.rep.result, f"cdc.arrays.t{mirror[7]}", "FAIL", "x",
                  events=[{"text": f"dml: {e['detail']} :: INSERT INTO t VALUES (1)"} for e in ev])
        self.assertEqual(statuses(h), {"cdc.arrays.t1": "XFAIL_LIMITATION", "cdc.arrays.t9": "FAIL"})

    def test_data_type_rules_pinned_to_their_event(self):
        h = stub_harness()
        other = "cdc_delete=errored (mirror scan error: boom)"
        for check, base in (("data.type.jsonb", "equivalent: type=jsonb typtype=b 5 values (jsonb)"),
                            ("data.type.character_4", "lossy: type=character(4) typtype=b 4 values (char)"),
                            ("data.type.integer_array", "crashed: type=integer[] typtype=b 3 values (arrays)")):
            quiet(h.rep.result, check, "FAIL", "x", events=[{"text": f"{base}; {other}"}])
        quiet(h.rep.result, "data.type.smallint_array", "FAIL", "x", events=[{"text": "crashed: type=smallint[] typtype=b "
              "3 values (arrays); copy=crashed (create_table panicked moonlink at moonlink/src/moonlink/src/new_site.rs:1:1)"}])
        self.assertEqual(set(statuses(h).values()), {"FAIL"})




class RealExpectedRules(unittest.TestCase):
    """The rules of the committed expected.txt: what they must and must not turn into an XFAIL.
    Signatures are taken from a run on PostgreSQL 17."""

    @classmethod
    def setUpClass(cls):
        cls.exp = mirror_diff.Expectations(os.path.join(HERE, "expected.txt"))

    def bugs(self, check, text):
        if check.startswith(("data.", "cdc.")):
            return self.exp.match_events(check, [{"text": text}])
        return self.exp.match(check, text)

    def assertBugs(self, check, text, bugs):
        self.assertEqual(self.bugs(check, text), bugs, text)

    def assertStaysFail(self, check, text):
        self.assertIsNone(self.bugs(check, text), text)

    REFUSED = "source table public.t cannot be mirrored: column c ({}) has a type mirrors cannot store"

    def test_refused_types(self):
        for typ in ("point", "interval", "bit varying(5)", "time(2) with time zone", "int4range", "int8range[]",
                    "numeric(210,10)", "numeric(3,-3)", "numeric(3,6)", "dcomptype[]", "rposint"):
            refused = self.REFUSED.format(typ)
            self.assertBugs("data.copy.f.t", f"rejected: types=[{typ}] column c type={typ} typtype=b: {refused}",
                            ["unsupported-types"])
            self.assertBugs("data.type.f.t", f"rejected: type={typ} typtype=b 3 values (f); copy=rejected (column v "
                            f"type={typ} typtype=b: {refused})", ["unsupported-types"])
            self.assertBugs("cdc.f.t", f"create: create_table rejected: column c type={typ} typtype=b: {refused} "
                            ":: CREATE TABLE t (c x);", ["unsupported-types"])

    def test_other_refused_types_stay_fail(self):
        for typ in ("hstore", "numeric(2,5)", "integer", "mycomp", "integer[]"):
            refused = self.REFUSED.format(typ)
            self.assertStaysFail("data.copy.f.t", f"rejected: types=[{typ}] column c type={typ} typtype=b: {refused}")
            self.assertStaysFail("cdc.f.t", f"create: create_table rejected: column c type={typ} typtype=b: {refused} :: x")

    def test_refused_table_shapes(self):
        for reason in ("it has inheritance children, whose changes are not replicated with it",
                       'generated columns ("twice") are not replicated',
                       "it is partitioned and has no primary key (postgres would reject updates and deletes on "
                       "partitions created later until their replica identity is set); add a primary key",
                       "its partition p is unlogged; only regular (logged) partitions are supported"):
            self.assertBugs("cdc.f.t", f"create: create_table rejected: source table public.t cannot be mirrored: "
                            f"{reason} :: CREATE TABLE t (a int);", ["unsupported-table-shapes"])
            self.assertBugs("data.copy.f.t", f"rejected: types=[integer] source table public.t cannot be mirrored: "
                            f"{reason}", ["unsupported-table-shapes"])
        self.assertBugs("cdc.f.t", "create: create_table rejected: not supported for unlogged tables. :: CREATE UNLOGGED "
                        "TABLE t (a int)", ["unsupported-table-shapes"])
        self.assertStaysFail("cdc.f.t", "dml: mirror read failed: error XX000 not supported for unlogged tables. :: x")
        self.assertStaysFail("query.f.L1", "mirror differs: not supported for unlogged tables. :: SELECT 1")
        self.assertStaysFail("cdc.f.t", "create: create_table rejected: source table public.t cannot be mirrored: it "
                             "has a column named xmin :: x")

    def test_values_that_cannot_be_stored(self):
        cases = [
            ("numeric-not-storable", "numeric", "numeric", "NaN", "NaN cannot be stored in a decimal column"),
            ("numeric-not-storable", "numeric(4,4)", "numeric", "-Infinity", "Infinity cannot be stored in a decimal column"),
            ("numeric-not-storable", "numeric", "numeric", "0.000000000001",
             "it has more than 10 fractional digits (stored as decimal(38, 10))"),
            ("numeric-not-storable", "numeric", "numeric", "9000000000000000000000000000000000000000",
             "it does not fit decimal(38, 10)"),
            ("infinite-dates", "timestamp(2) with time zone", "timestamptz", "-infinity",
             "'-infinity' (infinite date or timestamp) cannot be stored"),
            ("infinite-dates", "date", "date", "infinity", "'infinity' (infinite date or timestamp) cannot be stored"),
            ("multidim-arrays", "integer[]", "integer[]", "{{1,2},{3,4}}",
             "invalid array: multi-dimensional arrays and arrays with explicit bounds are not supported"),
        ]
        for bug, typ, ml_typ, value, reason in cases:
            self.assertBugs("data.type.f.t", f"rejected-values: type={typ} typtype=b 3 values (f); value rejected at "
                            f"create_table: {reason} [{value}]", [bug])
            self.assertBugs("data.copy.f.t", f"rejected: types=[{typ}] column c type={typ} typtype=b: conversion "
                            f"error: column c ({ml_typ}): cannot convert '{value}': {reason}", [bug])
            self.assertBugs("cdc.f.t", f"create: create_table rejected: column c type={typ} typtype=b: conversion "
                            f"error: column c ({ml_typ}): cannot convert '{value}': {reason} :: INSERT INTO t ...", [bug])
            self.assertBugs("cdc.f.t", "dml: mirror read failed: error XX000 (PGDuckDB/Duckdb_ExecCustomScan_Cpp) "
                            "Error: mirror md_f.mc.t is out of sync: a change from the source table could not be applied: "
                            f"column c ({ml_typ}): cannot convert '{value}': {reason}; drop and re-create it :: INSERT", [bug])
        # NaN and Infinity are numeric-only reasons.
        self.assertStaysFail("data.type.f.t", "rejected-values: type=double precision typtype=b 3 values (f); value "
                             "rejected at create_table: NaN cannot be stored in a decimal column [NaN]")

    def test_source_changes_that_error_a_mirror(self):
        prefix = "dml: mirror read failed: error XX000 (PGDuckDB/Duckdb_ExecCustomScan_Cpp) Error: mirror md_f.mc.t is out of sync: "
        for reason in ("source table public.t was truncated",
                       "the source table changed: it gained inheritance children, whose changes are not replicated with it",
                       "the source table changed: partition p1 already held rows when it was added; only empty "
                       "partitions can be added"):
            self.assertBugs("cdc.f.t", f"{prefix}{reason}; drop and re-create it :: TRUNCATE t", ["source-change-errors"])
            self.assertStaysFail("query.f.L1", f"mirror read failed: error XX000 Error: mirror md_f.mc.t is out of sync: "
                                 f"{reason} :: SELECT * FROM t")
        self.assertStaysFail("cdc.f.t", f"{prefix}the source table changed: column a was dropped and re-added; drop "
                             "and re-create it :: ALTER TABLE t")

    def test_numeric_scale_and_char_padding_events(self):
        scale = "numeric-scale col=v type=numeric: 3 value(s) equal the heap's as numbers, e.g. heap 1 mirror 1.0000000000"
        pad = "char-padding col=v type=character(4): 2 value(s) equal the heap's without trailing blanks, e.g. heap 'a   ' mirror 'a'"
        for event, bug in ((scale, "numeric-scale-padded"), (pad, "char-padding-lost")):
            self.assertBugs("data.type.f.t", f"lossy: type=x typtype=b 3 values (f); copy=lossy ({event})", [bug])
            self.assertBugs("data.copy.f.t", f"lossy: types=[x] {event}", [bug])
            self.assertBugs("cdc.f.t", f"dml: {event} :: INSERT INTO t VALUES (1)", [bug])
        self.assertStaysFail("cdc.f.t", "dml: numeric-scale col=v type=numeric(10,2): 1 value(s) equal the heap's as "
                             "numbers, e.g. heap 1.00 mirror 1.0000000000 :: INSERT")
        self.assertStaysFail("cdc.f.t", "dml: value col=v type=character(4) heap-only: ab   || mirror-only: xy :: INSERT")
        self.assertStaysFail("cdc.f.t", "dml: value col=v type=numeric heap-only: "
                             "12345678901234567890123456789.1234567891 || mirror-only: "
                             "12345678901234567890123456789.1234567892 :: INSERT")

    def test_enum_order(self):
        self.assertBugs("query.enum.L155", "mirror differs and DuckDB did not run the query on the heap tables: 1 vs 1 "
                        "rows; heap-only: red || mirror-only: blue :: SELECT min(col) FROM enumtest;",
                        ["enum-order-as-text"])
        self.assertStaysFail("query.enum.L155", "mirror differs and DuckDB did not run the query on the heap tables: 2 "
                             "vs 1 rows; heap-only: red || mirror-only: blue :: SELECT min(col) FROM enumtest;")

if __name__ == "__main__":
    unittest.main()
