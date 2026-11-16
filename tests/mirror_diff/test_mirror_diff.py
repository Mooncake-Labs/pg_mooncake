#!/usr/bin/env python3
"""Unit tests of the comparison, classification, known-difference and splitting logic (no
database needed): python3 tests/mirror_diff/test_mirror_diff.py"""

import contextlib
import io
import os
import sys
import tempfile
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
        self.assertTrue(compare.rows_match(a, b, None, ["a"], ["a", "b"], limit=2)[0])
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

    def test_divergence_events(self):
        ev = compare.divergence_events([("id", "integer"), ("c", "character(3)")],
                                       [["1", "ab "], ["2", "x  "]], [["1", "ab"], ["2", "x  "]])
        self.assertEqual([e["kind"] for e in ev], ["value"])
        self.assertIn("col=c type=character(3) heap-only: ab ", ev[0]["detail"])
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


if __name__ == "__main__":
    unittest.main()
