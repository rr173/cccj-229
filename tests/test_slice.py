"""Re-runnable local tests for the offline metric slice refresh pipeline.

Every batch applied through the incremental engine is checked against an
independent whole-computation baseline (``baseline_root_groups``) that scans
all facts for every metric and shares no code with the refresh
implementation.  Run from the repository root:

    python3 -m unittest
"""
from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from metric_analyzer import slices, store  # noqa: E402

D1 = "2026-10-01"
D2 = "2026-10-02"
D3 = "2026-10-03"


# ------------------------------------------------------------- test catalog

def _eq_status(value):
    return {"op": "cmp", "cmp": "=",
            "left": {"op": "field", "name": "orders.status"},
            "right": {"op": "lit", "value": value}}


def _sum(field):
    return {"op": "sum", "arg": {"op": "field", "name": field}}


def _ref(name, version="v1"):
    return {"op": "metric", "name": name, "version": version}


def _metric(name, grain, filt, agg, version="v1"):
    return {"name": name, "version": version, "effective_from": "2026-01-01",
            "grain": grain, "filter": filt, "agg": agg}


def make_catalog():
    """One catalog with good, broken and cyclic metrics; slice-init must only
    care about the root's own closure."""
    return {
        "catalog_id": "slice-cat",
        "dimensions": [
            {"name": "day", "field": "orders.day"},
            {"name": "region", "field": "orders.region"},
            {"name": "channel", "field": "orders.channel"},
            {"name": "flag", "field": "orders.flagged"},
        ],
        "fields": [
            {"name": "orders.amount", "table": "orders", "type": "number"},
            {"name": "orders.cost", "table": "orders", "type": "number"},
            {"name": "orders.status", "table": "orders", "type": "string"},
            {"name": "orders.region", "table": "orders", "type": "string"},
            {"name": "orders.channel", "table": "orders", "type": "string"},
            {"name": "orders.day", "table": "orders", "type": "date"},
            {"name": "orders.customer", "table": "orders", "type": "string"},
            {"name": "orders.flagged", "table": "orders", "type": "boolean"},
        ],
        "metrics": [
            _metric("rev_daily", ["day", "region"], _eq_status("completed"),
                    _sum("orders.amount")),
            _metric("rev_region", ["region"], _eq_status("completed"),
                    _sum("orders.amount")),
            _metric("rev_total", [], _eq_status("completed"),
                    _sum("orders.amount")),
            # parent with two dependencies, one of them coarser ([region])
            _metric("net_daily", ["day", "region"], None,
                    {"op": "sub", "args": [_ref("rev_daily"),
                                           _ref("rev_region")]}),
            # direct aggregate mixed with a same-grain dependency
            _metric("margin_daily", ["day", "region"], _eq_status("completed"),
                    {"op": "sub", "args": [_ref("rev_daily"),
                                           _sum("orders.cost")]}),
            _metric("cust_region", ["region"], _eq_status("completed"),
                    {"op": "count_distinct",
                     "arg": {"op": "field", "name": "orders.customer"}}),
            # add over two dependencies
            _metric("mix_region", ["region"], None,
                    {"op": "add", "args": [_ref("rev_region"),
                                           _ref("cust_region")]}),
            # dependency on the empty grain
            _metric("net_vs_total", ["region"], None,
                    {"op": "sub", "args": [_ref("rev_region"),
                                           _ref("rev_total")]}),
            # same facts as rev_daily but with reordered grain
            _metric("rev_region_day", ["region", "day"],
                    _eq_status("completed"), _sum("orders.amount")),
            # projection must follow dimension names, not key prefixes
            _metric("swap_daily", ["region", "day"], None,
                    {"op": "sub", "args": [_ref("rev_daily"),
                                           _ref("rev_region_day")]}),
            # the same dependency referenced twice in one expression
            _metric("double_region", ["region"], None,
                    {"op": "add", "args": [_ref("rev_region"),
                                           _ref("rev_region")]}),
            # diamond: two parents that share dependencies further down
            _metric("diamond", ["region"], None,
                    {"op": "add", "args": [_ref("mix_region"),
                                           _ref("net_vs_total")]}),
            # boolean fact in an equality filter
            _metric("flag_rev", ["region"],
                    {"op": "cmp", "cmp": "=",
                     "left": {"op": "field", "name": "orders.flagged"},
                     "right": {"op": "lit", "value": True}},
                    _sum("orders.amount")),
            # and/or filter composition
            _metric("andor_rev", ["region"],
                    {"op": "and", "args": [
                        _eq_status("completed"),
                        {"op": "or", "args": [
                            {"op": "cmp", "cmp": "=",
                             "left": {"op": "field", "name": "orders.channel"},
                             "right": {"op": "lit", "value": "web"}},
                            {"op": "cmp", "cmp": "=",
                             "left": {"op": "field", "name": "orders.channel"},
                             "right": {"op": "lit", "value": "store"}}]}]},
                    _sum("orders.amount")),
            # constant-false filter: no group ever exists
            _metric("false_rev", ["region"], {"op": "lit", "value": False},
                    _sum("orders.amount")),
            # dimension values must be non-empty strings (boolean is not)
            _metric("bool_dim", ["flag"], None, _sum("orders.amount")),
            # count_distinct over a numeric field is not sliceable
            _metric("cd_num", ["region"], None,
                    {"op": "count_distinct",
                     "arg": {"op": "field", "name": "orders.amount"}}),
            # ---- metrics below are broken for slices but valid catalogs ----
            _metric("broken_mul", ["region"], None,
                    {"op": "mul", "args": [_sum("orders.amount"),
                                           {"op": "lit", "value": 2}]}),
            _metric("in_filter", ["region"],
                    {"op": "in", "left": {"op": "field", "name": "orders.status"},
                     "values": ["completed", "refunded"]},
                    _sum("orders.amount")),
            _metric("cyc_a", ["region"], None,
                    {"op": "add", "args": [_ref("cyc_b"),
                                           _sum("orders.amount")]}),
            _metric("cyc_b", ["region"], None,
                    {"op": "add", "args": [_ref("cyc_a"),
                                           _sum("orders.amount")]}),
            _metric("bad_grain", ["region"], None, _ref("rev_daily")),
        ],
    }


# ------------------------------------------------------- independent baseline

def baseline_root_groups(catalog, root_name, root_version, facts):
    """Whole-computation baseline: scans every fact for every metric in the
    root's closure.  Shares no code with the incremental refresh engine."""
    dims = {d["name"]: d["field"] for d in catalog["dimensions"]}
    mets = {(m["name"], m["version"]): m for m in catalog["metrics"]}

    def filt(f, row):
        if f is None:
            return True
        op = f["op"]
        if op == "lit":
            return f["value"]
        if op == "cmp":
            left, right = f["left"], f["right"]
            lv = row[left["name"]] if left["op"] == "field" else left["value"]
            rv = row[right["name"]] if right["op"] == "field" else right["value"]
            return lv == rv
        if op == "and":
            return all(filt(a, row) for a in f["args"])
        if op == "or":
            return any(filt(a, row) for a in f["args"])
        raise AssertionError(f"baseline: unexpected filter op {op}")

    def agg(node, rows, dep):
        op = node["op"]
        if op == "sum":
            return sum(r[node["arg"]["name"]] for r in rows)
        if op == "count_distinct":
            return len({r[node["arg"]["name"]] for r in rows})
        if op == "metric":
            return dep((node["name"], node["version"]))
        if op == "add":
            vals = [agg(a, rows, dep) for a in node["args"]]
            return None if any(v is None for v in vals) else sum(vals)
        if op == "sub":
            a = agg(node["args"][0], rows, dep)
            b = agg(node["args"][1], rows, dep)
            return None if a is None or b is None else a - b
        raise AssertionError(f"baseline: unexpected agg op {op}")

    values = {}

    def compute(nv):
        if nv in values:
            return values[nv]
        m = mets[nv]
        groups = {}
        for row in facts.values():
            if filt(m.get("filter"), row):
                key = tuple(row[dims[g]] for g in m["grain"])
                groups.setdefault(key, []).append(row)
        out = {}
        for key, rows in groups.items():
            def dep(dnv, _key=key, _grain=m["grain"]):
                dm = mets[dnv]
                proj = tuple(_key[list(_grain).index(g)] for g in dm["grain"])
                return compute(dnv).get(proj)      # missing group -> None
            out[key] = agg(m["agg"], rows, dep)
        values[nv] = out
        return out

    return compute((root_name, root_version))


def groups_map(read_result):
    """Read output as {key tuple: value}; empty grain -> ()."""
    grain = read_result["grain"]
    return {tuple(g["key"][d] for d in grain): g["value"]
            for g in read_result["groups"]}


# ------------------------------------------------------------------ helpers

def row(amount=0, cost=0, status="completed", region="east", channel="web",
        day=D1, customer="c1", flagged=False):
    return {"orders.amount": amount, "orders.cost": cost,
            "orders.status": status, "orders.region": region,
            "orders.channel": channel, "orders.day": day,
            "orders.customer": customer, "orders.flagged": flagged}


def put(fid, **kw):
    return {"op": "put", "id": fid, "row": row(**kw)}


def delete(fid):
    return {"op": "delete", "id": fid}


def mirror(facts, corrections):
    for c in corrections:
        if c["op"] == "put":
            facts[c["id"]] = c["row"]
        else:
            facts.pop(c["id"], None)


class SliceCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.db = store.connect(self.db_path)
        self.addCleanup(self.db.close)
        store.insert_catalog(self.db, make_catalog())      # seq = 1

    def new_slice(self, slice_name, root="net_daily@v1"):
        name, _, version = root.partition("@")
        return slices.init_slice(self.db, slice_name, "slice-cat", 1,
                                 name, version)

    def apply(self, slice_name, corrections):
        meta = store.get_slice(self.db, slice_name)
        cat = store.get_catalog(self.db, meta["catalog_id"],
                                meta["catalog_seq"])
        closure = slices.compile_closure(cat["content"], meta["root_name"],
                                         meta["root_version"])
        text = "\n".join(json.dumps(c, ensure_ascii=False)
                         for c in corrections)
        corr, errors = slices.parse_batch(text, closure.required_spec())
        self.assertEqual(errors, [])
        return slices.apply_batch(self.db, slice_name, corr)

    def check(self, slice_name, root, facts):
        got = slices.read_slice(self.db, slice_name)
        name, _, version = root.partition("@")
        want = baseline_root_groups(make_catalog(), name, version, facts)
        self.assertEqual(groups_map(got), want,
                         f"slice '{slice_name}' diverged from baseline")


# --------------------------------------------------------------------- tests

class TestIncrementalVsBaseline(SliceCase):
    """Two fine groups share one coarse group; corrections simultaneously
    switch groups and move rows in/out of filters; deletes then remove the
    coarse group.  Zero, null and missing root groups are distinguished."""

    def test_batch_sequence(self):
        self.new_slice("s", "net_daily@v1")
        facts = {}
        batches = [
            # B1: (d1,east)+(d2,east) share coarse group (east)
            [put("o1", amount=100, cost=10, day=D1, region="east", customer="c1"),
             put("o2", amount=200, cost=20, day=D2, region="east", customer="c2"),
             put("o3", amount=50, cost=5, status="cancelled", day=D1,
                 region="east", customer="c1"),
             put("o4", amount=70, cost=7, day=D1, region="west", customer="c3"),
             put("o5", amount=30, cost=3, status="refunded", day=D2,
                 region="east", customer="c2")],
            # B2: o1 switches fine group d1->d2, o5 enters the filter,
            #     o2 leaves the filter, o3 (never qualifying for rev) switches
            #     region — all in one batch
            [put("o1", amount=100, cost=10, day=D2, region="east", customer="c1"),
             put("o5", amount=30, cost=3, status="completed", day=D2,
                 region="east", customer="c2"),
             put("o2", amount=200, cost=20, status="cancelled", day=D2,
                 region="east", customer="c2"),
             put("o3", amount=50, cost=5, status="cancelled", day=D1,
                 region="west", customer="c1")],
            [delete("o4")],     # B3a: west's rev groups disappear
            [delete("o3")],     # B3b: west's last fact -> root group disappears
            [delete("o1"), delete("o2"), delete("o5")],
        ]
        expected = [
            {(D1, "east"): -200, (D2, "east"): -100, (D1, "west"): 0},
            {(D2, "east"): 0, (D1, "west"): 0},
            {(D2, "east"): 0, (D1, "west"): None},   # parent survives, deps gone
            {(D2, "east"): 0},
            {},
        ]
        for batch, want in zip(batches, expected):
            self.apply("s", batch)
            mirror(facts, batch)
            got = groups_map(slices.read_slice(self.db, "s"))
            self.assertEqual(got, want)          # hand-computed spot check
            self.check("s", "net_daily@v1", facts)   # baseline cross-check
        # after the last batch the root has no groups at all
        self.assertEqual(slices.read_slice(self.db, "s")["groups"], [])


class TestCountDistinct(SliceCase):
    """Deleting some of a customer's orders must not decrement the distinct
    count; deleting their last order must."""

    def test_customer_deletions(self):
        self.new_slice("cd", "cust_region@v1")
        facts = {}
        batches = [
            [put("o1", customer="c1", region="east", day=D1),
             put("o2", customer="c1", region="east", day=D2),
             put("o3", customer="c2", region="east", day=D1),
             put("o4", customer="c3", region="west", day=D1)],
            [delete("o1")],                       # c1 still has o2
            [put("o2", customer="c9", region="east", day=D2)],  # c1 -> c9
            [delete("o2")],                       # c9's last order
            [delete("o3")],                       # east group disappears
            [delete("o4")],
        ]
        expected = [
            {("east",): 2, ("west",): 1},
            {("east",): 2, ("west",): 1},
            {("east",): 2, ("west",): 1},
            {("east",): 1, ("west",): 1},
            {("west",): 1},
            {},
        ]
        for batch, want in zip(batches, expected):
            self.apply("cd", batch)
            mirror(facts, batch)
            self.assertEqual(groups_map(slices.read_slice(self.db, "cd")), want)
            self.check("cd", "cust_region@v1", facts)


class TestClosureShapes(SliceCase):
    """Mixed direct aggregates + references, add, the empty grain, and
    name-based (not prefix-based) projection onto a reordered grain."""

    SLICES = [("m", "margin_daily@v1"), ("x", "mix_region@v1"),
              ("t", "net_vs_total@v1"), ("sw", "swap_daily@v1"),
              ("d", "double_region@v1"), ("di", "diamond@v1")]

    def test_batch_sequence(self):
        for s, root in self.SLICES:
            self.new_slice(s, root)
        facts = {}
        batches = [
            [put("o1", amount=100, cost=10, day=D1, region="east", customer="c1"),
             put("o2", amount=200, cost=20, day=D2, region="east", customer="c2"),
             put("o3", amount=50, cost=5, status="cancelled", day=D1,
                 region="east", customer="c1"),
             put("o4", amount=70, cost=7, day=D1, region="west", customer="c3")],
            [put("o2", amount=200, cost=20, status="cancelled", day=D2,
                 region="east", customer="c2")],
            [delete("o1"), delete("o4")],   # no completed facts remain anywhere
            [delete("o2"), delete("o3")],
        ]
        for i, batch in enumerate(batches):
            for s, _root in self.SLICES:
                self.apply(s, batch)
            mirror(facts, batch)
            for s, root in self.SLICES:
                self.check(s, root, facts)
            if i == 0:
                # reordered-grain projection: every swap group is exactly 0
                sw = groups_map(slices.read_slice(self.db, "sw"))
                self.assertEqual(sw, {("east", D1): 0, ("east", D2): 0,
                                      ("west", D1): 0})
                # empty-grain dependency is aggregated over all regions
                t = groups_map(slices.read_slice(self.db, "t"))
                self.assertEqual(t, {("east",): -70, ("west",): -300})
            if i == 2:
                # rev_total (empty grain) has no qualifying facts -> gone;
                # net_vs_total(east) survives on cancelled facts with null
                t = groups_map(slices.read_slice(self.db, "t"))
                self.assertEqual(t, {("east",): None})
                # mix of two vanished dependencies is null, group kept
                x = groups_map(slices.read_slice(self.db, "x"))
                self.assertEqual(x, {("east",): None})
        for s, _root in self.SLICES:
            self.assertEqual(slices.read_slice(self.db, s)["groups"], [])


class TestDeleteRecreateGroup(SliceCase):
    """A group deleted and re-created within one batch settles correctly,
    and batch-internal order does not change the final state."""

    def test_delete_and_recreate_in_one_batch(self):
        self.new_slice("s", "net_daily@v1")
        facts = {}
        b0 = [put("x", amount=10, cost=1, day=D1, region="east")]
        self.apply("s", b0)
        mirror(facts, b0)
        # delete the group's only row, then re-create the group, one batch
        b1 = [delete("x"), put("y", amount=20, cost=1, day=D1, region="east")]
        self.apply("s", b1)
        mirror(facts, b1)
        self.assertEqual(groups_map(slices.read_slice(self.db, "s")),
                         {(D1, "east"): 0})
        self.check("s", "net_daily@v1", facts)
        # same idea, opposite intra-batch order
        b2 = [put("z", amount=5, cost=1, day=D1, region="east"), delete("y")]
        self.apply("s", b2)
        mirror(facts, b2)
        self.assertEqual(groups_map(slices.read_slice(self.db, "s")),
                         {(D1, "east"): 0})
        self.check("s", "net_daily@v1", facts)


class TestEmptyGrainRoot(SliceCase):
    """An empty-grain root has exactly one group, existing iff at least one
    qualifying fact exists."""

    def test_empty_grain_root(self):
        self.new_slice("t", "rev_total@v1")
        facts = {}
        b1 = [put("a", amount=5, day=D1, region="east"),
              put("b", amount=7, day=D2, region="west", status="cancelled")]
        self.apply("t", b1)
        mirror(facts, b1)
        got = slices.read_slice(self.db, "t")
        self.assertEqual(got["groups"], [{"key": {}, "value": 5}])
        b2 = [delete("a")]
        self.apply("t", b2)
        mirror(facts, b2)
        self.assertEqual(slices.read_slice(self.db, "t")["groups"], [])
        self.check("t", "rev_total@v1", facts)


class TestUnicode(SliceCase):
    def test_non_ascii_dimensions(self):
        self.new_slice("s", "rev_region@v1")
        facts = {}
        batch = [put("u1", amount=3, region="北部", customer="客户甲"),
                 put("u2", amount=4, region="北部", customer="客户乙")]
        self.apply("s", batch)
        mirror(facts, batch)
        got = slices.read_slice(self.db, "s")
        self.assertEqual(groups_map(got), {("北部",): 7})
        self.assertEqual(got["groups"][0]["key"], {"region": "北部"})
        self.check("s", "rev_region@v1", facts)


class TestFilterShapes(SliceCase):
    """Boolean facts, and/or filter composition, constant-false filters."""

    def test_boolean_and_or_constant_filters(self):
        self.new_slice("f", "flag_rev@v1")
        self.new_slice("a", "andor_rev@v1")
        self.new_slice("z", "false_rev@v1")
        facts = {}
        batches = [
            [put("p1", amount=10, region="east", channel="web", flagged=True),
             put("p2", amount=20, region="east", channel="app", flagged=False),
             put("p3", amount=40, region="west", channel="store", flagged=True),
             put("p4", amount=80, region="west", channel="web",
                 status="cancelled", flagged=True)],
            # p2 enters both filters at once (flagged + channel correction)
            [put("p2", amount=20, region="east", channel="web", flagged=True)],
            [delete("p1")],
            [delete("p2"), delete("p3"), delete("p4")],
        ]
        expected_f = [{("east",): 10, ("west",): 120},
                      {("east",): 30, ("west",): 120},
                      {("east",): 20, ("west",): 120},
                      {}]
        expected_a = [{("east",): 10, ("west",): 40},
                      {("east",): 30, ("west",): 40},
                      {("east",): 20, ("west",): 40},
                      {}]
        for i, batch in enumerate(batches):
            for s in ("f", "a", "z"):
                self.apply(s, batch)
            mirror(facts, batch)
            self.assertEqual(groups_map(slices.read_slice(self.db, "f")),
                             expected_f[i])
            self.assertEqual(groups_map(slices.read_slice(self.db, "a")),
                             expected_a[i])
            self.assertEqual(slices.read_slice(self.db, "z")["groups"], [])
            for s, root in (("f", "flag_rev@v1"), ("a", "andor_rev@v1"),
                            ("z", "false_rev@v1")):
                self.check(s, root, facts)


class TestExactIntegers(SliceCase):
    """Signed 32-bit facts; sums remain exact across the boundaries."""

    def test_int32_boundaries(self):
        self.new_slice("s", "rev_region@v1")
        facts = {}
        hi, lo = 2 ** 31 - 1, -(2 ** 31)
        batch = [put("a", amount=hi, day=D1, region="east"),
                 put("b", amount=hi, day=D2, region="east"),
                 put("c", amount=lo, day=D3, region="east")]
        self.apply("s", batch)
        mirror(facts, batch)
        self.assertEqual(groups_map(slices.read_slice(self.db, "s")),
                         {("east",): 2 * hi + lo})
        # replace with a negative correction, then delete down to one row
        fix = [put("b", amount=-1, day=D2, region="east")]
        self.apply("s", fix)
        mirror(facts, fix)
        self.assertEqual(groups_map(slices.read_slice(self.db, "s")),
                         {("east",): hi + lo - 1})
        tail = [delete("a"), delete("b")]
        self.apply("s", tail)
        mirror(facts, tail)
        self.assertEqual(groups_map(slices.read_slice(self.db, "s")),
                         {("east",): lo})
        self.check("s", "rev_region@v1", facts)


class TestRejectionAndRollback(SliceCase):
    """Bad batches are rejected wholesale; the database (even reopened) still
    shows the last good batch, and refreshing continues afterwards."""

    def test_bad_batches_then_continue(self):
        self.new_slice("s", "net_daily@v1")
        facts = {}
        good1 = [put("o1", amount=100, cost=10, day=D1, region="east"),
                 put("o2", amount=70, cost=7, day=D1, region="west")]
        self.apply("s", good1)
        mirror(facts, good1)
        snapshot = slices.read_slice(self.db, "s")

        missing_region = row(amount=1, cost=1, day=D1)
        del missing_region["orders.region"]
        bad_batches = [
            [{"op": "put", "id": "b1", "row": missing_region}],
            [{"op": "put", "id": "b2",
              "row": row(amount="100", cost=1, day=D1)}],       # wrong type
            [{"op": "put", "id": "b3",
              "row": row(amount=True, cost=1, day=D1)}],        # bool != int
            [{"op": "put", "id": "b4",
              "row": row(amount=2 ** 31, cost=1, day=D1)}],     # > int32
            [{"op": "put", "id": "b5",
              "row": row(amount=1, cost=1, day="")}],           # empty dimension
            [put("o1", amount=1, cost=1, day=D1, region="east"),
             put("o1", amount=2, cost=2, day=D1, region="east")],  # dup id
            [delete("ghost")],                                  # no such id
            [put("b7", amount=1, cost=1, day=D1, region="east"),
             delete("ghost")],                                  # mixed: rejected
        ]
        meta = store.get_slice(self.db, "s")
        cat = store.get_catalog(self.db, meta["catalog_id"], meta["catalog_seq"])
        closure = slices.compile_closure(cat["content"], meta["root_name"],
                                         meta["root_version"])
        spec = closure.required_spec()
        for bad in bad_batches:
            text = "\n".join(json.dumps(c) for c in bad)
            corr, errors = slices.parse_batch(text, spec)
            if not errors:
                # structurally valid batches fail at apply time instead
                with self.assertRaises(slices.SliceError):
                    slices.apply_batch(self.db, "s", corr)
            # rejected batch left no trace
            self.assertEqual(slices.read_slice(self.db, "s"), snapshot)

        # reopen the database: still exactly the last committed batch
        self.db.close()
        self.db = store.connect(self.db_path)
        self.assertEqual(slices.read_slice(self.db, "s"), snapshot)

        # an empty batch is a no-op
        self.apply("s", [])
        self.assertEqual(slices.read_slice(self.db, "s"), snapshot)

        # refreshing continues; extra unreferenced fields are tolerated
        extra = row(amount=150, cost=10, day=D1, region="east")
        extra["orders.unknown"] = "ignored"
        good2 = [{"op": "put", "id": "o1", "row": extra}, delete("o2")]
        self.apply("s", good2)
        mirror(facts, good2)
        self.check("s", "net_daily@v1", facts)


class TestInitRejection(SliceCase):
    """slice-init rejects cycles, unprojectable grains and unsupported
    expressions inside the closure; broken unrelated metrics do not matter."""

    def test_closure_validation(self):
        self.new_slice("ok", "net_daily@v1")   # catalog has broken metrics too
        with self.assertRaises(slices.SliceError) as ctx:
            self.new_slice("c1", "cyc_a@v1")
        self.assertIn("cycle", str(ctx.exception))
        with self.assertRaises(slices.SliceError) as ctx:
            self.new_slice("g1", "bad_grain@v1")
        self.assertIn("not a subset", str(ctx.exception))
        with self.assertRaises(slices.SliceError) as ctx:
            self.new_slice("m1", "broken_mul@v1")
        self.assertIn("unsupported agg op", str(ctx.exception))
        with self.assertRaises(slices.SliceError) as ctx:
            self.new_slice("f1", "in_filter@v1")
        self.assertIn("unsupported filter op", str(ctx.exception))
        with self.assertRaises(slices.SliceError) as ctx:
            self.new_slice("bd", "bool_dim@v1")
        self.assertIn("non-empty strings", str(ctx.exception))
        with self.assertRaises(slices.SliceError) as ctx:
            self.new_slice("cn", "cd_num@v1")
        self.assertIn("non-empty strings", str(ctx.exception))
        with self.assertRaises(slices.SliceError):
            self.new_slice("u1", "ghost@v1")
        with self.assertRaises(slices.SliceError):
            self.new_slice("ok", "rev_daily@v1")           # name taken
        with self.assertRaises(slices.SliceError):
            slices.init_slice(self.db, "s2", "slice-cat", 99,
                              "rev_daily", "v1")           # unknown import


class TestNoFullRecompute(SliceCase):
    """A correction inside one coarse group must not degrade to a full
    recomputation: only the touched subtree is recomputed, while one coarse
    change still reaches every uncorrected parent group above it."""

    REGIONS = [f"r{i:02d}" for i in range(40)]

    def test_single_coarse_group_correction(self):
        self.new_slice("s", "net_daily@v1")
        facts = {}
        load = []
        for i, region in enumerate(self.REGIONS):
            load.append(put(f"{region}-a", amount=10 + i, cost=1,
                            day=D1, region=region))
            load.append(put(f"{region}-b", amount=100 + i, cost=1,
                            day=D2, region=region))
        self.apply("s", load)
        mirror(facts, load)
        self.check("s", "net_daily@v1", facts)

        # one order corrected inside coarse group r00
        fix = [put("r00-a", amount=15, cost=1, day=D1, region="r00")]
        stats = self.apply("s", fix)
        mirror(facts, fix)
        self.assertEqual(stats["rows_examined"], 1)
        recomputed = {(m, tuple(json.loads(k))) for m, k in stats["recomputed"]}
        self.assertEqual(recomputed, {
            ("rev_daily@v1", (D1, "r00")),
            ("rev_region@v1", ("r00",)),
            ("net_daily@v1", (D1, "r00")),
            ("net_daily@v1", (D2, "r00")),   # uncorrected parent of coarse change
        })
        self.assertEqual(stats["groups_recomputed"], 4)   # not 200
        self.check("s", "net_daily@v1", facts)

        # an irrelevant correction (fails every rev filter) on a new day
        # creates exactly one group; the other 200 groups stay untouched
        irr = [put("r07-x", amount=999, cost=1, status="cancelled",
                   day=D3, region="r07")]
        stats = self.apply("s", irr)
        mirror(facts, irr)
        self.assertEqual(stats["rows_examined"], 1)
        recomputed = {(m, tuple(json.loads(k))) for m, k in stats["recomputed"]}
        self.assertEqual(recomputed, {("net_daily@v1", (D3, "r07"))})
        self.check("s", "net_daily@v1", facts)

        # joining an existing group without changing any aggregate of any
        # metric recomputes nothing at all
        irr2 = [put("r07-y", amount=5, cost=1, status="cancelled",
                    day=D3, region="r07")]
        stats = self.apply("s", irr2)
        mirror(facts, irr2)
        self.assertEqual(stats["groups_recomputed"], 0)
        self.assertEqual(stats["facts"], 82)
        self.check("s", "net_daily@v1", facts)


# ------------------------------------------------------------------ CLI level

def run_cli(db_path, *args, expect=0):
    proc = subprocess.run(
        [sys.executable, "-m", "metric_analyzer.cli", "--db", db_path, *args],
        cwd=ROOT, capture_output=True, text=True)
    if expect is not None and proc.returncode != expect:
        raise AssertionError(
            f"cli {args}: exit {proc.returncode} (wanted {expect})\n"
            f"stdout: {proc.stdout}\nstderr: {proc.stderr}")
    return proc


class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "cli.db")
        self.cat_path = os.path.join(self.tmp.name, "cat.json")
        with open(self.cat_path, "w", encoding="utf-8") as fh:
            json.dump(make_catalog(), fh)

    def write_batch(self, name, corrections):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w", encoding="utf-8") as fh:
            for c in corrections:
                fh.write(json.dumps(c, ensure_ascii=False) + "\n")
        return path

    def read_cli(self, slice_name):
        out = run_cli(self.db_path, "slice-read", "--slice", slice_name).stdout
        return json.loads(out)

    def test_end_to_end(self):
        run_cli(self.db_path, "import-catalog", "--file", self.cat_path)
        run_cli(self.db_path, "slice-init", "--slice", "s",
                "--catalog", "slice-cat", "--seq", "1",
                "--metric", "net_daily@v1")
        facts = {}
        b1 = [put("o1", amount=100, cost=10, day=D1, region="east"),
              put("o2", amount=70, cost=7, day=D1, region="west")]
        run_cli(self.db_path, "slice-refresh", "--slice", "s",
                "--file", self.write_batch("b1.jsonl", b1))
        mirror(facts, b1)

        # a rejected batch leaves no trace for a fresh process
        proc = run_cli(self.db_path, "slice-refresh", "--slice", "s",
                       "--file", self.write_batch("bad.jsonl",
                                                  [delete("ghost")]),
                       expect=1)
        self.assertIn("rejected", proc.stderr)
        self.assertEqual(groups_map(self.read_cli("s")),
                         baseline_root_groups(make_catalog(), "net_daily",
                                              "v1", facts))

        # re-importing the catalog under the same id must not move the slice
        cat2 = copy.deepcopy(make_catalog())
        cat2["metrics"][1]["filter"] = None       # rev_region@v1 redefined
        with open(self.cat_path, "w", encoding="utf-8") as fh:
            json.dump(cat2, fh)
        run_cli(self.db_path, "import-catalog", "--file", self.cat_path)
        out = self.read_cli("s")
        self.assertEqual(out["catalog"]["seq"], 1)
        self.assertEqual(groups_map(out),
                         baseline_root_groups(make_catalog(), "net_daily",
                                              "v1", facts))

        # refreshing continues after the rejected batch
        b2 = [put("o1", amount=150, cost=10, day=D1, region="east"),
              delete("o2")]
        run_cli(self.db_path, "slice-refresh", "--slice", "s",
                "--file", self.write_batch("b2.jsonl", b2))
        mirror(facts, b2)
        self.assertEqual(groups_map(self.read_cli("s")),
                         baseline_root_groups(make_catalog(), "net_daily",
                                              "v1", facts))

        # listing, duplicate name, cyclic root, unknown import, unknown slice
        self.assertIn("s", run_cli(self.db_path, "slices").stdout)
        run_cli(self.db_path, "slice-init", "--slice", "s",
                "--catalog", "slice-cat", "--seq", "1",
                "--metric", "rev_daily@v1", expect=1)
        run_cli(self.db_path, "slice-init", "--slice", "cyc",
                "--catalog", "slice-cat", "--seq", "1",
                "--metric", "cyc_a@v1", expect=1)
        run_cli(self.db_path, "slice-init", "--slice", "x",
                "--catalog", "slice-cat", "--seq", "99",
                "--metric", "rev_daily@v1", expect=1)
        run_cli(self.db_path, "slice-refresh", "--slice", "ghost",
                "--file", self.write_batch("b1.jsonl", b1), expect=1)
        run_cli(self.db_path, "slice-read", "--slice", "ghost", expect=1)


if __name__ == "__main__":
    unittest.main()
