"""Tests for single-fact withdrawal-impact queries on offline slices.

The expected answer is computed by an independent whole-recomputation oracle:
for every committed fact, remove just that fact from the full fact set, rerun
the metric closure from scratch, and compare the target group's existence and
value with the committed result.  The query engine under test only ever looks
at the target group's reference cone, so agreement on these small examples is
a complete-list check, not just a spot check.

Run from the repository root:

    python3 -m unittest
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from metric_analyzer import slices, store  # noqa: E402
from tests.test_slice import (D1, D2, D3, baseline_root_groups,  # noqa: E402
                              delete, groups_map, make_catalog, put, row)

CAT = make_catalog()


# ------------------------------------------------------------------ helpers

def expected_changes(catalog, root, facts, key):
    """Whole-recompute oracle for the complete change list of one group."""
    name, _, version = root.partition("@")
    grain = next(m["grain"] for m in catalog["metrics"]
                 if m["name"] == name and m["version"] == version)
    kt = tuple(key[d] for d in grain)
    current = baseline_root_groups(catalog, name, version, facts)
    if kt not in current:
        raise KeyError("target group does not exist")
    cur = current[kt]
    out = []
    for fid in sorted(facts.keys(),
                      key=lambda f: (0, f) if isinstance(f, str) else (1, f)):
        rest = {k: v for k, v in facts.items() if k != fid}
        after = baseline_root_groups(catalog, name, version, rest)
        if kt not in after:
            out.append({"id": fid, "state": "group_removed"})
        elif after[kt] is None:
            if cur is None:
                continue
            out.append({"id": fid, "state": "null", "value": None})
        elif cur is not None and after[kt] == cur:
            continue
        else:
            out.append({"id": fid, "state": "value", "value": after[kt]})
    out.sort(key=lambda c: (0, c["id"]) if isinstance(c["id"], str)
             else (1, c["id"]))
    return out


class ImpactCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "impact.db")
        self.db = store.connect(self.db_path)
        self.addCleanup(self.db.close)
        store.insert_catalog(self.db, CAT)
        self.facts = {}

    def new_slice(self, name, root):
        rn, _, rv = root.partition("@")
        slices.init_slice(self.db, name, "slice-cat", 1, rn, rv)
        meta = store.get_slice(self.db, name)
        cat = store.get_catalog(self.db, meta["catalog_id"],
                                meta["catalog_seq"])
        self.closure = slices.compile_closure(cat["content"], rn, rv)

    def apply(self, name, corrections):
        text = "\n".join(json.dumps(c, ensure_ascii=False)
                         for c in corrections)
        corr, errors = slices.parse_batch(text, self.closure.required_spec())
        self.assertEqual(errors, [])
        slices.apply_batch(self.db, name, corr)
        for c in corrections:
            if c["op"] == "put":
                self.facts[c["id"]] = c["row"]
            else:
                self.facts.pop(c["id"], None)

    def oracle(self, name, root, key):
        got = slices.query_withdrawal_impact(self.db, name, key)
        self.assertEqual(got["changes"], expected_changes(CAT, root, self.facts,
                                                          key))
        # the reported current value really is the committed group value
        grain = got["grain"]
        kt = tuple(got["key"][d] for d in grain)
        committed = groups_map(slices.read_slice(self.db, name))
        self.assertEqual(got["current"], committed[kt])
        return got


# -------------------------------------------------------------------- tests

class TestRedistributedCounts(ImpactCase):
    """Coarse group keeps the exact same value while membership moves: the
    next query must list the newly responsible fact id, not the old one."""

    def test_count_redistribution_keeps_value_but_changes_ids(self):
        self.new_slice("cd", "cust_region@v1")
        self.apply("cd", [
            put("o1", customer="c1", region="east", day=D1),
            put("o2", customer="c1", region="east", day=D2),
            put("o3", customer="c2", region="east", day=D1),
        ])
        key = {"region": "east"}
        first = self.oracle("cd", "cust_region@v1", key)
        self.assertEqual(first["current"], 2)
        self.assertEqual(first["changes"],
                         [{"id": "o3", "state": "value", "value": 1}])

        # redistribute the same two customers: o2 moves c1 -> c2, the coarse
        # distinct count stays exactly 2
        self.apply("cd", [put("o2", customer="c2", region="east", day=D2)])
        self.assertEqual(groups_map(slices.read_slice(self.db, "cd")),
                         {("east",): 2})
        second = self.oracle("cd", "cust_region@v1", key)
        self.assertEqual(second["current"], 2)
        self.assertEqual(second["changes"],
                         [{"id": "o1", "state": "value", "value": 1}])
        # ids never accumulate: at most one entry per fact
        self.assertEqual(len({c["id"] for c in second["changes"]}),
                         len(second["changes"]))

    def test_shared_distinct_value_survives_one_withdrawal(self):
        self.new_slice("cd", "cust_region@v1")
        self.apply("cd", [
            put("a", customer="shared", region="east"),
            put("b", customer="shared", region="east"),
            put("c", customer="solo", region="east"),
        ])
        got = self.oracle("cd", "cust_region@v1", {"region": "east"})
        self.assertEqual(got["current"], 2)
        # withdrawing either 'shared' order leaves the value 2; only 'solo'
        # changes the count, although all three facts are in the group
        self.assertEqual(got["changes"],
                         [{"id": "c", "state": "value", "value": 1}])


class TestCancellingPaths(ImpactCase):
    """One fact can act through two expression paths whose contributions
    cancel at the root.  It must not be listed when the root value is
    unchanged, and a fact outside the target group must be listed when a
    dependency makes it move the target."""

    def test_cancelling_contributions_are_not_listed(self):
        self.new_slice("nv", "net_vs_total@v1")
        self.apply("nv", [
            put("east-a", amount=50, cost=1, region="east"),
            put("east-b", amount=30, cost=1, region="east"),
            put("west-a", amount=10, cost=1, region="west"),
        ])
        got = self.oracle("nv", "net_vs_total@v1", {"region": "east"})
        # rev_region(east)=80, rev_total=90 -> -10
        self.assertEqual(got["current"], -10)
        # east facts touch both rev_region(east) and rev_total by the same
        # amount: the target stays -10 (or stays ... only after value check:
        # withdrawing east-a leaves 30-40=-10).  west-a is not in the target
        # group but does shift rev_total, so it *is* a candidate.
        self.assertEqual(got["changes"],
                         [{"id": "west-a", "state": "value", "value": 0}])
        self.assertNotIn("east-a", {c["id"] for c in got["changes"]})
        self.assertNotIn("east-b", {c["id"] for c in got["changes"]})

    def test_same_fact_on_two_paths_is_one_entry(self):
        self.new_slice("di", "diamond@v1")
        self.apply("di", [
            put("o1", amount=50, cost=1, region="east", customer="c1"),
            put("o2", amount=30, cost=1, region="east", customer="c2"),
        ])
        got = self.oracle("di", "diamond@v1", {"region": "east"})
        ids = [c["id"] for c in got["changes"]]
        self.assertEqual(len(ids), len(set(ids)))
        # diamond = mix_region + net_vs_total; o1 moves both paths at once
        self.assertIn("o1", ids)


class TestDependencyLossVsGroupLoss(ImpactCase):
    """A missing dependency yields a surviving group with null; losing the
    last qualifying fact removes the group entirely."""

    def test_null_dependency_vs_group_removal(self):
        self.new_slice("s", "net_daily@v1")
        self.apply("s", [
            put("done", amount=100, cost=10, day=D1, region="east"),
            put("only-rev", amount=200, cost=20, day=D2, region="east"),
            # a non-qualifying fact that still belongs to the parent group of
            # D2/east: withdrawing only-rev removes the rev_* dependency but
            # the parent group survives with a null value
            put("other", amount=5, cost=1, status="cancelled", day=D2,
                region="east"),
        ])
        got = self.oracle("s", "net_daily@v1", {"day": D2, "region": "east"})
        # withdrawing only-rev removes the day/region dependency group while
        # the parent group (kept alive by the cancelled fact) becomes null;
        # withdrawing done (another day's fact) moves the coarse dependency
        # and therefore this target's value as well
        self.assertEqual(got["changes"],
                         [{"id": "done", "state": "value", "value": 0},
                          {"id": "only-rev", "state": "null", "value": None}])
        # the D1 group is removed entirely when its only qualifying fact goes
        got1 = self.oracle("s", "net_daily@v1", {"day": D1, "region": "east"})
        self.assertEqual(got1["changes"],
                         [{"id": "done", "state": "group_removed"},
                          {"id": "only-rev", "state": "value", "value": 0}])

    def test_null_to_null_is_not_a_change(self):
        self.new_slice("s", "net_daily@v1")
        self.apply("s", [
            put("c1", amount=1, cost=1, status="cancelled", day=D2,
                region="east"),
            put("c2", amount=2, cost=1, status="cancelled", day=D2,
                region="east"),
        ])
        got = self.oracle("s", "net_daily@v1", {"day": D2, "region": "east"})
        self.assertEqual(got["current"], None)
        # either withdrawal leaves one fact and still null -> no change at all
        self.assertEqual(got["changes"], [])

    def test_zero_is_reported_as_a_value(self):
        self.new_slice("s", "net_daily@v1")
        self.apply("s", [
            put("x", amount=10, cost=10, day=D1, region="east"),
            put("y", amount=7, cost=0, day=D1, region="west"),
        ])
        got = self.oracle("s", "net_daily@v1", {"day": D1, "region": "west"})
        self.assertEqual(got["current"], 0)
        # withdrawing the empty-grain rev_total's only... west's own fact is
        # the whole group; check through the oracle instead of hand-waving
        self.assertTrue(got["changes"])


class TestIdsAndMissingGroup(ImpactCase):
    def test_integer_ids_keep_their_type(self):
        self.new_slice("s", "margin_daily@v1")
        self.apply("s", [
            put(7, amount=10, cost=1, day=D1, region="east"),
            put("x", amount=20, cost=2, day=D1, region="east"),
        ])
        got = self.oracle("s", "margin_daily@v1",
                          {"day": D1, "region": "east"})
        ids = {c["id"] for c in got["changes"]}
        self.assertIn(7, ids)
        self.assertIn("x", ids)
        self.assertTrue(all(type(c["id"]) in (int, str)
                            for c in got["changes"]))

    def test_missing_group_is_an_explicit_error(self):
        self.new_slice("s", "net_daily@v1")
        self.apply("s", [put("x", amount=1, cost=1, day=D1, region="east")])
        with self.assertRaises(slices.SliceError) as ctx:
            slices.query_withdrawal_impact(
                self.db, "s", {"day": D2, "region": "east"})
        self.assertIn("does not exist", str(ctx.exception))
        with self.assertRaises(slices.SliceError):
            slices.query_withdrawal_impact(
                self.db, "s", {"day": D1})          # incomplete key

    def test_empty_grain_key(self):
        self.new_slice("t", "rev_total@v1")
        self.apply("t", [put("a", amount=5, day=D1, region="east"),
                         put("b", amount=7, day=D2, region="west")])
        got = self.oracle("t", "rev_total@v1", {})
        self.assertEqual(got["current"], 12)
        self.assertEqual(got["changes"],
                         [{"id": "a", "state": "value", "value": 7},
                          {"id": "b", "state": "value", "value": 5}])


class TestIndexLifecycle(ImpactCase):
    def test_missing_preparation_is_reported(self):
        self.new_slice("s", "net_daily@v1")
        self.apply("s", [put("x", amount=1, cost=1, day=D1, region="east")])
        self.db.execute("DELETE FROM slice_impact_prep WHERE slice_name='s'")
        self.db.commit()
        with self.assertRaises(slices.SliceError) as ctx:
            slices.query_withdrawal_impact(
                self.db, "s", {"day": D1, "region": "east"})
        self.assertIn("slice-impact-prepare", str(ctx.exception))
        info = slices.prepare_impact(self.db, "s")
        self.assertEqual(info["facts"], 1)
        # idempotent and query works again
        slices.prepare_impact(self.db, "s")
        self.oracle("s", "net_daily@v1", {"day": D1, "region": "east"})

    def test_refresh_keeps_index_current_without_repreparation(self):
        self.new_slice("s", "net_daily@v1")
        self.apply("s", [put("x", amount=1, cost=1, day=D1, region="east"),
                         put("y", amount=2, cost=2, day=D1, region="west")])
        # corrections move x to another day/region and add a fact
        self.apply("s", [
            put("x", amount=40, cost=4, day=D3, region="north"),
            put("z", amount=8, cost=1, day=D3, region="north"),
            delete("y"),
        ])
        present = groups_map(slices.read_slice(self.db, "s"))
        for key in ({"day": D1, "region": "east"},
                    {"day": D1, "region": "west"},
                    {"day": D3, "region": "north"}):
            if (key["day"], key["region"]) in present:
                self.oracle("s", "net_daily@v1", key)
            else:
                with self.assertRaises(slices.SliceError):
                    slices.query_withdrawal_impact(self.db, "s", key)

    def test_failed_batch_leaves_index_consistent_after_reopen(self):
        self.new_slice("s", "net_daily@v1")
        self.apply("s", [put("x", amount=10, cost=1, day=D1, region="east"),
                         put("y", amount=20, cost=2, day=D1, region="east")])
        key = {"day": D1, "region": "east"}
        before = self.oracle("s", "net_daily@v1", key)

        # structurally valid, fails at apply time: unknown delete
        with self.assertRaises(slices.SliceError):
            slices.apply_batch(self.db, "s", [
                {"op": "put", "id": "n", "row": row(amount=99, cost=9,
                                                    day=D1, region="east")},
                {"op": "delete", "id": "ghost", "row": None}])

        self.db.close()
        self.db = store.connect(self.db_path)
        after = slices.query_withdrawal_impact(self.db, "s", key)
        self.assertEqual(after, before)
        self.assertNotIn("n", {c["id"] for c in after["changes"]})
        # the index was not half-updated: the transient put left no trace
        rows = self.db.execute(
            "SELECT fact_id FROM slice_fact_groups WHERE slice_name='s'"
            " AND fact_id='n'").fetchall()
        self.assertEqual(rows, [])
        # refreshing continues normally afterwards
        self.apply("s", [put("x", amount=15, cost=1, day=D1, region="east")])
        self.oracle("s", "net_daily@v1", key)


class TestCandidatesAreLocal(ImpactCase):
    """With many unrelated facts around, the query's candidate set and its
    preparation work stay confined to the target group's reference cone."""

    def test_does_not_use_the_whole_fact_set(self):
        self.new_slice("s", "mix_region@v1")
        batch = [put("east-1", amount=10, cost=1, region="east",
                     customer="acme"),
                 put("east-2", amount=20, cost=2, region="east",
                     customer="beta")]
        for i in range(60):
            batch.append(put(f"other-{i}", amount=100 + i, cost=1,
                             region=f"r{i:02d}", customer=f"c{i}"))
        self.apply("s", batch)

        seen = []
        self.db.set_trace_callback(seen.append)
        try:
            got = slices.query_withdrawal_impact(
                self.db, "s", {"region": "east"})
        finally:
            self.db.set_trace_callback(None)
        self.assertEqual({c["id"] for c in got["changes"]},
                         {"east-1", "east-2"})
        self.oracle("s", "mix_region@v1", {"region": "east"})

        # no statement may read the fact tables without constraining the id
        # list (or the metric/gkey cone); i.e. no full fact or group scan
        for sql in seen:
            low = sql.lower()
            if "slice_facts" in low and " from " in low:
                self.assertTrue("fact_id in" in low or "fact_id=" in low,
                                f"unbounded fact scan: {sql}")
            if "slice_fact_groups" in low and " from " in low:
                self.assertTrue(("gkey=" in low) or ("fact_id=" in low)
                                or ("fact_id in" in low),
                                f"unbounded membership scan: {sql}")
            if "slice_groups" in low and " from " in low:
                self.assertTrue("metric=" in low,
                                f"unbounded group scan: {sql}")


class TestReadOnlyAndMirror(ImpactCase):
    def test_query_never_advances_mirror_version(self):
        from metric_analyzer import mirror
        self.new_slice("m", "net_daily@v1")
        meta = store.get_slice(self.db, "m")
        cat = store.get_catalog(self.db, "slice-cat", 1)
        closure = slices.compile_closure(cat["content"], "net_daily", "v1")

        def refresh(ops):
            corr, errors = slices.parse_batch(
                "\n".join(json.dumps(o) for o in ops), closure.required_spec())
            self.assertEqual(errors, [])
            slices.apply_batch(self.db, "m", corr,
                               on_commit=mirror.make_advance_hook(self.db, "m"))

        refresh([put("a", amount=9, cost=1, day=D1, region="east")])
        mirror.enable_sync(self.db, "m")
        refresh([put("b", amount=4, cost=1, day=D1, region="east")])
        version = store.get_slice_sync(self.db, "m")["current_version"]
        before = slices.read_slice(self.db, "m")
        got = slices.query_withdrawal_impact(
            self.db, "m", {"day": D1, "region": "east"})
        self.assertEqual(got["current"], 0)
        self.assertEqual(store.get_slice_sync(self.db, "m")["current_version"],
                         version)
        self.assertEqual(slices.read_slice(self.db, "m"), before)
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) AS n FROM slice_sync_log")
            .fetchone()["n"], 1)


class TestCliImpact(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = os.path.join(self.tmp.name, "cli.db")
        self.cat_path = os.path.join(self.tmp.name, "cat.json")
        with open(self.cat_path, "w", encoding="utf-8") as fh:
            json.dump(CAT, fh)

    def run_cli(self, *args, expect=0):
        from tests.test_slice import run_cli
        return run_cli(self.db_path, *args, expect=expect)

    def batch(self, name, corrections):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w", encoding="utf-8") as fh:
            for c in corrections:
                fh.write(json.dumps(c, ensure_ascii=False) + "\n")
        return path

    def test_end_to_end(self):
        self.run_cli("import-catalog", "--file", self.cat_path)
        self.run_cli("slice-init", "--slice", "s", "--catalog", "slice-cat",
                     "--seq", "1", "--metric", "net_daily@v1")
        self.run_cli("slice-refresh", "--slice", "s", "--file", self.batch(
            "b.jsonl", [put("o1", amount=100, cost=10, day=D1, region="east"),
                        put("o2", amount=200, cost=20, day=D2,
                            region="east")]))
        outs = []
        for key in ('{"day": "2026-10-01", "region": "east"}',
                    '["2026-10-02", "east"]'):
            out = json.loads(self.run_cli(
                "slice-impact", "--slice", "s", "--key", key).stdout)
            outs.append(out)
        self.assertEqual(outs[0]["changes"],
                         [{"id": "o1", "state": "group_removed"},
                          {"id": "o2", "state": "value", "value": 0}])
        self.assertEqual(outs[1]["changes"],
                         [{"id": "o1", "state": "value", "value": 0},
                          {"id": "o2", "state": "group_removed"}])

        # missing group is an error with a non-zero exit, not empty success
        proc = self.run_cli("slice-impact", "--slice", "s",
                            "--key", '{"day": "2026-10-03", "region": "east"}',
                            expect=1)
        self.assertIn("does not exist", proc.stderr)

        # preparation is a no-op for an already-prepared slice and works if
        # the marker is missing
        self.run_cli("slice-impact-prepare", "--slice", "s")

        # slice-read / impact are read-only: results and version stay stable
        before = self.run_cli("slice-read", "--slice", "s").stdout
        self.run_cli("slice-impact", "--slice", "s", "--key",
                     '{"day": "2026-10-01", "region": "east"}')
        self.assertEqual(self.run_cli("slice-read", "--slice", "s").stdout,
                         before)


if __name__ == "__main__":
    unittest.main()
