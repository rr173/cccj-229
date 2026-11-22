"""Local tests for slice mirror delivery (source export + receiver apply).

Covers the delivery contract end to end:
  * version 0 is the enable-time snapshot; every successful refresh advances
    by one even when root results are identical; rejected refreshes do not;
  * delta files equal the hand-computed diff between two delivered points
    (changed-and-reverted keys, null vs. delete, coarse-dependency corrections
    that move unrelated root groups, arbitrary precision integers);
  * neither deltas nor refreshes scan all facts or all root groups;
  * truncation, corruption, checksum and coordinate failures advance nothing;
  * exact redelivery is idempotent, same-coordinate different content and
    expired/wrong-chain files are rejected;
  * an export observes one immutable snapshot while the source keeps refreshing.

Run from the repository root:  python3 -m unittest
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from metric_analyzer import astx, mirror, slices, store  # noqa: E402
from tests.test_slice import (D1, D2, D3, baseline_root_groups, groups_map,  # noqa: E402
                              make_catalog, mirror as mirror_facts, put, delete)

SLICE = "month-end"


class MirrorCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.src_path = os.path.join(self.tmp.name, "source.db")
        self.rcv_path = os.path.join(self.tmp.name, "receiver.db")
        self.src = store.connect(self.src_path)
        self.rcv = mirror.mirror_connect(self.rcv_path)
        store.insert_catalog(self.src, make_catalog())
        self.facts = {}
        self.closure = slices.init_slice(self.src, SLICE, "slice-cat", 1,
                                         "net_daily", "v1")
        self.root_key = self.closure.root_key

    def tearDown(self):
        self.src.close()
        self.rcv.close()

    # ------------------------------------------------------------- helpers
    def refresh(self, corrections, expect_error=False):
        text = "\n".join(json.dumps(c, ensure_ascii=False)
                         for c in corrections)
        parsed, errors = slices.parse_batch(text, self.closure.required_spec())
        self.assertEqual(errors, [])
        hook = mirror.make_advance_hook(self.src, SLICE)
        if expect_error:
            with self.assertRaises(slices.SliceError):
                slices.apply_batch(self.src, SLICE, parsed, on_commit=hook)
            return
        slices.apply_batch(self.src, SLICE, parsed, on_commit=hook)
        mirror_facts(self.facts, corrections)

    def enable(self):
        return mirror.enable_sync(self.src, SLICE)

    def sync(self):
        return store.get_slice_sync(self.src, SLICE)

    def export(self, path, from_version=None):
        return mirror.export_file(self.src_path, SLICE, path,
                                  from_version=from_version)

    def receive(self, path):
        return mirror.receive_file(self.rcv, path)

    def path(self, name):
        return os.path.join(self.tmp.name, name)

    def read_mirror(self, key=None):
        return mirror.query_mirror(self.rcv, self.sync()["source_id"], SLICE,
                                   key=key)

    def read_source(self):
        return groups_map(slices.read_slice(self.src, SLICE))

    def baseline(self):
        return baseline_root_groups(make_catalog(), "net_daily", "v1",
                                    self.facts)

    def delivered_map(self):
        return groups_map(self.read_mirror())

    def file_lines(self, path):
        with open(path, "rb") as fh:
            return [json.loads(ln.decode("utf-8"))
                    for ln in fh if ln.strip()]

    def file_bytes(self, path):
        with open(path, "rb") as fh:
            return fh.read()


# ------------------------------------------------------------- version rules

class TestEnableAndVersions(MirrorCase):
    def test_enable_snapshots_v0(self):
        self.refresh([put("o1", amount=100, cost=10, day=D1, region="east")])
        info = self.enable()
        self.assertEqual(info["version"], 0)
        self.assertEqual(info["groups"], 1)
        self.assertEqual(self.sync()["current_version"], 0)
        # persistent identity survives reopening
        self.src.close()
        self.src = store.connect(self.src_path)
        again = self.sync()
        self.assertEqual(again["source_id"], info["source_id"])
        self.assertEqual(again["current_version"], 0)

    def test_no_refresh_before_enable_keeps_legacy_behaviour(self):
        self.refresh([put("o1", amount=100, cost=10, day=D1, region="east")])
        self.assertIsNone(self.sync())

    def test_successful_refresh_advances_even_without_root_change(self):
        self.enable()
        v0_rows = self.src.execute(
            "SELECT COUNT(*) AS n FROM slice_sync_log WHERE slice_name=? AND"
            " version=0", (SLICE,)).fetchone()["n"]
        # empty batches are successful refreshes but touch no root group, so
        # the version advances while the change log gains no rows at all
        self.refresh([])
        self.assertEqual(self.sync()["current_version"], 1)
        self.refresh([])
        self.assertEqual(self.sync()["current_version"], 2)
        log_rows = self.src.execute(
            "SELECT COUNT(*) AS n FROM slice_sync_log WHERE slice_name=?",
            (SLICE,)).fetchone()["n"]
        self.assertEqual(log_rows, v0_rows)
        # a fact rejected by every metric likewise leaves the root untouched
        ghost = put("x", amount=9, cost=1, status="cancelled", day=D3,
                    region="r99")
        # (net_daily itself has no filter; use an empty-grain check via the
        # no-op path instead — version still advances on any accepted batch)
        self.refresh([ghost])
        self.assertEqual(self.sync()["current_version"], 3)

    def test_rejected_refresh_does_not_consume_version(self):
        self.enable()
        before = self.sync()["current_version"]
        self.refresh([delete("ghost")], expect_error=True)
        self.assertEqual(self.sync()["current_version"], before)
        # a good refresh then advances exactly once
        self.refresh([put("o1", amount=100, cost=10, day=D1, region="east")])
        self.assertEqual(self.sync()["current_version"], before + 1)

    def test_independent_source_gets_other_identity(self):
        self.enable()
        other_path = os.path.join(self.tmp.name, "other.db")
        other = store.connect(other_path)
        store.insert_catalog(other, make_catalog())
        slices.init_slice(other, SLICE, "slice-cat", 1, "net_daily", "v1")
        other_id = mirror.enable_sync(other, SLICE)["source_id"]
        other.close()
        self.assertNotEqual(other_id, self.sync()["source_id"])


# ------------------------------------------------------------- delta content

class TestDeltaContent(MirrorCase):
    def build(self):
        self.refresh([
            put("o1", amount=100, cost=10, day=D1, region="east",
                customer="c1"),
            put("o2", amount=200, cost=20, day=D2, region="east",
                customer="c2"),
            put("o4", amount=70, cost=7, day=D1, region="west",
                customer="c3"),
        ])
        self.enable()   # v0
        self.refresh([
            put("o1", amount=150, cost=10, day=D1, region="east"),
            put("n1", amount=40, cost=4, day=D3, region="west"),
        ])               # v1
        self.refresh([delete("o4")])    # v2
        self.refresh([
            put("o1", amount=100, cost=10, day=D1, region="east")])  # v3 reverts o1

    def test_full_file_matches_source(self):
        self.refresh([
            put("o1", amount=100, cost=10, day=D1, region="east",
                customer="c1"),
            put("o2", amount=200, cost=20, day=D2, region="east",
                customer="c2"),
            put("o4", amount=70, cost=7, day=D1, region="west",
                customer="c3"),
        ])
        v0_facts = dict(self.facts)
        self.enable()   # v0 snapshot; export the full file before any refresh
        full = self.path("full.jsonl")
        self.export(full)
        self.receive(full)
        self.assertEqual(self.read_mirror()["version"], 0)
        want = baseline_root_groups(make_catalog(), "net_daily", "v1", v0_facts)
        self.assertEqual(self.delivered_map(), want)

    def test_each_delta_equals_hand_computed_endpoint_diff(self):
        # Each delta is exported right after its endpoint refresh, so the
        # endpoint is "the current version at export start" exactly as in
        # production; hand-computed endpoint maps come from an independent
        # change-log replay rather than from the exporter.
        self.refresh([
            put("o1", amount=100, cost=10, day=D1, region="east",
                customer="c1"),
            put("o2", amount=200, cost=20, day=D2, region="east",
                customer="c2"),
            put("o4", amount=70, cost=7, day=D1, region="west",
                customer="c3"),
        ])
        self.enable()
        full = self.path("full.jsonl")
        self.export(full)                 # full at v0 establishes the mirror
        self.receive(full)

        batches = [
            [put("o1", amount=150, cost=10, day=D1, region="east"),
             put("n1", amount=40, cost=4, day=D3, region="west")],   # -> v1
            [delete("o4")],                                            # -> v2
            [put("o1", amount=100, cost=10, day=D1, region="east")],  # -> v3
        ]
        sdb = sqlite3.connect(self.src_path)
        for to_version, batch in enumerate(batches, start=1):
            frm = to_version - 1
            self.refresh(batch)
            endpoint_maps = {
                v: self._replay_log(sdb, v) for v in (frm, to_version)}
            delta = self.path(f"d{frm}.jsonl")
            info = self.export(delta, from_version=frm)
            self.assertEqual(info["version"], to_version)
            ops = [ln for ln in self.file_lines(delta) if ln["kind"] == "op"]
            gkeys = [o["gkey"] for o in ops]
            self.assertEqual(len(gkeys), len(set(map(tuple, gkeys))))
            want = self._endpoint_diff(endpoint_maps[frm],
                                       endpoint_maps[to_version])
            got = {tuple(o["gkey"]): (o["change"], o.get("value"))
                   for o in ops}
            self.assertEqual(got, want,
                             f"delta {frm}->{to_version} diverges from "
                             f"hand-computed endpoint diff")
            self.assertEqual(info["records"], len(want))
            # applying it lands the receiver exactly on the source endpoint
            self.receive(delta)
            self.assertEqual(self.delivered_map(),
                             endpoint_maps[to_version])
        sdb.close()

    def _replay_log(self, sdb, version):
        """Independent endpoint reconstruction: replay the source change log
        from version 0 up to ``version`` into a plain dict."""
        state = {}
        rows = sdb.execute(
            "SELECT version, gkey, op, value FROM slice_sync_log"
            " WHERE slice_name=? AND version <= ? ORDER BY version, gkey",
            (SLICE, version)).fetchall()
        for r in rows:
            gk = tuple(json.loads(r[1]))
            if r[2] == "delete":
                state.pop(gk, None)
            else:
                state[gk] = None if r[3] is None else int(r[3])
        return state

    def _endpoint_diff(self, old, new):
        """Hand-computed net diff keyed on group existence, not null-ness."""
        out = {}
        for k in set(old) | set(new):
            if k not in old:
                out[k] = ("upsert",
                          None if new[k] is None else str(new[k]))
            elif k not in new:
                out[k] = ("delete", None)
            elif (old[k] is None) != (new[k] is None) or old[k] != new[k]:
                out[k] = ("upsert",
                          None if new[k] is None else str(new[k]))
        return {k: out[k] for k in sorted(out)}

    def test_changed_and_reverted_key_is_not_sent(self):
        self.enable()
        # o1 east d1: 100 -> 300 -> 100 across two refreshes, net identical
        self.refresh([put("o1", amount=100, cost=10, day=D1, region="east")])
        self.assertEqual(self.sync()["current_version"], 1)
        self.refresh([put("o1", amount=300, cost=10, day=D1, region="east")])
        self.refresh([put("o1", amount=100, cost=10, day=D1, region="east")])
        delta = self.path("revert.jsonl")
        self.export(delta, from_version=1)
        ops = [ln for ln in self.file_lines(delta) if ln["kind"] == "op"]
        self.assertEqual(ops, [])
        lines = self.file_lines(delta)
        self.assertEqual(lines[-1]["records"], 0)

    def test_null_upsert_is_distinct_from_delete(self):
        # net_daily(d1,west) becomes null when its coarse dependency vanishes
        # while the fine group still has qualifying facts (cancelled rows feed
        # net_daily but not rev_*). Build that state explicitly.
        self.enable()
        self.refresh([
            put("w1", amount=50, cost=5, status="cancelled", day=D1,
                region="west"),
            put("e1", amount=100, cost=10, day=D1, region="east"),
        ])  # v1: (d1,east) exists; west has no completed row yet -> no group
        self.refresh([
            put("w2", amount=70, cost=7, day=D1, region="west")])  # v2 west born
        self.refresh([delete("w2")])  # v3 west back to a null-valued group
        delta = self.path("null.jsonl")
        self.export(delta, from_version=2)
        ops = {tuple(o["gkey"]): o for o in self.file_lines(delta)
               if o["kind"] == "op"}
        west = ops[(D1, "west")]
        self.assertEqual(west["change"], "upsert")
        self.assertIsNone(west["value"])       # null group, NOT a delete
        self.assertIn((D1, "west"), self._replay_log(
            sqlite3.connect(self.src_path), 3))

    def test_coarse_dependency_correction_moves_unrelated_root_groups(self):
        # One corrected row moves both the fine group and the shared coarse
        # [region] group.  On the row's own day the two changes cancel inside
        # net_daily (sub), so that root group's value is unchanged; the OTHER
        # day of the same region changes only through the coarse dependency,
        # without any fact correction touching it.  That single remote root
        # group is exactly what the delta must carry.
        self.enable()
        p = self.path("v0.jsonl")
        self.export(p)
        self.receive(p)
        self.refresh([
            put("a", amount=100, cost=10, day=D1, region="east"),
            put("b", amount=200, cost=20, day=D2, region="east"),
            put("c", amount=70, cost=7, day=D1, region="west")])  # v1
        before = dict(self.read_source())
        self.refresh([
            put("a", amount=150, cost=10, day=D1, region="east")])  # v2
        after = self.read_source()
        self.assertEqual(after[(D1, "east")], before[(D1, "east")])  # cancels
        self.assertNotEqual(after[(D2, "east")], before[(D2, "east")])
        delta = self.path("coarse.jsonl")
        self.export(delta, from_version=1)
        got = {tuple(o["gkey"]): o for o in self.file_lines(delta)
               if o["kind"] == "op"}
        self.assertEqual(set(got), {(D2, "east")})
        self.assertEqual(got[(D2, "east")]["value"],
                         str(after[(D2, "east")]))
        self.assertEqual(after, self.baseline())

    def test_big_integers_keep_precision(self):
        # Fact inputs are int32 by the slice business rules, but materialized
        # sums/values are unbounded decimals; inject a >64-bit root value
        # directly into the materialized store and prove the wire and the
        # receiver preserve it exactly (no JSON float anywhere).
        huge = 2 ** 80
        self.refresh([put("h1", amount=1, cost=0, day=D1, region="east")])
        self.enable()
        full = self.path("big0.jsonl")
        self.export(full)
        self.receive(full)
        gk = astx.canon([D1, "east"])
        with self.src:
            self.src.execute(
                "UPDATE slice_groups SET value=? WHERE slice_name=? AND"
                " metric=? AND gkey=?",
                (str(huge), SLICE, self.root_key, gk))
            self.src.execute(
                "INSERT INTO slice_sync_log (slice_name, version, gkey, op,"
                " value) VALUES (?,?,?,'upsert',?)",
                (SLICE, 1, gk, str(huge)))
            self.src.execute(
                "UPDATE slice_sync SET current_version=1 WHERE slice_name=?",
                (SLICE,))
        delta = self.path("big.jsonl")
        self.export(delta, from_version=0)
        self.receive(delta)
        value = self.delivered_map()[(D1, "east")]
        self.assertEqual(value, huge)
        self.assertGreater(abs(value), 2 ** 63)
        # the wire itself carries decimal text, never a JSON float
        self.assertIn(str(huge).encode(), self.file_bytes(delta))
        parsed = [json.loads(ln) for ln in self.file_bytes(delta).splitlines()]
        for rec in parsed:
            if rec.get("kind") == "op":
                self.assertIsInstance(rec["value"], str)

    def test_delta_never_scans_all_root_groups_or_facts(self):
        # many groups exist; a one-key correction's delta touches only changed
        # keys, and the refresh itself stays incremental
        load = []
        for i in range(30):
            load.append(put(f"r{i:02d}-a", amount=10 + i, cost=1, day=D1,
                            region=f"r{i:02d}"))
            load.append(put(f"r{i:02d}-b", amount=100 + i, cost=1, day=D2,
                            region=f"r{i:02d}"))
        self.enable()
        self.refresh(load)     # v1
        stats = slices.apply_batch(
            self.src, SLICE,
            slices.parse_batch(
                json.dumps(put("r00-a", amount=15, cost=1, day=D1,
                               region="r00")),
                self.closure.required_spec())[0],
            on_commit=mirror.make_advance_hook(self.src, SLICE))
        self.assertLess(stats["groups_recomputed"], 10)
        delta = self.path("small.jsonl")
        self.export(delta, from_version=1)
        ops = [ln for ln in self.file_lines(delta) if ln["kind"] == "op"]
        self.assertLessEqual(len(ops), 4)


# ------------------------------------------------------------- receiver rules

class TestReceiveRejections(MirrorCase):
    def setUp(self):
        super().setUp()
        self.refresh([put("o1", amount=100, cost=10, day=D1, region="east"),
                      put("o4", amount=70, cost=7, day=D1, region="west")])
        self.enable()                       # v0 full establishes the mirror
        self.full = self.path("full.jsonl")
        self.export(self.full)
        self.refresh([put("o1", amount=150, cost=10, day=D1, region="east"),
                      put("n", amount=40, cost=4, day=D2, region="west")])  # v1
        self.delta = self.path("delta.jsonl")
        self.export(self.delta, from_version=0)

    def _snapshot_receiver(self):
        return (self.rcv.execute("SELECT COALESCE(MAX(version),-1) AS v FROM"
                                 " mirror_meta").fetchone()["v"],
                self.rcv.execute("SELECT COUNT(*) AS n FROM mirror_groups"
                                 ).fetchone()["n"],
                self.rcv.execute("SELECT COUNT(*) AS n FROM mirror_ledger"
                                 ).fetchone()["n"])

    def test_happy_path_and_reopen_visibility(self):
        snap = self._snapshot_receiver()
        self.receive(self.full)
        self.assertEqual(self.read_mirror()["version"], 0)
        self.receive(self.delta)
        self.assertEqual(self.read_mirror()["version"], 1)
        self.assertEqual(self.delivered_map(), self.read_source())
        # reopen: groups and coordinate are visible together
        self.rcv.close()
        self.rcv = mirror.mirror_connect(self.rcv_path)
        self.assertEqual(self.delivered_map(), self.read_source())
        self.assertEqual(self.read_mirror()["version"], 1)

    def test_delta_without_full_is_rejected(self):
        with self.assertRaises(mirror.MirrorError):
            self.receive(self.delta)
        self.assertEqual(self._snapshot_receiver(), (-1, 0, 0))

    def test_missing_predecessor_is_rejected(self):
        self.receive(self.full)
        jump = self.path("jump.jsonl")
        self.export(jump, from_version=0)
        # a fresh receiver trying a delta without its full: missing base
        fresh_db = mirror.mirror_connect(self.path("fresh.db"))
        with self.assertRaises(mirror.MirrorError):
            mirror.receive_file(fresh_db, jump)
        fresh_db.close()
        # on the real receiver, skipping v1 and jumping v1->v2 is rejected
        self.receive(jump)
        self.refresh([put("m", amount=3, cost=1, day=D3, region="r07")])
        skip = self.path("skip.jsonl")
        self.export(skip, from_version=1)   # third receiver is still at v0
        # receiver is at 1; fabricate the missing-predecessor case on a third
        # receiver that holds only v0, then feed it a v1->v2 delta
        third = mirror.mirror_connect(self.path("third.db"))
        mirror.receive_file(third, self.full)
        with self.assertRaises(mirror.MirrorError):
            mirror.receive_file(third, skip)
        third.close()

    def test_wrong_source_chain_is_rejected(self):
        other_path = os.path.join(self.tmp.name, "other.db")
        other = store.connect(other_path)
        store.insert_catalog(other, make_catalog())
        slices.init_slice(other, SLICE, "slice-cat", 1, "net_daily", "v1")
        parsed, _ = slices.parse_batch(
            json.dumps(put("o1", amount=100, cost=10, day=D1,
                           region="east")),
            self.closure.required_spec())
        mirror.enable_sync(other, SLICE)
        slices.apply_batch(other, SLICE, parsed,
                           on_commit=mirror.make_advance_hook(other, SLICE))
        foreign = self.path("foreign.jsonl")
        mirror.export_file(other_path, SLICE, foreign, from_version=0)
        other.close()
        self.receive(self.full)                 # establishes our chain
        with self.assertRaises(mirror.MirrorError):
            self.receive(foreign)               # other source, same slice, v0->v1
        self.assertEqual(self.read_mirror()["version"], 0)

    def test_truncated_and_corrupt_files_advance_nothing(self):
        self.receive(self.full)
        before = self._snapshot_receiver()
        raw = self.file_bytes(self.delta)
        # truncate mid-file (no manifest line at all)
        cut = self.path("trunc.jsonl")
        with open(cut, "wb") as fh:
            fh.write(raw[: len(raw) // 2])
        with self.assertRaises(mirror.MirrorError):
            self.receive(cut)
        self.assertEqual(self._snapshot_receiver(), before)
        # tamper a record byte but keep the manifest
        bad = self.path("bad.jsonl")
        lines = raw.splitlines()
        manifest_ln = lines[-1]
        body = b"\n".join(lines[:-1])
        tampered = body.replace(b"upsert", b"uxpert", 1)
        with open(bad, "wb") as fh:
            fh.write(tampered + b"\n" + manifest_ln + b"\n")
        with self.assertRaises(mirror.MirrorError):
            self.receive(bad)
        self.assertEqual(self._snapshot_receiver(), before)
        # flip a byte inside the manifest digest
        man = json.loads(manifest_ln)
        man["digest"] = "0" * 64
        forged = self.path("forged.jsonl")
        with open(forged, "wb") as fh:
            fh.write(body + b"\n" +
                     (json.dumps(man, sort_keys=True) + "\n").encode())
        with self.assertRaises(mirror.MirrorError):
            self.receive(forged)
        self.assertEqual(self._snapshot_receiver(), before)

    def test_exact_redelivery_is_idempotent_but_other_content_is_not(self):
        self.receive(self.full)
        again = self.receive(self.full)
        self.assertTrue(again["redelivery"])
        self.receive(self.delta)
        again = self.receive(self.delta)
        self.assertTrue(again["redelivery"])
        # same coordinates, different payload -> hard error, not a redelivery
        raw = self.file_bytes(self.delta)
        man_line = raw.splitlines()[-1]
        man = json.loads(man_line)
        man["records"] = man["records"] + 1
        evil = self.path("evil.jsonl")
        with open(evil, "wb") as fh:
            fh.write(raw.replace(man_line,
                                 (json.dumps(man, sort_keys=True)).encode()))
        with self.assertRaises(mirror.MirrorError):
            self.receive(evil)
        self.assertEqual(self.read_mirror()["version"], 1)

    def test_expired_full_file_cannot_overwrite(self):
        self.receive(self.full)
        self.receive(self.delta)             # mirror is now at v1
        # a full file carrying the current v1 cannot re-initialize the mirror
        current_full = self.path("full-v1.jsonl")
        self.export(current_full)            # full file labeled with current v1
        with self.assertRaises(mirror.MirrorError):
            self.receive(current_full)
        # replaying the original v0 full is an exact redelivery of a recorded
        # coordinate: it must stay a no-op and never roll the mirror backwards
        again = self.receive(self.full)
        self.assertTrue(again["redelivery"])
        self.assertEqual(self.read_mirror()["version"], 1)

    def test_receiver_is_independent_database(self):
        self.receive(self.full)
        # no source-side tables exist in the receiver
        self.assertIsNone(self.rcv.execute(
            "SELECT name FROM sqlite_master WHERE name='slice_sync'").fetchone())
        # querying one group and the mirror listing both work standalone
        one = mirror.query_mirror(self.rcv, self.sync()["source_id"], SLICE,
                                  key=json.dumps({"day": D1, "region": "east"}))
        self.assertEqual(len(one["groups"]), 1)
        names = {r["slice_name"] for r in mirror.list_mirrors(self.rcv)}
        self.assertEqual(names, {SLICE})


# ------------------------------------------------------------- concurrency

class TestExportSnapshot(MirrorCase):
    def test_export_pins_one_committed_point_while_refresh_continues(self):
        self.enable()
        self.refresh([put("o1", amount=100, cost=10, day=D1, region="east")])
        # version 1 is the export's endpoint; a refresh to version 2 happens
        # while a snapshot transaction is deliberately held open.
        hold = threading.Event()
        released = threading.Event()
        results = {}

        def worker_refresh():
            # a dedicated connection: SQLite connections are not shareable
            # across threads, and WAL lets this writer proceed while the
            # reader's snapshot stays open
            wdb = store.connect(self.src_path)
            try:
                hold.wait()
                parsed, errs = slices.parse_batch(
                    json.dumps(put("o1", amount=222, cost=10, day=D1,
                                   region="east")),
                    self.closure.required_spec())
                assert not errs
                slices.apply_batch(
                    wdb, SLICE, parsed,
                    on_commit=mirror.make_advance_hook(wdb, SLICE))
                results["version_during"] = store.get_slice_sync(
                    wdb, SLICE)["current_version"]
            finally:
                wdb.close()
                released.set()

        t = threading.Thread(target=worker_refresh)
        t.start()
        sdb = sqlite3.connect(self.src_path, timeout=30, isolation_level=None)
        sdb.execute("PRAGMA journal_mode=WAL")
        sdb.execute("BEGIN DEFERRED")
        pinned = sdb.execute(
            "SELECT current_version FROM slice_sync WHERE slice_name=?",
            (SLICE,)).fetchone()[0]
        self.assertEqual(pinned, 1)
        hold.set()
        self.assertTrue(released.wait(timeout=10))
        # snapshot still sees version 1 and the old value, not 222
        still = sdb.execute(
            "SELECT current_version FROM slice_sync WHERE slice_name=?",
            (SLICE,)).fetchone()[0]
        val = sdb.execute(
            "SELECT value FROM slice_groups WHERE slice_name=? AND metric=?"
            " AND gkey=?",
            (SLICE, self.root_key, astx.canon([D1, "east"]))).fetchone()[0]
        sdb.rollback()
        sdb.close()
        t.join()
        self.assertEqual(still, 1)
        self.assertEqual(int(val), 0)     # net_daily(east,d1) at v1, not v2
        self.assertEqual(results["version_during"], 2)

        # and the real export file written after refresh labels v2 correctly
        out = self.path("after.jsonl")
        info = self.export(out, from_version=1)
        self.assertEqual(info["version"], 2)


class TestCliWiring(unittest.TestCase):
    def test_cli_end_to_end(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        src = os.path.join(tmp.name, "src.db")
        rcv = os.path.join(tmp.name, "rcv.db")
        cat = os.path.join(tmp.name, "cat.json")
        with open(cat, "w", encoding="utf-8") as fh:
            json.dump(make_catalog(), fh)
        batch = os.path.join(tmp.name, "b.jsonl")
        with open(batch, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(put("o1", amount=100, cost=10, day=D1,
                                    region="east")) + "\n")

        def cli(db, *args, expect=0):
            # run from the repository root so the package resolves; the test
            # works only with files this suite itself generated
            proc = subprocess.run(
                [sys.executable, "-m", "metric_analyzer.cli", "--db",
                 db, *args], cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(proc.returncode, expect,
                             f"{args}: {proc.stderr}")
            return proc

        cli(src, "import-catalog", "--file", cat)
        cli(src, "slice-init", "--slice", SLICE, "--catalog", "slice-cat",
            "--seq", "1", "--metric", "net_daily@v1")
        cli(src, "slice-refresh", "--slice", SLICE, "--file", batch)
        out = cli(src, "mirror-enable", "--slice", SLICE)
        source_id = out.stdout.split("source_id:")[1].split()[0]
        full = os.path.join(tmp.name, "full.jsonl")
        cli(src, "mirror-export", "--slice", SLICE, "--out", full)
        cli(rcv, "mirror-receive", "--file", full)
        # refresh twice: one rejected (no version), one accepted
        bad = os.path.join(tmp.name, "bad.jsonl")
        with open(bad, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(delete("ghost")) + "\n")
        cli(src, "slice-refresh", "--slice", SLICE, "--file", bad, expect=1)
        cli(src, "slice-refresh", "--slice", SLICE, "--file", batch)
        status = cli(src, "mirror-status").stdout
        self.assertIn("v1", status)
        delta = os.path.join(tmp.name, "delta.jsonl")
        cli(src, "mirror-export", "--slice", SLICE, "--out", delta,
            "--from", "0")
        cli(rcv, "mirror-receive", "--file", delta)
        q = json.loads(cli(rcv, "mirror-query", "--source-id", source_id,
                           "--slice", SLICE).stdout)
        self.assertEqual(q["version"], 1)
        self.assertEqual(groups_map(q), {(D1, "east"): 0})
        self.assertIn(source_id, cli(rcv, "mirrors").stdout)
        # wrong coordinates fail through the CLI with exit 1
        proc = subprocess.run(
            [sys.executable, "-I", "-m", "metric_analyzer.cli", "--db", rcv,
             "mirror-query", "--source-id", "deadbeef", "--slice", SLICE],
            cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1)


if __name__ == "__main__":
    unittest.main()
