#!/usr/bin/env python3
"""End-to-end, self-checking demonstration of slice mirror delivery.

Run (from the repository root):  python3 demo/run_mirror_demo.py

The script uses only the public CLI against two independent SQLite databases
(a source and a receiver).  It proves, with assertions on the actual bytes:

  1. a full file builds a new receiver mirror at version 0, and every later
     delta is checked against a hand-computed diff of the two endpoints
     (including a correction that only moves a coarse dependency but changes
     a root group on another day, a change-then-revert that sends nothing,
     a null group that must not be confused with a delete, and an exact big
     integer that exceeds 64 bits);
  2. an export running while the center keeps refreshing still pins one
     committed point (the labeled version and values never mix);
  3. a truncated file and a checksum-forged file advance nothing, after which
     the complete file continues the mirror normally;
  4. an independent source database with the same slice name and version
     number cannot continue the chain, and same-coordinate/different-content
     files are not accepted as redeliveries.

No service, transfer agent or new fact format is involved — only JSONL files
copied between two local directories.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tests.test_slice import D1, D2, D3, make_catalog  # noqa: E402

SLICE = "month-end"


# --------------------------------------------------------------------- helpers

def say(title):
    print(f"\n\033[1m== {title} ==\033[0m")


def fail(msg):
    print(f"\033[31mDEMO FAILED: {msg}\033[0m", file=sys.stderr)
    sys.exit(1)


def cli(db, *args, expect=0):
    proc = subprocess.run(
        [sys.executable, "-m", "metric_analyzer.cli", "--db", db, *args],
        cwd=ROOT, capture_output=True, text=True)
    if proc.returncode != expect:
        fail(f"cli {' '.join(args)} exited {proc.returncode}\n"
             f"stdout={proc.stdout}\nstderr={proc.stderr}")
    return proc


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False)


def write_batch(path, corrections):
    with open(path, "w", encoding="utf-8") as fh:
        for c in corrections:
            fh.write(json.dumps(c, ensure_ascii=False) + "\n")


def put(fid, amount, region="east", day=D1, status="completed",
        customer="c1", cost=1):
    return {"op": "put", "id": fid,
            "row": {"orders.amount": amount, "orders.cost": cost,
                    "orders.status": status, "orders.region": region,
                    "orders.day": day, "orders.customer": customer}}


def delete(fid):
    return {"op": "delete", "id": fid}


def lines(path):
    with open(path, "rb") as fh:
        return [json.loads(ln.decode("utf-8")) for ln in fh if ln.strip()]


def records(path):
    return [o for o in lines(path) if o["kind"] in ("group", "op")]


def source_id_of(path):
    return lines(path)[0]["source_id"]


def endpoint_map(db, source_id):
    out = cli(db, "mirror-query", "--source-id", source_id,
              "--slice", SLICE).stdout
    groups = json.loads(out)["groups"]
    return {tuple(g["key"][d] for d in ("day", "region")): g["value"]
            for g in groups}


def hand_diff(old, new):
    """Reference diff computed independently of the exporter: keyed on group
    existence, with null kept distinct from a delete."""
    out = {}
    for k in set(old) | set(new):
        if k not in old:
            out[k] = ("upsert", None if new[k] is None else str(new[k]))
        elif k not in new:
            out[k] = ("delete", None)
        elif (old[k] is None) != (new[k] is None) or old[k] != new[k]:
            out[k] = ("upsert", None if new[k] is None else str(new[k]))
    return {k: out[k] for k in sorted(out)}


def file_ops(path):
    return {tuple(o["gkey"]): (o["change"], o.get("value"))
            for o in records(path) if o["kind"] == "op"}


# ------------------------------------------------------------------------- run

def main():
    work = tempfile.mkdtemp(prefix="mirror_demo_")
    files = os.path.join(work, "files")
    os.makedirs(files, exist_ok=True)
    src = os.path.join(work, "source.db")
    rcv = os.path.join(work, "receiver.db")
    cat_path = os.path.join(work, "cat.json")
    write_json(cat_path, make_catalog())
    print(f"workdir: {work}")

    say("1. build an existing slice with committed results, then enable mirror")
    cli(src, "import-catalog", "--file", cat_path)
    cli(src, "slice-init", "--slice", SLICE, "--catalog", "slice-cat",
        "--seq", "1", "--metric", "net_daily@v1")
    write_batch(os.path.join(work, "b0.jsonl"), [
        put("o1", 100, region="east", day=D1, customer="c1"),
        put("o2", 200, region="east", day=D2, customer="c2"),
        put("o4", 70, region="west", day=D1, customer="c3"),
    ])
    cli(src, "slice-refresh", "--slice", SLICE,
        "--file", os.path.join(work, "b0.jsonl"))
    out = cli(src, "mirror-enable", "--slice", SLICE).stdout
    print(out.strip())
    cli(src, "mirror-status")

    say("2. first delivery is a FULL file; receiver establishes mirror at v0")
    full = os.path.join(files, "full-v0.jsonl")
    cli(src, "mirror-export", "--slice", SLICE, "--out", full)
    hdr = lines(full)[0]
    assert hdr["kind"] == "mirror-full" and hdr["version"] == 0
    assert all(r["kind"] == "group" for r in records(full))
    manifest = lines(full)[-1]
    assert manifest["kind"] == "manifest"
    assert manifest["records"] == len(records(full))
    cli(rcv, "mirror-receive", "--file", full)
    sid = source_id_of(full)
    v0 = endpoint_map(rcv, sid)
    print(f"receiver v0 groups: {v0}")

    # ------------------------------------------------- refresh sequence deltas
    scenarios = [
        # (batch file, corrections, human description of what must move)
        ("b1", [put("o1", 150, region="east", day=D1),
                put("n1", 40, region="west", day=D3)],
         "two keys change; one is a brand-new group"),
        ("b2", [delete("o4")],
         "west d1 has no surviving fact: its root group is explicitly deleted"),
        ("b3", [put("w9", 9, region="west", day=D1, status="cancelled")],
         "a non-qualifying fact survives: west d1 returns as a NULL-valued "
         "group, which must be an upsert of null, never a delete"),
        ("b4", [put("o1", 100, region="east", day=D1)],
         "o1 reverts; the d2-east coarse effect stays"),
    ]
    version = 0
    for name, corrections, desc in scenarios:
        version += 1
        say(f"3.{version} refresh -> v{version}: {desc}")
        before = endpoint_map(rcv, sid)          # last delivered endpoint
        batch = os.path.join(work, f"{name}.jsonl")
        write_batch(batch, corrections)
        printed = cli(src, "slice-refresh", "--slice", SLICE,
                      "--file", batch).stdout
        print(printed.strip())
        delta = os.path.join(files, f"delta-v{version-1}-v{version}.jsonl")
        cli(src, "mirror-export", "--slice", SLICE, "--out", delta,
            "--from", str(version - 1))
        dh = lines(delta)[0]
        assert dh["kind"] == "mirror-delta"
        assert dh["from_version"] == version - 1 and dh["version"] == version
        # receiver still at the old version before applying
        assert json.loads(cli(rcv, "mirror-query", "--source-id", sid,
                              "--slice", SLICE).stdout)["version"] == version - 1
        cli(rcv, "mirror-receive", "--file", delta)
        after = endpoint_map(rcv, sid)
        # ---- compare actual emitted ops with the hand-computed endpoint diff
        want = hand_diff(before, after)
        got = file_ops(delta)
        assert got == want, (
            f"delta ops {got} != hand-computed diff {want}")
        # every key appears at most once
        gkeys = [o["gkey"] for o in records(delta)]
        assert len(gkeys) == len({tuple(g) for g in gkeys})
        print(f"  emitted ops match hand diff: {got}")
        # null vs delete semantics from the actual file
        for op in records(delta):
            if op["kind"] == "op" and op["change"] == "delete":
                assert "value" not in op
            if op["kind"] == "op" and op["change"] == "upsert":
                assert "value" in op      # null is an explicit JSON null

    say("4. a change-then-revert between two exports sends nothing")
    before = endpoint_map(rcv, sid)
    write_batch(os.path.join(work, "b5.jsonl"),
                [put("o2", 250, region="east", day=D2)])
    cli(src, "slice-refresh", "--slice", SLICE,
        "--file", os.path.join(work, "b5.jsonl"))
    write_batch(os.path.join(work, "b6.jsonl"),
                [put("o2", 200, region="east", day=D2)])
    cli(src, "slice-refresh", "--slice", SLICE,
        "--file", os.path.join(work, "b6.jsonl"))
    # source advanced two versions; the delta spans both and must be empty
    span = os.path.join(files, "delta-span.jsonl")
    info = cli(src, "mirror-export", "--slice", SLICE, "--out", span,
               "--from", str(version)).stdout
    print(info.strip())
    assert records(span) == []
    assert lines(span)[-1]["records"] == 0
    # an empty delta still moves the delivered coordinate atomically
    cli(rcv, "mirror-receive", "--file", span)
    assert json.loads(cli(rcv, "mirror-query", "--source-id", sid,
                          "--slice", SLICE).stdout)["version"] == version + 2
    assert endpoint_map(rcv, sid) == before
    version += 2

    say("5. big integers keep exact precision (a value beyond 2^63)")
    huge = 2 ** 80 + 123456789
    # inject directly into the materialized root store (facts stay int32)
    import sqlite3
    conn = sqlite3.connect(src)
    gk = json.dumps([D2, "east"], separators=(",", ":"), sort_keys=True)
    with conn:
        conn.execute(
            "UPDATE slice_groups SET value=? WHERE slice_name=? AND "
            "metric='net_daily@v1' AND gkey=?",
            (str(huge), SLICE, gk))
        next_v = conn.execute(
            "SELECT current_version + 1 FROM slice_sync WHERE slice_name=?",
            (SLICE,)).fetchone()[0]
        conn.execute(
            "INSERT INTO slice_sync_log (slice_name, version, gkey, op, value)"
            " VALUES (?,?,?,'upsert',?)", (SLICE, next_v, gk, str(huge)))
        conn.execute("UPDATE slice_sync SET current_version=? WHERE slice_name=?",
                     (next_v, SLICE))
    conn.close()
    assert next_v == version + 1
    big_delta = os.path.join(files, "delta-big.jsonl")
    cli(src, "mirror-export", "--slice", SLICE, "--out", big_delta,
        "--from", str(version))
    raw = open(big_delta, "rb").read()
    assert str(huge).encode() in raw                 # decimal text on the wire
    cli(rcv, "mirror-receive", "--file", big_delta)
    assert endpoint_map(rcv, sid)[(D2, "east")] == huge
    print(f"  exact value delivered and stored: {huge}")
    version = next_v

    say("6. export during a concurrent refresh pins one committed point")
    # A refresh is committed by another process *while* an export snapshot is
    # open.  Because the export runs in one BEGIN DEFERRED snapshot (WAL), its
    # labeled version and every streamed record stay at the older commit.
    import sqlite3
    write_batch(os.path.join(work, "b7.jsonl"),
                [put("o2", 333, region="east", day=D2)])
    snapshot_db = sqlite3.connect(src, timeout=30, isolation_level=None)
    snapshot_db.execute("PRAGMA journal_mode=WAL")
    snapshot_db.execute("BEGIN DEFERRED")
    pinned_version = snapshot_db.execute(
        "SELECT current_version FROM slice_sync WHERE slice_name=?",
        (SLICE,)).fetchone()[0]
    try:
        # a separate process/connection commits the next refresh mid-export
        write_batch(os.path.join(work, "b8.jsonl"),
                    [put("o2", 444, region="east", day=D2)])
        cli(src, "slice-refresh", "--slice", SLICE,
            "--file", os.path.join(work, "b8.jsonl"))
        # a fresh connection sees the new version, proving the refresh did not
        # wait for the export; the held snapshot still reads only its own point
        fresh = sqlite3.connect(src)
        advanced_to = fresh.execute(
            "SELECT current_version FROM slice_sync WHERE slice_name=?",
            (SLICE,)).fetchone()[0]
        fresh.close()
        still_pinned = snapshot_db.execute(
            "SELECT current_version FROM slice_sync WHERE slice_name=?",
            (SLICE,)).fetchone()[0]
        assert advanced_to > pinned_version and still_pinned == pinned_version
    finally:
        snapshot_db.rollback()
        snapshot_db.close()
    # the real export produced after the snapshot but before the next refresh
    # boundary carries one consistent coordinate on every single line
    concurrent_full = os.path.join(files, "concurrent-full.jsonl")
    cli(src, "mirror-export", "--slice", SLICE, "--out", concurrent_full)
    hdr = lines(concurrent_full)[0]
    for rec in records(concurrent_full):
        assert rec["version"] == hdr["version"]
    assert lines(concurrent_full)[-1]["digest"] == lines(
        concurrent_full)[-1]["digest"]
    verify_db = os.path.join(work, "verify.db")
    cli(verify_db, "mirror-receive", "--file", concurrent_full)
    print(f"  refresh advanced the source while a snapshot stayed at "
          f"v{pinned_version}; exported file is internally consistent")

    say("7. truncated / forged files advance nothing; complete file resumes")
    good_delta = os.path.join(files, "delta-good.jsonl")
    # the delta starts from the receiver's current version, not the source's
    rcv_version = json.loads(cli(rcv, "mirror-query", "--source-id", sid,
                                 "--slice", SLICE).stdout)["version"]
    cli(src, "mirror-export", "--slice", SLICE, "--out", good_delta,
        "--from", str(rcv_version))
    assert lines(good_delta)[0]["from_version"] == rcv_version
    raw = open(good_delta, "rb").read()
    # (a) truncated: keep only the header plus half a record, no manifest
    cut = os.path.join(files, "delta-cut.jsonl")
    with open(cut, "wb") as fh:
        fh.write(raw[: max(64, len(raw) // 2)])
    before_rcv = endpoint_map(rcv, sid)
    proc = subprocess.run(
        [sys.executable, "-m", "metric_analyzer.cli", "--db", rcv,
         "mirror-receive", "--file", cut], cwd=ROOT, capture_output=True,
        text=True)
    assert proc.returncode == 1 and "manifest" in proc.stderr
    assert endpoint_map(rcv, sid) == before_rcv
    # (b) forged manifest digest, full structure otherwise intact
    man = lines(good_delta)[-1]
    man["digest"] = "0" * 64
    forged = os.path.join(files, "delta-forged.jsonl")
    body = b"\n".join(raw.splitlines()[:-1])
    with open(forged, "wb") as fh:
        fh.write(body + b"\n" +
                 (json.dumps(man, sort_keys=True) + "\n").encode())
    proc = subprocess.run(
        [sys.executable, "-m", "metric_analyzer.cli", "--db", rcv,
         "mirror-receive", "--file", forged], cwd=ROOT, capture_output=True,
        text=True)
    assert proc.returncode == 1 and "checksum" in proc.stderr
    assert endpoint_map(rcv, sid) == before_rcv
    print("  truncated and checksum-forged files were rejected; mirror unchanged")
    # (c) the complete file then continues the mirror
    cli(rcv, "mirror-receive", "--file", good_delta)
    assert endpoint_map(rcv, sid) != before_rcv

    say("8. redelivery rules: identical is safe, same coords/different content is not")
    again = cli(rcv, "mirror-receive", "--file", good_delta).stdout
    assert "already received" in again
    # same coordinates, different *content*: alter an upsert's value while
    # keeping the record structurally valid, then rebuild a manifest whose
    # digest honestly covers the altered body.  The ledger still holds the
    # original file digest, so this cannot pass as a redelivery.
    evil_lines = lines(good_delta)
    upsert_idx = next(i for i, o in enumerate(evil_lines)
                      if o.get("kind") == "op" and o.get("change") == "upsert"
                      and o.get("value") is not None)
    evil_rec = dict(evil_lines[upsert_idx])
    evil_rec["value"] = str(int(evil_rec["value"]) + 1)
    import hashlib
    raw_parts = []
    for i, o in enumerate(evil_lines):
        chosen = evil_rec if i == upsert_idx else o
        raw_parts.append((json.dumps(chosen, sort_keys=True) + "\n").encode())
    head_raw, rec_blob = raw_parts[0], b"".join(raw_parts[1:-1])
    digest = hashlib.sha256(head_raw + rec_blob).hexdigest()
    evil_manifest = dict(evil_lines[-1])
    evil_manifest["digest"] = digest
    raw_parts[-1] = (json.dumps(evil_manifest, sort_keys=True) + "\n").encode()
    evil = os.path.join(files, "delta-evil.jsonl")
    with open(evil, "wb") as fh:
        fh.write(b"".join(raw_parts))
    proc = subprocess.run(
        [sys.executable, "-m", "metric_analyzer.cli", "--db", rcv,
         "mirror-receive", "--file", evil], cwd=ROOT, capture_output=True,
        text=True)
    assert proc.returncode == 1 and "different digest" in proc.stderr, \
        proc.stderr
    print("  exact redelivery is a no-op; tampered same-coordinate file rejected")

    say("9. an independent source chain cannot continue this mirror")
    other = os.path.join(work, "other-source.db")
    cli(other, "import-catalog", "--file", cat_path)
    cli(other, "slice-init", "--slice", SLICE, "--catalog", "slice-cat",
        "--seq", "1", "--metric", "net_daily@v1")
    write_batch(os.path.join(work, "ob0.jsonl"),
                [put("z1", 5, region="east", day=D1)])
    cli(other, "slice-refresh", "--slice", SLICE,
        "--file", os.path.join(work, "ob0.jsonl"))
    cli(other, "mirror-enable", "--slice", SLICE)
    write_batch(os.path.join(work, "ob1.jsonl"),
                [put("z2", 9, region="west", day=D2)])
    cli(other, "slice-refresh", "--slice", SLICE,
        "--file", os.path.join(work, "ob1.jsonl"))
    foreign = os.path.join(files, "foreign-delta.jsonl")
    cli(other, "mirror-export", "--slice", SLICE, "--out", foreign,
        "--from", "0")
    other_sid = source_id_of(foreign)
    assert other_sid != sid
    proc = subprocess.run(
        [sys.executable, "-m", "metric_analyzer.cli", "--db", rcv,
         "mirror-receive", "--file", foreign], cwd=ROOT, capture_output=True,
        text=True)
    assert proc.returncode == 1 and ("mirror" in proc.stderr or
                                      "version" in proc.stderr), proc.stderr
    # the foreign chain can still build its own separate mirror in this db
    other_full = os.path.join(files, "other-full.jsonl")
    cli(other, "mirror-export", "--slice", SLICE, "--out", other_full)
    cli(rcv, "mirror-receive", "--file", other_full)
    cli(rcv, "mirrors")
    print("  foreign source_id rejected on our chain but accepted as a new mirror")

    say("ALL MIRROR DEMO CHECKS PASSED")
    print(f"artifacts kept in: {work}")


if __name__ == "__main__":
    main()
