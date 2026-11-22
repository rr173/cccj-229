"""Command line interface for the metric catalog change analyzer.

Interfaces:
  import-catalog    import + validate a versioned metric catalog (JSON)
  import-registry   import + validate reports and consumer declarations
  validate          re-validate a stored catalog (AST / reference resolvability)
  analyze           impact analysis between two catalog versions
  plan              generate and store a migration plan (bound to input digests)
  blockers          list blocking items of a stored plan
  explain           item-by-item explanation of plan steps / blockers
  execute           apply a READY plan (fails if inputs changed -> INVALIDATED)
  export-registry   print the current registry content (post-migration queries)
  plans             list stored plans
  slice-init        create a named offline slice (pinned catalog import + root)
  slice-refresh     apply a JSONL fact-correction batch to a slice (incremental)
  slice-read        read the root metric's groups of a slice (JSON)
  slices            list stored slices
  mirror-enable     enable versioned mirror delivery for an existing slice (v0)
  mirror-export     stream a full / delta integrity-checked mirror JSONL file
  mirror-receive    verify + atomically apply a mirror file to a receiver db
  mirror-query      read delivered groups/version from a receiver db (JSON)
  mirror-status     list mirror-enabled slices in the source database
  mirrors           list mirrors held by a receiver database
"""
from __future__ import annotations

import argparse
import json
import sys

from . import core, loader, mirror, slices, store

EXIT_OK, EXIT_INVALID, EXIT_INVALIDATED, EXIT_BLOCKED = 0, 1, 3, 4


def _load_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _emit(obj, as_json):
    if as_json:
        print(json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True))
    return obj


def _fail(msg, code):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


# --------------------------------------------------------------------- commands

def cmd_import_catalog(args):
    cat = _load_json(args.file)
    errors = loader.validate_catalog(cat)
    if errors:
        for e in errors:
            print(f"  INVALID: {e}", file=sys.stderr)
        _fail(f"catalog '{cat.get('catalog_id')}' rejected: {len(errors)} "
              f"validation error(s)", EXIT_INVALID)
    db = store.connect(args.db)
    seq, h = store.insert_catalog(db, cat)
    names = sorted({m["name"] for m in cat["metrics"]})
    print(f"imported catalog '{cat['catalog_id']}' seq={seq} hash={h[:12]}...")
    print(f"  metrics: {len(cat['metrics'])} versions across {len(names)} names: "
          + ", ".join(names))
    print(f"  fields: {len(cat['fields'])}, dimensions: {len(cat['dimensions'])}")


def cmd_import_registry(args):
    reg = _load_json(args.file)
    errors = loader.validate_registry(reg, core.CHANGE_ORDER)
    if errors:
        for e in errors:
            print(f"  INVALID: {e}", file=sys.stderr)
        _fail(f"registry '{reg.get('registry_id')}' rejected: {len(errors)} "
              f"validation error(s)", EXIT_INVALID)
    db = store.connect(args.db)
    seq, h = store.insert_registry(db, reg)
    print(f"imported registry '{reg['registry_id']}' seq={seq} hash={h[:12]}...")
    print(f"  reports: {len(reg['reports'])}, consumers: {len(reg['consumers'])}")


def cmd_validate(args):
    db = store.connect(args.db)
    cat = store.latest_catalog(db, args.catalog)
    if cat is None:
        _fail(f"catalog '{args.catalog}' not found", EXIT_INVALID)
    errors = loader.validate_catalog(cat["content"])
    if errors:
        for e in errors:
            print(f"  INVALID: {e}")
        _fail(f"catalog '{args.catalog}' seq={cat['seq']} is invalid", EXIT_INVALID)
    deps = {}
    for m in cat["content"]["metrics"]:
        from . import astx
        deps[f"{m['name']}@{m['version']}"] = sorted(
            f"{n}@{v}" for n, v in astx.refs(m["agg"])["metrics"])
    print(f"catalog '{args.catalog}' seq={cat['seq']}: VALID")
    print(f"  {len(cat['content']['metrics'])} metric versions, "
          f"all field/metric references resolvable")
    for k in sorted(deps):
        if deps[k]:
            print(f"  deps: {k} -> {', '.join(deps[k])}")


def _fetch_inputs(db, args):
    from_cat = store.latest_catalog(db, args.from_catalog)
    to_cat = store.latest_catalog(db, args.to_catalog)
    reg = store.latest_registry(db, args.registry)
    if from_cat is None:
        _fail(f"catalog '{args.from_catalog}' not found", EXIT_INVALID)
    if to_cat is None:
        _fail(f"catalog '{args.to_catalog}' not found", EXIT_INVALID)
    if reg is None:
        _fail(f"registry '{args.registry}' not found", EXIT_INVALID)
    return from_cat, to_cat, reg


def _print_analysis(result):
    print(f"=== affected metrics ({len(result['migrations'])}) ===")
    for m in result["migrations"]:
        chs = ", ".join(c["type"] + (f"/{c['subtype']}" if c.get("subtype") else "")
                        for c in m["changes"])
        print(f"  {m['metric']}: {m['from_version']} -> {m['to_version']}  [{chs}]")
        print(f"    consumers: {[c['consumer_id'] for c in m['consumers']]}, "
              f"reports: {m['reports']}")
    print(f"=== contract violations ({len(result['violations'])}) ===")
    for v in result["violations"]:
        st = f"/{v['subtype']}" if v.get("subtype") else ""
        print(f"  {v['report']} select[{v['select_index']}] "
              f"({v['metric']} {v['from_version']}->{v['to_version']}): "
              f"{v['change_type']}{st}")
        print(f"    {v['contract']}")
    print(f"=== blockers ({len(result['blockers'])}) ===")
    for b in result["blockers"]:
        print(f"  [{b['type']}] {b['reason']}")
    for w in result["warnings"]:
        print(f"  warning[{w['type']}]: {w['reason']}")
    if result["steps"]:
        print(f"=== migration order ({len(result['steps'])} steps) ===")
        for s in result["steps"]:
            print(f"  {s['seq']:>2}. {s['id']}")


def cmd_analyze(args):
    db = store.connect(args.db)
    from_cat, to_cat, reg = _fetch_inputs(db, args)
    result = core.analyze(from_cat["content"], to_cat["content"], reg["content"])
    if args.json:
        _emit(result, True)
    else:
        print(f"impact analysis: {args.from_catalog} -> {args.to_catalog} "
              f"(registry {args.registry})")
        _print_analysis(result)


def _build_bindings(reg, result):
    reports = {r["report_id"]: r for r in reg["content"]["reports"]}
    consumers = {c["consumer_id"]: c for c in reg["content"]["consumers"]}
    affected_reports = sorted({v["report"] for v in result["violations"]} |
                              {s["report"] for s in result["steps"]
                               if s["kind"] == "report"})
    involved_consumers = sorted({c["consumer_id"]
                                 for m in result["migrations"]
                                 for c in m["consumers"]} |
                                {b["consumer"] for b in result["blockers"]
                                 if "consumer" in b})
    return {
        "reports": {rid: store.content_hash(reports[rid]["query"])
                    for rid in affected_reports if rid in reports},
        "consumers": {cid: store.content_hash(consumers[cid])
                      for cid in involved_consumers if cid in consumers},
    }


def cmd_plan(args):
    db = store.connect(args.db)
    from_cat, to_cat, reg = _fetch_inputs(db, args)
    result = core.analyze(from_cat["content"], to_cat["content"], reg["content"])
    bindings = _build_bindings(reg, result)
    plan_id, status = store.create_plan(db, from_cat, to_cat, reg, result, bindings)
    if args.json:
        _emit({"plan_id": plan_id, "status": status, **result}, True)
    else:
        print(f"plan {plan_id} created: status={status}")
        print(f"  bound inputs: {args.from_catalog}@{from_cat['hash'][:12]} -> "
              f"{args.to_catalog}@{to_cat['hash'][:12]}, "
              f"registry {args.registry}@{reg['hash'][:12]}")
        print(f"  bound digests: {len(bindings['reports'])} report queries, "
              f"{len(bindings['consumers'])} consumer declarations")
        _print_analysis(result)


def _get_plan_or_fail(db, plan_id):
    plan = store.get_plan(db, plan_id)
    if plan is None:
        _fail(f"plan '{plan_id}' not found", EXIT_INVALID)
    return plan


def cmd_blockers(args):
    db = store.connect(args.db)
    plan = _get_plan_or_fail(db, args.plan)
    blockers = plan["result"]["blockers"]
    if args.json:
        _emit({"plan_id": plan["plan_id"], "status": plan["status"],
               "blockers": blockers}, True)
        return
    print(f"plan {plan['plan_id']} status={plan['status']}: "
          f"{len(blockers)} blocker(s)")
    for b in blockers:
        print(f"  [{b['type']}] {b['reason']}")


def cmd_explain(args):
    db = store.connect(args.db)
    plan = _get_plan_or_fail(db, args.plan)
    result = plan["result"]
    out = {"plan_id": plan["plan_id"], "status": plan["status"],
           "items": []}
    migrations = {f"{m['metric']}:{m['from_version']}->{m['to_version']}": m
                  for m in result["migrations"]}
    if args.step is not None:
        steps = [s for s in result["steps"] if s["seq"] == args.step]
        if not steps:
            _fail(f"plan has no step #{args.step}", EXIT_INVALID)
        todo = steps
    else:
        todo = result["steps"]
    for s in todo:
        if s["kind"] == "metric":
            mig = migrations[f"{s['metric']}:{s['from_version']}->{s['to_version']}"]
            changes = "; ".join(
                c["type"] + (f"/{c['subtype']}" if c.get("subtype") else "")
                + f" {c.get('detail', {})}" for c in mig["changes"])
            wins = ", ".join(f"{c['consumer_id']} window "
                             f"{c['window']['start']}..{c['window']['end']}"
                             for c in mig["consumers"])
            text = (f"step {s['seq']} METRIC {s['metric']} "
                    f"{s['from_version']}->{s['to_version']}: declared by {wins}. "
                    f"Semantic changes: {changes}. "
                    f"Consumed by reports {s['reports']}.")
            item = {"seq": s["seq"], "kind": "metric", "object": s["id"],
                    "changes": mig["changes"], "explanation": text}
        else:
            parts = ", ".join(f"select item {rp['metric']} {rp['from']}->{rp['to']}"
                              for rp in s["repoints"])
            vs = [v for v in result["violations"] if v["report"] == s["report"]]
            notes = "; ".join(f"select[{v['select_index']}] {v['change_type']}: "
                              f"{v['contract']}" for v in vs)
            text = (f"step {s['seq']} REPORT {s['report']}: repoint {parts}. "
                    f"Contract impact per query part: {notes}")
            item = {"seq": s["seq"], "kind": "report", "object": s["id"],
                    "repoints": s["repoints"], "explanation": text}
        out["items"].append(item)
    if args.step is None and not todo:
        for i, b in enumerate(result["blockers"], 1):
            vs = [v for v in result["violations"] if v["report"] == b.get("report")]
            notes = "; ".join(f"select[{v['select_index']}] {v['change_type']}: "
                              f"{v['contract']}" for v in vs)
            text = f"blocker #{i} [{b['type']}]: {b['reason']}"
            if notes:
                text += f" | affected query parts: {notes}"
            out["items"].append({"blocker": i, "type": b["type"],
                                 "detail": b, "explanation": text})
    if args.json:
        _emit(out, True)
        return
    print(f"plan {plan['plan_id']} status={plan['status']} — item-by-item explanation")
    for item in out["items"]:
        print(f"  {item['explanation']}")


def cmd_execute(args):
    db = store.connect(args.db)
    plan = _get_plan_or_fail(db, args.plan)
    if plan["status"] != "READY":
        _fail(f"plan {plan['plan_id']} is {plan['status']}, not READY; "
              f"refusing to execute", EXIT_BLOCKED)

    # --- recompute every bound digest; any drift invalidates the plan ----
    mismatches = []
    from_cat = store.latest_catalog(db, plan["from_catalog"])
    to_cat = store.latest_catalog(db, plan["to_catalog"])
    reg = store.latest_registry(db, plan["registry_id"])
    if from_cat is None or from_cat["hash"] != plan["from_hash"]:
        mismatches.append(f"catalog '{plan['from_catalog']}' content changed")
    if to_cat is None or to_cat["hash"] != plan["to_hash"]:
        mismatches.append(f"catalog '{plan['to_catalog']}' content changed")
    if reg is None or reg["hash"] != plan["registry_hash"]:
        mismatches.append(f"registry '{plan['registry_id']}' content changed")
    if reg is not None and not mismatches:
        reports = {r["report_id"]: r for r in reg["content"]["reports"]}
        for rid, digest in plan["bindings"]["reports"].items():
            cur = (store.content_hash(reports[rid]["query"])
                   if rid in reports else None)
            if cur != digest:
                mismatches.append(f"query text of report '{rid}' changed")
        consumers = {c["consumer_id"]: c for c in reg["content"]["consumers"]}
        for cid, digest in plan["bindings"]["consumers"].items():
            cur = store.content_hash(consumers.get(cid))
            if cur != digest:
                mismatches.append(f"declaration of consumer '{cid}' changed")
    if mismatches:
        store.set_plan_status(db, plan["plan_id"], "INVALIDATED")
        for m in mismatches:
            print(f"  STALE: {m}", file=sys.stderr)
        _fail(f"plan {plan['plan_id']} INVALIDATED: bound inputs changed; "
              f"refusing to execute a stale plan", EXIT_INVALIDATED)

    # --- apply: pure registry state transition, no production queries ----
    steps = plan["result"]["steps"]
    new_reg = core.apply_plan(reg["content"], steps)
    seq, h = store.insert_registry(db, new_reg)
    store.set_plan_status(db, plan["plan_id"], "EXECUTED")
    print(f"plan {plan['plan_id']} EXECUTED ({len(steps)} steps)")
    for s in steps:
        print(f"  {s['seq']:>2}. {s['id']}  done")
    print(f"registry '{plan['registry_id']}' advanced to seq={seq} "
          f"hash={h[:12]}... (report queries repointed)")
    print("note: no production queries were executed; this was a metadata "
          "state transition only")


def cmd_export_registry(args):
    db = store.connect(args.db)
    reg = store.latest_registry(db, args.registry)
    if reg is None:
        _fail(f"registry '{args.registry}' not found", EXIT_INVALID)
    _emit(reg["content"], True)


def cmd_plans(args):
    db = store.connect(args.db)
    for row in store.list_plans(db):
        print(f"  {row['plan_id']}  {row['status']:<11} "
              f"{row['from_catalog']} -> {row['to_catalog']} "
              f"(registry {row['registry_id']})  {row['created_at']}")


# ------------------------------------------------------------- slice commands

def cmd_slice_init(args):
    name, sep, version = args.metric.partition("@")
    if not sep or not name or not version:
        _fail("--metric must be given as name@version", EXIT_INVALID)
    db = store.connect(args.db)
    try:
        closure = slices.init_slice(db, args.slice, args.catalog, args.seq,
                                    name, version)
    except slices.SliceError as exc:
        for err in exc.errors:
            print(f"  INVALID: {err}", file=sys.stderr)
        _fail(f"slice '{args.slice}' rejected: {len(exc.errors)} error(s)",
              EXIT_INVALID)
    print(f"slice '{args.slice}' created: root={name}@{version}, "
          f"catalog='{args.catalog}' seq={args.seq}")
    print(f"  closure: {len(closure.metrics)} metric(s): "
          + ", ".join(sorted(closure.metrics)))
    print("  pinned to this catalog import; starts empty and only accepts "
          "fact corrections from now on")


def cmd_slice_refresh(args):
    db = store.connect(args.db)
    meta = store.get_slice(db, args.slice)
    if meta is None:
        _fail(f"slice '{args.slice}' not found", EXIT_INVALID)
    cat = store.get_catalog(db, meta["catalog_id"], meta["catalog_seq"])
    closure = slices.compile_closure(cat["content"], meta["root_name"],
                                     meta["root_version"])
    with open(args.file, "r", encoding="utf-8") as fh:
        text = fh.read()
    corrections, errors = slices.parse_batch(text, closure.required_spec())
    if errors:
        for err in errors:
            print(f"  INVALID: {err}", file=sys.stderr)
        _fail(f"batch rejected: {len(errors)} error(s); "
              f"slice '{args.slice}' unchanged", EXIT_INVALID)
    try:
        hook = mirror.make_advance_hook(db, args.slice)
        stats = slices.apply_batch(db, args.slice, corrections, on_commit=hook)
    except (slices.SliceError, mirror.MirrorError) as exc:
        errs = exc.errors if isinstance(exc, slices.SliceError) else [str(exc)]
        for err in errs:
            print(f"  INVALID: {err}", file=sys.stderr)
        _fail(f"batch rejected; slice '{args.slice}' unchanged", EXIT_INVALID)
    puts = sum(1 for c in corrections if c["op"] == "put")
    print(f"slice '{args.slice}': applied {puts} put(s), "
          f"{len(corrections) - puts} delete(s); facts={stats['facts']}")
    print(f"  rows examined: {stats['rows_examined']}, "
          f"groups recomputed: {stats['groups_recomputed']}")
    if hook is not None:
        sync = store.get_slice_sync(db, args.slice)
        print(f"  mirror delivery: advanced to version {sync['current_version']}"
              f" (source {sync['source_id'][:12]}…)")


def cmd_slice_read(args):
    db = store.connect(args.db)
    try:
        result = slices.read_slice(db, args.slice)
    except slices.SliceError as exc:
        _fail(exc.errors[0], EXIT_INVALID)
    _emit(result, True)


def cmd_slices(args):
    db = store.connect(args.db)
    for row in store.list_slices(db):
        print(f"  {row['slice_name']}  root={row['root_name']}@"
              f"{row['root_version']}  catalog={row['catalog_id']}"
              f"#{row['catalog_seq']}  facts={row['facts_count']}  "
              f"{row['created_at']}")


# ------------------------------------------------------- mirror delivery cmds

def cmd_mirror_enable(args):
    db = store.connect(args.db)
    try:
        info = mirror.enable_sync(db, args.slice)
    except mirror.MirrorError as exc:
        _fail(str(exc), EXIT_INVALID)
    print(f"mirror delivery enabled for slice '{args.slice}'")
    print(f"  source_id: {info['source_id']}  (persistent identity of this "
          f"source; deliverable coordinates are source_id+slice+version)")
    print(f"  committed root groups snapshotted as version 0 "
          f"({info['groups']} group(s)); no older history is fabricated")


def cmd_mirror_export(args):
    try:
        info = mirror.export_file(args.db, args.slice, args.out,
                                  from_version=args.from_version)
    except mirror.MirrorError as exc:
        _fail(str(exc), EXIT_INVALID)
    span = ("full" if info["from_version"] is None
            else f"v{info['from_version']} -> v{info['version']}")
    print(f"wrote {info['kind']} ({span}) to {info['path']}")
    print(f"  source_id: {info['source_id']}, records: {info['records']}, "
          f"digest: {info['digest'][:16]}…")


def cmd_mirror_receive(args):
    db = mirror.mirror_connect(args.db)
    try:
        info = mirror.receive_file(db, args.file)
    except mirror.MirrorError as exc:
        _fail(str(exc), EXIT_INVALID)
    finally:
        db.close()
    if info["redelivery"]:
        print(f"identical file already received: ({info['source_id']}, "
              f"{info['slice']}, v{info['version']}); no change applied")
    else:
        span = ("full" if info["from_version"] is None
                else f"v{info['from_version']} -> v{info['version']}")
        print(f"accepted {span} ({info['records']} record(s)); mirror is now at "
              f"version {info['version']}")
        print(f"  digest: {info['digest'][:16]}…")


def cmd_mirror_query(args):
    db = mirror.mirror_connect(args.db)
    try:
        result = mirror.query_mirror(db, args.source_id, args.slice,
                                     key=args.key)
    except mirror.MirrorError as exc:
        _fail(str(exc), EXIT_INVALID)
    finally:
        db.close()
    _emit(result, True)


def cmd_mirror_status(args):
    db = store.connect(args.db)
    rows = store.list_slice_sync(db)
    if not rows:
        print("  (no slice has mirror delivery enabled)")
    for r in rows:
        print(f"  {r['slice_name']}  v{r['current_version']}  "
              f"source_id={r['source_id']}  root={r['root_name']}@"
              f"{r['root_version']}  catalog={r['catalog_id']}#"
              f"{r['catalog_seq']}  enabled={r['enabled_at']}")


def cmd_mirrors(args):
    db = mirror.mirror_connect(args.db)
    rows = mirror.list_mirrors(db)
    db.close()
    if not rows:
        print("  (no mirrors in this receiver database)")
    for r in rows:
        print(f"  {r['slice_name']}  v{r['version']}  "
              f"source_id={r['source_id']}  root={r['root_name']}@"
              f"{r['root_version']}  grain={r['grain_json']}")


# ------------------------------------------------------------------------ main

def main(argv=None):
    ap = argparse.ArgumentParser(prog="metric-analyzer",
                                 description="metric catalog change analyzer")
    ap.add_argument("--db", default="analyzer.db", help="SQLite database path")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("import-catalog", help="import + validate a catalog JSON")
    p.add_argument("--file", required=True)
    p.set_defaults(fn=cmd_import_catalog)

    p = sub.add_parser("import-registry", help="import + validate a registry JSON")
    p.add_argument("--file", required=True)
    p.set_defaults(fn=cmd_import_registry)

    p = sub.add_parser("validate", help="re-validate a stored catalog")
    p.add_argument("--catalog", required=True)
    p.set_defaults(fn=cmd_validate)

    p = sub.add_parser("analyze", help="impact analysis between catalog versions")
    p.add_argument("--from", dest="from_catalog", required=True)
    p.add_argument("--to", dest="to_catalog", required=True)
    p.add_argument("--registry", required=True)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_analyze)

    p = sub.add_parser("plan", help="generate and store a migration plan")
    p.add_argument("--from", dest="from_catalog", required=True)
    p.add_argument("--to", dest="to_catalog", required=True)
    p.add_argument("--registry", required=True)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_plan)

    p = sub.add_parser("blockers", help="list blockers of a stored plan")
    p.add_argument("--plan", required=True)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_blockers)

    p = sub.add_parser("explain", help="item-by-item explanation of a plan")
    p.add_argument("--plan", required=True)
    p.add_argument("--step", type=int, default=None)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_explain)

    p = sub.add_parser("execute", help="execute a READY plan (stale plans are rejected)")
    p.add_argument("--plan", required=True)
    p.set_defaults(fn=cmd_execute)

    p = sub.add_parser("export-registry", help="print current registry content")
    p.add_argument("--registry", required=True)
    p.set_defaults(fn=cmd_export_registry)

    p = sub.add_parser("plans", help="list stored plans")
    p.set_defaults(fn=cmd_plans)

    p = sub.add_parser("slice-init",
                       help="create a named offline slice bound to one catalog "
                            "import and one explicitly versioned root metric")
    p.add_argument("--slice", required=True, help="name of the slice to create")
    p.add_argument("--catalog", required=True, help="catalog_id of an imported catalog")
    p.add_argument("--seq", type=int, required=True,
                   help="import sequence number of the catalog")
    p.add_argument("--metric", required=True, help="root metric as name@version")
    p.set_defaults(fn=cmd_slice_init)

    p = sub.add_parser("slice-refresh",
                       help="apply a JSONL fact-correction batch to a slice")
    p.add_argument("--slice", required=True)
    p.add_argument("--file", required=True, help="JSONL corrections file")
    p.set_defaults(fn=cmd_slice_refresh)

    p = sub.add_parser("slice-read",
                       help="read the root metric's groups of a slice (JSON)")
    p.add_argument("--slice", required=True)
    p.set_defaults(fn=cmd_slice_read)

    p = sub.add_parser("slices", help="list stored slices")
    p.set_defaults(fn=cmd_slices)

    p = sub.add_parser("mirror-enable",
                       help="enable mirror delivery for an existing slice and "
                            "snapshot its committed root groups as version 0")
    p.add_argument("--slice", required=True)
    p.set_defaults(fn=cmd_mirror_enable)

    p = sub.add_parser("mirror-export",
                       help="stream a full file (no --from) or a delta file "
                            "(--from an older version) of one slice")
    p.add_argument("--slice", required=True)
    p.add_argument("--out", required=True, help="output JSONL path")
    p.add_argument("--from", dest="from_version", type=int, default=None,
                   help="old version the receiver holds; omit for a full file")
    p.set_defaults(fn=cmd_mirror_export)

    p = sub.add_parser("mirror-receive",
                       help="verify and atomically apply a mirror JSONL file "
                            "to an independent receiver database")
    p.add_argument("--file", required=True)
    p.set_defaults(fn=cmd_mirror_receive)

    p = sub.add_parser("mirror-query",
                       help="read groups of one delivered version from a "
                            "receiver database (JSON)")
    p.add_argument("--source-id", required=True)
    p.add_argument("--slice", required=True)
    p.add_argument("--key", default=None,
                   help="optional JSON key object/array for one group")
    p.set_defaults(fn=cmd_mirror_query)

    p = sub.add_parser("mirror-status",
                       help="list mirror-enabled slices in the source database")
    p.set_defaults(fn=cmd_mirror_status)

    p = sub.add_parser("mirrors",
                       help="list mirrors held by the receiver database")
    p.set_defaults(fn=cmd_mirrors)

    args = ap.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
