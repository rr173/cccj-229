"""Core analysis engine: semantic metric diff, impact analysis, blockers,
topological migration ordering, plan application.

Pure functions over parsed catalog / registry dicts — no I/O, no SQL against
production data. Persistence lives in store.py; this module is what the
independent reference implementation (reference/ref_analyzer.py) re-derives.
"""
from __future__ import annotations

import heapq

from . import astx

CHANGE_ORDER = [
    "DIMENSION_ADDED", "GRAIN_CHANGED", "FILTER_CHANGED", "DEPENDENCY_CHANGED",
    "AGG_CHANGED", "FIELD_REMOVED", "FIELD_ADDED",
]
#: change types a consumer must explicitly accept (FIELD_ADDED is informational)
DECLARABLE = set(CHANGE_ORDER) - {"FIELD_ADDED"}


# ------------------------------------------------------------------ metric diff

def diff_metric(old, new):
    """Semantic diff between two versions of one metric. Returns change list."""
    changes = []
    go, gn = set(old["grain"]), set(new["grain"])
    if go != gn:
        if gn > go:
            changes.append({"type": "DIMENSION_ADDED",
                            "detail": {"added": sorted(gn - go)}})
        else:
            changes.append({"type": "GRAIN_CHANGED",
                            "detail": {"added": sorted(gn - go),
                                       "removed": sorted(go - gn)}})
    rel = astx.filter_relation(old.get("filter"), new.get("filter"))
    if rel != "EQUIV":
        changes.append({"type": "FILTER_CHANGED", "subtype": rel,
                        "detail": {"relation": rel}})
    ro, rn = astx.refs(old["agg"]), astx.refs(new["agg"])
    if ro["metrics"] != rn["metrics"]:
        changes.append({"type": "DEPENDENCY_CHANGED",
                        "detail": {
                            "added": sorted(map(list, rn["metrics"] - ro["metrics"])),
                            "removed": sorted(map(list, ro["metrics"] - rn["metrics"])),
                        }})
    if astx.skeleton(old["agg"]) != astx.skeleton(new["agg"]):
        changes.append({"type": "AGG_CHANGED",
                        "detail": {"note": "aggregation expression shape changed"}})
    f_removed = ro["fields"] - rn["fields"]
    f_added = rn["fields"] - ro["fields"]
    if f_removed:
        changes.append({"type": "FIELD_REMOVED",
                        "detail": {"fields": sorted(f_removed)}})
    if f_added:
        changes.append({"type": "FIELD_ADDED",
                        "detail": {"fields": sorted(f_added)}})
    changes.sort(key=lambda c: CHANGE_ORDER.index(c["type"]))
    return changes


# ------------------------------------------------------------------ cycle check

def find_cycle(graph):
    """Deterministic DFS cycle finder. graph: node -> iterable of nodes.
    Returns the cycle as a sorted list of unique members, or None."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in graph}
    stack = []

    def dfs(n):
        color[n] = GRAY
        stack.append(n)
        for m in sorted(graph.get(n, ())):
            if m not in color:
                continue
            if color[m] == GRAY:
                return stack[stack.index(m):] + [m]
            if color[m] == WHITE:
                r = dfs(m)
                if r:
                    return r
        color[n] = BLACK
        stack.pop()
        return None

    for n in sorted(graph):
        if color[n] == WHITE:
            r = dfs(n)
            if r:
                return sorted(set(r))
    return None


# -------------------------------------------------------------------- analysis

def analyze(from_cat, to_cat, registry):
    """Compute migrations, contract violations, blockers and (if clean) the
    executable migration order for moving `registry` consumers from
    `from_cat` to `to_cat`."""
    mto = {(m["name"], m["version"]): m for m in to_cat["metrics"]}
    mfrom = {(m["name"], m["version"]): m for m in from_cat["metrics"]}
    reports = {r["report_id"]: r for r in registry["reports"]}
    dims_to = {d["name"] for d in to_cat.get("dimensions", [])}
    fields_to = {f["name"] for f in to_cat.get("fields", [])}

    blockers, violations, warnings = [], [], []
    migrations = {}  # (name, from_ver, to_ver) -> record

    # -- 1. static per-report checks -------------------------------------
    for r in registry["reports"]:
        rid = r["report_id"]
        by_name = {}
        for item in r["query"]["select"]:
            by_name.setdefault(item["metric"], set()).add(item["version"])
        for name, vers in sorted(by_name.items()):
            if len(vers) > 1:
                blockers.append({
                    "type": "VERSION_CONFLICT", "report": rid, "metric": name,
                    "versions": sorted(vers),
                    "reason": f"report '{rid}' selects metric '{name}' at "
                              f"conflicting versions {sorted(vers)} in one query"})
        for i, item in enumerate(r["query"]["select"]):
            if not any(item["metric"] == n for (n, _v) in mto):
                blockers.append({
                    "type": "UNKNOWN_METRIC", "report": rid, "metric": item["metric"],
                    "reason": f"select[{i}] references unknown metric "
                              f"'{item['metric']}'"})
        for d in r["query"].get("group_by", []):
            if d not in dims_to:
                blockers.append({
                    "type": "INVALID_QUERY", "report": rid,
                    "reason": f"group_by dimension '{d}' is not declared in "
                              f"catalog '{to_cat['catalog_id']}'"})
        wrefs = astx.refs(r["query"].get("where"))["fields"] if r["query"].get("where") else set()
        for f in sorted(wrefs - fields_to):
            blockers.append({
                "type": "INVALID_QUERY", "report": rid,
                "reason": f"where-clause references field '{f}' which is absent "
                          f"from the field lineage"})

    # -- 2. consumer declarations -> migrations ---------------------------
    for c in registry["consumers"]:
        rid, cid = c["report_id"], c["consumer_id"]
        rep = reports.get(rid)
        if rep is None:
            blockers.append({"type": "UNKNOWN_REPORT", "consumer": cid, "report": rid,
                             "reason": f"consumer '{cid}' declares unknown report '{rid}'"})
            continue
        for mname, tver in sorted(c.get("target_versions", {}).items()):
            items = [(i, it) for i, it in enumerate(rep["query"]["select"])
                     if it["metric"] == mname]
            if not items:
                warnings.append({"type": "UNUSED_TARGET", "consumer": cid, "report": rid,
                                 "metric": mname, "target": tver,
                                 "reason": f"declared target {mname}@{tver} but report "
                                           f"'{rid}' does not select that metric"})
                continue
            fver = items[0][1]["version"]
            if (mname, tver) not in mto:
                blockers.append({
                    "type": "MISSING_TARGET", "report": rid, "consumer": cid,
                    "metric": mname, "target": tver,
                    "reason": f"consumer '{cid}' declared target {mname}@{tver} but "
                              f"that version does not exist in catalog "
                              f"'{to_cat['catalog_id']}'"})
                continue
            if (mname, fver) not in mto:
                blockers.append({
                    "type": "VERSION_REMOVED", "report": rid, "consumer": cid,
                    "metric": mname, "version": fver,
                    "reason": f"report '{rid}' still uses {mname}@{fver} which was "
                              f"removed from catalog '{to_cat['catalog_id']}'"})
                continue
            if fver == tver:
                warnings.append({"type": "NOOP_TARGET", "consumer": cid, "report": rid,
                                 "metric": mname, "target": tver,
                                 "reason": f"{mname}@{tver} is already the version in use"})
                continue
            key = (mname, fver, tver)
            mig = migrations.get(key)
            if mig is None:
                old_def = mfrom.get((mname, fver), mto[(mname, fver)])
                mig = {"metric": mname, "from_version": fver, "to_version": tver,
                       "changes": diff_metric(old_def, mto[(mname, tver)]),
                       "consumer_map": {}, "reports": set()}
                migrations[key] = mig
            mig["consumer_map"][cid] = c
            mig["reports"].add(rid)

    # -- 3. referenced versions dropped without a declared migration ------
    covered = {(b.get("report"), b.get("metric")) for b in blockers
               if b["type"] == "VERSION_REMOVED"}
    declared_froms = {(k[0], k[1]) for k in migrations}
    for r in registry["reports"]:
        rid = r["report_id"]
        for i, item in enumerate(r["query"]["select"]):
            key = (item["metric"], item["version"])
            if (key not in mto and key not in declared_froms
                    and (rid, item["metric"]) not in covered):
                blockers.append({
                    "type": "MISSING_TARGET", "report": rid,
                    "metric": item["metric"], "version": item["version"],
                    "reason": f"select[{i}] uses {item['metric']}@{item['version']} "
                              f"which is absent from catalog '{to_cat['catalog_id']}' "
                              f"and no migration target was declared"})

    # -- 4. contract violations, pinpointed to query parts -----------------
    for (mname, fver, tver), mig in sorted(migrations.items()):
        new_grain = set(mto[(mname, tver)]["grain"])
        for rid in sorted(mig["reports"]):
            rep = reports[rid]
            group_by = rep["query"].get("group_by", [])
            for i, item in enumerate(rep["query"]["select"]):
                if item["metric"] != mname or item["version"] != fver:
                    continue
                for ch in mig["changes"]:
                    v = {"report": rid, "select_index": i,
                         "alias": item.get("alias"), "metric": mname,
                         "from_version": fver, "to_version": tver,
                         "change_type": ch["type"]}
                    if ch.get("subtype"):
                        v["subtype"] = ch["subtype"]
                    t = ch["type"]
                    if t == "GRAIN_CHANGED":
                        broken = [d for d in group_by if d not in new_grain]
                        v["broken_group_by"] = broken
                        v["contract"] = (
                            f"group_by {broken} is no longer provided by "
                            f"{mname}@{tver} (grain {sorted(new_grain)})"
                            if broken else
                            f"group_by still satisfiable, but grain contract changed "
                            f"{sorted(set(mto[(mname, fver)]['grain']))} -> {sorted(new_grain)}")
                    elif t == "DIMENSION_ADDED":
                        v["contract"] = (f"new optional dimensions {ch['detail']['added']}; "
                                         f"existing group_by {group_by} unchanged")
                    elif t == "FILTER_CHANGED":
                        v["contract"] = (f"filter semantics {ch['subtype']}: aggregated "
                                         f"values are not comparable with {mname}@{fver}")
                    elif t == "DEPENDENCY_CHANGED":
                        v["contract"] = f"dependency versions changed: {ch['detail']}"
                    elif t == "AGG_CHANGED":
                        v["contract"] = "aggregation expression shape changed"
                    elif t == "FIELD_REMOVED":
                        v["contract"] = (f"fields removed from the expression: "
                                         f"{ch['detail']['fields']}")
                    else:
                        v["contract"] = "informational: new fields referenced"
                    violations.append(v)

    # -- 5. undeclared compatibility ---------------------------------------
    for c in registry["consumers"]:
        rid, cid = c["report_id"], c["consumer_id"]
        allowed = set(c.get("allowed_changes", []))
        needed = set()
        for mig in migrations.values():
            if cid in mig["consumer_map"]:
                needed |= {ch["type"] for ch in mig["changes"]}
        missing = sorted(t for t in (needed - allowed) if t in DECLARABLE)
        if missing:
            blockers.append({
                "type": "UNDECLARED_COMPAT", "report": rid, "consumer": cid,
                "missing": missing,
                "reason": f"consumer '{cid}' did not declare compatibility for "
                          f"{missing} (allowed_changes={sorted(allowed)})"})

    # -- 6. dependency cycle among migration targets (and their closure) ---
    graph = {}
    stack = [(k[0], k[2]) for k in migrations]
    while stack:
        node = stack.pop()
        if node in graph:
            continue
        deps = set(astx.refs(mto[node]["agg"])["metrics"]) if node in mto else set()
        graph[node] = deps
        stack.extend(deps - set(graph))
    cycle = find_cycle(graph)
    if cycle:
        blockers.append({
            "type": "CYCLE", "cycle": [f"{n}@{v}" for n, v in cycle],
            "reason": "dependency cycle among migration target versions: "
                      + " -> ".join(f"{n}@{v}" for n, v in cycle)})

    # -- 7. executable order (only when nothing blocks) ---------------------
    steps = [] if blockers else topo_steps(migrations, mto)

    violations.sort(key=lambda v: (v["report"], v["select_index"],
                                   CHANGE_ORDER.index(v["change_type"])))
    blockers.sort(key=astx.canon)
    return {
        "migrations": serialize_migrations(migrations),
        "violations": violations,
        "blockers": blockers,
        "warnings": warnings,
        "steps": steps,
    }


def serialize_migrations(migrations):
    out = []
    for (n, f, t), mig in sorted(migrations.items()):
        out.append({
            "metric": n, "from_version": f, "to_version": t,
            "changes": mig["changes"],
            "consumers": [{"consumer_id": cid,
                           "window": mig["consumer_map"][cid].get("migration_window")}
                          for cid in sorted(mig["consumer_map"])],
            "reports": sorted(mig["reports"]),
        })
    return out


# --------------------------------------------------------------- topo ordering

def topo_steps(migrations, mto):
    """Kahn's algorithm over metric migrations and affected reports.

    Base metrics migrate before derived metrics that consume their target
    versions; reports migrate after every metric they consume. Ties break
    lexicographically on step id so the order is fully deterministic.
    """
    msteps, rsteps = {}, {}
    for (n, f, t), mig in migrations.items():
        sid = f"metric:{n}:{f}->{t}"
        msteps[sid] = {"id": sid, "kind": "metric", "metric": n,
                       "from_version": f, "to_version": t,
                       "changes": [c["type"] for c in mig["changes"]],
                       "consumers": sorted(mig["consumer_map"]),
                       "reports": sorted(mig["reports"])}
    affected_reports = sorted({rid for mig in migrations.values()
                               for rid in mig["reports"]})
    for rid in affected_reports:
        sid = f"report:{rid}"
        rsteps[sid] = {"id": sid, "kind": "report", "report": rid,
                       "repoints": [{"metric": n, "from": f, "to": t}
                                    for (n, f, t), mig in sorted(migrations.items())
                                    if rid in mig["reports"]]}
    deps = {sid: set() for sid in list(msteps) + list(rsteps)}
    for (n, f, t) in migrations:
        sid = f"metric:{n}:{f}->{t}"
        tgt_refs = astx.refs(mto[(n, t)]["agg"])["metrics"]
        for (dn, df, dt) in migrations:
            if (dn, dt) in tgt_refs:
                deps[sid].add(f"metric:{dn}:{df}->{dt}")
    for (n, f, t), mig in migrations.items():
        for rid in mig["reports"]:
            deps[f"report:{rid}"].add(f"metric:{n}:{f}->{t}")

    dependents = {sid: set() for sid in deps}
    for sid, ds in deps.items():
        for d in ds:
            dependents[d].add(sid)
    ready = [sid for sid, ds in deps.items() if not ds]
    heapq.heapify(ready)
    order = []
    while ready:
        sid = heapq.heappop(ready)
        order.append(sid)
        for dep in sorted(dependents[sid]):
            deps[dep].discard(sid)
            if not deps[dep]:
                heapq.heappush(ready, dep)
    if len(order) != len(deps):  # unreachable: cycles are blocked beforehand
        raise RuntimeError("dependency cycle detected during ordering")
    allsteps = {**msteps, **rsteps}
    steps = []
    for i, sid in enumerate(order, 1):
        s = dict(allsteps[sid])
        s["seq"] = i
        steps.append(s)
    return steps


# -------------------------------------------------------------- plan application

def apply_plan(registry, steps):
    """Return a new registry content with report queries repointed per plan.

    This is a pure state transition on the registry — the analyzer never
    executes the report queries themselves.
    """
    import copy
    new_reg = copy.deepcopy(registry)
    reports = {r["report_id"]: r for r in new_reg["reports"]}
    for step in steps:
        if step["kind"] != "report":
            continue
        rep = reports[step["report"]]
        repoints = {rp["metric"]: rp for rp in step["repoints"]}
        for item in rep["query"]["select"]:
            rp = repoints.get(item["metric"])
            if rp and item["version"] == rp["from"]:
                item["version"] = rp["to"]
    return new_reg
