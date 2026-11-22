#!/usr/bin/env python3
"""Independent reference implementation of the metric-catalog change rules.

Reads two catalog JSON files and one registry JSON file, then recomputes —
from first principles, sharing no code with the product — the affected
metric set, per-report contract violations, blockers and the topological
migration order. Used by demo/reconcile.py to cross-check the product.

Usage: ref_analyzer.py FROM_CATALOG TO_CATALOG REGISTRY
Output: canonical JSON on stdout.
"""
import heapq
import json
import sys

ORDER = ["DIMENSION_ADDED", "GRAIN_CHANGED", "FILTER_CHANGED",
         "DEPENDENCY_CHANGED", "AGG_CHANGED", "FIELD_REMOVED", "FIELD_ADDED"]
DECLARABLE = set(ORDER) - {"FIELD_ADDED"}
TRUE = {"op": "lit", "value": True}


def canon(x):
    return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ---- AST walking (independent implementation) -------------------------------

def children(n):
    op = n.get("op")
    if op in ("add", "sub", "mul", "div", "and", "or", "coalesce"):
        return list(n.get("args", []))
    if op in ("neg", "not", "sum", "avg", "min", "max", "count_distinct"):
        return [n.get("arg")]
    if op == "cmp":
        return [n.get("left"), n.get("right")]
    if op == "in":
        return [n.get("left")]
    if op == "case":
        out = []
        for w in n.get("when", []):
            out += [w.get("cond"), w.get("then")]
        return out + [n.get("else")]
    return []


def norm(n):
    if not isinstance(n, dict):
        return n
    op = n.get("op")
    m = dict(n)
    if op in ("add", "mul", "and", "or"):
        flat = []
        for a in n.get("args", []):
            na = norm(a)
            if isinstance(na, dict) and na.get("op") == op:
                flat.extend(na["args"])
            else:
                flat.append(na)
        flat.sort(key=canon)
        if op in ("and", "or"):
            uniq = []
            for a in flat:
                if not uniq or canon(uniq[-1]) != canon(a):
                    uniq.append(a)
            flat = uniq
        m["args"] = flat
    elif op in ("sub", "div", "coalesce"):
        m["args"] = [norm(a) for a in n.get("args", [])]
    elif op in ("neg", "not", "sum", "avg", "min", "max", "count_distinct"):
        m["arg"] = norm(n.get("arg"))
    elif op == "cmp":
        l, r = norm(n.get("left")), norm(n.get("right"))
        if n.get("cmp") in ("=", "!=") and canon(r) < canon(l):
            l, r = r, l
        m["left"], m["right"] = l, r
    elif op == "in":
        m["left"] = norm(n.get("left"))
        m["values"] = sorted(n.get("values", []), key=canon)
    elif op == "case":
        m["when"] = [{"cond": norm(w["cond"]), "then": norm(w["then"])}
                     for w in n.get("when", [])]
        m["else"] = norm(n.get("else"))
    return m


def collect(n, fields, metrics):
    if not isinstance(n, dict):
        return
    if n.get("op") == "field":
        fields.add(n.get("name"))
    if n.get("op") == "metric":
        metrics.add((n.get("name"), n.get("version")))
    for c in children(n):
        collect(c, fields, metrics)


def shape(n):
    if not isinstance(n, dict):
        return n
    op = n.get("op")
    if op == "field":
        return {"op": "field"}
    if op == "metric":
        return {"op": "metric"}
    if op in ("add", "sub", "mul", "div", "and", "or", "coalesce"):
        return {"op": op, "args": [shape(a) for a in n.get("args", [])]}
    if op in ("neg", "not", "sum", "avg", "min", "max", "count_distinct"):
        return {"op": op, "arg": shape(n.get("arg"))}
    if op == "cmp":
        return {"op": op, "cmp": n.get("cmp"),
                "left": shape(n.get("left")), "right": shape(n.get("right"))}
    if op == "in":
        return {"op": "in", "left": shape(n.get("left")),
                "values": sorted(n.get("values", []), key=canon)}
    if op == "case":
        return {"op": "case",
                "when": [{"cond": shape(w["cond"]), "then": shape(w["then"])}
                         for w in n.get("when", [])],
                "else": shape(n.get("else"))}
    return dict(n)


# ---- filter relation (independent implementation) ----------------------------

def conjuncts(f):
    f = norm(f if f is not None else TRUE)
    return f["args"] if isinstance(f, dict) and f.get("op") == "and" else [f]


def as_eq(f):
    if (isinstance(f, dict) and f.get("op") == "cmp"
            and isinstance(f.get("left"), dict) and f["left"].get("op") == "field"
            and isinstance(f.get("right"), dict) and f["right"].get("op") == "lit"):
        return f["left"]["name"], f["cmp"], f["right"]["value"]
    return None


def as_in(f):
    if (isinstance(f, dict) and f.get("op") == "in"
            and isinstance(f.get("left"), dict) and f["left"].get("op") == "field"):
        return f["left"]["name"], f.get("values", [])
    return None


def implies1(p, q):
    if canon(p) == canon(q):
        return True
    if isinstance(q, dict) and q.get("op") == "lit" and q.get("value") is True:
        return True
    pe, qe = as_eq(p), as_eq(q)
    pi, qi = as_in(p), as_in(q)
    if pe and qi and pe[0] == qi[0] and pe[1] == "=":
        return pe[2] in qi[1]
    if pi and qi and pi[0] == qi[0]:
        return set(map(canon, pi[1])) <= set(map(canon, qi[1]))
    if pe and qe and pe[0] == qe[0]:
        _, po, a = pe
        _, qo, b = qe
        try:
            if po == "=":
                return {">=": a >= b, ">": a > b, "<=": a <= b, "<": a < b}.get(qo, False)
            if po == ">=":
                return (a >= b) if qo == ">=" else (a > b if qo == ">" else False)
            if po == ">":
                return a >= b if qo in (">=", ">") else False
            if po == "<=":
                return (a <= b) if qo == "<=" else (a < b if qo == "<" else False)
            if po == "<":
                return a <= b if qo in ("<=", "<") else False
        except TypeError:
            return False
    return False


def filter_rel(old, new):
    old = old if old is not None else TRUE
    new = new if new is not None else TRUE
    if canon(norm(old)) == canon(norm(new)):
        return "EQUIV"
    ps, qs = conjuncts(old), conjuncts(new)
    o2n = all(any(implies1(p, q) for p in ps) for q in qs)
    n2o = all(any(implies1(q, p) for q in qs) for p in ps)
    if o2n and n2o:
        return "EQUIV"
    if n2o:
        return "NARROWED"
    if o2n:
        return "WIDENED"
    return "RESHAPED"


# ---- rules -------------------------------------------------------------------

def diff(old, new):
    out = []
    go, gn = set(old["grain"]), set(new["grain"])
    if go != gn:
        if gn > go:
            out.append({"type": "DIMENSION_ADDED", "detail": {"added": sorted(gn - go)}})
        else:
            out.append({"type": "GRAIN_CHANGED",
                        "detail": {"added": sorted(gn - go), "removed": sorted(go - gn)}})
    rel = filter_rel(old.get("filter"), new.get("filter"))
    if rel != "EQUIV":
        out.append({"type": "FILTER_CHANGED", "subtype": rel,
                    "detail": {"relation": rel}})
    of, om = set(), set()
    nf, nm = set(), set()
    collect(old["agg"], of, om)
    collect(new["agg"], nf, nm)
    if om != nm:
        out.append({"type": "DEPENDENCY_CHANGED",
                    "detail": {"added": sorted(map(list, nm - om)),
                               "removed": sorted(map(list, om - nm))}})
    if shape(old["agg"]) != shape(new["agg"]):
        out.append({"type": "AGG_CHANGED",
                    "detail": {"note": "aggregation expression shape changed"}})
    if of - nf:
        out.append({"type": "FIELD_REMOVED", "detail": {"fields": sorted(of - nf)}})
    if nf - of:
        out.append({"type": "FIELD_ADDED", "detail": {"fields": sorted(nf - of)}})
    out.sort(key=lambda c: ORDER.index(c["type"]))
    return out


def cycle_in(graph):
    color = {}
    stack = []

    def dfs(n):
        color[n] = 1
        stack.append(n)
        for m in sorted(graph.get(n, ())):
            if m not in graph:
                continue
            if color.get(m) == 1:
                return sorted(set(stack[stack.index(m):] + [m]))
            if color.get(m, 0) == 0:
                r = dfs(m)
                if r:
                    return r
        color[n] = 2
        stack.pop()
        return None

    for n in sorted(graph):
        if color.get(n, 0) == 0:
            r = dfs(n)
            if r:
                return r
    return None


def analyze(from_cat, to_cat, registry):
    mto = {(m["name"], m["version"]): m for m in to_cat["metrics"]}
    mfrom = {(m["name"], m["version"]): m for m in from_cat["metrics"]}
    reports = {r["report_id"]: r for r in registry["reports"]}
    dims = {d["name"] for d in to_cat.get("dimensions", [])}
    fields = {f["name"] for f in to_cat.get("fields", [])}
    blockers, violations, migrations = [], [], {}

    for r in registry["reports"]:
        rid = r["report_id"]
        by_name = {}
        for it in r["query"]["select"]:
            by_name.setdefault(it["metric"], set()).add(it["version"])
        for name, vers in sorted(by_name.items()):
            if len(vers) > 1:
                blockers.append({"type": "VERSION_CONFLICT", "report": rid,
                                 "metric": name, "versions": sorted(vers)})
        for it in r["query"]["select"]:
            if not any(it["metric"] == n for (n, _v) in mto):
                blockers.append({"type": "UNKNOWN_METRIC", "report": rid,
                                 "metric": it["metric"]})
        for d in r["query"].get("group_by", []):
            if d not in dims:
                blockers.append({"type": "INVALID_QUERY", "report": rid})
        if r["query"].get("where"):
            wf = set()
            collect(r["query"]["where"], wf, set())
            if wf - fields:
                blockers.append({"type": "INVALID_QUERY", "report": rid})

    for c in registry["consumers"]:
        rid, cid = c["report_id"], c["consumer_id"]
        rep = reports.get(rid)
        if rep is None:
            blockers.append({"type": "UNKNOWN_REPORT", "consumer": cid, "report": rid})
            continue
        for mname, tver in sorted(c.get("target_versions", {}).items()):
            items = [it for it in rep["query"]["select"] if it["metric"] == mname]
            if not items:
                continue
            fver = items[0]["version"]
            if (mname, tver) not in mto:
                blockers.append({"type": "MISSING_TARGET", "report": rid,
                                 "consumer": cid, "metric": mname, "target": tver})
                continue
            if (mname, fver) not in mto:
                blockers.append({"type": "VERSION_REMOVED", "report": rid,
                                 "consumer": cid, "metric": mname, "version": fver})
                continue
            if fver == tver:
                continue
            key = (mname, fver, tver)
            if key not in migrations:
                old_def = mfrom.get((mname, fver), mto[(mname, fver)])
                migrations[key] = {"changes": diff(old_def, mto[(mname, tver)]),
                                   "consumers": set(), "reports": set()}
            migrations[key]["consumers"].add(cid)
            migrations[key]["reports"].add(rid)

    covered = {(b.get("report"), b.get("metric")) for b in blockers
               if b["type"] == "VERSION_REMOVED"}
    declared = {(k[0], k[1]) for k in migrations}
    for r in registry["reports"]:
        rid = r["report_id"]
        for it in r["query"]["select"]:
            key = (it["metric"], it["version"])
            if (key not in mto and key not in declared
                    and (rid, it["metric"]) not in covered):
                blockers.append({"type": "MISSING_TARGET", "report": rid,
                                 "metric": it["metric"], "version": it["version"]})

    for (mname, fver, tver), mig in sorted(migrations.items()):
        new_grain = set(mto[(mname, tver)]["grain"])
        for rid in sorted(mig["reports"]):
            rep = reports[rid]
            for i, it in enumerate(rep["query"]["select"]):
                if it["metric"] != mname or it["version"] != fver:
                    continue
                for ch in mig["changes"]:
                    v = {"report": rid, "select_index": i, "metric": mname,
                         "from_version": fver, "to_version": tver,
                         "change_type": ch["type"]}
                    if ch.get("subtype"):
                        v["subtype"] = ch["subtype"]
                    if ch["type"] == "GRAIN_CHANGED":
                        v["broken_group_by"] = [
                            d for d in rep["query"].get("group_by", [])
                            if d not in new_grain]
                    violations.append(v)

    for c in registry["consumers"]:
        rid, cid = c["report_id"], c["consumer_id"]
        allowed = set(c.get("allowed_changes", []))
        needed = set()
        for (mname, fver, tver), mig in migrations.items():
            if cid in mig["consumers"]:
                needed |= {ch["type"] for ch in mig["changes"]}
        missing = sorted(t for t in (needed - allowed) if t in DECLARABLE)
        if missing:
            blockers.append({"type": "UNDECLARED_COMPAT", "report": rid,
                             "consumer": cid, "missing": missing})

    graph = {}
    todo = [(k[0], k[2]) for k in migrations]
    while todo:
        node = todo.pop()
        if node in graph:
            continue
        deps = set()
        if node in mto:
            collect(mto[node]["agg"], set(), deps)
        graph[node] = deps
        todo.extend(deps - set(graph))
    cyc = cycle_in(graph)
    if cyc:
        blockers.append({"type": "CYCLE", "cycle": [f"{n}@{v}" for n, v in cyc]})

    steps = []
    if not blockers:
        deps = {}
        info = {}
        for (n, f, t), mig in migrations.items():
            sid = f"metric:{n}:{f}->{t}"
            deps[sid] = set()
            info[sid] = mig
        for rid in sorted({r for m in migrations.values() for r in m["reports"]}):
            deps[f"report:{rid}"] = set()
        for (n, f, t) in migrations:
            sid = f"metric:{n}:{f}->{t}"
            tgt_refs = set()
            collect(mto[(n, t)]["agg"], set(), tgt_refs)
            for (dn, df, dt) in migrations:
                if (dn, dt) in tgt_refs:
                    deps[sid].add(f"metric:{dn}:{df}->{dt}")
        for (n, f, t), mig in migrations.items():
            for rid in mig["reports"]:
                deps[f"report:{rid}"].add(f"metric:{n}:{f}->{t}")
        dependents = {k: set() for k in deps}
        for k, ds in deps.items():
            for d in ds:
                dependents[d].add(k)
        ready = [k for k, ds in deps.items() if not ds]
        heapq.heapify(ready)
        order = []
        while ready:
            k = heapq.heappop(ready)
            order.append(k)
            for d in dependents[k]:
                deps[d].discard(k)
                if not deps[d]:
                    heapq.heappush(ready, d)
        steps = order

    violations.sort(key=lambda v: (v["report"], v["select_index"],
                                   ORDER.index(v["change_type"])))
    blockers.sort(key=canon)
    migs = []
    for (n, f, t), mig in sorted(migrations.items()):
        migs.append({"metric": n, "from_version": f, "to_version": t,
                     "changes": mig["changes"],
                     "consumers": sorted(mig["consumers"]),
                     "reports": sorted(mig["reports"])})
    return {"migrations": migs, "violations": violations,
            "blockers": blockers, "steps": steps}


def project(result):
    """Canonical comparison projection shared with the reconciler."""
    return {
        "migrations": [
            {"metric": m["metric"], "from_version": m["from_version"],
             "to_version": m["to_version"],
             "changes": [{"type": c["type"], "subtype": c.get("subtype"),
                          "detail": c.get("detail")} for c in m["changes"]],
             "consumers": m["consumers"], "reports": m["reports"]}
            for m in result["migrations"]],
        "violations": result["violations"],
        "blockers": result["blockers"],
        "steps": result["steps"],
    }


def main():
    if len(sys.argv) != 4:
        sys.exit("usage: ref_analyzer.py FROM_CATALOG TO_CATALOG REGISTRY")
    with open(sys.argv[1], encoding="utf-8") as fh:
        from_cat = json.load(fh)
    with open(sys.argv[2], encoding="utf-8") as fh:
        to_cat = json.load(fh)
    with open(sys.argv[3], encoding="utf-8") as fh:
        registry = json.load(fh)
    result = analyze(from_cat, to_cat, registry)
    print(json.dumps(project(result), indent=2, ensure_ascii=False,
                     sort_keys=True))


if __name__ == "__main__":
    main()
