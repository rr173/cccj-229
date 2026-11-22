#!/usr/bin/env python3
"""Reconcile the product analyzer against the independent reference program.

For each scenario both sides compute: affected metrics + semantic changes,
per-query-part contract violations, blockers, and the topological migration
order. Any item-level difference fails the reconciliation.

Usage: python3 demo/reconcile.py   (run from /workspace)
"""
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "demo", "data")
REF = os.path.join(ROOT, "reference", "ref_analyzer.py")

SCENARIOS = [
    # (name, from_catalog, to_catalog, registry, expect_ready)
    ("ready-plan (all changes declared)", "cat-v1", "cat-v2", "reg-v2", True),
    ("blocked-plan (undeclared/missing/conflict)", "cat-v1", "cat-v2", "reg-v1", False),
    ("field-removal", "cat-v2", "cat-v3", "reg-v3", True),
    ("dependency-cycle", "cat-cycle", "cat-cycle", "reg-cycle", False),
]

IMPORTS = ["cat_v1.json", "cat_v2.json", "cat_v3.json", "cat_cycle.json",
           "reg_v1.json", "reg_v2.json", "reg_v3.json", "reg_cycle.json"]


def run(cmd, **kw):
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, **kw)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(f"command failed: {' '.join(cmd)}")
    return proc.stdout


def product_analyze(db, frm, to, reg):
    out = run([sys.executable, "-m", "metric_analyzer.cli", "--db", db,
               "analyze", "--from", frm, "--to", to, "--registry", reg,
               "--json"])
    return json.loads(out)


def reference_analyze(frm, to, reg):
    out = run([sys.executable, "-I", REF,
               os.path.join(DATA, frm.replace("-", "_") + ".json"),
               os.path.join(DATA, to.replace("-", "_") + ".json"),
               os.path.join(DATA, reg.replace("-", "_") + ".json")])
    return json.loads(out)


def project_product(result):
    """Project the product's rich analysis onto the reference's canonical shape."""
    return {
        "migrations": [
            {"metric": m["metric"], "from_version": m["from_version"],
             "to_version": m["to_version"],
             "changes": [{"type": c["type"], "subtype": c.get("subtype"),
                          "detail": c.get("detail")} for c in m["changes"]],
             "consumers": sorted(c["consumer_id"] for c in m["consumers"]),
             "reports": m["reports"]}
            for m in result["migrations"]],
        "violations": [
            {k: v[k] for k in ("report", "select_index", "metric",
                               "from_version", "to_version", "change_type",
                               "subtype", "broken_group_by") if k in v}
            for v in result["violations"]],
        "blockers": [
            {k: b[k] for k in b if k != "reason"}
            for b in result["blockers"]],
        "steps": [s["id"] for s in result["steps"]],
    }


def compare_section(name, prod_items, ref_items, key):
    ok = True
    p = sorted((json.dumps(key(i), sort_keys=True), i)
               for i in prod_items)
    r = sorted((json.dumps(key(i), sort_keys=True), i)
               for i in ref_items)
    pk = [k for k, _ in p]
    rk = [k for k, _ in r]
    if pk == rk:
        print(f"    {name:<12} OK ({len(pk)} items)")
    else:
        ok = False
        print(f"    {name:<12} MISMATCH")
        only_p = [i for k, i in p if k not in rk]
        only_r = [i for k, i in r if k not in pk]
        for i in only_p:
            print(f"      product-only: {json.dumps(i, ensure_ascii=False, sort_keys=True)}")
        for i in only_r:
            print(f"      reference-only: {json.dumps(i, ensure_ascii=False, sort_keys=True)}")
    return ok


def main():
    db = os.path.join(tempfile.mkdtemp(prefix="reconcile-"), "recon.db")
    for fname in IMPORTS:
        kind = "import-catalog" if fname.startswith("cat_") else "import-registry"
        run([sys.executable, "-m", "metric_analyzer.cli", "--db", db, kind,
             "--file", os.path.join(DATA, fname)])

    all_ok = True
    for name, frm, to, reg, expect_ready in SCENARIOS:
        print(f"scenario: {name}  [{frm} -> {to}, {reg}]")
        prod = project_product(product_analyze(db, frm, to, reg))
        ref = reference_analyze(frm, to, reg)

        ok = True
        ok &= compare_section("migrations", prod["migrations"], ref["migrations"],
                              key=lambda m: (m["metric"], m["from_version"],
                                             m["to_version"],
                                             json.dumps(m["changes"], sort_keys=True),
                                             tuple(m["consumers"]), tuple(m["reports"])))
        ok &= compare_section("violations", prod["violations"], ref["violations"],
                              key=lambda v: (v["report"], v["select_index"],
                                             v["change_type"], v.get("subtype") or "",
                                             tuple(v.get("broken_group_by", []))))
        ok &= compare_section("blockers", prod["blockers"], ref["blockers"],
                              key=lambda b: json.dumps(b, sort_keys=True))
        if prod["steps"] == ref["steps"]:
            print(f"    {'steps':<12} OK ({len(prod['steps'])} steps, exact order)")
        else:
            ok = False
            print(f"    steps        MISMATCH\n      product:   {prod['steps']}\n"
                  f"      reference: {ref['steps']}")
        ready = bool(prod["steps"]) and not prod["blockers"]
        if ready != expect_ready:
            ok = False
            print(f"    expectation  MISMATCH: ready={ready}, expected {expect_ready}")
        all_ok &= ok
        print(f"  -> {'RECONCILED' if ok else 'DIVERGED'}")

    print("=" * 60)
    print("RECONCILIATION " + ("PASSED" if all_ok else "FAILED"))
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
