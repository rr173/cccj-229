#!/usr/bin/env bash
# End-to-end demo of the metric catalog change analyzer.
# Every expected failure is asserted; any silent success of a bad operation
# aborts the script.
set -euo pipefail
cd "$(dirname "$0")/.."

DB=/tmp/analyzer_demo.db
DATA=demo/data
CLI="python3 -m metric_analyzer.cli --db $DB"
rm -f "$DB"

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { printf '\033[31mDEMO FAILED: %s\033[0m\n' "$*" >&2; exit 1; }

json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }

say "1. import versioned catalogs and registries"
$CLI import-catalog  --file $DATA/cat_v1.json
$CLI import-catalog  --file $DATA/cat_v2.json
$CLI import-registry --file $DATA/reg_v1.json
$CLI import-registry --file $DATA/reg_v2.json

say "2. AST validation rejects unresolvable references (import-time)"
if $CLI import-catalog --file $DATA/broken_catalog.json 2>/tmp/broken.err; then
  fail "broken catalog was accepted"
fi
grep -q "orders.ghost" /tmp/broken.err || fail "missing field error not reported"
grep -q "revenue.*v9"   /tmp/broken.err || fail "missing metric-version error not reported"
grep -q "drop_table"    /tmp/broken.err || fail "illegal op not reported"
cat /tmp/broken.err

say "3. validate stored catalog cat-v2 (AST + dependency report)"
$CLI validate --catalog cat-v2

say "4. impact analysis with incomplete consumer declarations"
$CLI analyze --from cat-v1 --to cat-v2 --registry reg-v1 > /tmp/impact1.txt
cat /tmp/impact1.txt
grep -q "FILTER_CHANGED/WIDENED"  /tmp/impact1.txt || fail "filter widening not classified"
grep -q "DIMENSION_ADDED"         /tmp/impact1.txt || fail "dimension addition not classified"
grep -q "GRAIN_CHANGED"           /tmp/impact1.txt || fail "grain change not classified"
grep -q "DEPENDENCY_CHANGED"      /tmp/impact1.txt || fail "dependency change not classified"

say "5. plan with incomplete declarations is BLOCKED, blockers explained"
PLAN_BAD=$($CLI plan --from cat-v1 --to cat-v2 --registry reg-v1 --json | json_field "['plan_id']")
echo "plan: $PLAN_BAD"
$CLI blockers --plan "$PLAN_BAD" > /tmp/blockers1.txt
cat /tmp/blockers1.txt
grep -q "UNDECLARED_COMPAT" /tmp/blockers1.txt || fail "undeclared-compat blocker missing"
grep -q "MISSING_TARGET"    /tmp/blockers1.txt || fail "missing-target blocker missing"
grep -q "VERSION_CONFLICT"  /tmp/blockers1.txt || fail "version-conflict blocker missing"
$CLI explain --plan "$PLAN_BAD"

say "6. dependency cycle blocks planning"
$CLI import-catalog  --file $DATA/cat_cycle.json
$CLI import-registry --file $DATA/reg_cycle.json
PLAN_CYC=$($CLI plan --from cat-cycle --to cat-cycle --registry reg-cycle --json | json_field "['plan_id']")
$CLI blockers --plan "$PLAN_CYC" > /tmp/blockers_cyc.txt
cat /tmp/blockers_cyc.txt
grep -q "CYCLE" /tmp/blockers_cyc.txt || fail "cycle blocker missing"

say "7. corrected declarations produce an executable, topologically ordered plan"
PLAN_OK=$($CLI plan --from cat-v1 --to cat-v2 --registry reg-v2 --json | json_field "['plan_id']")
echo "plan: $PLAN_OK"
$CLI explain --plan "$PLAN_OK" > /tmp/explain_ok.txt
cat /tmp/explain_ok.txt
python3 - <<'EOF'
import json, subprocess
out = subprocess.run(
    ["python3", "-m", "metric_analyzer.cli", "--db", "/tmp/analyzer_demo.db",
     "analyze", "--from", "cat-v1", "--to", "cat-v2", "--registry", "reg-v2", "--json"],
    capture_output=True, text=True)
steps = [s["id"] for s in json.loads(out.stdout)["steps"]]
kinds = [s.split(":", 1)[0] for s in steps]
assert kinds == sorted(kinds, key=lambda k: 0 if k == "metric" else 1), \
    f"metrics must migrate before reports: {steps}"
i_net = steps.index("metric:net_revenue:v1->v2")
i_rev = steps.index("metric:revenue:v1->v2")
assert i_rev < i_net, f"base metric must migrate before dependent: {steps}"
assert steps.index("metric:revenue:v1->v2") < steps.index("report:rpt_regional")
print("order verified:", " -> ".join(steps))
EOF

say "8. tamper with the catalog, then the old plan must be REJECTED"
$CLI import-catalog --file $DATA/cat_v2_tampered.json
set +e
$CLI execute --plan "$PLAN_OK" > /tmp/exec_stale.txt 2>&1
RC=$?
set -e
cat /tmp/exec_stale.txt
[ "$RC" -eq 3 ] || fail "stale plan was not rejected with exit 3 (got $RC)"
grep -q "INVALIDATED" /tmp/exec_stale.txt || fail "plan not marked INVALIDATED"

say "9. restore the catalog, regenerate, execute: state transition succeeds"
$CLI import-catalog --file $DATA/cat_v2.json
PLAN_OK2=$($CLI plan --from cat-v1 --to cat-v2 --registry reg-v2 --json | json_field "['plan_id']")
$CLI execute --plan "$PLAN_OK2"
$CLI export-registry --registry reg-v2 > /tmp/reg_after.json
python3 - <<'EOF'
import json
reg = json.load(open("/tmp/reg_after.json"))
sel = {r["report_id"]: [(i["metric"], i["version"]) for i in r["query"]["select"]]
       for r in reg["reports"]}
assert ("revenue", "v2") in sel["rpt_regional"], sel
assert ("net_revenue", "v2") in sel["rpt_regional"], sel
assert ("daily_revenue", "v2") in sel["rpt_daily"], sel
assert ("revenue", "v3") in sel["rpt_channel"], sel
print("registry repointed as planned:", sel)
EOF

say "10. field removal impact (net_revenue v2 -> v3 drops orders.cost)"
$CLI import-catalog  --file $DATA/cat_v3.json
$CLI import-registry --file $DATA/reg_v3.json
$CLI analyze --from cat-v2 --to cat-v3 --registry reg-v3 > /tmp/impact3.txt
cat /tmp/impact3.txt
grep -q "FIELD_REMOVED" /tmp/impact3.txt || fail "field removal not classified"

say "11. reconcile product vs independent reference implementation"
python3 demo/reconcile.py

say "ALL DEMO CHECKS PASSED"
