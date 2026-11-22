"""Offline metric slices: incremental refresh of materialized metric groups
under fact corrections.

A *slice* is a named, re-readable offline view.  The first refresh
(``slice-init``) pins it to one already-imported catalog ``(catalog_id, seq)``
and one explicitly versioned root metric ``name@version``, and starts it from
an empty fact set.  From then on the slice only accepts fact corrections — it
never follows later imports of the same catalog and never re-binds metrics.

A correction batch is a JSONL file; each line is one of::

    {"op": "put",    "id": "o-1", "row": {"orders.amount": 100, ...}}
    {"op": "delete", "id": "o-1"}

``put`` carries the complete new row (insert or replace), ``delete`` removes
an existing row.  Row field names are the catalog field names of one
flattened fact table — no joins, no other shapes.  An ``id`` may occur at
most once per batch.  A missing referenced field, a type mismatch, a
duplicate id or deleting a non-existent id rejects the whole batch; facts and
materialized values commit in a single transaction, so after a rejected (or
crashed) batch the database still shows exactly the previous batch's results.

Refresh is incremental: corrected rows are looked up by id, only the groups
those rows directly touch are recomputed, and changes propagate to upstream
groups through a reverse reference index (``slice_refs``).  Neither all facts
nor all groups are ever scanned.

Semantics (whole version-reference closure of the root):
  * every metric applies its own row filter and grain to the corrected,
    complete fact set; a group exists iff at least one qualifying fact exists
    for it (including the empty-grain case);
  * a parent metric may only reference dependencies whose grain is a subset
    of its own; the dependency group is found by projecting the parent key
    onto same-named dimensions (never by key-prefix guessing, never by
    copying dependency values onto facts);
  * a missing dependency group evaluates to ``null``; arithmetic propagates
    ``null``, but the parent group record is kept while it has qualifying
    facts.

Supported definition subset (validated at slice-init, closure only — unrelated
metrics never affect the slice):
  filter:  absent | boolean literal | field = same-type literal | and | or
  agg:     sum(field) | count_distinct(field) | metric@version | add | sub(binary)
Facts carry string / boolean / signed 32-bit integer values only; dimension
and count_distinct fields must be non-empty strings; all arithmetic is exact
(Python integers, sums and values stored as decimal text).
"""
from __future__ import annotations

import heapq
import json

from . import astx, core, store

INT32_MIN = -(2 ** 31)
INT32_MAX = 2 ** 31 - 1

#: catalog field type -> slice-level fact kind
FACT_KIND = {"number": "int", "string": "str", "boolean": "bool", "date": "str"}
#: field types whose values may serve as dimension / count_distinct values
STRING_LIKE = {"string", "date"}


class SliceError(Exception):
    """A slice definition or a correction batch was rejected."""

    def __init__(self, errors):
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


def _key(name, version):
    return f"{name}@{version}"


# ------------------------------------------------------------------ compiling

class _Metric:
    """One closure metric compiled down to the executable restricted subset."""

    __slots__ = ("key", "grain", "grain_fields", "filter", "expr", "leaves",
                 "refs", "proj", "rank")

    def __init__(self, key, grain, grain_fields, filter_, expr, leaves, refs,
                 proj):
        self.key = key
        self.grain = grain                # [dimension, ...] in catalog order
        self.grain_fields = grain_fields  # [field, ...] parallel to grain
        self.filter = filter_             # compiled filter (see eval_filter)
        self.expr = expr                  # compiled agg (see eval_expr)
        self.leaves = leaves              # {leaf_id: ("sum"|"cd", field)}
        self.refs = refs                  # [dep metric key, ...] deduped
        self.proj = proj                  # {dep key: [parent grain indexes]}
        self.rank = 0                     # topological rank (deps first)


class Closure:
    """The compiled reference closure a slice is bound to."""

    def __init__(self, root_key, metrics, field_types):
        self.root_key = root_key
        self.metrics = metrics            # {metric key: _Metric}
        self.field_types = field_types    # {field: catalog type}

    def required_spec(self):
        """Fields every fact row must carry, and which must be non-empty."""
        req, nonempty = {}, set()
        for m in self.metrics.values():
            for f in m.grain_fields:
                req[f] = self.field_types[f]
                nonempty.add(f)
            for kind, f in m.leaves.values():
                req[f] = self.field_types[f]
                if kind == "cd":
                    nonempty.add(f)
            for f in _filter_fields(m.filter):
                req[f] = self.field_types[f]
        return {"fields": req, "nonempty": nonempty}


def _filter_fields(f):
    out = set()

    def walk(node):
        kind = node[0]
        if kind == "eq":
            out.add(node[1])
        elif kind in ("and", "or"):
            for a in node[1]:
                walk(a)

    walk(f)
    return out


def _lit_ok(ftype, v):
    kind = FACT_KIND[ftype]
    if kind == "int":
        return isinstance(v, int) and not isinstance(v, bool)
    if kind == "str":
        return isinstance(v, str)
    return isinstance(v, bool)  # bool


def _compile_filter(node, key, fields, errors):
    """Restricted filter: None | bool literal | field = same-type lit | and/or."""
    if node is None:
        return ("const", True)
    if not isinstance(node, dict):
        errors.append(f"{key}: filter must be an expression object")
        return ("const", True)
    op = node.get("op")
    if op == "lit":
        v = node.get("value")
        if not isinstance(v, bool):
            errors.append(f"{key}: filter literal must be boolean, got {v!r}")
            return ("const", True)
        return ("const", v)
    if op == "cmp":
        if node.get("cmp") != "=":
            errors.append(f"{key}: only '=' comparisons are supported in slice "
                          f"filters, got '{node.get('cmp')}'")
            return ("const", True)
        left, right = node.get("left"), node.get("right")
        if (isinstance(left, dict) and left.get("op") == "field"
                and isinstance(right, dict) and right.get("op") == "lit"):
            fname, lit = left.get("name"), right.get("value")
        elif (isinstance(right, dict) and right.get("op") == "field"
                and isinstance(left, dict) and left.get("op") == "lit"):
            fname, lit = right.get("name"), left.get("value")
        else:
            errors.append(f"{key}: filter comparison must be field = literal")
            return ("const", True)
        ftype = fields.get(fname)
        if ftype is None:
            errors.append(f"{key}: filter field '{fname}' is not in the catalog")
            return ("const", True)
        if not _lit_ok(ftype, lit):
            errors.append(f"{key}: literal {lit!r} does not match the type of "
                          f"field '{fname}' ({ftype})")
        return ("eq", fname, lit)
    if op in ("and", "or"):
        args = node.get("args")
        if not isinstance(args, list) or not args:
            errors.append(f"{key}: '{op}' requires a non-empty 'args' list")
            return ("const", True)
        return (op, [_compile_filter(a, key, fields, errors) for a in args])
    errors.append(f"{key}: unsupported filter op '{op}' for slices "
                  "(allowed: boolean literal, field = literal, and, or)")
    return ("const", True)


def _compile_agg(node, key, fields, metric_keys, leaves, refs, errors):
    """Restricted agg: sum(field) | count_distinct(field) | metric@version |
    add | binary sub.  Leaf aggregates get stable ids L0, L1, ... per metric."""
    dead = ("const0",)          # placeholder; discarded because errors abort
    if not isinstance(node, dict):
        errors.append(f"{key}: agg must be an expression object")
        return dead
    op = node.get("op")
    if op in ("sum", "count_distinct"):
        arg = node.get("arg")
        if not (isinstance(arg, dict) and arg.get("op") == "field"):
            errors.append(f"{key}: {op} argument must be a plain field reference")
            return dead
        fname = arg.get("name")
        ftype = fields.get(fname)
        if ftype is None:
            errors.append(f"{key}: {op} field '{fname}' is not in the catalog")
        elif op == "sum" and ftype != "number":
            errors.append(f"{key}: sum field '{fname}' must be numeric, "
                          f"got {ftype}")
        elif op == "count_distinct" and ftype not in STRING_LIKE:
            errors.append(f"{key}: count_distinct field '{fname}' must hold "
                          f"non-empty strings, got {ftype}")
        lid = f"L{len(leaves)}"
        leaves[lid] = ("sum" if op == "sum" else "cd", fname)
        return ("leaf", lid)
    if op == "metric":
        dep = _key(node.get("name"), node.get("version"))
        if dep not in metric_keys:
            errors.append(f"{key}: metric '{dep}' cannot be resolved")
            return dead
        refs.append(dep)
        return ("ref", dep)
    if op == "add":
        args = node.get("args")
        if not isinstance(args, list) or len(args) < 2:
            errors.append(f"{key}: 'add' requires at least 2 args")
            return dead
        return ("add", [_compile_agg(a, key, fields, metric_keys, leaves, refs,
                                     errors) for a in args])
    if op == "sub":
        args = node.get("args")
        if not isinstance(args, list) or len(args) != 2:
            errors.append(f"{key}: 'sub' requires exactly 2 args (binary)")
            return dead
        return ("sub",
                _compile_agg(args[0], key, fields, metric_keys, leaves, refs,
                             errors),
                _compile_agg(args[1], key, fields, metric_keys, leaves, refs,
                             errors))
    errors.append(f"{key}: unsupported agg op '{op}' for slices (allowed: "
                  "sum(field), count_distinct(field), metric@version, add, sub)")
    return dead


def compile_closure(catalog, root_name, root_version):
    """Validate + compile the root metric's reference closure.

    Only the closure is inspected: cycles, unprojectable grains and
    unsupported expressions inside it reject the slice; unrelated metrics in
    the catalog never do.  Raises SliceError.
    """
    fields = {f["name"]: f["type"] for f in catalog["fields"]}
    dims = {d["name"]: d["field"] for d in catalog["dimensions"]}
    metrics = {_key(m["name"], m["version"]): m for m in catalog["metrics"]}
    root_key = _key(root_name, root_version)
    if root_key not in metrics:
        raise SliceError([f"root metric '{root_key}' cannot be resolved in "
                          f"catalog '{catalog.get('catalog_id')}'"])

    errors = []
    order, seen = [], set()

    def visit(key):                     # closure = metrics reachable from root
        if key in seen:
            return
        seen.add(key)
        order.append(key)
        for n, v in sorted(astx.refs(metrics[key]["agg"])["metrics"]):
            dep = _key(n, v)
            if dep not in metrics:
                errors.append(f"{key}: metric '{dep}' cannot be resolved")
                continue
            visit(dep)

    visit(root_key)
    if errors:
        raise SliceError(errors)

    compiled = {}
    for key in order:
        m = metrics[key]
        grain = list(m.get("grain") or [])
        grain_fields = []
        for g in grain:
            f = dims.get(g)
            if f is None:
                errors.append(f"{key}: grain dimension '{g}' is not declared")
                continue
            if fields.get(f) not in STRING_LIKE:
                errors.append(f"{key}: dimension '{g}' maps to field '{f}' of "
                              f"type {fields.get(f)}; dimension values must be "
                              f"non-empty strings")
            grain_fields.append(f)
        filt = _compile_filter(m.get("filter"), key, fields, errors)
        leaves, refs = {}, []
        expr = _compile_agg(m.get("agg"), key, fields, set(metrics), leaves,
                            refs, errors)
        refs = list(dict.fromkeys(refs))
        compiled[key] = _Metric(key, grain, grain_fields, filt, expr, leaves,
                                refs, {})

    graph = {k: list(m.refs) for k, m in compiled.items()}
    cyc = core.find_cycle(graph)
    if cyc:
        errors.append("dependency cycle inside the slice closure: "
                      + " -> ".join(cyc))
    for key, m in compiled.items():
        for dep in m.refs:
            if not set(compiled[dep].grain) <= set(m.grain):
                errors.append(f"{key}: dependency {dep} has grain "
                              f"{compiled[dep].grain} which is not a subset of "
                              f"the parent grain {m.grain}; cannot project")
            else:
                m.proj[dep] = [m.grain.index(g) for g in compiled[dep].grain]
    if errors:
        raise SliceError(errors)

    def rank(key):                      # closure is acyclic here
        m = compiled[key]
        if key not in ranks:
            ranks[key] = 0 if not m.refs else 1 + max(rank(d) for d in m.refs)
        return ranks[key]

    ranks = {}
    for key in compiled:
        compiled[key].rank = rank(key)
    return Closure(root_key, compiled, fields)


# ------------------------------------------------------------------ evaluation

def eval_filter(f, row):
    kind = f[0]
    if kind == "const":
        return f[1]
    if kind == "eq":
        return row[f[1]] == f[2]
    if kind == "and":
        return all(eval_filter(a, row) for a in f[1])
    return any(eval_filter(a, row) for a in f[1])      # "or"


def eval_expr(e, leaf_vals, ref_vals):
    """Value of a compiled agg; None propagates out of any arithmetic."""
    kind = e[0]
    if kind == "leaf":
        return leaf_vals[e[1]]
    if kind == "ref":
        return ref_vals[e[1]]                          # int or None
    if kind == "const0":
        return 0                                       # unreachable in practice
    if kind == "add":
        vals = [eval_expr(a, leaf_vals, ref_vals) for a in e[1]]
        return None if any(v is None for v in vals) else sum(vals)
    a = eval_expr(e[1], leaf_vals, ref_vals)           # "sub"
    b = eval_expr(e[2], leaf_vals, ref_vals)
    return None if a is None or b is None else a - b


# -------------------------------------------------------------- batch parsing

def _valid_id(fid):
    return (isinstance(fid, str) and fid != "") or \
           (isinstance(fid, int) and not isinstance(fid, bool))


def _value_ok(ftype, v):
    kind = FACT_KIND[ftype]
    if kind == "int":
        return (isinstance(v, int) and not isinstance(v, bool)
                and INT32_MIN <= v <= INT32_MAX)
    if kind == "str":
        return isinstance(v, str)
    return isinstance(v, bool)  # bool


def parse_batch(text, spec):
    """Parse and validate one JSONL correction batch (pure, no I/O).

    spec: {"fields": {field: catalog type}, "nonempty": {field, ...}}
    Returns (corrections, errors); any error rejects the whole batch.
    A correction is {"op": "put"|"delete", "id": <canonical>, "row": {...}|None}.
    """
    corrections, errors, seen = [], [], set()
    for ln, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        path = f"line {ln}"
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"{path}: invalid JSON: {exc}")
            continue
        if not isinstance(obj, dict):
            errors.append(f"{path}: correction must be a JSON object")
            continue
        op = obj.get("op")
        if op not in ("put", "delete"):
            errors.append(f"{path}: 'op' must be 'put' or 'delete'")
            continue
        fid = obj.get("id")
        if not _valid_id(fid):
            errors.append(f"{path}: 'id' must be a non-empty string or an "
                          f"integer, got {fid!r}")
            continue
        cid = astx.canon(fid)
        if cid in seen:
            errors.append(f"{path}: duplicate id {fid!r} in this batch")
            continue
        seen.add(cid)
        if op == "delete":
            corrections.append({"op": "delete", "id": cid, "row": None})
            continue
        row = obj.get("row")
        if not isinstance(row, dict):
            errors.append(f"{path}: 'put' requires a 'row' object")
            continue
        ok = True
        for fname, ftype in spec["fields"].items():
            if fname not in row:
                errors.append(f"{path}: row is missing referenced field "
                              f"'{fname}'")
                ok = False
                continue
            v = row[fname]
            if not _value_ok(ftype, v):
                errors.append(f"{path}: field '{fname}' expects "
                              f"{FACT_KIND[ftype]} (catalog type {ftype}), "
                              f"got {v!r}")
                ok = False
                continue
            if fname in spec["nonempty"] and v == "":
                errors.append(f"{path}: field '{fname}' must be a non-empty "
                              f"string")
                ok = False
        if ok:
            corrections.append({"op": "put", "id": cid, "row": row})
    return corrections, errors


# ------------------------------------------------------------ slice lifecycle

def init_slice(db, slice_name, catalog_id, seq, root_name, root_version):
    """First refresh: pin (catalog_id, seq) + root metric, start empty."""
    cat = store.get_catalog(db, catalog_id, seq)
    if cat is None:
        raise SliceError([f"catalog '{catalog_id}' seq={seq} not found"])
    closure = compile_closure(cat["content"], root_name, root_version)
    if store.get_slice(db, slice_name) is not None:
        raise SliceError([f"slice '{slice_name}' already exists"])
    store.create_slice(db, slice_name, catalog_id, seq, root_name, root_version)
    return closure


def apply_batch(db, slice_name, corrections):
    """Apply one validated correction batch incrementally and atomically.

    Only corrected rows are looked up, only groups they touch are recomputed,
    and changes propagate upstream through the reverse reference index.
    Everything commits in one transaction.  Returns refresh statistics.
    """
    meta = store.get_slice(db, slice_name)
    if meta is None:
        raise SliceError([f"slice '{slice_name}' not found"])
    cat = store.get_catalog(db, meta["catalog_id"], meta["catalog_seq"])
    closure = compile_closure(cat["content"], meta["root_name"],
                              meta["root_version"])
    metrics = closure.metrics

    # ---- read-only phase: old rows + delete existence (no writes yet) ------
    ids = [c["id"] for c in corrections]
    old_rows = {}
    if ids:
        marks = ",".join("?" * len(ids))
        for r in db.execute(
                "SELECT fact_id, row_json FROM slice_facts"
                f" WHERE slice_name = ? AND fact_id IN ({marks})",
                [slice_name, *ids]):
            old_rows[r["fact_id"]] = json.loads(r["row_json"])
    missing = [c["id"] for c in corrections
               if c["op"] == "delete" and c["id"] not in old_rows]
    if missing:
        raise SliceError([f"delete references non-existent id {m}; the whole "
                          f"batch is rejected" for m in missing])

    dirty, changed = set(), set()       # (metric key, gkey) sets

    def cd_add(m, gk, lid, val, delta):
        cur = db.execute(
            "SELECT cnt FROM slice_cds WHERE slice_name=? AND metric=?"
            " AND gkey=? AND leaf=? AND val=?",
            (slice_name, m.key, gk, lid, val)).fetchone()
        if delta > 0:
            if cur is None:
                db.execute("INSERT INTO slice_cds VALUES (?,?,?,?,?,1)",
                           (slice_name, m.key, gk, lid, val))
            else:
                db.execute("UPDATE slice_cds SET cnt = cnt + 1 WHERE"
                           " slice_name=? AND metric=? AND gkey=? AND leaf=?"
                           " AND val=?", (slice_name, m.key, gk, lid, val))
        elif cur["cnt"] == 1:
            db.execute("DELETE FROM slice_cds WHERE slice_name=? AND metric=?"
                       " AND gkey=? AND leaf=? AND val=?",
                       (slice_name, m.key, gk, lid, val))
        else:
            db.execute("UPDATE slice_cds SET cnt = cnt - 1 WHERE slice_name=?"
                       " AND metric=? AND gkey=? AND leaf=? AND val=?",
                       (slice_name, m.key, gk, lid, val))

    def adjust_leaves(m, gk, old, new):
        """Fold one row's delta into a group's direct aggregates."""
        touched = False
        for lid, (kind, field) in m.leaves.items():
            if kind == "sum":
                delta = ((new[field] if new is not None else 0)
                         - (old[field] if old is not None else 0))
                if delta:
                    cur = db.execute(
                        "SELECT total FROM slice_sums WHERE slice_name=?"
                        " AND metric=? AND gkey=? AND leaf=?",
                        (slice_name, m.key, gk, lid)).fetchone()
                    db.execute(
                        "UPDATE slice_sums SET total=? WHERE slice_name=?"
                        " AND metric=? AND gkey=? AND leaf=?",
                        (str(int(cur["total"]) + delta),
                         slice_name, m.key, gk, lid))
                    touched = True
            else:
                ov = old[field] if old is not None else None
                nv = new[field] if new is not None else None
                if ov == nv:
                    continue
                if ov is not None:
                    cd_add(m, gk, lid, ov, -1)
                if nv is not None:
                    cd_add(m, gk, lid, nv, +1)
                touched = True
        return touched

    def add_row(m, klist, row):
        gk = astx.canon(klist)
        cur = db.execute(
            "SELECT row_count FROM slice_groups WHERE slice_name=? AND"
            " metric=? AND gkey=?", (slice_name, m.key, gk)).fetchone()
        if cur is not None:
            db.execute("UPDATE slice_groups SET row_count = row_count + 1"
                       " WHERE slice_name=? AND metric=? AND gkey=?",
                       (slice_name, m.key, gk))
            if adjust_leaves(m, gk, None, row):
                dirty.add((m.key, gk))
            return
        db.execute("INSERT INTO slice_groups VALUES (?,?,?,1,NULL)",
                   (slice_name, m.key, gk))
        for lid, (kind, field) in m.leaves.items():
            if kind == "sum":
                db.execute("INSERT INTO slice_sums VALUES (?,?,?,?,?)",
                           (slice_name, m.key, gk, lid, str(row[field])))
            else:
                db.execute("INSERT INTO slice_cds VALUES (?,?,?,?,?,1)",
                           (slice_name, m.key, gk, lid, row[field]))
        for dep, idx in m.proj.items():
            db.execute("INSERT OR IGNORE INTO slice_refs VALUES (?,?,?,?,?)",
                       (slice_name, dep, astx.canon([klist[i] for i in idx]),
                        m.key, gk))
        changed.add((m.key, gk))        # group appeared
        dirty.add((m.key, gk))

    def remove_row(m, klist, row):
        gk = astx.canon(klist)
        cur = db.execute(
            "SELECT row_count FROM slice_groups WHERE slice_name=? AND"
            " metric=? AND gkey=?", (slice_name, m.key, gk)).fetchone()
        if cur["row_count"] > 1:
            db.execute("UPDATE slice_groups SET row_count = row_count - 1"
                       " WHERE slice_name=? AND metric=? AND gkey=?",
                       (slice_name, m.key, gk))
            if adjust_leaves(m, gk, row, None):
                dirty.add((m.key, gk))
            return
        db.execute("DELETE FROM slice_groups WHERE slice_name=? AND metric=?"
                   " AND gkey=?", (slice_name, m.key, gk))
        db.execute("DELETE FROM slice_sums WHERE slice_name=? AND metric=?"
                   " AND gkey=?", (slice_name, m.key, gk))
        db.execute("DELETE FROM slice_cds WHERE slice_name=? AND metric=?"
                   " AND gkey=?", (slice_name, m.key, gk))
        db.execute("DELETE FROM slice_refs WHERE slice_name=? AND"
                   " parent_metric=? AND parent_gkey=?",
                   (slice_name, m.key, gk))
        changed.add((m.key, gk))        # group disappeared

    def compute_value(m, gk, klist):
        leaf_vals = {}
        for lid, (kind, _field) in m.leaves.items():
            if kind == "sum":
                r = db.execute(
                    "SELECT total FROM slice_sums WHERE slice_name=? AND"
                    " metric=? AND gkey=? AND leaf=?",
                    (slice_name, m.key, gk, lid)).fetchone()
                leaf_vals[lid] = int(r["total"])
            else:
                leaf_vals[lid] = db.execute(
                    "SELECT COUNT(*) AS n FROM slice_cds WHERE slice_name=?"
                    " AND metric=? AND gkey=? AND leaf=?",
                    (slice_name, m.key, gk, lid)).fetchone()["n"]
        ref_vals = {}
        for dep, idx in m.proj.items():
            r = db.execute(
                "SELECT value FROM slice_groups WHERE slice_name=? AND"
                " metric=? AND gkey=?",
                (slice_name, dep, astx.canon([klist[i] for i in idx]))
            ).fetchone()
            ref_vals[dep] = (None if r is None or r["value"] is None
                             else int(r["value"]))
        return eval_expr(m.expr, leaf_vals, ref_vals)

    recomputed = []
    try:
        # ---- phase 1: fold corrected rows into their direct groups ---------
        delta_facts = 0
        for c in corrections:
            old = old_rows.get(c["id"])
            new = c["row"]
            for m in metrics.values():
                old_q = old is not None and eval_filter(m.filter, old)
                new_q = new is not None and eval_filter(m.filter, new)
                if not old_q and not new_q:
                    continue
                old_k = [old[f] for f in m.grain_fields] if old_q else None
                new_k = [new[f] for f in m.grain_fields] if new_q else None
                if old_q and new_q and old_k == new_k:
                    gk = astx.canon(old_k)
                    if adjust_leaves(m, gk, old, new):
                        dirty.add((m.key, gk))
                else:
                    if old_q:
                        remove_row(m, old_k, old)
                    if new_q:
                        add_row(m, new_k, new)
            if new is None:
                delta_facts -= 1
                db.execute("DELETE FROM slice_facts WHERE slice_name=? AND"
                           " fact_id=?", (slice_name, c["id"]))
            else:
                if c["id"] not in old_rows:
                    delta_facts += 1
                db.execute("INSERT OR REPLACE INTO slice_facts VALUES (?,?,?)",
                           (slice_name, c["id"],
                            json.dumps(new, ensure_ascii=False)))
        db.execute("UPDATE slices SET facts_count = facts_count + ?"
                   " WHERE slice_name=?", (delta_facts, slice_name))

        # ---- phase 2: recompute touched groups, propagate upstream ---------
        heap = []

        def enqueue(mkey, gk):
            heapq.heappush(heap, (metrics[mkey].rank, mkey, gk))

        def notify(mkey, gk):
            for r in db.execute(
                    "SELECT parent_metric, parent_gkey FROM slice_refs"
                    " WHERE slice_name=? AND dep_metric=? AND dep_gkey=?",
                    (slice_name, mkey, gk)):
                enqueue(r["parent_metric"], r["parent_gkey"])

        for mkey, gk in dirty:
            enqueue(mkey, gk)
        for mkey, gk in changed:        # appeared / disappeared groups
            notify(mkey, gk)
        processed = set()
        while heap:
            # topological ranks make every group settle in one pass
            _, mkey, gk = heapq.heappop(heap)
            if (mkey, gk) in processed:
                continue
            processed.add((mkey, gk))
            row = db.execute(
                "SELECT value FROM slice_groups WHERE slice_name=? AND"
                " metric=? AND gkey=?", (slice_name, mkey, gk)).fetchone()
            if row is None:
                continue                # group went away within this batch
            val = compute_value(metrics[mkey], gk, json.loads(gk))
            recomputed.append([mkey, gk])
            old_val = None if row["value"] is None else int(row["value"])
            if val != old_val:
                db.execute("UPDATE slice_groups SET value=? WHERE slice_name=?"
                           " AND metric=? AND gkey=?",
                           (None if val is None else str(val),
                            slice_name, mkey, gk))
                notify(mkey, gk)

        db.commit()
    except Exception:
        db.rollback()
        raise

    facts = db.execute("SELECT facts_count FROM slices WHERE slice_name=?",
                       (slice_name,)).fetchone()["facts_count"]
    return {"rows_examined": len(corrections),
            "groups_recomputed": len(recomputed),
            "recomputed": sorted(recomputed),
            "facts": facts}


def read_slice(db, slice_name):
    """Root metric groups of a slice: keys in catalog grain order + values."""
    meta = store.get_slice(db, slice_name)
    if meta is None:
        raise SliceError([f"slice '{slice_name}' not found"])
    cat = store.get_catalog(db, meta["catalog_id"], meta["catalog_seq"])
    closure = compile_closure(cat["content"], meta["root_name"],
                              meta["root_version"])
    root = closure.metrics[closure.root_key]
    groups = []
    for r in db.execute(
            "SELECT gkey, value FROM slice_groups WHERE slice_name=? AND"
            " metric=? ORDER BY gkey",
            (slice_name, closure.root_key)):
        klist = json.loads(r["gkey"])
        groups.append({
            "key": {dim: val for dim, val in zip(root.grain, klist)},
            "value": None if r["value"] is None else int(r["value"]),
        })
    return {"slice": slice_name,
            "catalog": {"catalog_id": meta["catalog_id"],
                        "seq": meta["catalog_seq"]},
            "root": closure.root_key,
            "grain": list(root.grain),
            "facts": meta["facts_count"],
            "groups": groups}
