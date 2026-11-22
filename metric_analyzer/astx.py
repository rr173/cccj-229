"""Restricted declarative AST for metric definitions and report queries.

The analyzer never executes these expressions against production data and
never compares them as raw strings: every judgement (equivalence, filter
narrowing/widening, contract impact) is made on the parsed, normalized AST.

Node grammar (JSON objects carrying an "op" key)::

    lit      {"op":"lit","value":<scalar>}
    field    {"op":"field","name":"orders.amount"}
    metric   {"op":"metric","name":"revenue","version":"v1"}   # version pinned
    add/sub/mul/div {"op":"add","args":[e, ...]}
    neg      {"op":"neg","arg":e}
    sum/avg/min/max/count_distinct {"op":"sum","arg":e}
    count    {"op":"count"}
    and/or   {"op":"and","args":[e, ...]}
    not      {"op":"not","arg":e}
    cmp      {"op":"cmp","cmp":"=|!=|<|<=|>|>=","left":e,"right":e}
    in       {"op":"in","left":e,"values":[scalar, ...]}
    case     {"op":"case","when":[{"cond":e,"then":e}, ...],"else":e}
    coalesce {"op":"coalesce","args":[e, ...]}
"""
from __future__ import annotations

import json

ARITH = {"add", "sub", "mul", "div"}
AGGS = {"sum", "avg", "min", "max", "count", "count_distinct"}
LOGIC = {"and", "or"}
CMPS = {"=", "!=", "<", "<=", ">", ">="}
ALL_OPS = ARITH | AGGS | LOGIC | {
    "lit", "field", "metric", "neg", "not", "cmp", "in", "case", "coalesce",
}

FIELD_TYPES = {"number": "num", "string": "str", "date": "date", "boolean": "bool"}

#: a missing filter is semantically "true"
TRUE = {"op": "lit", "value": True}


def canon(node) -> str:
    """Canonical string form of any JSON value (sorted keys, tight separators)."""
    return json.dumps(node, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class AstError(Exception):
    def __init__(self, errors):
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


# ------------------------------------------------------------------ validation

def _lit_kind(v):
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return "num"
    if isinstance(v, str):
        return "str"
    return "any"


def _compat(a, b):
    if a == "any" or b == "any":
        return True
    if a == b:
        return True
    return {a, b} == {"str", "date"}


def validate(node, ctx, path="$", in_agg=False, errors=None):
    """Validate an expression AST, return inferred kind (num/str/bool/date/any).

    ctx keys:
      fields:       {name: kind} or None (None = accept any field name)
      metrics:      {(name, version)} or None (None = accept any metric ref)
      allow_agg:    aggregation operators permitted in this context
      allow_metric: metric references permitted in this context
    Problems are appended to ``errors`` (human readable, with JSON path).
    """
    if errors is None:
        errors = []
    if not isinstance(node, dict) or "op" not in node:
        errors.append(f"{path}: node must be an object carrying an 'op' key")
        return "any"
    op = node["op"]
    if op not in ALL_OPS:
        errors.append(f"{path}: unknown op '{op}'")
        return "any"

    def err(msg):
        errors.append(f"{path}: {msg}")

    if op == "lit":
        if "value" not in node or isinstance(node["value"], (dict, list)):
            err("lit requires a scalar 'value'")
            return "any"
        return _lit_kind(node["value"])

    if op == "field":
        name = node.get("name")
        if not isinstance(name, str) or not name:
            err("field requires a non-empty 'name'")
            return "any"
        fields = ctx.get("fields")
        if fields is not None:
            if name not in fields:
                err(f"field '{name}' is not present in the field lineage")
                return "any"
            return fields[name]
        return "any"

    if op == "metric":
        if in_agg:
            err("metric references are not allowed inside an aggregation argument")
        if not ctx.get("allow_metric"):
            err("metric references are not allowed in this context")
        name, ver = node.get("name"), node.get("version")
        if not isinstance(name, str) or not name:
            err("metric requires a non-empty 'name'")
        if not isinstance(ver, str) or not ver:
            err("metric requires an explicitly pinned 'version' "
                "(same-name versions never implicitly replace each other)")
        metrics = ctx.get("metrics")
        if metrics is not None and isinstance(name, str) and isinstance(ver, str):
            if (name, ver) not in metrics:
                err(f"metric '{name}@{ver}' cannot be resolved in the catalog")
        return "num"

    if op in ARITH:
        args = node.get("args")
        if not isinstance(args, list) or len(args) < 2:
            err(f"'{op}' requires an 'args' list with at least 2 items")
            return "num"
        if op in ("sub", "div") and len(args) != 2:
            err(f"'{op}' requires exactly 2 args")
        for i, a in enumerate(args):
            k = validate(a, ctx, f"{path}.args[{i}]", in_agg, errors)
            if k not in ("num", "any"):
                err(f"'{op}' operand #{i} must be numeric, got {k}")
        return "num"

    if op == "neg":
        k = validate(node.get("arg"), ctx, f"{path}.arg", in_agg, errors)
        if k not in ("num", "any"):
            err("neg argument must be numeric")
        return "num"

    if op in AGGS:
        if in_agg:
            err(f"aggregation '{op}' cannot be nested inside another aggregation")
        if not ctx.get("allow_agg"):
            err(f"aggregation '{op}' is not allowed in this context")
        if op == "count":
            if set(node) - {"op"}:
                err("count takes no argument")
            return "num"
        k = validate(node.get("arg"), ctx, f"{path}.arg", in_agg=True, errors=errors)
        if op in ("sum", "avg") and k not in ("num", "any"):
            err(f"{op} argument must be numeric, got {k}")
        return "num" if op in ("sum", "avg", "count_distinct") else k

    if op in LOGIC:
        args = node.get("args")
        if not isinstance(args, list) or not args:
            err(f"'{op}' requires a non-empty 'args' list")
            return "bool"
        for i, a in enumerate(args):
            k = validate(a, ctx, f"{path}.args[{i}]", in_agg, errors)
            if k not in ("bool", "any"):
                err(f"'{op}' operand #{i} must be boolean, got {k}")
        return "bool"

    if op == "not":
        k = validate(node.get("arg"), ctx, f"{path}.arg", in_agg, errors)
        if k not in ("bool", "any"):
            err("not argument must be boolean")
        return "bool"

    if op == "cmp":
        c = node.get("cmp")
        if c not in CMPS:
            err(f"cmp requires 'cmp' in {sorted(CMPS)}")
        kl = validate(node.get("left"), ctx, f"{path}.left", in_agg, errors)
        kr = validate(node.get("right"), ctx, f"{path}.right", in_agg, errors)
        if not _compat(kl, kr):
            err(f"comparison of incompatible kinds {kl} vs {kr}")
        return "bool"

    if op == "in":
        kl = validate(node.get("left"), ctx, f"{path}.left", in_agg, errors)
        values = node.get("values")
        if not isinstance(values, list) or not values:
            err("in requires a non-empty 'values' list")
        else:
            for i, v in enumerate(values):
                if isinstance(v, (dict, list)):
                    err(f"in.values[{i}] must be a scalar")
                elif not _compat(kl, _lit_kind(v)):
                    err(f"in.values[{i}] kind {_lit_kind(v)} incompatible with {kl}")
        return "bool"

    if op == "case":
        whens = node.get("when")
        if not isinstance(whens, list) or not whens:
            err("case requires a non-empty 'when' list")
            whens = []
        kinds = []
        for i, w in enumerate(whens):
            if not isinstance(w, dict) or "cond" not in w or "then" not in w:
                err(f"case.when[{i}] requires 'cond' and 'then'")
                continue
            kc = validate(w["cond"], ctx, f"{path}.when[{i}].cond", in_agg, errors)
            if kc not in ("bool", "any"):
                err(f"case.when[{i}].cond must be boolean")
            kinds.append(validate(w["then"], ctx, f"{path}.when[{i}].then", in_agg, errors))
        if "else" not in node:
            err("case requires an 'else' branch")
            kinds.append("any")
        else:
            kinds.append(validate(node["else"], ctx, f"{path}.else", in_agg, errors))
        first = kinds[0] if kinds else "any"
        for k in kinds[1:]:
            if not _compat(first, k):
                err("case branches have incompatible kinds")
        return first

    if op == "coalesce":
        args = node.get("args")
        if not isinstance(args, list) or not args:
            err("coalesce requires a non-empty 'args' list")
            return "any"
        kinds = [validate(a, ctx, f"{path}.args[{i}]", in_agg, errors)
                 for i, a in enumerate(args)]
        first = kinds[0]
        for k in kinds[1:]:
            if not _compat(first, k):
                err("coalesce arguments have incompatible kinds")
        return first

    return "any"  # pragma: no cover


# --------------------------------------------------------------- normalization

def normalize(node):
    """Commutative/associative normalization so semantic equality is structural."""
    if not isinstance(node, dict):
        return node
    op = node.get("op")
    n = dict(node)
    if op in ("add", "mul", "and", "or"):
        args = []
        for a in node.get("args", []):
            na = normalize(a)
            if isinstance(na, dict) and na.get("op") == op:
                args.extend(na["args"])          # associativity: flatten
            else:
                args.append(na)
        args.sort(key=canon)                     # commutativity: order-insensitive
        if op in ("and", "or"):                  # idempotent: dedupe
            dedup = []
            for a in args:
                if not dedup or canon(dedup[-1]) != canon(a):
                    dedup.append(a)
            args = dedup
        n["args"] = args
    elif op in ("sub", "div", "coalesce"):
        n["args"] = [normalize(a) for a in node.get("args", [])]
    elif op in ("neg", "not", "sum", "avg", "min", "max", "count_distinct"):
        n["arg"] = normalize(node.get("arg"))
    elif op == "cmp":
        left, right = normalize(node.get("left")), normalize(node.get("right"))
        if node.get("cmp") in ("=", "!=") and canon(right) < canon(left):
            left, right = right, left
        n["left"], n["right"] = left, right
    elif op == "in":
        n["left"] = normalize(node.get("left"))
        n["values"] = sorted(node.get("values", []), key=canon)  # IN is a set
    elif op == "case":
        n["when"] = [{"cond": normalize(w["cond"]), "then": normalize(w["then"])}
                     for w in node.get("when", [])]
        n["else"] = normalize(node.get("else"))
    return n


def equiv(a, b):
    """Semantic equivalence of two expression ASTs."""
    return canon(normalize(a)) == canon(normalize(b))


# ---------------------------------------------------- filter implication logic

def conjuncts(node):
    """Split a (normalized) boolean filter into top-level conjuncts."""
    n = normalize(node if node is not None else TRUE)
    if isinstance(n, dict) and n.get("op") == "and":
        return n["args"]
    return [n]


def _field_cmp(node):
    """Return (field, cmp, literal) if node is `field <cmp> literal`."""
    if (isinstance(node, dict) and node.get("op") == "cmp"
            and isinstance(node.get("left"), dict) and node["left"].get("op") == "field"
            and isinstance(node.get("right"), dict) and node["right"].get("op") == "lit"):
        return node["left"]["name"], node["cmp"], node["right"]["value"]
    return None


def _field_in(node):
    """Return (field, values) if node is `field IN (values)`."""
    if (isinstance(node, dict) and node.get("op") == "in"
            and isinstance(node.get("left"), dict) and node["left"].get("op") == "field"):
        return node["left"]["name"], node.get("values", [])
    return None


def pred_implies(p, q):
    """Conservative check: does predicate p logically imply predicate q?"""
    if canon(p) == canon(q):
        return True
    if isinstance(q, dict) and q.get("op") == "lit" and q.get("value") is True:
        return True
    pc, qc = _field_cmp(p), _field_cmp(q)
    pi, qi = _field_in(p), _field_in(q)
    if pc and qi and pc[0] == qi[0] and pc[1] == "=":
        return pc[2] in qi[1]                      # f = v  =>  f IN (..v..)
    if pi and qi and pi[0] == qi[0]:
        return set(map(canon, pi[1])) <= set(map(canon, qi[1]))  # subset
    if pc and qc and pc[0] == qc[0]:
        f, opc, a = pc
        _, oqc, b = qc
        try:
            if opc == "=":                         # f = v implies range holding v
                return {">=": a >= b, ">": a > b, "<=": a <= b, "<": a < b}.get(oqc, False)
            if opc == ">=":
                if oqc == ">=":
                    return a >= b
                if oqc == ">":
                    return a > b
            if opc == ">":
                if oqc in (">=", ">"):
                    return a >= b
            if opc == "<=":
                if oqc == "<=":
                    return a <= b
                if oqc == "<":
                    return a < b
            if opc == "<":
                if oqc in ("<=", "<"):
                    return a <= b
        except TypeError:
            return False
    return False


def filter_implies(a, b):
    """Does filter a imply filter b? (each conjunct of b covered by a)"""
    ps = conjuncts(a)
    return all(any(pred_implies(p, q) for p in ps) for q in conjuncts(b))


def filter_relation(old, new):
    """Classify how the new filter relates to the old one."""
    old = old if old is not None else TRUE
    new = new if new is not None else TRUE
    if equiv(old, new):
        return "EQUIV"
    o2n = filter_implies(old, new)
    n2o = filter_implies(new, old)
    if o2n and n2o:
        return "EQUIV"
    if n2o:
        return "NARROWED"      # new filter is stricter (smaller population)
    if o2n:
        return "WIDENED"       # new filter is looser (larger population)
    return "RESHAPED"          # neither implies the other


# ------------------------------------------------------------------- references

def _iter_children(node):
    op = node.get("op")
    if op in ("add", "sub", "mul", "div", "and", "or", "coalesce"):
        yield from node.get("args", [])
    elif op in ("neg", "not", "sum", "avg", "min", "max", "count_distinct"):
        yield node.get("arg")
    elif op == "cmp":
        yield node.get("left")
        yield node.get("right")
    elif op == "in":
        yield node.get("left")
    elif op == "case":
        for w in node.get("when", []):
            yield w.get("cond")
            yield w.get("then")
        yield node.get("else")


def refs(node):
    """Collect {'fields': set, 'metrics': {(name, version)}} referenced by an AST."""
    out = {"fields": set(), "metrics": set()}

    def walk(n):
        if not isinstance(n, dict):
            return
        op = n.get("op")
        if op == "field":
            out["fields"].add(n.get("name"))
        elif op == "metric":
            out["metrics"].add((n.get("name"), n.get("version")))
        for c in _iter_children(n):
            walk(c)

    walk(node)
    return out


def skeleton(node):
    """Computation shape with field/metric identity abstracted away.

    Used to detect a genuine aggregation-shape change independently of
    reference swaps (which are reported as DEPENDENCY_CHANGED / FIELD_*).
    """
    if not isinstance(node, dict):
        return node
    op = node.get("op")
    if op == "field":
        return {"op": "field"}
    if op == "metric":
        return {"op": "metric"}
    if op in ("add", "sub", "mul", "div", "and", "or", "coalesce"):
        return {"op": op, "args": [skeleton(a) for a in node.get("args", [])]}
    if op in ("neg", "not", "sum", "avg", "min", "max", "count_distinct"):
        return {"op": op, "arg": skeleton(node.get("arg"))}
    if op == "cmp":
        return {"op": "cmp", "cmp": node.get("cmp"),
                "left": skeleton(node.get("left")), "right": skeleton(node.get("right"))}
    if op == "in":
        return {"op": "in", "left": skeleton(node.get("left")),
                "values": sorted(node.get("values", []), key=canon)}
    if op == "case":
        return {"op": "case",
                "when": [{"cond": skeleton(w["cond"]), "then": skeleton(w["then"])}
                         for w in node.get("when", [])],
                "else": skeleton(node.get("else"))}
    return dict(node)
