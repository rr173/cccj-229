"""Import-time validation for catalogs (metrics + lineage) and registries
(reports + consumer compatibility declarations).

Every field/metric reference must resolve; a same-name new metric version
never implicitly replaces an old one — all versions coexist in a catalog and
references are always version-pinned.
"""
from __future__ import annotations

import datetime as _dt

from . import astx


def _is_date(s):
    try:
        _dt.date.fromisoformat(s)
        return True
    except (TypeError, ValueError):
        return False


def validate_catalog(cat):
    """Validate a catalog document. Returns list of errors (empty = valid)."""
    errors = []
    if not isinstance(cat.get("catalog_id"), str) or not cat["catalog_id"]:
        errors.append("catalog: 'catalog_id' must be a non-empty string")
    for key in ("fields", "dimensions", "metrics"):
        if not isinstance(cat.get(key), list):
            errors.append(f"catalog: '{key}' must be a list")
    if errors:
        return errors

    fields = {}
    for i, f in enumerate(cat["fields"]):
        path = f"fields[{i}]"
        if not isinstance(f.get("name"), str) or not f["name"]:
            errors.append(f"{path}: field requires a non-empty 'name'")
            continue
        if f["name"] in fields:
            errors.append(f"{path}: duplicate field '{f['name']}'")
        if f.get("type") not in astx.FIELD_TYPES:
            errors.append(f"{path}: type must be one of {sorted(astx.FIELD_TYPES)}")
            continue
        fields[f["name"]] = astx.FIELD_TYPES[f["type"]]

    dims = {}
    for i, d in enumerate(cat["dimensions"]):
        path = f"dimensions[{i}]"
        name = d.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{path}: dimension requires a non-empty 'name'")
            continue
        if name in dims:
            errors.append(f"{path}: duplicate dimension '{name}'")
        if d.get("field") not in fields:
            errors.append(f"{path}: dimension '{name}' maps to unknown field "
                          f"'{d.get('field')}'")
        dims[name] = d.get("field")

    seen = set()
    metric_keys = {(m.get("name"), m.get("version")) for m in cat["metrics"]
                   if isinstance(m, dict)}
    for i, m in enumerate(cat["metrics"]):
        path = f"metrics[{i}]({m.get('name')}@{m.get('version')})"
        name, ver = m.get("name"), m.get("version")
        if not isinstance(name, str) or not name:
            errors.append(f"{path}: metric requires a non-empty 'name'")
        if not isinstance(ver, str) or not ver:
            errors.append(f"{path}: metric requires a non-empty 'version'")
        if (name, ver) in seen:
            errors.append(f"{path}: duplicate metric version '{name}@{ver}'")
        seen.add((name, ver))
        if not _is_date(m.get("effective_from")):
            errors.append(f"{path}: 'effective_from' must be an ISO date")
        grain = m.get("grain")
        if not isinstance(grain, list) or not all(isinstance(g, str) for g in grain):
            errors.append(f"{path}: 'grain' must be a list of dimension names")
            grain = []
        for g in grain:
            if g not in dims:
                errors.append(f"{path}: grain dimension '{g}' is not declared")
        ctx_filter = {"fields": fields, "metrics": metric_keys,
                      "allow_agg": False, "allow_metric": False}
        ctx_agg = {"fields": fields, "metrics": metric_keys,
                   "allow_agg": True, "allow_metric": True}
        if m.get("filter") is not None:
            ferrs = []
            k = astx.validate(m["filter"], ctx_filter, path=f"{path}.filter",
                              errors=ferrs)
            errors.extend(ferrs)
            if k not in ("bool", "any") and not ferrs:
                errors.append(f"{path}.filter: filter must be boolean, got {k}")
        if "agg" not in m:
            errors.append(f"{path}: metric requires an 'agg' expression")
        else:
            aerrs = []
            astx.validate(m["agg"], ctx_agg, path=f"{path}.agg", errors=aerrs)
            errors.extend(aerrs)
            # direct self-reference is never meaningful
            if (name, ver) in astx.refs(m.get("agg"))["metrics"]:
                errors.append(f"{path}: metric references itself")
    return errors


def validate_registry(reg, known_change_types):
    """Validate a registry document (reports + consumer declarations)."""
    errors = []
    if not isinstance(reg.get("registry_id"), str) or not reg["registry_id"]:
        errors.append("registry: 'registry_id' must be a non-empty string")
    for key in ("reports", "consumers"):
        if not isinstance(reg.get(key), list):
            errors.append(f"registry: '{key}' must be a list")
    if errors:
        return errors

    report_ids = set()
    # permissive ctx: cross-catalog reference resolution happens at analysis time
    ctx = {"fields": None, "metrics": None, "allow_agg": False, "allow_metric": False}
    for i, r in enumerate(reg["reports"]):
        path = f"reports[{i}]({r.get('report_id')})"
        rid = r.get("report_id")
        if not isinstance(rid, str) or not rid:
            errors.append(f"{path}: report requires a non-empty 'report_id'")
        if rid in report_ids:
            errors.append(f"{path}: duplicate report_id '{rid}'")
        report_ids.add(rid)
        q = r.get("query")
        if not isinstance(q, dict):
            errors.append(f"{path}: report requires a 'query' object")
            continue
        sel = q.get("select")
        if not isinstance(sel, list) or not sel:
            errors.append(f"{path}.query: 'select' must be a non-empty list")
            sel = []
        for j, item in enumerate(sel):
            ipath = f"{path}.query.select[{j}]"
            if not isinstance(item.get("metric"), str) or not item["metric"]:
                errors.append(f"{ipath}: select item requires a metric name")
            if not isinstance(item.get("version"), str) or not item["version"]:
                errors.append(f"{ipath}: select item must pin an explicit 'version'")
        gb = q.get("group_by", [])
        if not isinstance(gb, list) or not all(isinstance(d, str) for d in gb):
            errors.append(f"{path}.query: 'group_by' must be a list of names")
        if q.get("where") is not None:
            werrs = []
            k = astx.validate(q["where"], ctx, path=f"{path}.query.where",
                              errors=werrs)
            errors.extend(werrs)
            if k not in ("bool", "any") and not werrs:
                errors.append(f"{path}.query.where: must be boolean, got {k}")

    for i, c in enumerate(reg["consumers"]):
        path = f"consumers[{i}]({c.get('consumer_id')})"
        if not isinstance(c.get("consumer_id"), str) or not c["consumer_id"]:
            errors.append(f"{path}: consumer requires a non-empty 'consumer_id'")
        if c.get("report_id") not in report_ids:
            errors.append(f"{path}: unknown report_id '{c.get('report_id')}'")
        ac = c.get("allowed_changes")
        if not isinstance(ac, list):
            errors.append(f"{path}: 'allowed_changes' must be a list")
        else:
            bad = sorted(set(ac) - set(known_change_types))
            if bad:
                errors.append(f"{path}: unknown change types in allowed_changes: {bad}")
        win = c.get("migration_window")
        if (not isinstance(win, dict) or not _is_date(win.get("start"))
                or not _is_date(win.get("end"))):
            errors.append(f"{path}: 'migration_window' requires ISO start/end dates")
        elif win["start"] > win["end"]:
            errors.append(f"{path}: migration_window start is after end")
        tv = c.get("target_versions")
        if not isinstance(tv, dict) or not all(
                isinstance(k, str) and isinstance(v, str) and v
                for k, v in (tv or {}).items()):
            errors.append(f"{path}: 'target_versions' must map metric names to "
                          f"explicit version strings")
    return errors
