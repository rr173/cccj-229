"""SQLite persistence: versioned catalog/registry history and migration plans.

Catalogs and registries are append-only per (id, seq); re-importing an id
creates a new sequence entry. A plan binds the exact content hashes of its
inputs, so any later re-import invalidates previously generated plans.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import sqlite3
import uuid

from . import astx

SCHEMA = """
CREATE TABLE IF NOT EXISTS catalogs (
  catalog_id   TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  content      TEXT NOT NULL,
  imported_at  TEXT NOT NULL,
  PRIMARY KEY (catalog_id, seq)
);
CREATE TABLE IF NOT EXISTS registries (
  registry_id  TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  content      TEXT NOT NULL,
  imported_at  TEXT NOT NULL,
  PRIMARY KEY (registry_id, seq)
);
CREATE TABLE IF NOT EXISTS plans (
  plan_id       TEXT PRIMARY KEY,
  created_at    TEXT NOT NULL,
  from_catalog  TEXT NOT NULL,
  to_catalog    TEXT NOT NULL,
  registry_id   TEXT NOT NULL,
  from_hash     TEXT NOT NULL,
  to_hash       TEXT NOT NULL,
  registry_hash TEXT NOT NULL,
  status        TEXT NOT NULL,           -- READY / BLOCKED / EXECUTED / INVALIDATED
  result        TEXT NOT NULL,           -- full analysis JSON
  bindings      TEXT NOT NULL            -- bound digests of reports/consumers
);
-- ---- offline metric slices (see slices.py) -------------------------------
CREATE TABLE IF NOT EXISTS slices (
  slice_name   TEXT PRIMARY KEY,
  catalog_id   TEXT NOT NULL,            -- pinned catalog import ...
  catalog_seq  INTEGER NOT NULL,         -- ... (catalog_id, catalog_seq) never follows re-imports
  root_name    TEXT NOT NULL,            -- root metric, explicitly versioned
  root_version TEXT NOT NULL,
  facts_count  INTEGER NOT NULL DEFAULT 0,
  created_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS slice_facts (
  slice_name TEXT NOT NULL,
  fact_id    TEXT NOT NULL,              -- canonical JSON of the correction id
  row_json   TEXT NOT NULL,              -- the flattened fact row as given
  PRIMARY KEY (slice_name, fact_id)
);
CREATE TABLE IF NOT EXISTS slice_groups (
  slice_name TEXT NOT NULL,
  metric     TEXT NOT NULL,              -- "name@version"
  gkey       TEXT NOT NULL,              -- JSON array of dim values, catalog grain order
  row_count  INTEGER NOT NULL,           -- qualifying facts behind this group (> 0)
  value      TEXT,                       -- decimal integer text; NULL = null value
  PRIMARY KEY (slice_name, metric, gkey)
);
CREATE TABLE IF NOT EXISTS slice_sums (   -- one row per sum(field) leaf per group
  slice_name TEXT NOT NULL,
  metric     TEXT NOT NULL,
  gkey       TEXT NOT NULL,
  leaf       TEXT NOT NULL,
  total      TEXT NOT NULL,              -- decimal integer text (exact, unbounded)
  PRIMARY KEY (slice_name, metric, gkey, leaf)
);
CREATE TABLE IF NOT EXISTS slice_cds (    -- per-value refcounts per count_distinct leaf
  slice_name TEXT NOT NULL,
  metric     TEXT NOT NULL,
  gkey       TEXT NOT NULL,
  leaf       TEXT NOT NULL,
  val        TEXT NOT NULL,
  cnt        INTEGER NOT NULL,
  PRIMARY KEY (slice_name, metric, gkey, leaf, val)
);
CREATE TABLE IF NOT EXISTS slice_refs (   -- reverse index: who references a dep group
  slice_name    TEXT NOT NULL,
  dep_metric    TEXT NOT NULL,
  dep_gkey      TEXT NOT NULL,
  parent_metric TEXT NOT NULL,
  parent_gkey   TEXT NOT NULL,
  PRIMARY KEY (slice_name, dep_metric, dep_gkey, parent_metric, parent_gkey)
);
"""


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    return db


def content_hash(obj):
    return hashlib.sha256(astx.canon(obj).encode("utf-8")).hexdigest()


def _now():
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _insert_versioned(db, table, id_col, obj_id, content):
    seq = db.execute(
        f"SELECT COALESCE(MAX(seq), 0) + 1 FROM {table} WHERE {id_col} = ?",
        (obj_id,)).fetchone()[0]
    h = content_hash(content)
    db.execute(
        f"INSERT INTO {table} ({id_col}, seq, content_hash, content, imported_at)"
        f" VALUES (?, ?, ?, ?, ?)",
        (obj_id, seq, h, json.dumps(content, ensure_ascii=False), _now()))
    db.commit()
    return seq, h


def insert_catalog(db, catalog):
    return _insert_versioned(db, "catalogs", "catalog_id",
                             catalog["catalog_id"], catalog)


def insert_registry(db, registry):
    return _insert_versioned(db, "registries", "registry_id",
                             registry["registry_id"], registry)


def _latest(db, table, id_col, obj_id):
    row = db.execute(
        f"SELECT * FROM {table} WHERE {id_col} = ? ORDER BY seq DESC LIMIT 1",
        (obj_id,)).fetchone()
    if row is None:
        return None
    return {"id": row[id_col], "seq": row["seq"], "hash": row["content_hash"],
            "content": json.loads(row["content"]),
            "imported_at": row["imported_at"]}


def latest_catalog(db, catalog_id):
    return _latest(db, "catalogs", "catalog_id", catalog_id)


def get_catalog(db, catalog_id, seq):
    """Fetch one immutable catalog import by (catalog_id, seq)."""
    row = db.execute(
        "SELECT * FROM catalogs WHERE catalog_id = ? AND seq = ?",
        (catalog_id, seq)).fetchone()
    if row is None:
        return None
    return {"id": row["catalog_id"], "seq": row["seq"], "hash": row["content_hash"],
            "content": json.loads(row["content"]),
            "imported_at": row["imported_at"]}
def latest_registry(db, registry_id):
    return _latest(db, "registries", "registry_id", registry_id)


def list_catalogs(db):
    return db.execute(
        "SELECT catalog_id, MAX(seq) AS seq, content_hash, imported_at"
        " FROM catalogs GROUP BY catalog_id ORDER BY catalog_id").fetchall()


def create_plan(db, from_cat, to_cat, reg, result, bindings):
    plan_id = "pln-" + uuid.uuid4().hex[:8]
    status = "BLOCKED" if result["blockers"] else "READY"
    db.execute(
        "INSERT INTO plans (plan_id, created_at, from_catalog, to_catalog,"
        " registry_id, from_hash, to_hash, registry_hash, status, result, bindings)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (plan_id, _now(), from_cat["id"], to_cat["id"], reg["id"],
         from_cat["hash"], to_cat["hash"], reg["hash"], status,
         json.dumps(result, ensure_ascii=False),
         json.dumps(bindings, ensure_ascii=False)))
    db.commit()
    return plan_id, status


def get_plan(db, plan_id):
    row = db.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,)).fetchone()
    if row is None:
        return None
    return {"plan_id": row["plan_id"], "created_at": row["created_at"],
            "from_catalog": row["from_catalog"], "to_catalog": row["to_catalog"],
            "registry_id": row["registry_id"],
            "from_hash": row["from_hash"], "to_hash": row["to_hash"],
            "registry_hash": row["registry_hash"], "status": row["status"],
            "result": json.loads(row["result"]),
            "bindings": json.loads(row["bindings"])}


def set_plan_status(db, plan_id, status):
    db.execute("UPDATE plans SET status = ? WHERE plan_id = ?", (status, plan_id))
    db.commit()


def list_plans(db):
    return db.execute(
        "SELECT plan_id, status, from_catalog, to_catalog, registry_id, created_at"
        " FROM plans ORDER BY created_at").fetchall()


# ------------------------------------------------------------- metric slices

def create_slice(db, slice_name, catalog_id, catalog_seq, root_name, root_version):
    db.execute(
        "INSERT INTO slices (slice_name, catalog_id, catalog_seq, root_name,"
        " root_version, facts_count, created_at) VALUES (?,?,?,?,?,0,?)",
        (slice_name, catalog_id, catalog_seq, root_name, root_version, _now()))
    db.commit()


def get_slice(db, slice_name):
    row = db.execute("SELECT * FROM slices WHERE slice_name = ?",
                     (slice_name,)).fetchone()
    if row is None:
        return None
    return {"slice_name": row["slice_name"], "catalog_id": row["catalog_id"],
            "catalog_seq": row["catalog_seq"], "root_name": row["root_name"],
            "root_version": row["root_version"], "facts_count": row["facts_count"],
            "created_at": row["created_at"]}


def list_slices(db):
    return db.execute(
        "SELECT slice_name, catalog_id, catalog_seq, root_name, root_version,"
        " facts_count, created_at FROM slices ORDER BY slice_name").fetchall()
