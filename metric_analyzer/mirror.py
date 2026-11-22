"""Slice mirror delivery: offline, versioned, integrity-checked JSONL files
that carry one named slice's *root metric groups* from a center database to an
independent report-machine database.

The module ships facts, dependency groups, catalogs or execution plans never;
it touches only the already-materialized root groups of one existing slice and
adds delivery bookkeeping around them.  Refresh and metric evaluation rules in
``slices.py`` are unchanged — an enabled slice refreshes exactly as before,
and the version bookkeeping runs inside the refresh's own transaction so that
a rolled-back refresh never consumes a version.

Delivery coordinates are ``(source_id, slice_name, version)``.  ``source_id``
is generated once when sync is enabled for a slice and is part of the
database's persistent state: reopening the database keeps the chain, while an
independent source database gets a different id even for a same-named slice at
the same version number, so it can never continue another chain's mirror.

File format (JSONL, one JSON object per line, UTF-8):

  line 1     header    {"kind": "mirror-full" | "mirror-delta", "format": 1,
                        "slice", "source_id", "version", "from_version"?,
                        "root", "grain", "catalog"}
  middle     records   full:  {"kind": "group", ...coords, "gkey",
                               "value": "<decimal>" | null}
                        delta: {"kind": "op", ...coords, "gkey",
                                "change": "upsert" | "delete", "value"?}
  last line  manifest  {"kind": "manifest", ...coords, "records",
                        "header_digest", "digest"}

A file is complete only when its last line is a manifest whose ``digest`` is
the SHA-256 of the exact bytes of every preceding line and whose
``record_count`` matches.  Integers travel as decimal strings, so arbitrary
precision is preserved end to end.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3

from . import astx, slices, store

FORMAT_VERSION = 1

# --------------------------------------------------------------------- errors


class MirrorError(Exception):
    """A delivery file or a delivery coordinate was rejected."""


# ------------------------------------------------------------ receiver schema

RECEIVER_SCHEMA = """
CREATE TABLE IF NOT EXISTS mirror_meta (
  source_id     TEXT NOT NULL,
  slice_name    TEXT NOT NULL,
  version       INTEGER NOT NULL,          -- last committed delivered version
  root_name     TEXT NOT NULL,
  root_version  TEXT NOT NULL,
  grain_json    TEXT NOT NULL,             -- canonical JSON array of dimensions
  catalog_json  TEXT NOT NULL,             -- catalog coordinates of the slice
  PRIMARY KEY (source_id, slice_name)
);
CREATE TABLE IF NOT EXISTS mirror_groups (
  source_id  TEXT NOT NULL,
  slice_name TEXT NOT NULL,
  gkey       TEXT NOT NULL,                -- canonical JSON array of dim values
  value      TEXT,                         -- decimal integer text; NULL = null
  PRIMARY KEY (source_id, slice_name, gkey)
);
CREATE TABLE IF NOT EXISTS mirror_ledger (
  source_id    TEXT NOT NULL,
  slice_name   TEXT NOT NULL,
  from_version INTEGER,                    -- NULL for a full file
  version      INTEGER NOT NULL,
  digest       TEXT NOT NULL,             -- whole-file digest of the accepted file
  received_at  TEXT NOT NULL,
  PRIMARY KEY (source_id, slice_name, from_version, version)
);
"""


def mirror_connect(path):
    """Open (and initialize) an independent receiver database."""
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript(RECEIVER_SCHEMA)
    return db


# ------------------------------------------------------------- source side

def enable_sync(db, slice_name):
    """Enable mirror delivery for an existing slice and snapshot its currently
    committed root groups as version 0.

    The snapshot is taken in one transaction; no older history is fabricated.
    Sequentially traversing the root groups once is allowed for the initial
    full snapshot only.
    """
    meta = store.get_slice(db, slice_name)
    if meta is None:
        raise MirrorError(f"slice '{slice_name}' not found")
    cat = store.get_catalog(db, meta["catalog_id"], meta["catalog_seq"])
    closure = slices.compile_closure(cat["content"], meta["root_name"],
                                     meta["root_version"])
    if store.get_slice_sync(db, slice_name) is not None:
        raise MirrorError(f"mirror delivery already enabled for slice "
                          f"'{slice_name}'")
    source_id = secrets.token_hex(16)
    db.execute("BEGIN IMMEDIATE")
    try:
        if db.execute("SELECT 1 FROM slice_sync WHERE slice_name = ?",
                      (slice_name,)).fetchone() is not None:
            raise MirrorError(f"mirror delivery already enabled for slice "
                              f"'{slice_name}'")
        db.execute(
            "INSERT INTO slice_sync (slice_name, source_id, current_version,"
            " enabled_at) VALUES (?,?,0,?)",
            (slice_name, source_id, store._now()))
        # one sequential pass over the root groups; streamed into the log
        db.execute(
            "INSERT INTO slice_sync_log (slice_name, version, gkey, op, value)"
            " SELECT ?, 0, gkey, 'upsert', value FROM slice_groups"
            " WHERE slice_name = ? AND metric = ?",
            (slice_name, slice_name, closure.root_key))
        db.commit()
    except Exception:
        db.rollback()
        raise
    return {"slice_name": slice_name, "source_id": source_id,
            "version": 0,
            "groups": db.execute(
                "SELECT COUNT(*) AS n FROM slice_sync_log WHERE slice_name=?"
                " AND version=0", (slice_name,)).fetchone()["n"]}


def make_advance_hook(db, slice_name):
    """Build the on-commit hook for ``slices.apply_batch`` when mirror delivery
    is enabled, else ``None`` (the refresh then behaves exactly as before).

    The hook runs inside the refresh transaction: it advances the version by
    exactly one and records that version's net root-group changes.  A refresh
    with identical root results still advances; a rolled-back refresh leaves
    both the version and the log untouched.
    """
    sync = store.get_slice_sync(db, slice_name)
    if sync is None:
        return None
    source_id = sync["source_id"]

    def hook(conn, root_changes):
        row = conn.execute(
            "SELECT current_version, source_id FROM slice_sync"
            " WHERE slice_name = ?", (slice_name,)).fetchone()
        if row is None:
            raise MirrorError(f"mirror state for slice '{slice_name}' "
                              f"vanished during refresh")
        if row["source_id"] != source_id:
            raise MirrorError("source identity changed inside the refresh "
                              "transaction; refusing to advance the version")
        new_version = row["current_version"] + 1
        conn.execute(
            "UPDATE slice_sync SET current_version = ? WHERE slice_name = ?",
            (new_version, slice_name))
        conn.executemany(
            "INSERT INTO slice_sync_log (slice_name, version, gkey, op, value)"
            " VALUES (?,?,?,?,?)",
            [(slice_name, new_version, gk, op, value)
             for gk, op, value in root_changes])

    return hook


def _header(sync, meta, root_key, grain, kind, from_version):
    hdr = {
        "kind": kind,
        "format": FORMAT_VERSION,
        "slice": meta["slice_name"],
        "source_id": sync["source_id"],
        "version": sync["current_version"],
        "root": root_key,
        "grain": list(grain),
        "catalog": {"catalog_id": meta["catalog_id"],
                    "seq": meta["catalog_seq"]},
    }
    if kind == "mirror-delta":
        hdr["from_version"] = from_version
    return hdr


def export_file(db_path, slice_name, out_path, from_version=None):
    """Stream one committed point in time to ``out_path`` (full or delta).

    A dedicated connection opens a snapshot transaction (WAL is enabled first
    so an export never blocks concurrent refreshes).  Whatever refreshes commit
    while the file is being written are invisible to this export: neither the
    values nor the labeled version can change mid-file.  Rows are streamed
    through a server-side cursor; the full result set is never held in memory.
    """
    # isolation_level=None gives manual transaction control: the Python
    # sqlite3 module would otherwise open an implicit transaction on the first
    # DML and make an explicit BEGIN raise, silently dropping the snapshot.
    sdb = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    sdb.row_factory = sqlite3.Row
    try:
        try:
            sdb.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass                      # read-only media etc.: snapshot still works
        sdb.execute("BEGIN DEFERRED")   # explicit multi-statement read snapshot
        try:
            sync = store.get_slice_sync(sdb, slice_name)
            if sync is None:
                raise MirrorError(
                    f"slice '{slice_name}' not found or mirror delivery not "
                    f"enabled; run mirror-enable first")
            meta = store.get_slice(sdb, slice_name)
            cat = store.get_catalog(sdb, meta["catalog_id"],
                                    meta["catalog_seq"])
            closure = slices.compile_closure(cat["content"], meta["root_name"],
                                             meta["root_version"])
            to_version = sync["current_version"]
            kind = ("mirror-full" if from_version is None
                    else "mirror-delta")
            if from_version is not None:
                if not isinstance(from_version, int) or isinstance(
                        from_version, bool) or from_version < 0:
                    raise MirrorError("from_version must be a non-negative "
                                      "integer")
                if from_version >= to_version:
                    raise MirrorError(
                        f"delta {from_version} -> {to_version}: from_version "
                        f"must be an older enabled version of this slice")
            hdr = _header(sync, meta, closure.root_key,
                          closure.metrics[closure.root_key].grain, kind,
                          from_version)
            part = out_path + ".part"
            records = 0
            running = hashlib.sha256()
            with open(part, "wb") as fh:
                raw = (json.dumps(hdr, ensure_ascii=False, sort_keys=True)
                       + "\n").encode("utf-8")
                fh.write(raw)
                header_digest = hashlib.sha256(raw).hexdigest()
                running.update(raw)

                if kind == "mirror-full":
                    cursor = sdb.execute(
                        "SELECT gkey, value FROM slice_groups"
                        " WHERE slice_name = ? AND metric = ?"
                        " ORDER BY gkey",
                        (slice_name, closure.root_key))
                    for r in cursor:            # server-side cursor, streamed
                        rec = {"kind": "group", "format": FORMAT_VERSION,
                               "slice": slice_name,
                               "source_id": sync["source_id"],
                               "version": to_version,
                               "gkey": json.loads(r["gkey"]),
                               "value": r["value"]}
                        raw = (json.dumps(rec, ensure_ascii=False,
                                          sort_keys=True) + "\n").encode("utf-8")
                        fh.write(raw)
                        running.update(raw)
                        records += 1
                else:
                    # only keys that actually changed inside (from, to]; the
                    # live root groups are never scanned
                    keys = sdb.execute(
                        "SELECT DISTINCT gkey FROM slice_sync_log"
                        " WHERE slice_name = ? AND version > ? AND version <= ?"
                        " ORDER BY gkey",
                        (slice_name, from_version, to_version)).fetchall()
                    for (gk,) in keys:
                        # state exactly at each endpoint: last log row at or
                        # below that version; None means the key did not exist
                        old = sdb.execute(
                            "SELECT op, value FROM slice_sync_log"
                            " WHERE slice_name = ? AND gkey = ? AND version <= ?"
                            " ORDER BY version DESC LIMIT 1",
                            (slice_name, gk, from_version)).fetchone()
                        new = sdb.execute(
                            "SELECT op, value FROM slice_sync_log"
                            " WHERE slice_name = ? AND gkey = ? AND version <= ?"
                            " ORDER BY version DESC LIMIT 1",
                            (slice_name, gk, to_version)).fetchone()
                        old_state = None if old is None else dict(old)
                        new_state = None if new is None else dict(new)
                        if old_state == new_state:
                            continue            # changed and reverted: skip
                        # existence follows the op, never the value: a delete
                        # row carries SQL NULL just like an upsert of a null
                        # valued group, but only the former means "gone"
                        if new_state is None or new_state["op"] == "delete":
                            rec = {"kind": "op", "format": FORMAT_VERSION,
                                   "slice": slice_name,
                                   "source_id": sync["source_id"],
                                   "version": to_version,
                                   "from_version": from_version,
                                   "gkey": json.loads(gk),
                                   "change": "delete"}
                        else:
                            # an upsert with value null is a null group, not a
                            # delete; values stay decimal strings (exact ints)
                            rec = {"kind": "op", "format": FORMAT_VERSION,
                                   "slice": slice_name,
                                   "source_id": sync["source_id"],
                                   "version": to_version,
                                   "from_version": from_version,
                                   "gkey": json.loads(gk),
                                   "change": "upsert",
                                   "value": new_state["value"]}
                        raw = (json.dumps(rec, ensure_ascii=False,
                                          sort_keys=True) + "\n").encode("utf-8")
                        fh.write(raw)
                        running.update(raw)
                        records += 1

                manifest = {
                    "kind": "manifest", "format": FORMAT_VERSION,
                    "slice": slice_name, "source_id": sync["source_id"],
                    "from_version": from_version, "version": to_version,
                    "records": records, "header_digest": header_digest,
                    "digest": running.hexdigest(),
                }
                fh.write((json.dumps(manifest, ensure_ascii=False,
                                     sort_keys=True) + "\n").encode("utf-8"))
            os.replace(part, out_path)
            return {"kind": kind, "slice": slice_name,
                    "source_id": sync["source_id"],
                    "from_version": from_version, "version": to_version,
                    "records": records, "digest": running.hexdigest(),
                    "path": out_path}
        finally:
            sdb.rollback()
    finally:
        sdb.close()


# ------------------------------------------------------------- receiver side

def _read_line(fh):
    """One raw line without its trailing newline; None at clean EOF."""
    raw = fh.readline()
    if raw == b"":
        return None
    if not raw.endswith(b"\n"):
        raise MirrorError("file is truncated: last line has no newline "
                          "(no manifest can follow)")
    return raw[:-1]


def _parse(raw, lineno):
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MirrorError(f"line {lineno}: corrupt JSON: {exc}")
    if not isinstance(obj, dict) or "kind" not in obj:
        raise MirrorError(f"line {lineno}: not a mirror JSONL object")
    return obj


def _check_int_text(value, lineno):
    if value is not None and not (isinstance(value, str) and
                                  _is_decimal_int(value)):
        raise MirrorError(f"line {lineno}: value must be a decimal integer "
                          f"string or null, got {value!r}")


def _is_decimal_int(text):
    s = text[1:] if text[:1] in ("-", "+") else text
    return s.isdigit() and s != ""


def receive_file(db, file_path):
    """Verify and atomically apply one mirror file to a receiver database.

    Streaming verification: the manifest on the last line must match the
    running SHA-256 over every preceding byte and the record count.  Records
    land in a temporary staging table inside one transaction; any truncation,
    corruption, checksum or coordinate failure rolls everything back, so
    neither groups nor the delivered coordinate advance.  An exact re-delivery
    of the last accepted file is a safe no-op; the same coordinates with
    different content are rejected.
    """
    db.execute("BEGIN IMMEDIATE")
    staged = False
    try:
        db.execute("CREATE TEMP TABLE IF NOT EXISTS mirror_stage ("
                   " gkey TEXT PRIMARY KEY, change TEXT NOT NULL,"
                   " value TEXT) WITHOUT ROWID")
        staged = True
        header = None
        records = 0
        running = hashlib.sha256()
        manifest = None
        with open(file_path, "rb") as fh:
            lineno = 0
            while True:
                raw = _read_line(fh)
                if raw is None:
                    break
                lineno += 1
                if lineno == 1:
                    header = _parse(raw, lineno)
                    if header.get("kind") not in ("mirror-full",
                                                   "mirror-delta"):
                        raise MirrorError(
                            "line 1: header kind must be mirror-full or "
                            "mirror-delta")
                    if header.get("format") != FORMAT_VERSION:
                        raise MirrorError(
                            f"unsupported format {header.get('format')!r}; "
                            f"expected {FORMAT_VERSION}")
                    for field in ("slice", "source_id", "version", "root",
                                  "grain", "catalog"):
                        if field not in header:
                            raise MirrorError(
                                f"line 1: header missing '{field}'")
                    if not isinstance(header["version"], int) or isinstance(
                            header["version"], bool) or header["version"] < 0:
                        raise MirrorError("header version must be a "
                                          "non-negative integer")
                    if header["kind"] == "mirror-delta":
                        fv = header.get("from_version")
                        if not (isinstance(fv, int) and not isinstance(fv, bool)
                                and fv >= 0 and fv < header["version"]):
                            raise MirrorError(
                                "delta header requires 0 <= from_version < "
                                "version")
                    if not isinstance(header["grain"], list) or not all(
                            isinstance(g, str) for g in header["grain"]):
                        raise MirrorError("header grain must be a list of "
                                          "dimension names")
                    running.update(raw + b"\n")
                    header_digest = hashlib.sha256(raw + b"\n").hexdigest()
                    continue

                obj = _parse(raw, lineno)
                kind = obj["kind"]
                if kind == "manifest":
                    manifest = obj
                    # the manifest must be the last line of the file
                    extra = fh.readline()
                    if extra != b"":
                        raise MirrorError(
                            "trailing bytes after the manifest line; the "
                            "manifest must be the last line")
                    break
                if kind not in ("group", "op"):
                    raise MirrorError(f"line {lineno}: unknown record kind "
                                      f"{kind!r}")
                _same_coords(obj, header, lineno)
                gk = _canon_gkey(obj, header, lineno)
                if kind == "group":
                    if header["kind"] != "mirror-full":
                        raise MirrorError(f"line {lineno}: group records are "
                                          f"valid in full files only")
                    value = obj.get("value")
                    _check_int_text(value, lineno)
                    _insert_stage(db, gk, "upsert", value, lineno)
                else:
                    if header["kind"] != "mirror-delta":
                        raise MirrorError(f"line {lineno}: op records are "
                                          f"valid in delta files only")
                    if obj.get("from_version") != header.get("from_version"):
                        raise MirrorError(f"line {lineno}: record coordinates "
                                          f"do not match the header")
                    change = obj.get("change")
                    if change not in ("upsert", "delete"):
                        raise MirrorError(f"line {lineno}: change must be "
                                          f"upsert or delete")
                    if change == "upsert":
                        value = obj.get("value")
                        _check_int_text(value, lineno)
                    else:
                        if "value" in obj:
                            raise MirrorError(
                                f"line {lineno}: delete records carry no value")
                        value = None
                    _insert_stage(db, gk, change, value, lineno)
                running.update(raw + b"\n")
                records += 1

        if manifest is None:
            raise MirrorError("file has no manifest line: it is truncated or "
                              "not a complete mirror file")
        if manifest.get("format") != FORMAT_VERSION:
            raise MirrorError("manifest format mismatch")
        if manifest.get("header_digest") != header_digest:
            raise MirrorError("header digest mismatch: header bytes do not "
                              "match the manifest")
        if manifest.get("digest") != running.hexdigest():
            raise MirrorError("checksum mismatch: file content does not match "
                              "the manifest digest")
        if manifest.get("records") != records:
            raise MirrorError(
                f"record count mismatch: manifest says "
                f"{manifest.get('records')}, file carries {records}")
        _same_coords(manifest, header, "manifest")
        if header["kind"] == "mirror-delta" and manifest.get("from_version") \
                != header["from_version"]:
            raise MirrorError("manifest from_version does not match the header")

        source_id = header["source_id"]
        slice_name = header["slice"]
        to_version = header["version"]
        from_version = header.get("from_version")
        digest = manifest["digest"]

        # ---- exact redelivery vs. same-coordinate different content -------
        led = db.execute(
            "SELECT digest FROM mirror_ledger WHERE source_id=? AND "
            "slice_name=? AND from_version IS ? AND version=?",
            (source_id, slice_name, from_version, to_version)).fetchone()
        if led is not None:
            if led["digest"] == digest:
                db.rollback()
                return {"redelivery": True, "source_id": source_id,
                        "slice": slice_name, "from_version": from_version,
                        "version": to_version, "records": records}
            raise MirrorError(
                f"a file with coordinates ({source_id}, {slice_name}, "
                f"{from_version} -> {to_version}) was already accepted with a "
                f"different digest; same coordinates with different content "
                f"is not a redelivery")

        meta = db.execute(
            "SELECT * FROM mirror_meta WHERE source_id=? AND slice_name=?",
            (source_id, slice_name)).fetchone()

        if header["kind"] == "mirror-full":
            if meta is not None:
                # an expired full file can never overwrite an advanced mirror;
                # exact redelivery was handled above
                raise MirrorError(
                    f"mirror ({source_id}, {slice_name}) already exists at "
                    f"version {meta['version']}; full files establish new "
                    f"mirrors only")
            db.execute(
                "INSERT INTO mirror_meta (source_id, slice_name, version,"
                " root_name, root_version, grain_json, catalog_json)"
                " VALUES (?,?,?,?,?,?,?)",
                (source_id, slice_name, to_version,
                 *_split_root(header["root"]),
                 astx.canon(header["grain"]),
                 astx.canon(header["catalog"])))
            db.execute(
                "INSERT INTO mirror_groups (source_id, slice_name, gkey, value)"
                " SELECT ?, ?, gkey, value FROM mirror_stage"
                " WHERE change = 'upsert'",
                (source_id, slice_name))
        else:
            if meta is None:
                raise MirrorError(
                    f"no mirror for ({source_id}, {slice_name}); a full file "
                    f"must establish the mirror before deltas")
            if meta["version"] != from_version:
                raise MirrorError(
                    f"delta starts at {from_version} but the mirror is at "
                    f"version {meta['version']}: missing predecessor or an "
                    f"expired file")
            # every key at most one op (enforced by staging PK); apply in order
            for r in db.execute(
                    "SELECT gkey, change, value FROM mirror_stage"
                    " ORDER BY gkey"):
                if r["change"] == "delete":
                    cur = db.execute(
                        "DELETE FROM mirror_groups WHERE source_id=? AND"
                        " slice_name=? AND gkey=?",
                        (source_id, slice_name, r["gkey"]))
                    if cur.rowcount == 0:
                        raise MirrorError(
                            f"delta deletes gkey {r['gkey']} which the mirror "
                            f"does not contain")
                else:
                    db.execute(
                        "INSERT INTO mirror_groups (source_id, slice_name,"
                        " gkey, value) VALUES (?,?,?,?)"
                        " ON CONFLICT(source_id, slice_name, gkey)"
                        " DO UPDATE SET value=excluded.value",
                        (source_id, slice_name, r["gkey"], r["value"]))
            db.execute(
                "UPDATE mirror_meta SET version=? WHERE source_id=? AND"
                " slice_name=?", (to_version, source_id, slice_name))

        db.execute(
            "INSERT INTO mirror_ledger (source_id, slice_name, from_version,"
            " version, digest, received_at) VALUES (?,?,?,?,?,?)",
            (source_id, slice_name, from_version, to_version, digest,
             store._now()))
        db.commit()
        return {"redelivery": False, "source_id": source_id,
                "slice": slice_name, "from_version": from_version,
                "version": to_version, "records": records, "digest": digest}
    except Exception:
        db.rollback()
        raise
    finally:
        if staged:
            try:
                db.execute("DROP TABLE IF EXISTS mirror_stage")
            except sqlite3.Error:
                pass


def _split_root(root):
    name, sep, version = str(root).partition("@")
    if not sep or not name or not version:
        raise MirrorError(f"header root must be name@version, got {root!r}")
    return name, version


def _same_coords(obj, header, lineno):
    for field in ("slice", "source_id", "version"):
        if obj.get(field) != header[field]:
            raise MirrorError(f"line {lineno}: {field} does not match the "
                              f"header coordinates")


def _canon_gkey(obj, header, lineno):
    gkey = obj.get("gkey")
    if not isinstance(gkey, list) or len(gkey) != len(header["grain"]):
        raise MirrorError(f"line {lineno}: gkey must be a JSON array matching "
                          f"the header grain {header['grain']}")
    if not all(isinstance(v, str) and v != "" for v in gkey):
        raise MirrorError(f"line {lineno}: dimension values must be non-empty "
                          f"strings")
    return astx.canon(gkey)


def _insert_stage(db, gkey, change, value, lineno):
    try:
        db.execute("INSERT INTO mirror_stage (gkey, change, value)"
                   " VALUES (?,?,?)", (gkey, change, value))
    except sqlite3.IntegrityError:
        raise MirrorError(f"line {lineno}: gkey {gkey} appears more than "
                          f"once in this file")


# ------------------------------------------------------------ receiver reads

def query_mirror(db, source_id, slice_name, key=None):
    """Read one delivered version's groups directly from the receiver db."""
    meta = db.execute(
        "SELECT * FROM mirror_meta WHERE source_id=? AND slice_name=?",
        (source_id, slice_name)).fetchone()
    if meta is None:
        raise MirrorError(f"mirror ({source_id}, {slice_name}) not found")
    grain = json.loads(meta["grain_json"])
    groups = []
    if key is None:
        rows = db.execute(
            "SELECT gkey, value FROM mirror_groups WHERE source_id=? AND"
            " slice_name=? ORDER BY gkey",
            (source_id, slice_name)).fetchall()
    else:
        gk = _key_to_gkey(key, grain)
        rows = db.execute(
            "SELECT gkey, value FROM mirror_groups WHERE source_id=? AND"
            " slice_name=? AND gkey=?",
            (source_id, slice_name, gk)).fetchall()
        if not rows:
            raise MirrorError(f"group {key!r} does not exist in this mirror")
    for r in rows:
        klist = json.loads(r["gkey"])
        groups.append({"key": {dim: val for dim, val in zip(grain, klist)},
                       "value": None if r["value"] is None
                       else int(r["value"])})
    return {"slice": meta["slice_name"], "source_id": meta["source_id"],
            "version": meta["version"],
            "root": f"{meta['root_name']}@{meta['root_version']}",
            "grain": grain, "catalog": json.loads(meta["catalog_json"]),
            "groups": groups}


def _key_to_gkey(key, grain):
    obj = json.loads(key) if isinstance(key, str) else key
    if isinstance(obj, dict):
        if set(obj) != set(grain):
            raise MirrorError(f"key dimensions {sorted(obj)} must equal the "
                              f"mirror grain {grain}")
        return astx.canon([obj[g] for g in grain])
    if isinstance(obj, list):
        if len(obj) != len(grain) or not all(
                isinstance(v, str) and v for v in obj):
            raise MirrorError("key array must match the mirror grain and hold "
                              "non-empty strings")
        return astx.canon(obj)
    raise MirrorError("key must be a JSON object or array")


def list_mirrors(db):
    return db.execute(
        "SELECT source_id, slice_name, version, root_name, root_version,"
        " grain_json FROM mirror_meta ORDER BY source_id, slice_name"
    ).fetchall()
