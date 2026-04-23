"""SQLite-backed scan cache.

Schema v2: one row per path, keyed by absolute path. Updates are O(1); no
full-file rewrite. WAL mode so concurrent readers don't block the scanner.
"""
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import fields
from typing import Any, Iterable, Optional

from .track import Track

SCHEMA_VERSION = 2

_COLUMNS = [
    "path", "size", "mtime", "ext", "duration", "bitrate", "sample_rate",
    "channels", "codec", "lossless", "artist", "album", "title", "track_no",
    "year", "content_hash", "partial_hash", "hash_algo", "fingerprint",
    "fp_duration", "corrupted", "error",
]

_CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS tracks (
  path          TEXT PRIMARY KEY,
  size          INTEGER NOT NULL,
  mtime         REAL    NOT NULL,
  ext           TEXT,
  duration      REAL,
  bitrate       INTEGER,
  sample_rate   INTEGER,
  channels      INTEGER,
  codec         TEXT,
  lossless      INTEGER,
  artist        TEXT,
  album         TEXT,
  title         TEXT,
  track_no      TEXT,
  year          TEXT,
  content_hash  TEXT,
  partial_hash  TEXT,
  hash_algo     TEXT,
  fingerprint   TEXT,
  fp_duration   REAL,
  corrupted     INTEGER,
  error         TEXT,
  schema_v      INTEGER NOT NULL DEFAULT {SCHEMA_VERSION}
);
CREATE INDEX IF NOT EXISTS idx_size ON tracks(size);
CREATE INDEX IF NOT EXISTS idx_fingerprint ON tracks(fingerprint);
"""

_UPSERT_SQL = f"""
INSERT OR REPLACE INTO tracks
  ({', '.join(_COLUMNS)}, schema_v)
VALUES ({', '.join('?' for _ in _COLUMNS)}, {SCHEMA_VERSION})
"""


def _row_to_track(row: sqlite3.Row) -> Track:
    return Track(
        path=str(row["path"]),
        size=int(row["size"] or 0),
        mtime=float(row["mtime"] or 0.0),
        ext=str(row["ext"] or ""),
        duration=float(row["duration"] or 0.0),
        bitrate=int(row["bitrate"] or 0),
        sample_rate=int(row["sample_rate"] or 0),
        channels=int(row["channels"] or 0),
        codec=str(row["codec"] or ""),
        lossless=bool(row["lossless"]),
        artist=str(row["artist"] or ""),
        album=str(row["album"] or ""),
        title=str(row["title"] or ""),
        track_no=str(row["track_no"] or ""),
        year=str(row["year"] or ""),
        content_hash=str(row["content_hash"] or ""),
        partial_hash=str(row["partial_hash"] or ""),
        hash_algo=str(row["hash_algo"] or ""),
        fingerprint=str(row["fingerprint"] or ""),
        fp_duration=float(row["fp_duration"] or 0.0),
        corrupted=bool(row["corrupted"]),
        error=str(row["error"] or ""),
    )


def _track_to_row(t: Track) -> tuple:
    return (
        t.path,
        t.size,
        t.mtime,
        t.ext,
        t.duration,
        t.bitrate,
        t.sample_rate,
        t.channels,
        t.codec,
        1 if t.lossless else 0,
        t.artist,
        t.album,
        t.title,
        t.track_no,
        t.year,
        t.content_hash,
        t.partial_hash,
        t.hash_algo,
        t.fingerprint,
        t.fp_duration,
        1 if t.corrupted else 0,
        t.error,
    )


class TrackCache:
    """SQLite cache for Track rows. Use as a context manager."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        # The grouping worker runs in a background thread and writes its
        # results through the same handle (scan → grouping access is
        # serialized; see cli.py), so we disable the stdlib check_same_thread
        # guard rather than juggle per-thread connections.
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_CREATE_SQL)
        # WAL + relaxed sync: big write-throughput win, still crash-safe.
        try:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
        except sqlite3.DatabaseError:
            pass

    def __enter__(self) -> "TrackCache":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._conn.commit()
        finally:
            self._conn.close()

    # --- reads ---------------------------------------------------------------

    def get(self, path: str, *, size: int, mtime: float) -> Optional[Track]:
        """Return the cached track if size+mtime still match; else None."""
        row = self._conn.execute(
            "SELECT * FROM tracks WHERE path=? AND size=? AND mtime=?",
            (path, size, mtime),
        ).fetchone()
        if row is None:
            return None
        return _row_to_track(row)

    def get_any(self, path: str) -> Optional[Track]:
        row = self._conn.execute("SELECT * FROM tracks WHERE path=?", (path,)).fetchone()
        if row is None:
            return None
        return _row_to_track(row)

    # --- writes --------------------------------------------------------------

    def upsert(self, t: Track) -> None:
        self._conn.execute(_UPSERT_SQL, _track_to_row(t))
        self._conn.commit()

    def upsert_many(self, tracks: Iterable[Track]) -> None:
        rows = [_track_to_row(t) for t in tracks]
        if not rows:
            return
        with self._conn:
            self._conn.executemany(_UPSERT_SQL, rows)

    def delete_paths(self, paths: Iterable[str]) -> None:
        paths = list(paths)
        if not paths:
            return
        with self._conn:
            self._conn.executemany("DELETE FROM tracks WHERE path=?", ((p,) for p in paths))

    # --- migration -----------------------------------------------------------

    def migrate_json(self, json_path: str) -> int:
        """Import a legacy .musicdedupe-cache.json, if present. Returns count imported.

        Renames the source file to `<name>.migrated` so this is a one-shot.
        """
        if not os.path.exists(json_path):
            return 0
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return 0
        if not isinstance(data, dict):
            return 0
        valid = {f.name for f in fields(Track)}
        tracks: list[Track] = []
        for path, entry in data.items():
            if not isinstance(entry, dict):
                continue
            kwargs: dict[str, Any] = {k: v for k, v in entry.items() if k in valid}
            kwargs.setdefault("path", path)
            try:
                tracks.append(Track(**kwargs))
            except TypeError:
                continue
        self.upsert_many(tracks)
        try:
            os.replace(json_path, json_path + ".migrated")
        except OSError:
            pass
        return len(tracks)
