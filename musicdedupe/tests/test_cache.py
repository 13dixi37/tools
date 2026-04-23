from __future__ import annotations

import json
import os
from pathlib import Path

from musicdedupe.cache import TrackCache
from musicdedupe.track import Track


def _sample(**overrides) -> Track:
    base = dict(
        path="/m/a.mp3", size=12345, mtime=1700000000.5, ext=".mp3",
        duration=184.7, bitrate=320, sample_rate=44100, channels=2, codec="mp3",
        lossless=False, artist="Artist", album="Album", title="Title",
        track_no="1", year="2024", content_hash="deadbeef", partial_hash="cafebabe",
        hash_algo="sha1", fingerprint="fpfpfp", fp_duration=184.0, corrupted=False,
        error="",
    )
    base.update(overrides)
    return Track(**base)


def test_roundtrip(tmp_path: Path) -> None:
    db = str(tmp_path / "cache.db")
    t = _sample()
    with TrackCache(db) as c:
        c.upsert(t)
        got = c.get(t.path, size=t.size, mtime=t.mtime)
    assert got is not None
    for attr in (
        "path", "size", "mtime", "ext", "duration", "bitrate", "sample_rate",
        "channels", "codec", "lossless", "artist", "album", "title",
        "track_no", "year", "content_hash", "partial_hash", "hash_algo",
        "fingerprint", "fp_duration", "corrupted", "error",
    ):
        assert getattr(got, attr) == getattr(t, attr), attr


def test_get_size_mtime_mismatch_returns_none(tmp_path: Path) -> None:
    db = str(tmp_path / "cache.db")
    t = _sample()
    with TrackCache(db) as c:
        c.upsert(t)
        # Size differs -> cache miss.
        assert c.get(t.path, size=t.size + 1, mtime=t.mtime) is None
        # Mtime differs -> cache miss.
        assert c.get(t.path, size=t.size, mtime=t.mtime + 1.0) is None


def test_upsert_many_transactional(tmp_path: Path) -> None:
    db = str(tmp_path / "cache.db")
    rows = [
        _sample(path=f"/m/{i}.mp3", size=1000 + i, mtime=100.0 + i)
        for i in range(50)
    ]
    with TrackCache(db) as c:
        c.upsert_many(rows)
    with TrackCache(db) as c:
        for r in rows:
            got = c.get(r.path, size=r.size, mtime=r.mtime)
            assert got is not None


def test_delete_paths(tmp_path: Path) -> None:
    db = str(tmp_path / "cache.db")
    a = _sample(path="/m/a.mp3")
    b = _sample(path="/m/b.mp3")
    with TrackCache(db) as c:
        c.upsert_many([a, b])
        c.delete_paths(["/m/a.mp3"])
        assert c.get_any("/m/a.mp3") is None
        assert c.get_any("/m/b.mp3") is not None


def test_migrate_json(tmp_path: Path) -> None:
    json_path = tmp_path / "legacy.json"
    legacy = {
        "/m/a.mp3": {
            "path": "/m/a.mp3",
            "size": 12,
            "mtime": 1.0,
            "ext": ".mp3",
            "artist": "X",
            "title": "Y",
        },
        "/m/b.mp3": {
            "path": "/m/b.mp3",
            "size": 34,
            "mtime": 2.0,
            "ext": ".mp3",
        },
    }
    json_path.write_text(json.dumps(legacy), encoding="utf-8")

    db = str(tmp_path / "cache.db")
    with TrackCache(db) as c:
        imported = c.migrate_json(str(json_path))
    assert imported == 2
    # Legacy file was renamed to `.migrated`, not deleted.
    assert not json_path.exists()
    assert (tmp_path / "legacy.json.migrated").exists()

    with TrackCache(db) as c:
        got = c.get("/m/a.mp3", size=12, mtime=1.0)
        assert got is not None and got.artist == "X"


def test_migrate_json_missing_file(tmp_path: Path) -> None:
    db = str(tmp_path / "cache.db")
    with TrackCache(db) as c:
        assert c.migrate_json(str(tmp_path / "does-not-exist.json")) == 0


def test_migrate_json_handles_extra_keys(tmp_path: Path) -> None:
    # Forward-compat: older JSON or newer JSON with extra keys should not crash.
    json_path = tmp_path / "legacy.json"
    json_path.write_text(json.dumps({
        "/m/a.mp3": {"path": "/m/a.mp3", "size": 1, "mtime": 1.0, "ext": ".mp3",
                     "unknown_key": "ignore me", "another": 42},
    }), encoding="utf-8")
    db = str(tmp_path / "cache.db")
    with TrackCache(db) as c:
        assert c.migrate_json(str(json_path)) == 1
