from __future__ import annotations

import os
from pathlib import Path

import pytest

from musicdedupe.grouping import (
    group_audio,
    group_exact,
    group_metadata,
    pick_best,
)
from musicdedupe.track import Track


def _make_file(tmp_path: Path, name: str, content: bytes) -> str:
    p = tmp_path / name
    p.write_bytes(content)
    return str(p)


def _track_for(path: str) -> Track:
    st = os.stat(path)
    return Track(path=path, size=st.st_size, mtime=st.st_mtime, ext=os.path.splitext(path)[1])


def test_group_exact_only_hashes_size_collisions(tmp_path: Path) -> None:
    # 3 files: two share bytes, one is unique-size. Unique should never be
    # hashed because it won't collide on size.
    a = _make_file(tmp_path, "a.mp3", b"hello world" * 100)
    b = _make_file(tmp_path, "b.mp3", b"hello world" * 100)
    c = _make_file(tmp_path, "c.mp3", b"different content entirely, different size")
    tracks = [_track_for(a), _track_for(b), _track_for(c)]

    groups, claimed = group_exact(tracks, algo="sha1", workers=1)

    assert len(groups) == 1
    group_paths = {t.path for t in groups[0]}
    assert group_paths == {a, b}
    assert claimed == {a, b}
    # Unique-size file never got a hash computed.
    c_track = next(t for t in tracks if t.path == c)
    assert c_track.content_hash == ""
    assert c_track.partial_hash == ""


def test_group_exact_partial_hash_short_circuits(tmp_path: Path, monkeypatch) -> None:
    # Two same-size files with different content: partial hashes differ, so
    # full_hash must NOT be called for either.
    a = _make_file(tmp_path, "a.mp3", b"A" * (4096 * 2 + 512))
    b = _make_file(tmp_path, "b.mp3", b"B" * (4096 * 2 + 512))
    assert os.path.getsize(a) == os.path.getsize(b)
    tracks = [_track_for(a), _track_for(b)]

    calls = {"full": 0}
    import musicdedupe.grouping as grouping_mod
    real_full = grouping_mod.hashing._full_worker

    def counting_full(args):
        calls["full"] += 1
        return real_full(args)

    monkeypatch.setattr(grouping_mod.hashing, "_full_worker", counting_full)

    groups, _ = group_exact(tracks, algo="sha1", workers=1)
    assert groups == []
    # Partial hashes differed → no full hash ever needed.
    assert calls["full"] == 0


def test_group_exact_skips_corrupted() -> None:
    t1 = Track(path="/m/a.mp3", size=100, corrupted=True)
    t2 = Track(path="/m/b.mp3", size=100, corrupted=True)
    groups, claimed = group_exact([t1, t2], algo="sha1", workers=1)
    assert groups == []
    assert claimed == set()


def test_group_audio_merges_identical_fingerprints() -> None:
    fp = "abc123" * 20
    t1 = Track(path="/m/a.flac", fingerprint=fp, duration=180.0, lossless=True, bitrate=900)
    t2 = Track(path="/m/b.mp3", fingerprint=fp, duration=180.0, bitrate=320)
    t3 = Track(path="/m/c.mp3", fingerprint="different" * 10, duration=180.0)
    groups, claimed = group_audio([t1, t2, t3], set())
    assert len(groups) == 1
    assert {t.path for t in groups[0]} == {"/m/a.flac", "/m/b.mp3"}
    assert "/m/c.mp3" not in claimed
    # Lossless/higher bitrate sorted first.
    assert groups[0][0].path == "/m/a.flac"


def test_group_audio_respects_claimed() -> None:
    fp = "x" * 100
    t1 = Track(path="/m/a.mp3", fingerprint=fp)
    t2 = Track(path="/m/b.mp3", fingerprint=fp)
    groups, _ = group_audio([t1, t2], claimed={"/m/a.mp3"})
    assert groups == []


def test_group_metadata_exact_key() -> None:
    tracks = [
        Track(path="/m/a.mp3", artist="Artist", title="Title"),
        Track(path="/m/b.mp3", artist="Artist", title="Title"),
        Track(path="/m/c.mp3", artist="Other", title="Title"),
    ]
    groups = group_metadata(tracks, set(), fuzzy=False)
    assert len(groups) == 1
    assert {t.path for t in groups[0]} == {"/m/a.mp3", "/m/b.mp3"}


def test_group_metadata_claimed() -> None:
    tracks = [
        Track(path="/m/a.mp3", artist="X", title="Y"),
        Track(path="/m/b.mp3", artist="X", title="Y"),
    ]
    groups = group_metadata(tracks, claimed={"/m/a.mp3"}, fuzzy=False)
    assert groups == []


def test_group_metadata_falls_back_to_filename() -> None:
    tracks = [
        Track(path="/m/song.mp3", artist="", title=""),
        Track(path="/m/sub/song.mp3", artist="", title=""),
    ]
    groups = group_metadata(tracks, set(), fuzzy=False)
    assert len(groups) == 1
    assert len(groups[0]) == 2


def test_pick_best_prefers_lossless_then_bitrate() -> None:
    g = [
        Track(path="/a", bitrate=320),
        Track(path="/b", bitrate=500, lossless=True),
        Track(path="/c", bitrate=128),
    ]
    assert pick_best(g) == 1
