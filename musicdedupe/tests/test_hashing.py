from __future__ import annotations

import hashlib
import os
from pathlib import Path

from musicdedupe.hashing import (
    PARTIAL_WINDOW,
    full_hash,
    partial_hash,
    preferred_algo,
    resolve_algo,
)


def _write(tmp_path: Path, name: str, data: bytes) -> str:
    p = tmp_path / name
    p.write_bytes(data)
    return str(p)


def test_resolve_algo_sha1_always_available() -> None:
    assert resolve_algo("sha1") == "sha1"
    # Unknown → falls back to preferred, which is always one of the three.
    assert resolve_algo("nonsense") in ("blake3", "xxhash", "sha1")
    assert resolve_algo("auto") == preferred_algo()


def test_full_hash_matches_reference(tmp_path: Path) -> None:
    data = os.urandom(64 * 1024)  # 64 KiB
    path = _write(tmp_path, "small.bin", data)
    assert full_hash(path, "sha1") == hashlib.sha1(data).hexdigest()


def test_full_hash_missing_file_returns_empty() -> None:
    assert full_hash("/nonexistent/path/no-such-file", "sha1") == ""


def test_partial_hash_small_file_hashes_everything(tmp_path: Path) -> None:
    data = b"x" * 4096
    path = _write(tmp_path, "tiny.bin", data)
    # Small file: partial_hash is sha1(data + size_bytes).
    expected = hashlib.sha1(data + len(data).to_bytes(8, "little")).hexdigest()
    assert partial_hash(path, "sha1") == expected


def test_partial_hash_large_file_hashes_windows(tmp_path: Path) -> None:
    # File strictly larger than 2 * PARTIAL_WINDOW → distinct front/back windows.
    size = PARTIAL_WINDOW * 2 + 1024
    head = os.urandom(PARTIAL_WINDOW)
    mid = os.urandom(size - 2 * PARTIAL_WINDOW)
    tail = os.urandom(PARTIAL_WINDOW)
    path = _write(tmp_path, "big.bin", head + mid + tail)

    expected = hashlib.sha1()
    expected.update(head)
    expected.update(tail)
    expected.update(size.to_bytes(8, "little"))
    assert partial_hash(path, "sha1") == expected.hexdigest()


def test_partial_hash_different_middles_same_windows_match(tmp_path: Path) -> None:
    # Two large files with identical 1 MiB head+tail but different middles:
    # partial_hash should match (that's the short-circuit we want), full_hash
    # should not.
    size = PARTIAL_WINDOW * 2 + 2048
    head = os.urandom(PARTIAL_WINDOW)
    tail = os.urandom(PARTIAL_WINDOW)
    mid_a = b"A" * (size - 2 * PARTIAL_WINDOW)
    mid_b = b"B" * (size - 2 * PARTIAL_WINDOW)
    pa = _write(tmp_path, "a.bin", head + mid_a + tail)
    pb = _write(tmp_path, "b.bin", head + mid_b + tail)

    assert partial_hash(pa, "sha1") == partial_hash(pb, "sha1")
    assert full_hash(pa, "sha1") != full_hash(pb, "sha1")
