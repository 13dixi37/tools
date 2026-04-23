"""Layered hashing: partial (first+last 1 MiB) then full.

Preferred algorithms, in order: blake3 > xxhash (xxh128) > sha1 (stdlib).
All returned digests are hex strings. Empty string means I/O error.

Module-level functions so they pickle cleanly across ProcessPoolExecutor.
"""
from __future__ import annotations

import hashlib
from typing import Callable, Optional

try:
    import blake3 as _blake3  # type: ignore[import-not-found]

    HAS_BLAKE3 = True
except ImportError:
    _blake3 = None  # type: ignore[assignment]
    HAS_BLAKE3 = False

try:
    import xxhash as _xxhash  # type: ignore[import-not-found]

    HAS_XXHASH = True
except ImportError:
    _xxhash = None  # type: ignore[assignment]
    HAS_XXHASH = False


PARTIAL_WINDOW = 1 << 20      # 1 MiB from each end
READ_CHUNK = 1 << 20          # streaming chunk size
ALGOS = ("blake3", "xxhash", "sha1")


def preferred_algo() -> str:
    if HAS_BLAKE3:
        return "blake3"
    if HAS_XXHASH:
        return "xxhash"
    return "sha1"


def resolve_algo(requested: str) -> str:
    """Return a concrete algo name, falling back when the requested one is missing."""
    req = (requested or "auto").lower()
    if req == "auto":
        return preferred_algo()
    if req == "blake3" and HAS_BLAKE3:
        return "blake3"
    if req == "xxhash" and HAS_XXHASH:
        return "xxhash"
    if req == "sha1":
        return "sha1"
    # Asked for something unavailable — degrade gracefully.
    return preferred_algo()


def _hasher(algo: str):  # type: ignore[no-untyped-def]
    if algo == "blake3":
        if not HAS_BLAKE3:
            raise RuntimeError("blake3 not installed")
        return _blake3.blake3()
    if algo == "xxhash":
        if not HAS_XXHASH:
            raise RuntimeError("xxhash not installed")
        return _xxhash.xxh128()
    if algo == "sha1":
        return hashlib.sha1()
    raise ValueError(f"unknown algo: {algo!r}")


def partial_hash(path: str, algo: str) -> str:
    """Hash the first and last 1 MiB of the file.

    For files <= 2 * PARTIAL_WINDOW, this hashes the whole file, so on small
    files partial == full (content-wise equal, still formatted as the algo's
    hex digest). Returns '' on I/O error.
    """
    try:
        h = _hasher(algo)
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(0)
            if size <= PARTIAL_WINDOW * 2:
                while True:
                    b = f.read(READ_CHUNK)
                    if not b:
                        break
                    h.update(b)
            else:
                h.update(f.read(PARTIAL_WINDOW))
                f.seek(size - PARTIAL_WINDOW)
                h.update(f.read(PARTIAL_WINDOW))
            # Mix in size so that two different-sized files with equal partial
            # windows (unlikely but possible on padded formats) still diverge.
            h.update(size.to_bytes(8, "little"))
        return h.hexdigest()
    except OSError:
        return ""


def full_hash(path: str, algo: str) -> str:
    """Stream the whole file and return the hex digest. '' on I/O error."""
    try:
        h = _hasher(algo)
        with open(path, "rb") as f:
            while True:
                b = f.read(READ_CHUNK)
                if not b:
                    break
                h.update(b)
        return h.hexdigest()
    except OSError:
        return ""


# Worker-pool entry points (picklable).

def _partial_worker(args: tuple[str, str]) -> tuple[str, str]:
    path, algo = args
    return path, partial_hash(path, algo)


def _full_worker(args: tuple[str, str]) -> tuple[str, str]:
    path, algo = args
    return path, full_hash(path, algo)
