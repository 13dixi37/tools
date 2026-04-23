"""Grouping: byte-identical / same-audio / same-metadata.

Exposes a streaming orchestrator `group_all` that yields finished groups via
a callback so the reviewer can start before the full library is grouped.
"""
from __future__ import annotations

import concurrent.futures as cf
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Callable, Iterable, Optional

from . import hashing
from .track import Track

try:
    from rapidfuzz import fuzz as _rf_fuzz  # type: ignore[import-not-found]

    HAS_RAPIDFUZZ = True
except ImportError:
    _rf_fuzz = None  # type: ignore[assignment]
    HAS_RAPIDFUZZ = False


GroupCallback = Callable[[str, list[Track]], None]
ProgressCallback = Callable[[str, int, int], None]  # stage, done, total

_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]")
_VERSION_TAG_RE = re.compile(r"[\(\[\{][^\)\]\}]*[\)\]\}]")


def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def normalize_title(s: str) -> str:
    if not s:
        return ""
    s = _VERSION_TAG_RE.sub(" ", s)
    return normalize_text(s)


def _sort_group(g: list[Track]) -> list[Track]:
    return sorted(g, key=lambda t: (-t.quality_score, t.path))


def pick_best(group: list[Track]) -> int:
    best = 0
    for i, t in enumerate(group):
        if t.quality_score > group[best].quality_score:
            best = i
    return best


# --- exact / byte-identical ---------------------------------------------------

def _hash_batch(
    paths: list[str],
    algo: str,
    workers: int,
    worker_fn: Callable[[tuple[str, str]], tuple[str, str]],
    progress: Optional[ProgressCallback] = None,
    stage: str = "hash",
) -> dict[str, str]:
    """Run `worker_fn` over `paths` in a process pool. Returns {path: digest}."""
    if not paths:
        return {}
    results: dict[str, str] = {}
    workers = max(1, workers)
    if workers == 1:
        total = len(paths)
        for i, p in enumerate(paths, 1):
            _, digest = worker_fn((p, algo))
            results[p] = digest
            if progress:
                progress(stage, i, total)
        return results

    # ProcessPoolExecutor gives us GIL-free hashing.
    args = [(p, algo) for p in paths]
    chunksize = max(1, len(args) // (workers * 4))
    total = len(args)
    done = 0
    with cf.ProcessPoolExecutor(max_workers=workers) as ex:
        for path, digest in ex.map(worker_fn, args, chunksize=chunksize):
            results[path] = digest
            done += 1
            if progress:
                progress(stage, done, total)
    return results


def group_exact(
    tracks: list[Track],
    *,
    algo: str,
    workers: int = 4,
    progress: Optional[ProgressCallback] = None,
    on_group: Optional[GroupCallback] = None,
) -> tuple[list[list[Track]], set[str]]:
    """Group files with identical content.

    Pipeline:
      1. Bucket by size. Discard buckets of 1.
      2. Compute partial hash (first+last 1 MiB) for survivors in a process
         pool. Re-bucket by (size, partial_hash); drop singletons.
      3. Compute full hash for survivors; re-bucket by full_hash.
    """
    by_size: dict[int, list[Track]] = defaultdict(list)
    for t in tracks:
        if not t.corrupted:
            by_size[t.size].append(t)

    size_survivors: list[Track] = []
    for ts in by_size.values():
        if len(ts) > 1:
            size_survivors.extend(ts)

    if not size_survivors:
        return [], set()

    # Partial hash pass.
    need_partial = [
        t for t in size_survivors
        if not (t.partial_hash and t.hash_algo == algo)
    ]
    if need_partial:
        digests = _hash_batch(
            [t.path for t in need_partial],
            algo,
            workers,
            hashing._partial_worker,
            progress,
            stage="partial",
        )
        for t in need_partial:
            d = digests.get(t.path, "")
            if d:
                t.partial_hash = d
                t.hash_algo = algo

    by_partial: dict[tuple[int, str], list[Track]] = defaultdict(list)
    for t in size_survivors:
        if t.partial_hash and t.hash_algo == algo:
            by_partial[(t.size, t.partial_hash)].append(t)

    partial_survivors: list[Track] = []
    for ts in by_partial.values():
        if len(ts) > 1:
            partial_survivors.extend(ts)

    if not partial_survivors:
        return [], set()

    # Full hash pass.
    need_full = [
        t for t in partial_survivors
        if not (t.content_hash and t.hash_algo == algo)
    ]
    if need_full:
        digests = _hash_batch(
            [t.path for t in need_full],
            algo,
            workers,
            hashing._full_worker,
            progress,
            stage="full",
        )
        for t in need_full:
            d = digests.get(t.path, "")
            if d:
                t.content_hash = d
                t.hash_algo = algo

    by_hash: dict[str, list[Track]] = defaultdict(list)
    for t in partial_survivors:
        if t.content_hash and t.hash_algo == algo:
            by_hash[t.content_hash].append(t)

    groups: list[list[Track]] = []
    for g in by_hash.values():
        if len(g) > 1:
            sorted_g = _sort_group(g)
            groups.append(sorted_g)
            if on_group:
                on_group("identical", sorted_g)

    groups.sort(key=lambda g: (-len(g), g[0].path))
    claimed = {t.path for g in groups for t in g}
    return groups, claimed


# --- audio fingerprint --------------------------------------------------------

def group_audio(
    tracks: list[Track],
    claimed: set[str],
    *,
    on_group: Optional[GroupCallback] = None,
) -> tuple[list[list[Track]], set[str]]:
    by_fp: dict[str, list[Track]] = defaultdict(list)
    for t in tracks:
        if t.path in claimed or t.corrupted or not t.fingerprint:
            continue
        by_fp[t.fingerprint].append(t)

    groups: list[list[Track]] = []
    for g in by_fp.values():
        if len(g) > 1:
            sorted_g = _sort_group(g)
            groups.append(sorted_g)
            if on_group:
                on_group("audio", sorted_g)

    used = {t.path for g in groups for t in g}
    by_prefix: dict[tuple[int, str], list[Track]] = defaultdict(list)
    for t in tracks:
        if t.path in claimed or t.path in used or t.corrupted or not t.fingerprint:
            continue
        by_prefix[(round(t.duration), t.fingerprint[:80])].append(t)
    for g in by_prefix.values():
        if len(g) > 1:
            sorted_g = _sort_group(g)
            groups.append(sorted_g)
            if on_group:
                on_group("audio", sorted_g)

    groups.sort(key=lambda g: (-len(g), g[0].path))
    new_claimed = {t.path for g in groups for t in g}
    return groups, claimed | new_claimed


# --- metadata + fuzzy ---------------------------------------------------------

def _fuzzy_key(t: Track) -> str:
    a = normalize_text(t.artist)
    ti = normalize_title(t.title)
    if a and ti:
        return f"{a}\t{ti}"
    stem = Path(t.path).stem
    return normalize_title(stem)


def group_metadata(
    tracks: list[Track],
    claimed: set[str],
    *,
    fuzzy: bool = True,
    fuzzy_threshold: float = 92.0,
    on_group: Optional[GroupCallback] = None,
) -> list[list[Track]]:
    by_meta: dict[tuple, list[Track]] = defaultdict(list)
    singletons: list[Track] = []
    for t in tracks:
        if t.path in claimed or t.corrupted:
            continue
        a = normalize_text(t.artist)
        ti = normalize_title(t.title)
        if not a or not ti:
            stem = Path(t.path).stem
            key: tuple = ("file", normalize_title(stem))
            if not key[1]:
                continue
        else:
            key = (a, ti)
        by_meta[key].append(t)

    groups: list[list[Track]] = []
    for g in by_meta.values():
        if len(g) > 1:
            sorted_g = _sort_group(g)
            groups.append(sorted_g)
            if on_group:
                on_group("meta", sorted_g)
        else:
            singletons.extend(g)

    if fuzzy and HAS_RAPIDFUZZ and len(singletons) > 1:
        fuzzy_groups = _fuzzy_merge(singletons, fuzzy_threshold)
        for g in fuzzy_groups:
            groups.append(g)
            if on_group:
                on_group("meta", g)

    groups.sort(key=lambda g: (-len(g), g[0].path))
    return groups


def _fuzzy_merge(singletons: list[Track], threshold: float) -> list[list[Track]]:
    """Union-find over singletons whose fuzzy keys are >= threshold similar.

    Uses rapidfuzz.fuzz.ratio on the combined normalized "artist\\ttitle"
    key, constrained to pairs within 2 seconds of duration (so similarly-
    titled but different tracks don't accidentally merge).
    """
    keys = [_fuzzy_key(t) for t in singletons]
    durs = [t.duration for t in singletons]
    n = len(singletons)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        if not keys[i]:
            continue
        for j in range(i + 1, n):
            if not keys[j]:
                continue
            if durs[i] and durs[j] and abs(durs[i] - durs[j]) > 2.0:
                continue
            score = _rf_fuzz.ratio(keys[i], keys[j])
            if score >= threshold:
                union(i, j)

    buckets: dict[int, list[Track]] = defaultdict(list)
    for i, t in enumerate(singletons):
        buckets[find(i)].append(t)
    return [_sort_group(g) for g in buckets.values() if len(g) > 1]


# --- streaming orchestrator ---------------------------------------------------

def group_all(
    tracks: list[Track],
    *,
    skip: Iterable[str] = (),
    algo: str,
    hash_workers: int = 4,
    fuzzy: bool = True,
    has_fpcalc: bool = True,
    progress: Optional[ProgressCallback] = None,
    on_group: Optional[GroupCallback] = None,
) -> list[tuple[str, list[Track]]]:
    """Run the three grouping stages, emitting each finished group via
    `on_group(kind, group)` as it's produced."""
    skip = set(skip)
    all_groups: list[tuple[str, list[Track]]] = []
    claimed: set[str] = set()

    def emit(kind: str, group: list[Track]) -> None:
        all_groups.append((kind, group))
        if on_group:
            on_group(kind, group)

    if "identical" not in skip:
        gs, claimed = group_exact(
            tracks, algo=algo, workers=hash_workers,
            progress=progress, on_group=emit,
        )
    if "audio" not in skip and has_fpcalc:
        _gs, claimed = group_audio(tracks, claimed, on_group=emit)
    if "meta" not in skip:
        group_metadata(tracks, claimed, fuzzy=fuzzy, on_group=emit)
    return all_groups
