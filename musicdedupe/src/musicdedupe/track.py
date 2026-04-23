from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(slots=True)
class Track:
    path: str
    size: int = 0
    mtime: float = 0.0
    ext: str = ""
    duration: float = 0.0
    bitrate: int = 0          # kbps
    sample_rate: int = 0      # Hz
    channels: int = 0
    codec: str = ""
    lossless: bool = False
    artist: str = ""
    album: str = ""
    title: str = ""
    track_no: str = ""
    year: str = ""
    content_hash: str = ""    # full-file hash, only computed when needed
    partial_hash: str = ""    # first 1 MiB + last 1 MiB
    hash_algo: str = ""       # "blake3" | "xxhash" | "sha1"
    fingerprint: str = ""     # chromaprint
    fp_duration: float = 0.0
    corrupted: bool = False
    error: str = ""

    @property
    def quality_score(self) -> int:
        """Higher == better. Used to suggest which copy to keep."""
        if self.corrupted:
            return -1_000_000
        score = 0
        if self.lossless:
            score += 10_000
        score += max(0, self.bitrate)
        meta_hits = sum(
            1 for f in (self.artist, self.album, self.title, self.track_no, self.year) if f
        )
        score += meta_hits * 10
        if self.sample_rate >= 96_000:
            score += 200
        elif self.sample_rate >= 48_000:
            score += 50
        return score

    @property
    def display_name(self) -> str:
        return os.path.basename(self.path)
