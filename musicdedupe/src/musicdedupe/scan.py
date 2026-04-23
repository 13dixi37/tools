"""File discovery and single-file probing (ffprobe + mutagen + fpcalc)."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import mutagen  # type: ignore[import-untyped]
except ImportError:
    sys.stderr.write(
        "Error: 'mutagen' is required.\n"
        "Install with:  pip install mutagen\n"
    )
    raise

from .track import Track


AUDIO_EXTS = {
    ".mp3", ".flac", ".m4a", ".mp4", ".aac", ".ogg", ".oga",
    ".opus", ".wav", ".wma", ".aiff", ".aif", ".ape", ".wv",
}
LOSSLESS_EXTS = {".flac", ".wav", ".aiff", ".aif", ".ape", ".wv"}

HAS_FFPROBE = shutil.which("ffprobe") is not None
HAS_FFPLAY = shutil.which("ffplay") is not None
HAS_FPCALC = shutil.which("fpcalc") is not None


def _run(cmd: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def find_audio_files(root: str, follow_symlinks: bool = False) -> list[str]:
    root = os.path.abspath(root)
    out = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fn in filenames:
            if fn.startswith("."):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext in AUDIO_EXTS:
                out.append(os.path.join(dirpath, fn))
    out.sort()
    return out


def probe_with_ffprobe(path: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "duration": 0.0, "bitrate": 0, "sample_rate": 0,
        "channels": 0, "codec": "", "corrupted": False, "error": "",
    }
    if not HAS_FFPROBE:
        return out
    cmd = [
        "ffprobe", "-v", "error", "-print_format", "json",
        "-show_format", "-show_streams", "-select_streams", "a:0", path,
    ]
    try:
        cp = _run(cmd, timeout=30)
    except subprocess.TimeoutExpired:
        out["corrupted"] = True
        out["error"] = "ffprobe timeout"
        return out
    if cp.returncode != 0:
        out["corrupted"] = True
        out["error"] = (cp.stderr.decode("utf-8", "replace").strip().splitlines() or ["ffprobe failed"])[-1]
        return out
    try:
        data = json.loads(cp.stdout)
    except json.JSONDecodeError:
        out["corrupted"] = True
        out["error"] = "ffprobe returned invalid JSON"
        return out
    fmt = data.get("format", {}) or {}
    streams = data.get("streams") or []
    if not streams:
        out["corrupted"] = True
        out["error"] = "no audio stream"
        return out
    st = streams[0]
    try:
        out["duration"] = float(fmt.get("duration") or st.get("duration") or 0.0)
    except (TypeError, ValueError):
        out["duration"] = 0.0
    try:
        out["bitrate"] = int(int(fmt.get("bit_rate") or st.get("bit_rate") or 0) / 1000)
    except (TypeError, ValueError):
        out["bitrate"] = 0
    try:
        out["sample_rate"] = int(st.get("sample_rate") or 0)
    except (TypeError, ValueError):
        out["sample_rate"] = 0
    try:
        out["channels"] = int(st.get("channels") or 0)
    except (TypeError, ValueError):
        out["channels"] = 0
    out["codec"] = (st.get("codec_name") or "").lower()
    if out["duration"] <= 0.1:
        out["corrupted"] = True
        out["error"] = "zero-length audio"
    return out


def probe_with_mutagen(path: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "duration": 0.0, "bitrate": 0, "sample_rate": 0, "channels": 0,
        "artist": "", "album": "", "title": "", "track_no": "", "year": "",
        "corrupted": False, "error": "",
    }
    try:
        mf = mutagen.File(path)
    except Exception as e:  # noqa: BLE001
        out["corrupted"] = True
        out["error"] = f"mutagen: {e}"
        return out
    if mf is None:
        out["corrupted"] = True
        out["error"] = "unknown format"
        return out
    info = getattr(mf, "info", None)
    if info is not None:
        out["duration"] = float(getattr(info, "length", 0.0) or 0.0)
        br = getattr(info, "bitrate", 0) or 0
        out["bitrate"] = int(br / 1000) if br > 10_000 else int(br)
        out["sample_rate"] = int(getattr(info, "sample_rate", 0) or 0)
        out["channels"] = int(getattr(info, "channels", 0) or 0)

    def first(keys: list[str]) -> str:
        if not mf.tags:
            return ""
        for k in keys:
            try:
                v = mf.tags.get(k)
            except (KeyError, ValueError, TypeError):
                continue
            if v:
                if isinstance(v, list):
                    v = v[0] if v else ""
                return str(v).strip()
        return ""

    if mf.tags:
        out["artist"] = first(["artist", "TPE1", "\xa9ART", "albumartist", "TPE2", "aART"])
        out["album"] = first(["album", "TALB", "\xa9alb"])
        out["title"] = first(["title", "TIT2", "\xa9nam"])
        out["track_no"] = first(["tracknumber", "TRCK", "trkn"])
        out["year"] = first(["date", "TDRC", "year", "\xa9day"])
    return out


def compute_fingerprint(path: str, length: int = 120) -> tuple[str, float]:
    if not HAS_FPCALC:
        return "", 0.0
    try:
        cp = _run(["fpcalc", "-length", str(length), path], timeout=60)
    except subprocess.TimeoutExpired:
        return "", 0.0
    if cp.returncode != 0:
        return "", 0.0
    fp, dur = "", 0.0
    for line in cp.stdout.decode("utf-8", "replace").splitlines():
        if line.startswith("FINGERPRINT="):
            fp = line[len("FINGERPRINT="):].strip()
        elif line.startswith("DURATION="):
            try:
                dur = float(line[len("DURATION="):].strip())
            except ValueError:
                pass
    return fp, dur


def scan_file(path: str) -> Track:
    try:
        st = os.stat(path)
    except OSError as e:
        return Track(path=path, ext=Path(path).suffix.lower(), corrupted=True, error=str(e))
    ext = Path(path).suffix.lower()
    t = Track(
        path=path,
        size=st.st_size,
        mtime=st.st_mtime,
        ext=ext,
        lossless=(ext in LOSSLESS_EXTS),
    )

    if HAS_FFPROBE:
        p = probe_with_ffprobe(path)
        t.duration = p["duration"]
        t.bitrate = p["bitrate"]
        t.sample_rate = p["sample_rate"]
        t.channels = p["channels"]
        t.codec = p["codec"]
        if p["corrupted"]:
            t.corrupted = True
            t.error = p["error"]

    m = probe_with_mutagen(path)
    if not t.duration:
        t.duration = m["duration"]
    if not t.bitrate:
        t.bitrate = m["bitrate"]
    if not t.sample_rate:
        t.sample_rate = m["sample_rate"]
    if not t.channels:
        t.channels = m["channels"]
    t.artist = m["artist"]
    t.album = m["album"]
    t.title = m["title"]
    t.track_no = m["track_no"]
    t.year = m["year"]
    if m["corrupted"] and not t.corrupted:
        if not HAS_FFPROBE:
            t.corrupted = True
            t.error = m["error"]

    if not t.corrupted and HAS_FPCALC:
        fp, fpdur = compute_fingerprint(path)
        t.fingerprint = fp
        t.fp_duration = fpdur

    return t
