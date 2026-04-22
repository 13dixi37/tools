#!/usr/bin/env python3
"""
musicdedupe - Interactive CLI to find and clean up duplicate music files.

Detection strategies (run in order, strongest first):
  1. Identical bytes      -> SHA1 hash of file content
  2. Identical audio      -> Chromaprint fingerprint (same song, any format/bitrate)
  3. Same song candidate  -> Normalized artist + title (catches remixes/versions
                             for manual review)

Also flags files that fail to decode (likely corrupted).

Required:
    Python 3.8+
    mutagen             (pip install mutagen)

Recommended (tool degrades gracefully if missing):
    ffmpeg + ffprobe    (for accurate duration/bitrate + playback)
    fpcalc              (chromaprint; for audio fingerprinting)
    rich                (pip install rich; nicer output)
    send2trash          (pip install send2trash; safe deletion)

Install system tools:
    macOS:        brew install ffmpeg chromaprint
    Debian/Ubu:   sudo apt install ffmpeg libchromaprint-tools
    Arch:         sudo pacman -S ffmpeg chromaprint
    Fedora:       sudo dnf install ffmpeg chromaprint-tools
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import (
        Progress, SpinnerColumn, BarColumn, TextColumn,
        TimeRemainingColumn, MofNCompleteColumn,
    )
    from rich.panel import Panel
    from rich.text import Text
    HAS_RICH = True
    console = Console()
except ImportError:
    HAS_RICH = False
    console = None

try:
    from send2trash import send2trash
    HAS_TRASH = True
except ImportError:
    HAS_TRASH = False

try:
    import mutagen
except ImportError:
    sys.stderr.write(
        "Error: 'mutagen' is required.\n"
        "Install with:  pip install mutagen\n"
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AUDIO_EXTS = {
    '.mp3', '.flac', '.m4a', '.mp4', '.aac', '.ogg', '.oga',
    '.opus', '.wav', '.wma', '.aiff', '.aif', '.ape', '.wv',
}
LOSSLESS_EXTS = {'.flac', '.wav', '.aiff', '.aif', '.ape', '.wv'}

HAS_FFPROBE = shutil.which('ffprobe') is not None
HAS_FFPLAY = shutil.which('ffplay') is not None
HAS_FPCALC = shutil.which('fpcalc') is not None


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
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
    content_hash: str = ""    # SHA1, only computed when needed
    fingerprint: str = ""     # chromaprint
    fp_duration: float = 0.0
    corrupted: bool = False
    error: str = ""

    # -- scoring --------------------------------------------------------

    @property
    def quality_score(self) -> int:
        """Higher == better. Used to suggest which copy to keep."""
        if self.corrupted:
            return -1_000_000
        score = 0
        if self.lossless:
            score += 10_000
        score += max(0, self.bitrate)           # 0-2000ish
        # Complete metadata is a plus
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


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r'\s+')
_PUNCT_RE = re.compile(r'[^\w\s]')

# Bracketed tags we strip for fuzzy compare (so "Song (Radio Edit)" and "Song"
# end up as separate fuzzy candidates, but "Song [2012 Remaster]" and
# "Song [Remaster]" collapse to the same key).
_VERSION_TAG_RE = re.compile(r'[\(\[\{][^\)\]\}]*[\)\]\}]')


def normalize_text(s: str) -> str:
    """Normalize a string for fuzzy comparison."""
    if not s:
        return ""
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = _PUNCT_RE.sub(' ', s)
    s = _WS_RE.sub(' ', s).strip()
    return s


def normalize_title(s: str) -> str:
    """Strip (feat...), (Remix), etc. for a looser title match."""
    if not s:
        return ""
    s = _VERSION_TAG_RE.sub(' ', s)
    return normalize_text(s)


def human_size(n: int) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if n < 1024:
            return f"{n:,.1f} {unit}" if unit != 'B' else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"


def human_duration(s: float) -> str:
    if not s or s <= 0:
        return "  ?:??"
    m, s = divmod(int(s), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m:>3d}:{s:02d}"


def human_bitrate(kbps: int, lossless: bool) -> str:
    if not kbps:
        return "   ?"
    tag = "lossless" if lossless else "kbps"
    return f"{kbps:>4d} {tag}"


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------

def _run(cmd: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def probe_with_ffprobe(path: str) -> dict:
    """Return dict(duration, bitrate, sample_rate, channels, codec, corrupted, error)."""
    out = {
        'duration': 0.0, 'bitrate': 0, 'sample_rate': 0,
        'channels': 0, 'codec': '', 'corrupted': False, 'error': '',
    }
    if not HAS_FFPROBE:
        return out
    cmd = [
        'ffprobe', '-v', 'error', '-print_format', 'json',
        '-show_format', '-show_streams', '-select_streams', 'a:0', path,
    ]
    try:
        cp = _run(cmd, timeout=30)
    except subprocess.TimeoutExpired:
        out['corrupted'] = True
        out['error'] = 'ffprobe timeout'
        return out
    if cp.returncode != 0:
        out['corrupted'] = True
        out['error'] = (cp.stderr.decode('utf-8', 'replace').strip().splitlines() or ['ffprobe failed'])[-1]
        return out
    try:
        data = json.loads(cp.stdout)
    except json.JSONDecodeError:
        out['corrupted'] = True
        out['error'] = 'ffprobe returned invalid JSON'
        return out
    fmt = data.get('format', {}) or {}
    streams = data.get('streams') or []
    if not streams:
        out['corrupted'] = True
        out['error'] = 'no audio stream'
        return out
    st = streams[0]
    try:
        out['duration'] = float(fmt.get('duration') or st.get('duration') or 0.0)
    except (TypeError, ValueError):
        out['duration'] = 0.0
    try:
        out['bitrate'] = int(int(fmt.get('bit_rate') or st.get('bit_rate') or 0) / 1000)
    except (TypeError, ValueError):
        out['bitrate'] = 0
    try:
        out['sample_rate'] = int(st.get('sample_rate') or 0)
    except (TypeError, ValueError):
        out['sample_rate'] = 0
    try:
        out['channels'] = int(st.get('channels') or 0)
    except (TypeError, ValueError):
        out['channels'] = 0
    out['codec'] = (st.get('codec_name') or '').lower()
    if out['duration'] <= 0.1:
        out['corrupted'] = True
        out['error'] = 'zero-length audio'
    return out


def probe_with_mutagen(path: str) -> dict:
    """Fallback/augment via mutagen (also used for tags)."""
    out = {
        'duration': 0.0, 'bitrate': 0, 'sample_rate': 0, 'channels': 0,
        'artist': '', 'album': '', 'title': '', 'track_no': '', 'year': '',
        'corrupted': False, 'error': '',
    }
    try:
        mf = mutagen.File(path)
    except Exception as e:  # noqa: BLE001
        out['corrupted'] = True
        out['error'] = f'mutagen: {e}'
        return out
    if mf is None:
        out['corrupted'] = True
        out['error'] = 'unknown format'
        return out
    info = getattr(mf, 'info', None)
    if info is not None:
        out['duration'] = float(getattr(info, 'length', 0.0) or 0.0)
        br = getattr(info, 'bitrate', 0) or 0
        out['bitrate'] = int(br / 1000) if br > 10_000 else int(br)  # some report bps, some kbps
        out['sample_rate'] = int(getattr(info, 'sample_rate', 0) or 0)
        out['channels'] = int(getattr(info, 'channels', 0) or 0)

    def first(keys):
        if not mf.tags:
            return ''
        for k in keys:
            # Some backends (Vorbis) raise ValueError on non-ASCII keys;
            # others (MP4) use those keys exclusively. Just try each.
            try:
                v = mf.tags.get(k)
            except (KeyError, ValueError, TypeError):
                continue
            if v:
                if isinstance(v, list):
                    v = v[0] if v else ''
                return str(v).strip()
        return ''

    if mf.tags:
        # EasyID3 / Vorbis / MP4 tag names all differ; try a few.
        out['artist'] = first(['artist', 'TPE1', '\xa9ART', 'albumartist', 'TPE2', 'aART'])
        out['album'] = first(['album', 'TALB', '\xa9alb'])
        out['title'] = first(['title', 'TIT2', '\xa9nam'])
        out['track_no'] = first(['tracknumber', 'TRCK', 'trkn'])
        out['year'] = first(['date', 'TDRC', 'year', '\xa9day'])
    return out


def compute_fingerprint(path: str, length: int = 120) -> tuple[str, float]:
    """Return (fingerprint, duration) from fpcalc. Empty on failure."""
    if not HAS_FPCALC:
        return '', 0.0
    try:
        cp = _run(['fpcalc', '-length', str(length), path], timeout=60)
    except subprocess.TimeoutExpired:
        return '', 0.0
    if cp.returncode != 0:
        return '', 0.0
    fp, dur = '', 0.0
    for line in cp.stdout.decode('utf-8', 'replace').splitlines():
        if line.startswith('FINGERPRINT='):
            fp = line[len('FINGERPRINT='):].strip()
        elif line.startswith('DURATION='):
            try:
                dur = float(line[len('DURATION='):].strip())
            except ValueError:
                pass
    return fp, dur


def compute_sha1(path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha1()
    try:
        with open(path, 'rb') as f:
            while True:
                b = f.read(chunk)
                if not b:
                    break
                h.update(b)
    except OSError:
        return ''
    return h.hexdigest()


def scan_file(path: str) -> Track:
    try:
        st = os.stat(path)
    except OSError as e:
        return Track(path=path, ext=Path(path).suffix.lower(), corrupted=True, error=str(e))
    ext = Path(path).suffix.lower()
    t = Track(path=path, size=st.st_size, mtime=st.st_mtime, ext=ext,
              lossless=(ext in LOSSLESS_EXTS))

    # Primary probe (ffprobe if available).
    if HAS_FFPROBE:
        p = probe_with_ffprobe(path)
        t.duration = p['duration']
        t.bitrate = p['bitrate']
        t.sample_rate = p['sample_rate']
        t.channels = p['channels']
        t.codec = p['codec']
        if p['corrupted']:
            t.corrupted = True
            t.error = p['error']

    # Always pull tags via mutagen, and fill missing audio info.
    m = probe_with_mutagen(path)
    if not t.duration:
        t.duration = m['duration']
    if not t.bitrate:
        t.bitrate = m['bitrate']
    if not t.sample_rate:
        t.sample_rate = m['sample_rate']
    if not t.channels:
        t.channels = m['channels']
    t.artist = m['artist']
    t.album = m['album']
    t.title = m['title']
    t.track_no = m['track_no']
    t.year = m['year']
    if m['corrupted'] and not t.corrupted:
        # mutagen failed but ffprobe succeeded => not corrupted
        if not HAS_FFPROBE:
            t.corrupted = True
            t.error = m['error']

    # Fingerprint (skip corrupted).
    if not t.corrupted and HAS_FPCALC:
        fp, fpdur = compute_fingerprint(path)
        t.fingerprint = fp
        t.fp_duration = fpdur

    return t


# ---------------------------------------------------------------------------
# Library scan orchestration (+ cache)
# ---------------------------------------------------------------------------

def find_audio_files(root: str, follow_symlinks: bool = False) -> list[str]:
    root = os.path.abspath(root)
    out = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=follow_symlinks):
        # Skip hidden directories (.git, .Trash, etc.)
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        for fn in filenames:
            if fn.startswith('.'):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext in AUDIO_EXTS:
                out.append(os.path.join(dirpath, fn))
    out.sort()
    return out


def load_cache(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def save_cache(path: str, cache: dict) -> None:
    if not path:
        return
    try:
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(cache, f)
        os.replace(tmp, path)
    except OSError:
        pass


def scan_library(files: list[str], cache_path: Optional[str] = None,
                 workers: int = 4) -> list[Track]:
    cache = load_cache(cache_path) if cache_path else {}
    tracks: list[Track] = []
    todo: list[str] = []

    # Reuse cache entries whose size+mtime still match the file on disk.
    for p in files:
        try:
            st = os.stat(p)
        except OSError:
            todo.append(p)
            continue
        c = cache.get(p)
        if c and c.get('size') == st.st_size and c.get('mtime') == st.st_mtime:
            try:
                tracks.append(Track(**c))
                continue
            except TypeError:
                pass  # schema changed, rescan
        todo.append(p)

    cached_n = len(tracks)
    if cached_n:
        msg = f"Using cached data for {cached_n} file(s)."
        console.print(msg, style="dim") if console else print(msg)

    if not todo:
        return tracks

    # Parallel scan of the rest.
    if HAS_RICH:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold]Scanning[/bold]"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            transient=False,
            console=console,
        ) as progress:
            task = progress.add_task("scan", total=len(todo))
            with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
                futs = {ex.submit(scan_file, p): p for p in todo}
                for fut in cf.as_completed(futs):
                    try:
                        t = fut.result()
                    except Exception as e:  # noqa: BLE001
                        p = futs[fut]
                        t = Track(path=p, ext=Path(p).suffix.lower(),
                                  corrupted=True, error=f'scan error: {e}')
                    tracks.append(t)
                    cache[t.path] = asdict(t)
                    progress.advance(task)
                    # Periodic cache flush so long scans are resumable.
                    if len(tracks) % 200 == 0 and cache_path:
                        save_cache(cache_path, cache)
    else:
        total = len(todo)
        last = 0.0
        done = 0
        with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futs = {ex.submit(scan_file, p): p for p in todo}
            for fut in cf.as_completed(futs):
                try:
                    t = fut.result()
                except Exception as e:  # noqa: BLE001
                    p = futs[fut]
                    t = Track(path=p, ext=Path(p).suffix.lower(),
                              corrupted=True, error=f'scan error: {e}')
                tracks.append(t)
                cache[t.path] = asdict(t)
                done += 1
                now = time.time()
                if now - last > 1.0 or done == total:
                    print(f"Scanning... {done}/{total}", end='\r', flush=True)
                    last = now
                if done % 200 == 0 and cache_path:
                    save_cache(cache_path, cache)
        print()

    if cache_path:
        save_cache(cache_path, cache)
    return tracks


# ---------------------------------------------------------------------------
# Grouping: identical / audio-match / version-candidates
# ---------------------------------------------------------------------------

def _sort_group(g: list[Track]) -> list[Track]:
    """Stable order: best quality first, tie-break on path."""
    return sorted(g, key=lambda t: (-t.quality_score, t.path))


def group_exact(tracks: list[Track]) -> tuple[list[list[Track]], set[str]]:
    """Group files with identical content (SHA1 over size-collision groups)."""
    # Only hash files whose size collides with another file.
    by_size: dict[int, list[Track]] = defaultdict(list)
    for t in tracks:
        if not t.corrupted:
            by_size[t.size].append(t)
    to_hash = [t for ts in by_size.values() if len(ts) > 1 for t in ts if not t.content_hash]

    if to_hash:
        if HAS_RICH:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold]Hashing collisions[/bold]"),
                BarColumn(bar_width=None),
                MofNCompleteColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("hash", total=len(to_hash))
                with cf.ThreadPoolExecutor(max_workers=4) as ex:
                    futs = {ex.submit(compute_sha1, t.path): t for t in to_hash}
                    for fut in cf.as_completed(futs):
                        t = futs[fut]
                        try:
                            t.content_hash = fut.result() or ''
                        except Exception:  # noqa: BLE001
                            t.content_hash = ''
                        progress.advance(task)
        else:
            for i, t in enumerate(to_hash, 1):
                t.content_hash = compute_sha1(t.path) or ''
                print(f"Hashing... {i}/{len(to_hash)}", end='\r', flush=True)
            print()

    by_hash: dict[str, list[Track]] = defaultdict(list)
    for ts in by_size.values():
        if len(ts) < 2:
            continue
        for t in ts:
            if t.content_hash:
                by_hash[t.content_hash].append(t)

    groups = [_sort_group(g) for g in by_hash.values() if len(g) > 1]
    groups.sort(key=lambda g: (-len(g), g[0].path))
    claimed = {t.path for g in groups for t in g}
    return groups, claimed


def group_audio(tracks: list[Track], claimed: set[str]) -> tuple[list[list[Track]], set[str]]:
    """Group by chromaprint fingerprint (same audio, any encoding)."""
    # Key on full fingerprint (deterministic across encodings for same source).
    # Fallback: first 80 chars + duration rounded, to catch minor fp drift.
    by_fp: dict[str, list[Track]] = defaultdict(list)
    for t in tracks:
        if t.path in claimed or t.corrupted or not t.fingerprint:
            continue
        key = t.fingerprint
        by_fp[key].append(t)

    groups = [_sort_group(g) for g in by_fp.values() if len(g) > 1]

    # Second pass: merge near-matches (same fp prefix + same rounded duration).
    used_paths = {t.path for g in groups for t in g}
    by_prefix: dict[tuple, list[Track]] = defaultdict(list)
    for t in tracks:
        if t.path in claimed or t.path in used_paths or t.corrupted or not t.fingerprint:
            continue
        key = (round(t.duration), t.fingerprint[:80])
        by_prefix[key].append(t)
    for g in by_prefix.values():
        if len(g) > 1:
            groups.append(_sort_group(g))

    groups.sort(key=lambda g: (-len(g), g[0].path))
    new_claimed = {t.path for g in groups for t in g}
    return groups, claimed | new_claimed


def group_metadata(tracks: list[Track], claimed: set[str]) -> list[list[Track]]:
    """Group by normalized artist + title (catches remixes/versions for review)."""
    by_meta: dict[tuple, list[Track]] = defaultdict(list)
    for t in tracks:
        if t.path in claimed or t.corrupted:
            continue
        a = normalize_text(t.artist)
        ti = normalize_title(t.title)
        if not a or not ti:
            # Fall back to filename-based normalization.
            stem = Path(t.path).stem
            key = ('file', normalize_title(stem))
            if not key[1]:
                continue
        else:
            key = (a, ti)
        by_meta[key].append(t)
    groups = [_sort_group(g) for g in by_meta.values() if len(g) > 1]
    groups.sort(key=lambda g: (-len(g), g[0].path))
    return groups


# ---------------------------------------------------------------------------
# Presentation
# ---------------------------------------------------------------------------

GROUP_LABELS = {
    'identical': 'Byte-identical copies',
    'audio': 'Same audio content (different encoding/bitrate/format)',
    'meta': 'Same artist & title (possibly different versions/remixes)',
}


def pick_best(group: list[Track]) -> int:
    """Index of the track with the highest quality score."""
    best = 0
    for i, t in enumerate(group):
        if t.quality_score > group[best].quality_score:
            best = i
    return best


def render_group(kind: str, group: list[Track], idx: int, total: int,
                 marks: dict[int, str]) -> None:
    """Print one group of candidates.

    marks: {local_idx: 'keep' | 'delete'}
    """
    best = pick_best(group)
    header = f"Group {idx}/{total}  —  {GROUP_LABELS[kind]}  ({len(group)} files)"

    if HAS_RICH:
        console.rule(f"[bold cyan]{header}[/bold cyan]")
        table = Table(show_header=True, header_style="bold", box=None, pad_edge=False)
        table.add_column("#", justify="right", style="dim", width=3)
        table.add_column("", width=3)                  # mark
        table.add_column("File", overflow="fold")
        table.add_column("Fmt", width=6)
        table.add_column("Bitrate", justify="right", width=12)
        table.add_column("SR", justify="right", width=7)
        table.add_column("Len", justify="right", width=8)
        table.add_column("Size", justify="right", width=10)
        for i, t in enumerate(group, 1):
            li = i - 1
            mark = ''
            style = ''
            if marks.get(li) == 'keep':
                mark, style = '✓', 'green'
            elif marks.get(li) == 'delete':
                mark, style = '✗', 'red'
            elif li == best:
                mark = '★'
            name = Text(t.display_name)
            if style:
                name.stylize(style)
            subline = Text('  └ ' + os.path.dirname(t.path), style='dim')
            table.add_row(
                str(i),
                Text(mark, style=style or ('yellow' if li == best else '')),
                Text.assemble(name, '\n', subline),
                t.ext.lstrip('.'),
                human_bitrate(t.bitrate, t.lossless),
                f"{t.sample_rate/1000:.1f}k" if t.sample_rate else "?",
                human_duration(t.duration),
                human_size(t.size),
            )
        console.print(table)
        # Metadata line (if any group member has tags)
        meta = next((t for t in group if t.artist or t.title), None)
        if meta:
            console.print(
                f"  [dim]tags:[/dim] "
                f"[bold]{meta.artist or '?'}[/bold] — "
                f"{meta.title or '?'}"
                + (f"  [dim]({meta.album})[/dim]" if meta.album else '')
            )
        console.print()
    else:
        print('=' * 72)
        print(header)
        print('-' * 72)
        for i, t in enumerate(group, 1):
            li = i - 1
            mark = ' '
            if marks.get(li) == 'keep':
                mark = '✓'
            elif marks.get(li) == 'delete':
                mark = '✗'
            elif li == best:
                mark = '★'
            print(f"[{i}] {mark} {t.display_name}")
            print(f"     {t.ext.lstrip('.')}  {human_bitrate(t.bitrate, t.lossless)}  "
                  f"{t.sample_rate}Hz  {human_duration(t.duration)}  {human_size(t.size)}")
            print(f"     {t.path}")
        print()


def print_info(t: Track) -> None:
    if HAS_RICH:
        table = Table(show_header=False, box=None)
        table.add_column(style="bold dim", justify="right")
        table.add_column()
        rows = [
            ("path", t.path),
            ("size", human_size(t.size)),
            ("codec", t.codec or t.ext.lstrip('.')),
            ("lossless", "yes" if t.lossless else "no"),
            ("bitrate", f"{t.bitrate} kbps" if t.bitrate else "?"),
            ("sample rate", f"{t.sample_rate} Hz" if t.sample_rate else "?"),
            ("channels", str(t.channels) if t.channels else "?"),
            ("duration", human_duration(t.duration)),
            ("artist", t.artist or "—"),
            ("album", t.album or "—"),
            ("title", t.title or "—"),
            ("track", t.track_no or "—"),
            ("year", t.year or "—"),
            ("fingerprint", (t.fingerprint[:48] + '…') if t.fingerprint else "—"),
            ("sha1", t.content_hash or "(not computed)"),
            ("quality score", str(t.quality_score)),
        ]
        if t.corrupted:
            rows.append(("ERROR", t.error))
        for k, v in rows:
            table.add_row(k, str(v))
        console.print(table)
    else:
        for k, v in asdict(t).items():
            print(f"  {k}: {v}")


# ---------------------------------------------------------------------------
# Audio preview
# ---------------------------------------------------------------------------

def play_snippet(path: str, start: int = 30, length: int = 15) -> None:
    """Play a short snippet using ffplay. Blocks until done or user hits 'q'."""
    if not HAS_FFPLAY:
        msg = "ffplay not found. Install ffmpeg to use the preview feature."
        console.print(f"[red]{msg}[/red]") if console else print(msg)
        return
    if not os.path.exists(path):
        msg = f"File not found: {path}"
        console.print(f"[red]{msg}[/red]") if console else print(msg)
        return

    # Clamp start if the file is shorter than `start` seconds.
    info_msg = f"▶ Playing {os.path.basename(path)} (from {start}s, {length}s)  — press q or Ctrl+C to stop"
    console.print(f"[cyan]{info_msg}[/cyan]") if console else print(info_msg)
    cmd = [
        'ffplay', '-nodisp', '-autoexit', '-hide_banner', '-loglevel', 'error',
        '-ss', str(start), '-t', str(length), path,
    ]
    try:
        subprocess.run(cmd, check=False)
    except KeyboardInterrupt:
        pass


# ---------------------------------------------------------------------------
# Interactive loop
# ---------------------------------------------------------------------------

HELP_TEXT = """\
Commands
  <n>          keep only file n (mark the rest for deletion)
  k <n>[..]    mark file(s) n to keep
  d <n>[..]    mark file(s) n to delete
  u <n>[..]    unmark file(s) n
  p <n>[..]    play snippet of file(s) n  (plays each in turn)
  i <n>        show full info for file n
  a            auto: keep the best-quality file, delete the rest
               (only offered for byte-identical / same-audio groups)
  n / s        next / skip this group without changes
  b            go back to previous group
  q            quit and review the deletion list
  ?            show this help
"""


def parse_indices(tokens: list[str], n: int) -> list[int]:
    out = []
    for tk in tokens:
        # Accept ranges like 1-3
        if '-' in tk and tk.count('-') == 1 and all(p.isdigit() for p in tk.split('-')):
            a, b = tk.split('-')
            for i in range(int(a), int(b) + 1):
                if 1 <= i <= n:
                    out.append(i - 1)
        elif tk.isdigit():
            i = int(tk)
            if 1 <= i <= n:
                out.append(i - 1)
    return out


def prompt(text: str) -> str:
    if HAS_RICH:
        try:
            return console.input(f"[bold]{text}[/bold] ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 'q'
    try:
        return input(text + ' ').strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return 'q'


def interactive_review(all_groups: list[tuple[str, list[Track]]],
                       play_start: int, play_length: int) -> list[Track]:
    """Walk through groups and collect files marked for deletion."""
    to_delete: list[Track] = []
    total = len(all_groups)
    gidx = 0

    # per-group state: {group_index: {local_track_idx: 'keep'|'delete'}}
    group_marks: dict[int, dict[int, str]] = defaultdict(dict)

    while 0 <= gidx < total:
        kind, group = all_groups[gidx]
        marks = group_marks[gidx]
        render_group(kind, group, gidx + 1, total, marks)

        cmd = prompt("›")
        if not cmd:
            continue
        cmd_l = cmd.lower()

        # Single bare number => keep that one, delete the rest
        if cmd.isdigit():
            idxs = parse_indices([cmd], len(group))
            if idxs:
                keep = idxs[0]
                marks.clear()
                for i in range(len(group)):
                    marks[i] = 'keep' if i == keep else 'delete'
                gidx += 1
            continue

        parts = cmd_l.split()
        head = parts[0]
        rest = parts[1:]

        if head in ('q', 'quit', 'exit'):
            break
        if head in ('?', 'h', 'help'):
            if HAS_RICH:
                console.print(Panel(HELP_TEXT, title="Commands", border_style="dim"))
            else:
                print(HELP_TEXT)
            continue
        if head in ('n', 'next', 's', 'skip'):
            gidx += 1
            continue
        if head in ('b', 'back', 'prev'):
            gidx = max(0, gidx - 1)
            continue
        if head == 'a':
            if kind == 'meta':
                msg = "Auto-pick is disabled for version/remix groups — choose manually."
                console.print(f"[yellow]{msg}[/yellow]") if console else print(msg)
                continue
            best = pick_best(group)
            marks.clear()
            for i in range(len(group)):
                marks[i] = 'keep' if i == best else 'delete'
            gidx += 1
            continue
        if head == 'i' and rest:
            idxs = parse_indices(rest, len(group))
            if idxs:
                print_info(group[idxs[0]])
            continue
        if head == 'p' and rest:
            for i in parse_indices(rest, len(group)):
                play_snippet(group[i].path, start=play_start, length=play_length)
            continue
        if head in ('k', 'keep') and rest:
            for i in parse_indices(rest, len(group)):
                marks[i] = 'keep'
            continue
        if head in ('d', 'delete', 'del', 'rm') and rest:
            for i in parse_indices(rest, len(group)):
                marks[i] = 'delete'
            continue
        if head in ('u', 'unmark') and rest:
            for i in parse_indices(rest, len(group)):
                marks.pop(i, None)
            continue

        msg = f"Unknown command: {cmd!r}. Type ? for help."
        console.print(f"[yellow]{msg}[/yellow]") if console else print(msg)

    # Collate deletions
    for gi, marks in group_marks.items():
        _, group = all_groups[gi]
        for li, action in marks.items():
            if action == 'delete':
                to_delete.append(group[li])
    return to_delete


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------

def do_delete(tracks: list[Track], mode: str, move_to: Optional[str] = None) -> int:
    """mode: 'trash' | 'remove' | 'move'"""
    n = 0
    for t in tracks:
        try:
            if mode == 'trash':
                if not HAS_TRASH:
                    raise RuntimeError("send2trash not installed")
                send2trash(t.path)
            elif mode == 'move':
                dest_dir = move_to or 'musicdedupe-removed'
                os.makedirs(dest_dir, exist_ok=True)
                base = os.path.basename(t.path)
                target = os.path.join(dest_dir, base)
                # Avoid collisions
                stem, ext = os.path.splitext(base)
                k = 1
                while os.path.exists(target):
                    target = os.path.join(dest_dir, f"{stem}__{k}{ext}")
                    k += 1
                shutil.move(t.path, target)
            else:  # 'remove'
                os.remove(t.path)
            n += 1
        except Exception as e:  # noqa: BLE001
            msg = f"FAILED to delete {t.path}: {e}"
            console.print(f"[red]{msg}[/red]") if console else print(msg)
    return n


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def show_corrupted(bad: list[Track]) -> list[Track]:
    """Offer to delete corrupted files. Returns list to delete."""
    if not bad:
        return []
    header = f"Found {len(bad)} file(s) that failed to decode (likely corrupted)"
    if HAS_RICH:
        console.rule(f"[bold red]{header}[/bold red]")
        table = Table(show_header=True, header_style="bold")
        table.add_column("#", justify="right", width=4)
        table.add_column("File", overflow="fold")
        table.add_column("Error", overflow="fold")
        for i, t in enumerate(bad, 1):
            table.add_row(str(i), t.path, t.error or '?')
        console.print(table)
    else:
        print(header)
        for i, t in enumerate(bad, 1):
            print(f"[{i}] {t.path}  ({t.error})")
    choice = prompt("Delete all corrupted? [y/N/select]")
    if choice.lower().startswith('y'):
        return list(bad)
    if choice.lower().startswith('s'):
        picks = prompt("Indices to delete (e.g. 1 3 5-7):")
        idxs = parse_indices(picks.split(), len(bad))
        return [bad[i] for i in idxs]
    return []


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog='musicdedupe',
        description='Interactive music library deduplicator.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument('path', help='Music directory to scan.')
    ap.add_argument('--cache', default=None,
                    help='Path to scan cache JSON (default: <path>/.musicdedupe-cache.json)')
    ap.add_argument('--no-cache', action='store_true', help='Disable scan cache.')
    ap.add_argument('--workers', type=int, default=4, help='Parallel scan workers (default: 4).')
    ap.add_argument('--play-start', type=int, default=30,
                    help='Preview start offset in seconds (default: 30).')
    ap.add_argument('--play-length', type=int, default=15,
                    help='Preview length in seconds (default: 15).')
    ap.add_argument('--delete-mode', choices=['trash', 'move', 'remove'], default=None,
                    help='How to delete. Default: trash if available, else move to ./musicdedupe-removed.')
    ap.add_argument('--move-to', default='musicdedupe-removed',
                    help='Destination dir when --delete-mode=move.')
    ap.add_argument('--skip', choices=['identical', 'audio', 'meta'], action='append', default=[],
                    help='Skip a category (can be repeated).')
    ap.add_argument('--dry-run', action='store_true',
                    help='Do not delete anything; just print the plan.')
    ap.add_argument('--follow-symlinks', action='store_true')
    args = ap.parse_args(argv)

    root = os.path.abspath(args.path)
    if not os.path.isdir(root):
        sys.stderr.write(f"Not a directory: {root}\n")
        return 2

    # Banner with available tools
    if HAS_RICH:
        console.rule("[bold]musicdedupe[/bold]")
        console.print(
            f"  scanning: [bold]{root}[/bold]\n"
            f"  ffprobe:  {'yes' if HAS_FFPROBE else '[red]missing[/red]  (duration/bitrate less accurate)'}\n"
            f"  fpcalc:   {'yes' if HAS_FPCALC else '[red]missing[/red]  (audio fingerprinting disabled)'}\n"
            f"  ffplay:   {'yes' if HAS_FFPLAY else '[red]missing[/red]  (preview disabled)'}\n"
            f"  trash:    {'yes' if HAS_TRASH else '[yellow]missing[/yellow] (will move to a folder instead of trashing)'}"
        )
    else:
        print(f"musicdedupe  scanning: {root}")
        print(f"  ffprobe: {HAS_FFPROBE}  fpcalc: {HAS_FPCALC}  "
              f"ffplay: {HAS_FFPLAY}  send2trash: {HAS_TRASH}")

    # Decide delete mode
    delete_mode = args.delete_mode
    if delete_mode is None:
        delete_mode = 'trash' if HAS_TRASH else 'move'

    # Cache path
    cache_path = None
    if not args.no_cache:
        cache_path = args.cache or os.path.join(root, '.musicdedupe-cache.json')

    # 1. Find files
    files = find_audio_files(root, follow_symlinks=args.follow_symlinks)
    msg = f"Found {len(files)} audio file(s)."
    console.print(msg) if console else print(msg)
    if not files:
        return 0

    # 2. Scan
    t0 = time.time()
    tracks = scan_library(files, cache_path=cache_path, workers=args.workers)
    scan_time = time.time() - t0
    msg = f"Scanned in {scan_time:.1f}s."
    console.print(msg, style="dim") if console else print(msg)

    # 3. Separate corrupted
    corrupted = [t for t in tracks if t.corrupted]
    healthy = [t for t in tracks if not t.corrupted]
    msg = f"{len(healthy)} ok, {len(corrupted)} corrupted/unreadable."
    console.print(msg) if console else print(msg)

    # 4. Group
    all_groups: list[tuple[str, list[Track]]] = []
    claimed: set[str] = set()

    if 'identical' not in args.skip:
        gs, claimed = group_exact(healthy)
        all_groups.extend(('identical', g) for g in gs)

    if 'audio' not in args.skip and HAS_FPCALC:
        gs, claimed = group_audio(healthy, claimed)
        all_groups.extend(('audio', g) for g in gs)

    if 'meta' not in args.skip:
        gs = group_metadata(healthy, claimed)
        all_groups.extend(('meta', g) for g in gs)

    # Stats
    if HAS_RICH:
        summary = Table(show_header=True, header_style="bold", box=None)
        summary.add_column("Category")
        summary.add_column("Groups", justify="right")
        summary.add_column("Files", justify="right")
        for key in ('identical', 'audio', 'meta'):
            gs = [g for k, g in all_groups if k == key]
            summary.add_row(GROUP_LABELS[key], str(len(gs)), str(sum(len(g) for g in gs)))
        console.print(Panel(summary, title="Duplicate scan summary", border_style="cyan"))
    else:
        for key in ('identical', 'audio', 'meta'):
            gs = [g for k, g in all_groups if k == key]
            print(f"  {key}: {len(gs)} groups, {sum(len(g) for g in gs)} files")

    # 5. Corrupted first
    to_delete: list[Track] = []
    to_delete.extend(show_corrupted(corrupted))

    # 6. Interactive review
    if all_groups:
        msg = f"\nReviewing {len(all_groups)} group(s). Type ? for help at any prompt.\n"
        console.print(msg) if console else print(msg)
        to_delete.extend(interactive_review(all_groups, args.play_start, args.play_length))
    else:
        msg = "No duplicate groups found."
        console.print(f"[green]{msg}[/green]") if console else print(msg)

    # 7. Confirm & delete
    if not to_delete:
        msg = "Nothing marked for deletion. Done."
        console.print(f"[green]{msg}[/green]") if console else print(msg)
        return 0

    # Deduplicate the deletion list (same path could appear via multiple groups)
    seen = set()
    unique = []
    for t in to_delete:
        if t.path in seen:
            continue
        seen.add(t.path)
        unique.append(t)

    total_size = sum(t.size for t in unique)
    if HAS_RICH:
        console.rule(f"[bold]{len(unique)} file(s) marked for deletion — {human_size(total_size)}[/bold]")
        for t in unique:
            console.print(f"  [red]✗[/red] {t.path}")
    else:
        print(f"{len(unique)} file(s) marked for deletion ({human_size(total_size)}):")
        for t in unique:
            print(f"  X  {t.path}")

    if args.dry_run:
        msg = "--dry-run: not deleting anything."
        console.print(f"[yellow]{msg}[/yellow]") if console else print(msg)
        return 0

    mode_desc = {
        'trash': 'Move to system trash',
        'move': f'Move to: {args.move_to}',
        'remove': 'PERMANENTLY DELETE (no trash)',
    }[delete_mode]
    confirm = prompt(f"{mode_desc}? [y/N]")
    if not confirm.lower().startswith('y'):
        msg = "Aborted. No files were deleted."
        console.print(f"[yellow]{msg}[/yellow]") if console else print(msg)
        return 0

    n = do_delete(unique, mode=delete_mode, move_to=args.move_to)
    msg = f"Done. {n}/{len(unique)} file(s) processed."
    console.print(f"[green]{msg}[/green]") if console else print(msg)

    # Invalidate cache entries for deleted files
    if cache_path and os.path.exists(cache_path):
        cache = load_cache(cache_path)
        for t in unique:
            cache.pop(t.path, None)
        save_cache(cache_path, cache)

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        sys.exit(130)
