#!/usr/bin/env python3
"""
move_audio - Walk SRC recursively, move audio files into a flat DST,
converting on the way for Pioneer DJ-gear playback.

Format policy
-------------
  .mp3                         -> move (no re-encode)
  .wav                         -> move
  .flac/.aiff/.aif/.ape/.wv    -> ffmpeg -> WAV (PCM s16le)
  .m4a/.mp4 (codec=alac)       -> ffmpeg -c copy -> .m4a (rewrap, no re-encode)
  .m4a/.mp4 (codec=aac)        -> ffmpeg -> MP3 (libmp3lame, V0)
  .aac/.ogg/.oga/.opus/.wma    -> ffmpeg -> MP3 (libmp3lame, V0)

Each completed action is appended to a TSV log so you can audit / undo
externally. Source files are deleted only after the destination is
written, mtime preserved, and (for converts) ffprobe re-verifies the
output.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

LOSSLESS_TO_WAV = {".flac", ".aiff", ".aif", ".ape", ".wv"}
LOSSY_TO_MP3 = {".aac", ".ogg", ".oga", ".opus", ".wma"}
M4A_LIKE = {".m4a", ".mp4"}
PASSTHROUGH = {".mp3", ".wav"}
ALL_EXTS = LOSSLESS_TO_WAV | LOSSY_TO_MP3 | M4A_LIKE | PASSTHROUGH

ACTION_MOVE = "move"
ACTION_CONVERT_WAV = "convert_wav"
ACTION_CONVERT_MP3 = "convert_mp3"
ACTION_REWRAP_M4A = "rewrap_m4a"

_SHUTDOWN = threading.Event()


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Job:
    src: str
    action: str
    target_ext: str
    size: int
    duration: float = 0.0  # filled in for convert/rewrap; 0 for plain move

    @property
    def stem(self) -> str:
        return Path(self.src).stem


def ffprobe_codec(path: str) -> str:
    """Return the codec_name of the first audio stream, or '' on failure."""
    try:
        cp = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name", "-of", "default=nw=1:nk=1", path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""
    return cp.stdout.decode("utf-8", "replace").strip().lower()


def ffprobe_full(path: str) -> tuple[float, bool, str]:
    """Return (duration_seconds, ok, error). ok=True iff there's a decodable
    audio stream with duration > 0.1s."""
    try:
        cp = subprocess.run(
            ["ffprobe", "-v", "error", "-print_format", "json",
             "-show_format", "-show_streams", "-select_streams", "a:0", path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False,
        )
    except subprocess.TimeoutExpired:
        return 0.0, False, "ffprobe timeout"
    except FileNotFoundError:
        return 0.0, False, "ffprobe not found"
    if cp.returncode != 0:
        err = cp.stderr.decode("utf-8", "replace").strip().splitlines()
        return 0.0, False, (err[-1] if err else "ffprobe failed")
    try:
        data = json.loads(cp.stdout)
    except json.JSONDecodeError:
        return 0.0, False, "ffprobe invalid json"
    streams = data.get("streams") or []
    if not streams:
        return 0.0, False, "no audio stream"
    fmt = data.get("format") or {}
    try:
        dur = float(fmt.get("duration") or streams[0].get("duration") or 0.0)
    except (TypeError, ValueError):
        dur = 0.0
    if dur <= 0.1:
        return dur, False, "zero-length audio"
    return dur, True, ""


def plan_action(src: str) -> str:
    """Decide what to do with a file based on its extension and (for m4a) codec."""
    ext = Path(src).suffix.lower()
    if ext == ".mp3" or ext == ".wav":
        return ACTION_MOVE
    if ext in LOSSLESS_TO_WAV:
        return ACTION_CONVERT_WAV
    if ext in LOSSY_TO_MP3:
        return ACTION_CONVERT_MP3
    if ext in M4A_LIKE:
        codec = ffprobe_codec(src)
        if codec == "alac":
            return ACTION_REWRAP_M4A
        return ACTION_CONVERT_MP3
    return ACTION_MOVE  # unknown extension: shouldn't reach here


def target_ext_for(action: str, src_ext: str) -> str:
    if action == ACTION_MOVE:
        return src_ext.lstrip(".")
    if action == ACTION_CONVERT_WAV:
        return "wav"
    if action == ACTION_CONVERT_MP3:
        return "mp3"
    if action == ACTION_REWRAP_M4A:
        return "m4a"
    return src_ext.lstrip(".")


# ---------------------------------------------------------------------------
# Path / log helpers
# ---------------------------------------------------------------------------

def find_audio_files(src: str) -> list[str]:
    out = []
    for dirpath, dirnames, filenames in os.walk(src):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fn in filenames:
            if fn.startswith("."):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext in ALL_EXTS:
                out.append(os.path.join(dirpath, fn))
    out.sort()
    return out


def reserve_target(dst_dir: str, stem: str, ext: str, claimed: set[str]) -> str:
    """Pick a non-colliding target path. claimed is the set of paths we've
    already promised to other jobs in this run (so two jobs for files with
    the same stem don't both pick the same dst)."""
    base = os.path.join(dst_dir, f"{stem}.{ext}")
    cand = base
    i = 1
    while cand in claimed or os.path.exists(cand):
        cand = os.path.join(dst_dir, f"{stem}_{i}.{ext}")
        i += 1
    claimed.add(cand)
    return cand


# Filesystem mtime resolution can be as coarse as 2s on FAT/exFAT, so allow
# that much slack when matching a resume target back to its source.
_MTIME_TOLERANCE_NS = 2_000_000_000


def _mtime_matches(src: str, dst: str) -> bool:
    try:
        s_ns = os.stat(src).st_mtime_ns
        d_ns = os.stat(dst).st_mtime_ns
    except OSError:
        return False
    return abs(s_ns - d_ns) <= _MTIME_TOLERANCE_NS


def existing_target_matches_move(src: str, dst: str) -> bool:
    """For ACTION_MOVE: dst is a resume-skip if its size and mtime match src.
    The mtime check is what makes this binding to *this* source rather than to
    an unrelated file that happens to share the stem."""
    try:
        if os.path.getsize(src) != os.path.getsize(dst):
            return False
    except OSError:
        return False
    return _mtime_matches(src, dst)


def existing_target_matches_convert(src: str, dst: str) -> bool:
    """For converts/rewraps: dst is a resume-skip if it's non-empty,
    ffprobe-decodable, and its mtime matches src (copy_mtime is called after
    every successful convert, so a dst produced by a prior run for *this*
    source carries src's mtime)."""
    try:
        if os.path.getsize(dst) == 0:
            return False
    except OSError:
        return False
    if not _mtime_matches(src, dst):
        return False
    _, ok, _ = ffprobe_full(dst)
    return ok


def copy_mtime(src: str, dst: str) -> None:
    try:
        st = os.stat(src)
        os.utime(dst, ns=(st.st_atime_ns, st.st_mtime_ns))
    except OSError:
        pass


class TSVLog:
    """Append-only TSV log. Thread-safe."""

    HEADER = "iso_ts\taction\tstatus\tsrc\tdst\tbytes_in\tbytes_out\tnote\n"

    def __init__(self, path: str) -> None:
        self.path = path
        self.lock = threading.Lock()
        new = not os.path.exists(path)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.fh = open(path, "a", encoding="utf-8")
        if new:
            self.fh.write(self.HEADER)
            self.fh.flush()

    def write(self, action: str, status: str, src: str, dst: str,
              bytes_in: int, bytes_out: int, note: str = "") -> None:
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        # Strip tabs/newlines from any user-supplied field.
        def clean(s: str) -> str:
            return s.replace("\t", " ").replace("\n", " ").replace("\r", " ")
        row = (f"{ts}\t{clean(action)}\t{clean(status)}\t{clean(src)}\t"
               f"{clean(dst)}\t{bytes_in}\t{bytes_out}\t{clean(note)}\n")
        with self.lock:
            self.fh.write(row)
            self.fh.flush()

    def close(self) -> None:
        try:
            self.fh.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# ffmpeg execution with -progress
# ---------------------------------------------------------------------------

def _ffmpeg_args(action: str, src: str, dst: str, mp3_quality: str) -> list[str]:
    base = ["ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "error",
            "-y", "-threads", "1", "-i", src, "-progress", "pipe:1", "-nostats"]
    if action == ACTION_CONVERT_WAV:
        return base + ["-vn", "-c:a", "pcm_s16le", dst]
    if action == ACTION_CONVERT_MP3:
        # mp3_quality may be a VBR digit ("0".."9") or a CBR kbps ("320","256",...)
        if mp3_quality.isdigit() and len(mp3_quality) == 1:
            return base + ["-vn", "-c:a", "libmp3lame", "-q:a", mp3_quality, dst]
        return base + ["-vn", "-c:a", "libmp3lame", "-b:a", f"{mp3_quality}k", dst]
    if action == ACTION_REWRAP_M4A:
        return base + ["-c", "copy", dst]
    raise ValueError(f"ffmpeg called for non-ffmpeg action: {action}")


def run_ffmpeg(action: str, src: str, dst: str, total_dur: float,
               mp3_quality: str, on_progress) -> tuple[bool, str]:
    """Run ffmpeg for the given action. on_progress(fraction in [0,1])
    is called as the file converts. Returns (ok, error_message)."""
    args = _ffmpeg_args(action, src, dst, mp3_quality)
    try:
        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
    except FileNotFoundError:
        return False, "ffmpeg not found"

    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            if _SHUTDOWN.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                return False, "interrupted"
            line = line.strip()
            if not line:
                continue
            if line.startswith("out_time_ms=") and total_dur > 0:
                try:
                    out_us = int(line.split("=", 1)[1])
                    on_progress(min(1.0, (out_us / 1_000_000.0) / total_dur))
                except (ValueError, ZeroDivisionError):
                    pass
            elif line == "progress=end":
                on_progress(1.0)
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
        return False, "interrupted"

    if proc.returncode != 0:
        err = (proc.stderr.read() if proc.stderr else "") or "ffmpeg failed"
        return False, err.strip().splitlines()[-1] if err.strip() else "ffmpeg failed"
    return True, ""


# ---------------------------------------------------------------------------
# Per-job execution
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Result:
    job: Job
    dst: str
    status: str   # 'ok' | 'skipped' | 'failed'
    bytes_out: int = 0
    note: str = ""


def execute_job(job: Job, dst: str, mp3_quality: str, verify: bool,
                progress_cb) -> Result:
    """Run a single job. progress_cb(fraction) reports per-file progress
    inside [0,1]. Idempotent in the sense that a partial dst is removed on
    failure."""
    if _SHUTDOWN.is_set():
        return Result(job, dst, "failed", 0, "interrupted")

    if job.action == ACTION_MOVE:
        try:
            try:
                os.rename(job.src, dst)
            except OSError:
                # Cross-filesystem; copy2 preserves mtime.
                shutil.copy2(job.src, dst)
                os.remove(job.src)
            progress_cb(1.0)
            try:
                bo = os.path.getsize(dst)
            except OSError:
                bo = job.size
            return Result(job, dst, "ok", bo)
        except OSError as e:
            if os.path.exists(dst):
                # Don't delete a successful rename target; only clean partial copy.
                if not os.path.exists(job.src):
                    return Result(job, dst, "failed", 0, f"move post-error: {e}")
                try:
                    os.remove(dst)
                except OSError:
                    pass
            return Result(job, dst, "failed", 0, f"move: {e}")

    # ffmpeg-driven actions
    ok, err = run_ffmpeg(job.action, job.src, dst, job.duration,
                         mp3_quality, progress_cb)
    if not ok:
        if os.path.exists(dst):
            try:
                os.remove(dst)
            except OSError:
                pass
        return Result(job, dst, "failed", 0, err or "ffmpeg failed")

    if verify:
        _, vok, verr = ffprobe_full(dst)
        if not vok:
            try:
                os.remove(dst)
            except OSError:
                pass
            return Result(job, dst, "failed", 0, f"verify: {verr}")

    copy_mtime(job.src, dst)
    try:
        bo = os.path.getsize(dst)
    except OSError:
        bo = 0

    try:
        os.remove(job.src)
    except OSError as e:
        # Conversion succeeded and was verified; only source cleanup failed.
        # Report as ok so the summary doesn't lie, with a note flagging the
        # leftover source for the operator.
        return Result(job, dst, "ok", bo, f"src not removed: {e}")

    return Result(job, dst, "ok", bo)


# ---------------------------------------------------------------------------
# Progress rendering (overall + per-file)
# ---------------------------------------------------------------------------

def fmt_time(s: float) -> str:
    if s < 0 or s != s:
        return "--:--"
    s = int(s)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"


def fmt_size(n: int) -> str:
    f = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if f < 1024:
            return f"{f:.1f} {unit}" if unit != "B" else f"{int(f)} B"
        f /= 1024
    return f"{f:.1f} TB"


class ProgressRenderer:
    """Single-line stderr progress: [overall] N/Total  per-file: name [bar] xx%"""

    def __init__(self, total_count: int, total_bytes: int, bar_w: int = 24) -> None:
        self.total_count = total_count
        self.total_bytes = max(1, total_bytes)
        self.bar_w = bar_w
        self.start = time.time()
        self.lock = threading.Lock()
        self.done_count = 0
        self.done_bytes = 0
        self.in_flight: dict[str, float] = {}  # src -> per-file fraction
        self.last_render = 0.0
        try:
            self.cols = shutil.get_terminal_size((100, 24)).columns
        except OSError:
            self.cols = 100

    def start_file(self, src: str) -> None:
        with self.lock:
            self.in_flight[src] = 0.0
        self._render(force=True)

    def update_file(self, src: str, frac: float) -> None:
        with self.lock:
            self.in_flight[src] = frac
        self._render()

    def finish_file(self, src: str, bytes_in: int, succeeded: bool) -> None:
        with self.lock:
            self.in_flight.pop(src, None)
            self.done_count += 1
            if succeeded:
                self.done_bytes += bytes_in
        self._render(force=True)

    def _bar(self, frac: float, w: int) -> str:
        frac = max(0.0, min(1.0, frac))
        filled = int(frac * w)
        return "#" * filled + "-" * (w - filled)

    def _render(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_render < 0.1:
            return
        self.last_render = now
        with self.lock:
            elapsed = now - self.start
            overall = self.done_bytes / self.total_bytes
            eta = -1.0
            if elapsed > 1.0 and overall > 0.001:
                eta = elapsed * (1 - overall) / overall
            in_flight_summary = ""
            if self.in_flight:
                src, frac = next(iter(self.in_flight.items()))
                name = os.path.basename(src)
                more = len(self.in_flight) - 1
                tail = f" (+{more})" if more else ""
                # leave room for fixed-width metadata at the end
                meta = f" [{self._bar(frac, 12)}] {int(frac*100):3d}%"
                avail = max(8, self.cols - len(meta) - len(tail) - 50)
                if len(name) > avail:
                    name = "..." + name[-(avail - 3):]
                in_flight_summary = f"  {name}{tail}{meta}"

            line = (f"\r\x1b[K[{self._bar(overall, self.bar_w)}] "
                    f"{self.done_count}/{self.total_count} "
                    f"{int(overall*100):3d}%  {fmt_time(elapsed)}<{fmt_time(eta)}"
                    f"{in_flight_summary}")
            sys.stderr.write(line)
            sys.stderr.flush()

    def finalize(self) -> None:
        sys.stderr.write("\n")
        sys.stderr.flush()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def install_signal_handlers() -> None:
    def handle(signum, frame):
        _SHUTDOWN.set()
    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)
    try:
        signal.signal(signal.SIGHUP, handle)
    except (AttributeError, ValueError):
        pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="move_audio",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--src", required=True, help="Source directory (recursive).")
    ap.add_argument("--dst", required=True, help="Destination directory (flat).")
    ap.add_argument("--workers", type=int, default=2,
                    help="Parallel workers (default: 2; raise for SSD-to-SSD).")
    ap.add_argument("--mp3-quality", default="0",
                    help="LAME quality. VBR digit 0-9 (default: 0 = highest VBR) "
                         "or CBR kbps like 320, 256, 192.")
    ap.add_argument("--log", default=None,
                    help="Append-only TSV log path (default: <dst>/.move_audio.log).")
    ap.add_argument("--no-verify", action="store_true",
                    help="Skip ffprobe re-verification of converted files.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print the plan and exit without touching anything.")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if shutil.which("ffmpeg") is None:
        print("error: ffmpeg not found in PATH", file=sys.stderr)
        return 1
    if shutil.which("ffprobe") is None:
        print("error: ffprobe not found in PATH", file=sys.stderr)
        return 1

    src = os.path.abspath(args.src)
    dst = os.path.abspath(args.dst)
    if not os.path.isdir(src):
        print(f"error: source directory not found: {src}", file=sys.stderr)
        return 1
    os.makedirs(dst, exist_ok=True)
    if dst == src or dst.startswith(src + os.sep):
        print("error: destination is equal to or inside source", file=sys.stderr)
        return 1

    install_signal_handlers()

    files = find_audio_files(src)
    if not files:
        print(f"No audio files found under {src}")
        return 0
    print(f"Found {len(files)} audio files. Planning...")

    # Plan: extension/codec -> action; pre-probe duration for converts/rewraps
    # so the per-file progress bar has a denominator. Probing is cheap relative
    # to converting.
    jobs: list[Job] = []
    for p in files:
        if _SHUTDOWN.is_set():
            return 130
        try:
            sz = os.path.getsize(p)
        except OSError:
            sz = 0
        action = plan_action(p)
        ext = Path(p).suffix.lower()
        target_ext = target_ext_for(action, ext)
        dur = 0.0
        # Duration only feeds the per-file progress bar denominator; dry-run
        # never renders one, so skip the (slow) probe in that mode.
        if action != ACTION_MOVE and not args.dry_run:
            dur, _, _ = ffprobe_full(p)
        jobs.append(Job(src=p, action=action, target_ext=target_ext,
                        size=sz, duration=dur))

    # Reserve dst paths up front so the planner is deterministic and resume
    # works the same way under --dry-run as for real.
    claimed: set[str] = set()
    targets: dict[str, str] = {}
    skips: list[Job] = []
    runnable: list[tuple[Job, str]] = []
    for job in jobs:
        # First, see if any matching dst already exists from a previous run
        # we can resume from. Iterate stem, stem_1, stem_2, ... until we hit
        # the first slot that's neither claimed nor present on disk — past
        # that point reserve_target would have allocated a fresh name, so
        # nothing further could possibly match.
        resume_dst = None
        i = 0
        while True:
            cand = (os.path.join(dst, f"{job.stem}.{job.target_ext}") if i == 0
                    else os.path.join(dst, f"{job.stem}_{i}.{job.target_ext}"))
            i += 1
            if cand in claimed:
                continue
            if not os.path.exists(cand):
                break
            ok = (existing_target_matches_move(job.src, cand)
                  if job.action == ACTION_MOVE
                  else existing_target_matches_convert(job.src, cand))
            if ok:
                resume_dst = cand
                break
        if resume_dst is not None:
            targets[job.src] = resume_dst
            claimed.add(resume_dst)
            skips.append(job)
            continue
        tgt = reserve_target(dst, job.stem, job.target_ext, claimed)
        targets[job.src] = tgt
        runnable.append((job, tgt))

    total_bytes = sum(j.size for j in jobs)
    print(f"Total: {fmt_size(total_bytes)} across {len(jobs)} files. "
          f"Plan: {len(runnable)} to do, {len(skips)} resume-skip.")

    if args.dry_run:
        for job, tgt in [(j, targets[j.src]) for j in jobs]:
            tag = "SKIP" if job in skips else job.action.upper()
            print(f"  [{tag:>12s}] {job.src} -> {tgt}")
        return 0

    # Open the log only when we're about to do real work.
    log_path = args.log or os.path.join(dst, ".move_audio.log")
    log = TSVLog(log_path)
    for job in skips:
        # Still record skips so the log mirrors the run.
        log.write(job.action, "skipped", job.src, targets[job.src],
                  job.size, 0, "resume")

    # If we already removed the source on a previous run, drop those skips
    # whose src no longer exists from the count of "pending work".
    runnable_bytes = sum(j.size for j, _ in runnable)
    progress = ProgressRenderer(len(runnable), max(1, runnable_bytes))

    results: list[Result] = []

    def submit(job: Job, tgt: str) -> Result:
        progress.start_file(job.src)
        r: Result | None = None
        try:
            r = execute_job(
                job, tgt, args.mp3_quality, not args.no_verify,
                lambda f: progress.update_file(job.src, f),
            )
            return r
        finally:
            succeeded = r is not None and r.status != "failed"
            progress.finish_file(job.src, job.size, succeeded)
            if r is not None:
                log.write(job.action, r.status, job.src, r.dst,
                          job.size, r.bytes_out, r.note)

    if len(runnable) == 0:
        print("Nothing to do.")
        log.close()
        return 0

    workers = max(1, args.workers)
    if workers == 1:
        for job, tgt in runnable:
            if _SHUTDOWN.is_set():
                break
            results.append(submit(job, tgt))
    else:
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(submit, j, t): (j, t) for j, t in runnable}
            try:
                for fut in cf.as_completed(futs):
                    results.append(fut.result())
            except KeyboardInterrupt:
                _SHUTDOWN.set()
                for fut in futs:
                    fut.cancel()

    progress.finalize()

    ok_n = sum(1 for r in results if r.status == "ok")
    fail = [r for r in results if r.status == "failed"]
    print(f"Summary: {ok_n} done, {len(skips)} skipped (resume), "
          f"{len(fail)} failed.")
    if fail:
        print("\nFailures:")
        for r in fail:
            print(f"  [{r.job.action}] {r.job.src}: {r.note}")
    log.close()
    if _SHUTDOWN.is_set():
        return 130
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
