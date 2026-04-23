"""Interactive duplicate review, with a streaming group source."""
from __future__ import annotations

import os
import queue
import subprocess
import threading
from collections import defaultdict
from dataclasses import asdict
from typing import Iterator, Optional, Protocol

from .scan import HAS_FFPLAY
from .track import Track
from .ui import UI, human_bitrate, human_duration, human_size

try:
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:  # pragma: no cover
    Panel = Table = Text = None  # type: ignore[assignment,misc]


GROUP_LABELS = {
    "identical": "Byte-identical copies",
    "audio": "Same audio content (different encoding/bitrate/format)",
    "meta": "Same artist & title (possibly different versions/remixes)",
}

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


# --- group sources -----------------------------------------------------------

class GroupSource(Protocol):
    def get(self, idx: int, timeout: Optional[float] = None) -> Optional[tuple[str, list[Track]]]: ...
    def total_known(self) -> int: ...
    def finished(self) -> bool: ...
    def stop(self) -> None: ...


class ListGroupSource:
    """A pre-computed list of groups (no streaming)."""

    def __init__(self, groups: list[tuple[str, list[Track]]]) -> None:
        self._groups = list(groups)

    def get(self, idx: int, timeout: Optional[float] = None) -> Optional[tuple[str, list[Track]]]:
        if 0 <= idx < len(self._groups):
            return self._groups[idx]
        return None

    def total_known(self) -> int:
        return len(self._groups)

    def finished(self) -> bool:
        return True

    def stop(self) -> None:
        pass


class QueueGroupSource:
    """Pulls groups from a queue as a background producer emits them.

    The producer thread calls `put(kind, group)` per discovered group and
    `close()` when done. Consumers use `get(idx)` to walk; `get` blocks on
    the queue only when `idx == len(buffered)` and the producer is still
    running.
    """

    _SENTINEL = object()

    def __init__(self) -> None:
        self._q: "queue.Queue[object]" = queue.Queue()
        self._buf: list[tuple[str, list[Track]]] = []
        self._lock = threading.Lock()
        self._closed = False
        self._stopped = False

    def put(self, kind: str, group: list[Track]) -> None:
        if self._stopped:
            return
        self._q.put((kind, group))

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._q.put(self._SENTINEL)

    def stop(self) -> None:
        self._stopped = True
        self.close()

    def _drain_ready(self) -> None:
        while True:
            try:
                item = self._q.get_nowait()
            except queue.Empty:
                return
            if item is self._SENTINEL:
                return
            self._buf.append(item)  # type: ignore[arg-type]

    def _wait_for_next(self, timeout: Optional[float]) -> bool:
        try:
            item = self._q.get(timeout=timeout)
        except queue.Empty:
            return False
        if item is self._SENTINEL:
            return False
        self._buf.append(item)  # type: ignore[arg-type]
        return True

    def get(self, idx: int, timeout: Optional[float] = None) -> Optional[tuple[str, list[Track]]]:
        with self._lock:
            self._drain_ready()
            if idx < len(self._buf):
                return self._buf[idx]
            if self._closed and self._q.empty():
                return None
        # Wait (without holding the lock) for another group to arrive.
        if self._closed:
            return None
        got = self._wait_for_next(timeout)
        if not got:
            return None
        with self._lock:
            if idx < len(self._buf):
                return self._buf[idx]
            return None

    def total_known(self) -> int:
        with self._lock:
            self._drain_ready()
            return len(self._buf)

    def finished(self) -> bool:
        return self._closed and self._q.empty()


# --- display -----------------------------------------------------------------

def render_group(
    ui: UI,
    kind: str,
    group: list[Track],
    idx: int,
    total_known: int,
    streaming: bool,
    marks: dict[int, str],
) -> None:
    best = pick_best(group)
    suffix = "+" if streaming else ""
    header = f"Group {idx}/{total_known}{suffix}  —  {GROUP_LABELS[kind]}  ({len(group)} files)"

    if ui.rich and Table is not None:
        assert ui.console is not None
        ui.console.rule(f"[bold cyan]{header}[/bold cyan]")
        table = Table(show_header=True, header_style="bold", box=None, pad_edge=False)
        table.add_column("#", justify="right", style="dim", width=3)
        table.add_column("", width=3)
        table.add_column("File", overflow="fold")
        table.add_column("Fmt", width=6)
        table.add_column("Bitrate", justify="right", width=12)
        table.add_column("SR", justify="right", width=7)
        table.add_column("Len", justify="right", width=8)
        table.add_column("Size", justify="right", width=10)
        for i, t in enumerate(group, 1):
            li = i - 1
            mark = ""
            style = ""
            if marks.get(li) == "keep":
                mark, style = "✓", "green"
            elif marks.get(li) == "delete":
                mark, style = "✗", "red"
            elif li == best:
                mark = "★"
            name = Text(t.display_name)
            if style:
                name.stylize(style)
            subline = Text("  └ " + os.path.dirname(t.path), style="dim")
            table.add_row(
                str(i),
                Text(mark, style=style or ("yellow" if li == best else "")),
                Text.assemble(name, "\n", subline),
                t.ext.lstrip("."),
                human_bitrate(t.bitrate, t.lossless),
                f"{t.sample_rate/1000:.1f}k" if t.sample_rate else "?",
                human_duration(t.duration),
                human_size(t.size),
            )
        ui.console.print(table)
        meta = next((t for t in group if t.artist or t.title), None)
        if meta:
            ui.console.print(
                f"  [dim]tags:[/dim] "
                f"[bold]{meta.artist or '?'}[/bold] — "
                f"{meta.title or '?'}"
                + (f"  [dim]({meta.album})[/dim]" if meta.album else "")
            )
        ui.console.print()
    else:
        print("=" * 72)
        print(header)
        print("-" * 72)
        for i, t in enumerate(group, 1):
            li = i - 1
            mark = " "
            if marks.get(li) == "keep":
                mark = "✓"
            elif marks.get(li) == "delete":
                mark = "✗"
            elif li == best:
                mark = "★"
            print(f"[{i}] {mark} {t.display_name}")
            print(f"     {t.ext.lstrip('.')}  {human_bitrate(t.bitrate, t.lossless)}  "
                  f"{t.sample_rate}Hz  {human_duration(t.duration)}  {human_size(t.size)}")
            print(f"     {t.path}")
        print()


def print_info(ui: UI, t: Track) -> None:
    if ui.rich and Table is not None:
        assert ui.console is not None
        table = Table(show_header=False, box=None)
        table.add_column(style="bold dim", justify="right")
        table.add_column()
        rows = [
            ("path", t.path),
            ("size", human_size(t.size)),
            ("codec", t.codec or t.ext.lstrip(".")),
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
            ("fingerprint", (t.fingerprint[:48] + "…") if t.fingerprint else "—"),
            ("hash", f"{t.hash_algo or '?'} {t.content_hash}" if t.content_hash else "(not computed)"),
            ("quality score", str(t.quality_score)),
        ]
        if t.corrupted:
            rows.append(("ERROR", t.error))
        for k, v in rows:
            table.add_row(k, str(v))
        ui.console.print(table)
    else:
        for k, v in asdict(t).items():
            print(f"  {k}: {v}")


def pick_best(group: list[Track]) -> int:
    best = 0
    for i, t in enumerate(group):
        if t.quality_score > group[best].quality_score:
            best = i
    return best


# --- snippet playback --------------------------------------------------------

def play_snippet(ui: UI, path: str, start: int = 30, length: int = 15) -> None:
    if not HAS_FFPLAY:
        ui.error("ffplay not found. Install ffmpeg to use the preview feature.")
        return
    if not os.path.exists(path):
        ui.error(f"File not found: {path}")
        return
    ui.print(
        f"[cyan]▶ Playing {os.path.basename(path)} "
        f"(from {start}s, {length}s)  — press q or Ctrl+C to stop[/cyan]"
    )
    cmd = [
        "ffplay", "-nodisp", "-autoexit", "-hide_banner", "-loglevel", "error",
        "-ss", str(start), "-t", str(length), path,
    ]
    try:
        subprocess.run(cmd, check=False)
    except KeyboardInterrupt:
        pass


# --- index parsing -----------------------------------------------------------

def parse_indices(tokens: list[str], n: int) -> list[int]:
    out = []
    for tk in tokens:
        if "-" in tk and tk.count("-") == 1 and all(p.isdigit() for p in tk.split("-")):
            a, b = tk.split("-")
            for i in range(int(a), int(b) + 1):
                if 1 <= i <= n:
                    out.append(i - 1)
        elif tk.isdigit():
            i = int(tk)
            if 1 <= i <= n:
                out.append(i - 1)
    return out


# --- interactive loop --------------------------------------------------------

def interactive_review(
    ui: UI,
    source: GroupSource,
    *,
    play_start: int,
    play_length: int,
) -> list[Track]:
    to_delete: list[Track] = []
    gidx = 0
    group_marks: dict[int, dict[int, str]] = defaultdict(dict)

    while True:
        current = source.get(gidx, timeout=0.1)
        if current is None:
            if source.finished():
                break
            # Producer still running; show a waiting hint and retry.
            ui.info("  … waiting for next group", dim=True)
            current = source.get(gidx, timeout=None)
            if current is None:
                break
        kind, group = current
        marks = group_marks[gidx]
        render_group(ui, kind, group, gidx + 1, source.total_known(), not source.finished(), marks)

        cmd = ui.prompt("›")
        if not cmd:
            continue
        cmd_l = cmd.lower()

        if cmd.isdigit():
            idxs = parse_indices([cmd], len(group))
            if idxs:
                keep = idxs[0]
                marks.clear()
                for i in range(len(group)):
                    marks[i] = "keep" if i == keep else "delete"
                gidx += 1
            continue

        parts = cmd_l.split()
        head = parts[0]
        rest = parts[1:]

        if head in ("q", "quit", "exit"):
            break
        if head in ("?", "h", "help"):
            if ui.rich and Panel is not None:
                assert ui.console is not None
                ui.console.print(Panel(HELP_TEXT, title="Commands", border_style="dim"))
            else:
                print(HELP_TEXT)
            continue
        if head in ("n", "next", "s", "skip"):
            gidx += 1
            continue
        if head in ("b", "back", "prev"):
            gidx = max(0, gidx - 1)
            continue
        if head == "a":
            if kind == "meta":
                ui.warning("Auto-pick is disabled for version/remix groups — choose manually.")
                continue
            best = pick_best(group)
            marks.clear()
            for i in range(len(group)):
                marks[i] = "keep" if i == best else "delete"
            gidx += 1
            continue
        if head == "i" and rest:
            idxs = parse_indices(rest, len(group))
            if idxs:
                print_info(ui, group[idxs[0]])
            continue
        if head == "p" and rest:
            for i in parse_indices(rest, len(group)):
                play_snippet(ui, group[i].path, start=play_start, length=play_length)
            continue
        if head in ("k", "keep") and rest:
            for i in parse_indices(rest, len(group)):
                marks[i] = "keep"
            continue
        if head in ("d", "delete", "del", "rm") and rest:
            for i in parse_indices(rest, len(group)):
                marks[i] = "delete"
            continue
        if head in ("u", "unmark") and rest:
            for i in parse_indices(rest, len(group)):
                marks.pop(i, None)
            continue

        ui.warning(f"Unknown command: {cmd!r}. Type ? for help.")

    # Collate deletions over what we've actually viewed.
    for gi, marks in group_marks.items():
        item = source.get(gi, timeout=0)
        if item is None:
            continue
        _, group = item
        for li, action in marks.items():
            if action == "delete" and 0 <= li < len(group):
                to_delete.append(group[li])
    return to_delete
