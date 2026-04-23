"""Interactive duplicate review, with a streaming group source."""
from __future__ import annotations

import os
import queue
import subprocess
import threading
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Iterator, Optional, Protocol

from .scan import HAS_FFPLAY
from .track import Track
from .ui import (
    UI,
    human_bitrate_compact,
    human_date,
    human_duration,
    human_size,
)

try:
    from rich.markup import escape as _esc
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
except ImportError:  # pragma: no cover
    Panel = Table = Text = None  # type: ignore[assignment,misc]

    def _esc(s: str) -> str:  # type: ignore[misc]
        return s


GROUP_LABELS = {
    "identical": "Byte-identical copies",
    "audio": "Same audio content (different encoding/bitrate/format)",
    "meta": "Same artist & title (possibly different versions/remixes)",
}


@dataclass
class ReviewedGroup:
    kind: str
    group: list[Track]
    marks: dict[int, str] = field(default_factory=dict)  # index -> "keep"|"delete"


@dataclass
class ReviewResult:
    reviewed: list[ReviewedGroup] = field(default_factory=list)
    to_delete: list[Track] = field(default_factory=list)

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
        ui.console.rule(f"[bold cyan]{_esc(header)}[/bold cyan]")
        table = _new_decision_table()
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
            name = Text(t.display_name, no_wrap=True, overflow="ellipsis")
            if style:
                name.stylize(style)
            table.add_row(
                str(i),
                Text(mark, style=style or ("yellow" if li == best else "")),
                name,
                human_date(t.mtime),
                t.ext.lstrip("."),
                human_bitrate_compact(t.bitrate, t.lossless),
                human_duration(t.duration),
                human_size(t.size),
            )
        ui.console.print(table)
        meta = next((t for t in group if t.artist or t.title), None)
        if meta:
            line = Text()
            line.append("  tags: ", style="dim")
            line.append(meta.artist or "?", style="bold")
            line.append(" — ")
            line.append(meta.title or "?")
            if meta.album:
                line.append(f"  ({meta.album})", style="dim")
            ui.console.print(line)
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
            print(
                f"[{i:>2}] {mark} {t.display_name}  "
                f"{human_date(t.mtime)}  {t.ext.lstrip('.'):<5}  "
                f"{human_bitrate_compact(t.bitrate, t.lossless):>8}  "
                f"{human_duration(t.duration)}  {human_size(t.size)}"
            )
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


# --- final confirmation ------------------------------------------------------

def _new_decision_table() -> "Table":
    """Shared table schema for the review and confirmation screens."""
    assert Table is not None
    table = Table(
        show_header=True, header_style="bold", box=None,
        pad_edge=False, expand=True,
    )
    table.add_column("#", justify="right", style="dim", width=3, no_wrap=True)
    table.add_column("", width=1, no_wrap=True)
    table.add_column("File", overflow="ellipsis", no_wrap=True, ratio=1, min_width=16)
    table.add_column("Date", justify="right", width=10, no_wrap=True)
    table.add_column("Fmt", width=5, no_wrap=True)
    table.add_column("Bitrate", justify="right", width=8, no_wrap=True)
    table.add_column("Len", justify="right", width=7, no_wrap=True)
    table.add_column("Size", justify="right", width=9, no_wrap=True)
    return table


def _build_decision_table(
    tracks: list[Track],
    marks_for: "dict[int, str] | None" = None,
    *,
    all_delete: bool = False,
) -> "Table":
    assert Table is not None and Text is not None
    table = _new_decision_table()
    for i, t in enumerate(tracks, 1):
        li = i - 1
        if all_delete:
            action = "delete"
        else:
            action = (marks_for or {}).get(li, "keep")
        if action == "delete":
            mark, style = "✗", "red"
        else:
            mark, style = "✓", "green"
        name = Text(t.display_name, no_wrap=True, overflow="ellipsis")
        name.stylize(style)
        table.add_row(
            str(i),
            Text(mark, style=style),
            name,
            human_date(t.mtime),
            t.ext.lstrip("."),
            human_bitrate_compact(t.bitrate, t.lossless),
            human_duration(t.duration),
            human_size(t.size),
        )
    return table


def _plain_decision_block(
    header: str,
    tracks: list[Track],
    marks_for: "dict[int, str] | None" = None,
    *,
    all_delete: bool = False,
) -> None:
    print("-" * 72)
    print(header)
    for i, t in enumerate(tracks, 1):
        li = i - 1
        action = "delete" if all_delete else (marks_for or {}).get(li, "keep")
        mark = "✗" if action == "delete" else "✓"
        print(
            f"[{i:>2}] {mark} {t.display_name}  "
            f"{human_date(t.mtime)}  {t.ext.lstrip('.'):<5}  "
            f"{human_bitrate_compact(t.bitrate, t.lossless):>8}  "
            f"{human_duration(t.duration)}  {human_size(t.size)}"
        )


def render_final_confirmation(
    ui: UI,
    result: ReviewResult,
    *,
    corrupted: list[Track] = (),
) -> None:
    """Show every group with full details before the delete prompt."""
    delete_groups = [rg for rg in result.reviewed if any(
        a == "delete" for a in rg.marks.values()
    )]

    total_delete_bytes = sum(t.size for t in result.to_delete)
    total_delete_files = len(result.to_delete)
    total_groups = len(delete_groups) + (1 if corrupted else 0)

    header = (
        f"{total_delete_files} file(s) marked for deletion — "
        f"{human_size(total_delete_bytes)}"
    )

    if ui.rich and Table is not None and Text is not None:
        assert ui.console is not None
        ui.console.rule(f"[bold]Confirm deletions[/bold]")
        ui.console.print(f"[bold]{_esc(header)}[/bold]\n")

        if corrupted:
            ui.console.print(
                f"[bold red]Corrupted files[/bold red] "
                f"[dim]({len(corrupted)} file(s))[/dim]"
            )
            ui.console.print(_build_decision_table(corrupted, all_delete=True))
            reclaim = sum(t.size for t in corrupted)
            ui.console.print(
                f"  [dim]reclaim:[/dim] {human_size(reclaim)}  "
                f"[dim]({len(corrupted)} of {len(corrupted)} files)[/dim]\n"
            )

        for i, rg in enumerate(delete_groups, 1):
            del_count = sum(1 for a in rg.marks.values() if a == "delete")
            label = GROUP_LABELS.get(rg.kind, rg.kind)
            ui.console.print(
                f"[dim]Group {i}/{total_groups} —[/dim] "
                f"[bold cyan]{_esc(label)}[/bold cyan]"
            )
            ui.console.print(_build_decision_table(rg.group, rg.marks))
            reclaim = sum(
                rg.group[li].size for li, a in rg.marks.items()
                if a == "delete" and 0 <= li < len(rg.group)
            )
            ui.console.print(
                f"  [dim]reclaim:[/dim] {human_size(reclaim)}  "
                f"[dim]({del_count} of {len(rg.group)} files)[/dim]\n"
            )

        ui.console.print(
            f"[bold]Total reclaim:[/bold] {human_size(total_delete_bytes)}  "
            f"across {total_delete_files} file(s) in "
            f"{total_groups} group(s)."
        )
    else:
        print("=" * 72)
        print("Confirm deletions")
        print(header)
        print()
        if corrupted:
            _plain_decision_block(
                f"Corrupted files ({len(corrupted)} file(s))",
                corrupted, all_delete=True,
            )
            reclaim = sum(t.size for t in corrupted)
            print(f"  reclaim: {human_size(reclaim)}  "
                  f"({len(corrupted)} of {len(corrupted)} files)")
            print()
        for i, rg in enumerate(delete_groups, 1):
            del_count = sum(1 for a in rg.marks.values() if a == "delete")
            label = GROUP_LABELS.get(rg.kind, rg.kind)
            _plain_decision_block(
                f"Group {i}/{total_groups} — {label}",
                rg.group, rg.marks,
            )
            reclaim = sum(
                rg.group[li].size for li, a in rg.marks.items()
                if a == "delete" and 0 <= li < len(rg.group)
            )
            print(f"  reclaim: {human_size(reclaim)}  "
                  f"({del_count} of {len(rg.group)} files)")
            print()
        print(f"Total reclaim: {human_size(total_delete_bytes)}  "
              f"across {total_delete_files} file(s) in {total_groups} group(s).")


# --- snippet playback --------------------------------------------------------

def play_snippet(ui: UI, path: str, start: int = 30, length: int = 15) -> None:
    if not HAS_FFPLAY:
        ui.error("ffplay not found. Install ffmpeg to use the preview feature.")
        return
    if not os.path.exists(path):
        ui.error(f"File not found: {path}")
        return
    ui.print(
        f"[cyan]▶ Playing {_esc(os.path.basename(path))} "
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
) -> ReviewResult:
    gidx = 0
    group_marks: dict[int, dict[int, str]] = defaultdict(dict)
    seen_groups: dict[int, tuple[str, list[Track]]] = {}

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
        seen_groups[gidx] = (kind, group)
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

    # Collate over what we've actually viewed, preserving group order.
    reviewed: list[ReviewedGroup] = []
    to_delete: list[Track] = []
    seen_paths: set[str] = set()
    for gi in sorted(group_marks):
        marks = group_marks[gi]
        if not marks:
            continue
        cached = seen_groups.get(gi)
        if cached is None:
            item = source.get(gi, timeout=0)
            if item is None:
                continue
            cached = item
        kind, group = cached
        reviewed.append(ReviewedGroup(kind=kind, group=group, marks=dict(marks)))
        for li, action in marks.items():
            if action != "delete" or not (0 <= li < len(group)):
                continue
            t = group[li]
            if t.path in seen_paths:
                continue
            seen_paths.add(t.path)
            to_delete.append(t)
    return ReviewResult(reviewed=reviewed, to_delete=to_delete)
