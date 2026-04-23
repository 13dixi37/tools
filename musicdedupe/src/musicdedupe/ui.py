"""Tiny UI layer so the rest of the code doesn't care whether rich is installed."""
from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        MofNCompleteColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeRemainingColumn,
    )
    from rich.table import Table
    from rich.text import Text

    HAS_RICH = True
except ImportError:
    HAS_RICH = False


try:
    from send2trash import send2trash  # noqa: F401

    HAS_TRASH = True
except ImportError:
    HAS_TRASH = False


# --- human formatting --------------------------------------------------------

def human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:,.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def human_duration(s: float) -> str:
    if not s or s <= 0:
        return "  ?:??"
    m, rem = divmod(int(s), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{rem:02d}"
    return f"{m:>3d}:{rem:02d}"


def human_bitrate(kbps: int, lossless: bool) -> str:
    if not kbps:
        return "   ?"
    tag = "lossless" if lossless else "kbps"
    return f"{kbps:>4d} {tag}"


# --- progress wrappers -------------------------------------------------------

class _PlainProgress:
    def __init__(self, label: str, total: int) -> None:
        self.label = label
        self.total = max(0, total)
        self.done = 0
        self._last = 0.0

    def advance(self, n: int = 1) -> None:
        self.done += n
        now = time.time()
        if now - self._last > 1.0 or self.done >= self.total:
            print(f"{self.label}... {self.done}/{self.total}", end="\r", flush=True)
            self._last = now

    def close(self) -> None:
        if self.total:
            print()


class _RichProgress:
    def __init__(self, progress: "Progress", task_id: Any) -> None:
        self._p = progress
        self._task = task_id

    def advance(self, n: int = 1) -> None:
        self._p.advance(self._task, n)

    def close(self) -> None:  # context exit handles this
        pass


# --- main UI -----------------------------------------------------------------

class UI:
    """Output helpers. Prefers rich, falls back to plain stdout."""

    def __init__(self, *, use_rich: Optional[bool] = None) -> None:
        if use_rich is None:
            use_rich = HAS_RICH
        self.rich = bool(use_rich and HAS_RICH)
        self._console: Optional["Console"] = Console() if self.rich else None

    # --- basic text ---------------------------------------------------------

    def print(self, msg: str = "", *, style: str = "") -> None:
        if self._console is not None:
            if style:
                self._console.print(msg, style=style)
            else:
                self._console.print(msg)
        else:
            print(_strip_markup(msg))

    def info(self, msg: str, *, dim: bool = False) -> None:
        self.print(msg, style="dim" if dim else "")

    def warning(self, msg: str) -> None:
        if self._console is not None:
            self._console.print(msg, style="yellow")
        else:
            print(_strip_markup(msg), file=sys.stderr)

    def error(self, msg: str) -> None:
        if self._console is not None:
            self._console.print(msg, style="red")
        else:
            print(_strip_markup(msg), file=sys.stderr)

    def success(self, msg: str) -> None:
        if self._console is not None:
            self._console.print(msg, style="green")
        else:
            print(_strip_markup(msg))

    def rule(self, msg: str, *, style: str = "bold") -> None:
        if self._console is not None:
            self._console.rule(f"[{style}]{msg}[/{style}]" if style else msg)
        else:
            print()
            print("=" * 72)
            print(_strip_markup(msg))
            print("=" * 72)

    def panel(self, body: str, *, title: str = "", border_style: str = "") -> None:
        if self._console is not None:
            self._console.print(Panel(body, title=title or None, border_style=border_style or "none"))
        else:
            if title:
                print(f"--- {title} ---")
            print(_strip_markup(body))

    # --- prompts ------------------------------------------------------------

    def prompt(self, text: str) -> str:
        try:
            if self._console is not None:
                return self._console.input(f"[bold]{text}[/bold] ").strip()
            return input(text + " ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return "q"

    # --- progress -----------------------------------------------------------

    @contextmanager
    def progress(self, label: str, total: int) -> Iterator[Any]:
        if self._console is not None:
            with Progress(
                SpinnerColumn(),
                TextColumn(f"[bold]{label}[/bold]"),
                BarColumn(bar_width=None),
                MofNCompleteColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
                transient=False,
                console=self._console,
            ) as p:
                task_id = p.add_task(label, total=total)
                yield _RichProgress(p, task_id)
        else:
            pp = _PlainProgress(label, total)
            try:
                yield pp
            finally:
                pp.close()

    # --- rich-specific passthroughs (callers gate on self.rich) -------------

    @property
    def console(self) -> Optional["Console"]:
        return self._console


def _strip_markup(s: str) -> str:
    """Crude rich-markup stripper for plain output."""
    if not s or "[" not in s:
        return s
    out: list[str] = []
    i = 0
    while i < len(s):
        if s[i] == "[":
            end = s.find("]", i)
            if end != -1 and _looks_like_tag(s[i + 1 : end]):
                i = end + 1
                continue
        out.append(s[i])
        i += 1
    return "".join(out)


def _looks_like_tag(inner: str) -> bool:
    if not inner:
        return False
    if " " in inner and not inner.startswith("/"):
        return False
    token = inner.lstrip("/").strip()
    if not token:
        return False
    known = {
        "bold", "dim", "italic", "underline", "red", "green", "blue",
        "yellow", "cyan", "magenta", "white",
    }
    first = token.split()[0]
    # also accept combined styles like "bold red"
    for part in token.split():
        if part not in known and not part.startswith(("bold", "dim", "italic")):
            if first not in known:
                return False
    return True
