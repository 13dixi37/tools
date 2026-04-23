"""Deletion (trash/move/remove) and corrupted-file triage."""
from __future__ import annotations

import os
import shutil
from typing import Optional

from .review import parse_indices
from .track import Track
from .ui import UI, HAS_TRASH

try:
    from rich.table import Table
except ImportError:  # pragma: no cover
    Table = None  # type: ignore[assignment,misc]

try:
    from send2trash import send2trash  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    send2trash = None  # type: ignore[assignment]


def do_delete(
    ui: UI,
    tracks: list[Track],
    mode: str,
    move_to: Optional[str] = None,
) -> int:
    """mode: 'trash' | 'remove' | 'move'. Returns number of successes."""
    n = 0
    for t in tracks:
        try:
            if mode == "trash":
                if not HAS_TRASH or send2trash is None:
                    raise RuntimeError("send2trash not installed")
                send2trash(t.path)
            elif mode == "move":
                dest_dir = move_to or "musicdedupe-removed"
                os.makedirs(dest_dir, exist_ok=True)
                base = os.path.basename(t.path)
                target = os.path.join(dest_dir, base)
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
            ui.error(f"FAILED to delete {t.path}: {e}")
    return n


def show_corrupted(ui: UI, bad: list[Track]) -> list[Track]:
    """Offer to delete corrupted files. Returns list to delete."""
    if not bad:
        return []
    header = f"Found {len(bad)} file(s) that failed to decode (likely corrupted)"
    if ui.rich and Table is not None:
        assert ui.console is not None
        ui.console.rule(f"[bold red]{header}[/bold red]")
        table = Table(show_header=True, header_style="bold")
        table.add_column("#", justify="right", width=4)
        table.add_column("File", overflow="fold")
        table.add_column("Error", overflow="fold")
        for i, t in enumerate(bad, 1):
            table.add_row(str(i), t.path, t.error or "?")
        ui.console.print(table)
    else:
        print(header)
        for i, t in enumerate(bad, 1):
            print(f"[{i}] {t.path}  ({t.error})")
    choice = ui.prompt("Delete all corrupted? [y/N/select]")
    if choice.lower().startswith("y"):
        return list(bad)
    if choice.lower().startswith("s"):
        picks = ui.prompt("Indices to delete (e.g. 1 3 5-7):")
        idxs = parse_indices(picks.split(), len(bad))
        return [bad[i] for i in idxs]
    return []
