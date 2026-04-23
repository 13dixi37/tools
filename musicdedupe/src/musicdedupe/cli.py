"""Command-line entry point and scan/grouping orchestration."""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from .cache import TrackCache
from .grouping import HAS_RAPIDFUZZ, group_all
from .hashing import HAS_BLAKE3, HAS_XXHASH, resolve_algo
from .review import (
    GroupSource,
    ListGroupSource,
    QueueGroupSource,
    ReviewResult,
    interactive_review,
    render_final_confirmation,
)
from .delete import do_delete, show_corrupted
from .scan import (
    HAS_FFPLAY,
    HAS_FFPROBE,
    HAS_FPCALC,
    find_audio_files,
    scan_file,
)
from .track import Track
from .ui import HAS_TRASH, UI

try:
    from rich.panel import Panel
    from rich.table import Table
except ImportError:  # pragma: no cover
    Panel = Table = None  # type: ignore[assignment,misc]


GROUP_LABEL_ORDER = ("identical", "audio", "meta")
GROUP_LABELS = {
    "identical": "Byte-identical copies",
    "audio": "Same audio content (different encoding/bitrate/format)",
    "meta": "Same artist & title (possibly different versions/remixes)",
}


def _scan_library(
    ui: UI,
    files: list[str],
    cache: Optional[TrackCache],
    workers: int,
) -> list[Track]:
    tracks: list[Track] = []
    todo: list[str] = []

    for p in files:
        try:
            st = os.stat(p)
        except OSError:
            todo.append(p)
            continue
        if cache is not None:
            hit = cache.get(p, size=st.st_size, mtime=st.st_mtime)
            if hit is not None:
                tracks.append(hit)
                continue
        todo.append(p)

    cached_n = len(tracks)
    if cached_n:
        ui.info(f"Using cached data for {cached_n} file(s).", dim=True)

    if not todo:
        return tracks

    flush_every = 200
    with ui.progress("Scanning", total=len(todo)) as prog:
        with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futs = {ex.submit(scan_file, p): p for p in todo}
            batch: list[Track] = []
            for fut in cf.as_completed(futs):
                p = futs[fut]
                try:
                    t = fut.result()
                except Exception as e:  # noqa: BLE001
                    t = Track(path=p, ext=Path(p).suffix.lower(),
                              corrupted=True, error=f"scan error: {e}")
                tracks.append(t)
                batch.append(t)
                prog.advance()
                if cache is not None and len(batch) >= flush_every:
                    cache.upsert_many(batch)
                    batch = []
            if cache is not None and batch:
                cache.upsert_many(batch)
    return tracks


def _run_grouping_thread(
    source: QueueGroupSource,
    tracks: list[Track],
    *,
    skip: set[str],
    algo: str,
    hash_workers: int,
    fuzzy: bool,
    has_fpcalc: bool,
    cache: Optional[TrackCache],
    error_holder: list[BaseException],
) -> None:
    try:
        group_all(
            tracks,
            skip=skip,
            algo=algo,
            hash_workers=hash_workers,
            fuzzy=fuzzy,
            has_fpcalc=has_fpcalc,
            on_group=source.put,
        )
        # Persist hash results computed during grouping.
        if cache is not None:
            cache.upsert_many(tracks)
    except BaseException as e:  # noqa: BLE001
        error_holder.append(e)
    finally:
        source.close()


def _build_source(
    tracks: list[Track],
    *,
    streaming: bool,
    skip: set[str],
    algo: str,
    hash_workers: int,
    fuzzy: bool,
    has_fpcalc: bool,
    cache: Optional[TrackCache],
) -> tuple[GroupSource, Optional[threading.Thread], list[BaseException]]:
    errors: list[BaseException] = []
    if not streaming:
        result = group_all(
            tracks,
            skip=skip,
            algo=algo,
            hash_workers=hash_workers,
            fuzzy=fuzzy,
            has_fpcalc=has_fpcalc,
        )
        if cache is not None:
            cache.upsert_many(tracks)
        return ListGroupSource(result), None, errors

    queue_source = QueueGroupSource()
    t = threading.Thread(
        target=_run_grouping_thread,
        args=(queue_source, tracks),
        kwargs=dict(
            skip=skip, algo=algo, hash_workers=hash_workers, fuzzy=fuzzy,
            has_fpcalc=has_fpcalc, cache=cache, error_holder=errors,
        ),
        daemon=True,
    )
    t.start()
    return queue_source, t, errors


def _print_summary(ui: UI, source: GroupSource) -> None:
    collected: dict[str, list[list[Track]]] = {k: [] for k in GROUP_LABEL_ORDER}
    idx = 0
    while True:
        item = source.get(idx, timeout=0)
        if item is None:
            break
        kind, group = item
        collected.setdefault(kind, []).append(group)
        idx += 1
    if ui.rich and Panel is not None and Table is not None:
        assert ui.console is not None
        summary = Table(show_header=True, header_style="bold", box=None)
        summary.add_column("Category")
        summary.add_column("Groups", justify="right")
        summary.add_column("Files", justify="right")
        for key in GROUP_LABEL_ORDER:
            gs = collected.get(key, [])
            summary.add_row(GROUP_LABELS[key], str(len(gs)), str(sum(len(g) for g in gs)))
        ui.console.print(Panel(summary, title="Duplicate scan summary", border_style="cyan"))
    else:
        for key in GROUP_LABEL_ORDER:
            gs = collected.get(key, [])
            print(f"  {key}: {len(gs)} groups, {sum(len(g) for g in gs)} files")


def _print_banner(ui: UI, root: str, algo: str) -> None:
    if ui.rich and ui.console is not None:
        ui.console.rule("[bold]musicdedupe[/bold]")
        ui.console.print(
            f"  scanning: [bold]{root}[/bold]\n"
            f"  ffprobe:  {'yes' if HAS_FFPROBE else '[red]missing[/red]  (duration/bitrate less accurate)'}\n"
            f"  fpcalc:   {'yes' if HAS_FPCALC else '[red]missing[/red]  (audio fingerprinting disabled)'}\n"
            f"  ffplay:   {'yes' if HAS_FFPLAY else '[red]missing[/red]  (preview disabled)'}\n"
            f"  trash:    {'yes' if HAS_TRASH else '[yellow]missing[/yellow] (will move to a folder instead of trashing)'}\n"
            f"  hash:     {algo}  "
            f"[dim](blake3={'y' if HAS_BLAKE3 else 'n'}, xxhash={'y' if HAS_XXHASH else 'n'})[/dim]\n"
            f"  fuzzy:    {'yes' if HAS_RAPIDFUZZ else '[yellow]missing[/yellow]  (install rapidfuzz for typo-tolerant grouping)'}"
        )
    else:
        print(f"musicdedupe  scanning: {root}")
        print(f"  ffprobe: {HAS_FFPROBE}  fpcalc: {HAS_FPCALC}  ffplay: {HAS_FFPLAY}  "
              f"send2trash: {HAS_TRASH}  hash: {algo}  rapidfuzz: {HAS_RAPIDFUZZ}")


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="musicdedupe",
        description="Interactive music library deduplicator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("path", help="Music directory to scan.")
    ap.add_argument("--cache", default=None,
                    help="Path to scan cache SQLite DB (default: <path>/.musicdedupe-cache.db)")
    ap.add_argument("--no-cache", action="store_true", help="Disable scan cache.")
    ap.add_argument("--workers", type=int, default=4, help="Parallel scan workers (default: 4).")
    ap.add_argument("--hash-workers", type=int, default=max(1, (os.cpu_count() or 4)),
                    help="Process-pool workers for hashing (default: CPU count).")
    ap.add_argument("--hash-algo", choices=["auto", "blake3", "xxhash", "sha1"],
                    default="auto", help="Hash algorithm for identical-byte detection.")
    ap.add_argument("--play-start", type=int, default=30,
                    help="Preview start offset in seconds (default: 30).")
    ap.add_argument("--play-length", type=int, default=15,
                    help="Preview length in seconds (default: 15).")
    ap.add_argument("--delete-mode", choices=["trash", "move", "remove"], default=None,
                    help="How to delete. Default: trash if available, else move to ./musicdedupe-removed.")
    ap.add_argument("--move-to", default="musicdedupe-removed",
                    help="Destination dir when --delete-mode=move.")
    ap.add_argument("--skip", choices=["identical", "audio", "meta"], action="append", default=[],
                    help="Skip a category (can be repeated).")
    ap.add_argument("--no-stream", action="store_true",
                    help="Don't stream groups; wait until all are computed before reviewing.")
    ap.add_argument("--no-fuzzy", action="store_true",
                    help="Disable Levenshtein fuzzy matching in the metadata stage.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Do not delete anything; just print the plan.")
    ap.add_argument("--follow-symlinks", action="store_true")
    ap.add_argument("--no-rich", action="store_true",
                    help="Force plain output even if rich is installed.")
    args = ap.parse_args(argv)

    ui = UI(use_rich=None if not args.no_rich else False)

    root = os.path.abspath(args.path)
    if not os.path.isdir(root):
        sys.stderr.write(f"Not a directory: {root}\n")
        return 2

    algo = resolve_algo(args.hash_algo)
    _print_banner(ui, root, algo)

    delete_mode = args.delete_mode or ("trash" if HAS_TRASH else "move")

    # Cache setup.
    cache_path: Optional[str] = None
    if not args.no_cache:
        cache_path = args.cache or os.path.join(root, ".musicdedupe-cache.db")

    cache: Optional[TrackCache] = None
    if cache_path:
        cache = TrackCache(cache_path)
        # One-shot migration from the old JSON format.
        legacy = os.path.join(os.path.dirname(cache_path), ".musicdedupe-cache.json")
        imported = cache.migrate_json(legacy)
        if imported:
            ui.info(f"Migrated {imported} entries from legacy JSON cache.", dim=True)

    try:
        files = find_audio_files(root, follow_symlinks=args.follow_symlinks)
        ui.print(f"Found {len(files)} audio file(s).")
        if not files:
            return 0

        t0 = time.time()
        tracks = _scan_library(ui, files, cache, workers=args.workers)
        ui.info(f"Scanned in {time.time() - t0:.1f}s.", dim=True)

        corrupted = [t for t in tracks if t.corrupted]
        healthy = [t for t in tracks if not t.corrupted]
        ui.print(f"{len(healthy)} ok, {len(corrupted)} corrupted/unreadable.")

        skip = set(args.skip)

        # Corrupted triage first.
        to_delete: list[Track] = []
        corrupted_picks = show_corrupted(ui, corrupted)
        to_delete.extend(corrupted_picks)

        source, grouping_thread, errors = _build_source(
            healthy,
            streaming=not args.no_stream,
            skip=skip,
            algo=algo,
            hash_workers=args.hash_workers,
            fuzzy=not args.no_fuzzy,
            has_fpcalc=HAS_FPCALC,
            cache=cache,
        )

        # In --no-stream mode we can summarize up front; otherwise skip it
        # (streaming ticker shows "Group N/M+" and fills in as discovery runs).
        if args.no_stream:
            _print_summary(ui, source)

        review_result = ReviewResult()
        first = source.get(0, timeout=0.5)
        if first is None and source.finished():
            ui.success("No duplicate groups found.")
        else:
            ui.print("\nReviewing duplicate groups. Type ? for help at any prompt.\n")
            review_result = interactive_review(
                ui, source,
                play_start=args.play_start, play_length=args.play_length,
            )
            to_delete.extend(review_result.to_delete)

        source.stop()
        if grouping_thread is not None:
            grouping_thread.join(timeout=5.0)
        if errors:
            ui.error(f"Grouping thread error: {errors[0]}")

        if not to_delete:
            ui.success("Nothing marked for deletion. Done.")
            return 0

        seen: set[str] = set()
        unique: list[Track] = []
        for t in to_delete:
            if t.path in seen:
                continue
            seen.add(t.path)
            unique.append(t)

        render_final_confirmation(ui, review_result, corrupted=corrupted_picks)

        if args.dry_run:
            ui.warning("--dry-run: not deleting anything.")
            return 0

        mode_desc = {
            "trash": "Move to system trash",
            "move": f"Move to: {args.move_to}",
            "remove": "PERMANENTLY DELETE (no trash)",
        }[delete_mode]
        confirm = ui.prompt(f"{mode_desc}? [y/N]")
        if not confirm.lower().startswith("y"):
            ui.warning("Aborted. No files were deleted.")
            return 0

        n = do_delete(ui, unique, mode=delete_mode, move_to=args.move_to)
        ui.success(f"Done. {n}/{len(unique)} file(s) processed.")

        if cache is not None:
            cache.delete_paths(t.path for t in unique)

        return 0
    finally:
        if cache is not None:
            cache.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print()
        raise SystemExit(130)
