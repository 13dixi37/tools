# musicdedupe

Interactive CLI to find and clean up duplicate music files — across
formats, bitrates, filenames, and versions — plus detects files that
won't decode.

## What it catches

| Kind                  | How                                     | Example                                           |
|-----------------------|-----------------------------------------|---------------------------------------------------|
| Byte-identical copies | SHA1 of file contents                   | `song.mp3` and `song (1).mp3` (same bytes)        |
| Same audio content    | Chromaprint acoustic fingerprint        | `song.flac` and `song.mp3` (same audio, different encoding) |
| Version / remix       | Normalized artist + title (loose match) | `Song`, `Song (Radio Edit)`, `Song (Extended Mix)` |
| Corrupted             | ffprobe decode test                     | truncated downloads, zero-length audio            |

Byte-identical and same-audio groups are shown with an auto-pick option
(keep best quality, delete the rest). Version/remix groups always
require a manual decision — because a remix isn't really a duplicate.

Requires Python 3.10+.

## Install

**Recommended** — install with pipx and pull in every optional dep:

```
pipx install "musicdedupe[all] @ file://$PWD"
# then just:
musicdedupe /path/to/music
```

`[all]` brings in `rich` (prettier output), `send2trash` (safe deletion),
`xxhash` + `blake3` (fast hashing), and `rapidfuzz` (typo-tolerant
metadata grouping). Each can also be installed individually via
`[rich]`, `[trash]`, `[fast]`, `[fuzzy]`.

**Dev install:**

```
pip install -e ".[all,dev]"
pytest
```

**Running without installing:**

```
pip install mutagen
python3 musicdedupe/musicdedupe.py /path/to/music
```

**System tools:**

| Tool     | macOS                         | Debian/Ubuntu                      | Arch                          |
|----------|-------------------------------|------------------------------------|-------------------------------|
| ffmpeg   | `brew install ffmpeg`         | `apt install ffmpeg`               | `pacman -S ffmpeg`            |
| fpcalc   | `brew install chromaprint`    | `apt install libchromaprint-tools` | `pacman -S chromaprint`       |

Without `fpcalc`, same-audio grouping is disabled (still catches
byte-identical and same-tag duplicates). Without `ffmpeg`, metadata
reading falls back to mutagen (slightly less accurate), and preview is
disabled.

## Usage

```
musicdedupe /path/to/music
```

The tool scans once (SQLite cache, so subsequent runs are instant), then
streams duplicate groups to you as they are discovered. You can start
reviewing the byte-identical matches while the fingerprint and metadata
passes are still running in the background.

**Commands at each group:**

```
<n>           keep only file n — mark the rest for deletion and move on
k <n>[ <n>…]  mark file(s) n as keep
d <n>[ <n>…]  mark file(s) n for deletion  (supports ranges: d 2-4)
u <n>[ <n>…]  unmark file(s) n
p <n>[ <n>…]  play a snippet of file(s) n  (plays each in turn)
i <n>         show detailed info for file n
a             auto: keep the highest-quality file, delete the rest
              (only for byte-identical / same-audio groups)
n  or  s      next / skip this group without changes
b             back to previous group
q             quit and review the deletion list
?             help
```

Nothing is deleted until the very end — you get a full summary and a
`y/N` confirmation before any file is touched.

## Flags

```
--dry-run            Show the deletion plan but don't delete anything
--delete-mode MODE   trash (default if send2trash installed),
                     move (to a folder), or remove (permanent rm)
--move-to DIR        Destination when --delete-mode=move
                     (default: ./musicdedupe-removed)
--skip CAT           Skip a category: identical / audio / meta
                     (repeatable)
--cache PATH         Scan cache location
                     (default: <music-dir>/.musicdedupe-cache.db)
--no-cache           Disable caching
--workers N          Parallel scan workers (default: 4)
--hash-workers N     Process-pool workers for hashing (default: CPU count)
--hash-algo A        blake3 / xxhash / sha1 / auto (default: auto picks
                     the fastest available)
--no-stream          Compute every group before starting review
--no-fuzzy           Disable Levenshtein metadata matching
--no-rich            Force plain output even if rich is installed
--play-start SEC     Preview offset into song (default: 30s)
--play-length SEC    Preview length (default: 15s)
--follow-symlinks    Follow symlinks while walking
```

## Performance notes

- Byte-identical detection is two-stage: first a cheap partial hash of
  the first and last 1 MiB (inside each same-size bucket), then a full
  hash only for files whose partial hashes collide. On large FLAC
  libraries this short-circuits almost every comparison without reading
  the full file.
- Hashing runs in a process pool (GIL-free). Use `--hash-workers` to
  tune.
- The cache is SQLite (WAL mode). Updates are O(1) per file and there's
  no full-file rewrite. A legacy `.musicdedupe-cache.json` is migrated
  automatically on first run.
- Fuzzy metadata grouping uses rapidfuzz when available and only merges
  pairs within 2 seconds of duration, which catches "The Beatls" vs
  "The Beatles" without collapsing genuinely distinct tracks.

## Recommended workflow

1. **Dry run first** to see what it wants to do:
   `musicdedupe ~/Music --dry-run`
2. **Start with the obvious group:** only review byte-identical for now:
   `musicdedupe ~/Music --skip audio --skip meta`
   Tap `a` then Enter on each — pure auto-cleanup.
3. **Second pass:** same-audio groups:
   `musicdedupe ~/Music --skip meta`
   Use `p 1` / `p 2` to A/B if unsure between a FLAC and a high-bitrate MP3.
4. **Third pass:** version/remix candidates:
   `musicdedupe ~/Music --skip identical --skip audio`
   These need your judgment — use `p` liberally.

## Safety

- Default delete mode is **system trash** (via `send2trash`), so nothing
  is gone for real unless you empty trash.
- If `send2trash` isn't installed, it falls back to **moving** files into
  `./musicdedupe-removed/` — still recoverable.
- Use `--delete-mode remove` only if you know what you're doing.
- The scan cache lives at `<music-dir>/.musicdedupe-cache.db`. Delete it
  to force a fresh scan.

## How quality is scored

When the tool offers `★` and `a` (auto) picks a "best" copy, it ranks by:

1. Lossless formats (FLAC, WAV, AIFF, APE, WavPack) beat lossy.
2. Higher bitrate wins within the same class.
3. More complete metadata wins on ties.
4. Higher sample rate as a final tiebreaker.

A corrupted file always scores lowest and is never the "best".
