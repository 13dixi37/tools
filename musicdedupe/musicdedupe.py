#!/usr/bin/env python3
"""Backwards-compat shim: `python3 musicdedupe/musicdedupe.py <path>`.

Prefer `pipx install .` (or `pip install -e .`) and run `musicdedupe` on
PATH; the real code lives in src/musicdedupe/.
"""
from __future__ import annotations


def _run() -> int:
    import os
    import sys

    here = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(here, "src")
    if os.path.isdir(os.path.join(src, "musicdedupe")):
        # Remove the script directory from sys.path so `import musicdedupe`
        # doesn't resolve to this file (which is a module, not a package).
        while here in sys.path:
            sys.path.remove(here)
        if src not in sys.path:
            sys.path.insert(0, src)

    from musicdedupe.cli import main

    return main()


if __name__ == "__main__":
    import sys

    try:
        sys.exit(_run())
    except KeyboardInterrupt:
        print()
        sys.exit(130)
