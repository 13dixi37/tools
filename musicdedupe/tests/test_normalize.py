from __future__ import annotations

import pytest

from musicdedupe.grouping import (
    HAS_RAPIDFUZZ,
    _fuzzy_merge,
    normalize_text,
    normalize_title,
)
from musicdedupe.track import Track


def test_normalize_text_strips_diacritics_and_case() -> None:
    assert normalize_text("Beyoncé") == "beyonce"
    assert normalize_text("Beyonce") == "beyonce"
    assert normalize_text("  The   Beatles  ") == "the beatles"


def test_normalize_text_strips_punctuation() -> None:
    assert normalize_text("AC/DC") == "ac dc"
    assert normalize_text("Mötley Crüe!") == "motley crue"


def test_normalize_text_empty() -> None:
    assert normalize_text("") == ""
    assert normalize_text(None) == ""  # type: ignore[arg-type]


def test_normalize_title_drops_bracket_tags() -> None:
    assert normalize_title("Song (Radio Edit)") == "song"
    assert normalize_title("Song [2012 Remaster]") == "song"
    assert normalize_title("Song {Live}") == "song"
    # Multiple tags collapse too.
    assert normalize_title("Song (feat. X) [Remaster]") == "song"


def test_normalize_title_preserves_body() -> None:
    assert normalize_title("Song Two Words") == "song two words"


@pytest.mark.skipif(not HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
def test_fuzzy_merge_catches_typo() -> None:
    tracks = [
        Track(path="/m/a.mp3", artist="The Beatles", title="Hey Jude", duration=431.0),
        Track(path="/m/b.mp3", artist="The Beatls", title="Hey Jude", duration=430.5),
        Track(path="/m/c.mp3", artist="Other Band", title="Different Song", duration=180.0),
    ]
    merged = _fuzzy_merge(tracks, threshold=92.0)
    # One merged group of 2; singleton stays out.
    assert len(merged) == 1
    paths = {t.path for t in merged[0]}
    assert paths == {"/m/a.mp3", "/m/b.mp3"}


@pytest.mark.skipif(not HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
def test_fuzzy_merge_respects_duration_window() -> None:
    # Same artist+title text but duration differs by 10s → no merge.
    tracks = [
        Track(path="/m/a.mp3", artist="Band", title="Song", duration=200.0),
        Track(path="/m/b.mp3", artist="Band", title="Sonng", duration=215.0),
    ]
    merged = _fuzzy_merge(tracks, threshold=92.0)
    assert merged == []
