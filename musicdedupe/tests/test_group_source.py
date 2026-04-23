from __future__ import annotations

import threading
import time

from musicdedupe.review import ListGroupSource, QueueGroupSource
from musicdedupe.track import Track


def _g(paths: list[str]) -> list[Track]:
    return [Track(path=p) for p in paths]


def test_list_source_basic() -> None:
    s = ListGroupSource([("identical", _g(["/a", "/b"])), ("meta", _g(["/c", "/d"]))])
    assert s.total_known() == 2
    assert s.finished()
    item = s.get(0)
    assert item is not None and item[0] == "identical"
    assert s.get(5) is None


def test_queue_source_streams_and_stops() -> None:
    s = QueueGroupSource()

    def producer() -> None:
        s.put("identical", _g(["/a", "/b"]))
        time.sleep(0.05)
        s.put("audio", _g(["/c", "/d"]))
        s.close()

    t = threading.Thread(target=producer)
    t.start()

    first = s.get(0, timeout=1.0)
    assert first is not None and first[0] == "identical"
    second = s.get(1, timeout=1.0)
    assert second is not None and second[0] == "audio"
    # After producer closes and everything's drained, get(next) returns None.
    assert s.get(2, timeout=0.2) is None
    t.join()
    assert s.finished()


def test_queue_source_stop_drops_further_puts() -> None:
    s = QueueGroupSource()
    s.put("identical", _g(["/a", "/b"]))
    s.stop()
    # Subsequent puts should be silently dropped.
    s.put("meta", _g(["/c", "/d"]))
    # Anything queued before stop may still be drainable, but dropped puts
    # are gone for good and once drained the source reports finished.
    first = s.get(0, timeout=0.2)
    assert first is not None
    assert s.get(1, timeout=0.2) is None
    assert s.finished()
