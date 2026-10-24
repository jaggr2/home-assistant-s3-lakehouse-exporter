"""Spool: crash safety, ordering, byte cap / drop-oldest behaviour."""

from __future__ import annotations

from datetime import UTC, datetime

from exporter.spool import Spool, iso

from tests.conftest import ts


def test_add_fetch_drop_roundtrip(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    first = spool.add("state_changes", ts(2026, 10, 24, 12), {"entity_id": "sensor.a"})
    second = spool.add("events", ts(2026, 10, 24, 12, 1), {"event_type": "call_service"})
    assert (first, second) == (1, 2)

    rows = spool.fetch(10)
    assert [row.id for row in rows] == [1, 2]
    assert rows[0].payload == {"entity_id": "sensor.a"}
    assert rows[0].event_time == "2026-10-24T12:00:00Z"
    assert spool.pending() == 2

    spool.drop([1])
    assert [row.id for row in spool.fetch(10)] == [2]
    spool.close()


def test_fetch_respects_limit_and_order(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    spool.add_many(
        [("events", ts(2026, 10, 24, 12, minute), {"n": minute}) for minute in range(5)]
    )
    assert [row.payload["n"] for row in spool.fetch(3)] == [0, 1, 2]
    spool.close()


def test_rows_survive_crash_before_ack(tmp_path) -> None:
    """Rows not yet acknowledged to storage must survive a restart."""
    path = tmp_path / "spool.db"
    spool = Spool(path, max_bytes=1024 * 1024)
    spool.add("state_changes", ts(2026, 10, 24, 12), {"entity_id": "sensor.a"})
    spool.add("state_changes", ts(2026, 10, 24, 12, 5), {"entity_id": "sensor.b"})
    spool.close()  # simulated crash: nothing was dropped

    reopened = Spool(path, max_bytes=1024 * 1024)
    rows = reopened.fetch(10)
    assert [row.payload["entity_id"] for row in rows] == ["sensor.a", "sensor.b"]
    # the recovery watermark survives too, for gap repair
    assert reopened.last_event_time() == ts(2026, 10, 24, 12, 5)
    reopened.close()


def test_last_event_time_tracks_maximum(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    assert spool.last_event_time() is None
    spool.add("events", ts(2026, 10, 24, 12, 30), {})
    spool.add("events", ts(2026, 10, 24, 12, 10), {})  # older
    assert spool.last_event_time() == ts(2026, 10, 24, 12, 30)
    spool.close()


def test_enforce_cap_drops_oldest_rows(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024)
    for minute in range(20):
        spool.add("state_changes", ts(2026, 10, 24, 12, minute), {"pad": "x" * 100, "n": minute})
    assert spool.total_bytes() > 1024

    dropped = spool.enforce_cap()
    assert dropped == 20 - spool.pending()
    assert dropped > 0
    assert spool.total_bytes() <= 1024
    # the newest rows survive, the oldest are gone
    remaining = [row.payload["n"] for row in spool.fetch(100)]
    assert remaining == list(range(20 - len(remaining), 20))
    spool.close()


def test_enforce_cap_is_noop_under_cap(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024)
    spool.add("events", ts(2026, 10, 24, 12), {"small": True})
    assert spool.enforce_cap() == 0
    assert spool.pending() == 1
    spool.close()


def test_total_bytes_accounting(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024)
    spool.add("events", ts(2026, 10, 24, 12), {"a": 1})
    one = spool.total_bytes()
    assert one > 0
    spool.add("events", ts(2026, 10, 24, 12, 1), {"b": 2})
    assert spool.total_bytes() > one
    assert spool.stats() == (2, spool.total_bytes())
    spool.drop([1, 2])
    assert spool.stats() == (0, 0)
    spool.close()


def test_iter_fetch_streams_in_pages(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    spool.add_many([("events", ts(2026, 10, 24, 12), {"n": n}) for n in range(25)])
    streamed = list(spool.iter_fetch(10, chunk_size=3))
    assert [row.payload["n"] for row in streamed] == list(range(10))
    assert spool.fetch(10) == streamed  # same rows as the buffered fetch
    spool.close()


def test_naive_datetimes_are_treated_as_utc(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024)
    spool.add("events", datetime(2026, 10, 24, 12), {})
    assert spool.fetch(1)[0].event_time == iso(datetime(2026, 10, 24, 12, tzinfo=UTC))
    spool.close()
