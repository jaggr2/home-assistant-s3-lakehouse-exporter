"""Micro-batching: triggers, retries and the degrade ladder."""

from __future__ import annotations

import asyncio

from exporter.batcher import Drainer, Pressure, PressureLevel
from exporter.capture import AttributeFilter
from exporter.spool import Spool

from tests.conftest import FakeStore, make_options, ts


class FakeWriter:
    def __init__(self, fail_times: int = 0) -> None:
        self.batches: list[list[int]] = []
        self.fail_times = fail_times
        self.store = FakeStore()

    def write_batch(self, rows):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("storage unavailable")
        self.batches.append([row.id for row in rows])
        return [f"object-{len(self.batches)}"]


class FakeStatus:
    def __init__(self) -> None:
        self.errors: list[Exception] = []
        self.flushes: list[int] = []
        self.drops: list[int] = []

    def record_error(self, err: Exception) -> None:
        self.errors.append(err)

    def record_flush(self, rows: int, results) -> None:
        self.flushes.append(rows)

    def record_drops(self, count: int) -> None:
        self.drops.append(count)


def _spool(tmp_path) -> Spool:
    return Spool(tmp_path / "spool.db", max_bytes=10 * 1024 * 1024)


def _add(spool: Spool, count: int) -> None:
    spool.add_many(
        [("state_changes", ts(2026, 10, 24, 12, minute), {"n": minute}) for minute in range(count)]
    )


async def _run_until(condition, task, timeout: float = 5.0) -> None:
    async def wait() -> None:
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), timeout=timeout)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_row_count_trigger(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("exporter.batcher.IDLE_TICK", 0.02)
    spool = _spool(tmp_path)
    writer = FakeWriter()
    options = make_options(capture={"max_batch_rows": 3, "flush_interval_seconds": 3600})
    drainer = Drainer(spool, writer, options, flush_interval=3600.0)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(drainer.run(stop))
        _add(spool, 3)
        await _run_until(lambda: len(writer.batches) >= 1 and spool.pending() == 0, task)

    asyncio.run(scenario())
    assert writer.batches == [[1, 2, 3]]
    spool.close()


def test_interval_trigger(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("exporter.batcher.IDLE_TICK", 0.02)
    spool = _spool(tmp_path)
    writer = FakeWriter()
    options = make_options(capture={"max_batch_rows": 10_000, "flush_interval_seconds": 3600})
    drainer = Drainer(spool, writer, options, flush_interval=0.05)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(drainer.run(stop))
        _add(spool, 2)
        await _run_until(lambda: len(writer.batches) >= 1 and spool.pending() == 0, task)

    asyncio.run(scenario())
    assert writer.batches == [[1, 2]]
    spool.close()


def test_failed_flush_keeps_rows_for_retry(tmp_path) -> None:
    spool = _spool(tmp_path)
    writer = FakeWriter(fail_times=1)
    status = FakeStatus()
    options = make_options(capture={"max_batch_rows": 100})
    drainer = Drainer(spool, writer, options, status=status)
    _add(spool, 5)

    async def scenario() -> None:
        assert await drainer.flush() == -1
        assert spool.pending() == 5  # nothing lost
        assert status.errors
        assert await drainer.flush() == 5
        assert spool.pending() == 0
        assert status.flushes == [5]

    asyncio.run(scenario())
    spool.close()


def test_flush_writes_at_most_max_batch_rows(tmp_path) -> None:
    spool = _spool(tmp_path)
    writer = FakeWriter()
    options = make_options(capture={"max_batch_rows": 2})
    drainer = Drainer(spool, writer, options)
    _add(spool, 5)

    async def scenario() -> None:
        assert await drainer.flush() == 2
        assert await drainer.flush() == 2
        assert await drainer.flush() == 1
        assert await drainer.flush() == 0

    asyncio.run(scenario())
    assert [len(batch) for batch in writer.batches] == [2, 2, 1]
    spool.close()


def test_byte_trigger_flushes_before_row_count(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("exporter.batcher.IDLE_TICK", 0.02)
    spool = _spool(tmp_path)
    writer = FakeWriter()
    options = make_options(capture={"max_batch_rows": 10_000, "flush_interval_seconds": 3600})
    drainer = Drainer(spool, writer, options, flush_interval=3600.0, max_batch_bytes=2000)

    def add_one(minute: int) -> None:
        spool.add("state_changes", ts(2026, 10, 24, 12, minute % 60), {"pad": "x" * 100})

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(drainer.run(stop))
        await asyncio.sleep(0.15)  # let the initial immediate flush tick pass (empty)

        for minute in range(5):  # ~750 bytes: below the byte target
            add_one(minute)
        await asyncio.sleep(0.15)
        assert writer.batches == []  # neither rows nor bytes triggered a flush

        minute = 5
        while spool.total_bytes() < 2500:  # cross the byte target, still ~30 rows
            add_one(minute)
            minute += 1
        await _run_until(lambda: len(writer.batches) >= 1 and spool.pending() == 0, task)

    asyncio.run(scenario())
    assert writer.batches  # flushed on bytes, far below the row trigger


def test_pressure_ladder(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1000)
    attributes = AttributeFilter("all")
    pressure = Pressure(spool, base_interval=300.0, attribute_filter=attributes)

    def add_until(threshold: int) -> None:
        minute = 0
        while spool.total_bytes() < threshold:
            spool.add("state_changes", ts(2026, 10, 24, 12, minute % 60), {"pad": "x" * 10})
            minute += 1

    assert pressure.update() == PressureLevel.OK
    assert pressure.flush_interval == 300.0

    add_until(820)  # ~82% full
    assert pressure.update() == PressureLevel.DROP_ATTRIBUTES
    assert attributes.effective_mode == "minimal"  # degraded

    add_until(910)  # ~91% full
    assert pressure.update() == PressureLevel.LENGTHEN_FLUSH
    assert pressure.flush_interval == 1200.0  # lengthened (300 * 4)

    add_until(955)  # ~95% full
    assert pressure.update() == PressureLevel.DROP_OLDEST

    spool.drop([row.id for row in spool.fetch(10_000)])
    assert pressure.update() == PressureLevel.OK
    assert attributes.effective_mode == "all"  # restored
    spool.close()


def test_drainer_reports_drops_when_storage_stays_down(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("exporter.batcher.IDLE_TICK", 0.02)
    spool = Spool(tmp_path / "spool.db", max_bytes=500)
    writer = FakeWriter(fail_times=10_000)  # storage stays down
    status = FakeStatus()
    options = make_options(capture={"max_batch_rows": 2, "flush_interval_seconds": 3600})
    drainer = Drainer(spool, writer, options, status=status, flush_interval=0.05)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(drainer.run(stop))
        for minute in range(20):
            spool.add("state_changes", ts(2026, 10, 24, 12, minute), {"pad": "x" * 100})
        await _run_until(lambda: bool(status.drops), task)

    asyncio.run(scenario())
    assert status.drops[0] > 0
    assert spool.total_bytes() <= 500
    assert spool.pending() > 0  # the newest rows are still queued
    spool.close()
