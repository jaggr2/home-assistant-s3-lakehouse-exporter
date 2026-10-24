"""Micro-batching and the background drainer.

Flushes happen every ``flush_interval_seconds``, every ``max_batch_rows``
rows, or every ``TARGET_OBJECT_BYTES`` of payload — whichever comes first. Under spool pressure
the exporter **degrades instead of growing**:

1. attribute capture drops to ``minimal`` (see
   :class:`exporter.capture.AttributeFilter`),
2. the flush interval is lengthened (smaller, less frequent batches),
3. the oldest spooled batches are dropped — loudly — before the cap is hit.

Rows are only deleted from the spool after the write succeeded, so a crash or
a storage outage can never lose data (at-least-once delivery).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from enum import IntEnum
from typing import TYPE_CHECKING, Any

from exporter.writers.base import TARGET_OBJECT_BYTES

if TYPE_CHECKING:  # pragma: no cover - typing only
    from exporter.capture import AttributeFilter
    from exporter.spool import Spool

LOGGER = logging.getLogger(__name__)

#: Seconds to wait before retrying a failed flush.
RETRY_SECONDS = 15.0
#: Longest time the loop sleeps without checking the stop event.
IDLE_TICK = 1.0


class PressureLevel(IntEnum):
    OK = 0
    DROP_ATTRIBUTES = 1
    LENGTHEN_FLUSH = 2
    DROP_OLDEST = 3


#: Spool fill ratio at which each degrade stage kicks in.
THRESHOLDS = {
    PressureLevel.DROP_ATTRIBUTES: 0.80,
    PressureLevel.LENGTHEN_FLUSH: 0.90,
    PressureLevel.DROP_OLDEST: 0.95,
}


class Pressure:
    """Tracks spool fill level and applies the degrade ladder."""

    def __init__(
        self,
        spool: Spool,
        base_interval: float,
        attribute_filter: AttributeFilter | None = None,
    ) -> None:
        self._spool = spool
        self._base_interval = base_interval
        self._attributes = attribute_filter
        self._level = PressureLevel.OK

    @property
    def level(self) -> PressureLevel:
        return self._level

    @property
    def flush_interval(self) -> float:
        if self._level >= PressureLevel.LENGTHEN_FLUSH:
            return min(self._base_interval * 4, 3600.0)
        return self._base_interval

    @property
    def fill_ratio(self) -> float:
        max_bytes = self._spool.max_bytes
        return (self._spool.total_bytes() / max_bytes) if max_bytes else 0.0

    def update(self) -> PressureLevel:
        fill = self.fill_ratio
        level = PressureLevel.OK
        for candidate in (
            PressureLevel.DROP_ATTRIBUTES,
            PressureLevel.LENGTHEN_FLUSH,
            PressureLevel.DROP_OLDEST,
        ):
            if fill >= THRESHOLDS[candidate]:
                level = candidate
        if level != self._level:
            LOGGER.warning(
                "spool pressure %s -> %s (fill %.0f%%)",
                self._level.name,
                level.name,
                fill * 100,
            )
            self._level = level
        if self._attributes is not None:
            if level >= PressureLevel.DROP_ATTRIBUTES:
                self._attributes.degrade()
            else:
                self._attributes.restore()
        return self._level


class Drainer:
    """Batches spooled rows and writes them through the configured writer."""

    def __init__(
        self,
        spool: Spool,
        writer: Any,
        options: Any,
        *,
        pressure: Pressure | None = None,
        status: Any | None = None,
        flush_interval: float | None = None,
        max_batch_bytes: int | None = None,
        now: Any = None,
    ) -> None:
        self.spool = spool
        self.writer = writer
        self.options = options
        self.pressure = pressure or Pressure(spool, options.capture.flush_interval_seconds)
        self.status = status
        self._interval = (
            flush_interval
            if flush_interval is not None
            else float(options.capture.flush_interval_seconds)
        )
        self._now = now or time.monotonic
        self.max_batch_rows = options.capture.max_batch_rows
        self.max_batch_bytes = max_batch_bytes or TARGET_OBJECT_BYTES

    async def run(self, stop_event: asyncio.Event) -> None:
        next_due = self._now()
        while not stop_event.is_set():
            self.pressure.update()
            now = self._now()
            pending_rows, pending_bytes = self.spool.stats()
            due = (
                pending_rows >= self.max_batch_rows
                or pending_bytes >= self.max_batch_bytes
                or now >= next_due
            )
            if due:
                written = await self.flush()
                interval = self.pressure.flush_interval if written >= 0 else RETRY_SECONDS
                next_due = self._now() + interval
            await self._sleep(stop_event, max(next_due - self._now(), 0.0))

    async def _sleep(self, stop_event: asyncio.Event, seconds: float) -> None:
        if seconds <= 0:
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=min(seconds, IDLE_TICK))

    async def flush(self) -> int:
        """Flush one batch. Returns rows written, ``-1`` on failure."""
        return await asyncio.to_thread(self._flush_sync)

    def _flush_sync(self) -> int:
        rows = self.spool.fetch(self.max_batch_rows)
        if not rows:
            self._enforce_cap()
            return 0
        try:
            results = self.writer.write_batch(rows)
        except Exception as err:
            LOGGER.error(
                "flush failed (%s: %s); %d rows stay spooled",
                type(err).__name__,
                err,
                len(rows),
            )
            if self.status is not None:
                self.status.record_error(err)
            self._enforce_cap()
            return -1
        self.spool.drop([row.id for row in rows])
        self._enforce_cap()
        LOGGER.info("flushed %d rows in %d object(s)", len(rows), len(results))
        if self.status is not None:
            self.status.record_flush(len(rows), results)
        return len(rows)

    def _enforce_cap(self) -> None:
        dropped = self.spool.enforce_cap()
        if dropped and self.status is not None:
            self.status.record_drops(dropped)
