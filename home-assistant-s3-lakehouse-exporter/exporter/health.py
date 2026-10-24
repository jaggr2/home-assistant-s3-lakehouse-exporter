"""Health state and the ``/health`` endpoint for the Supervisor watchdog.

The endpoint reports ``ok``, ``degraded`` or ``failed`` plus spool depth, the
last successful flush and the last error. It never exposes credentials: error
messages are passed through the redactor first.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from exporter import __version__

if TYPE_CHECKING:  # pragma: no cover - typing only
    from exporter.batcher import Pressure
    from exporter.redact import Redactor
    from exporter.spool import Spool

LOGGER = logging.getLogger(__name__)

#: Never-flushed + errors for this long after startup means "failed".
FAILED_AFTER_FACTOR = 3
#: No successful flush for this long means "degraded".
DEGRADED_AFTER_FACTOR = 2

OK = "ok"
DEGRADED = "degraded"
FAILED = "failed"


class HealthState:
    """Shared, thread-safe-ish status aggregator (single event loop)."""

    def __init__(
        self,
        spool: Spool,
        *,
        pressure: Pressure | None = None,
        flush_interval: float = 300.0,
        redactor: Redactor | None = None,
        now: Any = None,
    ) -> None:
        self.spool = spool
        self.pressure = pressure
        self.flush_interval = flush_interval
        self.redactor = redactor
        self._now = now or (lambda: datetime.now(UTC))
        self.started_at = self._now()
        self.last_flush_at: datetime | None = None
        self.last_error: str | None = None
        self.flushed_rows = 0
        self.written_objects = 0
        self.dropped_rows = 0
        self.stream_connected = False
        self._fatal = False

    # -- events -----------------------------------------------------------

    def record_flush(self, rows: int, results: Any) -> None:
        self.last_flush_at = self._now()
        self.last_error = None
        self.flushed_rows += rows
        self.written_objects += len(results)

    def record_error(self, error: Exception) -> None:
        self.last_error = self._safe(f"{type(error).__name__}: {error}")

    def record_drops(self, count: int) -> None:
        self.dropped_rows += count

    def set_stream(self, connected: bool) -> None:
        self.stream_connected = connected

    def mark_fatal(self) -> None:
        self._fatal = True

    # -- status -----------------------------------------------------------

    def status(self) -> str:
        now = self._now()
        if self._fatal:
            return FAILED
        age = (now - self.started_at).total_seconds()
        if (
            self.last_flush_at is None
            and self.last_error is not None
            and age > FAILED_AFTER_FACTOR * self.flush_interval
        ):
            return FAILED
        if not self.stream_connected:
            return DEGRADED
        if self.last_error is not None:
            return DEGRADED
        if self.pressure is not None and self.pressure.level > 0:
            return DEGRADED
        if self.dropped_rows > 0:
            return DEGRADED
        if (
            self.last_flush_at is not None
            and (now - self.last_flush_at).total_seconds()
            > DEGRADED_AFTER_FACTOR * self.flush_interval
        ):
            return DEGRADED
        return OK

    def snapshot(self) -> dict:
        return {
            "status": self.status(),
            "version": __version__,
            "uptime_seconds": int((self._now() - self.started_at).total_seconds()),
            "stream_connected": self.stream_connected,
            "spool": {
                "pending_rows": self.spool.pending(),
                "bytes": self.spool.total_bytes(),
                "max_bytes": self.spool.max_bytes,
                "dropped_rows": self.dropped_rows,
            },
            "flushed_rows": self.flushed_rows,
            "written_objects": self.written_objects,
            "last_flush": self.last_flush_at.isoformat() if self.last_flush_at else None,
            "last_error": self.last_error,
        }

    def _safe(self, text: str) -> str:
        return self.redactor.redact(text)[:500] if self.redactor else text[:500]


class HealthServer:
    """Tiny HTTP server exposing ``GET /health``."""

    def __init__(self, state: HealthState, host: str = "0.0.0.0", port: int = 8099) -> None:
        self.state = state
        self.host = host
        self.port = port
        self.bound_port: int | None = None

    async def run(self, stop_event: Any) -> None:
        from aiohttp import web

        async def handle(_request: Any) -> Any:
            snapshot = self.state.snapshot()
            status = snapshot["status"]
            code = 503 if status == FAILED else 200
            return web.json_response(snapshot, status=code)

        app = web.Application()
        app.router.add_get("/health", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        addresses = runner.addresses
        self.bound_port = addresses[0][1] if addresses else self.port
        LOGGER.info("health endpoint listening on port %d", self.bound_port)
        try:
            await stop_event.wait()
        finally:
            await runner.cleanup()
