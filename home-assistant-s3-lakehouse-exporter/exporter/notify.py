"""Optional status webhook notifications.

The webhook is POSTed on **status transitions only** (``ok`` -> ``degraded``
-> ``failed`` ...), never per event. The webhook URL is a secret: it is never
written to logs or to the health endpoint, and failures are logged with the
exception type only (exception texts can embed the URL).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from exporter.health import HealthState

LOGGER = logging.getLogger(__name__)

WEBHOOK_TIMEOUT = 10.0
MONITOR_INTERVAL = 5.0


class Notifier:
    def __init__(self, url: str | None) -> None:
        self._url = url or None
        if self._url is None:
            LOGGER.info("notify webhook disabled")

    async def notify(self, old_status: str, new_status: str, details: dict) -> None:
        if self._url is None:
            return
        import aiohttp

        payload = {
            "event": "status_change",
            "old_status": old_status,
            "new_status": new_status,
            "timestamp": datetime.now(UTC).isoformat(),
            "details": details,
        }
        try:
            timeout = aiohttp.ClientTimeout(total=WEBHOOK_TIMEOUT)
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.post(self._url, json=payload) as response,
            ):
                if response.status >= 400:
                    LOGGER.warning("notify webhook returned HTTP %d", response.status)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            LOGGER.warning("notify webhook failed: %s", type(err).__name__)


class Monitor:
    """Watches :class:`HealthState` and fires the notifier on transitions."""

    def __init__(
        self,
        state: HealthState,
        notifier: Notifier,
        *,
        capture: Any | None = None,
        interval: float = MONITOR_INTERVAL,
    ) -> None:
        self.state = state
        self.notifier = notifier
        self.capture = capture
        self.interval = interval

    async def run(self, stop_event: Any) -> None:
        previous = self.state.status()
        while not stop_event.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=self.interval)
            if self.capture is not None:
                self.state.set_stream(bool(self.capture.stream_connected))
            current = self.state.status()
            if current != previous:
                LOGGER.warning("health status %s -> %s", previous, current)
                await self.notifier.notify(previous, current, self.state.snapshot())
                previous = current
