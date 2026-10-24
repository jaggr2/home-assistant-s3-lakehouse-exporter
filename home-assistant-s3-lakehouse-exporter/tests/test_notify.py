"""Notify webhook: end-to-end delivery, and what may never leave the process."""

from __future__ import annotations

import asyncio
import logging

from exporter.health import HealthState
from exporter.notify import Monitor, Notifier
from exporter.spool import Spool

from tests.conftest import ts


class _Collector:
    """Tiny aiohttp app that records webhook POSTs."""

    def __init__(self) -> None:
        self.received: list[dict] = []

    async def start(self) -> str:
        from aiohttp import web

        async def handle(request):
            self.received.append(await request.json())
            return web.json_response({"ok": True})

        app = web.Application()
        app.router.add_post("/hook", handle)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        port = self._runner.addresses[0][1]
        return f"http://127.0.0.1:{port}/hook"

    async def stop(self) -> None:
        await self._runner.cleanup()


def test_webhook_receives_status_transitions() -> None:
    collector = _Collector()

    async def scenario() -> list[dict]:
        url = await collector.start()
        await Notifier(url).notify("ok", "degraded", {"status": "degraded", "spool": {"bytes": 1}})
        await collector.stop()
        return collector.received

    payloads = asyncio.run(scenario())
    assert len(payloads) == 1
    assert payloads[0]["event"] == "status_change"
    assert payloads[0]["old_status"] == "ok"
    assert payloads[0]["new_status"] == "degraded"
    assert payloads[0]["details"]["spool"]["bytes"] == 1
    assert payloads[0]["timestamp"]


def test_disabled_webhook_is_silent() -> None:
    asyncio.run(Notifier(None).notify("ok", "degraded", {}))  # no URL: no request, no error


def test_webhook_failure_never_logs_the_url() -> None:
    # an unreachable port: the request fails and the URL must stay out of logs
    secret_url = "http://127.0.0.1:1/hook?token=super-secret"
    records: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("exporter.notify")
    handler = Capture()
    logger.addHandler(handler)
    try:
        asyncio.run(Notifier(secret_url).notify("ok", "failed", {}))
    finally:
        logger.removeHandler(handler)

    assert records, "a failure must be logged"
    text = " ".join(record.getMessage() for record in records)
    assert "super-secret" not in text
    assert "127.0.0.1" not in text
    assert "http" not in text


def test_monitor_fires_once_per_transition(tmp_path) -> None:
    received: list[tuple[str, str]] = []

    class SpyNotifier:
        async def notify(self, old_status: str, new_status: str, details: dict) -> None:
            received.append((old_status, new_status))

    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    now = {"value": ts(2026, 10, 24, 12)}
    state = HealthState(spool, flush_interval=10.0, now=lambda: now["value"])
    monitor = Monitor(state, SpyNotifier(), interval=0.01)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(monitor.run(stop))
        await asyncio.sleep(0.05)  # degraded (stream not connected yet) -> fires once
        state.set_stream(True)
        await asyncio.sleep(0.05)  # degraded -> ok -> fires once
        state.record_drops(1)
        await asyncio.sleep(0.05)  # ok -> degraded -> fires once
        stop.set()
        await task

    asyncio.run(scenario())
    # the loop samples first, so exactly two transitions fire
    assert received == [("degraded", "ok"), ("ok", "degraded")]
    spool.close()
