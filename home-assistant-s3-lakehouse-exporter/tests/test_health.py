"""Health state, secret redaction and the /health endpoint."""

from __future__ import annotations

import asyncio
import logging

from exporter.health import DEGRADED, FAILED, OK, HealthServer, HealthState
from exporter.redact import RedactingFilter, Redactor
from exporter.spool import Spool

from tests.conftest import make_options, ts


def _state(tmp_path, **kwargs) -> HealthState:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    state = HealthState(spool, flush_interval=10.0, **kwargs)
    return state


def test_status_lifecycle(tmp_path) -> None:
    now = {"value": ts(2026, 10, 24, 12)}
    state = _state(tmp_path, now=lambda: now["value"])
    assert state.status() == DEGRADED  # stream not connected yet

    state.set_stream(True)
    assert state.status() == OK

    state.record_error(RuntimeError("blip"))
    assert state.status() == DEGRADED

    state.record_flush(5, ["object-1"])
    assert state.status() == OK  # successful flush clears the error

    state.record_drops(3)
    assert state.status() == DEGRADED
    state.dropped_rows = 0

    now["value"] = ts(2026, 10, 24, 12, 1)  # 60s since the flush, interval is 10s
    assert state.status() == DEGRADED
    now["value"] = ts(2026, 10, 24, 12, 0, 5)
    assert state.status() == OK


def test_status_failed_when_never_flushed(tmp_path) -> None:
    now = {"value": ts(2026, 10, 24, 12)}
    state = _state(tmp_path, now=lambda: now["value"])
    state.set_stream(True)
    state.record_error(RuntimeError("storage down"))
    now["value"] = ts(2026, 10, 24, 12, 1)  # 60s > 3 * 10s
    assert state.status() == FAILED
    state.mark_fatal()
    assert state.status() == FAILED


def test_snapshot_contains_no_secrets(tmp_path) -> None:
    options = make_options()
    redactor = Redactor.from_options(options)
    state = _state(tmp_path, redactor=redactor)
    state.record_error(RuntimeError("access denied for AKIA_TEST/secret-value"))
    snapshot = state.snapshot()
    assert "secret-value" not in snapshot["last_error"]
    assert "AKIA_TEST" not in snapshot["last_error"]
    assert "***" in snapshot["last_error"]
    assert set(snapshot) >= {"status", "spool", "last_flush", "last_error", "stream_connected"}


def test_redactor_covers_webhook_and_catalog_uri() -> None:
    options = make_options(
        runtime={"notify_webhook": "https://hooks.example.invalid/secret-path"},
        format={
            "type": "iceberg",
            "catalog": "sql",
            "catalog_uri": "postgresql://user:password@db/catalog",
            "warehouse": "s3://b/wh",
        },
    )
    redactor = Redactor.from_options(options)
    text = "failed to POST https://hooks.example.invalid/secret-path using postgresql://user:password@db/catalog"
    assert "secret-path" not in redactor.redact(text)
    assert "password" not in redactor.redact(text)


def test_redacting_logging_filter() -> None:
    redactor = Redactor(["hunter2"])
    records: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("exporter.test.redaction")
    handler = Capture()
    handler.addFilter(RedactingFilter(redactor))
    logger.addHandler(handler)
    logger.warning("token is %s", "hunter2")
    assert records[0].getMessage() == "token is ***"


def test_health_endpoint(tmp_path) -> None:
    import aiohttp

    state = _state(tmp_path)
    state.set_stream(True)
    server = HealthServer(state, host="127.0.0.1", port=0)

    async def scenario() -> tuple[int, dict, int]:
        stop = asyncio.Event()
        task = asyncio.create_task(server.run(stop))
        for _ in range(100):
            if server.bound_port is not None:
                break
            await asyncio.sleep(0.01)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{server.bound_port}/health") as response:
                healthy = (response.status, await response.json())
            state.mark_fatal()
            async with session.get(f"http://127.0.0.1:{server.bound_port}/health") as response:
                failed = (response.status, await response.json())
        stop.set()
        await task
        return healthy[0], healthy[1], failed[0]

    code, payload, failed_code = asyncio.run(scenario())
    assert code == 200
    assert payload["status"] == OK
    assert payload["spool"]["max_bytes"] == 1024 * 1024
    assert failed_code == 503
