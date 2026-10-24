"""Shared test fixtures and helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from exporter.config import Options, parse_options
from exporter.spool import Spool, SpoolRow, iso


def make_options(**sections: Any) -> Options:
    """Build Options from config.yaml defaults plus per-section overrides."""
    raw: dict[str, Any] = {
        "s3": {
            "bucket": "test-bucket",
            "prefix": "home-assistant",
            "region": "eu-central-1",
        },
        "auth": {"access_key_id": "AKIA_TEST", "secret_access_key": "secret-value"},
        "capture": {},
        "format": {"type": "parquet"},
        "runtime": {},
    }
    for name, values in sections.items():
        raw.setdefault(name, {}).update(values)
    return parse_options(raw)


class FakeStore:
    """In-memory ObjectStore double."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}

    def put_object(self, key: str, body: bytes, content_type: str) -> None:
        self.objects[key] = (body, content_type)

    def keys(self) -> list[str]:
        return sorted(self.objects)


def make_rows(
    specs: list[tuple[str, datetime, dict]],
    start_id: int = 1,
) -> list[SpoolRow]:
    return [
        SpoolRow(id=start_id + index, kind=kind, event_time=iso(when), payload=payload)
        for index, (kind, when, payload) in enumerate(specs)
    ]


def ts(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


@pytest.fixture()
def spool(tmp_path: Any) -> Any:
    handle = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    yield handle
    handle.close()
