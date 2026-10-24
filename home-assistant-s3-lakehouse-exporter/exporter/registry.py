"""Registry snapshots as slow-changing dimension tables.

Entity, device and area registries are snapshotted at startup and then daily
(``capture.registries: true``). Snapshots are appended with a ``snapshot_at``
column — no diffing, consumers pick the latest snapshot.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from exporter.spool import iso

if TYPE_CHECKING:  # pragma: no cover - typing only
    from exporter.capture import HomeAssistantClient, Row
    from exporter.spool import Spool

LOGGER = logging.getLogger(__name__)

SNAPSHOT_INTERVAL = timedelta(days=1)

REGISTRY_COMMANDS = {
    "registry_entities": "config/entity_registry/list",
    "registry_devices": "config/device_registry/list",
    "registry_areas": "config/area_registry/list",
}


def entity_rows(entries: list[dict], snapshot_at: datetime) -> list[Row]:
    return [
        (
            "registry_entities",
            snapshot_at,
            {
                "entity_id": entry.get("entity_id"),
                "device_id": entry.get("device_id"),
                "area_id": entry.get("area_id"),
                "name": entry.get("name"),
                "original_name": entry.get("original_name"),
                "platform": entry.get("platform"),
                "disabled": entry.get("disabled_by") is not None,
                "snapshot_at": iso(snapshot_at),
            },
        )
        for entry in entries
        if entry.get("entity_id")
    ]


def device_rows(entries: list[dict], snapshot_at: datetime) -> list[Row]:
    return [
        (
            "registry_devices",
            snapshot_at,
            {
                "id": entry.get("id"),
                "name": entry.get("name"),
                "model": entry.get("model"),
                "manufacturer": entry.get("manufacturer"),
                "area_id": entry.get("area_id"),
                "identifiers": json.dumps(entry.get("identifiers") or [], default=str),
                "snapshot_at": iso(snapshot_at),
            },
        )
        for entry in entries
        if entry.get("id")
    ]


def area_rows(entries: list[dict], snapshot_at: datetime) -> list[Row]:
    return [
        (
            "registry_areas",
            snapshot_at,
            {
                "id": entry.get("area_id") or entry.get("id"),
                "name": entry.get("name"),
                "aliases": json.dumps(entry.get("aliases") or [], default=str),
                "snapshot_at": iso(snapshot_at),
            },
        )
        for entry in entries
        if entry.get("area_id") or entry.get("id")
    ]


ROW_BUILDERS = {
    "registry_entities": entity_rows,
    "registry_devices": device_rows,
    "registry_areas": area_rows,
}


class RegistryService:
    def __init__(
        self,
        options: Any,
        spool: Spool,
        client: HomeAssistantClient,
        *,
        interval: timedelta = SNAPSHOT_INTERVAL,
        now: Any = None,
    ) -> None:
        self.options = options
        self.spool = spool
        self.client = client
        self.interval = interval
        self._now = now or (lambda: datetime.now(UTC))

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.snapshot()
            except asyncio.CancelledError:
                raise
            except Exception as err:
                LOGGER.warning("registry snapshot failed: %s: %s", type(err).__name__, err)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=self.interval.total_seconds())

    async def snapshot(self) -> int:
        """Fetch all registries into the spool; returns the number of rows."""
        snapshot_at = self._now()
        written = 0
        for kind, command in REGISTRY_COMMANDS.items():
            entries = await self.client.ws_request(command)
            if not isinstance(entries, list):
                continue
            rows = ROW_BUILDERS[kind](entries, snapshot_at)
            if rows:
                self.spool.add_many(rows)
                written += len(rows)
        LOGGER.info("registry snapshot spooled %d rows", written)
        return written
