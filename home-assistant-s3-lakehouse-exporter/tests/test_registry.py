"""Registry snapshot rows and the daily snapshot service."""

from __future__ import annotations

import asyncio
import json

from exporter.registry import RegistryService, area_rows, device_rows, entity_rows
from exporter.spool import Spool

from tests.conftest import make_options, ts

SNAPSHOT_AT = ts(2026, 10, 24, 3)

ENTITIES = [
    {
        "entity_id": "sensor.kitchen",
        "device_id": "dev-1",
        "area_id": "kitchen",
        "name": "Kitchen temp",
        "original_name": "Temperature",
        "platform": "mqtt",
        "disabled_by": None,
    },
    {
        "entity_id": "sensor.old",
        "device_id": None,
        "area_id": None,
        "name": None,
        "original_name": None,
        "platform": "template",
        "disabled_by": "user",
    },
]

DEVICES = [
    {
        "id": "dev-1",
        "name": "Kitchen sensor",
        "model": "X100",
        "manufacturer": "Acme",
        "area_id": "kitchen",
        "identifiers": [["mqtt", "kitchen-sensor"]],
    }
]

AREAS = [
    {"area_id": "kitchen", "name": "Kitchen", "aliases": ["Cuisine"]},
    {"id": "living", "name": "Living room", "aliases": None},
]


def test_entity_rows_mapping() -> None:
    rows = entity_rows(ENTITIES, SNAPSHOT_AT)
    assert [row[0] for row in rows] == ["registry_entities", "registry_entities"]
    payload = rows[0][2]
    assert payload["entity_id"] == "sensor.kitchen"
    assert payload["device_id"] == "dev-1"
    assert payload["name"] == "Kitchen temp"
    assert payload["original_name"] == "Temperature"
    assert payload["platform"] == "mqtt"
    assert payload["disabled"] is False
    assert payload["snapshot_at"] == "2026-10-24T03:00:00Z"
    assert rows[1][2]["disabled"] is True
    assert rows[1][2]["device_id"] is None


def test_device_rows_mapping() -> None:
    rows = device_rows(DEVICES, SNAPSHOT_AT)
    payload = rows[0][2]
    assert payload["id"] == "dev-1"
    assert payload["model"] == "X100"
    assert payload["identifiers"] == json.dumps([["mqtt", "kitchen-sensor"]])
    assert payload["snapshot_at"] == "2026-10-24T03:00:00Z"


def test_area_rows_mapping() -> None:
    rows = area_rows(AREAS, SNAPSHOT_AT)
    assert rows[0][2]["id"] == "kitchen"
    assert rows[0][2]["aliases"] == json.dumps(["Cuisine"])
    assert rows[1][2]["id"] == "living"  # falls back to "id"
    assert rows[1][2]["aliases"] == "[]"


def test_rows_skip_entries_without_ids() -> None:
    assert entity_rows([{"name": "nope"}], SNAPSHOT_AT) == []
    assert device_rows([{"name": "nope"}], SNAPSHOT_AT) == []
    assert area_rows([{"name": "nope"}], SNAPSHOT_AT) == []


class FakeClient:
    def __init__(self, responses: dict) -> None:
        self.responses = responses
        self.commands: list[str] = []

    async def ws_request(self, command, payload=None):
        self.commands.append(command)
        return self.responses.get(command, [])


def test_registry_service_snapshots_all_registries(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    client = FakeClient(
        {
            "config/entity_registry/list": ENTITIES,
            "config/device_registry/list": DEVICES,
            "config/area/list": AREAS,
        }
    )
    service = RegistryService(make_options(), spool, client, now=lambda: SNAPSHOT_AT)
    written = asyncio.run(service.snapshot())
    assert written == 2 + 1 + 2
    assert client.commands == [
        "config/entity_registry/list",
        "config/device_registry/list",
        "config/area/list",
    ]
    kinds = {row.kind for row in spool.fetch(10)}
    assert kinds == {"registry_entities", "registry_devices", "registry_areas"}
    spool.close()


def test_registry_service_tolerates_bad_responses(tmp_path) -> None:
    spool = Spool(tmp_path / "spool.db", max_bytes=1024 * 1024)
    client = FakeClient({"config/entity_registry/list": {"unexpected": "shape"}})
    service = RegistryService(make_options(), spool, client, now=lambda: SNAPSHOT_AT)
    assert asyncio.run(service.snapshot()) == 0
    assert spool.pending() == 0
    spool.close()
