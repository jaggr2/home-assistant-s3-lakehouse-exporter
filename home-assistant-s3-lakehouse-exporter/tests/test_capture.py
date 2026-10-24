"""Capture: filters, row conversion, gap detection, backfill."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from exporter.capture import (
    AttributeFilter,
    CaptureService,
    EntityFilter,
    GapTracker,
    HomeAssistantClient,
    chunks,
    event_row,
    history_state_row,
    initial_window,
    state_changed_row,
    time_windows,
)
from exporter.spool import Spool

from tests.conftest import make_options, ts

STATE_EVENT = {
    "type": "event",
    "event": {
        "event_type": "state_changed",
        "origin": "core",
        "time_fired": "2026-10-24T12:00:00.500000+00:00",
        "context": {"id": "ctx-1", "user_id": "user-1", "parent_id": None},
        "data": {
            "entity_id": "sensor.kitchen",
            "old_state": {"state": "20.0", "attributes": {}},
            "new_state": {
                "state": "21.0",
                "attributes": {"unit_of_measurement": "°C", "friendly_name": "Kitchen"},
                "last_changed": "2026-10-24T11:59:00+00:00",
                "last_updated": "2026-10-24T12:00:00+00:00",
            },
        },
    },
}

GENERIC_EVENT = {
    "type": "event",
    "event": {
        "event_type": "automation_triggered",
        "origin": "core",
        "time_fired": "2026-10-24T12:01:00+00:00",
        "context": {"id": "ctx-2", "user_id": None, "parent_id": "ctx-1"},
        "data": {"name": "Morning"},
    },
}


# -- attribute filtering ---------------------------------------------------


def test_attribute_filter_all() -> None:
    attrs = {"a": 1, "b": 2}
    assert AttributeFilter("all").filter(attrs) == '{"a":1,"b":2}'


def test_attribute_filter_minimal_without_allowlist_writes_none() -> None:
    assert AttributeFilter("minimal").filter({"a": 1}) is None
    assert AttributeFilter("minimal").wants_attributes is False


def test_attribute_filter_minimal_with_allowlist() -> None:
    flt = AttributeFilter("minimal", ["a"])
    assert flt.filter({"a": 1, "b": 2}) == '{"a":1}'
    assert flt.wants_attributes is True


def test_attribute_filter_allowlist_mode() -> None:
    flt = AttributeFilter("allowlist", ["b"])
    assert flt.filter({"a": 1, "b": 2}) == '{"b":2}'


def test_attribute_filter_degrades_to_minimal_under_pressure() -> None:
    flt = AttributeFilter("all", ["a"])
    flt.degrade()
    assert flt.effective_mode == "minimal"
    assert flt.filter({"a": 1, "b": 2}) == '{"a":1}'
    flt.restore()
    assert flt.filter({"a": 1, "b": 2}) == '{"a":1,"b":2}'


def test_attribute_filter_empty_and_none() -> None:
    assert AttributeFilter("all").filter({}) is None
    assert AttributeFilter("all").filter(None) is None


# -- entity filtering ------------------------------------------------------


def test_entity_filter_defaults_to_allow_all() -> None:
    assert EntityFilter().allows("sensor.anything")


def test_entity_filter_exclude_wins() -> None:
    flt = EntityFilter(include_entities=["sensor.a"], exclude_entities=["sensor.a"])
    assert not flt.allows("sensor.a")


def test_entity_filter_domains() -> None:
    flt = EntityFilter(include_domains=["light"], exclude_entities=["light.kitchen"])
    assert flt.allows("light.living_room")
    assert not flt.allows("light.kitchen")
    assert not flt.allows("sensor.other")

    excluded_domain = EntityFilter(include_domains=["light", "sensor"], exclude_domains=["sensor"])
    assert excluded_domain.allows("light.living_room")
    assert not excluded_domain.allows("sensor.other")


# -- row conversion --------------------------------------------------------


def test_state_changed_row_mapping() -> None:
    kind, when, payload = state_changed_row(STATE_EVENT["event"], AttributeFilter("all"))
    assert kind == "state_changes"
    assert when == ts(2026, 10, 24, 12)
    assert payload["entity_id"] == "sensor.kitchen"
    assert payload["state"] == "21.0"
    assert payload["old_state"] == "20.0"
    assert payload["last_changed"] == "2026-10-24T11:59:00Z"
    assert payload["last_updated"] == "2026-10-24T12:00:00Z"
    assert payload["origin"] == "core"
    assert payload["context_id"] == "ctx-1"
    assert payload["context_user_id"] == "user-1"
    assert payload["context_parent_id"] is None
    assert "Kitchen" in payload["attributes"]


def test_state_changed_row_without_new_state() -> None:
    event = {"event_type": "state_changed", "time_fired": "2026-10-24T12:00:00Z",
             "data": {"entity_id": "sensor.gone", "old_state": {"state": "on"}}}
    kind, _when, payload = state_changed_row(event, AttributeFilter("all"))
    assert kind == "state_changes"
    assert payload["state"] is None
    assert payload["old_state"] == "on"


def test_state_changed_row_requires_entity() -> None:
    assert state_changed_row({"event_type": "state_changed", "data": {}}, AttributeFilter()) is None


def test_event_row_mapping() -> None:
    kind, when, payload = event_row(GENERIC_EVENT["event"])
    assert kind == "events"
    assert when == ts(2026, 10, 24, 12, 1)
    assert payload["event_type"] == "automation_triggered"
    assert payload["data"] == '{"name":"Morning"}'
    assert payload["context_parent_id"] == "ctx-1"
    assert event_row(STATE_EVENT["event"]) is None
    assert event_row({"event_type": "x"}) is None


def test_history_state_row_mapping() -> None:
    state = {
        "entity_id": "sensor.kitchen",
        "state": "21.0",
        "attributes": {"a": 1},
        "last_changed": "2026-10-24T11:00:00+00:00",
        "last_updated": "2026-10-24T11:30:00+00:00",
    }
    kind, when, payload = history_state_row(state, AttributeFilter("all"))
    assert kind == "state_changes"
    assert when == ts(2026, 10, 24, 11, 30)
    assert payload["origin"] == "history_api"
    assert payload["old_state"] is None
    assert payload["attributes"] == '{"a":1}'


def test_history_state_row_respects_filters() -> None:
    state = {"entity_id": "sensor.skip", "state": "1", "last_updated": "2026-10-24T11:30:00Z"}
    flt = EntityFilter(exclude_entities=["sensor.skip"])
    assert history_state_row(state, AttributeFilter("all"), flt) is None


# -- gap tracking ----------------------------------------------------------


def test_gap_tracker_detects_gap() -> None:
    tracker = GapTracker(gap_threshold=timedelta(seconds=60))
    tracker.record(ts(2026, 10, 24, 12))
    tracker.record(ts(2026, 10, 24, 11))  # older, ignored
    assert tracker.last_event_time == ts(2026, 10, 24, 12)

    assert tracker.gap_window(ts(2026, 10, 24, 12, 0, 30)) is None
    start, end = tracker.gap_window(ts(2026, 10, 24, 12, 5))
    assert (start, end) == (ts(2026, 10, 24, 12), ts(2026, 10, 24, 12, 5))
    assert GapTracker().gap_window(ts(2026, 10, 24, 12)) is None


def test_initial_window() -> None:
    assert initial_window(ts(2026, 10, 24), 0) is None
    start, end = initial_window(ts(2026, 10, 24), 7)
    assert end == ts(2026, 10, 24)
    assert end - start == timedelta(days=7)


def test_time_windows_and_chunks() -> None:
    windows = list(time_windows(ts(2026, 10, 24), ts(2026, 10, 26, 12), timedelta(days=1)))
    assert windows == [
        (ts(2026, 10, 24), ts(2026, 10, 25)),
        (ts(2026, 10, 25), ts(2026, 10, 26)),
        (ts(2026, 10, 26), ts(2026, 10, 26, 12)),
    ]
    assert list(chunks(["a", "b", "c"], 2)) == [["a", "b"], ["c"]]


# -- service behaviour -----------------------------------------------------


class FakeClient:
    def __init__(self, history_data=None) -> None:
        self.history_data = history_data or []
        self.history_calls: list[tuple] = []
        self.ws_url = "ws://fake/core/websocket"
        self.token = "token"

    async def history(self, start, end, *, entity_ids=None, include_attributes=True):
        self.history_calls.append((start, end, tuple(entity_ids or ()), include_attributes))
        return self.history_data

    async def ws_request(self, command, payload=None):
        return []

    async def close(self) -> None:
        return None


class FakeWS:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


def _service(tmp_path, client, **capture_overrides) -> tuple[CaptureService, Spool]:
    options = make_options(capture=capture_overrides)
    spool = Spool(tmp_path / "spool.db", max_bytes=10 * 1024 * 1024)
    return CaptureService(options, spool, client), spool


def test_handle_message_filters_entities(tmp_path) -> None:
    client = FakeClient()
    service, spool = _service(tmp_path, client, exclude_entities=["sensor.kitchen"])
    assert service.handle_message(STATE_EVENT) == []
    assert spool.pending() == 0


def test_handle_message_only_captures_subscribed_events(tmp_path) -> None:
    client = FakeClient()
    service, _spool = _service(tmp_path, client, events=["state_changed"])
    rows = service.handle_message(GENERIC_EVENT)
    assert rows == []  # automation_triggered is not subscribed

    service, _spool = _service(tmp_path, client, events=["automation_triggered"])
    rows = service.handle_message(GENERIC_EVENT)
    assert len(rows) == 1 and rows[0][0] == "events"
    assert service.handle_message(STATE_EVENT)[0][0] == "state_changes"  # always captured


def test_mocked_websocket_stream_and_gap_detection(tmp_path) -> None:
    """Full message flow over a mocked WebSocket, then a gap after 'reconnect'."""
    client = FakeClient()
    service, spool = _service(tmp_path, client)
    ws = FakeWS()

    async def scenario() -> None:
        await service._handle_server_message(ws, {"type": "auth_required"})
        await service._handle_server_message(ws, {"type": "auth_ok"})
        await service._handle_server_message(ws, STATE_EVENT)
        await service._handle_server_message(ws, GENERIC_EVENT)  # not subscribed

    asyncio.run(scenario())
    assert ws.sent[0] == {"type": "auth", "access_token": "token"}
    assert ws.sent[1] == {"id": 1, "type": "subscribe_events", "event_type": "state_changed"}
    assert spool.pending() == 1
    assert service.gap_tracker.last_event_time == ts(2026, 10, 24, 12)

    # stream dropped for 5 minutes: the gap is detected and repaired
    service._now = lambda: ts(2026, 10, 24, 12, 5)
    service._backfilled = True
    asyncio.run(service._repair_gaps())
    assert len(client.history_calls) == 1
    start, end, _entities, _attrs = client.history_calls[0]
    assert (start, end) == (ts(2026, 10, 24, 12), ts(2026, 10, 24, 12, 5))


def test_startup_backfill_window(tmp_path) -> None:
    client = FakeClient()
    service, _spool = _service(tmp_path, client, backfill_days=2)
    service._now = lambda: ts(2026, 10, 24, 12)
    asyncio.run(service._repair_gaps())
    assert len(client.history_calls) == 2  # one request per day-sized window
    assert client.history_calls[0][0] == ts(2026, 10, 22, 12)
    assert client.history_calls[-1][1] == ts(2026, 10, 24, 12)

    # and no repeat backfill when there is no gap
    asyncio.run(service._repair_gaps())
    assert len(client.history_calls) == 2


def test_backfill_spools_history(tmp_path) -> None:
    history = [
        [
            {"entity_id": "sensor.a", "state": "1", "last_updated": "2026-10-24T10:00:00Z",
             "attributes": {"x": 1}},
            {"entity_id": "sensor.b", "state": "2", "last_updated": "2026-10-24T11:00:00Z"},
        ],
        [{"entity_id": "sensor.skip", "state": "3", "last_updated": "2026-10-24T11:30:00Z"}],
    ]
    client = FakeClient(history_data=history)
    service, spool = _service(tmp_path, client, exclude_entities=["sensor.skip"])
    written = asyncio.run(service.backfill(ts(2026, 10, 24), ts(2026, 10, 24, 12)))
    assert written == 2
    rows = spool.fetch(10)
    assert [row.payload["entity_id"] for row in rows] == ["sensor.a", "sensor.b"]
    assert all(row.payload["origin"] == "history_api" for row in rows)
    assert service.gap_tracker.last_event_time == ts(2026, 10, 24, 11)


def test_backfill_without_attributes_when_minimal(tmp_path) -> None:
    client = FakeClient(history_data=[[{"entity_id": "sensor.a", "state": "1",
                                        "last_updated": "2026-10-24T10:00:00Z",
                                        "attributes": {"x": 1}}]])
    service, spool = _service(tmp_path, client, attributes="minimal")
    asyncio.run(service.backfill(ts(2026, 10, 24), ts(2026, 10, 24, 12)))
    assert client.history_calls[0][3] is False  # include_attributes
    assert spool.fetch(1)[0].payload["attributes"] is None


def test_backfill_chunks_included_entities(tmp_path) -> None:
    entities = [f"sensor.e{index}" for index in range(120)]
    client = FakeClient(
        history_data=[[{"entity_id": entities[0], "state": "1",
                        "last_updated": "2026-10-24T10:00:00Z"}]]
    )
    service, _spool = _service(tmp_path, client, include_entities=entities)
    asyncio.run(service.backfill(ts(2026, 10, 24), ts(2026, 10, 24, 1)))
    assert [len(call[2]) for call in client.history_calls] == [50, 50, 20]


def test_client_defaults() -> None:
    client = HomeAssistantClient("token")
    assert client.http_url == "http://supervisor/core"
    assert client.ws_url == "ws://supervisor/core/websocket"
