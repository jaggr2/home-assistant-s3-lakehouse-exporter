"""Capture of Home Assistant history.

Two sources feed the spool:

* a WebSocket **CDC stream** (``state_changed`` plus the configured event
  types) for live data, and
* the REST **history API** for the startup backfill and for gap repair after
  stream outages, so the export is gap-free.

Home Assistant is treated as read-only: the add-on never writes state and
never touches the recorder database.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp

from exporter.spool import Spool, iso, parse_iso

LOGGER = logging.getLogger(__name__)

DEFAULT_HTTP_URL = "http://supervisor/core"
DEFAULT_WS_URL = "ws://supervisor/core/websocket"

#: Downtime after which a reconnect triggers gap repair via the history API.
DEFAULT_GAP_THRESHOLD = timedelta(seconds=60)
#: Entities per history request when a server-side filter is possible.
HISTORY_ENTITY_CHUNK = 50
#: Time window per history request; bounds response size and memory.
HISTORY_TIME_WINDOW = timedelta(days=1)

#: Origin marker for rows reconstructed from the REST history API (WebSocket
#: rows carry Home Assistant's own origin, usually ``core``).
HISTORY_ORIGIN = "history_api"

#: (kind, event time, payload) — the shape :meth:`Spool.add_many` accepts.
Row = tuple[str, datetime, dict]


class HomeAssistantError(RuntimeError):
    """Raised when Home Assistant could not be reached or rejected a call."""


# --------------------------------------------------------------------------
# filters


def domain_of(entity_id: str) -> str:
    return entity_id.partition(".")[0]


class EntityFilter:
    """Include/exclude filtering by entity id and domain."""

    def __init__(
        self,
        include_entities: Sequence[str] = (),
        exclude_entities: Sequence[str] = (),
        include_domains: Sequence[str] = (),
        exclude_domains: Sequence[str] = (),
    ) -> None:
        self._include_entities = frozenset(include_entities)
        self._exclude_entities = frozenset(exclude_entities)
        self._include_domains = frozenset(include_domains)
        self._exclude_domains = frozenset(exclude_domains)

    def allows(self, entity_id: str) -> bool:
        if entity_id in self._exclude_entities:
            return False
        domain = domain_of(entity_id)
        if domain in self._exclude_domains:
            return False
        if self._include_entities or self._include_domains:
            return entity_id in self._include_entities or domain in self._include_domains
        return True


class AttributeFilter:
    """Attribute capture modes: ``all``, ``minimal``, ``allowlist``.

    ``minimal`` writes only ``attribute_allowlist`` keys (none when the
    allowlist is empty); ``allowlist`` requires a non-empty allowlist and
    writes only those keys. Under spool pressure the filter degrades to
    ``minimal`` behaviour until pressure subsides.
    """

    def __init__(self, mode: str = "all", allowlist: Sequence[str] = ()) -> None:
        self.mode = mode
        self.allowlist = frozenset(allowlist)
        self._degraded = False

    @property
    def effective_mode(self) -> str:
        return "minimal" if self._degraded else self.mode

    def degrade(self) -> None:
        self._degraded = True

    def restore(self) -> None:
        self._degraded = False

    @property
    def wants_attributes(self) -> bool:
        """Whether attribute data should be fetched at all."""
        mode = self.effective_mode
        return mode == "all" or bool(self.allowlist)

    def filter(self, attributes: dict | None) -> str | None:
        """Return the attributes JSON string to store (``None`` stores none)."""
        if not attributes:
            return None
        mode = self.effective_mode
        if mode == "all":
            kept = attributes
        else:  # minimal / allowlist
            kept = {key: value for key, value in attributes.items() if key in self.allowlist}
        if not kept:
            return None
        return json.dumps(kept, separators=(",", ":"), default=str)


# --------------------------------------------------------------------------
# gap tracking


@dataclass(slots=True)
class GapTracker:
    """Tracks how much history still needs to be (re)covered."""

    gap_threshold: timedelta = DEFAULT_GAP_THRESHOLD
    last_event_time: datetime | None = None

    def record(self, when: datetime) -> None:
        when = _utc(when)
        if self.last_event_time is None or when > self.last_event_time:
            self.last_event_time = when

    def gap_window(self, now: datetime) -> tuple[datetime, datetime] | None:
        """Window to repair after a stream gap (``None`` when nothing is missing)."""
        now = _utc(now)
        if self.last_event_time is None:
            return None
        if now - self.last_event_time > self.gap_threshold:
            return (self.last_event_time, now)
        return None


def initial_window(now: datetime, backfill_days: int) -> tuple[datetime, datetime] | None:
    """Startup backfill window; ``backfill_days: 0`` disables the backfill."""
    if backfill_days <= 0:
        return None
    return (_utc(now) - timedelta(days=backfill_days), _utc(now))


def time_windows(start: datetime, end: datetime, size: timedelta = HISTORY_TIME_WINDOW):
    start, end = _utc(start), _utc(end)
    while start < end:
        stop = min(start + size, end)
        yield (start, stop)
        start = stop


def chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


# --------------------------------------------------------------------------
# row builders


def _ts(value: Any) -> str | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return iso(parse_iso(value))
    except ValueError:
        return None


def _context(source: dict) -> dict:
    context = source.get("context") or {}
    return {
        "context_id": context.get("id"),
        "context_user_id": context.get("user_id"),
        "context_parent_id": context.get("parent_id"),
    }


def state_changed_row(event: dict, attributes: AttributeFilter) -> Row | None:
    """Convert a ``state_changed`` WebSocket event into a state_changes row."""
    data = event.get("data") or {}
    entity_id = data.get("entity_id")
    if not entity_id:
        return None
    new_state = data.get("new_state") or {}
    old_state = data.get("old_state") or {}
    last_updated = _ts(new_state.get("last_updated")) or _ts(event.get("time_fired"))
    if last_updated is None:
        return None
    payload = {
        "entity_id": entity_id,
        "state": new_state.get("state"),
        "attributes": attributes.filter(new_state.get("attributes")),
        "last_changed": _ts(new_state.get("last_changed")) or last_updated,
        "last_updated": last_updated,
        "old_state": old_state.get("state"),
        "origin": event.get("origin"),
        **_context(event),
    }
    return ("state_changes", parse_iso(last_updated), payload)


def event_row(event: dict) -> Row | None:
    """Convert a non-state WebSocket event into an events row."""
    event_type = event.get("event_type")
    time_fired = _ts(event.get("time_fired"))
    if not event_type or event_type == "state_changed" or time_fired is None:
        return None
    payload = {
        "event_type": event_type,
        "time_fired": time_fired,
        "origin": event.get("origin"),
        "data": json.dumps(event.get("data") or {}, separators=(",", ":"), default=str),
        **_context(event),
    }
    return ("events", parse_iso(time_fired), payload)


def history_state_row(
    state: dict,
    attributes: AttributeFilter,
    entity_filter: EntityFilter | None = None,
) -> Row | None:
    """Convert one REST history state into a state_changes row."""
    entity_id = state.get("entity_id")
    if not entity_id or (entity_filter is not None and not entity_filter.allows(entity_id)):
        return None
    last_updated = _ts(state.get("last_updated")) or _ts(state.get("last_changed"))
    if last_updated is None:
        return None
    payload = {
        "entity_id": entity_id,
        "state": state.get("state"),
        "attributes": attributes.filter(state.get("attributes")),
        "last_changed": _ts(state.get("last_changed")) or last_updated,
        "last_updated": last_updated,
        "old_state": None,  # not available from the history API
        "origin": HISTORY_ORIGIN,
        **_context(state),
    }
    return ("state_changes", parse_iso(last_updated), payload)


# --------------------------------------------------------------------------
# Home Assistant client


class HomeAssistantClient:
    """Minimal Home Assistant REST + WebSocket client.

    Auth uses the Supervisor token (``homeassistant_api: true`` in the add-on
    config). Nothing else — no OAuth, no brokers, no metadata services.
    """

    def __init__(
        self,
        token: str,
        http_url: str = DEFAULT_HTTP_URL,
        ws_url: str = DEFAULT_WS_URL,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.token = token
        self.http_url = http_url.rstrip("/")
        self.ws_url = ws_url
        self._session = session

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=120),
                headers={"Authorization": f"Bearer {self.token}"},
            )
        return self._session

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def get_json(self, path: str, params: dict | None = None) -> Any:
        url = f"{self.http_url}{path}"
        try:
            async with self.session.get(url, params=params) as response:
                if response.status >= 400:
                    raise HomeAssistantError(f"GET {path} failed: HTTP {response.status}")
                return await response.json()
        except HomeAssistantError:
            raise
        except aiohttp.ClientError as err:
            raise HomeAssistantError(f"GET {path} failed: {type(err).__name__}") from err

    async def history(
        self,
        start: datetime,
        end: datetime,
        *,
        entity_ids: Sequence[str] | None = None,
        include_attributes: bool = True,
    ) -> list[list[dict]]:
        """Fetch recorder history for a window (list of per-entity state lists)."""
        params: dict[str, str] = {
            "end_time": iso(end),
            "significant_changes_only": "0",
            "minimal_response": "0",
            "no_attributes": "0" if include_attributes else "1",
        }
        if entity_ids:
            params["filter_entity_id"] = ",".join(entity_ids)
        path = f"/api/history/period/{iso(start)}"
        data = await self.get_json(path, params)
        return data if isinstance(data, list) else []

    async def ws_request(self, command: str, payload: dict | None = None) -> Any:
        """Open a short-lived WebSocket session and run one command."""
        request = {"id": 1, "type": command, **(payload or {})}
        try:
            async with self.session.ws_connect(self.ws_url, heartbeat=30.0) as ws:
                await self._authenticate(ws)
                await ws.send_json(request)
                async for message in ws:
                    data = _ws_json(message)
                    if data is None:
                        continue
                    if data.get("type") == "result" and data.get("id") == 1:
                        if not data.get("success", False):
                            raise HomeAssistantError(f"command {command} failed")
                        return data.get("result")
                    if data.get("type") == "auth_required":
                        await self._authenticate(ws)
        except aiohttp.ClientError as err:
            raise HomeAssistantError(f"websocket command {command} failed: {type(err).__name__}") from err
        raise HomeAssistantError(f"command {command} returned no result")

    async def _authenticate(self, ws: Any) -> None:
        await ws.send_json({"type": "auth", "access_token": self.token})


def _ws_json(message: Any) -> dict | None:
    if message.type == aiohttp.WSMsgType.TEXT:
        try:
            data = json.loads(message.data)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None
    return None


# --------------------------------------------------------------------------
# capture service


class CaptureService:
    """Maintains the WebSocket stream, backfills and gap repair."""

    def __init__(
        self,
        options: Any,
        spool: Spool,
        client: HomeAssistantClient,
        *,
        attribute_filter: AttributeFilter | None = None,
        entity_filter: EntityFilter | None = None,
        gap_tracker: GapTracker | None = None,
        now: Any = None,
    ) -> None:
        capture = options.capture
        self.options = options
        self.spool = spool
        self.client = client
        self.attributes = attribute_filter or AttributeFilter(
            capture.attributes, capture.attribute_allowlist
        )
        self.entities = entity_filter or EntityFilter(
            capture.include_entities,
            capture.exclude_entities,
            capture.include_domains,
            capture.exclude_domains,
        )
        self.gap_tracker = gap_tracker or GapTracker()
        if self.gap_tracker.last_event_time is None:
            self.gap_tracker.last_event_time = spool.last_event_time()
        self._now = now or (lambda: datetime.now(UTC))
        self._backfilled = False
        self.stream_connected = False
        self.backfill_days = capture.backfill_days
        self.event_types = capture.events

    # -- stream -----------------------------------------------------------

    async def run(self, stop_event: asyncio.Event) -> None:
        """Reconnect forever until ``stop_event`` is set."""
        attempt = 0
        while not stop_event.is_set():
            try:
                await self._capture_session()
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as err:
                LOGGER.warning("capture stream interrupted: %s: %s", type(err).__name__, err)
            finally:
                self.stream_connected = False
            if stop_event.is_set():
                break
            attempt += 1
            delay = min(2**attempt, 60)
            LOGGER.info("reconnecting to Home Assistant in %ds", delay)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=delay)

    async def _capture_session(self) -> None:
        await self._repair_gaps()
        LOGGER.info("connecting to %s", self.client.ws_url)
        try:
            async with self.client.session.ws_connect(self.client.ws_url, heartbeat=30.0) as ws:
                async for message in ws:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        await self._handle_server_message(ws, data)
                    elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except aiohttp.ClientError as err:
            raise HomeAssistantError(f"websocket failed: {type(err).__name__}") from err

    async def _handle_server_message(self, ws: Any, message: dict) -> None:
        message_type = message.get("type")
        if message_type == "auth_required":
            await ws.send_json({"type": "auth", "access_token": self.client.token})
            return
        if message_type == "auth_ok":
            self.stream_connected = True
            for index, event_type in enumerate(self.event_types, start=1):
                await ws.send_json(
                    {"id": index, "type": "subscribe_events", "event_type": event_type}
                )
            LOGGER.info("subscribed to events: %s", ", ".join(self.event_types))
            return
        if message_type == "auth_invalid":
            raise HomeAssistantError("authentication failed (invalid supervisor token)")
        if message_type == "result":
            if not message.get("success", False):
                LOGGER.warning("Home Assistant command failed: %s", message.get("error"))
            return
        if message_type != "event":
            return
        rows = self.handle_message(message)
        if rows:
            self.spool.add_many(rows)
            self.gap_tracker.record(max(row[1] for row in rows))

    def handle_message(self, message: dict) -> list[Row]:
        """Convert one ``event`` server message into spool rows."""
        event = message.get("event") or message
        event_type = event.get("event_type")
        if event_type == "state_changed":
            if event.get("data", {}).get("entity_id") and not self.entities.allows(
                event["data"]["entity_id"]
            ):
                return []
            row = state_changed_row(event, self.attributes)
        else:
            if event_type not in self.event_types:
                return []
            row = event_row(event)
        return [row] if row is not None else []

    # -- backfill ---------------------------------------------------------

    async def _repair_gaps(self) -> None:
        now = self._now()
        if not self._backfilled:
            window = initial_window(now, self.backfill_days)
            self._backfilled = True
        else:
            window = self.gap_tracker.gap_window(now)
        if window is None:
            return
        LOGGER.info("backfilling history from %s to %s", iso(window[0]), iso(window[1]))
        written = await self.backfill(*window)
        LOGGER.info("backfill spooled %d rows", written)

    async def backfill(self, start: datetime, end: datetime) -> int:
        """Fetch history for ``[start, end]`` into the spool."""
        written = 0
        entity_chunks: Iterable[Sequence[str] | None]
        if self.options.capture.include_entities:
            entity_chunks = [
                list(chunk)
                for chunk in chunks(self.options.capture.include_entities, HISTORY_ENTITY_CHUNK)
            ]
        else:
            entity_chunks = [None]
        for window_start, window_end in time_windows(start, end):
            for entity_ids in entity_chunks:
                rows = await self._backfill_window(window_start, window_end, entity_ids)
                if rows:
                    self.spool.add_many(rows)
                    self.spool.enforce_cap()
                    written += len(rows)
        return written

    async def _backfill_window(
        self,
        start: datetime,
        end: datetime,
        entity_ids: Sequence[str] | None,
    ) -> list[Row]:
        data = await self.client.history(
            start,
            end,
            entity_ids=entity_ids,
            include_attributes=self.attributes.wants_attributes,
        )
        rows: list[Row] = []
        for states in data:
            for state in states:
                row = history_state_row(state, self.attributes, self.entities)
                if row is not None:
                    rows.append(row)
                    self.gap_tracker.record(row[1])
        return rows


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def build_client(options: Any, token: str) -> HomeAssistantClient:
    import os

    return HomeAssistantClient(
        token=token,
        http_url=os.environ.get("HA_HTTP_URL", DEFAULT_HTTP_URL),
        ws_url=os.environ.get("HA_WS_URL", DEFAULT_WS_URL),
    )
