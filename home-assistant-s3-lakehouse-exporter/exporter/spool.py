"""Durable local spool built on SQLite.

Every captured event is committed here *before* any network write happens, so a
storage outage (or a crash) can never lose events: the background drainer only
deletes rows after the corresponding object has been written successfully.
That gives at-least-once delivery — downstream consumers are expected to
deduplicate.

When the spool reaches its byte cap, the oldest rows are dropped with a loud
warning. Degrading is always preferable to growing: the add-on shares its host
with Home Assistant and must never exhaust memory or disk.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

LOGGER = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS spool (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    event_time TEXT NOT NULL,
    payload TEXT NOT NULL,
    payload_bytes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS spool_kind_id ON spool (kind, id);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

#: Fraction of the byte cap to drop back to once the cap is hit. Hysteresis so
#: we do not drop rows on every single insert after the first overflow.
_CAP_RELEASE_FACTOR = 0.9

_LAST_EVENT_TIME_KEY = "last_event_time"


@dataclass(frozen=True, slots=True)
class SpoolRow:
    """One spooled event, ready to be written to storage."""

    id: int
    kind: str
    event_time: str  # ISO 8601, UTC
    payload: dict


class Spool:
    """SQLite-backed FIFO spool with a byte cap and crash safety."""

    def __init__(self, path: str | Path, max_bytes: int) -> None:
        self._path = Path(path)
        self._max_bytes = max_bytes
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False: the drainer and the capture loop may run in
        # different threads (asyncio.to_thread); all access is serialised by
        # SQLite itself and short transactions.
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # FULL: committed rows survive a power loss, which is the point of the
        # spool. Inserts are batched so the fsync cost is amortised.
        self._conn.execute("PRAGMA synchronous=FULL")
        with self._conn:
            self._conn.executescript(_SCHEMA)

    # -- writes -----------------------------------------------------------

    def add(self, kind: str, event_time: datetime, payload: dict) -> int:
        """Spool one event and return its row id."""
        return self.add_many([(kind, event_time, payload)])[0]

    def add_many(self, rows: Sequence[tuple[str, datetime, dict]]) -> list[int]:
        """Spool several events in one transaction. Returns the row ids."""
        if not rows:
            return []
        now_max = max((event_time for _, event_time, _ in rows), default=None)
        prepared: list[tuple[str, str, str, int]] = []
        for kind, event_time, payload in rows:
            text = json.dumps(payload, separators=(",", ":"), default=str)
            prepared.append((kind, _iso(event_time), text, len(text.encode("utf-8"))))
        with self._conn:
            ids: list[int] = []
            for row in prepared:
                cursor = self._conn.execute(
                    "INSERT INTO spool (kind, event_time, payload, payload_bytes) VALUES (?, ?, ?, ?)",
                    row,
                )
                ids.append(cursor.lastrowid)
            if now_max is not None:
                current = self._meta_get(_LAST_EVENT_TIME_KEY)
                if current is None or _iso(now_max) > current:
                    self._meta_set(_LAST_EVENT_TIME_KEY, _iso(now_max))
        return ids

    def enforce_cap(self) -> int:
        """Drop oldest rows until the spool fits its byte cap.

        Returns the number of dropped rows. Always logs loudly: dropping
        events is a last resort and must be visible to the operator.
        """
        total = self.total_bytes()
        if total <= self._max_bytes:
            return 0
        target = int(self._max_bytes * _CAP_RELEASE_FACTOR)
        dropped_rows = 0
        dropped_bytes = 0
        with self._conn:
            candidates = self._conn.execute(
                "SELECT id, payload_bytes FROM spool ORDER BY id ASC"
            ).fetchall()
            for row_id, size in candidates:
                if total <= target:
                    break
                self._conn.execute("DELETE FROM spool WHERE id = ?", (row_id,))
                total -= size
                dropped_bytes += size
                dropped_rows += 1
        if dropped_rows:
            LOGGER.warning(
                "spool exceeded %d bytes: dropped %d oldest events (%d bytes). "
                "Storage is too slow or unavailable; downstream consumers must backfill",
                self._max_bytes,
                dropped_rows,
                dropped_bytes,
            )
        return dropped_rows

    # -- reads ------------------------------------------------------------

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    def fetch(self, limit: int) -> list[SpoolRow]:
        """Return up to ``limit`` oldest rows in insertion order."""
        cursor = self._conn.execute(
            "SELECT id, kind, event_time, payload FROM spool ORDER BY id ASC LIMIT ?",
            (limit,),
        )
        return [
            SpoolRow(id=row_id, kind=kind, event_time=event_time, payload=json.loads(payload))
            for row_id, kind, event_time, payload in cursor
        ]

    def drop(self, ids: Iterable[int]) -> None:
        """Delete rows that have been written to storage successfully."""
        with self._conn:
            self._conn.executemany("DELETE FROM spool WHERE id = ?", [(row_id,) for row_id in ids])

    def pending(self) -> int:
        cursor = self._conn.execute("SELECT COUNT(*) FROM spool")
        return int(cursor.fetchone()[0])

    def total_bytes(self) -> int:
        cursor = self._conn.execute("SELECT COALESCE(SUM(payload_bytes), 0) FROM spool")
        return int(cursor.fetchone()[0])

    def last_event_time(self) -> datetime | None:
        """Highest event time ever spooled (kept across restarts).

        Used as the starting point for gap repair after a restart or a stream
        outage.
        """
        value = self._meta_get(_LAST_EVENT_TIME_KEY)
        return _parse_iso(value) if value else None

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        self._conn.close()

    # -- internals --------------------------------------------------------

    def _meta_get(self, key: str) -> str | None:
        cursor = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row[0] if row else None

    def _meta_set(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    """Normalise a datetime to the spool's ISO 8601 UTC representation."""
    return _iso(value)


def parse_iso(value: str) -> datetime:
    """Parse an ISO 8601 timestamp to an aware UTC datetime."""
    return _parse_iso(value)
