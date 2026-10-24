"""Storage writers.

A writer turns batches of spooled events into objects in the configured
S3-compatible bucket. Formats share:

* the logical table schemas (``TABLE_COLUMNS``) from AGENTS.md §3,
* the key layout ``{prefix}/{table}/year=YYYY/month=MM/day=DD/part-<uuid>.<ext>``
  bucketed on the event's own timestamp,
* the schema-stability guarantee: evolution is **additive only** — unknown
  columns in a payload become new nullable columns, nothing is renamed or
  dropped, and nulls are always allowed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterable, Sequence

    from exporter.config import Options
    from exporter.spool import SpoolRow


class WriteError(RuntimeError):
    """Raised when a batch could not be written to storage."""


#: Uncompressed payload budget per data file. Objects land around this size
#: (compressed codecs shrink it further); sized for lakehouse engines that
#: prefer ~128 MB files over many tiny ones.
TARGET_OBJECT_BYTES = 128 * 1024 * 1024


def estimate_row_bytes(row: SpoolRow) -> int:
    """Cheap payload size estimate without re-serialising the row."""
    return 32 + sum(len(str(value)) if value is not None else 0 for value in row.payload.values())


@dataclass(frozen=True, slots=True)
class Column:
    """Logical column. ``type`` is one of ``string``, ``timestamp``, ``bool``, ``json``.

    ``timestamp`` values are ISO 8601 strings in payloads and become real
    timestamp columns in columnar formats. ``json`` values are JSON *strings*
    (attribute shapes vary per entity and must not explode the schema).
    """

    name: str
    type: str


TABLE_COLUMNS: dict[str, tuple[Column, ...]] = {
    "state_changes": (
        Column("entity_id", "string"),
        Column("state", "string"),
        Column("attributes", "json"),
        Column("last_changed", "timestamp"),
        Column("last_updated", "timestamp"),
        Column("old_state", "string"),
        Column("context_id", "string"),
        Column("context_user_id", "string"),
        Column("context_parent_id", "string"),
        Column("origin", "string"),
    ),
    "events": (
        Column("event_type", "string"),
        Column("time_fired", "timestamp"),
        Column("origin", "string"),
        Column("context_id", "string"),
        Column("context_user_id", "string"),
        Column("data", "json"),
    ),
    "registry_entities": (
        Column("entity_id", "string"),
        Column("device_id", "string"),
        Column("area_id", "string"),
        Column("name", "string"),
        Column("original_name", "string"),
        Column("platform", "string"),
        Column("disabled", "bool"),
        Column("snapshot_at", "timestamp"),
    ),
    "registry_devices": (
        Column("id", "string"),
        Column("name", "string"),
        Column("model", "string"),
        Column("manufacturer", "string"),
        Column("area_id", "string"),
        Column("identifiers", "json"),
        Column("snapshot_at", "timestamp"),
    ),
    "registry_areas": (
        Column("id", "string"),
        Column("name", "string"),
        Column("aliases", "json"),
        Column("snapshot_at", "timestamp"),
    ),
}


@dataclass(frozen=True, slots=True)
class WriteResult:
    key: str
    table: str
    rows: int
    size: int


class ObjectStore(Protocol):
    def put_object(self, key: str, body: bytes, content_type: str) -> None: ...


class S3ObjectStore:
    """S3-compatible object store client (AWS S3, MinIO, R2, Garage, ...)."""

    def __init__(self, options: Options) -> None:
        import boto3  # local import: keeps the module importable without boto3
        from botocore.config import Config as BotoConfig

        self._bucket = options.s3.bucket
        self._storage_class = options.s3.storage_class
        client_options: dict = {
            "service_name": "s3",
            "region_name": options.s3.region,
            "config": BotoConfig(
                s3={"addressing_style": "path" if options.s3.path_style else "auto"},
                retries={"max_attempts": 5, "mode": "standard"},
                max_pool_connections=4,
            ),
        }
        if options.s3.endpoint_url:
            client_options["endpoint_url"] = options.s3.endpoint_url
        if options.auth.has_static_credentials:
            client_options["aws_access_key_id"] = options.auth.access_key_id
            client_options["aws_secret_access_key"] = options.auth.secret_access_key
            if options.auth.session_token:
                client_options["aws_session_token"] = options.auth.session_token
        self._client = boto3.client(**client_options)

    def put_object(self, key: str, body: bytes, content_type: str) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            StorageClass=self._storage_class,
        )


class BatchWriter:
    """Base class: partitioning, schema handling, table naming."""

    format_type = "base"

    def __init__(self, store: ObjectStore, options: Options) -> None:
        self.store = store
        self.options = options

    def write_batch(self, rows: Sequence[SpoolRow]) -> list[WriteResult]:
        raise NotImplementedError

    # -- sizing -----------------------------------------------------------

    def split_by_size(self, rows: Sequence[SpoolRow]) -> list[list[SpoolRow]]:
        """Split rows into chunks of at most ``TARGET_OBJECT_BYTES`` payload.

        Keeps data files near the target size even when a flush carries more
        payload than that (a single oversized row becomes its own object).
        """
        chunks: list[list[SpoolRow]] = []
        current: list[SpoolRow] = []
        current_bytes = 0
        for row in rows:
            row_bytes = estimate_row_bytes(row)
            if current and current_bytes + row_bytes > TARGET_OBJECT_BYTES:
                chunks.append(current)
                current, current_bytes = [], 0
            current.append(row)
            current_bytes += row_bytes
        if current:
            chunks.append(current)
        return chunks

    # -- schema -----------------------------------------------------------

    def columns_for(self, kind: str, rows: Sequence[SpoolRow]) -> list[Column]:
        """Declared columns for ``kind`` plus additive extras seen in ``rows``."""
        declared = TABLE_COLUMNS.get(kind)
        if declared is None:
            raise WriteError(f"unknown table kind: {kind!r}")
        known = {column.name for column in declared}
        extras = sorted({key for row in rows for key in row.payload if key not in known})
        return [*declared, *(Column(name, "string") for name in extras)]

    # -- partitioning -----------------------------------------------------

    def table_name(self, kind: str) -> str:
        return self.options.table_name(kind)

    def object_key(self, kind: str, when: datetime | date, extension: str) -> str:
        return build_object_key(
            self.options.s3.prefix,
            self.table_name(kind),
            when,
            extension,
        )

    def partition_rows(self, rows: Sequence[SpoolRow]) -> dict[tuple[str, date], list[SpoolRow]]:
        """Group rows by (kind, event date) — one object per group."""
        groups: dict[tuple[str, date], list[SpoolRow]] = {}
        for row in rows:
            groups.setdefault((row.kind, _event_date(row.event_time)), []).append(row)
        return groups


def build_object_key(prefix: str, table: str, when: datetime | date, extension: str) -> str:
    """Build the object key for one partition file.

    ``{prefix}/{table}/year=YYYY/month=MM/day=DD/part-<uuid><extension>``
    """
    if isinstance(when, datetime):
        when = (when if when.tzinfo else when.replace(tzinfo=UTC)).astimezone(UTC)
        when = when.date()
    parts = [
        part
        for part in (
            prefix.strip("/"),
            table,
            f"year={when:%Y}",
            f"month={when:%m}",
            f"day={when:%d}",
        )
        if part
    ]
    return "/".join(parts) + f"/part-{uuid4().hex}{extension}"


def _event_date(event_time: str) -> date:
    parsed = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).date()


def to_datetime(value: object) -> datetime | None:
    """Coerce an ISO 8601 string to an aware UTC datetime, else ``None``."""
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def timestamp_column_names(columns: Iterable[Column]) -> set[str]:
    return {column.name for column in columns if column.type == "timestamp"}
