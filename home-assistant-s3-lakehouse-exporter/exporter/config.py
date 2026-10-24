"""Parsing, validation and normalisation of the add-on options.

The Supervisor hands the resolved options to the add-on as JSON in
``/data/options.json`` (path overridable with ``OPTIONS_PATH``). This module
turns that JSON into immutable, validated option objects and is the only place
that knows about the option contract in ``config.yaml``.

Secrets (``auth.secret_access_key``, ``auth.session_token`` and the webhook
URL) must never end up in logs, error messages or the health endpoint; error
messages therefore only ever name the offending field, never its value.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

#: Hard cap on the local spool; the add-on must never fill the host's disk.
MAX_SPOOL_MB = 512
#: Lower bound for the flush interval; below this the exporter thrashes.
MIN_FLUSH_INTERVAL_SECONDS = 10

ATTRIBUTE_MODES = ("all", "minimal", "allowlist")
FORMAT_TYPES = ("iceberg", "parquet", "jsonl")
CATALOG_TYPES = ("glue", "rest", "nessie", "hive", "sql")
STORAGE_CLASSES = (
    "REDUCED_REDUNDANCY",
    "STANDARD",
    "STANDARD_IA",
    "INTELLIGENT_TIERING",
    "ONEZONE_IA",
)
COMPRESSIONS = ("zstd", "snappy", "gzip", "none")
LOG_LEVELS = ("debug", "info", "warning", "error")

#: Formats that can encode every compression mode as-is.
_JSONL_COMPRESSIONS = ("zstd", "gzip", "none")

_DEFAULT_EVENT_TYPES = ("state_changed",)

_TABLE_NAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


class ConfigError(ValueError):
    """Raised when the add-on options are missing or invalid."""


@dataclass(frozen=True, slots=True)
class S3Options:
    bucket: str
    prefix: str
    region: str
    endpoint_url: str | None
    path_style: bool
    storage_class: str


@dataclass(frozen=True, slots=True)
class AuthOptions:
    access_key_id: str | None
    secret_access_key: str | None
    session_token: str | None

    @property
    def has_static_credentials(self) -> bool:
        return bool(self.access_key_id and self.secret_access_key)


@dataclass(frozen=True, slots=True)
class CaptureOptions:
    include_entities: tuple[str, ...]
    exclude_entities: tuple[str, ...]
    include_domains: tuple[str, ...]
    exclude_domains: tuple[str, ...]
    events: tuple[str, ...]
    registries: bool
    attributes: str
    attribute_allowlist: tuple[str, ...]
    backfill_days: int
    flush_interval_seconds: int
    max_batch_rows: int


@dataclass(frozen=True, slots=True)
class FormatOptions:
    type: str
    catalog: str | None
    catalog_uri: str | None
    warehouse: str | None
    table_prefix: str
    compression: str


@dataclass(frozen=True, slots=True)
class RuntimeOptions:
    spool_max_mb: int
    log_level: str
    notify_webhook: str | None

    @property
    def spool_max_bytes(self) -> int:
        return self.spool_max_mb * 1024 * 1024


@dataclass(frozen=True, slots=True)
class Options:
    s3: S3Options
    auth: AuthOptions
    capture: CaptureOptions
    format: FormatOptions
    runtime: RuntimeOptions

    def table_name(self, kind: str) -> str:
        """Table name for a logical table kind (``state_changes``, ...)."""
        return f"{self.format.table_prefix}{kind}"


# --------------------------------------------------------------------------
# helpers


def _section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw.get(name)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a mapping")
    return value


def _opt_str(section: dict[str, Any], key: str) -> str | None:
    value = section.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{key} must be a string")
    value = value.strip()
    return value or None


def _opt_bool(section: dict[str, Any], key: str, default: bool) -> bool:
    value = section.get(key)
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ConfigError(f"{key} must be a boolean")
    return value


def _opt_int(section: dict[str, Any], key: str, default: int) -> int:
    value = section.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{key} must be an integer")
    return value


def _opt_enum(section: dict[str, Any], key: str, allowed: tuple[str, ...], default: str | None) -> str | None:
    value = _opt_str(section, key)
    if value is None:
        return default
    if value not in allowed:
        raise ConfigError(f"{key} must be one of: {', '.join(allowed)}")
    return value


def _opt_str_list(section: dict[str, Any], key: str) -> tuple[str, ...]:
    value = section.get(key)
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ConfigError(f"{key} must be a list of strings")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ConfigError(f"{key} must be a list of strings")
        item = item.strip()
        if item:
            out.append(item)
    return tuple(out)


# --------------------------------------------------------------------------
# sections


def _parse_s3(raw: dict[str, Any]) -> S3Options:
    section = _section(raw, "s3")
    bucket = _opt_str(section, "bucket")
    if not bucket:
        raise ConfigError("s3.bucket is required")
    prefix = (_opt_str(section, "prefix") or "").strip("/")
    region = _opt_str(section, "region") or "us-east-1"
    storage_class = _opt_enum(section, "storage_class", STORAGE_CLASSES, "STANDARD") or "STANDARD"
    return S3Options(
        bucket=bucket,
        prefix=prefix,
        region=region,
        endpoint_url=_opt_str(section, "endpoint_url"),
        path_style=_opt_bool(section, "path_style", False),
        storage_class=storage_class,
    )


def _parse_auth(raw: dict[str, Any]) -> AuthOptions:
    section = _section(raw, "auth")
    access_key_id = _opt_str(section, "access_key_id")
    secret_access_key = _opt_str(section, "secret_access_key")
    session_token = _opt_str(section, "session_token")
    if secret_access_key and not access_key_id:
        raise ConfigError("auth.secret_access_key requires auth.access_key_id")
    if session_token and not secret_access_key:
        raise ConfigError("auth.session_token requires auth.secret_access_key")
    return AuthOptions(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
    )


def _parse_capture(raw: dict[str, Any]) -> CaptureOptions:
    section = _section(raw, "capture")
    attributes = _opt_enum(section, "attributes", ATTRIBUTE_MODES, "all") or "all"
    allowlist = _opt_str_list(section, "attribute_allowlist")
    if attributes == "allowlist" and not allowlist:
        raise ConfigError("capture.attribute_allowlist must not be empty when capture.attributes is 'allowlist'")

    events = [event for event in _opt_str_list(section, "events") or _DEFAULT_EVENT_TYPES]
    for event in events:
        if not event.replace("_", "").isalnum() or event != event.lower():
            raise ConfigError(f"capture.events contains an invalid event type: {event!r}")
    # state_changed is always captured into the state_changes table; make sure
    # the stream subscription list contains it exactly once.
    if "state_changed" not in events:
        events.insert(0, "state_changed")

    backfill_days = _opt_int(section, "backfill_days", 30)
    if backfill_days < 0:
        raise ConfigError("capture.backfill_days must be >= 0")

    flush_interval = _opt_int(section, "flush_interval_seconds", 300)
    if flush_interval < MIN_FLUSH_INTERVAL_SECONDS:
        LOGGER.warning(
            "capture.flush_interval_seconds %d is below the minimum; using %d",
            flush_interval,
            MIN_FLUSH_INTERVAL_SECONDS,
        )
        flush_interval = MIN_FLUSH_INTERVAL_SECONDS

    max_batch_rows = _opt_int(section, "max_batch_rows", 20000)
    if max_batch_rows < 1:
        raise ConfigError("capture.max_batch_rows must be >= 1")

    return CaptureOptions(
        include_entities=_opt_str_list(section, "include_entities"),
        exclude_entities=_opt_str_list(section, "exclude_entities"),
        include_domains=_opt_str_list(section, "include_domains"),
        exclude_domains=_opt_str_list(section, "exclude_domains"),
        events=tuple(dict.fromkeys(events)),
        registries=_opt_bool(section, "registries", True),
        attributes=attributes,
        attribute_allowlist=allowlist,
        backfill_days=backfill_days,
        flush_interval_seconds=flush_interval,
        max_batch_rows=max_batch_rows,
    )


def _parse_format(raw: dict[str, Any]) -> FormatOptions:
    section = _section(raw, "format")
    fmt = _opt_enum(section, "type", FORMAT_TYPES, "iceberg")
    if fmt is None or fmt not in FORMAT_TYPES:
        raise ConfigError(f"format.type must be one of: {', '.join(FORMAT_TYPES)}")

    catalog = _opt_enum(section, "catalog", CATALOG_TYPES, "glue")
    catalog_uri = _opt_str(section, "catalog_uri")
    warehouse = _opt_str(section, "warehouse")
    compression = _opt_enum(section, "compression", COMPRESSIONS, "zstd") or "zstd"

    if fmt == "iceberg":
        if catalog is None:
            raise ConfigError("format.catalog is required when format.type is 'iceberg'")
        if catalog != "glue" and not catalog_uri:
            raise ConfigError(f"format.catalog_uri is required for the '{catalog}' catalog")
    if fmt == "jsonl" and compression not in _JSONL_COMPRESSIONS:
        raise ConfigError(
            f"format.compression '{compression}' is not supported for jsonl; "
            f"use one of: {', '.join(_JSONL_COMPRESSIONS)}"
        )

    table_prefix = _opt_str(section, "table_prefix") or "ha_"
    if not set(table_prefix) <= _TABLE_NAME_CHARS:
        raise ConfigError("format.table_prefix may only contain letters, digits and underscores")

    return FormatOptions(
        type=fmt,
        catalog=catalog if fmt == "iceberg" else None,
        catalog_uri=catalog_uri,
        warehouse=warehouse,
        table_prefix=table_prefix,
        compression=compression,
    )


def _parse_runtime(raw: dict[str, Any]) -> RuntimeOptions:
    section = _section(raw, "runtime")
    spool_max_mb = _opt_int(section, "spool_max_mb", 256)
    if spool_max_mb < 1:
        raise ConfigError("runtime.spool_max_mb must be >= 1")
    if spool_max_mb > MAX_SPOOL_MB:
        LOGGER.warning(
            "runtime.spool_max_mb %d exceeds the hard cap; clamping to %d",
            spool_max_mb,
            MAX_SPOOL_MB,
        )
        spool_max_mb = MAX_SPOOL_MB

    notify_webhook = _opt_str(section, "notify_webhook")
    if notify_webhook and not notify_webhook.startswith(("http://", "https://")):
        raise ConfigError("runtime.notify_webhook must be an http(s) URL")

    return RuntimeOptions(
        spool_max_mb=spool_max_mb,
        log_level=_opt_enum(section, "log_level", LOG_LEVELS, "info") or "info",
        notify_webhook=notify_webhook,
    )


# --------------------------------------------------------------------------
# public API


def parse_options(raw: dict[str, Any]) -> Options:
    """Validate raw option JSON and return an :class:`Options` object."""
    if not isinstance(raw, dict):
        raise ConfigError("options must be a JSON object")
    return Options(
        s3=_parse_s3(raw),
        auth=_parse_auth(raw),
        capture=_parse_capture(raw),
        format=_parse_format(raw),
        runtime=_parse_runtime(raw),
    )


def load_options(path: str | os.PathLike[str] | None = None) -> Options:
    """Load options from the add-on's ``options.json`` (default ``/data``)."""
    options_path = Path(path or os.environ.get("OPTIONS_PATH", "/data/options.json"))
    try:
        raw = json.loads(options_path.read_text(encoding="utf-8"))
    except FileNotFoundError as err:
        raise ConfigError(f"options file not found: {options_path}") from err
    except json.JSONDecodeError as err:
        raise ConfigError(f"options file is not valid JSON: {options_path}") from err
    return parse_options(raw)
