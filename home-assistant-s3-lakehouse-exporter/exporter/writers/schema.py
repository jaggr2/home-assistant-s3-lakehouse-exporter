"""Shared value coercion and Arrow schema mapping for the writers.

Payload values are JSON-ish Python objects. Writers map the logical column
types onto physical types here so Parquet and Iceberg agree, and so unknown
(additive) columns and nulls are always tolerated.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from exporter.writers.base import to_datetime

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from exporter.spool import SpoolRow
    from exporter.writers.base import Column


def as_string(value: Any) -> str | None:
    """Coerce a payload value to a string column value (``None`` stays null)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, separators=(",", ":"), default=str)


def as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return bool(value)


def column_value(column: Column, value: Any) -> Any:
    if value is None:
        return None
    if column.type == "timestamp":
        return to_datetime(value)
    if column.type == "bool":
        return as_bool(value)
    return as_string(value)


def column_values(columns: Sequence[Column], rows: Sequence[SpoolRow]) -> dict[str, list[Any]]:
    """Column-oriented, coerced values for a batch (missing keys become nulls)."""
    return {
        column.name: [column_value(column, row.payload.get(column.name)) for row in rows]
        for column in columns
    }


def arrow_schema_for(columns: Sequence[Column]):
    import pyarrow as pa

    return pa.schema([pa.field(column.name, arrow_type(column), nullable=True) for column in columns])


def arrow_type(column: Column):
    import pyarrow as pa

    if column.type == "timestamp":
        return pa.timestamp("us", tz="UTC")
    if column.type == "bool":
        return pa.bool_()
    return pa.string()
