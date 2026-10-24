"""Writer package: ``jsonl``, ``parquet`` and ``iceberg``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from exporter.writers.base import BatchWriter, WriteError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from exporter.config import Options
    from exporter.writers.base import ObjectStore


def create_writer(store: ObjectStore, options: Options) -> BatchWriter:
    """Instantiate the writer selected by ``format.type``."""
    fmt = options.format.type
    if fmt == "jsonl":
        from exporter.writers.jsonl import JsonlWriter

        return JsonlWriter(store, options)
    if fmt == "parquet":
        from exporter.writers.parquet import ParquetWriter

        return ParquetWriter(store, options)
    if fmt == "iceberg":
        from exporter.writers.iceberg import IcebergWriter

        return IcebergWriter(store, options)
    raise WriteError(f"unsupported format.type: {fmt!r}")


__all__ = ["BatchWriter", "WriteError", "create_writer"]
