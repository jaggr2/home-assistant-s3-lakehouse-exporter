"""Newline-delimited JSON writer (optionally zstd/gzip compressed).

The low-barrier mode: plain objects in a bucket, no catalog required. Payloads
are written as-is — timestamps stay ISO 8601 strings and ``attributes`` /
``data`` stay JSON strings — so every row is self-describing and trivially
readable with ``zstdcat``/``gunzip``, DuckDB or Spark.
"""

from __future__ import annotations

import gzip
import json
import logging
from typing import TYPE_CHECKING

from exporter.writers.base import BatchWriter, WriteResult

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from exporter.spool import SpoolRow

LOGGER = logging.getLogger(__name__)

_EXTENSIONS = {
    "zstd": ".jsonl.zst",
    "gzip": ".jsonl.gz",
    "none": ".jsonl",
}
_CONTENT_TYPES = {
    "zstd": "application/zstd",
    "gzip": "application/gzip",
    "none": "application/x-ndjson",
}


class JsonlWriter(BatchWriter):
    format_type = "jsonl"

    def write_batch(self, rows: Sequence[SpoolRow]) -> list[WriteResult]:
        compression = self.options.format.compression
        extension = _EXTENSIONS[compression]
        results: list[WriteResult] = []
        for (kind, day), group in self.partition_rows(rows).items():
            body = "".join(
                json.dumps(_normalise(row.payload), separators=(",", ":"), default=str) + "\n"
                for row in group
            ).encode("utf-8")
            body = _compress(body, compression)
            key = self.object_key(kind, day, extension)
            self.store.put_object(key, body, _CONTENT_TYPES[compression])
            results.append(
                WriteResult(key=key, table=self.table_name(kind), rows=len(group), size=len(body))
            )
        return results


def _compress(body: bytes, compression: str) -> bytes:
    if compression == "zstd":
        import zstandard

        return zstandard.ZstdCompressor(level=3).compress(body)
    if compression == "gzip":
        return gzip.compress(body, compresslevel=6, mtime=0)
    return body


def _normalise(payload: dict) -> dict:
    """Make sure every value is JSON-serialisable without surprises."""
    out: dict = {}
    for key, value in payload.items():
        if isinstance(value, (dict, list)) or value is None or isinstance(value, (str, int, float, bool)):
            out[key] = value
        else:
            out[key] = str(value)
    return out
