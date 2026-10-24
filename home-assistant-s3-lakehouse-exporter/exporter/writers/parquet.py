"""Apache Parquet writer (zstd/snappy/gzip/uncompressed).

Plain Parquet objects in a bucket — no catalog required. Timestamps become
real ``timestamp[us, tz=UTC]`` columns so query engines can prune and
aggregate them natively; everything else stays a string (``attributes`` and
``data`` remain JSON strings).
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from exporter.writers.base import BatchWriter, WriteResult
from exporter.writers.schema import arrow_schema_for, column_values

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from exporter.spool import SpoolRow

LOGGER = logging.getLogger(__name__)

_EXTENSIONS = {
    "zstd": ".parquet",
    "snappy": ".parquet",
    "gzip": ".parquet",
    "none": ".parquet",
}
_COMPRESSION = {
    "zstd": "zstd",
    "snappy": "snappy",
    "gzip": "gzip",
    "none": None,
}


class ParquetWriter(BatchWriter):
    format_type = "parquet"

    def write_batch(self, rows: Sequence[SpoolRow]) -> list[WriteResult]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        compression = _COMPRESSION[self.options.format.compression]
        results: list[WriteResult] = []
        for (kind, day), group in self.partition_rows(rows).items():
            columns = self.columns_for(kind, group)
            for chunk in self.split_by_size(group):
                table = pa.Table.from_pydict(
                    column_values(columns, chunk), schema=arrow_schema_for(columns)
                )
                buffer = io.BytesIO()
                pq.write_table(
                    table,
                    buffer,
                    compression=compression,
                    use_dictionary=True,
                    write_statistics=True,
                )
                body = buffer.getvalue()
                key = self.object_key(kind, day, _EXTENSIONS["zstd"])
                self.store.put_object(key, body, "application/vnd.apache.parquet")
                LOGGER.debug("wrote %s (%d rows, %d bytes)", key, len(chunk), len(body))
                results.append(
                    WriteResult(key=key, table=self.table_name(kind), rows=len(chunk), size=len(body))
                )
        return results
