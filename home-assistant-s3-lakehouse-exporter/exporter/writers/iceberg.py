"""Apache Iceberg writer (pyiceberg).

Commits Parquet data files plus metadata to the configured catalog
(``glue`` | ``rest`` | ``nessie`` | ``hive`` | ``sql``). Unlike the plain
formats there is no single object key: ``WriteResult.key`` reports the
Iceberg table the batch was committed to.

Schema stability is handled with Iceberg's additive-only evolution: a payload
column that the table does not know yet is appended as a nullable column;
nothing is ever renamed or dropped. Tables are partitioned by day on their
event timestamp when the optional ``pyiceberg-core`` extension is available;
otherwise batches are still written per day and Iceberg's per-file statistics
keep scans cheap.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

from exporter.writers.base import BatchWriter, WriteError, WriteResult
from exporter.writers.schema import arrow_schema_for, column_values

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Sequence

    from exporter.spool import SpoolRow
    from exporter.writers.base import Column

LOGGER = logging.getLogger(__name__)

#: All exported tables live in one namespace. Glue, Hive, Nessie and the SQL
#: catalog all understand this; the namespace is created on first use.
NAMESPACE = "default"


def iceberg_type(column: Column):
    from pyiceberg.types import BooleanType, StringType, TimestamptzType

    if column.type == "timestamp":
        return TimestamptzType()
    if column.type == "bool":
        return BooleanType()
    return StringType()


def iceberg_schema(columns: Sequence[Column]):
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    return Schema(
        *(
            NestedField(index + 1, column.name, iceberg_type(column), required=False)
            for index, column in enumerate(columns)
        )
    )


def has_partition_support() -> bool:
    """Day transforms need the optional ``pyiceberg-core`` extension (Rust).

    It ships glibc wheels only, so Alpine-based add-on images cannot use it.
    Without it, tables are created unpartitioned: batches are still grouped
    per day and Iceberg's per-file min/max stats prune scans well.
    """
    import importlib.util

    return importlib.util.find_spec("pyiceberg_core") is not None


#: The event timestamp each table is partitioned on.
PARTITION_COLUMNS = {
    "state_changes": "last_updated",
    "events": "time_fired",
    "registry_entities": "snapshot_at",
    "registry_devices": "snapshot_at",
    "registry_areas": "snapshot_at",
}


def partition_spec_for(kind: str, columns: Sequence[Column]):
    """Partition by day on the table's event timestamp column (if supported)."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import DayTransform

    if not has_partition_support():
        return PartitionSpec()
    source = PARTITION_COLUMNS.get(kind)
    for index, column in enumerate(columns):
        if column.type == "timestamp" and (source is None or column.name == source):
            return PartitionSpec(
                PartitionField(
                    source_id=index + 1,
                    field_id=1000,
                    transform=DayTransform(),
                    name=f"{column.name}_day",
                )
            )
    return PartitionSpec()


class IcebergWriter(BatchWriter):
    format_type = "iceberg"

    def __init__(self, store: Any, options: Any) -> None:
        super().__init__(store, options)
        from pyiceberg.catalog import load_catalog

        self._catalog = load_catalog("home_assistant", **catalog_properties(options))
        self._namespace = options.format.database or NAMESPACE
        self._catalog.create_namespace_if_not_exists(self._namespace)
        self._tables: dict[str, Any] = {}
        self._table_columns: dict[str, list[Column]] = {}
        self._lock = threading.Lock()
        if not has_partition_support():
            LOGGER.info(
                "pyiceberg-core is not installed: iceberg tables are unpartitioned "
                "(batches are still grouped per day; file stats keep scans cheap)"
            )

    def write_batch(self, rows: Sequence[SpoolRow]) -> list[WriteResult]:
        import pyarrow as pa

        results: list[WriteResult] = []
        for (kind, _day), group in self.partition_rows(rows).items():
            with self._lock:
                self._columns_for(kind, group)
                table = self._table_for(kind, self._table_columns[kind])
                columns = self._table_columns[kind]
                for chunk in self.split_by_size(group):
                    arrow = pa.Table.from_pydict(
                        column_values(columns, chunk),
                        schema=arrow_schema_for(columns),
                    )
                    try:
                        table.append(arrow)
                    except Exception as err:
                        raise WriteError(
                            f"iceberg append to {self.table_name(kind)} failed: {err}"
                        ) from err
                    LOGGER.debug(
                        "committed %d rows to iceberg table %s", len(chunk), self.table_name(kind)
                    )
                    results.append(
                        WriteResult(
                            key=self.table_name(kind),
                            table=self.table_name(kind),
                            rows=len(chunk),
                            size=arrow.nbytes,
                        )
                    )
        return results

    # -- internals --------------------------------------------------------

    def _columns_for(self, kind: str, rows: Sequence[SpoolRow]) -> list[Column]:
        columns = self.columns_for(kind, rows)
        known = self._table_columns.setdefault(kind, list(columns))
        seen = {column.name for column in known}
        for column in columns:
            if column.name not in seen:
                known.append(column)
                seen.add(column.name)
        return known

    def _table_for(self, kind: str, columns: Sequence[Column]) -> Any:
        if kind not in self._tables:
            name = self.table_name(kind)
            identifier = (self._namespace, name)
            self._tables[kind] = self._catalog.create_table_if_not_exists(
                identifier,
                iceberg_schema(columns),
                partition_spec=partition_spec_for(kind, columns),
            )
        table = self._tables[kind]
        self._ensure_columns(table, columns)
        return table

    def _ensure_columns(self, table: Any, columns: Sequence[Column]) -> None:
        """Additive-only evolution: append unknown columns as nullable."""
        existing = {field.name for field in table.schema().fields}
        missing = [column for column in columns if column.name not in existing]
        if not missing:
            return
        LOGGER.info(
            "iceberg table %s: adding column(s) %s",
            table.name,
            ", ".join(column.name for column in missing),
        )
        with table.update_schema() as update:
            for column in missing:
                update.add_column(column.name, iceberg_type(column), required=False)
        table.refresh()


def catalog_properties(options: Any) -> dict[str, str]:
    """Translate add-on options into pyiceberg catalog/FileIO properties."""
    fmt = options.format
    props: dict[str, str] = {"type": fmt.catalog}
    if fmt.catalog_uri:
        props["uri"] = fmt.catalog_uri
    if fmt.warehouse:
        props["warehouse"] = fmt.warehouse
    elif fmt.catalog in ("hive", "nessie", "sql"):
        props["warehouse"] = f"s3://{options.s3.bucket}/{options.s3.prefix}".rstrip("/")

    # Credentials and region for the Glue client and the S3 FileIO. Values are
    # secrets and must never be logged.
    props["client.region"] = options.s3.region
    props["s3.region"] = options.s3.region
    if fmt.catalog == "glue":
        props["glue.region"] = options.s3.region
    if options.auth.has_static_credentials:
        props["client.access-key-id"] = options.auth.access_key_id
        props["client.secret-access-key"] = options.auth.secret_access_key
        props["s3.access-key-id"] = options.auth.access_key_id
        props["s3.secret-access-key"] = options.auth.secret_access_key
        if options.auth.session_token:
            props["client.session-token"] = options.auth.session_token
            props["s3.session-token"] = options.auth.session_token
    if options.s3.endpoint_url:
        props["s3.endpoint"] = options.s3.endpoint_url
    if options.s3.path_style or options.s3.endpoint_url:
        # MinIO / R2 / Garage use path-style addressing; S3 endpoints default
        # to virtual-hosted style.
        props["s3.force-virtual-addressing"] = "false" if options.s3.path_style else "true"
    return props
