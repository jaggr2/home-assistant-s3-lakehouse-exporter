"""Iceberg writer against a real (local) pyiceberg SQL catalog."""

from __future__ import annotations

import pytest
from exporter.writers.iceberg import IcebergWriter, catalog_properties

from tests.conftest import FakeStore, make_options, make_rows, ts

STATE_ROWS = [
    (
        "state_changes",
        ts(2026, 10, 24, 12),
        {
            "entity_id": "sensor.a",
            "state": "1",
            "attributes": '{"x":1}',
            "last_changed": "2026-10-24T12:00:00Z",
            "last_updated": "2026-10-24T12:00:00Z",
            "old_state": "0",
            "context_id": "c1",
            "context_user_id": None,
            "context_parent_id": None,
            "origin": "core",
        },
    ),
    (
        "state_changes",
        ts(2026, 10, 24, 12, 1),
        {
            "entity_id": "sensor.b",
            "state": "2",
            "attributes": None,
            "last_changed": "2026-10-24T12:01:00Z",
            "last_updated": "2026-10-24T12:01:00Z",
            "old_state": None,
            "context_id": None,
            "context_user_id": "u1",
            "context_parent_id": None,
            "origin": "core",
        },
    ),
]


@pytest.fixture()
def iceberg_options(tmp_path):
    return make_options(
        s3={"bucket": "test-bucket", "prefix": "home-assistant"},
        format={
            "type": "iceberg",
            "catalog": "sql",
            "catalog_uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
            "warehouse": str(tmp_path / "warehouse"),
            "table_prefix": "ha_",
            "compression": "zstd",
        },
    )


def test_iceberg_roundtrip_and_partitioning(iceberg_options) -> None:
    writer = IcebergWriter(FakeStore(), iceberg_options)
    results = writer.write_batch(make_rows(STATE_ROWS))
    assert [result.table for result in results] == ["ha_state_changes"]
    assert results[0].rows == 2

    table = writer._tables["state_changes"]
    arrow = table.scan().to_arrow()
    assert arrow.num_rows == 2
    by_entity = {row["entity_id"]: row for row in arrow.to_pylist()}
    assert by_entity["sensor.a"]["attributes"] == '{"x":1}'
    assert by_entity["sensor.a"]["context_user_id"] is None
    assert by_entity["sensor.b"]["context_user_id"] == "u1"
    assert by_entity["sensor.b"]["old_state"] is None

    # partitioned by day on the event timestamp when pyiceberg-core is present
    from exporter.writers.iceberg import has_partition_support

    spec_names = {field.name for field in table.spec().fields}
    assert spec_names == ({"last_updated_day"} if has_partition_support() else set())

    # committed files live under the configured warehouse
    assert table.location().startswith(iceberg_options.format.warehouse)


def test_iceberg_schema_stability_additive_only(iceberg_options) -> None:
    writer = IcebergWriter(FakeStore(), iceberg_options)

    with_extra = [(*row[:2], dict(row[2], surprise="yes")) for row in STATE_ROWS[:1]]
    writer.write_batch(make_rows(with_extra))
    names = [field.name for field in writer._tables["state_changes"].schema().fields]
    assert "surprise" in names
    assert "entity_id" in names

    # another new column later: appended, nothing renamed or dropped
    writer.write_batch(make_rows([(*row[:2], dict(row[2], another=2)) for row in STATE_ROWS[1:]]))
    names = [field.name for field in writer._tables["state_changes"].schema().fields]
    assert {"surprise", "another", "old_state"} <= set(names)

    arrow = writer._tables["state_changes"].scan().to_arrow()
    assert arrow.num_rows == 2
    by_entity = {row["entity_id"]: row for row in arrow.to_pylist()}
    assert by_entity["sensor.a"]["surprise"] == "yes"
    assert by_entity["sensor.b"]["another"] == "2"


def test_iceberg_multiple_tables(iceberg_options) -> None:
    writer = IcebergWriter(FakeStore(), iceberg_options)
    writer.write_batch(
        make_rows(
            [
                ("events", ts(2026, 10, 24, 12), {"event_type": "x", "time_fired": "2026-10-24T12:00:00Z",
                                                   "origin": "core", "context_id": None,
                                                   "context_user_id": None, "data": "{}"}),
                ("registry_areas", ts(2026, 10, 24, 12), {"id": "kitchen", "name": "Kitchen",
                                                          "aliases": "[]",
                                                          "snapshot_at": "2026-10-24T12:00:00Z"}),
            ]
        )
    )
    assert set(writer._tables) == {"events", "registry_areas"}
    assert writer._tables["events"].scan().to_arrow().num_rows == 1


def test_partition_spec_depends_on_pyiceberg_core(monkeypatch) -> None:
    from exporter.writers.base import TABLE_COLUMNS
    from exporter.writers.iceberg import partition_spec_for

    state_columns = TABLE_COLUMNS["state_changes"]
    registry_columns = TABLE_COLUMNS["registry_areas"]

    monkeypatch.setattr("exporter.writers.iceberg.has_partition_support", lambda: False)
    assert partition_spec_for("state_changes", state_columns).fields == ()

    monkeypatch.setattr("exporter.writers.iceberg.has_partition_support", lambda: True)
    assert [f.name for f in partition_spec_for("state_changes", state_columns).fields] == [
        "last_updated_day"
    ]
    assert [f.name for f in partition_spec_for("registry_areas", registry_columns).fields] == [
        "snapshot_at_day"
    ]


def test_catalog_properties_mapping(iceberg_options) -> None:
    props = catalog_properties(iceberg_options)
    assert props["type"] == "sql"
    assert props["uri"].startswith("sqlite:///")
    assert props["warehouse"].endswith("warehouse")
    assert props["s3.region"] == "eu-central-1"
    assert props["s3.access-key-id"] == "AKIA_TEST"
    assert props["s3.secret-access-key"] == "secret-value"

    custom = make_options(
        s3={"bucket": "b", "path_style": True, "endpoint_url": "http://minio:9000"},
        format={"type": "iceberg", "catalog": "glue"},
    )
    props = catalog_properties(custom)
    assert props["glue.region"] == "eu-central-1"
    assert props["s3.endpoint"] == "http://minio:9000"
    assert props["s3.force-virtual-addressing"] == "false"

    # plain AWS S3: virtual-hosted addressing, no explicit endpoint
    aws_props = catalog_properties(
        make_options(s3={"bucket": "b"}, format={"type": "iceberg", "catalog": "glue"})
    )
    assert "s3.endpoint" not in aws_props
    assert "s3.force-virtual-addressing" not in aws_props
