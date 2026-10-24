"""Writer behaviour: formats, compression, schema stability."""

from __future__ import annotations

import gzip
import json

import pyarrow.parquet as pq
import pytest
import zstandard
from exporter.writers import create_writer
from exporter.writers.base import WriteError, WriteResult
from exporter.writers.jsonl import JsonlWriter
from exporter.writers.parquet import ParquetWriter

from tests.conftest import FakeStore, make_options, make_rows, ts

STATE_ROW = {
    "entity_id": "sensor.kitchen",
    "state": "21.5",
    "attributes": '{"unit_of_measurement":"°C"}',
    "last_changed": "2026-10-24T12:00:00Z",
    "last_updated": "2026-10-24T12:00:00Z",
    "old_state": "21.0",
    "context_id": "abc",
    "context_user_id": None,
    "context_parent_id": None,
    "origin": "core",
}


def test_create_writer_selects_format() -> None:
    assert isinstance(create_writer(FakeStore(), make_options(format={"type": "jsonl"})), JsonlWriter)
    assert isinstance(
        create_writer(FakeStore(), make_options(format={"type": "parquet"})), ParquetWriter
    )


def test_jsonl_roundtrip_and_layout() -> None:
    store = FakeStore()
    options = make_options(format={"type": "jsonl", "compression": "none"})
    writer = JsonlWriter(store, options)
    rows = make_rows([("state_changes", ts(2026, 10, 24, 12), STATE_ROW)])

    results = writer.write_batch(rows)
    assert len(results) == 1
    assert isinstance(results[0], WriteResult)
    key = results[0].key
    assert key.startswith("home-assistant/ha_state_changes/year=2026/month=10/day=24/part-")
    assert key.endswith(".jsonl")

    body, content_type = store.objects[key]
    assert content_type == "application/x-ndjson"
    lines = body.decode("utf-8").splitlines()
    assert json.loads(lines[0]) == STATE_ROW


def test_jsonl_zstd_and_gzip() -> None:
    for compression, module_check in (("zstd", zstandard), ("gzip", gzip)):
        store = FakeStore()
        options = make_options(format={"type": "jsonl", "compression": compression})
        writer = JsonlWriter(store, options)
        writer.write_batch(make_rows([("events", ts(2026, 10, 24, 12), {"event_type": "x"})]))
        key = store.keys()[0]
        body = store.objects[key][0]
        if compression == "zstd":
            assert key.endswith(".jsonl.zst")
            text = module_check.ZstdDecompressor().decompress(body).decode()
        else:
            assert key.endswith(".jsonl.gz")
            text = module_check.decompress(body).decode()
        assert json.loads(text.splitlines()[0])["event_type"] == "x"


def test_jsonl_one_object_per_partition() -> None:
    store = FakeStore()
    writer = JsonlWriter(store, make_options(format={"type": "jsonl", "compression": "none"}))
    rows = make_rows(
        [
            ("state_changes", ts(2026, 10, 24, 1), {"a": 1}),
            ("state_changes", ts(2026, 10, 24, 23), {"a": 2}),
            ("state_changes", ts(2026, 10, 25, 1), {"a": 3}),
            ("events", ts(2026, 10, 24, 5), {"a": 4}),
        ]
    )
    results = writer.write_batch(rows)
    assert len(results) == 3
    assert sum(result.rows for result in results) == 4
    assert len(store.objects) == 3


def test_parquet_roundtrip_with_real_types() -> None:
    store = FakeStore()
    writer = ParquetWriter(store, make_options(format={"type": "parquet", "compression": "zstd"}))
    rows = make_rows([("state_changes", ts(2026, 10, 24, 12), STATE_ROW)])
    key = writer.write_batch(rows)[0].key
    assert key.endswith(".parquet")

    table = pq.read_table(_buffer(store, key))
    assert table.num_rows == 1
    schema = table.schema
    assert schema.field("entity_id").type == "string"
    assert str(schema.field("last_updated").type) == "timestamp[us, tz=UTC]"
    assert schema.field("context_user_id").nullable
    row = table.to_pylist()[0]
    assert row["entity_id"] == "sensor.kitchen"
    assert row["attributes"] == '{"unit_of_measurement":"°C"}'
    assert row["last_updated"].isoformat() == "2026-10-24T12:00:00+00:00"
    assert row["context_user_id"] is None


def test_parquet_schema_stability_adds_extra_columns() -> None:
    store = FakeStore()
    writer = ParquetWriter(store, make_options(format={"type": "parquet", "compression": "snappy"}))

    with_extra = dict(STATE_ROW, unexpected_column="hello")
    first = writer.write_batch(make_rows([("state_changes", ts(2026, 10, 24, 12), with_extra)]))
    table = pq.read_table(_buffer(store, first[0].key))
    assert "unexpected_column" in table.schema.names
    assert table.schema.field("unexpected_column").type == "string"
    assert table.to_pylist()[0]["unexpected_column"] == "hello"

    # a later batch without the column still writes fine (additive, tolerant)
    second = writer.write_batch(make_rows([("state_changes", ts(2026, 10, 24, 13), STATE_ROW)]))
    table = pq.read_table(_buffer(store, second[0].key))
    assert "unexpected_column" not in table.schema.names
    assert "old_state" in table.schema.names  # declared columns never disappear


def test_split_by_size_targets_object_size(monkeypatch) -> None:
    monkeypatch.setattr("exporter.writers.base.TARGET_OBJECT_BYTES", 500)
    writer = JsonlWriter(FakeStore(), make_options(format={"type": "jsonl", "compression": "none"}))
    rows = make_rows(
        [("state_changes", ts(2026, 10, 24, 12, minute), {"pad": "x" * 100}) for minute in range(20)]
    )
    chunks = writer.split_by_size(rows)
    assert len(chunks) > 1  # 20 rows x ~132 bytes exceeds the 500-byte target
    assert [len(chunk) for chunk in chunks][:-1] == [3] * (len(chunks) - 1)
    assert sum(len(chunk) for chunk in chunks) == 20

    # a single oversized row still becomes exactly one object
    big = make_rows([("state_changes", ts(2026, 10, 24, 12), {"pad": "y" * 5000})])
    assert [[row.id for row in chunk] for chunk in writer.split_by_size(big)] == [[1]]


def test_jsonl_objects_split_per_target_size(monkeypatch) -> None:
    monkeypatch.setattr("exporter.writers.base.TARGET_OBJECT_BYTES", 500)
    store = FakeStore()
    writer = JsonlWriter(store, make_options(format={"type": "jsonl", "compression": "none"}))
    rows = make_rows(
        [("state_changes", ts(2026, 10, 24, 12, minute), {"pad": "x" * 100}) for minute in range(20)]
    )
    results = writer.write_batch(rows)
    assert len(results) > 1  # one object per size chunk, all in the same partition
    assert sum(result.rows for result in results) == 20
    assert {result.key.rsplit("/part-", 1)[0] for result in results} == {
        "home-assistant/ha_state_changes/year=2026/month=10/day=24"
    }


def test_writers_tolerate_missing_and_null_values() -> None:
    store = FakeStore()
    parquet = ParquetWriter(store, make_options(format={"type": "parquet", "compression": "zstd"}))
    jsonl = JsonlWriter(store, make_options(format={"type": "jsonl", "compression": "none"}))

    sparse = {"entity_id": "sensor.x", "state": None}
    rows = make_rows([("state_changes", ts(2026, 10, 24, 12), sparse)])
    table = pq.read_table(_buffer(store, parquet.write_batch(rows)[0].key))
    assert table.to_pylist()[0]["entity_id"] == "sensor.x"
    assert table.to_pylist()[0]["old_state"] is None

    body, _ = store.objects[jsonl.write_batch(rows)[0].key]
    assert json.loads(body.decode()) == sparse


def test_unknown_table_kind_is_rejected() -> None:
    writer = ParquetWriter(FakeStore(), make_options(format={"type": "parquet"}))
    rows = make_rows([("nope", ts(2026, 10, 24, 12), {})])
    with pytest.raises(WriteError):
        writer.write_batch(rows)


def test_jsonl_mixed_value_types_are_normalised() -> None:
    store = FakeStore()
    writer = JsonlWriter(store, make_options(format={"type": "jsonl", "compression": "none"}))
    rows = make_rows(
        [("events", ts(2026, 10, 24, 12), {"event_type": "x", "data": "{}", "extra": object()})]
    )
    body, _ = store.objects[writer.write_batch(rows)[0].key]
    assert json.loads(body.decode())["extra"]  # coerced to a string, not a crash


def _buffer(store: FakeStore, key: str):
    import io

    return io.BytesIO(store.objects[key][0])
