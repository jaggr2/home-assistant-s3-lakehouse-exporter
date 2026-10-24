"""Integration tests against a real S3-compatible endpoint (MinIO et al.).

These are **not** run by default (see ``addopts`` in ``pyproject.toml``) and
are not part of CI. Start the bundled stack first:

    docker compose up -d

then run with connection settings from the environment:

    HA_EXPORTER_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
    HA_EXPORTER_TEST_S3_ACCESS_KEY=ha-exporter \
    HA_EXPORTER_TEST_S3_SECRET_KEY=<secret> \
    HA_EXPORTER_TEST_S3_BUCKET=ha-lakehouse \
    pytest -m integration
"""

from __future__ import annotations

import asyncio
import gzip
import io
import json
import os
from uuid import uuid4

import pytest
import zstandard
from exporter.spool import Spool, parse_iso
from exporter.writers.base import S3ObjectStore
from exporter.writers.jsonl import JsonlWriter
from exporter.writers.parquet import ParquetWriter

from tests.conftest import make_options, make_rows, ts

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("HA_EXPORTER_TEST_S3_ENDPOINT"),
        reason="set HA_EXPORTER_TEST_S3_ENDPOINT to run S3 integration tests",
    ),
]


def _options(**format_overrides):
    return make_options(
        s3={
            "bucket": os.environ.get("HA_EXPORTER_TEST_S3_BUCKET", "ha-lakehouse"),
            "prefix": f"it-{uuid4().hex[:8]}",
            "region": "us-east-1",
            "endpoint_url": os.environ["HA_EXPORTER_TEST_S3_ENDPOINT"],
            "path_style": True,
        },
        auth={
            "access_key_id": os.environ.get("HA_EXPORTER_TEST_S3_ACCESS_KEY", "ha-exporter"),
            "secret_access_key": os.environ.get("HA_EXPORTER_TEST_S3_SECRET_KEY", "test-secret"),
        },
        format={"type": "parquet", "compression": "zstd", **format_overrides},
    )


def _s3_client(options):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=options.s3.endpoint_url,
        region_name=options.s3.region,
        aws_access_key_id=options.auth.access_key_id,
        aws_secret_access_key=options.auth.secret_access_key,
    )


def _get(options, key: str) -> bytes:
    response = _s3_client(options).get_object(Bucket=options.s3.bucket, Key=key)
    return response["Body"].read()


def _rows(count: int):
    return make_rows(
        [
            (
                "state_changes",
                ts(2026, 10, 24, 12, minute),
                {
                    "entity_id": f"sensor.it_{minute}",
                    "state": str(minute),
                    "attributes": json.dumps({"unit_of_measurement": "W"}),
                    "last_changed": "2026-10-24T12:00:00Z",
                    "last_updated": "2026-10-24T12:00:00Z",
                    "old_state": None,
                    "context_id": f"ctx-{minute}",
                    "context_user_id": None,
                    "context_parent_id": None,
                    "origin": "core",
                },
            )
            for minute in range(count)
        ]
    )


def test_object_store_roundtrip() -> None:
    options = _options()
    store = S3ObjectStore(options)
    key = f"{options.s3.prefix}/probe/part-{uuid4().hex}.txt"
    store.put_object(key, b"hello integration", "text/plain")
    assert _get(options, key) == b"hello integration"


def test_jsonl_writer_against_s3() -> None:
    options = _options(type="jsonl", compression="zstd")
    writer = JsonlWriter(S3ObjectStore(options), options)
    results = writer.write_batch(_rows(5))
    assert len(results) == 1

    key = results[0].key
    assert key.startswith(f"{options.s3.prefix}/ha_state_changes/")
    body = zstandard.ZstdDecompressor().decompress(_get(options, key))
    lines = body.decode().splitlines()
    assert len(lines) == 5
    assert json.loads(lines[0])["entity_id"] == "sensor.it_0"


def test_parquet_writer_against_s3() -> None:
    import pyarrow.parquet as pq

    options = _options(type="parquet", compression="gzip")
    writer = ParquetWriter(S3ObjectStore(options), options)
    results = writer.write_batch(_rows(10))
    assert len(results) == 1

    raw = gzip.decompress(_get(options, results[0].key))
    table = pq.read_table(io.BytesIO(raw))
    assert table.num_rows == 10
    assert "entity_id" in table.schema.names
    assert str(table.schema.field("last_updated").type) == "timestamp[us, tz=UTC]"


def test_drainer_end_to_end_against_s3(tmp_path) -> None:
    from exporter.batcher import Drainer

    options = _options(type="parquet", compression="zstd")
    spool = Spool(tmp_path / "spool.db", max_bytes=64 * 1024 * 1024)
    writer = ParquetWriter(S3ObjectStore(options), options)
    drainer = Drainer(spool, writer, options)

    rows = _rows(150)
    spool.add_many([(row.kind, parse_iso(row.event_time), row.payload) for row in rows])
    written = asyncio.run(drainer.flush())

    assert written == 150
    assert spool.pending() == 0  # rows dropped only after a successful write
    spool.close()
