"""Profile memory and throughput of the spool -> writer pipeline.

Ingests synthetic state_changes rows through the real :class:`Spool` and
writer code and reports peak RSS, throughput and object sizes. This is the
harness behind ``docs/performance.md``; it is not part of the add-on runtime.

Usage::

    python scripts/profile_ingest.py --rows 200000 --format parquet
    python scripts/profile_ingest.py --rows 500000 --format jsonl --trace

Objects are written to a local directory (``--workdir``) instead of S3 so the
measurement isolates exporter memory from the network stack.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from exporter.batcher import Drainer
from exporter.config import parse_options
from exporter.spool import Spool
from exporter.writers import create_writer

SAMPLE_INTERVAL = 0.02


class PeakRss:
    """Samples resident memory in the background and keeps the maximum."""

    def __init__(self) -> None:
        import psutil

        self._process = psutil.Process()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.baseline = self._process.memory_info().rss
        self.peak = self.baseline

    def _run(self) -> None:
        while not self._stop.is_set():
            rss = self._process.memory_info().rss
            self.peak = max(self.peak, rss)
            self._stop.wait(SAMPLE_INTERVAL)

    def __enter__(self) -> PeakRss:
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)
        self.peak = max(self.peak, self._process.memory_info().rss)

    @property
    def peak_mb(self) -> float:
        return self.peak / (1024 * 1024)

    @property
    def delta_mb(self) -> float:
        return (self.peak - self.baseline) / (1024 * 1024)


class FileStore:
    """ObjectStore that writes to a local directory."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.objects = 0
        self.bytes = 0

    def put_object(self, key: str, body: bytes, content_type: str) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        self.objects += 1
        self.bytes += len(body)


def _payload(index: int, attributes_bytes: int) -> dict:
    stamp = (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index)).isoformat()
    return {
        "entity_id": f"sensor.profiling_{index % 250}",
        "state": str(index % 100),
        "attributes": json.dumps(
            {"friendly_name": f"Sensor {index % 250}", "blob": "x" * attributes_bytes}
        ),
        "last_changed": stamp,
        "last_updated": stamp,
        "old_state": str((index - 1) % 100),
        "context_id": f"ctx-{index}",
        "context_user_id": None,
        "context_parent_id": None,
        "origin": "core",
    }


def profile(rows: int, attributes_bytes: int, batch_rows: int, fmt: str, workdir: Path) -> dict:
    options = parse_options(
        {
            "s3": {"bucket": "profiling", "prefix": "home-assistant", "region": "eu-central-1"},
            "auth": {},
            "capture": {"max_batch_rows": batch_rows, "backfill_days": 0},
            "format": {"type": fmt, "compression": "zstd"},
            "runtime": {"spool_max_mb": 512},
        }
    )
    spool = Spool(workdir / "spool.db", options.runtime.spool_max_bytes)
    store = FileStore(workdir / "out")
    writer = create_writer(store, options)
    drainer = Drainer(spool, writer, options)

    results: dict = {
        "rows": rows,
        "attributes_bytes": attributes_bytes,
        "max_batch_rows": batch_rows,
        "format": fmt,
    }

    with PeakRss() as rss:
        started = time.perf_counter()
        batch: list = []
        for index in range(rows):
            batch.append(("state_changes", datetime.now(UTC), _payload(index, attributes_bytes)))
            if len(batch) >= 1000:
                spool.add_many(batch)
                batch.clear()
        if batch:
            spool.add_many(batch)
        ingest_seconds = time.perf_counter() - started
        results["ingest_seconds"] = round(ingest_seconds, 2)
        results["ingest_rows_per_second"] = int(rows / ingest_seconds) if ingest_seconds else None
        results["spool_bytes_after_ingest"] = spool.total_bytes()
        ingest_peak = rss.peak_mb

        started = time.perf_counter()
        flushed = asyncio.run(_drain(drainer, spool))
        drain_seconds = time.perf_counter() - started
        results["drain_seconds"] = round(drain_seconds, 2)
        results["drain_rows_per_second"] = int(flushed / drain_seconds) if drain_seconds else None
        results["objects_written"] = store.objects
        results["bytes_written"] = store.bytes
        results["average_object_mb"] = round(store.bytes / store.objects / (1024 * 1024), 2) if store.objects else None

    results["peak_rss_mb"] = round(rss.peak_mb, 1)
    results["peak_rss_delta_mb"] = round(rss.delta_mb, 1)
    results["peak_rss_ingest_mb"] = round(ingest_peak, 1)
    results["spool_rows_remaining"] = spool.pending()
    spool.close()
    return results


async def _drain(drainer: Drainer, spool: Spool) -> int:
    flushed = 0
    while spool.pending():
        flushed += await drainer.flush()
    return flushed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--attributes-bytes", type=int, default=200)
    parser.add_argument("--batch-rows", type=int, default=20_000)
    parser.add_argument("--format", choices=["parquet", "jsonl"], default="parquet")
    parser.add_argument("--workdir", type=Path, default=None)
    parser.add_argument("--trace", action="store_true", help="also report tracemalloc peak")
    args = parser.parse_args()

    workdir = args.workdir or Path(tempfile.mkdtemp(prefix="ha-exporter-profile-"))
    workdir.mkdir(parents=True, exist_ok=True)
    try:
        if args.trace:
            import tracemalloc

            tracemalloc.start()
        results = profile(
            rows=args.rows,
            attributes_bytes=args.attributes_bytes,
            batch_rows=args.batch_rows,
            fmt=args.format,
            workdir=workdir,
        )
        if args.trace:
            import tracemalloc

            _current, peak = tracemalloc.get_traced_memory()
            results["tracemalloc_peak_mb"] = round(peak / (1024 * 1024), 1)
            tracemalloc.stop()
        print(json.dumps(results, indent=2))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
