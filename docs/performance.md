# Performance & memory report

Reproducible measurements of the exporter's ingest pipeline
(`capture → spool → writer`), produced with
`home-assistant-s3-lakehouse-exporter/scripts/profile_ingest.py`. The goal is
to verify the **resource budget** from the README: ≤ 512 MB RSS steady state,
bounded spool, degrade instead of grow.

## Method

- Synthetic `state_changes` rows with a ~200-byte attribute blob
  (~500 bytes JSON per row), written through the real `Spool` and writer code.
- Objects written to a local directory instead of S3 so the numbers isolate
  exporter memory from the network stack.
- Compression: zstd (the default). Batch size: `max_batch_rows: 20000`.
- RSS sampled every 20 ms via `psutil`; "peak RSS delta" is the peak minus the
  baseline at process start.
- Environment: Windows 11, Python 3.12.10, x86-64 laptop-class CPU. The add-on
  runs on Linux (musl) inside the container; absolute throughput there is
  similar or slightly lower, memory behaviour is the same.

## Results

| Run | Format | Rows | Spool payload | Ingest | Drain | Peak RSS (Δ) | Objects | Avg object |
|---|---|---|---|---|---|---|---|---|
| 1 | jsonl (zstd) | 200 000 | 97 MB | 2.4 s (~83k rows/s) | 2.8 s (~71k rows/s) | 77.9 MB (50.9) | 10 | 0.15 MB |
| 2 | parquet (zstd) | 500 000 | 242 MB | 5.9 s (~84k rows/s) | 8.7 s (~58k rows/s) | 115.5 MB (88.4) | 25 | 0.21 MB |
| 3 | parquet (zstd) | 200 000 | 97 MB | 10.7 s¹ | 7.2 s¹ | 173.0 MB (144.5)¹ | 10 | 0.21 MB |

¹ Run 3 has `--trace` (tracemalloc) enabled: Python allocation tracking adds
significant overhead to both time and memory. Its useful number is
**tracemalloc peak: 39.2 MB** of live Python allocations for a 20 000-row
parquet batch.

## Observations

- **Memory tracks the batch, not the spool.** 500 000 rows (242 MB spooled)
  drain in a process whose peak RSS grows by 88 MB; a 20 000-row batch itself
  peaks around 40 MB of Python allocations. Steady state stays far below the
  512 MB budget, leaving headroom for the Python runtime and buffers.
- **Ingest is cheap** (~85k rows/s): SQLite WAL inserts in transactions of
  1 000 rows, one fsync per transaction. Storage writes are the slower phase
  and run in a worker thread (`asyncio.to_thread`), so capture keeps flowing.
- **Spool reads are page-streamed.** Rows are pulled off the SQLite cursor in
  pages of 1 000, and drop-oldest scans are paged too, so a full 512 MB spool
  never materialises in memory. (SQLite is also memory-mapped by the OS; a
  custom mmap layer would not reduce Python-side memory further — bounded
  batches do.)
- **Object sizes are governed by batch size.** With the default
  `max_batch_rows: 20000`, objects land around 0.2 MB compressed (~10 MB
  payload). The writer splits any larger batch at the **128 MB payload
  target** (`TARGET_OBJECT_BYTES`) so files never grow unbounded. For fewer,
  larger files — better for lakehouse table planning — raise
  `capture.max_batch_rows` and/or `capture.flush_interval_seconds`; the
  exporter will happily drain ~128 MB objects at that setting.
- **Degrade ladder works as designed** (covered by unit tests): at 80 % spool
  fill attribute capture drops to `minimal`, at 90 % the flush interval is
  lengthened, at 95 % the oldest rows are dropped with a loud warning —
  memory and disk stay capped in every case.

## Reproduce

```sh
cd home-assistant-s3-lakehouse-exporter
pip install -r requirements-dev.txt
python scripts/profile_ingest.py --rows 200000 --format parquet --trace
python scripts/profile_ingest.py --rows 500000 --format parquet
python scripts/profile_ingest.py --rows 200000 --format jsonl
```
