# Home Assistant S3 Lakehouse Exporter

> **Status: implementation in progress (v0.1.0 skeleton).** See `AGENTS.md`
> for the full work package.

A Home Assistant **add-on** that exports HA history to S3-compatible object
storage (AWS S3, MinIO, R2, Garage, ...) as **Apache Iceberg**, **Parquet**, or
**JSONL** — ready for querying with Athena, DuckDB, Trino, or any lakehouse
engine.

- Event-stream capture (WebSocket CDC) + history backfill, gap-free
- Durable local spool: survives storage outages without losing events
- Generic by design: works with any S3-compatible target, no AWS lock-in
- Bounded resources: designed for the same box Home Assistant runs on

## Install

1. Add this repository to the Add-on store
   (`https://github.com/jaggr2/home-assistant-s3-lakehouse-exporter`).
2. Install **Home Assistant S3 Lakehouse Exporter**, fill in the config
   (bucket, region/endpoint, credentials), start.

## Configuration

<!-- TODO(P1): full option reference table -->

See `home-assistant-s3-lakehouse-exporter/config.yaml` for the option contract.

## IAM policy (least privilege)

<!-- TODO(P1): example policy, bucket policy, KMS snippet, MinIO recipe -->

## What leaves your Home Assistant instance

<!-- TODO(P1): privacy guidance -->

## Resource budget

Designed for co-located operation: ≤ 512 MB RAM, one CPU share, bounded spool.
Under pressure the exporter degrades (fewer attributes, longer flush
intervals, oldest-batch drops) instead of growing.

## License

<!-- TODO: maintainer decision (MIT or Apache-2.0 suggested) -->
