# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-10-24

### Fixed

- Release images are now tagged with the plain version (`0.1.2`) in addition
  to the git-tag form (`v0.1.2`). The Supervisor resolves
  `image:<config.yaml version>`, so the previous `v`-only tags made add-on
  installs fail with `manifest unknown`. The `vX.Y.Z` tags remain as aliases.

## [0.1.1] - 2026-10-24

### Added

- `format.database`: target catalog database/namespace for Iceberg tables
  (Glue database, Hive/Nessie namespace). Defaults to `default`, matching
  previous behaviour; set it to write into a dedicated database such as
  `lake_bronze`.

## [0.1.0] - 2026-10-24

### Added

- Initial release of the Home Assistant S3 Lakehouse Exporter add-on.
- Live capture of Home Assistant state changes and selected event types via the
  WebSocket event stream, plus startup history backfill and gap repair so the
  export is gap-free.
- Export to any S3-compatible object storage (AWS S3, MinIO, Cloudflare R2,
  Garage) in three formats: Apache Iceberg (Glue, REST, Nessie, Hive, or SQL
  catalog), plain Parquet, and newline-delimited JSON.
- Durable local spool: events are written to local storage first and survive
  storage outages; the spool is bounded and degrades gracefully instead of
  exhausting host memory.
- Configurable entity/domain filters, event-type selection, and attribute
  capture modes (`all`, `minimal`, `allowlist`) for privacy control.
- Registry snapshots (entities, devices, areas) exported as slowly changing
  dimension tables.
- Health endpoint for Supervisor watchdog integration (`/health` on port 8099)
  reporting `ok` / `degraded` / `failed` plus spool depth and last flush.
- Optional webhook notification on health status transitions.
- Day-partitioned object key layout and an additive-only schema-stability
  guarantee for all tables.
