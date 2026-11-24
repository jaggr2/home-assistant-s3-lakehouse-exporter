# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-11-24

### Changed

- **Default `capture.flush_interval_seconds` is now `3600` (was `300`).**
  The interval is the Iceberg commit cadence, and on per-request-priced S3
  backends each commit costs several billed requests (data file + manifest +
  manifest-list + metadata). Hourly batching cut a real deployment's request
  volume about 6x. Set `flush_interval_seconds: 300` to keep five-minute
  freshness at proportionally higher request cost.

### Added

- Iceberg tables are created with
  `write.metadata.delete-after-commit.enabled=true` and
  `write.metadata.previous-versions-max=20`, so metadata files no longer pile
  up between maintenance runs (a 10 GB metadata directory was observed in the
  wild after months of five-minute commits). The storage credentials need
  delete permission (`s3:DeleteObject`) for this to take effect - the
  least-privilege policies in the README now include it.
- `docs/performance.md` documents S3 request cost versus flush cadence and
  recommends warehouse-side table maintenance (compaction + snapshot expiry).

## [0.1.5] - 2026-10-24

### Fixed

- `catalog_properties` now always sets a default `warehouse`, including for
  the `glue` catalog. Without it pyiceberg refuses to create tables when no
  explicit location is given ("No default path is set"), so every flush failed
  on Glue even with correct credentials.

## [0.1.4] - 2026-10-24

### Fixed

- Backfill works on Home Assistant versions whose history REST API requires
  `filter_entity_id` (it used to answer `400 filter_entity_id is missing`).
  The exporter now resolves concrete entity ids from `/api/states` and sends
  them in chunks of 100.
- Registry snapshots use the correct `config/area_registry/list` WebSocket
  command (`config/area/list` is not a command, area snapshots always failed).

## [0.1.3] - 2026-10-24

### Fixed

- Namespace creation is now best-effort: least-privilege policies that omit
  `glue:CreateDatabase` (as documented in `docs/glue-athena.md`) no longer
  crash startup when the target database already exists. A missing database
  still surfaces as a clear table-creation error.

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
