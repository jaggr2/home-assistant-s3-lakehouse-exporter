# AGENTS.md — work package for agents (and humans) building this add-on

This repo contains the **Home Assistant S3 Lakehouse Exporter** add-on. It is a
**generic, public, standalone product**: it exports Home Assistant history to
S3-compatible object storage as Apache Iceberg, Parquet, or JSONL. It must have
**zero dependencies on any specific lab, account, or org** — the first consumer
is a private AWS lakehouse (Athena + Glue), but nothing in the code may assume
that. MinIO, R2, and Garage users are first-class.

You are implementing this from scratch. `config.yaml` (the add-on config
contract) and `repository.yaml` are already in place — treat them as the
interface contract.

## 1. Hard constraints (non-negotiable)

- **Self-contained**: no dependencies on private/internal code or
  infrastructure. PyPI dependencies are allowed but must be pinned.
- **Multi-arch**: `linux/amd64` + `linux/arm64` images.
- **Resource budget**: ≤ 512 MB RSS steady state, one CPU share. Local spool
  default 256 MB, hard cap 512 MB. **Degrade, never grow**: under memory or
  spool pressure first drop attributes to `minimal`, then lengthen the flush
  interval, then drop the oldest batches with a loud log warning. Never OOM
  the Home Assistant host.
- **Python 3.12**. All text files **LF** line endings.
- **No telemetry.** Network access only to HA core, the configured storage
  endpoint, and the configured catalog endpoint.
- **Read-only toward Home Assistant**: never write HA state. Never open the
  recorder database directly — the HA API/WebSocket only.
- Secrets (access keys, webhook URLs) use `password:` schema types and must
  never appear in logs, error messages, or the health endpoint.

## 2. Target architecture

Long-running service, supervised via s6-overlay on the official
`ghcr.io/home-assistant/{arch}-base` images (use `init: false` + s6 stage2
scripts, per current HA add-on conventions):

1. **CDC stream**: WebSocket to HA core (`ws://supervisor/core/websocket`,
   auth via `SUPERVISOR_TOKEN` — provided because `homeassistant_api: true`).
   Subscribe to `state_changed` plus the configured `capture.events` list
   (e.g. `automation_triggered`, `call_service`).
2. **Backfill + gap repair**: REST `GET /api/history/period/<ts>` for
   `capture.backfill_days` at startup, and again after stream gaps
   (reconnects, spool recovery) so the export is gap-free.
3. **Spool first (at-least-once)**: every event lands in a local **sqlite**
   spool in `/data` (WAL mode) before any network write. A background drainer
   batches from the spool and writes to storage. Storage outage ⇒ spool grows
   to `spool_max_mb`, then oldest rows are dropped with a warning. Downstream
   consumers dedupe; do not build a dedupe layer here.
4. **Micro-batch flush**: every `flush_interval_seconds` or
   `max_batch_rows`, whichever comes first.
5. **Registry snapshots** (`capture.registries: true`): entity registry,
   device registry, and area registry (WebSocket `config/*/list` commands or
   REST) snapshotted at startup and daily → dimension tables
   (`registry_entities`, `registry_devices`, `registry_areas`) with a
   `snapshot_at` column. Slow-changing dimensions — append snapshots, do not
   try to diff.
6. **Health & failure surfacing**: HTTP health endpoint (default port `8099`,
   path `/health`) for Supervisor `watchdog`; report `ok | degraded | failed`
   plus spool depth, last successful flush, last error. Optional
   `runtime.notify_webhook` POSTed on state transitions only (not per event).

## 3. Storage layout & schemas

Key layout: `{s3.prefix}/{table}/year=YYYY/month=MM/day=DD/part-<uuid>.<ext>`
(time bucketed on the event's `last_updated` / `time_fired`).

Tables (with `format.table_prefix`, default `ha_`):

| Table | Columns (core) |
|---|---|
| `state_changes` | `entity_id`, `state`, `attributes` (JSON string), `last_changed`, `last_updated`, `old_state`, `context_id`, `context_user_id`, `context_parent_id`, `origin` |
| `events` | `event_type`, `time_fired`, `origin`, `context_id`, `context_user_id`, `data` (JSON string) |
| `registry_entities` | `entity_id`, `device_id`, `area_id`, `name`, `original_name`, `platform`, `disabled`, `snapshot_at` |
| `registry_devices` | `id`, `name`, `model`, `manufacturer`, `area_id`, `identifiers` (JSON), `snapshot_at` |
| `registry_areas` | `id`, `name`, `aliases` (JSON), `snapshot_at` |

- Keep `attributes` / `data` as **JSON string columns** — attribute shapes vary
  per entity and must not explode the schema. `attributes: minimal` writes only
  `attribute_allowlist` keys; `minimal` without allowlist writes none.
- **Schema stability**: additive evolution only (new columns allowed, never
  rename or drop). Writers must tolerate unknown/extra columns and nulls.

## 4. Formats & catalogs (v1 scope — all of it)

- `format.type`:
  - `iceberg` — pyiceberg tables (Parquet data files, zstd).
  - `parquet` — plain Parquet objects (zstd).
  - `jsonl` — newline-delimited JSON, zstd-compressed (`.jsonl.zst`).
- `format.catalog` (Iceberg only): `glue | rest | nessie | hive | sql` via
  pyiceberg's catalog implementations; `catalog_uri` + `warehouse` carry the
  endpoint details (for `sql`, `catalog_uri` is a SQLAlchemy URL).
- `s3.endpoint_url` + `s3.path_style` make MinIO/R2/Garage work; when
  `endpoint_url` is empty, use the AWS S3 endpoint for `s3.region`.
- Parquet/JSONL modes must work **without any catalog** — just objects in a
  bucket. That is the low-barrier mode for casual users.

## 5. Auth

Static credentials from add-on config (`auth.access_key_id` /
`auth.secret_access_key` / optional `auth.session_token`). Use standard AWS
SDK chain semantics (env → config), but populate them from the add-on config.
No OAuth, no credential brokers, no instance metadata assumptions — those are
deployment-specific and out of scope.

The README must document a **least-privilege IAM example policy**:
`s3:PutObject` + `s3:AbortMultipartUpload` on `arn:aws:s3:::BUCKET/PREFIX/*`,
prefix-conditioned `s3:ListBucket`, optional `kms:Encrypt`; plus a MinIO
console-recipe equivalent.

## 6. Repo layout (target)

```
repository.yaml                                     (given)
home-assistant-s3-lakehouse-exporter/
  config.yaml                                       (given — interface contract)
  Dockerfile
  build.yaml                                        (base image / build args for HA builder)
  rootfs/etc/s6-overlay/s6-rc.d/...                 (service supervision)
  exporter/                                         (the Python package)
    __main__.py  config.py  capture.py  spool.py  batcher.py  writers/
      base.py  jsonl.py  parquet.py  iceberg.py
    registry.py  health.py  notify.py
  tests/                                            (pytest)
README.md
CHANGELOG.md
.github/workflows/ci.yml
```

## 7. CI/CD & release

- **PR**: `ruff check` + `pytest` + Docker build (both arches, no push).
- **Tag/main with version bump**: buildx multi-arch build + push to
  `ghcr.io/jaggr2/home-assistant-s3-lakehouse-exporter` (single multi-arch
  manifest — `config.yaml` references it without `{arch}`). If Supervisor
  rejects the manifest list, fall back to per-arch tags
  `ghcr.io/jaggr2/{arch}-home-assistant-s3-lakehouse-exporter` and adjust
  `config.yaml` `image:` with the `{arch}` placeholder.
- A release requires a `version:` bump in `config.yaml` **and** a CHANGELOG
  entry (add-on store users get updates from the `version:` bump).
- Enable Renovate for the base image (pin digests) and Python deps.

## 8. Testing bar

- pytest unit tests for: spool crash-safety and cap/drop-oldest behavior,
  batcher timing/row-count triggers, key layout partitioning, config
  validation/normalization (including defaults and `str?` optionals),
  attribute filtering modes, gap-detection logic (mocked WebSocket), and each
  writer's schema-stability guarantees (extra column → additive only).
- An optional local `docker-compose.yml` with MinIO for integration runs is
  welcome (not CI-blocking).

## 9. Suggested build order

1. **P1 — skeleton + the easy path**: config parsing, capture (backfill + WS),
   spool, batcher, `jsonl` + `parquet` writers to S3/MinIO, health endpoint,
   CI, README core sections. *(Runnable end-to-end against MinIO.)*
2. **P2 — Iceberg + Glue**: `iceberg` writer with the `glue` catalog.
3. **P3 — breadth**: `rest`/`nessie`/`hive`/`sql` catalogs, registry
   snapshots, notify webhook, docs complete (IAM policy, MinIO recipe, privacy
   guidance) → tag **v1.0.0**.
4. **P4 — polish**: integration tests, performance pass (mmap spool reads,
   file-size targets ~128 MB), memory profiling report.

## 10. Decisions already made — do not revisit without asking

- Add-on name/slug: `home-assistant-s3-lakehouse-exporter`.
- Dedicated repo per add-on (this repo); prebuilt GHCR image, Supervisor pulls.
- v1 format scope: **iceberg + parquet + jsonl**, catalogs:
  **glue + rest + nessie + hive + sql**.
- Auth: static credentials only.
- At-least-once semantics; downstream dedupes.
- Resource budget and degrade behavior per §1.

## 11. Open decisions (ask the maintainer, then record here)

- License (MIT or Apache-2.0 suggested for a public HA add-on).
- Minimum supported HA core version (drives which WS registry commands are
  safe to use).
- Whether `attribute_allowlist` should also apply to `events.data`.

## 12. Style

- Conventional Commits (`feat(writers): ...`, `fix(spool): ...`).
- Keep diffs reviewable; pin dependencies; LF everywhere.
- README is user-facing: config reference table, IAM policy, MinIO recipe,
  privacy note ("what leaves your HA instance"), troubleshooting, and the
  resource budget. No internal jargon.
