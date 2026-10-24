# Home Assistant S3 Lakehouse Exporter

A Home Assistant **add-on** that continuously exports your Home Assistant
history to S3-compatible object storage as **Apache Iceberg**, **Parquet**, or
**JSONL** — ready for querying with Athena, DuckDB, Trino, or any lakehouse
engine. Works with AWS S3, MinIO, Cloudflare R2, and Garage; no AWS lock-in.

- **Live capture + backfill** — subscribes to the Home Assistant event stream
  (change-data-capture) for real-time state changes and configured event types,
  and backfills history at startup (and after reconnects) so the export is
  gap-free.
- **Durable local spool** — every event is spooled locally before any network
  write. A storage outage does not lose data; the spool drains when storage
  returns.
- **Three output formats** — Apache Iceberg tables (Glue / REST / Nessie /
  Hive / SQL catalog), plain Parquet objects, or zstd-compressed JSONL. The
  Parquet and JSONL modes need no catalog at all.
- **Registry snapshots** — entity, device, and area registries exported as
  slowly changing dimension tables.
- **Bounded resources** — designed to share the box with Home Assistant;
  degrades gracefully under pressure instead of exhausting memory.
- **No telemetry** — network access only to Home Assistant, your storage
  endpoint, and (if used) your catalog endpoint.

## Requirements

- **Home Assistant core 2024.6 or newer**, installed on **Home Assistant OS or
  another Supervisor-based installation** (the add-on uses the Supervisor API
  and a health watchdog).
- **Any S3-compatible object storage**: AWS S3, MinIO, Cloudflare R2, Garage,
  or similar.

## Install

1. In Home Assistant, open **Settings → Add-ons → Add-on store**, and add this
   repository URL:
   `https://github.com/jaggr2/home-assistant-s3-lakehouse-exporter`
2. Install **Home Assistant S3 Lakehouse Exporter**.
3. Open the **Configuration** tab and fill in at least `s3.bucket` and
   credentials (`auth.access_key_id` / `auth.secret_access_key`); for MinIO,
   R2, or Garage also set `s3.endpoint_url` and `s3.path_style: true`.
4. Start the add-on. Check the **Log** tab for a startup summary.

## Configuration

Example (MinIO or Garage, catalog-free Parquet output):

```yaml
s3:
  bucket: "ha-lakehouse"
  prefix: "home-assistant"
  region: "eu-central-1"
  endpoint_url: "http://192.168.1.10:9000"
  path_style: true
auth:
  access_key_id: "ha-exporter"
  secret_access_key: "********"
capture:
  events:
    - state_changed
    - automation_triggered
  attributes: "minimal"
  attribute_allowlist:
    - unit_of_measurement
    - device_class
    - friendly_name
format:
  type: "parquet"
```

All options and their defaults:

### `s3` — object storage target

| Option | Type | Default | Description |
|---|---|---|---|
| `s3.bucket` | string | `""` | **Required.** Target bucket name. |
| `s3.prefix` | string | `"home-assistant"` | Key prefix (folder) under which all objects are written. |
| `s3.region` | string | `"eu-central-1"` | AWS region; selects the AWS S3 endpoint when `endpoint_url` is empty. |
| `s3.endpoint_url` | string (optional) | `""` | Custom S3 endpoint for MinIO, R2, Garage, etc. Empty = AWS S3. |
| `s3.path_style` | bool | `false` | Use path-style URLs (`https://endpoint/bucket/key`). Set to `true` for most MinIO and Garage setups. |
| `s3.storage_class` | enum (optional) | `"STANDARD"` | Object storage class: `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `INTELLIGENT_TIERING`, `ONEZONE_IA`. |

### `auth` — credentials

| Option | Type | Default | Description |
|---|---|---|---|
| `auth.access_key_id` | string (optional) | `""` | Static access key ID. |
| `auth.secret_access_key` | password (optional) | `""` | Secret access key. |
| `auth.session_token` | password (optional) | `""` | Session token, for temporary credentials. |

Credentials are used for the configured storage (and catalog) endpoint only.
Options marked `password` are treated as secrets: they never appear in logs,
error messages, or the health endpoint.

### `capture` — what to export

| Option | Type | Default | Description |
|---|---|---|---|
| `capture.include_entities` | list of strings | `[]` | Only capture these entity IDs. Empty = no include restriction. |
| `capture.exclude_entities` | list of strings | `[]` | Never capture these entity IDs. |
| `capture.include_domains` | list of strings | `[]` | Only capture these domains (e.g. `sensor`, `climate`). Empty = no include restriction. |
| `capture.exclude_domains` | list of strings | `[]` | Never capture these domains. |
| `capture.events` | list of strings | `["state_changed"]` | Event types to capture into the `ha_events` table (e.g. `automation_triggered`, `call_service`). State changes always land in `ha_state_changes` and need no entry here. |
| `capture.registries` | bool | `true` | Snapshot the entity, device, and area registries at startup and daily into the registry tables. |
| `capture.attributes` | enum | `"all"` | Attribute capture mode. `all`: write the full attribute set. `minimal`: write only the keys listed in `attribute_allowlist` (none if the allowlist is empty). `allowlist`: same key filtering as `minimal`, but a non-empty `attribute_allowlist` is required. |
| `capture.attribute_allowlist` | list of strings | `[]` | Attribute keys kept when `attributes` is `minimal` or `allowlist`. |
| `capture.backfill_days` | int | `30` | How far back to fetch history at startup, in days. `0` disables the startup import (gap repair after stream outages still runs). |
| `capture.flush_interval_seconds` | int | `300` | Maximum time a batch is buffered before it is written to storage. |
| `capture.max_batch_rows` | int | `20000` | Maximum rows per write; a flush is triggered as soon as this many rows are buffered. |

Flushes happen every `flush_interval_seconds` **or** every `max_batch_rows`,
whichever comes first.

### `format` — output format and catalog

| Option | Type | Default | Description |
|---|---|---|---|
| `format.type` | enum | `"iceberg"` | Output format: `iceberg`, `parquet`, or `jsonl`. |
| `format.catalog` | enum (optional) | `"glue"` | Iceberg catalog implementation (`format.type: iceberg` only): `glue`, `rest`, `nessie`, `hive`, or `sql`. |
| `format.catalog_uri` | string (optional) | `""` | Catalog endpoint URI. For `format.catalog: sql` this is a SQLAlchemy database URL. |
| `format.warehouse` | string (optional) | `""` | Warehouse setting for the catalog (location/warehouse, depending on the catalog type). |
| `format.table_prefix` | string | `"ha_"` | Prefix prepended to every table name (e.g. `ha_state_changes`). |
| `format.compression` | enum (optional) | `"zstd"` | Compression codec for data files: `zstd`, `snappy`, `gzip`, `none`. |

The `parquet` and `jsonl` formats need **no catalog** — just objects in a
bucket. That is the lowest-barrier way to get started.

### `runtime` — service behavior

| Option | Type | Default | Description |
|---|---|---|---|
| `runtime.spool_max_mb` | int | `256` | Size cap for the local spool, in MB. Hard maximum is 512. When the cap is hit, the oldest spooled rows are dropped with a loud log warning. |
| `runtime.log_level` | enum (optional) | `"info"` | Log verbosity: `debug`, `info`, `warning`, `error`. |
| `runtime.notify_webhook` | string (optional) | `""` | URL that receives a POST when the health status changes (e.g. `ok` → `degraded`). Not called per event. |

## IAM policy (least privilege)

### AWS IAM

The exporter only ever **writes** objects and (for some operations) **lists**
its own prefix. A least-privilege policy looks like this — replace `BUCKET`
and `PREFIX` (the latter matching `s3.prefix`, default `home-assistant`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WriteExportObjects",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::BUCKET/PREFIX/*"
    },
    {
      "Sid": "ListExportPrefix",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::BUCKET",
      "Condition": {
        "StringLike": {
          "s3:prefix": "PREFIX/*"
        }
      }
    }
  ]
}
```

If the bucket uses a **KMS** customer-managed key, also allow encryption of
objects (and nothing more):

```json
{
  "Sid": "EncryptExportObjects",
  "Effect": "Allow",
  "Action": "kms:Encrypt",
  "Resource": "arn:aws:kms:REGION:ACCOUNT_ID:key/KEY_ID"
}
```

### MinIO recipe

> **Note (2026):** MinIO Community Edition is archived and its container
> images are no longer publicly distributed (removed from Docker Hub in
> September 2026; `quay.io/minio/*` now requires a login). If you already run
> MinIO, the recipe below still applies unchanged — the exporter speaks plain
> S3 and works the same against any MinIO-compatible server.

```sh
# 1. Create a dedicated user (do not use the root credentials).
mc admin user add myminio ha-exporter 'CHOOSE-A-SECRET'

# 2. Give it write-only access to the prefix the add-on exports to.
cat > ha-exporter-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::ha-lakehouse/home-assistant/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::ha-lakehouse",
      "Condition": {
        "StringLike": { "s3:prefix": "home-assistant/*" }
      }
    }
  ]
}
EOF
mc admin policy create myminio ha-exporter-write ha-exporter-policy.json
mc admin policy attach myminio ha-exporter-write --user ha-exporter
```

Then point the add-on at it: `s3.endpoint_url` = your MinIO URL,
`s3.path_style: true`, `s3.bucket: ha-lakehouse`,
`auth.access_key_id` / `auth.secret_access_key` = the user you created.

A ready-to-run local S3 stack (SeaweedFS, with a bucket and a scoped
`ha-exporter` identity included) is provided via `docker-compose.yml` at the
root of this repository.

## Data model

Table names use `format.table_prefix` (default `ha_`). Objects are written
under:

```
{prefix}/{table}/year=YYYY/month=MM/day=DD/part-<uuid>.<ext>
```

partitioned by the event's own timestamp (`last_updated` / `time_fired`); the
extension depends on the format (`.parquet`, or `.jsonl.zst` when compressed).
Objects are sized toward ~128 MB of payload: larger flushes are split into
several files in the same partition.

| Table | Columns (core) |
|---|---|
| `ha_state_changes` | `entity_id`, `state`, `attributes` (JSON string), `last_changed`, `last_updated`, `old_state`, `context_id`, `context_user_id`, `context_parent_id`, `origin` |
| `ha_events` | `event_type`, `time_fired`, `origin`, `context_id`, `context_user_id`, `data` (JSON string) |
| `ha_registry_entities` | `entity_id`, `device_id`, `area_id`, `name`, `original_name`, `platform`, `disabled`, `snapshot_at` |
| `ha_registry_devices` | `id`, `name`, `model`, `manufacturer`, `area_id`, `identifiers` (JSON), `snapshot_at` |
| `ha_registry_areas` | `id`, `name`, `aliases` (JSON), `snapshot_at` |

- `attributes` and `data` are **JSON strings**, because their shape varies per
  entity and event type. Parse them in your query engine of choice.
- Registry tables are slowly changing dimensions: a full snapshot is appended
  at startup and daily, stamped with `snapshot_at`.
- **Schema stability promise**: evolution is additive only. New columns may
  appear, existing columns are never renamed or dropped. Writers tolerate
  unknown/extra columns and nulls.

## What leaves your Home Assistant instance

The exporter sends data **only** to the storage endpoint (and catalog endpoint)
**you** configure:

- state changes for the captured entities (state, timestamp, context IDs, and
  attributes — filtered per `capture.attributes`), and
- the configured `capture.events` event types, and
- optionally (`capture.registries: true`) entity/device/area registry
  snapshots.

There is **no telemetry** and no other network destinations. The exporter is
**read-only** toward Home Assistant: it never writes HA state and never touches
the recorder database directly.

**Attributes can contain sensitive values** (locations, power readings, names
of people, notification texts, …). Consider
`capture.attributes: minimal` together with a small `attribute_allowlist` —
or exclude entire entities/domains — if your storage is shared.

## Delivery guarantees

Delivery is **at-least-once**: events are spooled locally first and written to
storage from the spool, so a crash or retry around a write can produce
duplicates. Downstream consumers should deduplicate, e.g. by
`(entity_id, last_updated)` for state changes or by object key for files.

## Resource budget

Designed for co-located operation on the Home Assistant host:

- ≤ **512 MB RSS** steady state, one CPU share.
- Local spool: **256 MB default**, **512 MB hard cap** (`runtime.spool_max_mb`).
- Under memory or spool pressure the exporter **degrades, never grows**:
  1. drop attributes down to `minimal`,
  2. lengthen the flush interval,
  3. drop the oldest spooled batches — with a loud log warning.

It never OOMs the Home Assistant host.

## Health endpoint

The add-on serves `http://<host>:8099/health` (used by the Supervisor
watchdog). It returns JSON like:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_seconds": 812,
  "stream_connected": true,
  "spool": {"pending_rows": 0, "bytes": 0, "max_bytes": 268435456, "dropped_rows": 0},
  "flushed_rows": 51234,
  "written_objects": 12,
  "last_flush": "2026-10-24T12:00:00+00:00",
  "last_error": null
}
```

`status` is `ok`, `degraded`, or `failed`; the endpoint returns HTTP 503 only
for `failed` (the watchdog restarts the add-on in that case). Secrets never
appear in health output — error messages are redacted. With
`runtime.notify_webhook` set, a POST is sent to your URL whenever the status
transitions.

## Troubleshooting

Read the logs via **Settings → Add-ons → Home Assistant S3 Lakehouse Exporter
→ Log**. Raise `runtime.log_level` to `debug` for detail.

| Symptom | Likely cause / fix |
|---|---|
| `403 AccessDenied` on writes | IAM policy or bucket policy does not cover the prefix. Check the `s3:prefix` condition matches `s3.prefix` and that `s3:PutObject`/`s3:AbortMultipartUpload` are allowed on `BUCKET/PREFIX/*`. |
| Connection refused / timeout to storage | Wrong `s3.endpoint_url`, or path-style addressing not enabled. Most MinIO/Garage setups need `s3.path_style: true`. Include the scheme (`http://` / `https://`) and port. |
| Spool keeps growing (`spool depth` in `/health`) | Storage (or catalog) is unreachable or rejecting writes — the exporter buffers locally up to `runtime.spool_max_mb`, then drops the oldest rows with a warning. Fix the storage-side error seen above in the log. |
| Watchdog restarts the add-on | The watchdog polls `/health`; check `status` and `last_error` there and in the log. `failed` means the exporter cannot make progress at all. |
| No data for some entities | Filtered out by `capture.include_entities` / `include_domains` / `exclude_*`, or backfill disabled (`capture.backfill_days: 0`) and no new events yet. |
| `allowlist` rejected at start | `capture.attributes: allowlist` requires a non-empty `capture.attribute_allowlist`. |

## Querying

**Amazon Athena** (Iceberg tables registered in Glue; table names include the
`format.table_prefix`, default `ha_`):

```sql
SELECT entity_id, state, last_updated
FROM ha_state_changes
WHERE entity_id = 'sensor.outdoor_temperature'
  AND last_updated >= current_date - INTERVAL '7' DAY
ORDER BY last_updated DESC
LIMIT 100;
```

**DuckDB** (directly against Parquet/JSONL objects — no catalog needed):

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint = 'minio.example.local:9000';
SET s3_use_ssl  = false;
SET s3_access_key_id = 'ha-exporter';
SET s3_secret_access_key = '********';

SELECT entity_id, state, last_updated
FROM read_parquet('s3://ha-lakehouse/home-assistant/ha_state_changes/*/*/*/*.parquet')
WHERE last_updated >= now() - INTERVAL 7 DAY
ORDER BY last_updated DESC;
```

Parse the JSON attribute columns with `json_extract` / `json_extract_string`
(DuckDB) or `json_extract_scalar` (Athena) as needed.

## Development

```sh
cd home-assistant-s3-lakehouse-exporter
pip install -r requirements-dev.txt
ruff check .        # lint
pytest              # unit tests (98)
```

Integration tests run against a real S3-compatible endpoint and are excluded
by default:

```sh
docker compose up -d   # SeaweedFS with a ready-made bucket + scoped user
HA_EXPORTER_TEST_S3_ENDPOINT=http://127.0.0.1:8333 \
HA_EXPORTER_TEST_S3_ACCESS_KEY=ha-exporter \
HA_EXPORTER_TEST_S3_SECRET_KEY=ha-exporter-secret-change-me \
HA_EXPORTER_TEST_S3_BUCKET=ha-lakehouse \
pytest -m integration
```

`scripts/profile_ingest.py` measures ingest memory and throughput; results
and methodology are in [`docs/performance.md`](docs/performance.md).

Maintainer runbooks: [`docs/RELEASE.md`](docs/RELEASE.md) (cutting and
verifying a release), [`docs/dogfooding.md`](docs/dogfooding.md) (validating
against a real Home Assistant), and
[`docs/glue-athena.md`](docs/glue-athena.md) (verifying the Iceberg path
against AWS Glue + Athena).

## Minimum supported version

Home Assistant core **2024.6+**, on a Supervisor-based installation.

## License

[MIT](LICENSE)
