# Dogfooding: validating against a real Home Assistant

Pre-release (and post-release) validation on a real Home Assistant instance.
Unit tests cannot cover real WebSocket auth, real `/api/history` response
shapes, the registry commands, or a large backfill — this runbook can.

## What this validates

- [ ] WebSocket auth on HA core **2024.6+** — `SUPERVISOR_TOKEN` in an add-on,
      or a long-lived access token via `HA_TOKEN` outside one.
- [ ] Real REST history shapes from `GET /api/history/period/<ts>` over a
      realistic **30-day backfill**.
- [ ] Registry commands `config/entity_registry/list`,
      `config/device_registry/list`, `config/area/list` populate the
      `ha_registry_*` tables.
- [ ] End to end: stream → spool → flush → objects in the bucket.

## Install before the first release

The Add-on store cannot install a local checkout on HA OS: the store fetches
git repositories, and `config.yaml` declares a prebuilt `image:`, so nothing is
built locally inside HA. Two honest options:

**(a) Run the image with Docker next to Home Assistant.** Build it on any
machine with Docker
(`docker build -t ha-lakehouse-exporter home-assistant-s3-lakehouse-exporter`),
prepare `options.json` — the JSON form of the add-on options (omit keys to take
their defaults) — and put it in a directory to mount at `/data`:

```json
{
  "s3": {
    "bucket": "ha-lakehouse",
    "prefix": "home-assistant",
    "region": "eu-central-1",
    "endpoint_url": "http://<host>:8333",
    "path_style": true
  },
  "auth": {
    "access_key_id": "ha-exporter",
    "secret_access_key": "ha-exporter-secret-change-me"
  },
  "capture": {
    "backfill_days": 7,
    "attributes": "minimal",
    "attribute_allowlist": ["unit_of_measurement", "device_class", "friendly_name"],
    "max_batch_rows": 20000
  },
  "format": { "type": "parquet" },
  "runtime": { "spool_max_mb": 64 }
}
```

```sh
docker run -d --name ha-exporter \
  -p 8099:8099 \
  -e HA_HTTP_URL=http://<ha-host>:8123 \
  -e HA_WS_URL=ws://<ha-host>:8123/api/websocket \
  -e HA_TOKEN=<long-lived-access-token> \
  -e OPTIONS_PATH=/data/options.json \
  -e SPOOL_PATH=/data/spool.db \
  -v "$PWD/run:/data" \
  ha-lakehouse-exporter
```

- `HA_HTTP_URL` is a **base URL**; the exporter appends
  `/api/history/period/<ts>` itself. Use `http://<ha-host>:8123`, **not**
  `.../api`. `HA_WS_URL` is the full WebSocket endpoint
  (`ws://<ha-host>:8123/api/websocket`).
- Create the token under **Profile → Security → Long-lived access tokens**.
  Inside a real add-on, `SUPERVISOR_TOKEN` provides this instead.
- Keep the mounted `/data` across restarts: the spool (`/data/spool.db`,
  `SPOOL_PATH`) must survive for the crash drill.

**(b) After the first GHCR release** — install normally from the add-on store
(README → Install) and validate upgrades the same way.

### Storage target for the first run

Use the repo's SeaweedFS stack as the bucket — `docker compose up -d` starts an
S3 API on `http://<host>:8333` with bucket `ha-lakehouse` and the `ha-exporter`
identity (`ha-exporter-secret-change-me` — change it). Matching config snippet
(from the exporter container's point of view):

```yaml
s3:
  bucket: "ha-lakehouse"
  endpoint_url: "http://<host>:8333"
  path_style: true
format:
  type: "parquet"   # catalog-free; simplest first run
```

## Staged rollout config

Start conservative; raise only after the first green run:

- [ ] `capture.backfill_days: 7` — raise to the default `30` once green.
- [ ] `capture.attributes: minimal` with allowlist `unit_of_measurement`,
      `device_class`, `friendly_name`.
- [ ] `capture.max_batch_rows: 20000`.
- [ ] `runtime.spool_max_mb: 64`.

## What good looks like

- [ ] Log, in order: `Home Assistant S3 Lakehouse Exporter X.Y.Z starting`,
      `backfilling history from … to …`, `backfill spooled N rows`, then
      recurring `flushed N rows in M object(s)`. With
      `capture.registries: true`, also `registry snapshot spooled N rows`.
- [ ] `/health` (port 8099) shows `"status": "ok"`,
      `"stream_connected": true`, `spool.pending_rows` oscillating near 0,
      `last_flush` recent, `last_error: null`.
- [ ] Bucket contents match
      `{prefix}/{table}/year=YYYY/month=MM/day=DD/part-<uuid>.<ext>`:

```sh
aws s3 ls s3://ha-lakehouse/home-assistant/ --recursive --endpoint-url http://<host>:8333
```

- [ ] One query returns sane rows — run the README's DuckDB example (point
      `s3_endpoint` at your endpoint).

## Failure drills

- [ ] **Storage outage** — stop or block the storage endpoint: `spool.pending_rows`
      and `spool.bytes` grow in `/health`, `status` becomes `degraded`, and the
      log warns through the pressure stages (attributes dropped to minimal →
      flush interval lengthened → oldest batches dropped). Restart storage: the
      spool drains, `flushed_rows` climbs.
- [ ] **Kill mid-write** — `docker kill` (or restart the add-on) during a
      flush: after restart the spool recovers and no rows are lost. Delivery is
      at-least-once — expect duplicates and dedupe downstream (e.g. on
      `(entity_id, last_updated)`).
- [ ] **Status transitions + webhook** — watch `/health` move
      `ok` → `degraded` → `ok`. With `runtime.notify_webhook` set, confirm one
      POST per transition (payload: `event: status_change`, `old_status`,
      `new_status`) and none per event.

## Privacy spot-check

- [ ] Before ever switching to `capture.attributes: all`, inspect exported
      attributes for sensitive values (locations, notification texts, person
      names, power readings) — query the `attributes` JSON strings with DuckDB
      and grep.
- [ ] Remember `attribute_allowlist` filters `state_changes.attributes` only;
      `events.data` is exported whole in v1.

## Sign-off

- [ ] All "what good looks like" checks pass on HA core 2024.6+.
- [ ] 30-day backfill completed on the staged config.
- [ ] Failure drills passed; no rows lost.
- [ ] Privacy spot-check done for the chosen `capture.attributes` mode.
- [ ] Version + CHANGELOG ready — proceed to `docs/RELEASE.md`.
