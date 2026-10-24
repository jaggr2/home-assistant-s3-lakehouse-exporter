# Verifying the Iceberg path against AWS Glue + Athena

## Scope

Validates `format.type: iceberg` with `format.catalog: glue` against real AWS —
the Athena + Glue lakehouse path. The `parquet` and `jsonl` formats are
catalog-free and need none of this; use the README's DuckDB example for them.

## Least-privilege IAM

Merge the README's S3 policy (`s3:PutObject` + `s3:AbortMultipartUpload` on
`BUCKET/PREFIX/*`, prefix-conditioned `s3:ListBucket`) with Glue access scoped
to one database.

**Namespace**: in v1 the Iceberg namespace is fixed to `default`
(`exporter/writers/iceberg.py`), which maps to the Glue database `default`.
Tables are `default.<table_prefix><kind>`, e.g. `default.ha_state_changes`. If
you want a dedicated Glue database, note that the exporter still writes to
`default` until a configurable namespace ships — scope the policy accordingly.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:CreatePartition",
        "glue:BatchCreatePartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition"
      ],
      "Resource": [
        "arn:aws:glue:<region>:<account>:catalog",
        "arn:aws:glue:<region>:<account>:database/default",
        "arn:aws:glue:<region>:<account>:table/default/*"
      ]
    }
  ]
}
```

- The very first run also needs `glue:CreateDatabase` on
  `arn:aws:glue:<region>:<account>:catalog` (the exporter creates the `default`
  namespace if it does not exist). Drop it afterwards if databases are
  provisioned centrally.
- If the bucket uses a KMS customer-managed key, add `kms:Encrypt` and
  `kms:GenerateDataKey` on that key only (see the README for the base policy).

## Config snippet

```yaml
s3:
  bucket: "my-lakehouse"
  prefix: "home-assistant"
  region: "eu-central-1"
auth:
  access_key_id: "ha-exporter"
  secret_access_key: "********"
format:
  type: "iceberg"
  catalog: "glue"
  warehouse: "s3://my-lakehouse/home-assistant"   # optional
```

- `format.catalog_uri` is **unused** for `glue`.
- `format.warehouse` is optional; when set, use the S3 location the table data
  lives under (`s3://BUCKET/PREFIX`).
- `s3.region` drives **both** the S3 FileIO and the Glue client
  (`glue.region`); static credentials (`auth.*`, optionally
  `auth.session_token`) are used for both.

## Verification

1. Start the add-on. After the first flush the log shows
   `flushed N rows in M object(s)`.
2. Tables appear in Glue:

   ```sh
   aws glue get-tables --database default | grep ha_
   ```

   Expect `ha_state_changes`, `ha_events`, and (with
   `capture.registries: true`) `ha_registry_entities`, `ha_registry_devices`,
   `ha_registry_areas`.
3. Query in the Athena console (engine v3), database `default`:

   ```sql
   SELECT entity_id, state, last_updated
   FROM ha_state_changes
   WHERE last_updated >= current_date - INTERVAL '1' DAY
   LIMIT 10;
   ```

Notes:

- Timestamps are `timestamp with time zone`. `attributes` and `data` are JSON
  **strings** — parse them with `json_extract_scalar`, e.g.
  `json_extract_scalar(attributes, '$.unit_of_measurement')`.
- Tables are unpartitioned in the v1 image (the optional `pyiceberg-core`
  extension is not bundled), but batches are grouped per day and Iceberg file
  statistics keep scans cheap; installing `pyiceberg-core` switches tables to
  day partitioning. File sizes are tuned toward ~128 MB payloads — see
  `docs/performance.md`.
- Always filter on `last_updated` (or `time_fired`) to keep Athena scan costs
  down.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `AccessDenied` on Glue calls | Policy does not cover one of the three resource levels (`catalog`, `database/default`, `table/default/*`) or a needed action. Include `glue:CreateDatabase` on the first run. |
| Tables missing from `aws glue get-tables` | No successful flush yet — check the log for `flushed …` and `last_error` in `/health`. The `default` database is created at startup; tables are created on first write. |
| Region errors / objects in the wrong place | `s3.region` drives both Glue and S3. Keep the bucket and the Glue database in that region. |
| Athena: "table not found" | Wrong database or workgroup — query database `default`; table names carry `format.table_prefix` (default `ha_`). |
| Athena scan cost higher than expected | Missing `last_updated` filter prunes no files; add the date filter from the example above. |
