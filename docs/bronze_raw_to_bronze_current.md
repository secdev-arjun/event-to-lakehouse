# Bronze Raw → Bronze Current

This document explains the **Bronze Raw → Bronze Current** job in `scripts/bronze/bronze_raw_to_bronze_current.py`.

---

**What This Job Does (One‑Line)**

It reads raw Bronze tables, builds a deterministic `entity_id` per row, and **MERGEs** into source‑specific **bronze_current** tables so each entity has only its latest row.

---

**Inputs (Raw Bronze Tables)**

- `iceberg.bronze.rapid7__assets__raw`
- `iceberg.bronze.fortisiem__device__raw`
- `iceberg.bronze.sentinalone__agents__raw`

---

**Outputs (Bronze Current Tables)**

- `iceberg.bronze_current.rapid7__assets__current`
- `iceberg.bronze_current.fortisiem__device__current`
- `iceberg.bronze_current.sentinalone__agents__current`

Checkpoint table:
- `iceberg.bronze_current.bronze_current_checkpoint`

---

**Identity Rules (entity_id)**

`entity_id` is a **human‑readable** identifier built from source‑specific fields.

Defaults (in‑script config):
- `rapid7__assets` → `id`
- `sentinalone__agents` → `id`
- `fortisiem__device` → `naturalId`

Construction rules:
1. Use configured field order.
2. Cast to string and trim whitespace.
3. Replace nulls with `<null>`.
4. Escape the delimiter `__` inside values by converting it to `\__`.
5. Join fields using the delimiter `__`.

Examples:
- `fields = ["id"]`, `id = 408` → `entity_id = "408"`
- `fields = ["site_id", "id"]`, values `("us-east", 408)` → `entity_id = "us-east__408"`

---

**Source Metadata**

The job also writes a `source` column into bronze_current so downstream layers can identify the source family:
- `rapid7__assets`
- `fortisiem__device`
- `sentinalone__agents`

---

**Ingest Timestamp Handling**

The job builds an internal `_ingest_ts` column used for ordering and merge decisions.

Parsing behavior:
1. If `ingest_ts` exists, the job attempts to parse epoch seconds, millis, micros, or nanos, then falls back to ISO‑8601 or other timestamp strings.
2. If `ingest_ts` does not exist, the job tries fallback fields in order: `event_time`, `source_updated_at`, `updated_at`, `timestamp`.
3. If none exist, the job uses `current_timestamp()` as `_ingest_ts`.

---

**Merge Behavior (Current Table)**

Keyed on `entity_id`.

Rules:
1. New `entity_id` → INSERT
2. Existing `entity_id` → UPDATE only if incoming `_ingest_ts` is newer or equal

Merge uses Iceberg‑friendly `MERGE INTO`.

---

**Checkpoint Logic (Incremental Processing)**

If `USE_INGEST_TS=true` and the checkpoint table exists:
- The job only reads rows where `_ingest_ts` is newer than the last checkpoint (with a configurable lookback window).
- After a successful merge, it updates the checkpoint for that source.

If required columns (`entity_id`, `source`, `_ingest_ts`) are missing in a current table, the job forces a **full rebuild** for that source and logs a warning.

---

**Key Environment Variables**

Inputs:
- `RAPID7_BRONZE_TABLE`
- `FORTI_BRONZE_TABLE`
- `SENTINEL_BRONZE_TABLE`

Outputs:
- `RAPID7_CURRENT_TABLE`
- `FORTI_CURRENT_TABLE`
- `SENTINEL_CURRENT_TABLE`

Checkpoint:
- `BRONZE_CURRENT_CHECKPOINT_TABLE`
- `CHECKPOINT_LOOKBACK_MINUTES`

Behavior:
- `USE_INGEST_TS`

---

**How to Run (Docker)**

```bash
docker compose up -d bronze-current
```

Check logs:

```bash
docker compose logs -f bronze-current
```

---

**How to Verify**

```sql
SELECT entity_id, source, ingest_ts
FROM bronze_current.rapid7__assets__current
LIMIT 5;
```

```sql
SELECT count(*)
FROM bronze_current.rapid7__assets__current
WHERE entity_id = '10001';
```

You should see exactly **one row per entity_id** in each bronze_current table.

---

**Common Issues**

- Missing configured `entity_id` fields in raw tables.
- Invalid timestamp values in `ingest_ts` causing parse failures.
- Checkpoint table missing or mismatched schema.
