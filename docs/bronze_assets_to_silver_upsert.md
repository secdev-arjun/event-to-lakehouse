# Bronze → Silver Assets

This document explains the **Bronze to Silver** streaming job in **`scripts/bronze/bronze_assets_to_silver_assets.py`**.

---

**Big Idea**

Imagine three friends send you notes about computers. Each friend writes notes differently.

The Bronze → Silver job does three things:
1. Collect the notes as they arrive (Kafka + MinIO files).
2. Translate them into the **same language** (normalize fields).
3. Keep a **clean table** of the newest truth, plus a **history** of changes.

---

**What This Job Does (One‑Line)**

It reads MinIO object notifications from Kafka, loads the corresponding JSON files from S3A, normalizes them into a shared schema, then **MERGEs** into a Silver current table and a Silver history table.

---

**Inputs**

- Kafka topic: `minio.object.events`
- MinIO files: `s3a://bronze/topics/<topic>/...`
- Latest schema for each topic: `s3a://warehouse/schemas/<topic>/schema/`

---

**Outputs**

- Current Silver table (SCD1):
  `iceberg.silver.assets`

- History Silver table (SCD2):
  `iceberg.silver.assets_history`

---

**Key Helper Modules**

- `scripts/mapping/rapid7.py`
- `scripts/mapping/fortisiem.py`
- `scripts/mapping/sentinel.py`
- `scripts/mapping/target.py`
- `scripts/bronze/noramlizer/kafka_notifications.py`
- `scripts/bronze/noramlizer/minio_reader.py`

These keep the main script readable.

---

**High‑Level Flow**

1. Read Kafka events (MinIO notifications).
2. Decode file paths and filter allowed topics.
3. Load each file using the latest inferred schema.
4. Normalize each source into the shared schema.
5. Union all sources into one DataFrame.
6. Deduplicate by `entity_key_hash`.
7. Merge into **current** table (SCD1 logic).
8. Merge into **history** table (SCD2 logic).

---

**How We Build Identity and Hashes**

The job uses two important hashes:

- `entity_key_hash` = identity of the asset
- `payload_hash` = hash of the normalized business fields

```python
.withColumn("entity_key_str", concat_ws("|", col("topic_name"), col("vendor_id")))
.withColumn("entity_key_hash", sha2(col("entity_key_str"), 256))
```

The `payload_hash` is built from only normalized business fields:

```python
payload_hash = sha2(to_json(struct(*ordered_cols)), 256)
```

This lets us detect:
- Same entity, same payload → only update timestamps
- Same entity, different payload → update row + history

---

**How Normalization Works**

Each source has its own mapping function:

- Rapid7: `normalize_rapid7(df)`
- FortiSIEM: `normalize_fortisiem(df)`
- SentinelOne: `normalize_sentinel(df)`

They all return the same **TARGET_FIELDS** structure.

Example call in the main job:

```python
_read_topic(RAPID7_TOPIC, normalize_rapid7)
_read_topic(FORTI_TOPIC, normalize_fortisiem)
_read_topic(SENTINEL_TOPIC, normalize_sentinel)
```

---

**Deduplication (One Row Per Entity Per Batch)**

We keep only the latest row per `entity_key_hash`:

```python
order_col = F.coalesce(col("source_updated_at"), col("event_time"), col("ingest_ts"))
w = Window.partitionBy("entity_key_hash").orderBy(order_col.desc(), col("ingest_ts").desc())
combined = combined.withColumn("_rn", F.row_number().over(w)) \
    .filter(col("_rn") == 1) \
    .drop("_rn")
```

This prevents multiple updates in the same batch from conflicting.

---

**Current Table Logic (SCD1)**

We MERGE into the current table:

```sql
MERGE INTO iceberg.silver.assets t
USING incoming_updates s
ON t.entity_key_hash = s.entity_key_hash
WHEN MATCHED AND t.payload_hash = s.payload_hash THEN
  UPDATE SET last_seen_at = greatest(t.last_seen_at, s.ingest_ts), ingest_ts = s.ingest_ts
WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN
  UPDATE SET ...
WHEN NOT MATCHED THEN
  INSERT (...)
```

Behavior:
- New entity → INSERT
- Same entity, same payload → only update `last_seen_at`
- Same entity, new payload → update all fields

---

**History Table Logic (SCD2)**

The history table keeps every change.

When payload changes:
1. Close the old row (`valid_to`, `is_current = false`)
2. Insert a new row (`valid_from`, `is_current = true`)

```sql
MERGE INTO iceberg.silver.assets_history h
USING history_changes c
ON h.entity_key_hash = c.entity_key_hash AND h.is_current = true
WHEN MATCHED THEN
  UPDATE SET valid_to = c.valid_from, is_current = false
```

New history rows use a deterministic `version_id`:

```python
version_id = sha2(concat_ws("|", entity_key_hash, payload_hash, valid_from), 256)
```

This makes retries safe and idempotent.

---

**Why We Use Two Tables**

- `iceberg.silver.assets` is fast and clean (latest values only)
- `iceberg.silver.assets_history` keeps all changes for auditing

This is an industry standard pattern.

---

**Configuration (Environment Variables)**

Important ones:

- `SCHEMA_ROOT` (default `s3a://warehouse/schemas/`)
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC` (default `minio.object.events`)
- `STARTING_OFFSETS` (default `latest`)
- `MAX_OFFSETS_PER_TRIGGER` (default `200`)
- `TRIGGER_INTERVAL` (default `30 seconds`)
- `MAX_FILES_PER_BATCH` (default `200`)
- `READ_RETRY_COUNT` (default `3`)
- `READ_RETRY_SLEEP_SEC` (default `2`)
- `EVENTS_CKPT` (default `s3a://warehouse/checkpoints/silver_assets_events/`)

---

**How to Run (Docker)**

```bash
docker compose up -d spark-iceberg
```

Check logs:

```bash
docker compose logs -f spark-iceberg
```

---

**How to Verify**

Check current table:

```sql
SELECT vendor_id, payload_hash, last_seen_at
FROM iceberg.silver.assets
ORDER BY ingest_ts DESC
LIMIT 5;
```

Check history table:

```sql
SELECT vendor_id, payload_hash, valid_from, valid_to, is_current
FROM iceberg.silver.assets_history
ORDER BY valid_from DESC
LIMIT 5;
```

Compare current vs history:

```sql
SELECT h.vendor_id, h.payload_hash AS history_payload, a.payload_hash AS current_payload
FROM iceberg.silver.assets_history h
JOIN iceberg.silver.assets a
  ON h.entity_key_hash = a.entity_key_hash
WHERE h.is_current = true;
```

---

**Common Issues**

- **ModuleNotFoundError: bronze.noramlizer**
  The job needs the `scripts/bronze/noramlizer` package in the container.

- **No inferred schema found**
  The schema inferer has not written a schema for that topic yet.

- **No new data**
  If Kafka has no new object events, the stream stays idle.

---

**Summary (Short)**

- Kafka events tell us which files arrived
- We read the files with the latest schema
- We normalize to one shared format
- We update the current table and keep history

---

If you want a line‑by‑line walkthrough of any single normalizer, ask and I’ll add it.
