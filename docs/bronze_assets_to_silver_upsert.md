# Bronze to Silver Upsert (Iceberg MERGE)

This document explains the updated `scripts/bronze_assets_to_silver_assets.py` job.

## Overview

The job now:

1. Consumes MinIO object events from Kafka (`minio.object.events`).
2. Reads new JSON files from `s3a://bronze/topics/<topic>/...` using the **latest inferred schema** from `s3a://warehouse/schemas/<topic>/schema/`.
3. Normalizes Rapid7, FortiSIEM, and SentinelOne (typo topic `centinel.agents.raw`) into a common schema.
4. Deduplicates per entity and **MERGEs** into Iceberg to implement upsert behavior:
   - Insert if new entity.
   - Update `last_seen_at` if payload unchanged.
   - Replace row if payload changed.

## Key Pieces

### 1) Load latest inferred schema

```python
schema = load_latest_schema(topic_name)
if schema is None:
    raise RuntimeError(f"No inferred schema found for {topic_name}")
```

The schema is read from:

```
s3a://warehouse/schemas/<topic>/schema/part-*.txt
```

### 2) Composite identity + payload hash

```python
df = (
    df
    .withColumn("vendor_id", vendor_id_col.cast("string"))
    .withColumn("entity_key_str", concat_ws("|", col("topic_name"), col("vendor_id")))
    .withColumn("entity_key_hash", sha2(col("entity_key_str"), 256))
)

# Hash only normalized business fields (exclude volatile timestamps)
df = df.withColumn("payload_hash", sha2(to_json(struct(*ordered_cols)), 256))
```

### 3) Deduplicate per entity

```python
order_col = F.coalesce(col("source_updated_at"), col("event_time"), col("ingest_ts"))
window = Window.partitionBy("entity_key_hash").orderBy(order_col.desc(), col("ingest_ts").desc())

deduped = (
    combined
    .withColumn("_rn", F.row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
```

### 4) Iceberg MERGE

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

## SentinelOne (centinel.agents.raw)

The job treats the typo topic name as the SentinelOne source:

```
centinel.agents.raw
```

Key mapping example:

- `vendor_id` = `uuid` (fallback to `id`)
- `primary_hostname` = `computerName`
- `primary_ip` = `lastIpToMgmt`
- `ip_addresses` = networkInterfaces[*].inet + lastIpToMgmt
- `source_updated_at` = `updatedAt`

## Expected Behavior

- **New entity:** INSERT
- **Same entity, same payload:** UPDATE only `last_seen_at`
- **Same entity, changed payload:** UPDATE row and `last_seen_at`

## Notes

- The table is created automatically if missing.
- Missing columns are added via `ALTER TABLE ... ADD COLUMNS`.
- Row-level MERGE uses Iceberg DML (Spark SQL).
