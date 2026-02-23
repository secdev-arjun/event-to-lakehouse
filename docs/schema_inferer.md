# Schema Inferer 

This document explains **`scripts/schema_inferer.py`** in very simple words, with enough detail for engineers to review or troubleshoot.

---

**Big Idea**

Imagine each Kafka topic is a shelf of books. Each book is a JSON file.

The schema inferer job:
- Looks at the newest books on each shelf
- Figures out the “shape” of the story inside
- Saves that shape so other jobs can read files safely

If the shape changes, it updates the saved schema.

---

**One‑Line Summary**

This job listens to MinIO object notifications from Kafka, keeps a rolling list of the newest files per topic, infers a schema from those files, and stores the schema and metadata in S3.

---

**Where It Reads From**

- Kafka topic: `minio.object.events`
- MinIO object paths: `s3a://bronze/topics/<topic>/...`

---

**Where It Writes To**

- Schema JSON files:
  `s3a://warehouse/schemas/<topic>/schema/`

- State metadata:
  `s3a://warehouse/schemas/<topic>/_state/`

- Rolling “recent files” table:
  `iceberg.schema_registry.recent_files`

---

**High‑Level Flow**

1. Read Kafka events for object creation.
2. Decode the file path from the event.
3. Keep only files under `topics/<topic>/`.
4. Store newest files in an Iceberg table (rolling window).
5. For each topic, pick the latest N files.
6. Infer schema using Spark JSON reader.
7. Write schema only if it changed.
8. Always write a state file with metadata.

---

**Why We Keep a Rolling Window**

We do **not** scan the whole topic folder every time. That would be slow.

Instead, we keep the **latest N files** in an Iceberg table. This gives us:
- Fast lookup
- A stable sample for schema inference
- Easy debugging

---

**Key Code Snippets (With Explanation)**

**1) Decode the object key**

```python
def _decode_key(raw_key: str, bucket: str):
    decoded = unquote(raw_key).lstrip("/")
    if bucket and decoded.startswith(bucket + "/"):
        decoded = decoded[len(bucket) + 1:]
    if "topics/" in decoded and not decoded.startswith("topics/"):
        decoded = decoded[decoded.find("topics/"):]
    return decoded
```

Explanation:
- MinIO events often give URL‑encoded keys
- We decode them and make sure the path starts at `topics/`

---

**2) Parse the Kafka event JSON**

```python
parsed = (
    batch_df.selectExpr("CAST(value AS STRING) AS json_str")
    .withColumn("event", F.from_json(F.col("json_str"), event_schema))
    .withColumn("record", F.explode_outer(F.col("event.Records")))
    .select(
        F.col("record.s3.bucket.name").alias("bucket"),
        F.col("record.s3.object.key").alias("object_key_raw"),
        F.col("record.eventTime").alias("event_time_str")
    )
)
```

Explanation:
- Kafka gives raw JSON strings
- We parse them into a struct
- We pull out bucket, key, and event time

---

**3) Build S3A file paths and keep only topic files**

```python
decoded = (
    parsed
    .withColumn("decoded_key", decode_key_udf(F.col("object_key_raw"), F.col("bucket")))
    .filter(F.col("decoded_key").startswith("topics/"))
    .withColumn("topic_name", F.regexp_extract(F.col("decoded_key"), r"^topics/([^/]+)/", 1))
    .withColumn("file_path", F.concat(F.lit("s3a://"), F.col("bucket"), F.lit("/"), F.col("decoded_key")))
)
```

Explanation:
- We only care about files inside `topics/<topic>/...`
- We extract the topic name
- We build the final `s3a://` path

---

**4) Keep the latest N files per topic**

```python
w = Window.partitionBy("topic_name").orderBy(sort_ts.desc(), F.col("ingest_ts").desc())
recent = combined.withColumn("rn", F.row_number().over(w)) \
    .filter(F.col("rn") <= MAX_FILES_FOR_INFERENCE) \
    .drop("rn")
```

Explanation:
- We keep only the newest N files
- This is our rolling sample set

---

**5) Infer schema from the sample files**

```python
reader = spark.read.options(**JSON_READ_OPTS)
if SAMPLING_RATIO < 1.0:
    reader = reader.option("samplingRatio", SAMPLING_RATIO)

schema_json = reader.json(sample_files).schema.json()
```

Explanation:
- Spark reads the JSON files
- It infers a schema automatically
- We save that schema in JSON format

---

**6) Only write schema if it changed**

```python
schema_hash = _hash_schema(schema_json)
if prev_hash != schema_hash:
    write_schema(topic_name, schema_json)
```

Explanation:
- We hash the schema
- If it’s the same, we skip writing

---

**What the State File Contains**

The `_state/` file is **metadata**, not schema.

Common fields:
- `sample_files`
- `schema_hash`
- `schema_changed`
- `last_success_ts`
- `last_attempt_ts`
- `failure_reason`

This helps with debugging and avoids re‑work.

---

**Configuration (Environment Variables)**

You can override these in Docker or env:

- `SCHEMA_ROOT` (default `s3a://warehouse/schemas/`)
- `CHECKPOINT_ROOT` (default `s3a://warehouse/checkpoints/schema_inferer/`)
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC` (default `minio.object.events`)
- `STARTING_OFFSETS` (default `latest`)
- `MAX_OFFSETS_PER_TRIGGER` (default `200`)
- `TRIGGER_INTERVAL` (default `30 seconds`)
- `MAX_EVENTS_PER_BATCH` (default `200`)
- `MAX_TOPICS_PER_BATCH` (default `5`)
- `DEFAULT_BUCKET` (default `bronze`)
- `MAX_FILES_FOR_INFERENCE` (default `20`)
- `SAMPLING_RATIO` (default `0.1`)
- `COUNT_SAMPLE_RECORDS` (default `false`)
- `DROP_ALL_NULL_FIELDS` (default `false`)
- `MAX_SAMPLE_BYTES` (default `0`)
- `MAX_SAMPLE_FILE_BYTES` (default `0`)
- `READ_RETRY_COUNT` (default `3`)
- `READ_RETRY_SLEEP_SEC` (default `2`)
- `MIN_SECONDS_BETWEEN_INFER` (default `300`)
- `MIN_NEW_FILES_TO_INFER` (default `3`)
- `MAX_EVENT_AGE_HOURS` (default `0`)
- `RECENT_FILES_TABLE` (default `iceberg.schema_registry.recent_files`)
- `CORRUPT_RECORD_COL` (default `_corrupt_record`)
- `JSON_MULTILINE` (default `true`)
- `JSON_MODE` (default `PERMISSIVE`)

---

**How to Run (Docker)**

```bash
docker compose up -d spark-schema-infer
```

Check logs:

```bash
docker compose logs -f spark-schema-infer
```

---

**How to Verify**

Check schema output:

```sql
SELECT * FROM iceberg.schema_registry.recent_files LIMIT 5;
```

Read a schema file:

```python
spark.read.text("s3a://warehouse/schemas/fortisiem.devices.raw/schema/") \
     .show(truncate=False)
```

Check state metadata:

```python
spark.read.text("s3a://warehouse/schemas/fortisiem.devices.raw/_state/") \
     .show(truncate=False)
```

---

**Common Issues**

- **Schema file looks wrong**: You may be reading `_state/` instead of `schema/`.
- **Missing files**: Object events can arrive before MinIO exposes the file. The job retries.
- **No schema change**: That is expected if the shape didn’t change.

---

**Summary (Short)**

- Kafka events tell us which files arrived
- We keep only the newest N files per topic
- We infer schema and write it only if it changes
- We store metadata for debugging

---

If you want a walkthrough with a real topic example (Rapid7, FortiSIEM, Sentinel), say the word.
