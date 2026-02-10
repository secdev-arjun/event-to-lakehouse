import json
import os
import time
import hashlib
from datetime import datetime, timezone
from urllib.parse import unquote

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, LongType, TimestampType
)
from pyspark.sql.window import Window

# ------------------------------------------------------------------------------
# Spark session (docker spark-submit provides Iceberg + s3a configs)
# ------------------------------------------------------------------------------

spark = SparkSession.builder.appName("infer-schemas-from-minio-events").getOrCreate()

# ------------------------------------------------------------------------------
# Hadoop FS helpers (MinIO/S3A)
# ------------------------------------------------------------------------------

jvm = spark._jvm
hconf = spark._jsc.hadoopConfiguration()
Path = jvm.org.apache.hadoop.fs.Path


def _fs_for(path: str):
    # Use the filesystem that matches the scheme of the path (avoids Wrong FS errors).
    return Path(path).getFileSystem(hconf)


def _exists(path: str) -> bool:
    try:
        return _fs_for(path).exists(Path(path))
    except Exception:
        return False

# ------------------------------------------------------------------------------
# Config (override via env vars)
# ------------------------------------------------------------------------------

def _with_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

SCHEMA_ROOT = _with_trailing_slash(os.getenv("SCHEMA_ROOT", "s3a://warehouse/schemas/"))
CHECKPOINT_ROOT = _with_trailing_slash(
    os.getenv("CHECKPOINT_ROOT", "s3a://warehouse/checkpoints/schema_inferer/")
)

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker-1:19092,broker-2:19092,broker-3:19092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "minio.object.events")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "1000"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

DEFAULT_BUCKET = os.getenv("DEFAULT_BUCKET", "bronze")

MAX_FILES_FOR_INFERENCE = int(os.getenv("MAX_FILES_FOR_INFERENCE", "50"))
SAMPLING_RATIO = float(os.getenv("SAMPLING_RATIO", "0.2"))
COUNT_SAMPLE_RECORDS = os.getenv("COUNT_SAMPLE_RECORDS", "false").lower() == "true"
DROP_ALL_NULL_FIELDS = os.getenv("DROP_ALL_NULL_FIELDS", "false").lower() == "true"
MAX_SAMPLE_BYTES = int(os.getenv("MAX_SAMPLE_BYTES", "0"))  # 0 = no cap
MAX_SAMPLE_FILE_BYTES = int(os.getenv("MAX_SAMPLE_FILE_BYTES", "0"))  # 0 = no cap

READ_RETRY_COUNT = int(os.getenv("READ_RETRY_COUNT", "3"))
READ_RETRY_SLEEP_SEC = float(os.getenv("READ_RETRY_SLEEP_SEC", "2"))

RECENT_FILES_TABLE = os.getenv(
    "RECENT_FILES_TABLE", "iceberg.schema_registry.recent_files"
)

CORRUPT_RECORD_COL = os.getenv("CORRUPT_RECORD_COL", "_corrupt_record")
JSON_READ_OPTS = {
    "multiLine": os.getenv("JSON_MULTILINE", "true"),
    "mode": os.getenv("JSON_MODE", "PERMISSIVE"),
    "columnNameOfCorruptRecord": CORRUPT_RECORD_COL,
}
if DROP_ALL_NULL_FIELDS:
    JSON_READ_OPTS["dropFieldIfAllNull"] = "true"

# ------------------------------------------------------------------------------
# State helpers (schema + metadata)
# ------------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_schema(schema_json: str) -> str:
    return hashlib.sha256(schema_json.encode("utf-8")).hexdigest()


def read_state(topic_name: str) -> dict:
    state_dir = f"{SCHEMA_ROOT}{topic_name}/_state/"
    try:
        rows = spark.read.text(state_dir).collect()
    except Exception:
        return {}
    for r in rows:
        val = r["value"]
        if not val or not val.strip():
            continue
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            continue
    return {}


def write_state(topic_name: str, state: dict):
    state_dir = f"{SCHEMA_ROOT}{topic_name}/_state/"
    payload = json.dumps(state, sort_keys=True)
    (
        spark.createDataFrame([(payload,)], ["value"])
        .coalesce(1)
        .write.mode("overwrite")
        .text(state_dir)
    )


def write_schema(topic_name: str, schema_json: str):
    # Write schema as a folder (not a single renamed file) to avoid S3A rename/copy issues.
    schema_dir = f"{SCHEMA_ROOT}{topic_name}/schema/"
    (
        spark.createDataFrame([(schema_json,)], ["value"])
        .coalesce(1)
        .write.mode("overwrite")
        .text(schema_dir)
    )

# ------------------------------------------------------------------------------
# Recent files state (Iceberg table)
# ------------------------------------------------------------------------------

RECENT_SCHEMA = StructType([
    StructField("topic_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("event_time", TimestampType(), True),
    StructField("ingest_ts", TimestampType(), False),
    StructField("bucket", StringType(), True),
    StructField("object_key", StringType(), True),
    StructField("file_size", LongType(), True),
])


def ensure_recent_table():
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.schema_registry")
    if not spark.catalog.tableExists(RECENT_FILES_TABLE):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {RECENT_FILES_TABLE} (
                topic_name STRING,
                file_path STRING,
                event_time TIMESTAMP,
                ingest_ts TIMESTAMP,
                bucket STRING,
                object_key STRING,
                file_size BIGINT
            )
            USING iceberg
            PARTITIONED BY (topic_name)
        """)

# ------------------------------------------------------------------------------
# Event parsing helpers
# ------------------------------------------------------------------------------

event_schema = StructType([
    StructField("EventName", StringType(), True),
    StructField("Key", StringType(), True),
    StructField("Records", ArrayType(StructType([
        StructField("eventName", StringType(), True),
        StructField("eventTime", StringType(), True),
        StructField("s3", StructType([
            StructField("bucket", StructType([
                StructField("name", StringType(), True),
            ]), True),
            StructField("object", StructType([
                StructField("key", StringType(), True),
                StructField("size", LongType(), True),
            ]), True),
        ]), True),
    ])), True),
])


def _decode_key(raw_key: str, bucket: str):
    if not raw_key:
        return None
    decoded = unquote(raw_key).lstrip("/")
    if bucket and decoded.startswith(bucket + "/"):
        decoded = decoded[len(bucket) + 1:]
    # If the key still contains a nested bucket prefix, normalize to start at topics/
    if "topics/" in decoded and not decoded.startswith("topics/"):
        decoded = decoded[decoded.find("topics/"):]
    return decoded


decode_key_udf = F.udf(_decode_key, StringType())

# ------------------------------------------------------------------------------
# Inference
# ------------------------------------------------------------------------------

def infer_schema(sample_files):
    reader = spark.read.options(**JSON_READ_OPTS)
    if SAMPLING_RATIO < 1.0:
        reader = reader.option("samplingRatio", SAMPLING_RATIO)

    last_err = None
    for attempt in range(READ_RETRY_COUNT):
        try:
            df_raw = reader.json(sample_files)
            if CORRUPT_RECORD_COL in df_raw.columns:
                df = df_raw.drop(CORRUPT_RECORD_COL)
            else:
                df = df_raw

            if not df.schema.fields:
                return None, {"sample_record_count": 0, "sample_column_count": 0}

            record_count = None
            if COUNT_SAMPLE_RECORDS:
                record_count = df.count()

            meta = {
                "sample_record_count": record_count,
                "sample_column_count": len(df.schema.fields),
            }
            return df.schema.json(), meta
        except Exception as e:
            last_err = e
            if attempt < READ_RETRY_COUNT - 1:
                time.sleep(READ_RETRY_SLEEP_SEC)
                continue
            raise last_err


def apply_sample_limits(paths_with_size):
    """
    Apply optional size caps to sample paths.
    paths_with_size: list of (path, size)
    """
    if MAX_SAMPLE_BYTES <= 0 and MAX_SAMPLE_FILE_BYTES <= 0:
        total = sum(sz or 0 for _, sz in paths_with_size)
        return paths_with_size, total, False

    bytes_total = 0
    out = []
    truncated = False
    for p, sz in paths_with_size:
        if MAX_SAMPLE_FILE_BYTES > 0 and sz and sz > MAX_SAMPLE_FILE_BYTES:
            truncated = True
            continue
        if MAX_SAMPLE_BYTES > 0 and bytes_total + (sz or 0) > MAX_SAMPLE_BYTES:
            truncated = True
            break
        out.append((p, sz))
        bytes_total += (sz or 0)
    return out, bytes_total, truncated


def filter_existing_paths(paths, retries: int, sleep_sec: float):
    """
    Filter out paths that are not yet visible in object storage.
    Returns (existing_paths, missing_paths).
    """
    remaining = list(paths)
    missing = []

    for attempt in range(max(1, retries)):
        missing = [p for p in remaining if not _exists(p)]
        if not missing:
            return remaining, []
        if attempt < retries - 1:
            time.sleep(sleep_sec)
            remaining = [p for p in remaining if p not in missing]
            # Re-check only the missing paths next attempt
            remaining = remaining + missing

    existing = [p for p in paths if p not in missing]
    return existing, missing

# ------------------------------------------------------------------------------
# foreachBatch logic
# ------------------------------------------------------------------------------

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    ensure_recent_table()

    parsed = (
        batch_df.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("event", F.from_json(F.col("json_str"), event_schema))
        .filter(F.col("event").isNotNull())
        .withColumn("record", F.explode_outer(F.col("event.Records")))
        .select(
            F.coalesce(F.col("record.s3.bucket.name"), F.lit(DEFAULT_BUCKET)).alias("bucket"),
            F.coalesce(F.col("record.s3.object.key"), F.col("event.Key")).alias("object_key_raw"),
            F.col("record.eventTime").alias("event_time_str"),
            F.col("record.eventName").alias("event_name"),
            F.col("record.s3.object.size").alias("file_size"),
        )
    )

    if parsed.rdd.isEmpty():
        print(f"[BATCH {batch_id}] No valid JSON events")
        return

    # Decode URL-encoded key and build s3a path
    decoded = (
        parsed.withColumn(
            "decoded_key",
            decode_key_udf(F.col("object_key_raw"), F.col("bucket"))
        )
        .filter(F.col("decoded_key").isNotNull())
        .filter(F.col("decoded_key").startswith("topics/"))
        .filter(
            (F.col("event_name").isNull()) |
            (F.col("event_name").startswith("s3:ObjectCreated"))
        )
        # Extract topic name reliably from topics/<topic_name>/...
        .withColumn("topic_name", F.regexp_extract(F.col("decoded_key"), r"^topics/([^/]+)/", 1))
        .filter(F.col("topic_name") != "")
        .withColumn("file_path", F.concat(F.lit("s3a://"), F.col("bucket"), F.lit("/"), F.col("decoded_key")))
        .withColumn("event_time", F.to_timestamp(F.col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        .withColumn("ingest_ts", F.current_timestamp())
        .drop("event_time_str")
        .dropDuplicates(["file_path"])
    )

    if decoded.rdd.isEmpty():
        print(f"[BATCH {batch_id}] No usable object events after filtering")
        return

    topics = [r["topic_name"] for r in decoded.select("topic_name").distinct().collect()]
    if not topics:
        return

    # Build rolling recent list by topic (latest N) and overwrite those partitions
    existing = spark.table(RECENT_FILES_TABLE).filter(F.col("topic_name").isin(topics))
    combined = existing.unionByName(
        decoded.select(
            "topic_name", "file_path", "event_time", "ingest_ts",
            "bucket", "decoded_key", "file_size"
        ).withColumnRenamed("decoded_key", "object_key")
    ).dropDuplicates(["file_path"])

    sort_ts = F.coalesce(F.col("event_time"), F.col("ingest_ts"))
    w = Window.partitionBy("topic_name").orderBy(sort_ts.desc(), F.col("ingest_ts").desc())
    recent = combined.withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") <= MAX_FILES_FOR_INFERENCE) \
        .drop("rn")
    # Safety: keep only rows whose file path actually matches the topic folder
    recent = recent.filter(
        F.col("file_path").contains(
            F.concat(F.lit("topics/"), F.col("topic_name"), F.lit("/"))
        )
    )

    # Overwrite only touched topic partitions (rolling window state)
    recent.writeTo(RECENT_FILES_TABLE).overwritePartitions()

    # Infer schema per topic using the recent N files (sorted newest-first)
    for topic_name in topics:
        topic_recent = (
            recent.filter(F.col("topic_name") == topic_name)
            .orderBy(sort_ts.desc(), F.col("ingest_ts").desc())
            .select(
                "topic_name", "file_path", "event_time", "ingest_ts",
                "bucket", "object_key", "file_size"
            )
        )
        rows = topic_recent.select("file_path", "file_size").collect()
        paths_with_size = [(r["file_path"], r["file_size"]) for r in rows]

        # Apply optional size caps before inference
        filtered, sample_bytes, sample_truncated = apply_sample_limits(paths_with_size)
        sample_files = [p for p, _ in filtered]

        if not sample_files:
            print(f"[SKIP] {topic_name}: no usable files after size limits")
            continue

        # Drop paths that are not yet visible (event arrived before object is readable)
        existing_paths, missing_paths = filter_existing_paths(
            sample_files, READ_RETRY_COUNT, READ_RETRY_SLEEP_SEC
        )
        if missing_paths:
            print(
                f"[WARN] {topic_name}: {len(missing_paths)} paths not found yet; "
                "skipping those for now"
            )
            # Clean the rolling state for this topic to remove missing paths
            topic_recent.filter(F.col("file_path").isin(existing_paths)) \
                .writeTo(RECENT_FILES_TABLE).overwritePartitions()

        sample_files = existing_paths
        if not sample_files:
            print(f"[SKIP] {topic_name}: no readable files available yet")
            continue

        state = read_state(topic_name)
        try:
            schema_json, meta = infer_schema(sample_files)
            if not schema_json:
                raise RuntimeError("empty schema (no readable records)")

            schema_hash = _hash_schema(schema_json)
            prev_hash = state.get("schema_hash")
            schema_changed = prev_hash is None or prev_hash != schema_hash

            if schema_changed:
                write_schema(topic_name, schema_json)

            new_state = {
                "topic": topic_name,
                "sample_files": sample_files,
                "sample_file_count": len(sample_files),
                "sample_bytes": sample_bytes,
                "sample_truncated": sample_truncated,
                "schema_hash": schema_hash,
                "previous_schema_hash": prev_hash,
                "schema_changed": schema_changed,
                "last_success_ts": _now_iso(),
                "last_attempt_ts": _now_iso(),
                "failure_reason": None,
            }
            new_state.update(meta)
            write_state(topic_name, new_state)

            if schema_changed:
                print(f"[OK] {topic_name}: schema updated from {len(sample_files)} files")
            else:
                print(f"[OK] {topic_name}: schema unchanged; state refreshed")

        except Exception as e:
            failure_state = {
                "topic": topic_name,
                "sample_files": sample_files,
                "schema_hash": state.get("schema_hash"),
                "last_success_ts": state.get("last_success_ts"),
                "last_attempt_ts": _now_iso(),
                "failure_reason": str(e),
            }
            try:
                write_state(topic_name, failure_state)
            except Exception:
                pass
            print(f"[FAIL] {topic_name}: {e}")


if __name__ == "__main__":
    kafka_opts = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "subscribe": KAFKA_TOPIC,
        "startingOffsets": STARTING_OFFSETS,
        "failOnDataLoss": "false",
    }
    if MAX_OFFSETS_PER_TRIGGER > 0:
        kafka_opts["maxOffsetsPerTrigger"] = str(MAX_OFFSETS_PER_TRIGGER)

    stream_df = (
        spark.readStream.format("kafka")
        .options(**kafka_opts)
        .load()
    )

    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_ROOT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()
