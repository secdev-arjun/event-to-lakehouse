import json
import os
import time
from urllib.parse import unquote

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType,
    IntegerType, ArrayType
)
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_json, struct,
    concat_ws, lower, trim, sha2
)

# ------------------------------------------------------------------------------
# Spark session (your docker spark-submit provides Iceberg + s3a configs)
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("Bronze Assets -> Iceberg Silver assets").getOrCreate()

# Safety defaults: ignore missing/corrupt data files during ingestion
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

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

TARGET_TABLE = "iceberg.silver.assets"

RAPID7_TOPIC = "rapid7.assets.raw"
FORTI_TOPIC = "fortisiem.devices.raw"

def _with_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

SCHEMA_ROOT = _with_trailing_slash(os.getenv("SCHEMA_ROOT", "s3a://warehouse/schemas/"))

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker-1:19092,broker-2:19092,broker-3:19092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "minio.object.events")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
MAX_FILES_PER_BATCH = int(os.getenv("MAX_FILES_PER_BATCH", "200"))
READ_RETRY_COUNT = int(os.getenv("READ_RETRY_COUNT", "3"))
READ_RETRY_SLEEP_SEC = float(os.getenv("READ_RETRY_SLEEP_SEC", "2"))

EVENTS_CKPT = os.getenv(
    "EVENTS_CKPT",
    "s3a://warehouse/checkpoints/silver_assets_events/"
)

# JSON reader hardening:
# - multiLine: true for pretty/indented JSON objects
# - mode: PERMISSIVE keeps going on malformed records
# - columnNameOfCorruptRecord: capture bad JSON into a column (requires schema to include it)
JSON_OPTIONS = {
    "multiLine": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record"
}

# ------------------------------------------------------------------------------
# MinIO event parsing helpers
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
                StructField("size", IntegerType(), True),
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


def normalize_rapid7(df):
    # If JSON is corrupt, many fields will be null and _corrupt_record will be populated.
    # We drop corrupt records from the silver table by filtering them out here.
    rapid7_clean = df.filter(col("_corrupt_record").isNull())

    return (
        rapid7_clean
        .withColumn("source_system", lit("rapid7"))
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("rapid7_id", col("id").cast("string"))
        .withColumn("fortisiem_id", lit(None).cast("string"))

        .withColumn("asset_name", col("hostName"))
        .withColumn("primary_hostname", col("hostName"))

        .withColumn("primary_ip", col("ip"))
        .withColumn("access_ip", lit(None).cast("string"))

        .withColumn("natural_id", lit(None).cast("string"))
        .withColumn("approved", lit(None).cast("boolean"))
        .withColumn("unmanaged", lit(None).cast("boolean"))

        .withColumn("device_vendor", lit(None).cast("string"))
        .withColumn("device_model", lit(None).cast("string"))
        .withColumn("device_version", lit(None).cast("string"))

        .withColumn("os_name", col("os"))
        .withColumn("os_family", col("osFingerprint.family"))
        .withColumn("os_vendor", col("osFingerprint.vendor"))
        .withColumn("os_product", col("osFingerprint.product"))
        .withColumn(
            "os_version",
            F.coalesce(col("osFingerprint.cpe.version"), col("osFingerprint.version"))
        )
        .withColumn("os_architecture", col("osFingerprint.architecture"))
        .withColumn("os_certainty", col("osCertainty").cast("double"))

        .withColumn("assessed_for_policies", col("assessedForPolicies"))
        .withColumn("assessed_for_vulnerabilities", col("assessedForVulnerabilities"))
        .withColumn("risk_score", col("riskScore").cast("double"))
        .withColumn("raw_risk_score", col("rawRiskScore").cast("double"))

        .withColumn("vuln_total", col("vulnerabilities.total").cast("int"))
        .withColumn("vuln_critical", col("vulnerabilities.critical").cast("int"))
        .withColumn("vuln_severe", col("vulnerabilities.severe").cast("int"))
        .withColumn("vuln_moderate", col("vulnerabilities.moderate").cast("int"))
        .withColumn("vuln_exploits", col("vulnerabilities.exploits").cast("int"))
        .withColumn("vuln_malware_kits", col("vulnerabilities.malwareKits").cast("int"))

        # Store original JSON (from the cleaned DF columns, excluding _corrupt_record)
        .withColumn(
            "raw_json",
            to_json(struct([col(c) for c in rapid7_clean.columns if c != "_corrupt_record"]))
        )

        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("primary_ip"))),
                    col("rapid7_id")
                ),
                256
            )
        )
        .select(
            "asset_uid", "source_system", "ingest_ts",
            "rapid7_id", "fortisiem_id",
            "asset_name", "primary_hostname",
            "primary_ip", "access_ip",
            "natural_id", "approved", "unmanaged",
            "device_vendor", "device_model", "device_version",
            "os_name", "os_family", "os_vendor", "os_product", "os_version", "os_architecture", "os_certainty",
            "assessed_for_policies", "assessed_for_vulnerabilities",
            "risk_score", "raw_risk_score",
            "vuln_total", "vuln_critical", "vuln_severe", "vuln_moderate", "vuln_exploits", "vuln_malware_kits",
            "raw_json"
        )
    )


def load_latest_schema(topic_name: str):
    schema_dir = f"{SCHEMA_ROOT}{topic_name}/schema/"
    try:
        rows = spark.read.text(schema_dir).collect()
    except Exception as e:
        print(f"[WARN] {topic_name}: unable to read schema at {schema_dir}: {e}")
        return None

    for r in rows:
        val = r["value"]
        if val and val.strip():
            try:
                return StructType.fromJson(json.loads(val))
            except Exception as e:
                print(f"[WARN] {topic_name}: invalid schema JSON in {schema_dir}: {e}")
                return None

    print(f"[WARN] {topic_name}: no schema content found in {schema_dir}")
    return None

def normalize_fortisiem(df):
    forti_clean = df.filter(col("_corrupt_record").isNull())

    return (
        forti_clean
        .withColumn("source_system", lit("fortisiem"))
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("rapid7_id", lit(None).cast("string"))
        .withColumn("fortisiem_id", col("_id.$oid").cast("string"))

        .withColumn("asset_name", col("name"))
        .withColumn("primary_hostname", col("name"))

        .withColumn("primary_ip", lit(None).cast("string"))
        .withColumn("access_ip", col("accessIp"))

        .withColumn("natural_id", col("naturalId"))
        .withColumn("approved", col("approved"))
        .withColumn("unmanaged", col("unmanaged"))

        .withColumn("device_vendor", col("deviceType.vendor"))
        .withColumn("device_model", col("deviceType.model"))
        .withColumn("device_version", col("deviceType.version"))

        .withColumn("os_name", lit(None).cast("string"))
        .withColumn("os_family", lit(None).cast("string"))
        .withColumn("os_vendor", lit(None).cast("string"))
        .withColumn("os_product", lit(None).cast("string"))
        .withColumn("os_version", lit(None).cast("string"))
        .withColumn("os_architecture", lit(None).cast("string"))
        .withColumn("os_certainty", lit(None).cast("double"))

        .withColumn("assessed_for_policies", lit(None).cast("boolean"))
        .withColumn("assessed_for_vulnerabilities", lit(None).cast("boolean"))
        .withColumn("risk_score", lit(None).cast("double"))
        .withColumn("raw_risk_score", lit(None).cast("double"))

        .withColumn("vuln_total", lit(None).cast("int"))
        .withColumn("vuln_critical", lit(None).cast("int"))
        .withColumn("vuln_severe", lit(None).cast("int"))
        .withColumn("vuln_moderate", lit(None).cast("int"))
        .withColumn("vuln_exploits", lit(None).cast("int"))
        .withColumn("vuln_malware_kits", lit(None).cast("int"))

        .withColumn(
            "raw_json",
            to_json(struct([col(c) for c in forti_clean.columns if c != "_corrupt_record"]))
        )

        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("access_ip"))),
                    col("fortisiem_id")
                ),
                256
            )
        )
        .select(
            "asset_uid", "source_system", "ingest_ts",
            "rapid7_id", "fortisiem_id",
            "asset_name", "primary_hostname",
            "primary_ip", "access_ip",
            "natural_id", "approved", "unmanaged",
            "device_vendor", "device_model", "device_version",
            "os_name", "os_family", "os_vendor", "os_product", "os_version", "os_architecture", "os_certainty",
            "assessed_for_policies", "assessed_for_vulnerabilities",
            "risk_score", "raw_risk_score",
            "vuln_total", "vuln_critical", "vuln_severe", "vuln_moderate", "vuln_exploits", "vuln_malware_kits",
            "raw_json"
        )
    )


# ------------------------------------------------------------------------------
# Streaming from MinIO object events (Kafka) to avoid ghost prefixes
# ------------------------------------------------------------------------------

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    parsed = (
        batch_df.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("event", F.from_json(F.col("json_str"), event_schema))
        .filter(F.col("event").isNotNull())
        .withColumn("record", F.explode_outer(F.col("event.Records")))
        .select(
            F.coalesce(F.col("record.s3.bucket.name"), F.lit("bronze")).alias("bucket"),
            F.coalesce(F.col("record.s3.object.key"), F.col("event.Key")).alias("object_key_raw"),
            F.col("record.eventTime").alias("event_time_str"),
            F.col("record.eventName").alias("event_name"),
            F.col("record.s3.object.size").alias("file_size"),
        )
    )

    if parsed.rdd.isEmpty():
        return

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
        .withColumn("topic_name", F.regexp_extract(F.col("decoded_key"), r"^topics/([^/]+)/", 1))
        .filter(F.col("topic_name").isin([RAPID7_TOPIC, FORTI_TOPIC]))
        .withColumn("file_path", F.concat(F.lit("s3a://"), F.col("bucket"), F.lit("/"), F.col("decoded_key")))
        .withColumn("event_time", F.to_timestamp(F.col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        .withColumn("ingest_ts", F.current_timestamp())
        .drop("event_time_str")
        .dropDuplicates(["file_path"])
    )

    if decoded.rdd.isEmpty():
        return

    if MAX_FILES_PER_BATCH > 0:
        decoded = decoded.orderBy(
            F.coalesce(F.col("event_time"), F.col("ingest_ts")).desc(),
            F.col("ingest_ts").desc()
        ).limit(MAX_FILES_PER_BATCH)

    rows = decoded.select("topic_name", "file_path").collect()
    topic_paths = {RAPID7_TOPIC: [], FORTI_TOPIC: []}
    for r in rows:
        topic_paths[r["topic_name"]].append(r["file_path"])

    # Rapid7
    rapid7_paths = list(dict.fromkeys(topic_paths[RAPID7_TOPIC]))
    if rapid7_paths:
        existing, _missing = filter_existing_paths(
            rapid7_paths, READ_RETRY_COUNT, READ_RETRY_SLEEP_SEC
        )
        if existing:
            rapid7_schema = load_latest_schema(RAPID7_TOPIC)
            if rapid7_schema is None:
                raise RuntimeError(f"No inferred schema found for {RAPID7_TOPIC}")
            df = spark.read.schema(rapid7_schema).options(**JSON_OPTIONS).json(existing)
            out = normalize_rapid7(df)
            if not out.rdd.isEmpty():
                out.writeTo(TARGET_TABLE).append()

    # FortiSIEM
    forti_paths = list(dict.fromkeys(topic_paths[FORTI_TOPIC]))
    if forti_paths:
        existing, _missing = filter_existing_paths(
            forti_paths, READ_RETRY_COUNT, READ_RETRY_SLEEP_SEC
        )
        if existing:
            forti_schema = load_latest_schema(FORTI_TOPIC)
            if forti_schema is None:
                raise RuntimeError(f"No inferred schema found for {FORTI_TOPIC}")
            df = spark.read.schema(forti_schema).options(**JSON_OPTIONS).json(existing)
            out = normalize_fortisiem(df)
            if not out.rdd.isEmpty():
                out.writeTo(TARGET_TABLE).append()


events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .load()
)

query = (
    events.writeStream
    .outputMode("append")
    .option("checkpointLocation", EVENTS_CKPT)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .foreachBatch(process_batch)
    .start()
)

# Keep the container running (returns/throws if any query terminates)
spark.streams.awaitAnyTermination()
