import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import unquote

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, DoubleType,
    IntegerType, ArrayType, LongType, TimestampType
)
from pyspark.sql.functions import (
    col, lit, to_json, struct,
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

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

TARGET_TABLE = "iceberg.silver.assets"
HISTORY_TABLE = "iceberg.silver.assets_history"
SCHEMA_VERSION = "silver.asset_observation.v1"

RAPID7_TOPIC = "rapid7.assets.raw"
FORTI_TOPIC = "fortisiem.devices.raw"
SENTINEL_TOPIC = "centinel.agents.raw"


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

# ------------------------------------------------------------------------------
# Schema loader
# ------------------------------------------------------------------------------

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

# ------------------------------------------------------------------------------
# Normalization helpers
# ------------------------------------------------------------------------------

def _as_timestamp(col_name: str):
    return F.to_timestamp(col(col_name))


def add_common_fields(df, topic_name: str, vendor_id_col, source_updated_at_col):
    return (
        df
        .withColumn("schema_version", lit(SCHEMA_VERSION))
        .withColumn("topic_name", lit(topic_name))
        .withColumn("vendor_id", vendor_id_col.cast("string"))
        .withColumn("entity_key_str", concat_ws("|", col("topic_name"), col("vendor_id")))
        .withColumn("entity_key_hash", sha2(col("entity_key_str"), 256))
        .withColumn("source_updated_at", source_updated_at_col)
        .withColumn("first_seen_at", lit(None).cast("timestamp"))
        .withColumn("last_seen_at", lit(None).cast("timestamp"))
    )


def _array_sort_nullable(arr_col):
    return F.when(arr_col.isNull(), arr_col).otherwise(F.array_sort(arr_col))


def ensure_columns(df, fields):
    for field in fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
    return df.select([col(f.name) for f in fields])

def col_if_exists(df, name: str):
    return col(name) if name in df.columns else lit(None)

def nested_col_if_exists(df, path: str):
    root = path.split(".")[0]
    return col(path) if root in df.columns else lit(None)

def drop_corrupt_if_present(df):
    if "_corrupt_record" in df.columns:
        return df.filter(col("_corrupt_record").isNull())
    return df


TARGET_FIELDS = [
    StructField("schema_version", StringType(), True),
    StructField("entity_key_str", StringType(), True),
    StructField("entity_key_hash", StringType(), True),
    StructField("payload_hash", StringType(), True),
    StructField("topic_name", StringType(), True),
    StructField("vendor_id", StringType(), True),
    StructField("ingest_ts", TimestampType(), True),
    StructField("first_seen_at", TimestampType(), True),
    StructField("last_seen_at", TimestampType(), True),
    StructField("source_updated_at", TimestampType(), True),
    StructField("event_time", TimestampType(), True),

    StructField("asset_uid", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("rapid7_id", StringType(), True),
    StructField("fortisiem_id", StringType(), True),
    StructField("asset_name", StringType(), True),
    StructField("primary_hostname", StringType(), True),
    StructField("primary_ip", StringType(), True),
    StructField("access_ip", StringType(), True),
    StructField("natural_id", StringType(), True),
    StructField("approved", BooleanType(), True),
    StructField("unmanaged", BooleanType(), True),
    StructField("device_vendor", StringType(), True),
    StructField("device_model", StringType(), True),
    StructField("device_version", StringType(), True),
    StructField("os_name", StringType(), True),
    StructField("os_family", StringType(), True),
    StructField("os_vendor", StringType(), True),
    StructField("os_product", StringType(), True),
    StructField("os_version", StringType(), True),
    StructField("os_architecture", StringType(), True),
    StructField("os_certainty", DoubleType(), True),
    StructField("assessed_for_policies", BooleanType(), True),
    StructField("assessed_for_vulnerabilities", BooleanType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("raw_risk_score", DoubleType(), True),
    StructField("vuln_total", IntegerType(), True),
    StructField("vuln_critical", IntegerType(), True),
    StructField("vuln_severe", IntegerType(), True),
    StructField("vuln_moderate", IntegerType(), True),
    StructField("vuln_exploits", IntegerType(), True),
    StructField("vuln_malware_kits", IntegerType(), True),

    StructField("host_domain", StringType(), True),
    StructField("ip_addresses", ArrayType(StringType()), True),
    StructField("external_ip", StringType(), True),
    StructField("cpu_count", IntegerType(), True),
    StructField("memory_bytes", LongType(), True),
    StructField("posture_is_active", BooleanType(), True),
    StructField("posture_firewall_enabled", BooleanType(), True),
    StructField("posture_network_quarantine_enabled", BooleanType(), True),
    StructField("posture_active_threats", IntegerType(), True),
    StructField("tags", ArrayType(StringType()), True),

    StructField("raw_payload", StringType(), True),
    StructField("raw_json", StringType(), True),
]

HISTORY_EXTRA_FIELDS = [
    StructField("valid_from", TimestampType(), True),
    StructField("valid_to", TimestampType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("version_id", StringType(), True),
    StructField("change_ts", TimestampType(), True),
]
HISTORY_FIELDS = TARGET_FIELDS + HISTORY_EXTRA_FIELDS

PAYLOAD_HASH_COLUMNS = [
    "asset_uid",
    "source_system",
    "rapid7_id",
    "fortisiem_id",
    "asset_name",
    "primary_hostname",
    "primary_ip",
    "access_ip",
    "natural_id",
    "approved",
    "unmanaged",
    "device_vendor",
    "device_model",
    "device_version",
    "os_name",
    "os_family",
    "os_vendor",
    "os_product",
    "os_version",
    "os_architecture",
    "os_certainty",
    "assessed_for_policies",
    "assessed_for_vulnerabilities",
    "risk_score",
    "raw_risk_score",
    "vuln_total",
    "vuln_critical",
    "vuln_severe",
    "vuln_moderate",
    "vuln_exploits",
    "vuln_malware_kits",
    "host_domain",
    "ip_addresses",
    "external_ip",
    "cpu_count",
    "memory_bytes",
    "posture_is_active",
    "posture_firewall_enabled",
    "posture_network_quarantine_enabled",
    "posture_active_threats",
    "tags",
]


def add_payload_hash(df):
    ordered_cols = [col(c) for c in PAYLOAD_HASH_COLUMNS]
    return df.withColumn("payload_hash", sha2(to_json(struct(*ordered_cols)), 256))


# ------------------------------------------------------------------------------
# Rapid7 normalization
# ------------------------------------------------------------------------------

def normalize_rapid7(df):
    rapid7_clean = drop_corrupt_if_present(df)

    df = (
        rapid7_clean
        .withColumn("source_system", lit("rapid7"))
        .withColumn("ingest_ts", col("ingest_ts"))
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

        .withColumn("host_domain", lit(None).cast("string"))
        .withColumn("ip_addresses", lit(None).cast(ArrayType(StringType())))
        .withColumn("external_ip", lit(None).cast("string"))
        .withColumn("cpu_count", lit(None).cast("int"))
        .withColumn("memory_bytes", lit(None).cast("long"))
        .withColumn("posture_is_active", lit(None).cast("boolean"))
        .withColumn("posture_firewall_enabled", lit(None).cast("boolean"))
        .withColumn("posture_network_quarantine_enabled", lit(None).cast("boolean"))
        .withColumn("posture_active_threats", lit(None).cast("int"))
        .withColumn("tags", lit(None).cast(ArrayType(StringType())))

        .withColumn(
            "raw_json",
            to_json(struct([col(c) for c in rapid7_clean.columns if c != "_corrupt_record"]))
        )
        .withColumn("raw_payload", col("raw_json"))
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
    )

    df = add_common_fields(
        df,
        RAPID7_TOPIC,
        col("rapid7_id"),
        lit(None).cast("timestamp")
    )
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df

# ------------------------------------------------------------------------------
# FortiSIEM normalization
# ------------------------------------------------------------------------------

def normalize_fortisiem(df):
    forti_clean = drop_corrupt_if_present(df)
    forti_id_expr = F.coalesce(
        nested_col_if_exists(forti_clean, "_id.$oid"),
        col_if_exists(forti_clean, "id"),
        col_if_exists(forti_clean, "naturalId")
    ).cast("string")

    df = (
        forti_clean
        .withColumn("source_system", lit("fortisiem"))
        .withColumn("ingest_ts", col("ingest_ts"))
        .withColumn("rapid7_id", lit(None).cast("string"))
        .withColumn("fortisiem_id", forti_id_expr)

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

        .withColumn("host_domain", lit(None).cast("string"))
        .withColumn("ip_addresses", lit(None).cast(ArrayType(StringType())))
        .withColumn("external_ip", lit(None).cast("string"))
        .withColumn("cpu_count", lit(None).cast("int"))
        .withColumn("memory_bytes", lit(None).cast("long"))
        .withColumn("posture_is_active", lit(None).cast("boolean"))
        .withColumn("posture_firewall_enabled", lit(None).cast("boolean"))
        .withColumn("posture_network_quarantine_enabled", lit(None).cast("boolean"))
        .withColumn("posture_active_threats", lit(None).cast("int"))
        .withColumn("tags", lit(None).cast(ArrayType(StringType())))

        .withColumn(
            "raw_json",
            to_json(struct([col(c) for c in forti_clean.columns if c != "_corrupt_record"]))
        )
        .withColumn("raw_payload", col("raw_json"))
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
    )

    df = add_common_fields(
        df,
        FORTI_TOPIC,
        col("natural_id"),
        lit(None).cast("timestamp")
    )
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df

# ------------------------------------------------------------------------------
# SentinelOne (centinel) normalization
# ------------------------------------------------------------------------------

def normalize_sentinel(df):
    clean = drop_corrupt_if_present(df)

    inet_expr = F.expr("flatten(transform(networkInterfaces, x -> x.inet))")
    inet_arr = F.coalesce(inet_expr, F.expr("array()"))
    ip_array = F.array_union(inet_arr, F.array(col("lastIpToMgmt")))
    ip_array = F.array_distinct(F.filter(ip_array, lambda x: x.isNotNull()))

    df = (
        clean
        .withColumn("source_system", lit("sentinelone"))
        .withColumn("ingest_ts", col("ingest_ts"))

        .withColumn("vendor_id", F.coalesce(col("uuid"), col("id")).cast("string"))
        .withColumn("rapid7_id", lit(None).cast("string"))
        .withColumn("fortisiem_id", lit(None).cast("string"))

        .withColumn("asset_name", col("computerName"))
        .withColumn("primary_hostname", col("computerName"))

        .withColumn("primary_ip", col("lastIpToMgmt"))
        .withColumn("access_ip", col("lastIpToMgmt"))

        .withColumn("natural_id", lit(None).cast("string"))
        .withColumn("approved", lit(None).cast("boolean"))
        .withColumn("unmanaged", lit(None).cast("boolean"))

        .withColumn("device_vendor", lit(None).cast("string"))
        .withColumn("device_model", lit(None).cast("string"))
        .withColumn("device_version", lit(None).cast("string"))

        .withColumn("os_name", col("osName"))
        .withColumn("os_family", col("osType"))
        .withColumn("os_vendor", lit(None).cast("string"))
        .withColumn("os_product", lit(None).cast("string"))
        .withColumn("os_version", col("osRevision"))
        .withColumn("os_architecture", col("osArch"))
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

        .withColumn("host_domain", col("domain"))
        .withColumn("ip_addresses", _array_sort_nullable(ip_array))
        .withColumn("external_ip", col("externalIp"))
        .withColumn("cpu_count", col("cpuCount").cast("int"))
        .withColumn("memory_bytes", (col("totalMemory") * lit(1024 * 1024)).cast("long"))
        .withColumn("posture_is_active", col("isActive"))
        .withColumn("posture_firewall_enabled", col("firewallEnabled"))
        .withColumn("posture_network_quarantine_enabled", col("networkQuarantineEnabled"))
        .withColumn("posture_active_threats", col("activeThreats").cast("int"))
        .withColumn("tags", _array_sort_nullable(col("tags.sentinelone")))

        .withColumn("raw_json", to_json(struct([col(c) for c in clean.columns if c != "_corrupt_record"])) )
        .withColumn("raw_payload", col("raw_json"))
        .withColumn("asset_uid", sha2(concat_ws("|", lower(trim(col("primary_hostname"))), lower(trim(col("primary_ip"))), col("vendor_id")), 256))
    )

    df = add_common_fields(
        df,
        SENTINEL_TOPIC,
        col("vendor_id"),
        _as_timestamp("updatedAt")
    )
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df

# ------------------------------------------------------------------------------
# Table helpers
# ------------------------------------------------------------------------------

def ensure_table(df):
    if not spark.catalog.tableExists(TARGET_TABLE):
        df.limit(0).writeTo(TARGET_TABLE).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(TARGET_TABLE).schema.fields}
    missing = [f for f in TARGET_FIELDS if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {TARGET_TABLE} ADD COLUMNS ({cols_sql})")

def ensure_history_table(df):
    if not spark.catalog.tableExists(HISTORY_TABLE):
        df.limit(0).writeTo(HISTORY_TABLE).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(HISTORY_TABLE).schema.fields}
    missing = [f for f in HISTORY_FIELDS if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMNS ({cols_sql})")


def _with_history_fields(df):
    df = (
        df.withColumn("first_seen_at", col("ingest_ts"))
          .withColumn("last_seen_at", col("ingest_ts"))
          .withColumn("valid_from", col("ingest_ts"))
          .withColumn("valid_to", lit(None).cast("timestamp"))
          .withColumn("is_current", lit(True))
          .withColumn("change_ts", col("ingest_ts"))
          .withColumn(
              "version_id",
              sha2(
                  concat_ws(
                      "|",
                      col("entity_key_hash"),
                      col("payload_hash"),
                      col("valid_from").cast("string")
                  ),
                  256
              )
          )
    )
    return ensure_columns(df, HISTORY_FIELDS)


def merge_history_with_retry(incoming_df, retries: int = 3, sleep_sec: float = 2.0):
    incoming_df = incoming_df.persist()
    try:
        if not incoming_df.take(1):
            return

        current_df = None
        if spark.catalog.tableExists(TARGET_TABLE):
            current_df = (
                spark.table(TARGET_TABLE)
                .select("entity_key_hash", "payload_hash")
                .withColumnRenamed("payload_hash", "cur_payload_hash")
            )

        if current_df is None:
            new_rows = incoming_df
            changed_rows = incoming_df.limit(0)
            same_rows = incoming_df.limit(0)
        else:
            joined = incoming_df.join(current_df, "entity_key_hash", "left")
            new_rows = (
                joined.filter(col("cur_payload_hash").isNull())
                .select(incoming_df.columns)
            )
            changed_rows = (
                joined.filter(
                    col("cur_payload_hash").isNotNull() &
                    (col("payload_hash") != col("cur_payload_hash"))
                )
                .select(incoming_df.columns)
            )
            same_rows = (
                joined.filter(
                    col("cur_payload_hash").isNotNull() &
                    (col("payload_hash") == col("cur_payload_hash"))
                )
                .select("entity_key_hash", "payload_hash", "ingest_ts")
            )

        history_inserts = new_rows.unionByName(changed_rows)
        if not history_inserts.rdd.isEmpty():
            history_inserts = _with_history_fields(history_inserts)
            ensure_history_table(history_inserts)

        if spark.catalog.tableExists(HISTORY_TABLE):
            changed_keys = (
                changed_rows
                .select(
                    "entity_key_hash",
                    "payload_hash",
                    col("ingest_ts").alias("valid_from")
                )
                .distinct()
            )

            last_err = None
            for attempt in range(retries):
                try:
                    if not changed_keys.rdd.isEmpty():
                        changed_keys.createOrReplaceTempView("history_changes")
                        close_sql = f"""
                            MERGE INTO {HISTORY_TABLE} h
                            USING history_changes c
                            ON h.entity_key_hash = c.entity_key_hash
                               AND h.is_current = true
                               AND h.payload_hash <> c.payload_hash
                            WHEN MATCHED THEN
                              UPDATE SET valid_to = c.valid_from, is_current = false
                        """
                        spark.sql(close_sql)

                    if not history_inserts.rdd.isEmpty():
                        history_inserts.createOrReplaceTempView("history_inserts")
                        insert_cols = ", ".join([f.name for f in HISTORY_FIELDS])
                        insert_vals = ", ".join([f"s.{f.name}" for f in HISTORY_FIELDS])
                        insert_sql = f"""
                            MERGE INTO {HISTORY_TABLE} h
                            USING history_inserts s
                            ON h.version_id = s.version_id
                            WHEN NOT MATCHED THEN
                              INSERT ({insert_cols}) VALUES ({insert_vals})
                        """
                        spark.sql(insert_sql)
                    return
                except Exception as e:
                    last_err = e
                    if attempt < retries - 1:
                        time.sleep(sleep_sec)
                        continue
                    raise last_err
    finally:
        incoming_df.unpersist()


def merge_with_retry(df, retries: int = 3, sleep_sec: float = 2.0):
    # Materialize the source to avoid non-deterministic expressions in MERGE.
    df = df.persist()
    try:
        if not df.take(1):
            return

        ensure_table(df)

        # Freeze the dataset so MERGE sees deterministic input.
        df.count()
        df.createOrReplaceTempView("incoming_updates")

        all_cols = [f.name for f in TARGET_FIELDS]
        update_changed_cols = [c for c in all_cols if c not in ("first_seen_at", "last_seen_at")]
        update_changed_sql = ", ".join([f"{c} = s.{c}" for c in update_changed_cols])
        update_changed_sql += ", last_seen_at = s.ingest_ts, first_seen_at = t.first_seen_at"

        update_same_sql = "last_seen_at = greatest(t.last_seen_at, s.ingest_ts), ingest_ts = s.ingest_ts"

        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([
            "s.ingest_ts" if c in ("first_seen_at", "last_seen_at") else f"s.{c}" for c in all_cols
        ])

        merge_sql = f"""
            MERGE INTO {TARGET_TABLE} t
            USING incoming_updates s
            ON t.entity_key_hash = s.entity_key_hash
            WHEN MATCHED AND t.payload_hash = s.payload_hash THEN
              UPDATE SET {update_same_sql}
            WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN
              UPDATE SET {update_changed_sql}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        last_err = None
        for attempt in range(retries):
            try:
                spark.sql(merge_sql)
                return
            except Exception as e:
                last_err = e
                if attempt < retries - 1:
                    time.sleep(sleep_sec)
                    continue
                raise last_err
    finally:
        df.unpersist()

# ------------------------------------------------------------------------------
# Streaming from MinIO object events (Kafka)
# ------------------------------------------------------------------------------

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_ts_row = batch_df.select(F.max("timestamp").alias("batch_ts")).collect()[0]["batch_ts"]
    batch_ts = batch_ts_row if batch_ts_row is not None else datetime.now(timezone.utc)

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
        .filter(F.col("topic_name").isin([RAPID7_TOPIC, FORTI_TOPIC, SENTINEL_TOPIC]))
        .withColumn("file_path", F.concat(F.lit("s3a://"), F.col("bucket"), F.lit("/"), F.col("decoded_key")))
        .withColumn("event_time", F.to_timestamp(F.col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        .withColumn("ingest_ts", F.lit(batch_ts))
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

    rows = decoded.select("topic_name", "file_path", "event_time", "ingest_ts").collect()
    topic_paths = {RAPID7_TOPIC: [], FORTI_TOPIC: [], SENTINEL_TOPIC: []}
    for r in rows:
        topic_paths[r["topic_name"]].append((r["file_path"], r["event_time"], r["ingest_ts"]))

    incoming = []

    def _read_topic(topic_name, normalize_fn):
        entries = topic_paths[topic_name]
        if not entries:
            return
        paths = list(dict.fromkeys([e[0] for e in entries]))
        if not paths:
            return
        existing, _missing = filter_existing_paths(
            paths, READ_RETRY_COUNT, READ_RETRY_SLEEP_SEC
        )
        if not existing:
            return
        schema = load_latest_schema(topic_name)
        if schema is None:
            raise RuntimeError(f"No inferred schema found for {topic_name}")

        meta_by_path = {p: (et, it) for p, et, it in entries}
        dfs = []
        for path in existing:
            event_time, ingest_ts = meta_by_path.get(path, (None, batch_ts))
            df_path = (
                spark.read.schema(schema).options(**JSON_OPTIONS).json(path)
                .withColumn("file_path", F.lit(path))
                .withColumn("event_time", F.lit(event_time))
                .withColumn("ingest_ts", F.lit(ingest_ts if ingest_ts is not None else batch_ts))
            )
            dfs.append(df_path)
        if not dfs:
            return
        df = dfs[0]
        for other in dfs[1:]:
            df = df.unionByName(other)

        incoming.append(normalize_fn(df))

    _read_topic(RAPID7_TOPIC, normalize_rapid7)
    _read_topic(FORTI_TOPIC, normalize_fortisiem)
    _read_topic(SENTINEL_TOPIC, normalize_sentinel)

    if not incoming:
        return

    combined = incoming[0]
    for df in incoming[1:]:
        combined = combined.unionByName(df)

    # Deduplicate per entity_key_hash by most recent timestamps
    combined = combined.filter(col("entity_key_hash").isNotNull())
    order_col = F.coalesce(col("source_updated_at"), col("event_time"), col("ingest_ts"))
    w = Window.partitionBy("entity_key_hash").orderBy(order_col.desc(), col("ingest_ts").desc())
    combined = (
        combined.withColumn("_rn", F.row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    merge_history_with_retry(combined)
    merge_with_retry(combined)


# ------------------------------------------------------------------------------
# Streaming query
# ------------------------------------------------------------------------------

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
