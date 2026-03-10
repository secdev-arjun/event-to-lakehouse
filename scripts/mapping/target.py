from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, to_json, struct,
    concat_ws, lower, trim, sha2
)
from pyspark.sql.types import (
    StructField, StringType, BooleanType, DoubleType,
    IntegerType, ArrayType, LongType, TimestampType
)

SCHEMA_VERSION = "silver.asset_observation.v1"


def _as_timestamp(col_name: str):
    return F.to_timestamp(col(col_name))


def add_common_fields(df, topic_name: str, vendor_id_col, source_updated_at_col):
    return (
        df
        .withColumn("schema_version", lit(SCHEMA_VERSION))
        .withColumn("topic_name", lit(topic_name))
        .withColumn("vendor_id", vendor_id_col.cast("string"))
        .withColumn("entity_key_str", concat_ws("|", col("topic_name"), col("vendor_id")))
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
    StructField("source", StringType(), True),
    StructField("entity_id", StringType(), True),
    StructField("entity_key_str", StringType(), True),
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
    StructField("site_id", StringType(), True),
    StructField("site_name", StringType(), True),
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


PAYLOAD_HASH_COLUMNS = [
    "asset_uid",
    "source_system",
    "site_id",
    "site_name",
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
