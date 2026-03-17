from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, sha2
from pyspark.sql.types import (
    StructField,
    StringType,
    BooleanType,
    DoubleType,
    IntegerType,
    ArrayType,
    LongType,
    TimestampType,
)

SCHEMA_VERSION = "silver.asset_observation.v1"

ORG_NAME_NORMALIZATION_RULES = [
    (r"(?i)^cced windows quarter$", "CCED"),
    (r"(?i)^ccedorg$", "CCED"),
    (r"(?i)^cc energy development oman$", "CCED"),
    (r"(?i)^securado$", "Securado"),
    (r"(?i)^securado hq$", "Securado"),
    (r"(?i)^securado - hq$", "Securado"),
    (r"(?i)^securado ho$", "Securado"),
    (r"(?i)^securado - ho$", "Securado"),
    (r"(?i)^securado headoffice$", "Securado"),
    (r"(?i)^securado - head office$", "Securado"),
    (r"(?i)^securado head office$", "Securado"),
    (r"(?i)^securado-in$", "Securado"),
]


def _as_timestamp(col_name: str):
    return F.to_timestamp(col(col_name))


def clean_string(col_expr):
    casted = col_expr.cast("string")
    trimmed = F.trim(casted)
    return F.when(casted.isNull() | (trimmed == ""), lit(None).cast("string")).otherwise(trimmed)


def clean_string_array(arr_col):
    as_array = arr_col.cast("array<string>")
    trimmed = F.transform(F.filter(as_array, lambda x: x.isNotNull()), lambda x: F.trim(x))
    filtered = F.filter(trimmed, lambda x: x != "")
    cleaned = F.array_sort(F.array_distinct(filtered))
    return F.when(as_array.isNull(), lit(None).cast("array<string>")).otherwise(cleaned)


def first_array_value(arr_col):
    return F.when(arr_col.isNotNull() & (F.size(arr_col) > 0), arr_col.getItem(0)).otherwise(
        lit(None).cast("string")
    )


def normalize_org_name(col_expr):
    expr = clean_string(col_expr)
    for pattern, replacement in ORG_NAME_NORMALIZATION_RULES:
        expr = F.regexp_replace(expr, pattern, replacement)
    return clean_string(expr)


def org_map_matched(raw_col_expr, normalized_col_expr):
    raw_norm = F.lower(clean_string(raw_col_expr))
    mapped_norm = F.lower(clean_string(normalized_col_expr))
    return (
        F.when(raw_norm.isNull() | mapped_norm.isNull(), lit(False))
        .otherwise(raw_norm != mapped_norm)
        .cast("boolean")
    )


def add_common_fields(df, topic_name: str, vendor_id_col, source_updated_at_col):
    return (
        df.withColumn("schema_version", lit(SCHEMA_VERSION))
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
    StructField("normalised_org_name", StringType(), True),
    StructField("account_name", StringType(), True),
    StructField("org_map_matched", BooleanType(), True),
    StructField("rapid7_id", StringType(), True),
    StructField("fortisiem_id", StringType(), True),
    StructField("asset_name", StringType(), True),
    StructField("primary_hostname", StringType(), True),
    StructField("hostnames", ArrayType(StringType()), True),
    StructField("host_domain", StringType(), True),
    StructField("primary_ip", StringType(), True),
    StructField("ip_addresses", ArrayType(StringType()), True),
    StructField("primary_mac", StringType(), True),
    StructField("mac_addresses", ArrayType(StringType()), True),
    StructField("serial_number", StringType(), True),
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
    StructField("external_ip", StringType(), True),
    StructField("cpu_count", IntegerType(), True),
    StructField("memory_bytes", LongType(), True),
    StructField("posture_is_active", BooleanType(), True),
    StructField("posture_firewall_enabled", BooleanType(), True),
    StructField("posture_network_quarantine_enabled", BooleanType(), True),
    StructField("posture_active_threats", IntegerType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("sentinelone_account_id", StringType(), True),
    StructField("sentinelone_account_name", StringType(), True),
    StructField("sentinelone_agent_version", StringType(), True),
    StructField("sentinelone_machine_type", StringType(), True),
    StructField("sentinelone_mitigation_mode", StringType(), True),
    StructField("sentinelone_mitigation_mode_suspicious", StringType(), True),
    StructField("sentinelone_scan_status", StringType(), True),
    StructField("sentinelone_operational_state", StringType(), True),
    StructField("sentinelone_operational_state_expiration", StringType(), True),
    StructField("sentinelone_is_decommissioned", BooleanType(), True),
    StructField("sentinelone_firewall_enabled", BooleanType(), True),
    StructField("sentinelone_network_quarantine_enabled", BooleanType(), True),
    StructField("sentinelone_ranger_status", StringType(), True),
    StructField("sentinelone_network_status", StringType(), True),
    StructField("sentinelone_group_id", StringType(), True),
    StructField("sentinelone_group_name", StringType(), True),
    StructField("sentinelone_active_threats", IntegerType(), True),
    StructField("sentinelone_last_logged_in_user_name", StringType(), True),
    StructField("sentinelone_serial_number", StringType(), True),
    StructField("sentinelone_ip_addresses", ArrayType(StringType()), True),
    StructField("sentinelone_mac_addresses", ArrayType(StringType()), True),
    StructField("sentinelone_gateway_mac_addresses", ArrayType(StringType()), True),
    StructField("sentinelone_active_protection", ArrayType(StringType()), True),
    StructField("sentinelone_missing_permissions", ArrayType(StringType()), True),
    StructField("sentinelone_user_actions_needed", ArrayType(StringType()), True),
    StructField("sentinelone_locations", ArrayType(StringType()), True),
    StructField("rapid7_asset_id", StringType(), True),
    StructField("rapid7_site_id", StringType(), True),
    StructField("rapid7_site_name", StringType(), True),
    StructField("rapid7_primary_mac", StringType(), True),
    StructField("rapid7_ip_addresses", ArrayType(StringType()), True),
    StructField("rapid7_mac_addresses", ArrayType(StringType()), True),
    StructField("rapid7_hostnames", ArrayType(StringType()), True),
    StructField("rapid7_os_certainty", DoubleType(), True),
    StructField("rapid7_assessed_for_policies", BooleanType(), True),
    StructField("rapid7_assessed_for_vulnerabilities", BooleanType(), True),
    StructField("rapid7_risk_score", DoubleType(), True),
    StructField("rapid7_raw_risk_score", DoubleType(), True),
    StructField("rapid7_vuln_total", IntegerType(), True),
    StructField("rapid7_vuln_critical", IntegerType(), True),
    StructField("rapid7_vuln_severe", IntegerType(), True),
    StructField("rapid7_vuln_moderate", IntegerType(), True),
    StructField("rapid7_vuln_exploits", IntegerType(), True),
    StructField("rapid7_vuln_malware_kits", IntegerType(), True),
    StructField("rapid7_services_count", IntegerType(), True),
    StructField("rapid7_software_count", IntegerType(), True),
    StructField("rapid7_asset_type", StringType(), True),
    StructField("fortisiem_device_id", StringType(), True),
    StructField("fortisiem_natural_id", StringType(), True),
    StructField("fortisiem_site_id", StringType(), True),
    StructField("fortisiem_site_name", StringType(), True),
    StructField("fortisiem_access_ip", StringType(), True),
    StructField("fortisiem_primary_ip", StringType(), True),
    StructField("fortisiem_ip_addresses", ArrayType(StringType()), True),
    StructField("fortisiem_ipv6_addresses", ArrayType(StringType()), True),
    StructField("fortisiem_mac_addresses", ArrayType(StringType()), True),
    StructField("fortisiem_primary_mac", StringType(), True),
    StructField("fortisiem_hw_vendor", StringType(), True),
    StructField("fortisiem_hw_model", StringType(), True),
    StructField("fortisiem_hw_serial", StringType(), True),
    StructField("fortisiem_bios", StringType(), True),
    StructField("fortisiem_device_category", StringType(), True),
    StructField("fortisiem_device_status", StringType(), True),
    StructField("fortisiem_discover_method", StringType(), True),
    StructField("fortisiem_event_log_status", StringType(), True),
    StructField("fortisiem_perf_mon_status", StringType(), True),
    StructField("fortisiem_system_uptime", LongType(), True),
    StructField("fortisiem_os_edition", StringType(), True),
    StructField("fortisiem_version", StringType(), True),
    StructField("fortisiem_update_method", StringType(), True),
    StructField("fortisiem_approved", BooleanType(), True),
    StructField("fortisiem_unmanaged", BooleanType(), True),
    StructField("raw_payload", StringType(), True),
    StructField("raw_json", StringType(), True),
]


PAYLOAD_HASH_EXCLUDE = {
    "schema_version",
    "source",
    "entity_id",
    "entity_key_str",
    "payload_hash",
    "topic_name",
    "vendor_id",
    "ingest_ts",
    "first_seen_at",
    "last_seen_at",
    "source_updated_at",
    "event_time",
    "raw_payload",
    "raw_json",
}
PAYLOAD_HASH_COLUMNS = [f.name for f in TARGET_FIELDS if f.name not in PAYLOAD_HASH_EXCLUDE]


def add_payload_hash(df):
    ordered_cols = [col(c) for c in PAYLOAD_HASH_COLUMNS]
    return df.withColumn("payload_hash", sha2(to_json(struct(*ordered_cols)), 256))
