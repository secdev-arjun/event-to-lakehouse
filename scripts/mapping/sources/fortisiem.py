from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, to_json, struct,
    concat_ws, lower, trim, sha2
)
from pyspark.sql.types import ArrayType, StringType

from mapping.target import (
    add_common_fields,
    ensure_columns,
    add_payload_hash,
    col_if_exists,
    nested_col_if_exists,
    drop_corrupt_if_present,
    TARGET_FIELDS,
)

FORTI_TOPIC = "fortisiem.devices.raw"


def normalize_fortisiem(df):
    forti_clean = drop_corrupt_if_present(df)
    forti_id_expr = F.coalesce(
        nested_col_if_exists(forti_clean, "_id.$oid"),
        col_if_exists(forti_clean, "id"),
        col_if_exists(forti_clean, "naturalId")
    ).cast("string")

    df = (
        forti_clean
        .withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
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
            "site_id",
            nested_col_if_exists(forti_clean, "organization.attr_id").cast("string")
        )
        .withColumn(
            "site_name",
            nested_col_if_exists(forti_clean, "organization.attr_name").cast("string")
        )

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
