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
    drop_corrupt_if_present,
    TARGET_FIELDS,
)

RAPID7_TOPIC = "rapid7.assets.raw"


def normalize_rapid7(df):
    rapid7_clean = drop_corrupt_if_present(df)

    df = (
        rapid7_clean
        .withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
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
        .withColumn("site_id", col("site_id").cast("string"))
        .withColumn("site_name", col("site_name").cast("string"))

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
