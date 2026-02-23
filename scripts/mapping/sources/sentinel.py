from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, to_json, struct,
    concat_ws, lower, trim, sha2
)

from mapping.target import (
    add_common_fields,
    ensure_columns,
    add_payload_hash,
    drop_corrupt_if_present,
    _array_sort_nullable,
    _as_timestamp,
    TARGET_FIELDS,
)

SENTINEL_TOPIC = "centinel.agents.raw"


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
        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("primary_ip"))),
                    col("vendor_id")
                ),
                256
            )
        )
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
