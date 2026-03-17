from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, lower, trim, sha2
from pyspark.sql.types import ArrayType, StringType

from mapping.target import (
    add_common_fields,
    ensure_columns,
    add_payload_hash,
    drop_corrupt_if_present,
    TARGET_FIELDS,
    clean_string,
    clean_string_array,
    normalize_org_name,
    org_map_matched,
)

RAPID7_TOPIC = "rapid7.assets.raw"


def normalize_rapid7(df):
    rapid7_clean = drop_corrupt_if_present(df)

    address_ips = F.expr("transform(addresses, x -> x.ip)")
    address_macs = F.expr("transform(addresses, x -> x.mac)")
    hostnames_from_array = F.expr("transform(hostNames, x -> x.name)")

    all_ips = clean_string_array(F.array_union(F.array(col("ip")), address_ips))
    all_macs = clean_string_array(F.array_union(F.array(col("mac")), address_macs))
    all_hostnames = clean_string_array(F.array_union(F.array(col("hostName")), hostnames_from_array))

    site_name_raw = col("site_name").cast("string")
    normalised_org = normalize_org_name(site_name_raw)

    df = (
        rapid7_clean.withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
        .withColumn("source_system", lit("rapid7"))
        .withColumn("ingest_ts", col("ingest_ts"))
        .withColumn("rapid7_id", col("id").cast("string"))
        .withColumn("fortisiem_id", lit(None).cast("string"))
        .withColumn("asset_name", clean_string(col("hostName")))
        .withColumn("primary_hostname", clean_string(col("hostName")))
        .withColumn("hostnames", all_hostnames)
        .withColumn("host_domain", lit(None).cast("string"))
        .withColumn("primary_ip", clean_string(col("ip")))
        .withColumn("ip_addresses", all_ips)
        .withColumn("primary_mac", clean_string(col("mac")))
        .withColumn("mac_addresses", all_macs)
        .withColumn("serial_number", lit(None).cast("string"))
        .withColumn("access_ip", lit(None).cast("string"))
        .withColumn("natural_id", lit(None).cast("string"))
        .withColumn("approved", lit(None).cast("boolean"))
        .withColumn("unmanaged", lit(None).cast("boolean"))
        .withColumn("device_vendor", lit(None).cast("string"))
        .withColumn("device_model", lit(None).cast("string"))
        .withColumn("device_version", lit(None).cast("string"))
        .withColumn("os_name", clean_string(col("os")))
        .withColumn("os_family", clean_string(col("osFingerprint.family")))
        .withColumn("os_vendor", clean_string(col("osFingerprint.vendor")))
        .withColumn("os_product", clean_string(col("osFingerprint.product")))
        .withColumn("os_version", clean_string(F.coalesce(col("osFingerprint.cpe.version"), col("osFingerprint.version"))))
        .withColumn("os_architecture", clean_string(col("osFingerprint.architecture")))
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
        .withColumn("external_ip", lit(None).cast("string"))
        .withColumn("cpu_count", lit(None).cast("int"))
        .withColumn("memory_bytes", lit(None).cast("long"))
        .withColumn("posture_is_active", lit(None).cast("boolean"))
        .withColumn("posture_firewall_enabled", lit(None).cast("boolean"))
        .withColumn("posture_network_quarantine_enabled", lit(None).cast("boolean"))
        .withColumn("posture_active_threats", lit(None).cast("int"))
        .withColumn("tags", lit(None).cast(ArrayType(StringType())))
        .withColumn("site_id", clean_string(col("site_id").cast("string")))
        .withColumn("site_name", clean_string(site_name_raw))
        .withColumn("normalised_org_name", normalised_org)
        .withColumn("account_name", lit(None).cast("string"))
        .withColumn("org_map_matched", org_map_matched(col("site_name"), col("normalised_org_name")))
        .withColumn("rapid7_asset_id", col("id").cast("string"))
        .withColumn("rapid7_site_id", clean_string(col("site_id").cast("string")))
        .withColumn("rapid7_site_name", clean_string(site_name_raw))
        .withColumn("rapid7_primary_mac", clean_string(col("mac")))
        .withColumn("rapid7_ip_addresses", all_ips)
        .withColumn("rapid7_mac_addresses", all_macs)
        .withColumn("rapid7_hostnames", all_hostnames)
        .withColumn("rapid7_os_certainty", col("osCertainty").cast("double"))
        .withColumn("rapid7_assessed_for_policies", col("assessedForPolicies"))
        .withColumn("rapid7_assessed_for_vulnerabilities", col("assessedForVulnerabilities"))
        .withColumn("rapid7_risk_score", col("riskScore").cast("double"))
        .withColumn("rapid7_raw_risk_score", col("rawRiskScore").cast("double"))
        .withColumn("rapid7_vuln_total", col("vulnerabilities.total").cast("int"))
        .withColumn("rapid7_vuln_critical", col("vulnerabilities.critical").cast("int"))
        .withColumn("rapid7_vuln_severe", col("vulnerabilities.severe").cast("int"))
        .withColumn("rapid7_vuln_moderate", col("vulnerabilities.moderate").cast("int"))
        .withColumn("rapid7_vuln_exploits", col("vulnerabilities.exploits").cast("int"))
        .withColumn("rapid7_vuln_malware_kits", col("vulnerabilities.malwareKits").cast("int"))
        .withColumn("rapid7_services_count", F.size(F.coalesce(col("services"), F.expr("array()"))).cast("int"))
        .withColumn("rapid7_software_count", F.size(F.coalesce(col("software"), F.expr("array()"))).cast("int"))
        .withColumn("rapid7_asset_type", clean_string(col("type")))
        .withColumn("raw_json", to_json(struct([col(c) for c in rapid7_clean.columns if c != "_corrupt_record"])))
        .withColumn("raw_payload", col("raw_json"))
        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("primary_ip"))),
                    col("rapid7_id"),
                ),
                256,
            ),
        )
    )

    df = add_common_fields(df, RAPID7_TOPIC, col("rapid7_id"), lit(None).cast("timestamp"))
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df
