from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, lower, trim, sha2
from pyspark.sql.types import ArrayType, StringType

from mapping.target import (
    add_common_fields,
    ensure_columns,
    add_payload_hash,
    col_if_exists,
    nested_col_if_exists,
    drop_corrupt_if_present,
    TARGET_FIELDS,
    clean_string,
    clean_string_array,
    normalize_org_name,
    org_map_matched,
    first_array_value,
)

FORTI_TOPIC = "fortisiem.devices.raw"


def normalize_fortisiem(df):
    forti_clean = drop_corrupt_if_present(df)
    forti_id_expr = F.coalesce(
        nested_col_if_exists(forti_clean, "_id.$oid"),
        col_if_exists(forti_clean, "id"),
        col_if_exists(forti_clean, "naturalId"),
    ).cast("string")

    empty_str_array = F.array().cast("array<string>")
    if "interfaces" in forti_clean.columns:
        iface_ipv4_expr = F.expr(
            "transform(coalesce(interfaces.networkinterface, array()), x -> x.ipv4Addr)"
        )
        iface_ipv6_expr = F.expr(
            "transform(coalesce(interfaces.networkinterface, array()), x -> x.ipv6Addr)"
        )
        iface_mac_expr = F.expr(
            "transform(coalesce(interfaces.networkinterface, array()), x -> x.macAddr)"
        )
    else:
        iface_ipv4_expr = lit(None).cast("array<string>")
        iface_ipv6_expr = lit(None).cast("array<string>")
        iface_mac_expr = lit(None).cast("array<string>")

    access_ip_array = clean_string_array(F.array(col("accessIp")))
    iface_ipv4_array = clean_string_array(iface_ipv4_expr)
    forti_ip_addresses = clean_string_array(
        F.array_union(
            F.coalesce(access_ip_array, empty_str_array),
            F.coalesce(iface_ipv4_array, empty_str_array),
        )
    )
    forti_ipv6_addresses = clean_string_array(iface_ipv6_expr)
    forti_mac_addresses = clean_string_array(iface_mac_expr)

    site_name_raw = nested_col_if_exists(forti_clean, "organization.attr_name").cast("string")
    normalised_org = normalize_org_name(site_name_raw)

    df = (
        forti_clean.withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
        .withColumn("source_system", lit("fortisiem"))
        .withColumn("ingest_ts", col("ingest_ts"))
        .withColumn("rapid7_id", lit(None).cast("string"))
        .withColumn("fortisiem_id", forti_id_expr)
        .withColumn("asset_name", clean_string(col("name")))
        .withColumn("primary_hostname", lit(None).cast("string"))
        .withColumn("hostnames", lit(None).cast("array<string>"))
        .withColumn("host_domain", lit(None).cast("string"))
        .withColumn("primary_ip", lit(None).cast("string"))
        .withColumn("ip_addresses", forti_ip_addresses)
        .withColumn("primary_mac", lit(None).cast("string"))
        .withColumn("mac_addresses", forti_mac_addresses)
        .withColumn("serial_number", clean_string(col("hwSerialNum")))
        .withColumn("access_ip", clean_string(col("accessIp")))
        .withColumn("natural_id", clean_string(col("naturalId")))
        .withColumn("approved", col("approved"))
        .withColumn("unmanaged", col("unmanaged"))
        .withColumn("device_vendor", clean_string(col("deviceType.vendor")))
        .withColumn("device_model", clean_string(col("deviceType.model")))
        .withColumn("device_version", clean_string(col("deviceType.version")))
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
        .withColumn("external_ip", lit(None).cast("string"))
        .withColumn("cpu_count", lit(None).cast("int"))
        .withColumn("memory_bytes", lit(None).cast("long"))
        .withColumn("posture_is_active", lit(None).cast("boolean"))
        .withColumn("posture_firewall_enabled", lit(None).cast("boolean"))
        .withColumn("posture_network_quarantine_enabled", lit(None).cast("boolean"))
        .withColumn("posture_active_threats", lit(None).cast("int"))
        .withColumn("tags", lit(None).cast(ArrayType(StringType())))
        .withColumn("site_id", clean_string(nested_col_if_exists(forti_clean, "organization.attr_id").cast("string")))
        .withColumn("site_name", clean_string(site_name_raw))
        .withColumn("normalised_org_name", normalised_org)
        .withColumn("account_name", lit(None).cast("string"))
        .withColumn("org_map_matched", org_map_matched(col("site_name"), col("normalised_org_name")))
        .withColumn("fortisiem_device_id", forti_id_expr)
        .withColumn("fortisiem_natural_id", clean_string(col("naturalId")))
        .withColumn("fortisiem_site_id", clean_string(nested_col_if_exists(forti_clean, "organization.attr_id").cast("string")))
        .withColumn("fortisiem_site_name", clean_string(site_name_raw))
        .withColumn("fortisiem_access_ip", clean_string(col("accessIp")))
        .withColumn("fortisiem_primary_ip", clean_string(col("accessIp")))
        .withColumn("fortisiem_ip_addresses", forti_ip_addresses)
        .withColumn("fortisiem_ipv6_addresses", forti_ipv6_addresses)
        .withColumn("fortisiem_mac_addresses", forti_mac_addresses)
        .withColumn("fortisiem_primary_mac", first_array_value(forti_mac_addresses))
        .withColumn("fortisiem_hw_vendor", clean_string(col("hwVendor")))
        .withColumn("fortisiem_hw_model", clean_string(col("hwModel")))
        .withColumn("fortisiem_hw_serial", clean_string(col("hwSerialNum")))
        .withColumn("fortisiem_bios", clean_string(col("bios")))
        .withColumn("fortisiem_device_category", clean_string(col("deviceType.category")))
        .withColumn("fortisiem_device_status", clean_string(col("deviceStatus")))
        .withColumn("fortisiem_discover_method", clean_string(col("discoverMethod")))
        .withColumn("fortisiem_event_log_status", clean_string(col("eventLogStatus")))
        .withColumn("fortisiem_perf_mon_status", clean_string(col("perfMonStatus")))
        .withColumn("fortisiem_system_uptime", col("systemUpTime").cast("long"))
        .withColumn("fortisiem_os_edition", clean_string(col("osEdition")))
        .withColumn("fortisiem_version", clean_string(col("version")))
        .withColumn("fortisiem_update_method", clean_string(col("updateMethod")))
        .withColumn("fortisiem_approved", col("approved"))
        .withColumn("fortisiem_unmanaged", col("unmanaged"))
        .withColumn("raw_json", to_json(struct([col(c) for c in forti_clean.columns if c != "_corrupt_record"])))
        .withColumn("raw_payload", col("raw_json"))
        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("access_ip"))),
                    col("fortisiem_id"),
                ),
                256,
            ),
        )
    )

    df = add_common_fields(df, FORTI_TOPIC, col("natural_id"), lit(None).cast("timestamp"))
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df
