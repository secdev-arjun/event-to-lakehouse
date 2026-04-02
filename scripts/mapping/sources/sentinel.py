from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, lower, trim, sha2

from mapping.target import (
    add_common_fields,
    ensure_columns,
    add_payload_hash,
    drop_corrupt_if_present,
    col_if_exists,
    _as_timestamp,
    TARGET_FIELDS,
    clean_string,
    clean_string_array,
    normalize_mac_array,
    split_site_name_and_org,
    org_map_matched,
    filter_matching_ip_addresses,
    valid_hardware_serial,
)

SENTINEL_TOPIC = "centinel.agents.raw"


def normalize_sentinel(df):
    clean = drop_corrupt_if_present(df)

    if "networkInterfaces" in clean.columns:
        inet_expr = F.expr("flatten(transform(coalesce(networkInterfaces, array()), x -> coalesce(x.inet, array())))")
        interface_physical_macs_expr = F.expr(
            "transform(coalesce(networkInterfaces, array()), x -> x.physical)"
        )
        gateway_macs_expr = F.expr(
            "transform(coalesce(networkInterfaces, array()), x -> x.gatewayMacAddress)"
        )
    else:
        inet_expr = lit(None).cast("array<string>")
        interface_physical_macs_expr = lit(None).cast("array<string>")
        gateway_macs_expr = lit(None).cast("array<string>")

    if "locations" in clean.columns:
        locations_expr = F.expr("transform(coalesce(locations, array()), x -> x.name)")
    else:
        locations_expr = lit(None).cast("array<string>")

    inet_arr = F.coalesce(inet_expr, F.array().cast("array<string>"))
    ip_array_raw = clean_string_array(F.array_union(inet_arr, F.array(col("lastIpToMgmt"))))

    hostnames = clean_string_array(F.array(col("computerName")))
    ip_addresses = filter_matching_ip_addresses(ip_array_raw)
    mac_addresses = normalize_mac_array(interface_physical_macs_expr)
    gateway_mac_addresses = normalize_mac_array(gateway_macs_expr)

    site_name_raw = F.coalesce(
        col_if_exists(clean, "siteName"),
        col_if_exists(clean, "sitename"),
        col_if_exists(clean, "site_name"),
    ).cast("string")
    source_site_ref_id = clean_string(
        F.coalesce(
            col_if_exists(clean, "siteId"),
            col_if_exists(clean, "siteid"),
            col_if_exists(clean, "site_id"),
        ).cast("string")
    )
    mapped_site_name, normalised_org = split_site_name_and_org(site_name_raw)

    source_record_id = clean_string(col("id").cast("string"))
    source_natural_id = clean_string(col("uuid"))

    df = (
        clean.withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
        .withColumn("source_system", lit("sentinelone"))
        .withColumn("source_record_id", source_record_id)
        .withColumn("source_natural_id", source_natural_id)
        .withColumn("source_site_ref_id", source_site_ref_id)
        .withColumn("source_display_name", lit(None).cast("string"))
        .withColumn("ingest_ts", col("ingest_ts"))
        .withColumn("asset_name", clean_string(col("computerName")))
        .withColumn("primary_hostname", clean_string(col("computerName")))
        .withColumn("hostnames", hostnames)
        .withColumn("host_domain", clean_string(col("domain")))
        .withColumn("primary_ip", clean_string(col("lastIpToMgmt")))
        .withColumn("ip_addresses_raw", ip_array_raw)
        .withColumn("ip_addresses", ip_addresses)
        .withColumn("ipv6_addresses", lit(None).cast("array<string>"))
        .withColumn("primary_mac", lit(None).cast("string"))
        .withColumn("mac_addresses", mac_addresses)
        .withColumn("gateway_mac_addresses", gateway_mac_addresses)
        .withColumn("virtual_mac_addresses", lit(None).cast("array<string>"))
        .withColumn("serial_number", valid_hardware_serial(col("serialNumber")))
        .withColumn("access_ip", clean_string(col("lastIpToMgmt")))
        .withColumn("external_ip", clean_string(col("externalIp")))
        .withColumn("approved", lit(None).cast("boolean"))
        .withColumn("unmanaged", lit(None).cast("boolean"))
        .withColumn("tags", clean_string_array(col("tags.sentinelone")))
        .withColumn("site_id", source_site_ref_id)
        .withColumn("site_name", mapped_site_name)
        .withColumn("normalised_org_name", normalised_org)
        .withColumn("account_id", clean_string(col("accountId")))
        .withColumn("account_name", clean_string(col("accountName")))
        .withColumn("org_map_matched", org_map_matched(site_name_raw, col("normalised_org_name")))
        .withColumn("site_description", lit(None).cast("string"))
        .withColumn("site_type", lit(None).cast("string"))
        .withColumn("site_importance", lit(None).cast("string"))
        .withColumn("site_assets_count", lit(None).cast("long"))
        .withColumn("site_risk_score", lit(None).cast("double"))
        .withColumn("site_last_scan_time", lit(None).cast("timestamp"))
        .withColumn("site_scan_engine", lit(None).cast("long"))
        .withColumn("site_scan_template", lit(None).cast("string"))
        .withColumn("site_vuln_total", lit(None).cast("long"))
        .withColumn("site_vuln_critical", lit(None).cast("long"))
        .withColumn("site_vuln_severe", lit(None).cast("long"))
        .withColumn("site_vuln_moderate", lit(None).cast("long"))
        .withColumn("platform_version", lit(None).cast("string"))
        .withColumn("asset_type", lit(None).cast("string"))
        .withColumn("os_name", clean_string(col("osName")))
        .withColumn("os_family", clean_string(col("osType")))
        .withColumn("os_vendor", lit(None).cast("string"))
        .withColumn("os_product", lit(None).cast("string"))
        .withColumn("os_version", clean_string(col("osRevision")))
        .withColumn("os_architecture", clean_string(col("osArch")))
        .withColumn("os_edition", lit(None).cast("string"))
        .withColumn("os_certainty", lit(None).cast("double"))
        .withColumn("cpu_count", col("cpuCount").cast("int"))
        .withColumn("memory_bytes", (col("totalMemory") * lit(1024 * 1024)).cast("long"))
        .withColumn("system_uptime", lit(None).cast("long"))
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
        .withColumn("posture_is_active", col("isActive"))
        .withColumn("posture_firewall_enabled", col("firewallEnabled"))
        .withColumn("posture_network_quarantine_enabled", col("networkQuarantineEnabled"))
        .withColumn("posture_active_threats", col("activeThreats").cast("int"))
        .withColumn("scan_status", clean_string(col("scanStatus")))
        .withColumn("operational_state", clean_string(col("operationalState")))
        .withColumn("operational_state_expiration", clean_string(col("operationalStateExpiration")))
        .withColumn("network_status", clean_string(col("networkStatus")))
        .withColumn("ranger_status", clean_string(col("rangerStatus")))
        .withColumn("mitigation_mode", clean_string(col("mitigationMode")))
        .withColumn("mitigation_mode_suspicious", clean_string(col("mitigationModeSuspicious")))
        .withColumn("machine_type", clean_string(col("machineType")))
        .withColumn("group_id", clean_string(col("groupId")))
        .withColumn("group_name", clean_string(col("groupName")))
        .withColumn("last_logged_in_user_name", clean_string(col("lastLoggedInUserName")))
        .withColumn("active_protection_modes", clean_string_array(col("activeProtection")))
        .withColumn("missing_permissions", clean_string_array(col("missingPermissions")))
        .withColumn("user_actions_needed", clean_string_array(col("userActionsNeeded")))
        .withColumn("location_names", clean_string_array(locations_expr))
        .withColumn("discover_method", lit(None).cast("string"))
        .withColumn("event_log_status", lit(None).cast("string"))
        .withColumn("perf_mon_status", lit(None).cast("string"))
        .withColumn("update_method", lit(None).cast("string"))
        .withColumn("services_count", lit(None).cast("int"))
        .withColumn("software_count", lit(None).cast("int"))
        .withColumn("raw_json", to_json(struct([col(c) for c in clean.columns if c != "_corrupt_record"])))
        .withColumn("raw_payload", col("raw_json"))
        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("primary_ip"))),
                    col("source_record_id"),
                ),
                256,
            ),
        )
    )

    df = add_common_fields(
        df,
        SENTINEL_TOPIC,
        F.coalesce(col("source_natural_id"), col("source_record_id")),
        _as_timestamp("updatedAt"),
    )
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df
