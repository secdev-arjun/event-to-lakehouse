from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, lower, trim, sha2

from mapping.target import (
    add_common_fields,
    ensure_columns,
    add_payload_hash,
    drop_corrupt_if_present,
    col_if_exists,
    _array_sort_nullable,
    _as_timestamp,
    TARGET_FIELDS,
    clean_string,
    clean_string_array,
    normalize_org_name,
    org_map_matched,
)

SENTINEL_TOPIC = "centinel.agents.raw"


def normalize_sentinel(df):
    clean = drop_corrupt_if_present(df)

    inet_expr = F.expr("flatten(transform(networkInterfaces, x -> x.inet))")
    inet_arr = F.coalesce(inet_expr, F.expr("array()"))
    ip_array_raw = F.array_union(inet_arr, F.array(col("lastIpToMgmt")))

    interface_physical_macs_expr = F.expr("transform(networkInterfaces, x -> x.physical)")
    gateway_macs_expr = F.expr("transform(networkInterfaces, x -> x.gatewayMacAddress)")
    locations_expr = F.expr("transform(locations, x -> x.name)")

    hostnames = clean_string_array(F.array(col("computerName")))
    ip_addresses = clean_string_array(ip_array_raw)
    sentinelone_mac_addresses = clean_string_array(interface_physical_macs_expr)
    sentinelone_gateway_mac_addresses = clean_string_array(gateway_macs_expr)

    site_name_raw = F.coalesce(
        col_if_exists(clean, "siteName"),
        col_if_exists(clean, "sitename"),
        col_if_exists(clean, "site_name"),
    ).cast("string")
    normalised_org = normalize_org_name(site_name_raw)

    df = (
        clean.withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
        .withColumn("source_system", lit("sentinelone"))
        .withColumn("ingest_ts", col("ingest_ts"))
        .withColumn("vendor_id", F.coalesce(col("uuid"), col("id")).cast("string"))
        .withColumn("rapid7_id", lit(None).cast("string"))
        .withColumn("fortisiem_id", lit(None).cast("string"))
        .withColumn("asset_name", clean_string(col("computerName")))
        .withColumn("primary_hostname", clean_string(col("computerName")))
        .withColumn("hostnames", hostnames)
        .withColumn("host_domain", clean_string(col("domain")))
        .withColumn("primary_ip", clean_string(col("lastIpToMgmt")))
        .withColumn("ip_addresses", ip_addresses)
        .withColumn("primary_mac", lit(None).cast("string"))
        .withColumn("mac_addresses", sentinelone_mac_addresses)
        .withColumn("serial_number", clean_string(col("serialNumber")))
        .withColumn("access_ip", clean_string(col("lastIpToMgmt")))
        .withColumn("natural_id", lit(None).cast("string"))
        .withColumn("approved", lit(None).cast("boolean"))
        .withColumn("unmanaged", lit(None).cast("boolean"))
        .withColumn("device_vendor", lit(None).cast("string"))
        .withColumn("device_model", lit(None).cast("string"))
        .withColumn("device_version", lit(None).cast("string"))
        .withColumn("os_name", clean_string(col("osName")))
        .withColumn("os_family", clean_string(col("osType")))
        .withColumn("os_vendor", lit(None).cast("string"))
        .withColumn("os_product", lit(None).cast("string"))
        .withColumn("os_version", clean_string(col("osRevision")))
        .withColumn("os_architecture", clean_string(col("osArch")))
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
        .withColumn("external_ip", clean_string(col("externalIp")))
        .withColumn("cpu_count", col("cpuCount").cast("int"))
        .withColumn("memory_bytes", (col("totalMemory") * lit(1024 * 1024)).cast("long"))
        .withColumn("posture_is_active", col("isActive"))
        .withColumn("posture_firewall_enabled", col("firewallEnabled"))
        .withColumn("posture_network_quarantine_enabled", col("networkQuarantineEnabled"))
        .withColumn("posture_active_threats", col("activeThreats").cast("int"))
        .withColumn("tags", _array_sort_nullable(clean_string_array(col("tags.sentinelone"))))
        .withColumn(
            "site_id",
            F.coalesce(
                col_if_exists(clean, "siteId"),
                col_if_exists(clean, "siteid"),
                col_if_exists(clean, "site_id"),
            ).cast("string"),
        )
        .withColumn("site_name", clean_string(site_name_raw))
        .withColumn("normalised_org_name", normalised_org)
        .withColumn("account_name", clean_string(col("accountName")))
        .withColumn("org_map_matched", org_map_matched(col("site_name"), col("normalised_org_name")))
        .withColumn("sentinelone_account_id", clean_string(col("accountId")))
        .withColumn("sentinelone_account_name", clean_string(col("accountName")))
        .withColumn("sentinelone_agent_version", clean_string(col("agentVersion")))
        .withColumn("sentinelone_machine_type", clean_string(col("machineType")))
        .withColumn("sentinelone_mitigation_mode", clean_string(col("mitigationMode")))
        .withColumn(
            "sentinelone_mitigation_mode_suspicious", clean_string(col("mitigationModeSuspicious"))
        )
        .withColumn("sentinelone_scan_status", clean_string(col("scanStatus")))
        .withColumn("sentinelone_operational_state", clean_string(col("operationalState")))
        .withColumn(
            "sentinelone_operational_state_expiration", clean_string(col("operationalStateExpiration"))
        )
        .withColumn("sentinelone_is_decommissioned", col("isDecommissioned"))
        .withColumn("sentinelone_firewall_enabled", col("firewallEnabled"))
        .withColumn("sentinelone_network_quarantine_enabled", col("networkQuarantineEnabled"))
        .withColumn("sentinelone_ranger_status", clean_string(col("rangerStatus")))
        .withColumn("sentinelone_network_status", clean_string(col("networkStatus")))
        .withColumn("sentinelone_group_id", clean_string(col("groupId")))
        .withColumn("sentinelone_group_name", clean_string(col("groupName")))
        .withColumn("sentinelone_active_threats", col("activeThreats").cast("int"))
        .withColumn(
            "sentinelone_last_logged_in_user_name", clean_string(col("lastLoggedInUserName"))
        )
        .withColumn("sentinelone_serial_number", clean_string(col("serialNumber")))
        .withColumn("sentinelone_ip_addresses", ip_addresses)
        .withColumn("sentinelone_mac_addresses", sentinelone_mac_addresses)
        .withColumn("sentinelone_gateway_mac_addresses", sentinelone_gateway_mac_addresses)
        .withColumn(
            "sentinelone_active_protection",
            _array_sort_nullable(clean_string_array(col("activeProtection"))),
        )
        .withColumn(
            "sentinelone_missing_permissions",
            _array_sort_nullable(clean_string_array(col("missingPermissions"))),
        )
        .withColumn(
            "sentinelone_user_actions_needed",
            _array_sort_nullable(clean_string_array(col("userActionsNeeded"))),
        )
        .withColumn(
            "sentinelone_locations",
            _array_sort_nullable(clean_string_array(locations_expr)),
        )
        .withColumn("raw_json", to_json(struct([col(c) for c in clean.columns if c != "_corrupt_record"])))
        .withColumn("raw_payload", col("raw_json"))
        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("primary_ip"))),
                    col("vendor_id"),
                ),
                256,
            ),
        )
    )

    df = add_common_fields(df, SENTINEL_TOPIC, col("vendor_id"), _as_timestamp("updatedAt"))
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df
