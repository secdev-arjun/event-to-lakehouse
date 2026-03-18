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
    normalize_mac_array,
    normalize_org_name,
    org_map_matched,
    first_array_value,
    filter_matching_ip_addresses,
    valid_hardware_serial,
)

FORTI_TOPIC = "fortisiem.devices.raw"


def _forti_interface_exprs(df):
    if "interfaces" not in df.columns:
        none_arr = lit(None).cast("array<string>")
        return {
            "ipv4": none_arr,
            "ipv6": none_arr,
            "mac_physical": none_arr,
            "mac_virtual": none_arr,
            "primary_up": none_arr,
            "primary_relaxed": none_arr,
        }

    iface = "coalesce(interfaces.networkinterface, array())"
    primary_up = f"""
        transform(
          array_sort(
            transform(
              filter(
                {iface},
                x -> coalesce(x.macIsVirtual, false) = false
                     AND coalesce(x.isWAN, false) = false
                     AND lower(coalesce(x.operStatus, '')) = 'up'
                     AND x.macAddr IS NOT NULL
                     AND trim(x.macAddr) <> ''
              ),
              x -> named_struct(
                'sort_key', coalesce(x.snmpIndex, cast(9223372036854775807 as bigint)),
                'mac', x.macAddr
              )
            )
          ),
          x -> x.mac
        )
    """
    primary_relaxed = f"""
        transform(
          array_sort(
            transform(
              filter(
                {iface},
                x -> coalesce(x.macIsVirtual, false) = false
                     AND coalesce(x.isWAN, false) = false
                     AND x.macAddr IS NOT NULL
                     AND trim(x.macAddr) <> ''
              ),
              x -> named_struct(
                'sort_key', coalesce(x.snmpIndex, cast(9223372036854775807 as bigint)),
                'mac', x.macAddr
              )
            )
          ),
          x -> x.mac
        )
    """

    return {
        "ipv4": F.expr(f"transform({iface}, x -> x.ipv4Addr)"),
        "ipv6": F.expr(f"transform({iface}, x -> x.ipv6Addr)"),
        "mac_physical": F.expr(
            f"transform(filter({iface}, x -> coalesce(x.macIsVirtual, false) = false), x -> x.macAddr)"
        ),
        "mac_virtual": F.expr(
            f"transform(filter({iface}, x -> coalesce(x.macIsVirtual, false) = true), x -> x.macAddr)"
        ),
        "primary_up": F.expr(primary_up),
        "primary_relaxed": F.expr(primary_relaxed),
    }



def normalize_fortisiem(df):
    forti_clean = drop_corrupt_if_present(df)
    forti_id_expr = clean_string(
        F.coalesce(
            nested_col_if_exists(forti_clean, "_id.$oid"),
            col_if_exists(forti_clean, "id"),
            col_if_exists(forti_clean, "naturalId"),
        ).cast("string")
    )

    iface_exprs = _forti_interface_exprs(forti_clean)

    empty_str_array = F.array().cast("array<string>")
    access_ip_array = clean_string_array(F.array(col("accessIp")))
    iface_ipv4_array = clean_string_array(iface_exprs["ipv4"])
    ip_addresses_raw = clean_string_array(
        F.array_union(
            F.coalesce(access_ip_array, empty_str_array),
            F.coalesce(iface_ipv4_array, empty_str_array),
        )
    )
    ip_addresses = filter_matching_ip_addresses(ip_addresses_raw)
    ipv6_addresses = clean_string_array(iface_exprs["ipv6"])

    mac_addresses = normalize_mac_array(iface_exprs["mac_physical"])
    virtual_mac_addresses = normalize_mac_array(iface_exprs["mac_virtual"])
    preferred_primary_macs = normalize_mac_array(iface_exprs["primary_up"])
    fallback_primary_macs = normalize_mac_array(iface_exprs["primary_relaxed"])
    primary_mac = F.coalesce(
        first_array_value(preferred_primary_macs),
        first_array_value(fallback_primary_macs),
        lit(None).cast("string"),
    )

    source_site_ref_id = clean_string(nested_col_if_exists(forti_clean, "organization.attr_id").cast("string"))
    site_name_raw = clean_string(nested_col_if_exists(forti_clean, "organization.attr_name").cast("string"))
    normalised_org = normalize_org_name(site_name_raw)

    source_natural_id = clean_string(col("naturalId"))

    df = (
        forti_clean.withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
        .withColumn("source_system", lit("fortisiem"))
        .withColumn("source_record_id", forti_id_expr)
        .withColumn("source_natural_id", source_natural_id)
        .withColumn("source_site_ref_id", source_site_ref_id)
        .withColumn("source_display_name", clean_string(col("name")))
        .withColumn("ingest_ts", col("ingest_ts"))
        .withColumn("asset_name", clean_string(col("name")))
        .withColumn("primary_hostname", lit(None).cast("string"))
        .withColumn("hostnames", lit(None).cast("array<string>"))
        .withColumn("host_domain", lit(None).cast("string"))
        .withColumn("primary_ip", lit(None).cast("string"))
        .withColumn("ip_addresses_raw", ip_addresses_raw)
        .withColumn("ip_addresses", ip_addresses)
        .withColumn("ipv6_addresses", ipv6_addresses)
        .withColumn("primary_mac", primary_mac)
        .withColumn("mac_addresses", mac_addresses)
        .withColumn("gateway_mac_addresses", lit(None).cast("array<string>"))
        .withColumn("virtual_mac_addresses", virtual_mac_addresses)
        .withColumn("serial_number", valid_hardware_serial(col("hwSerialNum")))
        .withColumn("access_ip", clean_string(col("accessIp")))
        .withColumn("external_ip", lit(None).cast("string"))
        .withColumn("approved", col("approved"))
        .withColumn("unmanaged", col("unmanaged"))
        .withColumn("tags", lit(None).cast(ArrayType(StringType())))
        .withColumn("site_id", source_site_ref_id)
        .withColumn("site_name", site_name_raw)
        .withColumn("normalised_org_name", normalised_org)
        .withColumn("account_id", lit(None).cast("string"))
        .withColumn("account_name", lit(None).cast("string"))
        .withColumn("org_map_matched", org_map_matched(col("site_name"), col("normalised_org_name")))
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
        .withColumn("device_vendor", clean_string(col("deviceType.vendor")))
        .withColumn("device_model", clean_string(col("deviceType.model")))
        .withColumn("device_version", clean_string(col("deviceType.version")))
        .withColumn("platform_version", clean_string(col("version")))
        .withColumn("asset_type", lit(None).cast("string"))
        .withColumn("os_name", lit(None).cast("string"))
        .withColumn("os_family", lit(None).cast("string"))
        .withColumn("os_vendor", lit(None).cast("string"))
        .withColumn("os_product", lit(None).cast("string"))
        .withColumn("os_version", lit(None).cast("string"))
        .withColumn("os_architecture", lit(None).cast("string"))
        .withColumn("os_edition", clean_string(col("osEdition")))
        .withColumn("os_certainty", lit(None).cast("double"))
        .withColumn("cpu_count", lit(None).cast("int"))
        .withColumn("memory_bytes", lit(None).cast("long"))
        .withColumn("system_uptime", col("systemUpTime").cast("long"))
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
        .withColumn("posture_is_active", lit(None).cast("boolean"))
        .withColumn("posture_firewall_enabled", lit(None).cast("boolean"))
        .withColumn("posture_network_quarantine_enabled", lit(None).cast("boolean"))
        .withColumn("posture_active_threats", lit(None).cast("int"))
        .withColumn("scan_status", lit(None).cast("string"))
        .withColumn("operational_state", lit(None).cast("string"))
        .withColumn("operational_state_expiration", lit(None).cast("string"))
        .withColumn("network_status", lit(None).cast("string"))
        .withColumn("ranger_status", lit(None).cast("string"))
        .withColumn("mitigation_mode", lit(None).cast("string"))
        .withColumn("mitigation_mode_suspicious", lit(None).cast("string"))
        .withColumn("machine_type", lit(None).cast("string"))
        .withColumn("group_id", lit(None).cast("string"))
        .withColumn("group_name", lit(None).cast("string"))
        .withColumn("last_logged_in_user_name", lit(None).cast("string"))
        .withColumn("active_protection_modes", lit(None).cast("array<string>"))
        .withColumn("missing_permissions", lit(None).cast("array<string>"))
        .withColumn("user_actions_needed", lit(None).cast("array<string>"))
        .withColumn("location_names", lit(None).cast("array<string>"))
        .withColumn("device_status", clean_string(col("deviceStatus")))
        .withColumn("discover_method", clean_string(col("discoverMethod")))
        .withColumn("event_log_status", clean_string(col("eventLogStatus")))
        .withColumn("perf_mon_status", clean_string(col("perfMonStatus")))
        .withColumn("update_method", clean_string(col("updateMethod")))
        .withColumn("services_count", lit(None).cast("int"))
        .withColumn("software_count", lit(None).cast("int"))
        .withColumn("raw_json", to_json(struct([col(c) for c in forti_clean.columns if c != "_corrupt_record"])))
        .withColumn("raw_payload", col("raw_json"))
        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("source_display_name"))),
                    lower(trim(col("access_ip"))),
                    col("source_record_id"),
                ),
                256,
            ),
        )
    )

    df = add_common_fields(
        df,
        FORTI_TOPIC,
        F.coalesce(col("source_natural_id"), col("source_record_id")),
        lit(None).cast("timestamp"),
    )
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df
