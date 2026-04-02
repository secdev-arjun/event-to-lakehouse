from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, lower, trim, sha2

from mapping.target import (
    add_common_fields,
    ensure_columns,
    add_payload_hash,
    drop_corrupt_if_present,
    TARGET_FIELDS,
    clean_string,
    clean_string_array,
    normalize_mac_string,
    normalize_mac_array,
    split_fileshare_site_and_org,
    org_map_matched,
    filter_matching_ip_addresses,
)

FILESHARE_TOPIC = "fileshare.assets.raw"


def _split_csv_strings(col_expr):
    cleaned = clean_string(col_expr)
    return F.when(
        cleaned.isNull(),
        lit(None).cast("array<string>"),
    ).otherwise(
        clean_string_array(F.split(cleaned, r"\s*,\s*"))
    )


def _array_count_nullable(arr_col):
    return F.when(arr_col.isNull(), lit(None).cast("int")).otherwise(F.size(arr_col).cast("int"))


def normalize_fileshare(df):
    clean = drop_corrupt_if_present(df)

    source_record_id = clean_string(col("sl_no"))
    primary_hostname = clean_string(col("hostname"))
    primary_ip = clean_string(col("ip_address"))
    primary_mac = normalize_mac_string(col("mac_address"))
    site_name_raw = clean_string(col("location"))
    org_name_raw = clean_string(col("organization_name"))
    mapped_site_name, normalised_org = split_fileshare_site_and_org(site_name_raw, org_name_raw)

    ip_addresses_raw = clean_string_array(F.array(primary_ip))
    ip_addresses = filter_matching_ip_addresses(ip_addresses_raw)
    mac_addresses = normalize_mac_array(F.array(primary_mac))
    hostnames = clean_string_array(F.array(primary_hostname))

    applications_arr = _split_csv_strings(col("applications"))
    database_arr = _split_csv_strings(col("database"))
    services_arr = _split_csv_strings(col("services_enabled"))
    software_arr = clean_string_array(
        F.array_union(
            F.coalesce(applications_arr, F.array().cast("array<string>")),
            F.coalesce(database_arr, F.array().cast("array<string>")),
        )
    )

    location_names = clean_string_array(
        F.array(
            clean_string(col("location")),
            clean_string(col("zone")),
        )
    )

    if "ingest_ts" in clean.columns:
        ingest_ts_expr = col("ingest_ts")
    elif "_ingest_ts" in clean.columns:
        ingest_ts_expr = col("_ingest_ts")
    else:
        ingest_ts_expr = lit(None).cast("timestamp")

    df = (
        clean.withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
        .withColumn("source_system", lit("fileshare"))
        .withColumn("source_record_id", source_record_id)
        .withColumn("source_natural_id", lit(None).cast("string"))
        .withColumn("source_site_ref_id", clean_string(col("zone")))
        .withColumn("source_display_name", primary_hostname)
        .withColumn("ingest_ts", ingest_ts_expr.cast("timestamp"))
        .withColumn("asset_name", primary_hostname)
        .withColumn("primary_hostname", primary_hostname)
        .withColumn("hostnames", hostnames)
        .withColumn("host_domain", lit(None).cast("string"))
        .withColumn("primary_ip", primary_ip)
        .withColumn("ip_addresses_raw", ip_addresses_raw)
        .withColumn("ip_addresses", ip_addresses)
        .withColumn("ipv6_addresses", lit(None).cast("array<string>"))
        .withColumn("primary_mac", primary_mac)
        .withColumn("mac_addresses", mac_addresses)
        .withColumn("gateway_mac_addresses", lit(None).cast("array<string>"))
        .withColumn("virtual_mac_addresses", lit(None).cast("array<string>"))
        .withColumn("serial_number", source_record_id)
        .withColumn("access_ip", primary_ip)
        .withColumn("external_ip", lit(None).cast("string"))
        .withColumn("approved", lit(None).cast("boolean"))
        .withColumn("unmanaged", lit(None).cast("boolean"))
        .withColumn("tags", lit(None).cast("array<string>"))
        .withColumn("site_id", clean_string(col("zone")))
        .withColumn("site_name", mapped_site_name)
        .withColumn("normalised_org_name", normalised_org)
        .withColumn("account_id", lit(None).cast("string"))
        .withColumn("account_name", lit(None).cast("string"))
        .withColumn("org_map_matched", org_map_matched(org_name_raw, col("normalised_org_name")))
        .withColumn("site_description", clean_string(col("notes")))
        .withColumn("site_type", lit(None).cast("string"))
        .withColumn("site_importance", clean_string(col("priority_business_impact")))
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
        .withColumn("asset_type", clean_string(col("device_type")))
        .withColumn("os_name", clean_string(col("operating_system")))
        .withColumn("os_family", clean_string(col("os_type")))
        .withColumn("os_vendor", lit(None).cast("string"))
        .withColumn("os_product", lit(None).cast("string"))
        .withColumn("os_version", lit(None).cast("string"))
        .withColumn("os_architecture", lit(None).cast("string"))
        .withColumn("os_edition", lit(None).cast("string"))
        .withColumn("os_certainty", lit(None).cast("double"))
        .withColumn("cpu_count", lit(None).cast("int"))
        .withColumn("memory_bytes", lit(None).cast("long"))
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
        .withColumn("posture_is_active", lit(None).cast("boolean"))
        .withColumn("posture_firewall_enabled", lit(None).cast("boolean"))
        .withColumn("posture_network_quarantine_enabled", lit(None).cast("boolean"))
        .withColumn("posture_active_threats", lit(None).cast("int"))
        .withColumn("scan_status", lit(None).cast("string"))
        .withColumn("operational_state", clean_string(col("status")))
        .withColumn("operational_state_expiration", lit(None).cast("string"))
        .withColumn("network_status", lit(None).cast("string"))
        .withColumn("ranger_status", lit(None).cast("string"))
        .withColumn("mitigation_mode", lit(None).cast("string"))
        .withColumn("mitigation_mode_suspicious", lit(None).cast("string"))
        .withColumn("machine_type", clean_string(col("role_function")))
        .withColumn("group_id", lit(None).cast("string"))
        .withColumn("group_name", lit(None).cast("string"))
        .withColumn("last_logged_in_user_name", clean_string(col("asset_owner_name")))
        .withColumn("active_protection_modes", lit(None).cast("array<string>"))
        .withColumn("missing_permissions", lit(None).cast("array<string>"))
        .withColumn("user_actions_needed", lit(None).cast("array<string>"))
        .withColumn("location_names", location_names)
        .withColumn("discover_method", lit(None).cast("string"))
        .withColumn("event_log_status", lit(None).cast("string"))
        .withColumn("perf_mon_status", lit(None).cast("string"))
        .withColumn("update_method", lit(None).cast("string"))
        .withColumn("services_count", _array_count_nullable(services_arr))
        .withColumn("software_count", _array_count_nullable(software_arr))
        .withColumn("raw_json", to_json(struct([col(c) for c in clean.columns if c != "_corrupt_record"])))
        .withColumn("raw_payload", col("raw_json"))
        .withColumn(
            "asset_uid",
            sha2(
                concat_ws(
                    "|",
                    lower(trim(col("primary_hostname"))),
                    lower(trim(col("primary_ip"))),
                    lower(trim(col("source_record_id"))),
                ),
                256,
            ),
        )
    )

    df = add_common_fields(
        df,
        FILESHARE_TOPIC,
        F.coalesce(col("source_record_id"), col("entity_id").cast("string")),
        lit(None).cast("timestamp"),
    )
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df
