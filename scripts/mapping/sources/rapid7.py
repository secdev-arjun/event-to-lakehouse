from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_json, struct, concat_ws, lower, trim, sha2
from pyspark.sql.types import ArrayType, StructType, StringType

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
    normalize_org_name,
    org_map_matched,
    filter_matching_ip_addresses,
)

RAPID7_TOPIC = "rapid7.assets.raw"


def _array_string_field(
    df,
    field_name: str,
    nested_key: str | None = None,
    allow_raw_string_fallback: bool = False,
):
    field = next((f for f in df.schema.fields if f.name == field_name), None)
    if field is None or not isinstance(field.dataType, ArrayType):
        return lit(None).cast("array<string>")

    array_col = col(field_name)
    if not nested_key:
        return clean_string_array(array_col.cast("array<string>"))

    element_type = field.dataType.elementType
    if isinstance(element_type, StructType):
        field_names = {f.name for f in element_type.fields}
        if nested_key in field_names:
            nested_values = F.transform(array_col, lambda x: x.getField(nested_key))
            return clean_string_array(nested_values)
        return lit(None).cast("array<string>")

    if isinstance(element_type, StringType):
        extracted_from_json = clean_string_array(
            F.transform(array_col, lambda x: F.get_json_object(x, f"$.{nested_key}"))
        )
        if allow_raw_string_fallback:
            raw_values = clean_string_array(array_col)
            return F.when(
                extracted_from_json.isNotNull() & (F.size(extracted_from_json) > 0),
                extracted_from_json,
            ).otherwise(raw_values)
        return extracted_from_json

    as_json = F.to_json(array_col)
    as_structs = F.from_json(as_json, f"array<struct<{nested_key}:string>>")
    return clean_string_array(F.transform(as_structs, lambda x: x.getField(nested_key)))


def _site_id_key(col_expr):
    return clean_string(col_expr.cast("string"))


def normalize_rapid7(df, rapid7_site_df=None):
    rapid7_clean = drop_corrupt_if_present(df)

    if rapid7_site_df is not None and "id" in rapid7_site_df.columns:
        site_clean = drop_corrupt_if_present(rapid7_site_df)
        site_enrichment = (
            site_clean.withColumn("_site_id_join", _site_id_key(col("id")))
            .filter(col("_site_id_join").isNotNull() & (col("_site_id_join") != ""))
            .select(
                col("_site_id_join"),
                clean_string(col("name")).alias("_rapid7_site_lookup_name"),
                clean_string(col("id").cast("string")).alias("_rapid7_site_lookup_id"),
                clean_string(col("description")).alias("_rapid7_site_description"),
                clean_string(col("importance")).alias("_rapid7_site_importance"),
                clean_string(col("lastScanTime")).alias("_rapid7_site_last_scan_time"),
                col("riskScore").cast("double").alias("_rapid7_site_risk_score"),
                col("scanEngine").cast("long").alias("_rapid7_site_scan_engine"),
                clean_string(col("scanTemplate")).alias("_rapid7_site_scan_template"),
                clean_string(col("type")).alias("_rapid7_site_type"),
                col("assets").cast("long").alias("_rapid7_site_assets"),
                col("vulnerabilities.total").cast("long").alias("_rapid7_site_vuln_total"),
                col("vulnerabilities.critical").cast("long").alias("_rapid7_site_vuln_critical"),
                col("vulnerabilities.severe").cast("long").alias("_rapid7_site_vuln_severe"),
                col("vulnerabilities.moderate").cast("long").alias("_rapid7_site_vuln_moderate"),
            )
            .dropDuplicates(["_site_id_join"])
        )
        rapid7_clean = (
            rapid7_clean.withColumn("_site_id_join", _site_id_key(col("site_id")))
            .join(site_enrichment, on="_site_id_join", how="left")
            .drop("_site_id_join")
        )

    site_intermediate_defaults = {
        "_rapid7_site_lookup_name": "string",
        "_rapid7_site_lookup_id": "string",
        "_rapid7_site_description": "string",
        "_rapid7_site_importance": "string",
        "_rapid7_site_last_scan_time": "string",
        "_rapid7_site_risk_score": "double",
        "_rapid7_site_scan_engine": "long",
        "_rapid7_site_scan_template": "string",
        "_rapid7_site_type": "string",
        "_rapid7_site_assets": "long",
        "_rapid7_site_vuln_total": "long",
        "_rapid7_site_vuln_critical": "long",
        "_rapid7_site_vuln_severe": "long",
        "_rapid7_site_vuln_moderate": "long",
    }
    for col_name, dtype in site_intermediate_defaults.items():
        if col_name not in rapid7_clean.columns:
            rapid7_clean = rapid7_clean.withColumn(col_name, lit(None).cast(dtype))

    address_ips = _array_string_field(rapid7_clean, "addresses", "ip")
    address_macs = _array_string_field(rapid7_clean, "addresses", "mac")
    hostnames_from_array = _array_string_field(
        rapid7_clean,
        "hostNames",
        "name",
        allow_raw_string_fallback=True,
    )

    empty_str_array = F.array().cast("array<string>")
    primary_ip_array = clean_string_array(F.array(col("ip")))
    primary_mac_array = normalize_mac_array(F.array(col("mac")))
    primary_hostname_array = clean_string_array(F.array(col("hostName")))

    all_ips_raw = clean_string_array(
        F.array_union(
            F.coalesce(primary_ip_array, empty_str_array),
            F.coalesce(address_ips, empty_str_array),
        )
    )
    all_ips = filter_matching_ip_addresses(all_ips_raw)
    all_macs = normalize_mac_array(
        F.array_union(
            F.coalesce(primary_mac_array, empty_str_array),
            F.coalesce(normalize_mac_array(address_macs), empty_str_array),
        )
    )
    all_hostnames = clean_string_array(
        F.array_union(
            F.coalesce(primary_hostname_array, empty_str_array),
            F.coalesce(hostnames_from_array, empty_str_array),
        )
    )

    source_record_id = clean_string(col("id").cast("string"))
    source_site_ref_id = F.coalesce(
        clean_string(col("site_id").cast("string")),
        clean_string(col("_rapid7_site_lookup_id")),
    )
    canonical_site_name = F.coalesce(
        clean_string(col("_rapid7_site_lookup_name")),
        clean_string(col("site_name")),
    )
    normalised_org = normalize_org_name(canonical_site_name)

    df = (
        rapid7_clean.withColumn("source", col("source"))
        .withColumn("entity_id", col("entity_id").cast("string"))
        .withColumn("source_system", lit("rapid7"))
        .withColumn("source_record_id", source_record_id)
        .withColumn("source_natural_id", lit(None).cast("string"))
        .withColumn("source_site_ref_id", source_site_ref_id)
        .withColumn("source_display_name", lit(None).cast("string"))
        .withColumn("ingest_ts", col("ingest_ts"))
        .withColumn("asset_name", clean_string(col("hostName")))
        .withColumn("primary_hostname", clean_string(col("hostName")))
        .withColumn("hostnames", all_hostnames)
        .withColumn("host_domain", lit(None).cast("string"))
        .withColumn("primary_ip", clean_string(col("ip")))
        .withColumn("ip_addresses_raw", all_ips_raw)
        .withColumn("ip_addresses", all_ips)
        .withColumn("ipv6_addresses", lit(None).cast("array<string>"))
        .withColumn("primary_mac", normalize_mac_string(col("mac")))
        .withColumn("mac_addresses", all_macs)
        .withColumn("gateway_mac_addresses", lit(None).cast("array<string>"))
        .withColumn("virtual_mac_addresses", lit(None).cast("array<string>"))
        .withColumn("serial_number", lit(None).cast("string"))
        .withColumn("access_ip", lit(None).cast("string"))
        .withColumn("external_ip", lit(None).cast("string"))
        .withColumn("approved", lit(None).cast("boolean"))
        .withColumn("unmanaged", lit(None).cast("boolean"))
        .withColumn("tags", lit(None).cast("array<string>"))
        .withColumn("site_id", source_site_ref_id)
        .withColumn("site_name", canonical_site_name)
        .withColumn("normalised_org_name", normalised_org)
        .withColumn("account_id", lit(None).cast("string"))
        .withColumn("account_name", lit(None).cast("string"))
        .withColumn("org_map_matched", org_map_matched(col("site_name"), col("normalised_org_name")))
        .withColumn("site_description", clean_string(col("_rapid7_site_description")))
        .withColumn("site_type", clean_string(col("_rapid7_site_type")))
        .withColumn("site_importance", clean_string(col("_rapid7_site_importance")))
        .withColumn("site_assets_count", col("_rapid7_site_assets").cast("long"))
        .withColumn("site_risk_score", col("_rapid7_site_risk_score").cast("double"))
        .withColumn("site_last_scan_time", F.to_timestamp(clean_string(col("_rapid7_site_last_scan_time"))))
        .withColumn("site_scan_engine", col("_rapid7_site_scan_engine").cast("long"))
        .withColumn("site_scan_template", clean_string(col("_rapid7_site_scan_template")))
        .withColumn("site_vuln_total", col("_rapid7_site_vuln_total").cast("long"))
        .withColumn("site_vuln_critical", col("_rapid7_site_vuln_critical").cast("long"))
        .withColumn("site_vuln_severe", col("_rapid7_site_vuln_severe").cast("long"))
        .withColumn("site_vuln_moderate", col("_rapid7_site_vuln_moderate").cast("long"))
        .withColumn("device_vendor", lit(None).cast("string"))
        .withColumn("device_model", lit(None).cast("string"))
        .withColumn("device_version", lit(None).cast("string"))
        .withColumn("platform_version", lit(None).cast("string"))
        .withColumn("asset_type", clean_string(col("type")))
        .withColumn("os_name", clean_string(col("os")))
        .withColumn("os_family", clean_string(col("osFingerprint.family")))
        .withColumn("os_vendor", clean_string(col("osFingerprint.vendor")))
        .withColumn("os_product", clean_string(col("osFingerprint.product")))
        .withColumn("os_version", clean_string(F.coalesce(col("osFingerprint.cpe.version"), col("osFingerprint.version"))))
        .withColumn("os_architecture", clean_string(col("osFingerprint.architecture")))
        .withColumn("os_edition", lit(None).cast("string"))
        .withColumn("os_certainty", col("osCertainty").cast("double"))
        .withColumn("cpu_count", lit(None).cast("int"))
        .withColumn("memory_bytes", lit(None).cast("long"))
        .withColumn("system_uptime", lit(None).cast("long"))
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
        .withColumn("device_status", lit(None).cast("string"))
        .withColumn("discover_method", lit(None).cast("string"))
        .withColumn("event_log_status", lit(None).cast("string"))
        .withColumn("perf_mon_status", lit(None).cast("string"))
        .withColumn("update_method", lit(None).cast("string"))
        .withColumn("services_count", F.size(F.coalesce(col("services"), F.expr("array()"))).cast("int"))
        .withColumn("software_count", F.size(F.coalesce(col("software"), F.expr("array()"))).cast("int"))
        .withColumn("raw_json", to_json(struct([col(c) for c in rapid7_clean.columns if c != "_corrupt_record"])))
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

    df = add_common_fields(df, RAPID7_TOPIC, col("source_record_id"), lit(None).cast("timestamp"))
    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df
