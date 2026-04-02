from pyspark.sql import DataFrame, functions as F

from mapping.target import TARGET_FIELDS
from .config import GOLD_HASH_COLUMNS
from .utils import max_non_null, min_non_null, prefix_columns


def _empty_str_array():
    return F.array().cast("array<string>")


def _union_arrays(*arr_cols):
    wrapped = [F.coalesce(col.cast("array<string>"), _empty_str_array()) for col in arr_cols]
    return F.array_sort(F.array_distinct(F.flatten(F.array(*wrapped))))


def _add_missing_target_fields(df: DataFrame) -> DataFrame:
    out = df
    for field in TARGET_FIELDS:
        if field.name not in out.columns:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))
    return out


def _col_or_null(df: DataFrame, name: str, dtype: str = "string"):
    if name in df.columns:
        return F.col(name)
    return F.lit(None).cast(dtype)


def build_gold_rows(
    sentinel_df: DataFrame,
    rapid7_df: DataFrame,
    forti_df: DataFrame,
    groups_df: DataFrame,
    accepted_edges_df: DataFrame,
) -> DataFrame:
    s = prefix_columns(sentinel_df, "s_")
    r = prefix_columns(rapid7_df, "r_")
    f = prefix_columns(forti_df, "f_")

    grouped = (
        groups_df.alias("g")
        .join(r.alias("r"), F.col("g.rapid7_entity_id") == F.col("r.r_entity_id"), "left")
        .join(f.alias("f"), F.col("g.fortisiem_entity_id") == F.col("f.f_entity_id"), "left")
        .join(s.alias("s"), F.col("g.sentinalone_entity_id") == F.col("s.s_entity_id"), "left")
    )

    seen_in_sentinalone = F.col("g.seen_in_sentinalone")
    seen_in_rapid7 = F.col("g.seen_in_rapid7")
    seen_in_fortisiem = F.col("g.seen_in_fortisiem")
    source_count = F.col("g.source_count")
    matched_sources = F.col("g.matched_sources")
    match_rule_summary = F.coalesce(F.col("g.match_rule_summary"), _empty_str_array())
    min_match_rule_rank = F.col("g.min_match_rule_rank").cast("int")

    primary_hostname = F.coalesce(F.col("s_primary_hostname"), F.col("r_primary_hostname"))

    asset_name = F.coalesce(F.col("s_asset_name"), F.col("r_asset_name"), F.col("f_source_display_name"))

    primary_ip = F.coalesce(F.col("r_primary_ip"), F.col("s_primary_ip"))
    access_ip = F.coalesce(F.col("f_access_ip"), F.col("s_access_ip"))
    primary_mac = F.coalesce(F.col("r_primary_mac"), F.col("f_primary_mac"), F.col("s_primary_mac"))

    serial_number = F.coalesce(F.col("s_serial_number"), F.col("f_serial_number"), F.col("r_serial_number"))

    normalised_org_name = F.coalesce(
        F.col("r_normalised_org_name"),
        F.col("s_normalised_org_name"),
        F.col("f_normalised_org_name"),
    )

    site_name = F.coalesce(F.col("r_site_name"), F.col("s_site_name"), F.col("f_site_name"))

    os_name = F.coalesce(F.col("s_os_name"), F.col("r_os_name"), F.col("f_os_name"))

    risk_score = F.col("r_risk_score")

    device_vendor = _col_or_null(grouped, "f_device_vendor")

    all_hostnames = _union_arrays(F.col("r_hostnames"), F.col("s_hostnames"), F.col("f_hostnames"))
    all_ip_addresses = _union_arrays(F.col("r_ip_addresses"), F.col("f_ip_addresses"), F.col("s_ip_addresses"))
    all_ip_addresses_raw = _union_arrays(F.col("r_ip_addresses_raw"), F.col("f_ip_addresses_raw"), F.col("s_ip_addresses_raw"))
    all_mac_addresses = _union_arrays(F.col("r_mac_addresses"), F.col("f_mac_addresses"), F.col("s_mac_addresses"))
    all_ipv6_addresses = _union_arrays(F.col("r_ipv6_addresses"), F.col("f_ipv6_addresses"), F.col("s_ipv6_addresses"))
    gateway_mac_addresses = _union_arrays(
        F.col("r_gateway_mac_addresses"),
        F.col("f_gateway_mac_addresses"),
        F.col("s_gateway_mac_addresses"),
    )
    virtual_mac_addresses = _union_arrays(
        F.col("r_virtual_mac_addresses"),
        F.col("f_virtual_mac_addresses"),
        F.col("s_virtual_mac_addresses"),
    )

    r7_last_seen = F.coalesce(F.col("r_last_seen_at"), F.col("r_ingest_ts"))
    fsm_last_seen = F.coalesce(F.col("f_last_seen_at"), F.col("f_ingest_ts"))
    s1_last_seen = F.coalesce(F.col("s_last_seen_at"), F.col("s_ingest_ts"))

    first_seen_at = min_non_null(
        F.col("r_first_seen_at"),
        F.col("f_first_seen_at"),
        F.col("s_first_seen_at"),
        F.col("r_ingest_ts"),
        F.col("f_ingest_ts"),
        F.col("s_ingest_ts"),
    )
    last_seen_at = max_non_null(r7_last_seen, fsm_last_seen, s1_last_seen)
    source_updated_at = max_non_null(F.col("r_source_updated_at"), F.col("f_source_updated_at"), F.col("s_source_updated_at"))

    match_method = F.concat_ws("+", match_rule_summary)
    master_entity_id = F.col("g.component_id")
    now_ts = F.current_timestamp()

    base = grouped.select(
        F.lit("gold.asset_master.v2").alias("schema_version"),
        F.lit("gold").alias("source"),
        master_entity_id.alias("entity_id"),
        F.to_json(
            F.struct(
                F.col("g.component_id").alias("component_id"),
                F.col("g.sentinalone_entity_id").alias("sentinalone_entity_id"),
                F.col("g.rapid7_entity_id").alias("rapid7_entity_id"),
                F.col("g.fortisiem_entity_id").alias("fortisiem_entity_id"),
            )
        ).alias("entity_key_str"),
        F.lit(None).cast("string").alias("payload_hash"),
        F.lit(None).cast("string").alias("topic_name"),
        F.lit(None).cast("string").alias("vendor_id"),
        now_ts.alias("ingest_ts"),
        first_seen_at.alias("first_seen_at"),
        last_seen_at.alias("last_seen_at"),
        source_updated_at.alias("source_updated_at"),
        F.lit(None).cast("timestamp").alias("event_time"),
        master_entity_id.alias("asset_uid"),
        F.lit("gold").alias("source_system"),
        F.lit(None).cast("string").alias("source_record_id"),
        F.lit(None).cast("string").alias("source_natural_id"),
        F.coalesce(F.col("r_source_site_ref_id"), F.col("s_source_site_ref_id"), F.col("f_source_site_ref_id")).alias("source_site_ref_id"),
        F.coalesce(F.col("s_source_display_name"), F.col("f_source_display_name"), F.col("r_source_display_name")).alias("source_display_name"),
        asset_name.alias("asset_name"),
        primary_hostname.alias("primary_hostname"),
        all_hostnames.alias("hostnames"),
        F.coalesce(F.col("s_host_domain"), F.col("r_host_domain"), F.col("f_host_domain")).alias("host_domain"),
        primary_ip.alias("primary_ip"),
        all_ip_addresses.alias("ip_addresses"),
        all_ip_addresses_raw.alias("ip_addresses_raw"),
        all_ipv6_addresses.alias("ipv6_addresses"),
        primary_mac.alias("primary_mac"),
        all_mac_addresses.alias("mac_addresses"),
        gateway_mac_addresses.alias("gateway_mac_addresses"),
        virtual_mac_addresses.alias("virtual_mac_addresses"),
        serial_number.alias("serial_number"),
        access_ip.alias("access_ip"),
        F.coalesce(F.col("s_external_ip"), F.col("r_external_ip"), F.col("f_external_ip")).alias("external_ip"),
        F.col("f_approved").alias("approved"),
        F.col("f_unmanaged").alias("unmanaged"),
        _union_arrays(F.col("s_tags"), F.col("r_tags"), F.col("f_tags")).alias("tags"),
        F.coalesce(F.col("r_site_id"), F.col("s_site_id"), F.col("f_site_id")).alias("site_id"),
        site_name.alias("site_name"),
        normalised_org_name.alias("normalised_org_name"),
        F.col("s_account_id").alias("account_id"),
        F.col("s_account_name").alias("account_name"),
        (
            F.coalesce(F.col("r_org_map_matched"), F.lit(False))
            | F.coalesce(F.col("s_org_map_matched"), F.lit(False))
            | F.coalesce(F.col("f_org_map_matched"), F.lit(False))
        ).alias("org_map_matched"),
        F.coalesce(F.col("r_site_description"), F.col("s_site_description"), F.col("f_site_description")).alias("site_description"),
        F.coalesce(F.col("r_site_type"), F.col("s_site_type"), F.col("f_site_type")).alias("site_type"),
        F.coalesce(F.col("r_site_importance"), F.col("s_site_importance"), F.col("f_site_importance")).alias("site_importance"),
        F.coalesce(F.col("r_site_assets_count"), F.col("s_site_assets_count"), F.col("f_site_assets_count")).alias("site_assets_count"),
        F.coalesce(F.col("r_site_risk_score"), F.col("s_site_risk_score"), F.col("f_site_risk_score")).alias("site_risk_score"),
        F.coalesce(F.col("r_site_last_scan_time"), F.col("s_site_last_scan_time"), F.col("f_site_last_scan_time")).alias("site_last_scan_time"),
        F.coalesce(F.col("r_site_scan_engine"), F.col("s_site_scan_engine"), F.col("f_site_scan_engine")).alias("site_scan_engine"),
        F.coalesce(F.col("r_site_scan_template"), F.col("s_site_scan_template"), F.col("f_site_scan_template")).alias("site_scan_template"),
        F.coalesce(F.col("r_site_vuln_total"), F.col("s_site_vuln_total"), F.col("f_site_vuln_total")).alias("site_vuln_total"),
        F.coalesce(F.col("r_site_vuln_critical"), F.col("s_site_vuln_critical"), F.col("f_site_vuln_critical")).alias("site_vuln_critical"),
        F.coalesce(F.col("r_site_vuln_severe"), F.col("s_site_vuln_severe"), F.col("f_site_vuln_severe")).alias("site_vuln_severe"),
        F.coalesce(F.col("r_site_vuln_moderate"), F.col("s_site_vuln_moderate"), F.col("f_site_vuln_moderate")).alias("site_vuln_moderate"),
        device_vendor.alias("device_vendor"),
        _col_or_null(grouped, "f_device_model").alias("device_model"),
        _col_or_null(grouped, "f_device_version").alias("device_version"),
        F.col("f_platform_version").alias("platform_version"),
        F.coalesce(F.col("r_asset_type"), F.col("f_asset_type"), F.col("s_asset_type")).alias("asset_type"),
        os_name.alias("os_name"),
        F.coalesce(F.col("s_os_family"), F.col("r_os_family"), F.col("f_os_family")).alias("os_family"),
        F.coalesce(F.col("r_os_vendor"), F.col("s_os_vendor"), F.col("f_os_vendor")).alias("os_vendor"),
        F.coalesce(F.col("r_os_product"), F.col("s_os_product"), F.col("f_os_product")).alias("os_product"),
        F.coalesce(F.col("s_os_version"), F.col("r_os_version"), F.col("f_os_version")).alias("os_version"),
        F.coalesce(F.col("s_os_architecture"), F.col("r_os_architecture"), F.col("f_os_architecture")).alias("os_architecture"),
        F.coalesce(F.col("f_os_edition"), F.col("s_os_edition"), F.col("r_os_edition")).alias("os_edition"),
        F.coalesce(F.col("r_os_certainty"), F.col("s_os_certainty"), F.col("f_os_certainty")).alias("os_certainty"),
        F.coalesce(F.col("s_cpu_count"), F.col("r_cpu_count"), F.col("f_cpu_count")).alias("cpu_count"),
        F.coalesce(F.col("s_memory_bytes"), F.col("r_memory_bytes"), F.col("f_memory_bytes")).alias("memory_bytes"),
        F.coalesce(F.col("f_system_uptime"), F.col("s_system_uptime"), F.col("r_system_uptime")).alias("system_uptime"),
        F.col("r_assessed_for_policies").alias("assessed_for_policies"),
        F.col("r_assessed_for_vulnerabilities").alias("assessed_for_vulnerabilities"),
        risk_score.alias("risk_score"),
        F.col("r_raw_risk_score").alias("raw_risk_score"),
        F.col("r_vuln_total").alias("vuln_total"),
        F.col("r_vuln_critical").alias("vuln_critical"),
        F.col("r_vuln_severe").alias("vuln_severe"),
        F.col("r_vuln_moderate").alias("vuln_moderate"),
        F.col("r_vuln_exploits").alias("vuln_exploits"),
        F.col("r_vuln_malware_kits").alias("vuln_malware_kits"),
        F.col("s_posture_is_active").alias("posture_is_active"),
        F.col("s_posture_firewall_enabled").alias("posture_firewall_enabled"),
        F.col("s_posture_network_quarantine_enabled").alias("posture_network_quarantine_enabled"),
        F.col("s_posture_active_threats").alias("posture_active_threats"),
        F.col("s_scan_status").alias("scan_status"),
        F.col("s_operational_state").alias("operational_state"),
        F.col("s_operational_state_expiration").alias("operational_state_expiration"),
        F.col("s_network_status").alias("network_status"),
        F.col("s_ranger_status").alias("ranger_status"),
        F.col("s_mitigation_mode").alias("mitigation_mode"),
        F.col("s_mitigation_mode_suspicious").alias("mitigation_mode_suspicious"),
        F.col("s_machine_type").alias("machine_type"),
        F.col("s_group_id").alias("group_id"),
        F.col("s_group_name").alias("group_name"),
        F.col("s_last_logged_in_user_name").alias("last_logged_in_user_name"),
        _union_arrays(F.col("s_active_protection_modes"), F.col("r_active_protection_modes"), F.col("f_active_protection_modes")).alias("active_protection_modes"),
        _union_arrays(F.col("s_missing_permissions"), F.col("r_missing_permissions"), F.col("f_missing_permissions")).alias("missing_permissions"),
        _union_arrays(F.col("s_user_actions_needed"), F.col("r_user_actions_needed"), F.col("f_user_actions_needed")).alias("user_actions_needed"),
        _union_arrays(F.col("s_location_names"), F.col("r_location_names"), F.col("f_location_names")).alias("location_names"),
        _col_or_null(grouped, "f_device_status").alias("device_status"),
        _col_or_null(grouped, "f_discover_method").alias("discover_method"),
        F.col("f_event_log_status").alias("event_log_status"),
        F.col("f_perf_mon_status").alias("perf_mon_status"),
        F.col("f_update_method").alias("update_method"),
        F.col("r_services_count").alias("services_count"),
        F.col("r_software_count").alias("software_count"),
        F.coalesce(F.col("s_raw_payload"), F.col("r_raw_payload"), F.col("f_raw_payload")).alias("raw_payload"),
        F.coalesce(F.col("s_raw_json"), F.col("r_raw_json"), F.col("f_raw_json")).alias("raw_json"),
        master_entity_id.alias("master_entity_id"),
        F.lit(1).cast("int").alias("master_version"),
        master_entity_id.alias("gold_asset_id"),
        F.col("g.sentinalone_entity_id").alias("sentinalone_entity_id"),
        F.col("g.rapid7_entity_id").alias("rapid7_entity_id"),
        F.col("g.fortisiem_entity_id").alias("fortisiem_entity_id"),
        seen_in_sentinalone.alias("seen_in_sentinalone"),
        seen_in_rapid7.alias("seen_in_rapid7"),
        seen_in_fortisiem.alias("seen_in_fortisiem"),
        source_count.alias("source_count"),
        matched_sources.alias("matched_sources"),
        F.col("g.match_rule_summary").alias("match_rule_summary"),
        min_match_rule_rank.alias("min_match_rule_rank"),
        F.col("g.component_id").alias("component_id"),
        r7_last_seen.alias("r7_last_seen"),
        fsm_last_seen.alias("fsm_last_seen"),
        s1_last_seen.alias("s1_last_seen"),
        match_method.alias("match_method"),
        F.lit("deterministic").alias("match_confidence"),
        F.lit(None).cast("int").alias("match_score"),
        F.lit(False).alias("match_review_flag"),
        match_rule_summary.alias("match_keys_used"),
        F.lit(False).alias("ambiguity_flag"),
        F.lit(False).alias("transitive_link_flag"),
        now_ts.alias("gold_created_at"),
        now_ts.alias("gold_updated_at"),
    )
    base = _add_missing_target_fields(base)

    hash_cols = [F.col(name) for name in GOLD_HASH_COLUMNS if name in base.columns]
    base = base.withColumn("gold_payload_hash", F.sha2(F.to_json(F.struct(*hash_cols)), 256))
    base = base.withColumn("payload_hash", F.col("gold_payload_hash"))
    return base
