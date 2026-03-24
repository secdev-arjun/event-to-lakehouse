from pyspark.sql import DataFrame, functions as F

from mapping.target import TARGET_FIELDS
from .config import GOLD_HASH_COLUMNS
from .derived_fields import add_derived_fields
from .utils import max_non_null, min_non_null, prefix_columns


def _empty_str_array():
    return F.array().cast("array<string>")


def _union_arrays(*arr_cols):
    wrapped = [F.coalesce(c.cast("array<string>"), _empty_str_array()) for c in arr_cols]
    return F.array_sort(F.array_distinct(F.flatten(F.array(*wrapped))))


def _add_missing_target_fields(df: DataFrame) -> DataFrame:
    out = df
    for field in TARGET_FIELDS:
        if field.name not in out.columns:
            out = out.withColumn(field.name, F.lit(None).cast(field.dataType))
    return out


def _presence_label(seen_s, seen_r, seen_f):
    return (
        F.when(seen_r & seen_f & seen_s, F.lit("R7_FSM_S1"))
        .when(seen_r & seen_f, F.lit("R7_FSM"))
        .when(seen_r & seen_s, F.lit("R7_S1"))
        .when(seen_f & seen_s, F.lit("FSM_S1"))
        .when(seen_r, F.lit("R7_only"))
        .when(seen_f, F.lit("FSM_only"))
        .when(seen_s, F.lit("S1_only"))
        .otherwise(F.lit("unknown"))
    )


def _source_candidates_json(*candidate_structs):
    cands = F.filter(F.array(*candidate_structs), lambda x: x.isNotNull())
    return cands, F.to_json(cands)


def _conflict_flag(cands):
    norm = F.array_distinct(F.transform(cands, lambda x: F.lower(F.trim(x["value"]))))
    norm = F.filter(norm, lambda x: x.isNotNull() & (x != ""))
    return F.size(norm) > 1


def _col_or_null(df: DataFrame, name: str, dtype: str = "string"):
    if name in df.columns:
        return F.col(name)
    return F.lit(None).cast(dtype)


def build_gold_rows(
    sentinel_df: DataFrame,
    rapid7_df: DataFrame,
    forti_df: DataFrame,
    groups_df: DataFrame,
    all_pairs_df: DataFrame,
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

    review_pairs = all_pairs_df.filter((~F.col("auto_merge")) & (F.col("match_score") >= F.lit(40)))
    r7_review = (
        review_pairs.filter(F.col("rapid7_entity_id").isNotNull())
        .groupBy("rapid7_entity_id")
        .agg(
            F.max("match_score").alias("r7_review_score"),
            F.array_sort(F.collect_set("match_method")).alias("r7_review_methods"),
        )
    )
    fsm_review = (
        review_pairs.filter(F.col("fortisiem_entity_id").isNotNull())
        .groupBy("fortisiem_entity_id")
        .agg(
            F.max("match_score").alias("fsm_review_score"),
            F.array_sort(F.collect_set("match_method")).alias("fsm_review_methods"),
        )
    )
    s1_review = (
        review_pairs.filter(F.col("sentinalone_entity_id").isNotNull())
        .groupBy("sentinalone_entity_id")
        .agg(
            F.max("match_score").alias("s1_review_score"),
            F.array_sort(F.collect_set("match_method")).alias("s1_review_methods"),
        )
    )

    grouped = (
        grouped.join(r7_review, on="rapid7_entity_id", how="left")
        .join(fsm_review, on="fortisiem_entity_id", how="left")
        .join(s1_review, on="sentinalone_entity_id", how="left")
    )

    seen_in_sentinalone = F.col("sentinalone_entity_id").isNotNull()
    seen_in_rapid7 = F.col("rapid7_entity_id").isNotNull()
    seen_in_fortisiem = F.col("fortisiem_entity_id").isNotNull()
    source_count = (
        F.when(seen_in_sentinalone, F.lit(1)).otherwise(F.lit(0))
        + F.when(seen_in_rapid7, F.lit(1)).otherwise(F.lit(0))
        + F.when(seen_in_fortisiem, F.lit(1)).otherwise(F.lit(0))
    )
    matched_sources = F.filter(
        F.array(
            F.when(seen_in_sentinalone, F.lit("sentinalone")),
            F.when(seen_in_rapid7, F.lit("rapid7")),
            F.when(seen_in_fortisiem, F.lit("fortisiem")),
        ),
        lambda x: x.isNotNull(),
    )

    presence_label = _presence_label(seen_in_sentinalone, seen_in_rapid7, seen_in_fortisiem)

    singleton_review_score = F.coalesce(F.col("r7_review_score"), F.col("fsm_review_score"), F.col("s1_review_score"))
    singleton_review_methods = _union_arrays(F.col("r7_review_methods"), F.col("fsm_review_methods"), F.col("s1_review_methods"))
    singleton_needs_review = (source_count == F.lit(1)) & singleton_review_score.isNotNull()

    match_method = F.when(singleton_needs_review, F.lit("review_candidate_only")).otherwise(F.col("match_method"))
    match_score = F.when(singleton_needs_review, singleton_review_score).otherwise(F.col("match_score"))
    match_review_flag = F.when(singleton_needs_review, F.lit(True)).otherwise(F.col("match_review_flag"))
    match_confidence = (
        F.when(singleton_needs_review, F.lit("review"))
        .when(source_count == F.lit(1), F.lit("singleton"))
        .otherwise(F.col("match_confidence"))
    )
    match_keys_used = F.when(singleton_needs_review, singleton_review_methods).otherwise(F.coalesce(F.col("match_keys_used"), _empty_str_array()))

    primary_hostname = F.coalesce(F.col("s_primary_hostname"), F.col("r_primary_hostname"))
    primary_hostname_source = (
        F.when(F.col("s_primary_hostname").isNotNull(), F.lit("sentinalone"))
        .when(F.col("r_primary_hostname").isNotNull(), F.lit("rapid7"))
        .otherwise(F.lit(None).cast("string"))
    )

    asset_name = F.coalesce(F.col("s_asset_name"), F.col("r_asset_name"), F.col("f_source_display_name"))
    asset_name_source = (
        F.when(F.col("s_asset_name").isNotNull(), F.lit("sentinalone"))
        .when(F.col("r_asset_name").isNotNull(), F.lit("rapid7"))
        .when(F.col("f_source_display_name").isNotNull(), F.lit("fortisiem"))
        .otherwise(F.lit(None).cast("string"))
    )

    primary_ip = F.coalesce(F.col("r_primary_ip"), F.col("s_primary_ip"))
    primary_ip_source = (
        F.when(F.col("r_primary_ip").isNotNull(), F.lit("rapid7"))
        .when(F.col("s_primary_ip").isNotNull(), F.lit("sentinalone"))
        .otherwise(F.lit(None).cast("string"))
    )
    access_ip = F.coalesce(F.col("f_access_ip"), F.col("s_access_ip"))
    primary_mac = F.coalesce(F.col("r_primary_mac"), F.col("f_primary_mac"))
    primary_mac_source = (
        F.when(F.col("r_primary_mac").isNotNull(), F.lit("rapid7"))
        .when(F.col("f_primary_mac").isNotNull(), F.lit("fortisiem"))
        .otherwise(F.lit(None).cast("string"))
    )

    serial_number = F.coalesce(F.col("s_serial_number"), F.col("f_serial_number"))
    serial_number_source = (
        F.when(F.col("s_serial_number").isNotNull(), F.lit("sentinalone"))
        .when(F.col("f_serial_number").isNotNull(), F.lit("fortisiem"))
        .otherwise(F.lit(None).cast("string"))
    )

    normalised_org_name = F.coalesce(F.col("r_normalised_org_name"), F.col("s_normalised_org_name"), F.col("f_normalised_org_name"))
    normalised_org_name_source = (
        F.when(F.col("r_normalised_org_name").isNotNull(), F.lit("rapid7"))
        .when(F.col("s_normalised_org_name").isNotNull(), F.lit("sentinalone"))
        .when(F.col("f_normalised_org_name").isNotNull(), F.lit("fortisiem"))
        .otherwise(F.lit(None).cast("string"))
    )

    site_name = F.coalesce(F.col("r_site_name"), F.col("s_site_name"), F.col("f_site_name"))
    site_name_source = (
        F.when(F.col("r_site_name").isNotNull(), F.lit("rapid7"))
        .when(F.col("s_site_name").isNotNull(), F.lit("sentinalone"))
        .when(F.col("f_site_name").isNotNull(), F.lit("fortisiem"))
        .otherwise(F.lit(None).cast("string"))
    )

    os_name = F.coalesce(F.col("s_os_name"), F.col("r_os_name"))
    os_name_source = (
        F.when(F.col("s_os_name").isNotNull(), F.lit("sentinalone"))
        .when(F.col("r_os_name").isNotNull(), F.lit("rapid7"))
        .otherwise(F.lit(None).cast("string"))
    )

    risk_score = F.col("r_risk_score")
    risk_score_source = F.when(F.col("r_risk_score").isNotNull(), F.lit("rapid7")).otherwise(F.lit(None).cast("string"))

    device_vendor = F.col("f_device_vendor")
    device_vendor_source = F.when(F.col("f_device_vendor").isNotNull(), F.lit("fortisiem")).otherwise(F.lit(None).cast("string"))

    all_hostnames = _union_arrays(F.col("r_hostnames"), F.col("s_hostnames"))
    all_ip_addresses = _union_arrays(F.col("r_ip_addresses"), F.col("f_ip_addresses"), F.col("s_ip_addresses"))
    all_ip_addresses_raw = _union_arrays(F.col("r_ip_addresses_raw"), F.col("f_ip_addresses_raw"), F.col("s_ip_addresses_raw"))
    all_mac_addresses = _union_arrays(F.col("r_mac_addresses"), F.col("f_mac_addresses"), F.col("s_mac_addresses"))
    all_ipv6_addresses = _union_arrays(F.col("r_ipv6_addresses"), F.col("f_ipv6_addresses"), F.col("s_ipv6_addresses"))
    gateway_mac_addresses = _union_arrays(F.col("r_gateway_mac_addresses"), F.col("f_gateway_mac_addresses"), F.col("s_gateway_mac_addresses"))
    virtual_mac_addresses = _union_arrays(F.col("r_virtual_mac_addresses"), F.col("f_virtual_mac_addresses"), F.col("s_virtual_mac_addresses"))

    r7_last_seen = F.coalesce(F.col("r_last_seen_at"), F.col("r_ingest_ts"))
    fsm_last_seen = F.coalesce(F.col("f_last_seen_at"), F.col("f_ingest_ts"))
    s1_last_seen = F.coalesce(F.col("s_last_seen_at"), F.col("s_ingest_ts"))
    most_recent_source_ts = max_non_null(
        r7_last_seen,
        fsm_last_seen,
        s1_last_seen,
        F.col("r_source_updated_at"),
        F.col("f_source_updated_at"),
        F.col("s_source_updated_at"),
    )

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

    hostname_struct_s = F.when(
        F.col("s_primary_hostname").isNotNull(),
        F.struct(F.lit("sentinalone").alias("source"), F.col("s_primary_hostname").alias("value"), s1_last_seen.alias("last_seen_at")),
    )
    hostname_struct_r = F.when(
        F.col("r_primary_hostname").isNotNull(),
        F.struct(F.lit("rapid7").alias("source"), F.col("r_primary_hostname").alias("value"), r7_last_seen.alias("last_seen_at")),
    )
    hostname_cands, hostname_candidates = _source_candidates_json(hostname_struct_s, hostname_struct_r)
    hostname_conflict = _conflict_flag(hostname_cands)

    os_struct_s = F.when(
        F.col("s_os_name").isNotNull(),
        F.struct(F.lit("sentinalone").alias("source"), F.col("s_os_name").alias("value"), s1_last_seen.alias("last_seen_at")),
    )
    os_struct_r = F.when(
        F.col("r_os_name").isNotNull(),
        F.struct(F.lit("rapid7").alias("source"), F.col("r_os_name").alias("value"), r7_last_seen.alias("last_seen_at")),
    )
    os_cands, os_candidates = _source_candidates_json(os_struct_s, os_struct_r)
    os_conflict = _conflict_flag(os_cands)

    site_struct_r = F.when(
        F.col("r_site_name").isNotNull(),
        F.struct(F.lit("rapid7").alias("source"), F.col("r_site_name").alias("value"), r7_last_seen.alias("last_seen_at")),
    )
    site_struct_s = F.when(
        F.col("s_site_name").isNotNull(),
        F.struct(F.lit("sentinalone").alias("source"), F.col("s_site_name").alias("value"), s1_last_seen.alias("last_seen_at")),
    )
    site_struct_f = F.when(
        F.col("f_site_name").isNotNull(),
        F.struct(F.lit("fortisiem").alias("source"), F.col("f_site_name").alias("value"), fsm_last_seen.alias("last_seen_at")),
    )
    site_cands, site_candidates = _source_candidates_json(site_struct_r, site_struct_s, site_struct_f)
    site_conflict = _conflict_flag(site_cands)
    has_conflicts = hostname_conflict | os_conflict | site_conflict

    merged_basis = F.array_sort(
        F.filter(
            F.array(
                F.when(F.col("rapid7_entity_id").isNotNull(), F.concat(F.lit("rapid7|"), F.col("rapid7_entity_id"))),
                F.when(F.col("fortisiem_entity_id").isNotNull(), F.concat(F.lit("fortisiem|"), F.col("fortisiem_entity_id"))),
                F.when(F.col("sentinalone_entity_id").isNotNull(), F.concat(F.lit("sentinalone|"), F.col("sentinalone_entity_id"))),
            ),
            lambda x: x.isNotNull(),
        )
    )
    merged_id = F.sha2(F.concat_ws("|", merged_basis), 256)
    singleton_basis = F.coalesce(
        F.when(F.col("rapid7_entity_id").isNotNull(), F.concat(F.lit("rapid7|"), F.col("rapid7_entity_id"))),
        F.when(F.col("fortisiem_entity_id").isNotNull(), F.concat(F.lit("fortisiem|"), F.col("fortisiem_entity_id"))),
        F.when(F.col("sentinalone_entity_id").isNotNull(), F.concat(F.lit("sentinalone|"), F.col("sentinalone_entity_id"))),
    )
    singleton_id = F.sha2(singleton_basis, 256)
    master_entity_id = F.when(source_count > F.lit(1), merged_id).otherwise(singleton_id)

    now_ts = F.current_timestamp()

    base = grouped.select(
        F.lit("gold.asset_master.v1").alias("schema_version"),
        F.lit("gold").alias("source"),
        master_entity_id.alias("entity_id"),
        F.to_json(
            F.struct(
                F.col("sentinalone_entity_id").alias("sentinalone_entity_id"),
                F.col("rapid7_entity_id").alias("rapid7_entity_id"),
                F.col("fortisiem_entity_id").alias("fortisiem_entity_id"),
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
        all_hostnames.alias("all_hostnames"),
        F.coalesce(F.col("s_host_domain"), F.col("r_host_domain"), F.col("f_host_domain")).alias("host_domain"),
        primary_ip.alias("primary_ip"),
        all_ip_addresses.alias("ip_addresses"),
        all_ip_addresses.alias("all_ip_addresses"),
        all_ip_addresses_raw.alias("ip_addresses_raw"),
        all_ip_addresses_raw.alias("all_ip_addresses_raw"),
        all_ipv6_addresses.alias("ipv6_addresses"),
        all_ipv6_addresses.alias("all_ipv6_addresses"),
        primary_mac.alias("primary_mac"),
        all_mac_addresses.alias("mac_addresses"),
        all_mac_addresses.alias("all_mac_addresses"),
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
        (F.coalesce(F.col("r_org_map_matched"), F.lit(False)) | F.coalesce(F.col("s_org_map_matched"), F.lit(False)) | F.coalesce(F.col("f_org_map_matched"), F.lit(False))).alias("org_map_matched"),
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
        F.col("f_device_model").alias("device_model"),
        F.col("f_device_version").alias("device_version"),
        F.col("f_platform_version").alias("platform_version"),
        F.col("r_asset_type").alias("asset_type"),
        os_name.alias("os_name"),
        F.coalesce(F.col("s_os_family"), F.col("r_os_family")).alias("os_family"),
        F.coalesce(F.col("r_os_vendor"), F.col("s_os_vendor"), F.col("f_os_vendor")).alias("os_vendor"),
        F.coalesce(F.col("r_os_product"), F.col("s_os_product"), F.col("f_os_product")).alias("os_product"),
        F.coalesce(F.col("s_os_version"), F.col("r_os_version")).alias("os_version"),
        F.coalesce(F.col("s_os_architecture"), F.col("r_os_architecture")).alias("os_architecture"),
        F.coalesce(F.col("f_os_edition"), F.col("s_os_edition"), F.col("r_os_edition")).alias("os_edition"),
        F.col("r_os_certainty").alias("os_certainty"),
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
        F.col("f_device_status").alias("device_status"),
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
        F.col("sentinalone_entity_id"),
        F.col("rapid7_entity_id"),
        F.col("fortisiem_entity_id"),
        seen_in_sentinalone.alias("seen_in_sentinalone"),
        seen_in_rapid7.alias("seen_in_rapid7"),
        seen_in_fortisiem.alias("seen_in_fortisiem"),
        source_count.alias("source_count"),
        presence_label.alias("source_presence_label"),
        matched_sources.alias("matched_sources"),
        F.col("r_source_record_id").alias("r7_source_record_id"),
        F.col("f_source_record_id").alias("fsm_source_record_id"),
        F.col("s_source_record_id").alias("s1_source_record_id"),
        F.col("r_source_natural_id").alias("r7_source_natural_id"),
        F.col("f_source_natural_id").alias("fsm_source_natural_id"),
        F.col("s_source_natural_id").alias("s1_source_natural_id"),
        F.col("r_payload_hash").alias("r7_payload_hash"),
        F.col("f_payload_hash").alias("fsm_payload_hash"),
        F.col("s_payload_hash").alias("s1_payload_hash"),
        r7_last_seen.alias("r7_last_seen"),
        fsm_last_seen.alias("fsm_last_seen"),
        s1_last_seen.alias("s1_last_seen"),
        most_recent_source_ts.alias("most_recent_source_ts"),
        asset_name_source.alias("asset_name_source"),
        primary_hostname_source.alias("primary_hostname_source"),
        primary_ip_source.alias("primary_ip_source"),
        primary_mac_source.alias("primary_mac_source"),
        serial_number_source.alias("serial_number_source"),
        normalised_org_name_source.alias("normalised_org_name_source"),
        site_name_source.alias("site_name_source"),
        os_name_source.alias("os_name_source"),
        risk_score_source.alias("risk_score_source"),
        device_vendor_source.alias("device_vendor_source"),
        match_method.alias("match_method"),
        match_confidence.alias("match_confidence"),
        match_score.cast("int").alias("match_score"),
        match_review_flag.alias("match_review_flag"),
        match_keys_used.alias("match_keys_used"),
        F.col("ambiguity_flag").alias("ambiguity_flag"),
        F.col("transitive_link_flag").alias("transitive_link_flag"),
        has_conflicts.alias("has_conflicts"),
        hostname_conflict.alias("hostname_conflict"),
        os_conflict.alias("os_conflict"),
        site_conflict.alias("site_conflict"),
        hostname_candidates.alias("hostname_candidates"),
        os_candidates.alias("os_candidates"),
        site_candidates.alias("site_candidates"),
        now_ts.alias("gold_created_at"),
        now_ts.alias("gold_updated_at"),
    )

    base = add_derived_fields(base)
    base = _add_missing_target_fields(base)

    hash_cols = [F.col(c) for c in GOLD_HASH_COLUMNS if c in base.columns]
    base = base.withColumn("gold_payload_hash", F.sha2(F.to_json(F.struct(*hash_cols)), 256))
    base = base.withColumn("payload_hash", F.col("gold_payload_hash"))
    return base
