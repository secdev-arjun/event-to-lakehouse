from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from .config import (
    AUTO_MERGE_TIER1_MIN_SCORE,
    AUTO_MERGE_TIER2_MIN_SCORE,
    MATCH_SCORES,
    PRIVATE_IP_REGEX,
    REVIEW_MIN_SCORE,
    REVIEW_ONLY_METHODS,
    TIER1_METHODS,
    TIER2_METHODS,
)


def _method_priority_expr():
    priority = {
        "serial_exact": 1,
        "primary_mac_exact": 2,
        "primary_mac_in_array": 3,
        "mac_overlap": 4,
        "hostname_org": 5,
        "ip_org": 6,
        "hostname_site": 7,
        "access_ip_org": 8,
        "hostname_os": 9,
        "ip_site": 10,
        "access_ip_site": 11,
        "ip_array_org": 12,
        "hostname_only": 90,
        "primary_ip_only": 91,
        "ip_array_only": 92,
        "virtual_mac_only": 93,
    }
    expr = F.lit(999)
    for method, rank in priority.items():
        expr = F.when(F.col("match_method") == F.lit(method), F.lit(rank)).otherwise(expr)
    return expr


def _score_to_confidence(col_score, col_method):
    return (
        F.when(col_method.isin(*TIER1_METHODS), F.lit("deterministic"))
        .when(col_method.isin(*REVIEW_ONLY_METHODS), F.lit("review"))
        .when(col_score >= F.lit(80), F.lit("high"))
        .when(col_score >= F.lit(65), F.lit("medium"))
        .when(col_score >= F.lit(REVIEW_MIN_SCORE), F.lit("review"))
        .otherwise(F.lit("low"))
    )


def _auto_merge_flag(col_score, col_method):
    tier1_auto = col_method.isin(*TIER1_METHODS) & (col_score >= F.lit(AUTO_MERGE_TIER1_MIN_SCORE))
    tier2_auto = col_method.isin(*TIER2_METHODS) & (col_score >= F.lit(AUTO_MERGE_TIER2_MIN_SCORE))
    return tier1_auto | tier2_auto


def _explode_ip_array(df: DataFrame, id_alias: str, ts_alias: str, ip_col: str, extra_cols: dict | None = None) -> DataFrame:
    extras = extra_cols or {}
    select_cols = [
        F.col("entity_id").alias(id_alias),
        F.col("source_updated_at").alias(ts_alias),
        F.explode_outer(F.col(ip_col)).alias("ip"),
    ]
    select_cols.extend(F.col(src).alias(dst) for src, dst in extras.items())
    out = df.select(*select_cols).filter(F.col("ip").isNotNull())
    for dst in extras.values():
        out = out.filter(F.col(dst).isNotNull())
    return out


def _with_entity_columns(df: DataFrame, left_source: str, right_source: str, left_id: str, right_id: str) -> DataFrame:
    def _col_for(source_name: str):
        if left_source == source_name:
            return F.col(left_id)
        if right_source == source_name:
            return F.col(right_id)
        return F.lit(None).cast("string")

    return (
        df.withColumn("rapid7_entity_id", _col_for("rapid7"))
        .withColumn("fortisiem_entity_id", _col_for("fortisiem"))
        .withColumn("sentinalone_entity_id", _col_for("sentinalone"))
    )


def _apply_ambiguity_guard(df: DataFrame, left_id: str, right_id: str) -> DataFrame:
    left_counts = (
        df.groupBy(left_id, "match_method")
        .agg(F.countDistinct(right_id).alias("_left_method_matches"))
    )
    right_counts = (
        df.groupBy(right_id, "match_method")
        .agg(F.countDistinct(left_id).alias("_right_method_matches"))
    )
    guarded = (
        df.join(left_counts, on=[left_id, "match_method"], how="left")
        .join(right_counts, on=[right_id, "match_method"], how="left")
        .withColumn(
            "ambiguity_flag",
            (F.coalesce(F.col("_left_method_matches"), F.lit(0)) > F.lit(1))
            | (F.coalesce(F.col("_right_method_matches"), F.lit(0)) > F.lit(1)),
        )
        .withColumn("auto_merge", F.when(F.col("ambiguity_flag"), F.lit(False)).otherwise(F.col("auto_merge")))
        .withColumn("match_review_flag", F.when(F.col("ambiguity_flag"), F.lit(True)).otherwise(F.col("match_review_flag")))
        .withColumn("match_confidence", F.when(F.col("ambiguity_flag"), F.lit("review")).otherwise(F.col("match_confidence")))
        .drop("_left_method_matches", "_right_method_matches")
    )
    return guarded


def _finalize_pair_candidates(
    candidates: list[DataFrame],
    left_source: str,
    right_source: str,
    left_id: str,
    right_id: str,
) -> DataFrame:
    unified = None
    for frame in candidates:
        if frame is None:
            continue
        unified = frame if unified is None else unified.unionByName(frame, allowMissingColumns=True)
    if unified is None:
        return None

    method_priority = _method_priority_expr()
    ranked = (
        unified
        .withColumn("match_score", F.col("match_score").cast("int"))
        .withColumn("_method_priority", method_priority)
    )

    w_pair = Window.partitionBy(left_id, right_id).orderBy(
        F.col("match_score").desc(),
        F.col("_method_priority").asc(),
        F.col("match_method").asc(),
    )
    best = ranked.withColumn("_rn_pair", F.row_number().over(w_pair)).filter(F.col("_rn_pair") == 1)

    agg = (
        ranked.groupBy(left_id, right_id)
        .agg(
            F.array_sort(F.array_distinct(F.flatten(F.collect_list(F.coalesce(F.col("match_keys_used"), F.array().cast("array<string>")))))).alias("match_keys_used"),
            F.array_sort(F.array_distinct(F.flatten(F.collect_list(F.coalesce(F.col("matched_mac_values"), F.array().cast("array<string>")))))).alias("matched_mac_values"),
            F.max("left_source_updated_at").alias("left_source_updated_at"),
            F.max("right_source_updated_at").alias("right_source_updated_at"),
        )
    )

    final_df = (
        best.drop("_method_priority", "_rn_pair")
        .drop("match_keys_used", "matched_mac_values", "left_source_updated_at", "right_source_updated_at")
        .join(agg, on=[left_id, right_id], how="left")
        .withColumn("match_confidence", _score_to_confidence(F.col("match_score"), F.col("match_method")))
        .withColumn("auto_merge", _auto_merge_flag(F.col("match_score"), F.col("match_method")))
        .withColumn(
            "match_review_flag",
            (~F.col("auto_merge")) & (F.col("match_score") >= F.lit(REVIEW_MIN_SCORE)),
        )
        .withColumn("left_source_system", F.lit(left_source))
        .withColumn("right_source_system", F.lit(right_source))
        .withColumnRenamed(left_id, "left_entity_id")
        .withColumnRenamed(right_id, "right_entity_id")
    )

    final_df = _apply_ambiguity_guard(final_df, "left_entity_id", "right_entity_id")
    final_df = _with_entity_columns(final_df, left_source, right_source, "left_entity_id", "right_entity_id")
    return final_df


def build_r7_fsm_pairs(r7: DataFrame, fsm: DataFrame) -> DataFrame:
    r = r7.alias("r")
    f = fsm.alias("f")

    primary_mac_exact = (
        r.join(
            f,
            (F.col("r.primary_mac_tier1").isNotNull())
            & (F.col("r.primary_mac_tier1") == F.col("f.primary_mac_tier1")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("f.entity_id").alias("fsm_id"),
            F.lit("primary_mac_exact").alias("match_method"),
            F.lit(MATCH_SCORES["primary_mac_exact"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_mac")).alias("match_keys_used"),
            F.array(F.col("r.primary_mac_tier1")).alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("f.source_updated_at").alias("right_source_updated_at"),
        )
    )

    r_mac = r.select(F.col("entity_id").alias("r7_id"), F.col("source_updated_at"), F.explode_outer("physical_mac_addresses_tier1").alias("mac")).filter(F.col("mac").isNotNull())
    f_mac = f.select(F.col("entity_id").alias("fsm_id"), F.col("source_updated_at"), F.explode_outer("physical_mac_addresses_tier1").alias("mac")).filter(F.col("mac").isNotNull())
    mac_overlap = (
        r_mac.alias("r")
        .join(f_mac.alias("f"), on="mac", how="inner")
        .groupBy("r.r7_id", "f.fsm_id")
        .agg(
            F.array_sort(F.array_distinct(F.collect_set("mac"))).alias("matched_mac_values"),
            F.max("r.source_updated_at").alias("left_source_updated_at"),
            F.max("f.source_updated_at").alias("right_source_updated_at"),
        )
        .select(
            F.col("r7_id"),
            F.col("fsm_id"),
            F.lit("mac_overlap").alias("match_method"),
            F.lit(MATCH_SCORES["mac_overlap"]).cast("int").alias("match_score"),
            F.array(F.lit("mac_addresses")).alias("match_keys_used"),
            F.col("matched_mac_values"),
            F.col("left_source_updated_at"),
            F.col("right_source_updated_at"),
        )
    )

    ip_org = (
        r.join(
            f,
            (F.col("r.primary_ip_norm").isNotNull())
            & (F.col("r.primary_ip_norm") == F.col("f.access_ip_norm"))
            & (F.col("r.org_name_norm").isNotNull())
            & (F.col("r.org_name_norm") == F.col("f.org_name_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("f.entity_id").alias("fsm_id"),
            F.lit("ip_org").alias("match_method"),
            F.lit(MATCH_SCORES["ip_org"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_ip"), F.lit("access_ip"), F.lit("normalised_org_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("f.source_updated_at").alias("right_source_updated_at"),
        )
    )

    r_ip_org = _explode_ip_array(r7, "r7_id", "left_source_updated_at", "ip_addresses_norm", {"org_name_norm": "org_norm"})
    f_ip_org = _explode_ip_array(fsm, "fsm_id", "right_source_updated_at", "ip_addresses_norm", {"org_name_norm": "org_norm"})
    ip_array_org = (
        r_ip_org.alias("r")
        .join(f_ip_org.alias("f"), on=["ip", "org_norm"], how="inner")
        .groupBy("r.r7_id", "f.fsm_id")
        .agg(
            F.max("r.left_source_updated_at").alias("left_source_updated_at"),
            F.max("f.right_source_updated_at").alias("right_source_updated_at"),
        )
        .select(
            F.col("r7_id"),
            F.col("fsm_id"),
            F.lit("ip_array_org").alias("match_method"),
            F.lit(MATCH_SCORES["ip_array_org"]).cast("int").alias("match_score"),
            F.array(F.lit("ip_addresses"), F.lit("normalised_org_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("left_source_updated_at"),
            F.col("right_source_updated_at"),
        )
    )

    ip_site = (
        r.join(
            f,
            (F.col("r.primary_ip_norm").isNotNull())
            & (F.col("r.primary_ip_norm") == F.col("f.access_ip_norm"))
            & (F.col("r.site_name_norm").isNotNull())
            & (F.col("r.site_name_norm") == F.col("f.site_name_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("f.entity_id").alias("fsm_id"),
            F.lit("ip_site").alias("match_method"),
            F.lit(MATCH_SCORES["ip_site"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_ip"), F.lit("access_ip"), F.lit("site_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("f.source_updated_at").alias("right_source_updated_at"),
        )
    )

    r_ip_any = _explode_ip_array(r7, "r7_id", "left_source_updated_at", "ip_addresses_norm")
    f_ip_any = _explode_ip_array(fsm, "fsm_id", "right_source_updated_at", "ip_addresses_norm")
    ip_array_only = (
        r_ip_any.alias("r")
        .join(f_ip_any.alias("f"), on="ip", how="inner")
        .groupBy("r.r7_id", "f.fsm_id")
        .agg(
            F.max("r.left_source_updated_at").alias("left_source_updated_at"),
            F.max("f.right_source_updated_at").alias("right_source_updated_at"),
        )
        .select(
            F.col("r7_id"),
            F.col("fsm_id"),
            F.lit("ip_array_only").alias("match_method"),
            F.lit(MATCH_SCORES["ip_array_only"]).cast("int").alias("match_score"),
            F.array(F.lit("ip_addresses")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("left_source_updated_at"),
            F.col("right_source_updated_at"),
        )
    )

    return _finalize_pair_candidates(
        [primary_mac_exact, mac_overlap, ip_org, ip_array_org, ip_site, ip_array_only],
        left_source="rapid7",
        right_source="fortisiem",
        left_id="r7_id",
        right_id="fsm_id",
    )


def build_r7_s1_pairs(r7: DataFrame, s1: DataFrame) -> DataFrame:
    r = r7.alias("r")
    s = s1.alias("s")

    r_mac = r.select(F.col("entity_id").alias("r7_id"), F.col("source_updated_at"), F.explode_outer("physical_mac_addresses_tier1").alias("mac")).filter(F.col("mac").isNotNull())
    s_mac = s.select(F.col("entity_id").alias("s1_id"), F.col("source_updated_at"), F.explode_outer("physical_mac_addresses_tier1").alias("mac")).filter(F.col("mac").isNotNull())
    mac_overlap = (
        r_mac.alias("r")
        .join(s_mac.alias("s"), on="mac", how="inner")
        .groupBy("r.r7_id", "s.s1_id")
        .agg(
            F.array_sort(F.array_distinct(F.collect_set("mac"))).alias("matched_mac_values"),
            F.max("r.source_updated_at").alias("left_source_updated_at"),
            F.max("s.source_updated_at").alias("right_source_updated_at"),
        )
        .select(
            F.col("r7_id"),
            F.col("s1_id"),
            F.lit("mac_overlap").alias("match_method"),
            F.lit(MATCH_SCORES["mac_overlap"]).cast("int").alias("match_score"),
            F.array(F.lit("mac_addresses")).alias("match_keys_used"),
            F.col("matched_mac_values"),
            F.col("left_source_updated_at"),
            F.col("right_source_updated_at"),
        )
    )

    primary_mac_in_array = (
        r.join(
            s,
            (F.col("r.primary_mac_tier1").isNotNull())
            & F.array_contains(F.col("s.physical_mac_addresses_tier1"), F.col("r.primary_mac_tier1")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("primary_mac_in_array").alias("match_method"),
            F.lit(MATCH_SCORES["primary_mac_in_array"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_mac"), F.lit("mac_addresses")).alias("match_keys_used"),
            F.array(F.col("r.primary_mac_tier1")).alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    hostname_org = (
        r.join(
            s,
            (F.col("r.primary_hostname_norm").isNotNull())
            & (F.col("r.primary_hostname_norm") == F.col("s.primary_hostname_norm"))
            & (F.col("r.org_name_norm").isNotNull())
            & (F.col("r.org_name_norm") == F.col("s.org_name_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("hostname_org").alias("match_method"),
            F.lit(MATCH_SCORES["hostname_org"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_hostname"), F.lit("normalised_org_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    hostname_site = (
        r.join(
            s,
            (F.col("r.primary_hostname_norm").isNotNull())
            & (F.col("r.primary_hostname_norm") == F.col("s.primary_hostname_norm"))
            & (F.col("r.site_name_norm").isNotNull())
            & (F.col("r.site_name_norm") == F.col("s.site_name_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("hostname_site").alias("match_method"),
            F.lit(MATCH_SCORES["hostname_site"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_hostname"), F.lit("site_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    hostname_os = (
        r.join(
            s,
            (F.col("r.primary_hostname_norm").isNotNull())
            & (F.col("r.primary_hostname_norm") == F.col("s.primary_hostname_norm"))
            & (F.col("r.os_family_norm").isNotNull())
            & (F.col("r.os_family_norm") == F.col("s.os_family_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("hostname_os").alias("match_method"),
            F.lit(MATCH_SCORES["hostname_os"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_hostname"), F.lit("os_family")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    ip_org = (
        r.join(
            s,
            (F.col("r.primary_ip_norm").isNotNull())
            & (F.col("r.primary_ip_norm") == F.col("s.primary_ip_norm"))
            & (F.col("r.primary_ip_norm").rlike(PRIVATE_IP_REGEX))
            & (F.col("r.org_name_norm").isNotNull())
            & (F.col("r.org_name_norm") == F.col("s.org_name_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("ip_org").alias("match_method"),
            F.lit(75).cast("int").alias("match_score"),
            F.array(F.lit("primary_ip"), F.lit("normalised_org_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    ip_site = (
        r.join(
            s,
            (F.col("r.primary_ip_norm").isNotNull())
            & (F.col("r.primary_ip_norm") == F.col("s.primary_ip_norm"))
            & (F.col("r.site_name_norm").isNotNull())
            & (F.col("r.site_name_norm") == F.col("s.site_name_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("ip_site").alias("match_method"),
            F.lit(65).cast("int").alias("match_score"),
            F.array(F.lit("primary_ip"), F.lit("site_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    hostname_only = (
        r.join(
            s,
            (F.col("r.primary_hostname_norm").isNotNull())
            & (F.col("r.primary_hostname_norm") == F.col("s.primary_hostname_norm")),
            "inner",
        )
        .select(
            F.col("r.entity_id").alias("r7_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("hostname_only").alias("match_method"),
            F.lit(MATCH_SCORES["hostname_only"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_hostname")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("r.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    return _finalize_pair_candidates(
        [mac_overlap, primary_mac_in_array, hostname_org, hostname_site, hostname_os, ip_org, ip_site, hostname_only],
        left_source="rapid7",
        right_source="sentinalone",
        left_id="r7_id",
        right_id="s1_id",
    )


def build_fsm_s1_pairs(fsm: DataFrame, s1: DataFrame) -> DataFrame:
    f = fsm.alias("f")
    s = s1.alias("s")

    serial_exact = (
        f.join(
            s,
            (F.col("f.serial_norm").isNotNull())
            & (F.col("f.serial_norm") == F.col("s.serial_norm")),
            "inner",
        )
        .select(
            F.col("f.entity_id").alias("fsm_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("serial_exact").alias("match_method"),
            F.lit(MATCH_SCORES["serial_exact"]).cast("int").alias("match_score"),
            F.array(F.lit("serial_number")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("f.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    primary_mac_in_array = (
        f.join(
            s,
            (F.col("f.primary_mac_tier1").isNotNull())
            & F.array_contains(F.col("s.physical_mac_addresses_tier1"), F.col("f.primary_mac_tier1")),
            "inner",
        )
        .select(
            F.col("f.entity_id").alias("fsm_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("primary_mac_in_array").alias("match_method"),
            F.lit(MATCH_SCORES["primary_mac_in_array"]).cast("int").alias("match_score"),
            F.array(F.lit("primary_mac"), F.lit("mac_addresses")).alias("match_keys_used"),
            F.array(F.col("f.primary_mac_tier1")).alias("matched_mac_values"),
            F.col("f.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    f_mac = f.select(F.col("entity_id").alias("fsm_id"), F.col("source_updated_at"), F.explode_outer("physical_mac_addresses_tier1").alias("mac")).filter(F.col("mac").isNotNull())
    s_mac = s.select(F.col("entity_id").alias("s1_id"), F.col("source_updated_at"), F.explode_outer("physical_mac_addresses_tier1").alias("mac")).filter(F.col("mac").isNotNull())
    mac_overlap = (
        f_mac.alias("f")
        .join(s_mac.alias("s"), on="mac", how="inner")
        .groupBy("f.fsm_id", "s.s1_id")
        .agg(
            F.array_sort(F.array_distinct(F.collect_set("mac"))).alias("matched_mac_values"),
            F.max("f.source_updated_at").alias("left_source_updated_at"),
            F.max("s.source_updated_at").alias("right_source_updated_at"),
        )
        .select(
            F.col("fsm_id"),
            F.col("s1_id"),
            F.lit("mac_overlap").alias("match_method"),
            F.lit(MATCH_SCORES["mac_overlap"]).cast("int").alias("match_score"),
            F.array(F.lit("mac_addresses")).alias("match_keys_used"),
            F.col("matched_mac_values"),
            F.col("left_source_updated_at"),
            F.col("right_source_updated_at"),
        )
    )

    access_ip_org = (
        f.join(
            s,
            (F.col("f.access_ip_norm").isNotNull())
            & (F.col("f.access_ip_norm") == F.col("s.access_ip_norm"))
            & (F.col("f.org_name_norm").isNotNull())
            & (F.col("f.org_name_norm") == F.col("s.org_name_norm")),
            "inner",
        )
        .select(
            F.col("f.entity_id").alias("fsm_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("access_ip_org").alias("match_method"),
            F.lit(MATCH_SCORES["access_ip_org"]).cast("int").alias("match_score"),
            F.array(F.lit("access_ip"), F.lit("normalised_org_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("f.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    f_ip_org = _explode_ip_array(fsm, "fsm_id", "left_source_updated_at", "ip_addresses_norm", {"org_name_norm": "org_norm"})
    s_ip_org = _explode_ip_array(s1, "s1_id", "right_source_updated_at", "ip_addresses_norm", {"org_name_norm": "org_norm"})
    ip_array_org = (
        f_ip_org.alias("f")
        .join(s_ip_org.alias("s"), on=["ip", "org_norm"], how="inner")
        .groupBy("f.fsm_id", "s.s1_id")
        .agg(
            F.max("f.left_source_updated_at").alias("left_source_updated_at"),
            F.max("s.right_source_updated_at").alias("right_source_updated_at"),
        )
        .select(
            F.col("fsm_id"),
            F.col("s1_id"),
            F.lit("ip_array_org").alias("match_method"),
            F.lit(MATCH_SCORES["ip_array_org"]).cast("int").alias("match_score"),
            F.array(F.lit("ip_addresses"), F.lit("normalised_org_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("left_source_updated_at"),
            F.col("right_source_updated_at"),
        )
    )

    access_ip_site = (
        f.join(
            s,
            (F.col("f.access_ip_norm").isNotNull())
            & (F.col("f.access_ip_norm") == F.col("s.access_ip_norm"))
            & (F.col("f.site_name_norm").isNotNull())
            & (F.col("f.site_name_norm") == F.col("s.site_name_norm")),
            "inner",
        )
        .select(
            F.col("f.entity_id").alias("fsm_id"),
            F.col("s.entity_id").alias("s1_id"),
            F.lit("access_ip_site").alias("match_method"),
            F.lit(MATCH_SCORES["access_ip_site"]).cast("int").alias("match_score"),
            F.array(F.lit("access_ip"), F.lit("site_name")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("f.source_updated_at").alias("left_source_updated_at"),
            F.col("s.source_updated_at").alias("right_source_updated_at"),
        )
    )

    f_ip_any = _explode_ip_array(fsm, "fsm_id", "left_source_updated_at", "ip_addresses_norm")
    s_ip_any = _explode_ip_array(s1, "s1_id", "right_source_updated_at", "ip_addresses_norm")
    ip_array_only = (
        f_ip_any.alias("f")
        .join(s_ip_any.alias("s"), on="ip", how="inner")
        .groupBy("f.fsm_id", "s.s1_id")
        .agg(
            F.max("f.left_source_updated_at").alias("left_source_updated_at"),
            F.max("s.right_source_updated_at").alias("right_source_updated_at"),
        )
        .select(
            F.col("fsm_id"),
            F.col("s1_id"),
            F.lit("ip_array_only").alias("match_method"),
            F.lit(MATCH_SCORES["ip_array_only"]).cast("int").alias("match_score"),
            F.array(F.lit("ip_addresses")).alias("match_keys_used"),
            F.array().cast("array<string>").alias("matched_mac_values"),
            F.col("left_source_updated_at"),
            F.col("right_source_updated_at"),
        )
    )

    return _finalize_pair_candidates(
        [serial_exact, primary_mac_in_array, mac_overlap, access_ip_org, ip_array_org, access_ip_site, ip_array_only],
        left_source="fortisiem",
        right_source="sentinalone",
        left_id="fsm_id",
        right_id="s1_id",
    )
