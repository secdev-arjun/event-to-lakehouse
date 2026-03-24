from pyspark.sql import DataFrame, functions as F


def _risk_band_expr(score_col):
    return (
        F.when(score_col.isNull(), F.lit("unknown"))
        .when((score_col >= F.lit(9.0)) | (score_col >= F.lit(900.0)), F.lit("critical"))
        .when((score_col >= F.lit(7.0)) | (score_col >= F.lit(700.0)), F.lit("high"))
        .when((score_col >= F.lit(4.0)) | (score_col >= F.lit(400.0)), F.lit("medium"))
        .otherwise(F.lit("low"))
    )


def _vuln_band_expr(critical_col, severe_col, moderate_col, total_col):
    return (
        F.when(critical_col > F.lit(0), F.lit("critical_vulns"))
        .when(severe_col > F.lit(0), F.lit("severe_vulns"))
        .when(moderate_col > F.lit(0), F.lit("moderate_only"))
        .when(total_col == F.lit(0), F.lit("clean"))
        .otherwise(F.lit("unknown"))
    )


def add_derived_fields(df: DataFrame) -> DataFrame:
    source_presence_summary = (
        F.when(F.col("seen_in_rapid7") & F.col("seen_in_fortisiem") & F.col("seen_in_sentinalone"), F.lit("All 3 sources"))
        .when(F.col("seen_in_rapid7") & F.col("seen_in_fortisiem"), F.lit("2 sources: R7+FSM"))
        .when(F.col("seen_in_rapid7") & F.col("seen_in_sentinalone"), F.lit("2 sources: R7+S1"))
        .when(F.col("seen_in_fortisiem") & F.col("seen_in_sentinalone"), F.lit("2 sources: FSM+S1"))
        .when(F.col("seen_in_rapid7"), F.lit("Single source: R7"))
        .when(F.col("seen_in_fortisiem"), F.lit("Single source: FSM"))
        .when(F.col("seen_in_sentinalone"), F.lit("Single source: S1"))
        .otherwise(F.lit("unknown"))
    )

    device_type_category = (
        F.when(
            F.lower(F.coalesce(F.col("machine_type"), F.lit(""))).rlike("server")
            | F.lower(F.coalesce(F.col("asset_type"), F.lit(""))).rlike("server"),
            F.lit("server"),
        )
        .when(
            F.lower(F.coalesce(F.col("machine_type"), F.lit(""))).rlike("desktop|laptop|endpoint|workstation")
            | F.lower(F.coalesce(F.col("asset_type"), F.lit(""))).rlike("endpoint|desktop|laptop"),
            F.lit("endpoint"),
        )
        .when(
            F.lower(F.coalesce(F.col("asset_type"), F.lit(""))).rlike("network|router|switch|firewall"),
            F.lit("network_device"),
        )
        .otherwise(F.lit("unknown"))
    )

    posture_status = (
        F.when(F.lower(F.coalesce(F.col("operational_state"), F.lit(""))).rlike("infect"), F.lit("infected"))
        .when(F.col("posture_network_quarantine_enabled") == F.lit(True), F.lit("quarantined"))
        .when(F.col("posture_active_threats") > F.lit(0), F.lit("threat_present"))
        .when(F.col("posture_is_active").isNotNull(), F.lit("clean"))
        .otherwise(F.lit("unknown"))
    )

    match_confidence_band = (
        F.when(F.col("source_count") == F.lit(1), F.lit("singleton"))
        .when(F.col("match_review_flag"), F.lit("review"))
        .when(F.col("match_confidence") == F.lit("deterministic"), F.lit("deterministic"))
        .when(F.col("match_confidence") == F.lit("high"), F.lit("high"))
        .when(F.col("match_confidence") == F.lit("medium"), F.lit("medium"))
        .otherwise(F.lit("review"))
    )

    with_days = (
        df.withColumn("days_since_r7_seen", F.when(F.col("r7_last_seen").isNull(), F.lit(None).cast("int")).otherwise(F.datediff(F.current_date(), F.to_date(F.col("r7_last_seen")))))
        .withColumn("days_since_fsm_seen", F.when(F.col("fsm_last_seen").isNull(), F.lit(None).cast("int")).otherwise(F.datediff(F.current_date(), F.to_date(F.col("fsm_last_seen")))))
        .withColumn("days_since_s1_seen", F.when(F.col("s1_last_seen").isNull(), F.lit(None).cast("int")).otherwise(F.datediff(F.current_date(), F.to_date(F.col("s1_last_seen")))))
    )

    freshest_days = F.when(F.col("last_seen_at").isNull(), F.lit(None).cast("int")).otherwise(
        F.datediff(F.current_date(), F.to_date(F.col("last_seen_at")))
    )

    return (
        with_days.withColumn("source_presence_summary", source_presence_summary)
        .withColumn("device_type_category", device_type_category)
        .withColumn("posture_status", posture_status)
        .withColumn("risk_band", _risk_band_expr(F.col("risk_score")))
        .withColumn(
            "vuln_band",
            _vuln_band_expr(
                F.coalesce(F.col("vuln_critical"), F.lit(0)),
                F.coalesce(F.col("vuln_severe"), F.lit(0)),
                F.coalesce(F.col("vuln_moderate"), F.lit(0)),
                F.coalesce(F.col("vuln_total"), F.lit(None).cast("int")),
            ),
        )
        .withColumn("match_confidence_band", match_confidence_band)
        .withColumn(
            "freshness_status",
            F.when(freshest_days.isNull(), F.lit("unknown"))
            .when(freshest_days <= F.lit(7), F.lit("fresh"))
            .when(freshest_days <= F.lit(30), F.lit("stale"))
            .otherwise(F.lit("aged")),
        )
    )
