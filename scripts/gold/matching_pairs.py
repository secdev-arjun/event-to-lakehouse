from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from .config import MATCH_RULES, MatchRuleDefinition, SOURCE_FSM, SOURCE_R7, SOURCE_S1


MATCH_OUTPUT_SCHEMA = StructType(
    [
        StructField("record_scope", StringType(), True),
        StructField("source_pair", StringType(), True),
        StructField("rule_name", StringType(), True),
        StructField("rule_rank", IntegerType(), True),
        StructField("rule_status", StringType(), True),
        StructField("rule_status_note", StringType(), True),
        StructField("match_status", StringType(), True),
        StructField("review_reason", StringType(), True),
        StructField("match_key", StringType(), True),
        StructField("key_columns_used", ArrayType(StringType()), True),
        StructField("match_rule_description", StringType(), True),
        StructField("left_source_system", StringType(), True),
        StructField("right_source_system", StringType(), True),
        StructField("left_entity_id", StringType(), True),
        StructField("right_entity_id", StringType(), True),
        StructField("left_source_record_id", StringType(), True),
        StructField("right_source_record_id", StringType(), True),
        StructField("left_source_natural_id", StringType(), True),
        StructField("right_source_natural_id", StringType(), True),
        StructField("rapid7_entity_id", StringType(), True),
        StructField("fortisiem_entity_id", StringType(), True),
        StructField("sentinalone_entity_id", StringType(), True),
        StructField("left_duplicate_count", LongType(), True),
        StructField("right_duplicate_count", LongType(), True),
        StructField("left_semantic_ambiguity", BooleanType(), True),
        StructField("right_semantic_ambiguity", BooleanType(), True),
        StructField("left_preferred_by_site", BooleanType(), True),
        StructField("right_preferred_by_site", BooleanType(), True),
        StructField("left_pair_count", LongType(), True),
        StructField("right_pair_count", LongType(), True),
        StructField("left_candidate_entity_ids", ArrayType(StringType()), True),
        StructField("right_candidate_entity_ids", ArrayType(StringType()), True),
        StructField("left_freshness_ts", TimestampType(), True),
        StructField("right_freshness_ts", TimestampType(), True),
        StructField("auto_accepted", BooleanType(), True),
    ]
)

METRIC_OUTPUT_SCHEMA = StructType(
    [
        StructField("source_pair", StringType(), True),
        StructField("rule_name", StringType(), True),
        StructField("rule_rank", IntegerType(), True),
        StructField("rule_status", StringType(), True),
        StructField("rule_status_note", StringType(), True),
        StructField("left_candidate_rows", LongType(), True),
        StructField("right_candidate_rows", LongType(), True),
        StructField("left_duplicate_keys", LongType(), True),
        StructField("right_duplicate_keys", LongType(), True),
        StructField("left_rows_preferred_by_site", LongType(), True),
        StructField("right_rows_preferred_by_site", LongType(), True),
        StructField("auto_matches_count", LongType(), True),
        StructField("ambiguous_count", LongType(), True),
        StructField("unmatched_remaining_count", LongType(), True),
    ]
)


@dataclass
class RuleExecutionResult:
    accepted_edges: DataFrame
    review_rows: DataFrame
    metrics: DataFrame
    left_residue: DataFrame
    right_residue: DataFrame


def _materialize(df: DataFrame) -> DataFrame:
    try:
        return df.localCheckpoint(eager=True)
    except Exception:
        cached = df.cache()
        cached.count()
        return cached


def _empty_match_output(df: DataFrame) -> DataFrame:
    return df.sparkSession.createDataFrame([], MATCH_OUTPUT_SCHEMA)


def _empty_metric_output(df: DataFrame) -> DataFrame:
    return df.sparkSession.createDataFrame([], METRIC_OUTPUT_SCHEMA)


def _pair_name(left_source: str, right_source: str) -> str:
    return f"{left_source}__{right_source}"


def _source_entity_column(source_name: str, left_source: str, right_source: str):
    if left_source == source_name:
        return F.col("left_entity_id")
    if right_source == source_name:
        return F.col("right_entity_id")
    return F.lit(None).cast("string")


def build_rule_definitions(source_pair: tuple[str, str] | None = None) -> list[MatchRuleDefinition]:
    rules = sorted(MATCH_RULES, key=lambda rule: (rule.rule_rank, rule.rule_name))
    if source_pair is None:
        return rules
    return [rule for rule in rules if source_pair in rule.applicable_pairs]


def _required_columns(rule_def: MatchRuleDefinition) -> set[str]:
    required = {
        "entity_id",
        "source_system",
        "source_record_id",
        "source_natural_id",
        "site_present_flag",
        "evidence_completeness_score",
        "freshness_ts",
    }
    for key_part in rule_def.key_parts:
        if rule_def.explode_alias and key_part == rule_def.explode_alias:
            continue
        required.add(key_part)
    if rule_def.explode_array_column:
        required.add(rule_def.explode_array_column)
    return required


def _build_rule_rows(df: DataFrame, rule_def: MatchRuleDefinition) -> DataFrame:
    stage = df
    if rule_def.explode_array_column and rule_def.explode_alias:
        stage = stage.withColumn(rule_def.explode_alias, F.explode_outer(F.col(rule_def.explode_array_column)))

    filters = None
    for column_name in rule_def.key_parts:
        column_filter = F.col(column_name).isNotNull()
        filters = column_filter if filters is None else (filters & column_filter)

    if filters is not None:
        stage = stage.filter(filters)

    key_struct = F.struct(*[F.col(name).alias(name) for name in rule_def.key_parts])
    return stage.withColumn("match_key", F.to_json(key_struct))


def rank_source_candidates_for_rule(df: DataFrame, rule_def: MatchRuleDefinition) -> DataFrame:
    stage = _build_rule_rows(df, rule_def)
    key_window = Window.partitionBy("match_key")
    semantic_order = [
        # Business rule: prefer the richer duplicate when site is populated.
        F.col("site_present_flag").desc(),
        F.col("evidence_completeness_score").desc(),
        F.col("freshness_ts").desc_nulls_last(),
    ]
    stable_order = semantic_order + [
        F.col("entity_id").asc_nulls_last(),
        F.col("source_record_id").asc_nulls_last(),
        F.col("source_natural_id").asc_nulls_last(),
    ]

    ranked = (
        stage.withColumn("semantic_rank", F.dense_rank().over(Window.partitionBy("match_key").orderBy(*semantic_order)))
        .withColumn("stable_rank", F.row_number().over(Window.partitionBy("match_key").orderBy(*stable_order)))
    )

    # Source-internal uniqueness is evaluated per rule key before any cross-source join happens.
    key_stats = stage.groupBy("match_key").agg(
        F.count("*").alias("duplicate_count"),
        F.array_sort(F.collect_set("entity_id")).alias("candidate_entity_ids"),
        F.min("site_present_flag").alias("min_site_present_flag"),
    )
    top_rank_counts = ranked.filter(F.col("semantic_rank") == F.lit(1)).groupBy("match_key").agg(
        F.count("*").alias("top_semantic_tie_count")
    )

    winners = (
        ranked.filter(F.col("stable_rank") == F.lit(1))
        .join(key_stats, on="match_key", how="left")
        .join(top_rank_counts, on="match_key", how="left")
        .withColumn("semantic_ambiguity", F.coalesce(F.col("top_semantic_tie_count"), F.lit(0)) > F.lit(1))
        .withColumn(
            "preferred_by_site",
            (F.col("duplicate_count") > F.lit(1))
            & (F.col("site_present_flag") == F.lit(1))
            & (F.col("min_site_present_flag") == F.lit(0))
            & (~F.col("semantic_ambiguity")),
        )
        .drop("semantic_rank", "stable_rank", "top_semantic_tie_count", "min_site_present_flag")
    )
    return _materialize(winners)


def qualify_source_keys(df: DataFrame, rule_def: MatchRuleDefinition) -> DataFrame:
    return rank_source_candidates_for_rule(df, rule_def)


def _make_metric_row(
    df: DataFrame,
    *,
    source_pair: str,
    rule_name: str,
    rule_rank: int,
    rule_status: str,
    rule_status_note: str | None,
    left_candidate_rows: int,
    right_candidate_rows: int,
    left_duplicate_keys: int,
    right_duplicate_keys: int,
    left_rows_preferred_by_site: int,
    right_rows_preferred_by_site: int,
    auto_matches_count: int,
    ambiguous_count: int,
    unmatched_remaining_count: int,
) -> DataFrame:
    spark = df.sparkSession
    row = [
        (
            source_pair,
            rule_name,
            rule_rank,
            rule_status,
            rule_status_note,
            left_candidate_rows,
            right_candidate_rows,
            left_duplicate_keys,
            right_duplicate_keys,
            left_rows_preferred_by_site,
            right_rows_preferred_by_site,
            auto_matches_count,
            ambiguous_count,
            unmatched_remaining_count,
        )
    ]
    return spark.createDataFrame(row, schema=METRIC_OUTPUT_SCHEMA)


def _missing_columns(df: DataFrame, rule_def: MatchRuleDefinition) -> list[str]:
    return sorted(name for name in _required_columns(rule_def) if name not in df.columns)


def _count_distinct_rows(df: DataFrame, column_name: str) -> int:
    return df.select(column_name).distinct().count()


def _count_distinct_keys(df: DataFrame) -> int:
    return df.select("match_key").distinct().count()


def _reason_expr(rule_def: MatchRuleDefinition):
    return F.concat_ws(
        "|",
        F.filter(
            F.array(
                F.when(F.lit(not rule_def.auto_accept), F.lit("review_only_rule")),
                F.when(F.col("left_semantic_ambiguity"), F.lit("left_source_ambiguous")),
                F.when(F.col("right_semantic_ambiguity"), F.lit("right_source_ambiguous")),
                F.when(F.col("left_pair_count") > F.lit(1), F.lit("left_entity_multi_match")),
                F.when(F.col("right_pair_count") > F.lit(1), F.lit("right_entity_multi_match")),
            ),
            lambda x: x.isNotNull(),
        ),
    )


def _skipped_rule_outputs(
    left_df: DataFrame,
    right_df: DataFrame,
    left_source: str,
    right_source: str,
    rule_def: MatchRuleDefinition,
    left_missing: list[str],
    right_missing: list[str],
) -> RuleExecutionResult:
    note_parts = []
    if left_missing:
        note_parts.append(f"left_missing={','.join(left_missing)}")
    if right_missing:
        note_parts.append(f"right_missing={','.join(right_missing)}")

    metrics = _make_metric_row(
        left_df,
        source_pair=_pair_name(left_source, right_source),
        rule_name=rule_def.rule_name,
        rule_rank=rule_def.rule_rank,
        rule_status="skipped_missing_columns",
        rule_status_note="; ".join(note_parts),
        left_candidate_rows=0,
        right_candidate_rows=0,
        left_duplicate_keys=0,
        right_duplicate_keys=0,
        left_rows_preferred_by_site=0,
        right_rows_preferred_by_site=0,
        auto_matches_count=0,
        ambiguous_count=0,
        unmatched_remaining_count=left_df.count() + right_df.count(),
    )
    return RuleExecutionResult(
        accepted_edges=_empty_match_output(left_df),
        review_rows=_empty_match_output(left_df),
        metrics=metrics,
        left_residue=left_df,
        right_residue=right_df,
    )


def match_pair_by_rule(
    left_df: DataFrame,
    right_df: DataFrame,
    left_source: str,
    right_source: str,
    rule_def: MatchRuleDefinition,
) -> RuleExecutionResult:
    left_missing = _missing_columns(left_df, rule_def)
    right_missing = _missing_columns(right_df, rule_def)
    if left_missing or right_missing:
        return _skipped_rule_outputs(left_df, right_df, left_source, right_source, rule_def, left_missing, right_missing)

    left_ranked = qualify_source_keys(left_df, rule_def)
    right_ranked = qualify_source_keys(right_df, rule_def)

    left_candidate_rows = _count_distinct_rows(left_ranked, "entity_id")
    right_candidate_rows = _count_distinct_rows(right_ranked, "entity_id")
    left_duplicate_keys = _count_distinct_keys(left_ranked.filter(F.col("duplicate_count") > F.lit(1)))
    right_duplicate_keys = _count_distinct_keys(right_ranked.filter(F.col("duplicate_count") > F.lit(1)))
    left_rows_preferred_by_site = _count_distinct_rows(
        left_ranked.filter(F.col("preferred_by_site") == F.lit(True)),
        "entity_id",
    )
    right_rows_preferred_by_site = _count_distinct_rows(
        right_ranked.filter(F.col("preferred_by_site") == F.lit(True)),
        "entity_id",
    )

    joined = (
        left_ranked.alias("l")
        .join(right_ranked.alias("r"), on="match_key", how="inner")
        .select(
            F.lit("pairwise").alias("record_scope"),
            F.lit(_pair_name(left_source, right_source)).alias("source_pair"),
            F.lit(rule_def.rule_name).alias("rule_name"),
            F.lit(rule_def.rule_rank).cast("int").alias("rule_rank"),
            F.lit("review_only_rule" if not rule_def.auto_accept else "evaluated").alias("rule_status"),
            F.lit(None).cast("string").alias("rule_status_note"),
            F.col("match_key"),
            F.array(*[F.lit(name) for name in rule_def.key_columns_used]).alias("key_columns_used"),
            F.lit(rule_def.description).alias("match_rule_description"),
            F.col("l.source_system").alias("left_source_system"),
            F.col("r.source_system").alias("right_source_system"),
            F.col("l.entity_id").alias("left_entity_id"),
            F.col("r.entity_id").alias("right_entity_id"),
            F.col("l.source_record_id").alias("left_source_record_id"),
            F.col("r.source_record_id").alias("right_source_record_id"),
            F.col("l.source_natural_id").alias("left_source_natural_id"),
            F.col("r.source_natural_id").alias("right_source_natural_id"),
            F.col("l.duplicate_count").alias("left_duplicate_count"),
            F.col("r.duplicate_count").alias("right_duplicate_count"),
            F.col("l.semantic_ambiguity").alias("left_semantic_ambiguity"),
            F.col("r.semantic_ambiguity").alias("right_semantic_ambiguity"),
            F.col("l.preferred_by_site").alias("left_preferred_by_site"),
            F.col("r.preferred_by_site").alias("right_preferred_by_site"),
            F.col("l.candidate_entity_ids").alias("left_candidate_entity_ids"),
            F.col("r.candidate_entity_ids").alias("right_candidate_entity_ids"),
            F.col("l.freshness_ts").alias("left_freshness_ts"),
            F.col("r.freshness_ts").alias("right_freshness_ts"),
        )
    )

    if joined.count() == 0:
        metrics = _make_metric_row(
            left_df,
            source_pair=_pair_name(left_source, right_source),
            rule_name=rule_def.rule_name,
            rule_rank=rule_def.rule_rank,
            rule_status="evaluated" if rule_def.auto_accept else "review_only_rule",
            rule_status_note=None,
            left_candidate_rows=left_candidate_rows,
            right_candidate_rows=right_candidate_rows,
            left_duplicate_keys=left_duplicate_keys,
            right_duplicate_keys=right_duplicate_keys,
            left_rows_preferred_by_site=left_rows_preferred_by_site,
            right_rows_preferred_by_site=right_rows_preferred_by_site,
            auto_matches_count=0,
            ambiguous_count=0,
            unmatched_remaining_count=left_df.count() + right_df.count(),
        )
        return RuleExecutionResult(
            accepted_edges=_empty_match_output(left_df),
            review_rows=_empty_match_output(left_df),
            metrics=metrics,
            left_residue=left_df,
            right_residue=right_df,
        )

    left_pair_counts = joined.groupBy("left_entity_id").agg(F.countDistinct("right_entity_id").alias("left_pair_count"))
    right_pair_counts = joined.groupBy("right_entity_id").agg(F.countDistinct("left_entity_id").alias("right_pair_count"))

    paired = (
        joined.join(left_pair_counts, on="left_entity_id", how="left")
        .join(right_pair_counts, on="right_entity_id", how="left")
        .withColumn("review_reason", _reason_expr(rule_def))
        .withColumn(
            "auto_accepted",
            # Only safe one-to-one deterministic matches are allowed into accepted edges.
            F.lit(rule_def.auto_accept)
            & (~F.col("left_semantic_ambiguity"))
            & (~F.col("right_semantic_ambiguity"))
            & (F.col("left_pair_count") == F.lit(1))
            & (F.col("right_pair_count") == F.lit(1)),
        )
        .withColumn("match_status", F.when(F.col("auto_accepted"), F.lit("accepted")).otherwise(F.lit("review")))
        .withColumn("rapid7_entity_id", _source_entity_column(SOURCE_R7, left_source, right_source))
        .withColumn("fortisiem_entity_id", _source_entity_column(SOURCE_FSM, left_source, right_source))
        .withColumn("sentinalone_entity_id", _source_entity_column(SOURCE_S1, left_source, right_source))
    )
    paired = _materialize(paired.select(*[field.name for field in MATCH_OUTPUT_SCHEMA.fields]))

    accepted_edges = _materialize(paired.filter(F.col("auto_accepted") == F.lit(True)))
    review_rows = _materialize(paired.filter(F.col("auto_accepted") == F.lit(False)))

    # Residue progression is driven only by accepted matches; review rows stay available for later stages.
    accepted_left_ids = accepted_edges.select(F.col("left_entity_id").alias("entity_id")).distinct()
    accepted_right_ids = accepted_edges.select(F.col("right_entity_id").alias("entity_id")).distinct()

    left_residue = _materialize(left_df.join(accepted_left_ids, on="entity_id", how="left_anti"))
    right_residue = _materialize(right_df.join(accepted_right_ids, on="entity_id", how="left_anti"))

    metrics = _make_metric_row(
        left_df,
        source_pair=_pair_name(left_source, right_source),
        rule_name=rule_def.rule_name,
        rule_rank=rule_def.rule_rank,
        rule_status="evaluated" if rule_def.auto_accept else "review_only_rule",
        rule_status_note=None,
        left_candidate_rows=left_candidate_rows,
        right_candidate_rows=right_candidate_rows,
        left_duplicate_keys=left_duplicate_keys,
        right_duplicate_keys=right_duplicate_keys,
        left_rows_preferred_by_site=left_rows_preferred_by_site,
        right_rows_preferred_by_site=right_rows_preferred_by_site,
        auto_matches_count=accepted_edges.count(),
        ambiguous_count=review_rows.count(),
        unmatched_remaining_count=left_residue.count() + right_residue.count(),
    )

    return RuleExecutionResult(
        accepted_edges=accepted_edges,
        review_rows=review_rows,
        metrics=metrics,
        left_residue=left_residue,
        right_residue=right_residue,
    )


def _union_frames(base_df: DataFrame, frames: list[DataFrame], empty_builder) -> DataFrame:
    non_empty = [frame for frame in frames if frame is not None]
    if not non_empty:
        return empty_builder(base_df)

    result = non_empty[0]
    for frame in non_empty[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return _materialize(result)


def run_pairwise_rule_hierarchy(
    left_df: DataFrame,
    right_df: DataFrame,
    left_source: str,
    right_source: str,
    rules: list[MatchRuleDefinition],
) -> RuleExecutionResult:
    accepted_frames: list[DataFrame] = []
    review_frames: list[DataFrame] = []
    metric_frames: list[DataFrame] = []

    left_residue = _materialize(left_df)
    right_residue = _materialize(right_df)

    for rule_def in rules:
        result = match_pair_by_rule(left_residue, right_residue, left_source, right_source, rule_def)
        accepted_frames.append(result.accepted_edges)
        review_frames.append(result.review_rows)
        metric_frames.append(result.metrics)
        left_residue = result.left_residue
        right_residue = result.right_residue

    accepted_edges = _union_frames(left_df, accepted_frames, _empty_match_output)
    review_rows = _union_frames(left_df, review_frames, _empty_match_output)
    metrics = _union_frames(left_df, metric_frames, _empty_metric_output)

    return RuleExecutionResult(
        accepted_edges=accepted_edges,
        review_rows=review_rows,
        metrics=metrics,
        left_residue=left_residue,
        right_residue=right_residue,
    )
