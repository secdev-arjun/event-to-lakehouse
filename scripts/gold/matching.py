from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F

from .config import SOURCE_FSM, SOURCE_PAIR_FSM_S1, SOURCE_PAIR_R7_FSM, SOURCE_PAIR_R7_S1, SOURCE_R7, SOURCE_S1
from .matching_pairs import build_rule_definitions, run_pairwise_rule_hierarchy
from .prepare_sources import prepare_sources


@dataclass
class MatchOutputs:
    sentinel_prepared: DataFrame
    rapid7_prepared: DataFrame
    forti_prepared: DataFrame
    accepted_edges: DataFrame
    review_rows: DataFrame
    metrics: DataFrame


def _materialize(df: DataFrame) -> DataFrame:
    try:
        return df.localCheckpoint(eager=True)
    except Exception:
        cached = df.cache()
        cached.count()
        return cached


def _union_frames(frames: list[DataFrame]) -> DataFrame:
    non_empty = [frame for frame in frames if frame is not None]
    if not non_empty:
        raise ValueError("At least one DataFrame is required for union")

    result = non_empty[0]
    for frame in non_empty[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return _materialize(result)


def match_sources(sentinel_df: DataFrame, rapid7_df: DataFrame, forti_df: DataFrame) -> MatchOutputs:
    sentinel_prepared, rapid7_prepared, forti_prepared = prepare_sources(sentinel_df, rapid7_df, forti_df)

    sentinel_prepared = _materialize(sentinel_prepared)
    rapid7_prepared = _materialize(rapid7_prepared)
    forti_prepared = _materialize(forti_prepared)

    r7_fsm = run_pairwise_rule_hierarchy(
        rapid7_prepared,
        forti_prepared,
        SOURCE_R7,
        SOURCE_FSM,
        build_rule_definitions(SOURCE_PAIR_R7_FSM),
    )
    r7_s1 = run_pairwise_rule_hierarchy(
        rapid7_prepared,
        sentinel_prepared,
        SOURCE_R7,
        SOURCE_S1,
        build_rule_definitions(SOURCE_PAIR_R7_S1),
    )
    fsm_s1 = run_pairwise_rule_hierarchy(
        forti_prepared,
        sentinel_prepared,
        SOURCE_FSM,
        SOURCE_S1,
        build_rule_definitions(SOURCE_PAIR_FSM_S1),
    )

    accepted_edges = _union_frames([r7_fsm.accepted_edges, r7_s1.accepted_edges, fsm_s1.accepted_edges])
    review_rows = _union_frames([r7_fsm.review_rows, r7_s1.review_rows, fsm_s1.review_rows])
    metrics = _union_frames([r7_fsm.metrics, r7_s1.metrics, fsm_s1.metrics])

    return MatchOutputs(
        sentinel_prepared=sentinel_prepared,
        rapid7_prepared=rapid7_prepared,
        forti_prepared=forti_prepared,
        accepted_edges=accepted_edges,
        review_rows=review_rows,
        metrics=metrics,
    )


def _build_matched_entity_ids(accepted_edges: DataFrame) -> DataFrame:
    left_ids = accepted_edges.select(
        F.col("left_source_system").alias("source_system"),
        F.col("left_entity_id").alias("entity_id"),
    )
    right_ids = accepted_edges.select(
        F.col("right_source_system").alias("source_system"),
        F.col("right_entity_id").alias("entity_id"),
    )
    return _materialize(left_ids.unionByName(right_ids).distinct())


def _add_unmatched_metadata(df: DataFrame, source_name: str) -> DataFrame:
    return (
        df.withColumn("unmatched_reason", F.lit("no_safe_cross_source_match"))
        .withColumn("singleton_ready", F.lit(True))
        .withColumn("source_count", F.lit(1).cast("int"))
        .withColumn("matched_sources", F.array(F.lit(source_name)))
        .withColumn("edge_count", F.lit(0).cast("int"))
        .withColumn("match_rule_summary", F.array().cast("array<string>"))
        .withColumn("min_match_rule_rank", F.lit(None).cast("int"))
        .withColumn("match_status", F.lit("unmatched"))
        .withColumn("component_id", F.lit(None).cast("string"))
        .withColumn("record_scope", F.lit("unmatched"))
        .withColumn("seen_in_rapid7", F.lit(source_name == SOURCE_R7))
        .withColumn("seen_in_fortisiem", F.lit(source_name == SOURCE_FSM))
        .withColumn("seen_in_sentinalone", F.lit(source_name == SOURCE_S1))
        .withColumn("rapid7_entity_id", F.when(F.lit(source_name == SOURCE_R7), F.col("entity_id")).otherwise(F.lit(None).cast("string")))
        .withColumn("fortisiem_entity_id", F.when(F.lit(source_name == SOURCE_FSM), F.col("entity_id")).otherwise(F.lit(None).cast("string")))
        .withColumn("sentinalone_entity_id", F.when(F.lit(source_name == SOURCE_S1), F.col("entity_id")).otherwise(F.lit(None).cast("string")))
    )


def build_unmatched_rows(
    sentinel_prepared: DataFrame,
    rapid7_prepared: DataFrame,
    forti_prepared: DataFrame,
    accepted_edges: DataFrame,
) -> DataFrame:
    matched_entity_ids = _build_matched_entity_ids(accepted_edges)

    rapid7_unmatched = _add_unmatched_metadata(
        rapid7_prepared.join(matched_entity_ids, on=["source_system", "entity_id"], how="left_anti"),
        SOURCE_R7,
    )
    forti_unmatched = _add_unmatched_metadata(
        forti_prepared.join(matched_entity_ids, on=["source_system", "entity_id"], how="left_anti"),
        SOURCE_FSM,
    )
    sentinel_unmatched = _add_unmatched_metadata(
        sentinel_prepared.join(matched_entity_ids, on=["source_system", "entity_id"], how="left_anti"),
        SOURCE_S1,
    )

    return _union_frames([rapid7_unmatched, forti_unmatched, sentinel_unmatched])
