from __future__ import annotations

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

from .config import MAX_COMPONENT_ITERATIONS, SOURCE_FSM, SOURCE_R7, SOURCE_S1


COMPONENT_REVIEW_SCHEMA = StructType(
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
        StructField("component_id", StringType(), True),
        StructField("source_count", IntegerType(), True),
        StructField("edge_count", IntegerType(), True),
        StructField("matched_sources", ArrayType(StringType()), True),
        StructField("match_rule_summary", ArrayType(StringType()), True),
        StructField("min_match_rule_rank", IntegerType(), True),
        StructField("rapid7_entity_ids", ArrayType(StringType()), True),
        StructField("fortisiem_entity_ids", ArrayType(StringType()), True),
        StructField("sentinalone_entity_ids", ArrayType(StringType()), True),
    ]
)


def _materialize(df: DataFrame) -> DataFrame:
    try:
        return df.localCheckpoint(eager=True)
    except Exception:
        cached = df.cache()
        cached.count()
        return cached


def _empty_component_review(df: DataFrame) -> DataFrame:
    return df.sparkSession.createDataFrame([], COMPONENT_REVIEW_SCHEMA)


def _build_nodes(accepted_edges: DataFrame) -> DataFrame:
    left_nodes = accepted_edges.select(
        F.concat_ws("|", F.col("left_source_system"), F.col("left_entity_id")).alias("node_id"),
        F.col("left_source_system").alias("source_system"),
        F.col("left_entity_id").alias("entity_id"),
    )
    right_nodes = accepted_edges.select(
        F.concat_ws("|", F.col("right_source_system"), F.col("right_entity_id")).alias("node_id"),
        F.col("right_source_system").alias("source_system"),
        F.col("right_entity_id").alias("entity_id"),
    )
    return _materialize(left_nodes.unionByName(right_nodes).distinct())


def _build_edges(accepted_edges: DataFrame) -> DataFrame:
    direct_edges = accepted_edges.select(
        F.concat_ws("|", F.col("left_source_system"), F.col("left_entity_id")).alias("src_node_id"),
        F.concat_ws("|", F.col("right_source_system"), F.col("right_entity_id")).alias("dst_node_id"),
    ).distinct()
    reverse_edges = direct_edges.select(
        F.col("dst_node_id").alias("src_node_id"),
        F.col("src_node_id").alias("dst_node_id"),
    )
    return _materialize(direct_edges.unionByName(reverse_edges).distinct())


def _compute_component_labels(nodes: DataFrame, edges: DataFrame) -> DataFrame:
    labels = _materialize(nodes.select("node_id", F.col("node_id").alias("component_label")))
    for _ in range(MAX_COMPONENT_ITERATIONS):
        propagated = edges.alias("e").join(
            labels.alias("l"),
            F.col("e.src_node_id") == F.col("l.node_id"),
            "inner",
        ).select(
            F.col("e.dst_node_id").alias("node_id"),
            F.col("l.component_label"),
        )
        next_labels = _materialize(
            labels.unionByName(propagated)
            .groupBy("node_id")
            .agg(F.min("component_label").alias("component_label"))
        )
        changes = next_labels.alias("n").join(
            labels.alias("l"),
            on="node_id",
            how="inner",
        ).filter(F.col("n.component_label") != F.col("l.component_label")).count()
        labels = next_labels
        if changes == 0:
            break
    return labels


def _component_members(nodes: DataFrame, labels: DataFrame) -> DataFrame:
    members = nodes.join(labels, on="node_id", how="inner")
    return _materialize(
        members.groupBy("component_label").agg(
            F.array_sort(F.collect_set("node_id")).alias("member_node_ids"),
            F.array_sort(
                F.filter(
                    F.collect_set(F.when(F.col("source_system") == F.lit(SOURCE_R7), F.col("entity_id"))),
                    lambda x: x.isNotNull(),
                )
            ).alias("rapid7_entity_ids"),
            F.array_sort(
                F.filter(
                    F.collect_set(F.when(F.col("source_system") == F.lit(SOURCE_FSM), F.col("entity_id"))),
                    lambda x: x.isNotNull(),
                )
            ).alias("fortisiem_entity_ids"),
            F.array_sort(
                F.filter(
                    F.collect_set(F.when(F.col("source_system") == F.lit(SOURCE_S1), F.col("entity_id"))),
                    lambda x: x.isNotNull(),
                )
            ).alias("sentinalone_entity_ids"),
        )
    )


def _component_edge_summary(accepted_edges: DataFrame, labels: DataFrame) -> DataFrame:
    edge_membership = (
        accepted_edges.withColumn(
            "left_node_id",
            F.concat_ws("|", F.col("left_source_system"), F.col("left_entity_id")),
        )
        .withColumn(
            "right_node_id",
            F.concat_ws("|", F.col("right_source_system"), F.col("right_entity_id")),
        )
        .join(
            labels.select(
                F.col("node_id").alias("left_node_id"),
                F.col("component_label"),
            ),
            on="left_node_id",
            how="inner",
        )
    )
    return _materialize(
        edge_membership.groupBy("component_label").agg(
            F.count("*").cast("int").alias("edge_count"),
            F.min("rule_rank").cast("int").alias("min_match_rule_rank"),
            F.transform(
                F.array_sort(
                    F.array_distinct(
                        F.collect_list(F.struct(F.col("rule_rank"), F.col("rule_name")))
                    )
                ),
                lambda x: x["rule_name"],
            ).alias("match_rule_summary"),
        )
    )


def build_entity_groups(
    accepted_edges: DataFrame,
    *,
    allow_duplicate_source_components: bool = False,
) -> tuple[DataFrame, DataFrame]:
    if accepted_edges.count() == 0:
        empty_groups = accepted_edges.sparkSession.createDataFrame(
            [],
            "component_id string, rapid7_entity_id string, fortisiem_entity_id string, sentinalone_entity_id string, "
            "rapid7_entity_ids array<string>, fortisiem_entity_ids array<string>, sentinalone_entity_ids array<string>, "
            "seen_in_rapid7 boolean, seen_in_fortisiem boolean, seen_in_sentinalone boolean, source_count int, "
            "edge_count int, matched_sources array<string>, match_rule_summary array<string>, min_match_rule_rank int",
        )
        return empty_groups, _empty_component_review(accepted_edges)

    # Connected components are built strictly from accepted edges, never from review-only candidates.
    nodes = _build_nodes(accepted_edges)
    edges = _build_edges(accepted_edges)
    labels = _compute_component_labels(nodes, edges)
    members = _component_members(nodes, labels)
    edge_summary = _component_edge_summary(accepted_edges, labels)

    groups = (
        members.join(edge_summary, on="component_label", how="left")
        .withColumn("component_id", F.sha2(F.concat_ws("|", F.col("member_node_ids")), 256))
        .withColumn("seen_in_rapid7", F.size(F.col("rapid7_entity_ids")) > F.lit(0))
        .withColumn("seen_in_fortisiem", F.size(F.col("fortisiem_entity_ids")) > F.lit(0))
        .withColumn("seen_in_sentinalone", F.size(F.col("sentinalone_entity_ids")) > F.lit(0))
        .withColumn(
            "source_count",
            F.when(F.col("seen_in_rapid7"), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("seen_in_fortisiem"), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("seen_in_sentinalone"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "matched_sources",
            F.filter(
                F.array(
                    F.when(F.col("seen_in_rapid7"), F.lit(SOURCE_R7)),
                    F.when(F.col("seen_in_fortisiem"), F.lit(SOURCE_FSM)),
                    F.when(F.col("seen_in_sentinalone"), F.lit(SOURCE_S1)),
                ),
                lambda x: x.isNotNull(),
            ),
        )
        .withColumn(
            "duplicate_source_in_component",
            (F.size(F.col("rapid7_entity_ids")) > F.lit(1))
            | (F.size(F.col("fortisiem_entity_ids")) > F.lit(1))
            | (F.size(F.col("sentinalone_entity_ids")) > F.lit(1)),
        )
    )
    groups = _materialize(groups)

    eligibility_filter = F.col("source_count") >= F.lit(2)
    if not allow_duplicate_source_components:
        eligibility_filter = eligibility_filter & (~F.col("duplicate_source_in_component"))

    accepted_groups = _materialize(
        groups.filter(eligibility_filter).select(
            "component_id",
            F.element_at(F.col("rapid7_entity_ids"), 1).alias("rapid7_entity_id"),
            F.element_at(F.col("fortisiem_entity_ids"), 1).alias("fortisiem_entity_id"),
            F.element_at(F.col("sentinalone_entity_ids"), 1).alias("sentinalone_entity_id"),
            "rapid7_entity_ids",
            "fortisiem_entity_ids",
            "sentinalone_entity_ids",
            "seen_in_rapid7",
            "seen_in_fortisiem",
            "seen_in_sentinalone",
            "source_count",
            "edge_count",
            "matched_sources",
            "match_rule_summary",
            "min_match_rule_rank",
        )
    )

    component_review = _materialize(
        groups.filter(F.col("duplicate_source_in_component")).select(
            F.lit("component").alias("record_scope"),
            F.lit(None).cast("string").alias("source_pair"),
            F.lit(None).cast("string").alias("rule_name"),
            F.col("min_match_rule_rank").cast("int").alias("rule_rank"),
            F.lit("component_review").alias("rule_status"),
            F.lit("duplicate_source_in_component").alias("rule_status_note"),
            F.lit("review").alias("match_status"),
            F.lit("duplicate_source_in_component").alias("review_reason"),
            F.lit(None).cast("string").alias("match_key"),
            F.array().cast("array<string>").alias("key_columns_used"),
            F.lit("Accepted edges formed a component with more than one row from the same source.").alias("match_rule_description"),
            F.lit(None).cast("string").alias("left_source_system"),
            F.lit(None).cast("string").alias("right_source_system"),
            F.lit(None).cast("string").alias("left_entity_id"),
            F.lit(None).cast("string").alias("right_entity_id"),
            F.lit(None).cast("string").alias("left_source_record_id"),
            F.lit(None).cast("string").alias("right_source_record_id"),
            F.lit(None).cast("string").alias("left_source_natural_id"),
            F.lit(None).cast("string").alias("right_source_natural_id"),
            F.element_at(F.col("rapid7_entity_ids"), 1).alias("rapid7_entity_id"),
            F.element_at(F.col("fortisiem_entity_ids"), 1).alias("fortisiem_entity_id"),
            F.element_at(F.col("sentinalone_entity_ids"), 1).alias("sentinalone_entity_id"),
            F.lit(None).cast("bigint").alias("left_duplicate_count"),
            F.lit(None).cast("bigint").alias("right_duplicate_count"),
            F.lit(None).cast("boolean").alias("left_semantic_ambiguity"),
            F.lit(None).cast("boolean").alias("right_semantic_ambiguity"),
            F.lit(None).cast("boolean").alias("left_preferred_by_site"),
            F.lit(None).cast("boolean").alias("right_preferred_by_site"),
            F.lit(None).cast("bigint").alias("left_pair_count"),
            F.lit(None).cast("bigint").alias("right_pair_count"),
            F.col("rapid7_entity_ids").alias("left_candidate_entity_ids"),
            F.col("fortisiem_entity_ids").alias("right_candidate_entity_ids"),
            F.lit(None).cast("timestamp").alias("left_freshness_ts"),
            F.lit(None).cast("timestamp").alias("right_freshness_ts"),
            F.lit(False).alias("auto_accepted"),
            "component_id",
            F.col("source_count").cast("int").alias("source_count"),
            F.col("edge_count").cast("int").alias("edge_count"),
            "matched_sources",
            "match_rule_summary",
            F.col("min_match_rule_rank").cast("int").alias("min_match_rule_rank"),
            "rapid7_entity_ids",
            "fortisiem_entity_ids",
            "sentinalone_entity_ids",
        )
    )

    return accepted_groups, component_review
