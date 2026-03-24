from functools import reduce

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def _empty_str_array():
    return F.array().cast("array<string>")


def _materialize(df: DataFrame) -> DataFrame:
    """
    Truncate long lineage to keep Catalyst planning stable for chained gold grouping joins.
    """
    try:
        return df.localCheckpoint(eager=True)
    except Exception:
        cached = df.cache()
        cached.count()
        return cached


def _select_best_auto_pairs(df: DataFrame, left_id_col: str, right_id_col: str, prefix: str) -> DataFrame:
    auto_df = df.filter(F.col("auto_merge") == F.lit(True))
    w_left = Window.partitionBy(left_id_col).orderBy(
        F.col("match_score").desc(),
        F.col("match_method").asc(),
    )
    one_left = auto_df.withColumn("_rn_left", F.row_number().over(w_left)).filter(F.col("_rn_left") == 1)
    w_right = Window.partitionBy(right_id_col).orderBy(
        F.col("match_score").desc(),
        F.col("match_method").asc(),
    )
    one_to_one = one_left.withColumn("_rn_right", F.row_number().over(w_right)).filter(F.col("_rn_right") == 1)
    return one_to_one.select(
        F.col(left_id_col),
        F.col(right_id_col),
        F.col("match_method").alias(f"{prefix}match_method"),
        F.col("match_score").alias(f"{prefix}match_score"),
        F.col("match_confidence").alias(f"{prefix}match_confidence"),
        F.col("match_review_flag").alias(f"{prefix}match_review_flag"),
        F.col("match_keys_used").alias(f"{prefix}match_keys_used"),
        F.col("matched_mac_values").alias(f"{prefix}matched_mac_values"),
        F.col("ambiguity_flag").alias(f"{prefix}ambiguity_flag"),
    )


def _merge_arrays(*arr_cols):
    wrapped = [F.coalesce(c.cast("array<string>"), _empty_str_array()) for c in arr_cols]
    return F.array_sort(F.array_distinct(F.flatten(F.array(*wrapped))))


def _confidence_from_score(score_col, review_col, source_count_col):
    return (
        F.when(source_count_col == F.lit(1), F.lit("singleton"))
        .when(review_col, F.lit("review"))
        .when(score_col >= F.lit(95), F.lit("deterministic"))
        .when(score_col >= F.lit(80), F.lit("high"))
        .when(score_col >= F.lit(65), F.lit("medium"))
        .otherwise(F.lit("review"))
    )


def _build_match_fields(df: DataFrame) -> DataFrame:
    methods = F.filter(
        F.array(F.col("m1"), F.col("m2"), F.col("m3")),
        lambda x: x.isNotNull(),
    )
    scores = F.array(
        F.coalesce(F.col("s1"), F.lit(0)),
        F.coalesce(F.col("s2"), F.lit(0)),
        F.coalesce(F.col("s3"), F.lit(0)),
    )
    review_flag = (
        F.coalesce(F.col("r1"), F.lit(False))
        | F.coalesce(F.col("r2"), F.lit(False))
        | F.coalesce(F.col("r3"), F.lit(False))
        | F.coalesce(F.col("transitive_link_flag"), F.lit(False))
    )
    ambiguity_flag = (
        F.coalesce(F.col("a1"), F.lit(False))
        | F.coalesce(F.col("a2"), F.lit(False))
        | F.coalesce(F.col("a3"), F.lit(False))
    )
    source_count = (
        F.when(F.col("rapid7_entity_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("fortisiem_entity_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("sentinalone_entity_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
    max_score = F.array_max(scores)

    return (
        df.withColumn("source_count", source_count)
        .withColumn("match_keys_used", _merge_arrays(F.col("k1"), F.col("k2"), F.col("k3")))
        .withColumn("matched_mac_values", _merge_arrays(F.col("mm1"), F.col("mm2"), F.col("mm3")))
        .withColumn("match_method", F.when(F.size(methods) == 0, F.lit("singleton")).otherwise(F.concat_ws("+", F.array_sort(F.array_distinct(methods)))))
        .withColumn("match_score", F.when(F.size(methods) == 0, F.lit(0)).otherwise(max_score))
        .withColumn("match_review_flag", F.when(F.size(methods) == 0, F.lit(False)).otherwise(review_flag))
        .withColumn("ambiguity_flag", ambiguity_flag)
        .withColumn("match_confidence", _confidence_from_score(F.col("match_score"), F.col("match_review_flag"), F.col("source_count")))
        .drop("m1", "m2", "m3", "s1", "s2", "s3", "r1", "r2", "r3", "k1", "k2", "k3", "mm1", "mm2", "mm3", "a1", "a2", "a3")
    )


def build_entity_groups(
    r7_df: DataFrame,
    fsm_df: DataFrame,
    s1_df: DataFrame,
    r7_fsm_pairs: DataFrame,
    r7_s1_pairs: DataFrame,
    fsm_s1_pairs: DataFrame,
) -> DataFrame:
    r7_fsm_best = _materialize(_select_best_auto_pairs(r7_fsm_pairs, "rapid7_entity_id", "fortisiem_entity_id", "r7_fsm_"))
    r7_s1_best = _materialize(_select_best_auto_pairs(r7_s1_pairs, "rapid7_entity_id", "sentinalone_entity_id", "r7_s1_"))
    fsm_s1_best = _materialize(_select_best_auto_pairs(fsm_s1_pairs, "fortisiem_entity_id", "sentinalone_entity_id", "fsm_s1_"))

    r7_ids = _materialize(r7_df.select(F.col("entity_id").alias("rapid7_entity_id")).distinct())

    base = (
        r7_ids.alias("r")
        .join(r7_fsm_best.alias("rf"), on="rapid7_entity_id", how="left")
        .join(r7_s1_best.alias("rs"), on="rapid7_entity_id", how="left")
        .withColumn("fortisiem_entity_id", F.col("fortisiem_entity_id"))
        .withColumn("sentinalone_entity_id", F.col("sentinalone_entity_id"))
        .withColumn("_r7_fsm_entity", F.col("fortisiem_entity_id"))
        .withColumn("_r7_s1_entity", F.col("sentinalone_entity_id"))
    )
    base = _materialize(base)

    # Indirect transitive link attempt via FSM<->S1 pairs:
    # if r7->s1 exists and r7->fsm missing, fill fsm via fsm<->s1 mapping (and vice versa).
    by_s1 = fsm_s1_best.select(
        F.col("sentinalone_entity_id").alias("s1_id_lookup"),
        F.col("fortisiem_entity_id").alias("fsm_from_s1"),
        F.col("fsm_s1_match_method").alias("fsm_s1_method_from_s1"),
        F.col("fsm_s1_match_score").alias("fsm_s1_score_from_s1"),
        F.col("fsm_s1_match_review_flag").alias("fsm_s1_review_from_s1"),
        F.col("fsm_s1_match_keys_used").alias("fsm_s1_keys_from_s1"),
        F.col("fsm_s1_matched_mac_values").alias("fsm_s1_macs_from_s1"),
        F.col("fsm_s1_ambiguity_flag").alias("fsm_s1_ambiguity_from_s1"),
    )
    by_fsm = fsm_s1_best.select(
        F.col("fortisiem_entity_id").alias("fsm_id_lookup"),
        F.col("sentinalone_entity_id").alias("s1_from_fsm"),
        F.col("fsm_s1_match_method").alias("fsm_s1_method_from_fsm"),
        F.col("fsm_s1_match_score").alias("fsm_s1_score_from_fsm"),
        F.col("fsm_s1_match_review_flag").alias("fsm_s1_review_from_fsm"),
        F.col("fsm_s1_match_keys_used").alias("fsm_s1_keys_from_fsm"),
        F.col("fsm_s1_matched_mac_values").alias("fsm_s1_macs_from_fsm"),
        F.col("fsm_s1_ambiguity_flag").alias("fsm_s1_ambiguity_from_fsm"),
    )

    base = base.join(by_s1, base.sentinalone_entity_id == by_s1.s1_id_lookup, "left")
    base = (
        base.withColumn(
            "fortisiem_entity_id",
            F.coalesce(F.col("fortisiem_entity_id"), F.col("fsm_from_s1")),
        )
        .withColumn(
            "transitive_link_flag_1",
            F.col("_r7_fsm_entity").isNull() & F.col("fsm_from_s1").isNotNull(),
        )
        .drop("s1_id_lookup", "fsm_from_s1")
    )

    base = base.join(by_fsm, base.fortisiem_entity_id == by_fsm.fsm_id_lookup, "left")
    base = (
        base.withColumn(
            "sentinalone_entity_id",
            F.coalesce(F.col("sentinalone_entity_id"), F.col("s1_from_fsm")),
        )
        .withColumn(
            "transitive_link_flag_2",
            F.col("_r7_s1_entity").isNull() & F.col("s1_from_fsm").isNotNull(),
        )
        .drop("fsm_id_lookup", "s1_from_fsm")
    )

    base = (
        base.withColumn("transitive_link_flag", F.col("transitive_link_flag_1") | F.col("transitive_link_flag_2"))
        .drop("transitive_link_flag_1", "transitive_link_flag_2", "_r7_fsm_entity", "_r7_s1_entity")
        .withColumn("m1", F.col("r7_fsm_match_method"))
        .withColumn("m2", F.col("r7_s1_match_method"))
        .withColumn(
            "m3",
            F.coalesce(F.col("fsm_s1_method_from_s1"), F.col("fsm_s1_method_from_fsm")),
        )
        .withColumn("s1", F.col("r7_fsm_match_score"))
        .withColumn("s2", F.col("r7_s1_match_score"))
        .withColumn("s3", F.coalesce(F.col("fsm_s1_score_from_s1"), F.col("fsm_s1_score_from_fsm")))
        .withColumn("r1", F.col("r7_fsm_match_review_flag"))
        .withColumn("r2", F.col("r7_s1_match_review_flag"))
        .withColumn("r3", F.coalesce(F.col("fsm_s1_review_from_s1"), F.col("fsm_s1_review_from_fsm")))
        .withColumn("k1", F.col("r7_fsm_match_keys_used"))
        .withColumn("k2", F.col("r7_s1_match_keys_used"))
        .withColumn("k3", F.coalesce(F.col("fsm_s1_keys_from_s1"), F.col("fsm_s1_keys_from_fsm")))
        .withColumn("mm1", F.col("r7_fsm_matched_mac_values"))
        .withColumn("mm2", F.col("r7_s1_matched_mac_values"))
        .withColumn("mm3", F.coalesce(F.col("fsm_s1_macs_from_s1"), F.col("fsm_s1_macs_from_fsm")))
        .withColumn("a1", F.col("r7_fsm_ambiguity_flag"))
        .withColumn("a2", F.col("r7_s1_ambiguity_flag"))
        .withColumn("a3", F.coalesce(F.col("fsm_s1_ambiguity_from_s1"), F.col("fsm_s1_ambiguity_from_fsm")))
    )
    base = _materialize(base)

    r7_groups = _build_match_fields(base).select(
        "rapid7_entity_id",
        "fortisiem_entity_id",
        "sentinalone_entity_id",
        "match_method",
        "match_score",
        "match_confidence",
        "match_review_flag",
        "match_keys_used",
        "matched_mac_values",
        "ambiguity_flag",
        "transitive_link_flag",
    )
    r7_groups = _materialize(r7_groups)

    used_fsm_from_r7 = _materialize(
        r7_groups.filter(F.col("fortisiem_entity_id").isNotNull()).select("fortisiem_entity_id").distinct()
    )
    used_s1_from_r7 = _materialize(
        r7_groups.filter(F.col("sentinalone_entity_id").isNotNull()).select("sentinalone_entity_id").distinct()
    )

    fsm_s1_only = (
        fsm_s1_best.alias("p")
        .join(used_fsm_from_r7.alias("uf"), F.col("p.fortisiem_entity_id") == F.col("uf.fortisiem_entity_id"), "left_anti")
        .join(used_s1_from_r7.alias("us"), F.col("p.sentinalone_entity_id") == F.col("us.sentinalone_entity_id"), "left_anti")
        .select(
            F.lit(None).cast("string").alias("rapid7_entity_id"),
            F.col("p.fortisiem_entity_id"),
            F.col("p.sentinalone_entity_id"),
            F.col("p.fsm_s1_match_method").alias("match_method"),
            F.col("p.fsm_s1_match_score").alias("match_score"),
            F.col("p.fsm_s1_match_confidence").alias("match_confidence"),
            F.col("p.fsm_s1_match_review_flag").alias("match_review_flag"),
            F.col("p.fsm_s1_match_keys_used").alias("match_keys_used"),
            F.col("p.fsm_s1_matched_mac_values").alias("matched_mac_values"),
            F.col("p.fsm_s1_ambiguity_flag").alias("ambiguity_flag"),
            F.lit(False).alias("transitive_link_flag"),
        )
    )
    fsm_s1_only = _materialize(fsm_s1_only)

    combined = _materialize(r7_groups.unionByName(fsm_s1_only, allowMissingColumns=True))

    used_r7 = _materialize(combined.filter(F.col("rapid7_entity_id").isNotNull()).select("rapid7_entity_id").distinct())
    used_fsm = _materialize(combined.filter(F.col("fortisiem_entity_id").isNotNull()).select("fortisiem_entity_id").distinct())
    used_s1 = _materialize(combined.filter(F.col("sentinalone_entity_id").isNotNull()).select("sentinalone_entity_id").distinct())

    fsm_single = (
        fsm_df.select(F.col("entity_id").alias("fortisiem_entity_id")).distinct()
        .join(used_fsm, on="fortisiem_entity_id", how="left_anti")
        .select(
            F.lit(None).cast("string").alias("rapid7_entity_id"),
            F.col("fortisiem_entity_id"),
            F.lit(None).cast("string").alias("sentinalone_entity_id"),
            F.lit("singleton").alias("match_method"),
            F.lit(0).cast("int").alias("match_score"),
            F.lit("singleton").alias("match_confidence"),
            F.lit(False).alias("match_review_flag"),
            _empty_str_array().alias("match_keys_used"),
            _empty_str_array().alias("matched_mac_values"),
            F.lit(False).alias("ambiguity_flag"),
            F.lit(False).alias("transitive_link_flag"),
        )
    )

    s1_single = (
        s1_df.select(F.col("entity_id").alias("sentinalone_entity_id")).distinct()
        .join(used_s1, on="sentinalone_entity_id", how="left_anti")
        .select(
            F.lit(None).cast("string").alias("rapid7_entity_id"),
            F.lit(None).cast("string").alias("fortisiem_entity_id"),
            F.col("sentinalone_entity_id"),
            F.lit("singleton").alias("match_method"),
            F.lit(0).cast("int").alias("match_score"),
            F.lit("singleton").alias("match_confidence"),
            F.lit(False).alias("match_review_flag"),
            _empty_str_array().alias("match_keys_used"),
            _empty_str_array().alias("matched_mac_values"),
            F.lit(False).alias("ambiguity_flag"),
            F.lit(False).alias("transitive_link_flag"),
        )
    )

    # R7 singletons are already present in r7_groups; this anti-join keeps only uncovered rows if needed.
    r7_single_missing = (
        r7_df.select(F.col("entity_id").alias("rapid7_entity_id")).distinct()
        .join(used_r7, on="rapid7_entity_id", how="left_anti")
        .select(
            F.col("rapid7_entity_id"),
            F.lit(None).cast("string").alias("fortisiem_entity_id"),
            F.lit(None).cast("string").alias("sentinalone_entity_id"),
            F.lit("singleton").alias("match_method"),
            F.lit(0).cast("int").alias("match_score"),
            F.lit("singleton").alias("match_confidence"),
            F.lit(False).alias("match_review_flag"),
            _empty_str_array().alias("match_keys_used"),
            _empty_str_array().alias("matched_mac_values"),
            F.lit(False).alias("ambiguity_flag"),
            F.lit(False).alias("transitive_link_flag"),
        )
    )

    result = reduce(
        lambda l, r: l.unionByName(r, allowMissingColumns=True),
        [combined, fsm_single, s1_single, r7_single_missing],
    ).dropDuplicates(["rapid7_entity_id", "fortisiem_entity_id", "sentinalone_entity_id"])
    return _materialize(result)
