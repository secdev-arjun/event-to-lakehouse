import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"


def _apply_cli_env_overrides() -> None:
    """
    Allow Livy batch `args` to override config while preserving env/default fallback.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--rapid7-silver-current-table")
    parser.add_argument("--forti-silver-current-table")
    parser.add_argument("--sentinel-silver-current-table")
    parser.add_argument("--gold-assets-current-table")
    parser.add_argument("--gold-assets-history-table")
    parser.add_argument("--gold-max-component-iterations")
    parser.add_argument("--gold-include-review-matches")

    args, _ = parser.parse_known_args()
    env_overrides = {
        "RAPID7_SILVER_CURRENT_TABLE": args.rapid7_silver_current_table,
        "FORTI_SILVER_CURRENT_TABLE": args.forti_silver_current_table,
        "SENTINEL_SILVER_CURRENT_TABLE": args.sentinel_silver_current_table,
        "GOLD_ASSETS_CURRENT_TABLE": args.gold_assets_current_table,
        "GOLD_ASSETS_HISTORY_TABLE": args.gold_assets_history_table,
        "GOLD_MAX_COMPONENT_ITERATIONS": args.gold_max_component_iterations,
        "GOLD_INCLUDE_REVIEW_MATCHES": args.gold_include_review_matches,
    }
    for key, value in env_overrides.items():
        if value is not None:
            os.environ[key] = str(value)


_apply_cli_env_overrides()

from gold.config import (
    GOLD_ALLOW_DUPLICATE_SOURCE_COMPONENTS,
    FORTI_SILVER_CURRENT_TABLE,
    GOLD_INCLUDE_REVIEW_MATCHES,
    GOLD_ASSETS_CURRENT_TABLE,
    RAPID7_SILVER_CURRENT_TABLE,
    SENTINEL_SILVER_CURRENT_TABLE,
)
from gold.readers import read_table
from gold.matching import match_sources
from gold.grouping import build_entity_groups
from gold.survivorship import build_gold_rows
from gold.writer import write_gold_current


spark = (
    SparkSession.builder
    .appName("Silver -> Gold Assets (POC)")
    .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", ""))
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "16")
    .config("spark.sql.autoBroadcastJoinThreshold", "67108864")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.files.io.connectionTimeout", "600s")
    .getOrCreate()
)


# ------------------------------------------------------------------------------
# Main pipeline
# ------------------------------------------------------------------------------

def main():
    sentinel_df = read_table(spark, SENTINEL_SILVER_CURRENT_TABLE)
    rapid7_df = read_table(spark, RAPID7_SILVER_CURRENT_TABLE)
    forti_df = read_table(spark, FORTI_SILVER_CURRENT_TABLE)

    matching_outputs = match_sources(sentinel_df, rapid7_df, forti_df)

    edges_for_grouping = matching_outputs.accepted_edges
    if GOLD_INCLUDE_REVIEW_MATCHES:
        review_as_test_matches = (
            matching_outputs.review_rows
            .withColumn("auto_accepted", F.lit(True))
            .withColumn("match_status", F.lit("accepted_test"))
            .withColumn("review_reason", F.lit(None).cast("string"))
        )
        edges_for_grouping = edges_for_grouping.unionByName(
            review_as_test_matches,
            allowMissingColumns=True,
        )
        print("[WARN] GOLD_INCLUDE_REVIEW_MATCHES=true: review/ambiguous candidates are included for testing.")

    groups, _ = build_entity_groups(
        edges_for_grouping,
        allow_duplicate_source_components=GOLD_ALLOW_DUPLICATE_SOURCE_COMPONENTS,
    )

    gold_df = build_gold_rows(
        matching_outputs.sentinel_prepared,
        matching_outputs.rapid7_prepared,
        matching_outputs.forti_prepared,
        groups,
        edges_for_grouping,
    )

    write_gold_current(gold_df, GOLD_ASSETS_CURRENT_TABLE)


if __name__ == "__main__":
    main()
