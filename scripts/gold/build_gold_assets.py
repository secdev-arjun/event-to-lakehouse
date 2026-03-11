import os
import sys

from pyspark.sql import SparkSession, functions as F

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"

from gold.config import (
    RAPID7_SILVER_CURRENT_TABLE,
    FORTI_SILVER_CURRENT_TABLE,
    SENTINEL_SILVER_CURRENT_TABLE,
    GOLD_ASSETS_CURRENT_TABLE,
)
from gold.readers import read_table
from gold.matching import match_sources
from gold.survivorship import build_gold_rows
from gold.writer import write_gold_current


spark = (
    SparkSession.builder
    .appName("Silver -> Gold Assets (POC)")
    .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", ""))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)


# ------------------------------------------------------------------------------
# Main pipeline
# ------------------------------------------------------------------------------

def main():
    sentinel_df = read_table(spark, SENTINEL_SILVER_CURRENT_TABLE)
    rapid7_df = read_table(spark, RAPID7_SILVER_CURRENT_TABLE)
    forti_df = read_table(spark, FORTI_SILVER_CURRENT_TABLE)

    joined = match_sources(sentinel_df, rapid7_df, forti_df)
    gold_df = build_gold_rows(joined)

    # Keep only cross-source matched assets (drop SentinelOne-only rows)
    gold_df = gold_df.filter(F.col("seen_in_rapid7") | F.col("seen_in_fortisiem"))

    write_gold_current(gold_df, GOLD_ASSETS_CURRENT_TABLE)


if __name__ == "__main__":
    main()
