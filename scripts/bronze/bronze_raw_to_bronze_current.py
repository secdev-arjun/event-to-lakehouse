import os
import sys
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StringType, TimestampType, StructType

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"

from mapping.sources.rapid7 import RAPID7_TOPIC
from mapping.sources.fortisiem import FORTI_TOPIC
from mapping.sources.sentinel import SENTINEL_TOPIC

# ------------------------------------------------------------------------------
# Spark session (configured by docker spark-submit)
# ------------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Bronze Raw -> Bronze Current")
    .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", ""))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

# Safety defaults: ignore missing/corrupt data files
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

RAPID7_BRONZE_TABLE = os.getenv("RAPID7_BRONZE_TABLE", "iceberg.bronze.rapid7__assets")
FORTI_BRONZE_TABLE = os.getenv("FORTI_BRONZE_TABLE", "iceberg.bronze.fortisiem__device")
SENTINEL_BRONZE_TABLE = os.getenv("SENTINEL_BRONZE_TABLE", "iceberg.bronze.sentinelone__agents")

RAPID7_CURRENT_TABLE = os.getenv(
    "RAPID7_CURRENT_TABLE",
    "iceberg.bronze.rapid7_bronze_current"
)
FORTI_CURRENT_TABLE = os.getenv(
    "FORTI_CURRENT_TABLE",
    "iceberg.bronze.fortisiem_bronze_current"
)
SENTINEL_CURRENT_TABLE = os.getenv(
    "SENTINEL_CURRENT_TABLE",
    "iceberg.bronze.sentinelone_bronze_current"
)

CHECKPOINT_TABLE = os.getenv(
    "BRONZE_CURRENT_CHECKPOINT_TABLE",
    "iceberg.bronze.bronze_current_checkpoint"
)
CHECKPOINT_LOOKBACK_MINUTES = int(os.getenv("CHECKPOINT_LOOKBACK_MINUTES", "5"))

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

CHECKPOINT_FIELDS = [
    StructField("source_system", StringType(), False),
    StructField("last_ingest_ts", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
]


def _ensure_table_from_raw(raw_df, table_name: str):
    if not spark.catalog.tableExists(table_name):
        raw_df.limit(0).writeTo(table_name).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    missing = [f for f in raw_df.schema.fields if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")


def _ensure_checkpoint_table():
    if spark.catalog.tableExists(CHECKPOINT_TABLE):
        return
    schema = StructType(CHECKPOINT_FIELDS)
    schema_df = spark.createDataFrame([], schema=schema)
    schema_df.writeTo(CHECKPOINT_TABLE).create()


def _get_checkpoint(source_system: str):
    if not spark.catalog.tableExists(CHECKPOINT_TABLE):
        return None
    rows = (
        spark.table(CHECKPOINT_TABLE)
        .filter(F.col("source_system") == source_system)
        .select("last_ingest_ts")
        .take(1)
    )
    return rows[0][0] if rows else None


def _update_checkpoint(source_system: str, max_ingest_ts):
    if max_ingest_ts is None:
        return
    try:
        _ensure_checkpoint_table()
    except Exception as exc:
        print(f"[WARN] Failed to ensure checkpoint table: {exc}")
        return
    now_ts = datetime.now(timezone.utc)
    cp_df = spark.createDataFrame(
        [(source_system, max_ingest_ts, now_ts)],
        ["source_system", "last_ingest_ts", "updated_at"]
    )
    cp_df.createOrReplaceTempView("cp_updates")
    spark.sql(f"""
        MERGE INTO {CHECKPOINT_TABLE} c
        USING cp_updates u
        ON c.source_system = u.source_system
        WHEN MATCHED THEN
          UPDATE SET last_ingest_ts = u.last_ingest_ts, updated_at = u.updated_at
        WHEN NOT MATCHED THEN
          INSERT (source_system, last_ingest_ts, updated_at)
          VALUES (u.source_system, u.last_ingest_ts, u.updated_at)
    """)


def _latest_per_entity(df, key_col):
    w = Window.partitionBy(key_col).orderBy(F.col("_ingest_ts").desc_nulls_last())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


def _parse_ingest_ts(col_name: str):
    raw = F.col(col_name)
    return F.coalesce(
        raw.cast("timestamp"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(raw)
    )


def _merge_current_raw(incoming_df, current_table: str, key_col):
    if incoming_df.rdd.isEmpty():
        return

    _ensure_table_from_raw(incoming_df, current_table)

    if spark.catalog.tableExists(current_table):
        current_df = spark.table(current_table)
        current_df = current_df.withColumn("_ingest_ts", _parse_ingest_ts("ingest_ts"))
        current_df = current_df.withColumn("_entity_key", key_col(current_df))
        combined = current_df.unionByName(
            incoming_df.withColumn("_entity_key", key_col(incoming_df)),
            allowMissingColumns=True
        )
    else:
        combined = incoming_df.withColumn("_entity_key", key_col(incoming_df))

    combined = combined.withColumn("_ingest_ts", _parse_ingest_ts("ingest_ts"))
    combined = combined.filter(F.col("_entity_key").isNotNull())
    latest = _latest_per_entity(combined, "_entity_key")
    latest = latest.drop("_entity_key", "_ingest_ts")

    latest.writeTo(current_table).overwrite(F.lit(True))


def _process_source(source_system, raw_table, current_table, key_col_fn):
    if not spark.catalog.tableExists(raw_table):
        print(f"[WARN] Source table not found: {raw_table}")
        return

    raw_df = spark.table(raw_table)
    raw_df = raw_df.withColumn("_ingest_ts", _parse_ingest_ts("ingest_ts"))

    last_ts = _get_checkpoint(source_system)
    if last_ts is not None:
        start_ts = last_ts - timedelta(minutes=CHECKPOINT_LOOKBACK_MINUTES)
        raw_df = raw_df.filter(F.col("_ingest_ts") > F.lit(start_ts))

    # Drop rows without a stable entity key
    raw_df = raw_df.withColumn("_entity_key", key_col_fn(raw_df))
    raw_df = raw_df.filter(F.col("_entity_key").isNotNull()).drop("_entity_key")

    if raw_df.rdd.isEmpty():
        print(f"[INFO] No new rows for {source_system}")
        return

    incoming_df = raw_df.filter(F.col("_ingest_ts").isNotNull()).drop("_ingest_ts")
    _merge_current_raw(incoming_df, current_table, key_col_fn)

    max_ts = raw_df.agg(F.max("_ingest_ts")).collect()[0][0]
    _update_checkpoint(source_system, max_ts)


def main():
    def rapid7_key(df):
        vendor_id = F.col("id").cast("string")
        return F.sha2(F.concat_ws("|", F.lit(RAPID7_TOPIC), vendor_id), 256)

    def forti_key(df):
        vendor_id = F.col("naturalId").cast("string")
        return F.sha2(F.concat_ws("|", F.lit(FORTI_TOPIC), vendor_id), 256)

    def sentinel_key(df):
        vendor_id = F.coalesce(F.col("uuid"), F.col("id")).cast("string")
        return F.sha2(F.concat_ws("|", F.lit(SENTINEL_TOPIC), vendor_id), 256)

    _process_source("rapid7", RAPID7_BRONZE_TABLE, RAPID7_CURRENT_TABLE, rapid7_key)
    _process_source("fortisiem", FORTI_BRONZE_TABLE, FORTI_CURRENT_TABLE, forti_key)
    _process_source("sentinelone", SENTINEL_BRONZE_TABLE, SENTINEL_CURRENT_TABLE, sentinel_key)


if __name__ == "__main__":
    main()
