import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

RAPID7_SILVER_CURRENT_TABLE = os.getenv(
    "RAPID7_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__rapid7__assets",
)
FORTI_SILVER_CURRENT_TABLE = os.getenv(
    "FORTI_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__fortisiem__devices",
)
SENTINEL_SILVER_CURRENT_TABLE = os.getenv(
    "SENTINEL_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__sentinelone__agents",
)
FILESHARE_SILVER_CURRENT_TABLE = os.getenv(
    "FILESHARE_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__fileshare__assets",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker-1:19092,broker-2:19092,broker-3:19092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cmdb__silver")

# Key column for Kafka key. Falls back to silver_ci_id, then stable hash.
KEY_COLUMN = os.getenv("KAFKA_KEY_COLUMN", "silver_ci_id")

# Exclude large/raw fields by default.
EXCLUDED_FIELDS = {
    f.strip()
    for f in os.getenv("EXCLUDED_FIELDS", "raw_json,raw_payload").split(",")
    if f.strip()
}

MAX_ROWS_PER_RUN = int(os.getenv("MAX_ROWS_PER_RUN", "0"))  # 0 = no cap
OUTPUT_PARTITIONS = int(os.getenv("OUTPUT_PARTITIONS", "4"))

# Kafka producer tuning
KAFKA_ACKS = os.getenv("KAFKA_ACKS", "all")
KAFKA_COMPRESSION = os.getenv("KAFKA_COMPRESSION_TYPE", "snappy")
KAFKA_LINGER_MS = os.getenv("KAFKA_LINGER_MS", "50")
KAFKA_BATCH_SIZE = os.getenv("KAFKA_BATCH_SIZE", "131072")
KAFKA_MAX_REQUEST_SIZE = os.getenv("KAFKA_MAX_REQUEST_SIZE", "1048576")


# ------------------------------------------------------------------------------
# Spark session
# ------------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Silver Current (4 tables) -> Kafka")
    .config("spark.sql.shuffle.partitions", str(max(1, OUTPUT_PARTITIONS)))
    .config("spark.default.parallelism", str(max(1, OUTPUT_PARTITIONS)))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def _ensure_required_silver_cols(df):
    required = ["source_system", "entity_id", "payload_hash"]
    out = df
    for col_name in required:
        if col_name not in out.columns:
            out = out.withColumn(col_name, F.lit(None).cast("string"))
    return out


def _build_silver_payload_hash(df):
    fallback_cols = sorted(
        c
        for c in df.columns
        if c not in {"payload_hash", "silver_payload_hash", "raw_json", "raw_payload"}
    )
    fallback_struct = (
        F.struct(*[F.col(c) for c in fallback_cols]) if fallback_cols else F.struct()
    )
    return F.coalesce(
        F.col("payload_hash").cast("string"),
        F.sha2(F.to_json(fallback_struct), 256),
    )


def _exclude_rapid7_cced_windows(df):
    """
    Exclude only Rapid7 rows mapped to org=CCED and site=Windows.
    Same behavior as your RP360 job.
    """
    for col_name in ("source_system", "normalised_org_name", "site_name"):
        if col_name not in df.columns:
            return df

    src = F.lower(F.trim(F.coalesce(F.col("source_system"), F.lit(""))))
    org = F.lower(F.trim(F.coalesce(F.col("normalised_org_name"), F.lit(""))))
    site = F.lower(F.trim(F.coalesce(F.col("site_name"), F.lit(""))))

    exclude_cond = (
        (src == F.lit("rapid7"))
        & (org == F.lit("cced"))
        & (site == F.lit("windows"))
    )
    return df.filter(~exclude_cond)


def load_combined_silver_df():
    r7 = _ensure_required_silver_cols(spark.table(RAPID7_SILVER_CURRENT_TABLE))
    fsm = _ensure_required_silver_cols(spark.table(FORTI_SILVER_CURRENT_TABLE))
    s1 = _ensure_required_silver_cols(spark.table(SENTINEL_SILVER_CURRENT_TABLE))
    fs = _ensure_required_silver_cols(spark.table(FILESHARE_SILVER_CURRENT_TABLE))

    # Optional: add source table marker for traceability
    r7 = r7.withColumn("_source_table", F.lit(RAPID7_SILVER_CURRENT_TABLE))
    fsm = fsm.withColumn("_source_table", F.lit(FORTI_SILVER_CURRENT_TABLE))
    s1 = s1.withColumn("_source_table", F.lit(SENTINEL_SILVER_CURRENT_TABLE))
    fs = fs.withColumn("_source_table", F.lit(FILESHARE_SILVER_CURRENT_TABLE))

    combined = (
        r7.unionByName(fsm, allowMissingColumns=True)
        .unionByName(s1, allowMissingColumns=True)
        .unionByName(fs, allowMissingColumns=True)
    )

    combined = _exclude_rapid7_cced_windows(combined)

    combined = (
        combined.withColumn(
            "silver_ci_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lower(F.trim(F.coalesce(F.col("source_system"), F.lit("")))),
                    F.trim(F.coalesce(F.col("entity_id").cast("string"), F.lit(""))),
                ),
                256,
            ),
        )
        .withColumn("silver_payload_hash", _build_silver_payload_hash(combined))
    )

    return combined


def build_kafka_df(df):
    keep_cols = [c for c in df.columns if c not in EXCLUDED_FIELDS]
    payload_df = df.select(*[F.col(c) for c in keep_cols])

    if MAX_ROWS_PER_RUN > 0:
        payload_df = payload_df.limit(MAX_ROWS_PER_RUN)

    payload_json = F.to_json(F.struct(*[F.col(c) for c in keep_cols]))

    if KEY_COLUMN in payload_df.columns:
        key_expr = F.coalesce(
            F.col(KEY_COLUMN).cast("string"),
            F.col("silver_ci_id").cast("string") if "silver_ci_id" in payload_df.columns else None,
            F.sha2(payload_json, 256),
        )
        # coalesce can't take None, so rebuild safely
        if "silver_ci_id" in payload_df.columns:
            key_expr = F.coalesce(
                F.col(KEY_COLUMN).cast("string"),
                F.col("silver_ci_id").cast("string"),
                F.sha2(payload_json, 256),
            )
        else:
            key_expr = F.coalesce(
                F.col(KEY_COLUMN).cast("string"),
                F.sha2(payload_json, 256),
            )
    elif "silver_ci_id" in payload_df.columns:
        key_expr = F.coalesce(
            F.col("silver_ci_id").cast("string"),
            F.sha2(payload_json, 256),
        )
    else:
        key_expr = F.sha2(payload_json, 256)

    kafka_df = payload_df.select(
        key_expr.alias("key"),
        payload_json.alias("value"),
    )

    if OUTPUT_PARTITIONS > 0:
        kafka_df = kafka_df.repartition(OUTPUT_PARTITIONS)

    return kafka_df


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main() -> None:
    combined_df = load_combined_silver_df()
    kafka_df = build_kafka_df(combined_df)

    row_count = kafka_df.count()
    print(
        "[INFO] Exporting combined silver snapshot to Kafka "
        f"(rows={row_count}, topic={KAFKA_TOPIC})"
    )
    print(
        "[INFO] Source tables: "
        f"{RAPID7_SILVER_CURRENT_TABLE}, "
        f"{FORTI_SILVER_CURRENT_TABLE}, "
        f"{SENTINEL_SILVER_CURRENT_TABLE}, "
        f"{FILESHARE_SILVER_CURRENT_TABLE}"
    )

    (
        kafka_df
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC)
        .option("kafka.acks", KAFKA_ACKS)
        .option("kafka.compression.type", KAFKA_COMPRESSION)
        .option("kafka.linger.ms", KAFKA_LINGER_MS)
        .option("kafka.batch.size", KAFKA_BATCH_SIZE)
        .option("kafka.max.request.size", KAFKA_MAX_REQUEST_SIZE)
        .save()
    )

    print(
        "[OK] Kafka export complete "
        f"(topic={KAFKA_TOPIC}, rows_sent={row_count}, mode=batch)"
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        spark.stop()
