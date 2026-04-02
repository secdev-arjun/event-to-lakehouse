import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

SOURCE_TABLE = os.getenv(
    "FORTI_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__fortisiem__devices",
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker-1:19092,broker-2:19092,broker-3:19092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cmdb__silver")

# Key column for Kafka message key. Falls back to a stable hash when missing/null.
KEY_COLUMN = os.getenv("KAFKA_KEY_COLUMN", "silver_ci_id")

# Exclude very large/raw fields by default unless explicitly needed.
EXCLUDED_FIELDS = {
    f.strip()
    for f in os.getenv("EXCLUDED_FIELDS", "raw_json,raw_payload").split(",")
    if f.strip()
}

MAX_ROWS_PER_RUN = int(os.getenv("MAX_ROWS_PER_RUN", "0"))  # 0 = no cap
OUTPUT_PARTITIONS = int(os.getenv("OUTPUT_PARTITIONS", "4"))

# Kafka producer tuning (batch behavior)
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
    .appName("FortiSIEM Silver Current -> Kafka")
    .config("spark.sql.shuffle.partitions", str(max(1, OUTPUT_PARTITIONS)))
    .config("spark.default.parallelism", str(max(1, OUTPUT_PARTITIONS)))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")


def main() -> None:
    df = spark.table(SOURCE_TABLE)

    keep_cols = [c for c in df.columns if c not in EXCLUDED_FIELDS]
    payload_df = df.select(*[F.col(c) for c in keep_cols])

    if MAX_ROWS_PER_RUN > 0:
        payload_df = payload_df.limit(MAX_ROWS_PER_RUN)

    if KEY_COLUMN in payload_df.columns:
        key_expr = F.coalesce(
            F.col(KEY_COLUMN).cast("string"),
            F.sha2(F.to_json(F.struct(*[F.col(c) for c in keep_cols])), 256),
        )
    else:
        key_expr = F.sha2(F.to_json(F.struct(*[F.col(c) for c in keep_cols])), 256)

    kafka_df = payload_df.select(
        key_expr.alias("key"),
        F.to_json(F.struct(*[F.col(c) for c in keep_cols])).alias("value"),
    )

    if OUTPUT_PARTITIONS > 0:
        kafka_df = kafka_df.repartition(OUTPUT_PARTITIONS)

    row_count = kafka_df.count()
    print(
        "[INFO] Exporting FortiSIEM silver current snapshot to Kafka "
        f"(rows={row_count}, table={SOURCE_TABLE}, topic={KAFKA_TOPIC})"
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
