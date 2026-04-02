import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ------------------------------------------------------------------------------
# Args
# ------------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export CMDB gold current table to Kafka")
    parser.add_argument("--gold-current-table", required=True)
    parser.add_argument("--kafka-bootstrap-servers", required=True)
    parser.add_argument("--kafka-topic", required=True)
    parser.add_argument("--kafka-key-column", required=True)
    parser.add_argument("--excluded-fields", default="raw_json,raw_payload")
    parser.add_argument("--max-rows-per-run", type=int, default=0)  # 0 = no cap
    parser.add_argument("--output-partitions", type=int, default=4)
    parser.add_argument("--kafka-acks", default="all")
    parser.add_argument("--kafka-compression-type", default="snappy")
    parser.add_argument("--kafka-linger-ms", default="50")
    parser.add_argument("--kafka-batch-size", default="131072")
    parser.add_argument("--kafka-max-request-size", default="1048576")
    return parser.parse_args()


# ------------------------------------------------------------------------------
# Spark
# ------------------------------------------------------------------------------


def build_spark(output_partitions: int) -> SparkSession:
    partitions = max(1, output_partitions)
    spark = (
        SparkSession.builder
        .appName("CMDB Gold Current -> Kafka")
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.default.parallelism", str(partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    return spark


def main() -> None:
    args = parse_args()
    spark = build_spark(args.output_partitions)

    excluded_fields = {
        f.strip()
        for f in args.excluded_fields.split(",")
        if f.strip()
    }
    try:
        df = spark.table(args.gold_current_table)

        keep_cols = [c for c in df.columns if c not in excluded_fields]
        if not keep_cols:
            raise ValueError("No columns left to publish after applying excluded fields")
        payload_df = df.select(*[F.col(c) for c in keep_cols])

        if args.max_rows_per_run > 0:
            payload_df = payload_df.limit(args.max_rows_per_run)

        payload_struct = F.struct(*[F.col(c) for c in keep_cols])
        if args.kafka_key_column in payload_df.columns:
            key_expr = F.coalesce(
                F.col(args.kafka_key_column).cast("string"),
                F.sha2(F.to_json(payload_struct), 256),
            )
        else:
            key_expr = F.sha2(F.to_json(payload_struct), 256)

        kafka_df = payload_df.select(
            key_expr.alias("key"),
            F.to_json(payload_struct).alias("value"),
        )

        if args.output_partitions > 0:
            kafka_df = kafka_df.repartition(args.output_partitions)

        row_count = kafka_df.count()
        print(
            "[INFO] Exporting CMDB gold current snapshot to Kafka "
            f"(rows={row_count}, table={args.gold_current_table}, topic={args.kafka_topic})"
        )

        (
            kafka_df
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers)
            .option("topic", args.kafka_topic)
            .option("kafka.acks", args.kafka_acks)
            .option("kafka.compression.type", args.kafka_compression_type)
            .option("kafka.linger.ms", args.kafka_linger_ms)
            .option("kafka.batch.size", args.kafka_batch_size)
            .option("kafka.max.request.size", args.kafka_max_request_size)
            .save()
        )

        print(
            "[OK] Kafka export complete "
            f"(topic={args.kafka_topic}, rows_sent={row_count}, mode=batch)"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
