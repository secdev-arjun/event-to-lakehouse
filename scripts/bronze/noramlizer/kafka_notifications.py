from urllib.parse import unquote

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType
)


EVENT_SCHEMA = StructType([
    StructField("EventName", StringType(), True),
    StructField("Key", StringType(), True),
    StructField("Records", ArrayType(StructType([
        StructField("eventName", StringType(), True),
        StructField("eventTime", StringType(), True),
        StructField("s3", StructType([
            StructField("bucket", StructType([
                StructField("name", StringType(), True),
            ]), True),
            StructField("object", StructType([
                StructField("key", StringType(), True),
                StructField("size", IntegerType(), True),
            ]), True),
        ]), True),
    ])), True),
])


def _decode_key(raw_key: str, bucket: str):
    if not raw_key:
        return None
    decoded = unquote(raw_key).lstrip("/")
    if bucket and decoded.startswith(bucket + "/"):
        decoded = decoded[len(bucket) + 1:]
    # If the key still contains a nested bucket prefix, normalize to start at topics/
    if "topics/" in decoded and not decoded.startswith("topics/"):
        decoded = decoded[decoded.find("topics/"):]
    return decoded


decode_key_udf = F.udf(_decode_key, StringType())


def extract_decoded_events(
    batch_df,
    default_bucket: str,
    allowed_topics,
    batch_ts,
    max_files_per_batch: int,
):
    parsed = (
        batch_df.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("event", F.from_json(F.col("json_str"), EVENT_SCHEMA))
        .filter(F.col("event").isNotNull())
        .withColumn("record", F.explode_outer(F.col("event.Records")))
        .select(
            F.coalesce(F.col("record.s3.bucket.name"), F.lit(default_bucket)).alias("bucket"),
            F.coalesce(F.col("record.s3.object.key"), F.col("event.Key")).alias("object_key_raw"),
            F.col("record.eventTime").alias("event_time_str"),
            F.col("record.eventName").alias("event_name"),
            F.col("record.s3.object.size").alias("file_size"),
        )
    )

    if parsed.rdd.isEmpty():
        return parsed

    decoded = (
        parsed.withColumn(
            "decoded_key",
            decode_key_udf(F.col("object_key_raw"), F.col("bucket"))
        )
        .filter(F.col("decoded_key").isNotNull())
        .filter(F.col("decoded_key").startswith("topics/"))
        .filter(
            (F.col("event_name").isNull()) |
            (F.col("event_name").startswith("s3:ObjectCreated"))
        )
        .withColumn("topic_name", F.regexp_extract(F.col("decoded_key"), r"^topics/([^/]+)/", 1))
        .filter(F.col("topic_name").isin(allowed_topics))
        .withColumn("file_path", F.concat(F.lit("s3a://"), F.col("bucket"), F.lit("/"), F.col("decoded_key")))
        .withColumn("event_time", F.to_timestamp(F.col("event_time_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        .withColumn("ingest_ts", F.lit(batch_ts))
        .drop("event_time_str")
        .dropDuplicates(["file_path"])
    )

    if max_files_per_batch > 0:
        decoded = decoded.orderBy(
            F.coalesce(F.col("event_time"), F.col("ingest_ts")).desc(),
            F.col("ingest_ts").desc()
        ).limit(max_files_per_batch)

    return decoded
