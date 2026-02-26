import os
import sys
import time
from datetime import datetime, timezone
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StringType, BooleanType, TimestampType
from pyspark.sql.functions import (
    col, lit, concat_ws, sha2
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"

from mapping.target import TARGET_FIELDS, ensure_columns, add_payload_hash
from mapping.sources.rapid7 import RAPID7_TOPIC, normalize_rapid7
from mapping.sources.fortisiem import FORTI_TOPIC, normalize_fortisiem
from mapping.sources.sentinel import SENTINEL_TOPIC, normalize_sentinel
from bronze.noramlizer.kafka_notifications import extract_decoded_events
from bronze.noramlizer.minio_reader import filter_existing_paths, load_latest_schema, read_topic_files

# ------------------------------------------------------------------------------
# Spark session (your docker spark-submit provides Iceberg + s3a configs)
# ------------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Bronze Assets -> Iceberg Silver assets")
    .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", ""))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

# Safety defaults: ignore missing/corrupt data files during ingestion
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

CONFORMED_TABLE = os.getenv("CONFORMED_TABLE", "iceberg.silver.assets_conformed")
HISTORY_TABLE = os.getenv("HISTORY_TABLE", "iceberg.silver.assets_history")

CONFORMED_CONTRACT_PATH = os.getenv(
    "CONFORMED_CONTRACT_PATH",
    "/opt/spark/scripts/bronze/contracts/assets_silver_contract.yaml"
)


def _with_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"


SCHEMA_ROOT = _with_trailing_slash(os.getenv("SCHEMA_ROOT", "s3a://warehouse/schemas/"))

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker-1:19092,broker-2:19092,broker-3:19092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "minio.object.events")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
MAX_FILES_PER_BATCH = int(os.getenv("MAX_FILES_PER_BATCH", "200"))
READ_RETRY_COUNT = int(os.getenv("READ_RETRY_COUNT", "3"))
READ_RETRY_SLEEP_SEC = float(os.getenv("READ_RETRY_SLEEP_SEC", "2"))
COALESCE_PARTITIONS = int(os.getenv("COALESCE_PARTITIONS", "4"))
CONTRACT_CACHE_TTL_SEC = int(os.getenv("CONTRACT_CACHE_TTL_SEC", "300"))
SCHEMA_CACHE_TTL_SEC = int(os.getenv("SCHEMA_CACHE_TTL_SEC", "300"))

EVENTS_CKPT = os.getenv(
    "EVENTS_CKPT",
    "s3a://warehouse/checkpoints/silver_assets_events/"
)

# JSON reader hardening:
# - multiLine: true for pretty/indented JSON objects
# - mode: PERMISSIVE keeps going on malformed records
# - columnNameOfCorruptRecord: capture bad JSON into a column (requires schema to include it)
JSON_OPTIONS = {
    "multiLine": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record"
}

# ------------------------------------------------------------------------------
# History fields
# ------------------------------------------------------------------------------
HISTORY_EXTRA_FIELDS = [
    StructField("valid_from", TimestampType(), True),
    StructField("valid_to", TimestampType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("version_id", StringType(), True),
    StructField("change_ts", TimestampType(), True),
]
HISTORY_FIELDS = TARGET_FIELDS + HISTORY_EXTRA_FIELDS

# ------------------------------------------------------------------------------
# Simple in-memory caches (with TTL) to avoid re-reading on every micro-batch
# ------------------------------------------------------------------------------
_contract_cache = {"path": None, "loaded_at": 0.0, "contract": None}
_schema_cache = {}  # topic_name -> (loaded_at, schema)
# ------------------------------------------------------------------------------
# Table helpers
# ------------------------------------------------------------------------------

def ensure_table(df, table_name: str):
    if not spark.catalog.tableExists(table_name):
        df.limit(0).writeTo(table_name).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    missing = [f for f in TARGET_FIELDS if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")

def ensure_history_table(df):
    if not spark.catalog.tableExists(HISTORY_TABLE):
        df.limit(0).writeTo(HISTORY_TABLE).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(HISTORY_TABLE).schema.fields}
    missing = [f for f in HISTORY_FIELDS if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMNS ({cols_sql})")


def _with_history_fields(df):
    df = (
        df.withColumn("first_seen_at", col("ingest_ts"))
          .withColumn("last_seen_at", col("ingest_ts"))
          .withColumn("valid_from", col("ingest_ts"))
          .withColumn("valid_to", lit(None).cast("timestamp"))
          .withColumn("is_current", lit(True))
          .withColumn("change_ts", col("ingest_ts"))
          .withColumn(
              "version_id",
              sha2(
                  concat_ws(
                      "|",
                      col("entity_key_hash"),
                      col("payload_hash"),
                      col("valid_from").cast("string")
                  ),
                  256
              )
          )
    )
    return ensure_columns(df, HISTORY_FIELDS)


def merge_history_with_retry(
    incoming_df,
    current_table: str,
    retries: int = 3,
    sleep_sec: float = 2.0,
    materialized: bool = False
):
    if not materialized:
        incoming_df = incoming_df.persist()
    try:
        if not materialized:
            if not incoming_df.take(1):
                return

        current_df = None
        if spark.catalog.tableExists(current_table):
            current_df = (
                spark.table(current_table)
                .select("entity_key_hash", "payload_hash")
                .withColumnRenamed("payload_hash", "cur_payload_hash")
            )

        if current_df is None:
            new_rows = incoming_df
            changed_rows = incoming_df.limit(0)
            same_rows = incoming_df.limit(0)
        else:
            joined = incoming_df.join(current_df, "entity_key_hash", "left")
            new_rows = (
                joined.filter(col("cur_payload_hash").isNull())
                .select(incoming_df.columns)
            )
            changed_rows = (
                joined.filter(
                    col("cur_payload_hash").isNotNull() &
                    (col("payload_hash") != col("cur_payload_hash"))
                )
                .select(incoming_df.columns)
            )
            same_rows = (
                joined.filter(
                    col("cur_payload_hash").isNotNull() &
                    (col("payload_hash") == col("cur_payload_hash"))
                )
                .select("entity_key_hash", "payload_hash", "ingest_ts")
            )

        history_inserts = new_rows.unionByName(changed_rows)
        if not history_inserts.rdd.isEmpty():
            history_inserts = _with_history_fields(history_inserts)
            ensure_history_table(history_inserts)

        if spark.catalog.tableExists(HISTORY_TABLE):
            changed_keys = (
                changed_rows
                .select(
                    "entity_key_hash",
                    "payload_hash",
                    col("ingest_ts").alias("valid_from")
                )
                .distinct()
            )

            last_err = None
            for attempt in range(retries):
                try:
                    if not changed_keys.rdd.isEmpty():
                        changed_keys.createOrReplaceTempView("history_changes")
                        close_sql = f"""
                            MERGE INTO {HISTORY_TABLE} h
                            USING history_changes c
                            ON h.entity_key_hash = c.entity_key_hash
                               AND h.is_current = true
                               AND h.payload_hash <> c.payload_hash
                            WHEN MATCHED THEN
                              UPDATE SET valid_to = c.valid_from, is_current = false
                        """
                        spark.sql(close_sql)

                    if not history_inserts.rdd.isEmpty():
                        history_inserts.createOrReplaceTempView("history_inserts")
                        insert_cols = ", ".join([f.name for f in HISTORY_FIELDS])
                        insert_vals = ", ".join([f"s.{f.name}" for f in HISTORY_FIELDS])
                        insert_sql = f"""
                            MERGE INTO {HISTORY_TABLE} h
                            USING history_inserts s
                            ON h.version_id = s.version_id
                            WHEN NOT MATCHED THEN
                              INSERT ({insert_cols}) VALUES ({insert_vals})
                        """
                        spark.sql(insert_sql)
                    return
                except Exception as e:
                    last_err = e
                    if attempt < retries - 1:
                        time.sleep(sleep_sec)
                        continue
                    raise last_err
    finally:
        if not materialized:
            incoming_df.unpersist()


def merge_with_retry(
    df,
    table_name: str,
    retries: int = 3,
    sleep_sec: float = 2.0,
    materialized: bool = False
):
    # Materialize the source to avoid non-deterministic expressions in MERGE.
    if not materialized:
        df = df.persist()
    try:
        if not materialized:
            if not df.take(1):
                return

        ensure_table(df, table_name)

        # Freeze the dataset so MERGE sees deterministic input.
        if not materialized:
            df.count()
        df.createOrReplaceTempView("incoming_updates")

        all_cols = [f.name for f in TARGET_FIELDS]
        update_changed_cols = [c for c in all_cols if c not in ("first_seen_at", "last_seen_at")]
        update_changed_sql = ", ".join([f"{c} = s.{c}" for c in update_changed_cols])
        update_changed_sql += ", last_seen_at = s.ingest_ts, first_seen_at = t.first_seen_at"

        update_same_sql = "last_seen_at = greatest(t.last_seen_at, s.ingest_ts), ingest_ts = s.ingest_ts"

        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([
            "s.ingest_ts" if c in ("first_seen_at", "last_seen_at") else f"s.{c}" for c in all_cols
        ])

        merge_sql = f"""
            MERGE INTO {table_name} t
            USING incoming_updates s
            ON t.entity_key_hash = s.entity_key_hash
            WHEN MATCHED AND t.payload_hash = s.payload_hash THEN
              UPDATE SET {update_same_sql}
            WHEN MATCHED AND t.payload_hash <> s.payload_hash THEN
              UPDATE SET {update_changed_sql}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        last_err = None
        for attempt in range(retries):
            try:
                spark.sql(merge_sql)
                return
            except Exception as e:
                last_err = e
                if attempt < retries - 1:
                    time.sleep(sleep_sec)
                    continue
                raise last_err
    finally:
        if not materialized:
            df.unpersist()


# ------------------------------------------------------------------------------
# Conformance rules (merged into bronze_to_silver)
# ------------------------------------------------------------------------------

def _read_contract_text(path: str) -> str:
    if path.startswith("s3a://") or path.startswith("s3://"):
        rows = spark.read.text(path).collect()
        return "\n".join([r["value"] for r in rows])
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_contract(path: str) -> dict:
    raw = _read_contract_text(path).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore
            return yaml.safe_load(raw)
        except Exception as e:
            raise RuntimeError(f"Failed to parse contract at {path}: {e}")


def load_contract_cached(path: str) -> dict:
    now = time.time()
    if (
        _contract_cache["contract"] is not None
        and _contract_cache["path"] == path
        and (now - _contract_cache["loaded_at"]) < CONTRACT_CACHE_TTL_SEC
    ):
        return _contract_cache["contract"]

    contract = load_contract(path)
    _contract_cache["path"] = path
    _contract_cache["loaded_at"] = now
    _contract_cache["contract"] = contract
    return contract


def load_latest_schema_cached(spark, schema_root: str, topic_name: str):
    now = time.time()
    cached = _schema_cache.get(topic_name)
    if cached and (now - cached[0]) < SCHEMA_CACHE_TTL_SEC:
        return cached[1]

    schema = load_latest_schema(spark, schema_root, topic_name)
    if schema is not None:
        _schema_cache[topic_name] = (now, schema)
    return schema


def apply_rules(col_expr, rules):
    if not rules:
        return col_expr
    expr = col_expr
    for rule in rules:
        op = rule.get("op")
        if op == "trim":
            expr = F.trim(expr)
        elif op == "lower":
            expr = F.lower(expr)
        elif op == "upper":
            expr = F.upper(expr)
        elif op == "regex_replace":
            expr = F.regexp_replace(expr, rule.get("pattern", ""), rule.get("replacement", ""))
        elif op == "split":
            expr = F.split(expr, rule.get("delimiter", ","))
        elif op == "scale":
            expr = expr * lit(rule.get("factor", 1.0))
        elif op == "scale_if_gt":
            value = rule.get("value")
            factor = rule.get("factor", 1.0)
            expr = F.when(expr > lit(value), expr * lit(factor)).otherwise(expr)
        elif op == "clamp":
            min_v = rule.get("min")
            max_v = rule.get("max")
            if min_v is not None:
                expr = F.when(expr < lit(min_v), lit(min_v)).otherwise(expr)
            if max_v is not None:
                expr = F.when(expr > lit(max_v), lit(max_v)).otherwise(expr)
        elif op == "map":
            mapping = rule.get("values", {})
            default = rule.get("default")
            if mapping:
                map_expr = F.create_map([lit(x) for kv in mapping.items() for x in kv])
                expr = map_expr.getItem(expr)
                if default is not None:
                    expr = F.when(expr.isNull(), lit(default)).otherwise(expr)
        elif op == "array_distinct":
            expr = F.array_distinct(expr)
        elif op == "array_sort":
            expr = F.array_sort(expr)
        elif op == "array_filter_nulls":
            expr = F.filter(expr, lambda x: x.isNotNull())
        elif op == "to_timestamp":
            expr = F.to_timestamp(expr, rule.get("format"))
        else:
            expr = expr
    return expr


def conform_df(df, contract: dict):
    fields = contract.get("fields", {})
    for name, spec in fields.items():
        if name in df.columns:
            expr = col(name)
        else:
            expr = lit(None)
        expr = apply_rules(expr, spec.get("rules"))
        if spec.get("type"):
            expr = expr.cast(spec.get("type"))
        df = df.withColumn(name, expr)

    df = ensure_columns(df, TARGET_FIELDS)
    df = add_payload_hash(df)
    return df

# ------------------------------------------------------------------------------
# Streaming from MinIO object events (Kafka)
# ------------------------------------------------------------------------------

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_ts_row = batch_df.select(F.max("timestamp").alias("batch_ts")).collect()[0]["batch_ts"]
    batch_ts = batch_ts_row if batch_ts_row is not None else datetime.now(timezone.utc)

    decoded = extract_decoded_events(
        batch_df,
        default_bucket="bronze",
        allowed_topics=[RAPID7_TOPIC, FORTI_TOPIC, SENTINEL_TOPIC],
        batch_ts=batch_ts,
        max_files_per_batch=MAX_FILES_PER_BATCH,
    )

    if decoded.rdd.isEmpty():
        return

    rows = decoded.select("topic_name", "file_path", "event_time", "ingest_ts").collect()
    topic_paths = {RAPID7_TOPIC: [], FORTI_TOPIC: [], SENTINEL_TOPIC: []}
    for r in rows:
        topic_paths[r["topic_name"]].append((r["file_path"], r["event_time"], r["ingest_ts"]))

    incoming = []

    def _read_topic(topic_name, normalize_fn):
        entries = topic_paths[topic_name]
        if not entries:
            return
        paths = list(dict.fromkeys([e[0] for e in entries]))
        if not paths:
            return
        existing, _missing = filter_existing_paths(
            spark, paths, READ_RETRY_COUNT, READ_RETRY_SLEEP_SEC
        )
        if not existing:
            return
        schema = load_latest_schema_cached(spark, SCHEMA_ROOT, topic_name)
        if schema is None:
            raise RuntimeError(f"No inferred schema found for {topic_name}")

        meta_by_path = {p: (et, it) for p, et, it in entries}
        entries_existing = [
            (p, *meta_by_path.get(p, (None, batch_ts))) for p in existing
        ]
        df = read_topic_files(spark, entries_existing, schema, JSON_OPTIONS, batch_ts)
        if df is None:
            return

        incoming.append(normalize_fn(df))

    _read_topic(RAPID7_TOPIC, normalize_rapid7)
    _read_topic(FORTI_TOPIC, normalize_fortisiem)
    _read_topic(SENTINEL_TOPIC, normalize_sentinel)

    if not incoming:
        return

    combined = incoming[0]
    for df in incoming[1:]:
        combined = combined.unionByName(df)

    # Deduplicate per entity_key_hash by most recent timestamps
    combined = combined.filter(col("entity_key_hash").isNotNull())
    order_col = F.coalesce(col("source_updated_at"), col("event_time"), col("ingest_ts"))
    w = Window.partitionBy("entity_key_hash").orderBy(order_col.desc(), col("ingest_ts").desc())
    combined = (
        combined.withColumn("_rn", F.row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    try:
        contract = load_contract_cached(CONFORMED_CONTRACT_PATH)
        conformed = conform_df(combined, contract).coalesce(COALESCE_PARTITIONS)
        conformed = conformed.persist()
        try:
            row_count = conformed.count()
            if row_count == 0:
                return
            merge_history_with_retry(conformed, CONFORMED_TABLE, materialized=True)
            merge_with_retry(conformed, CONFORMED_TABLE, materialized=True)
        finally:
            conformed.unpersist()
    except Exception as e:
        # Conformed output is the only target now; surface errors clearly.
        print(f"[ERROR] Conformance failed, no data written: {e}")


# ------------------------------------------------------------------------------
# Streaming query
# ------------------------------------------------------------------------------

events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .load()
)

query = (
    events.writeStream
    .outputMode("append")
    .option("checkpointLocation", EVENTS_CKPT)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .foreachBatch(process_batch)
    .start()
)

# Keep the container running (returns/throws if any query terminates)
spark.streams.awaitAnyTermination()
