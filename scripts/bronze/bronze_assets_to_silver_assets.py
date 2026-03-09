import os
import sys
import time
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
from mapping.sources.rapid7 import normalize_rapid7
from mapping.sources.fortisiem import normalize_fortisiem
from mapping.sources.sentinel import normalize_sentinel

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

# Input: bronze current tables
RAPID7_BRONZE_CURRENT_TABLE = os.getenv(
    "RAPID7_BRONZE_CURRENT_TABLE",
    "iceberg.bronze_current.rapid7__assets__current"
)
FORTI_BRONZE_CURRENT_TABLE = os.getenv(
    "FORTI_BRONZE_CURRENT_TABLE",
    "iceberg.bronze_current.fortisiem__device__current"
)
SENTINEL_BRONZE_CURRENT_TABLE = os.getenv(
    "SENTINEL_BRONZE_CURRENT_TABLE",
    "iceberg.bronze_current.sentinalone__agents__current"
)

# Output: silver current + history per source
RAPID7_SILVER_CURRENT_TABLE = os.getenv(
    "RAPID7_SILVER_CURRENT_TABLE",
    "iceberg.silver.rapid7__assets__silver__current"
)
RAPID7_SILVER_HISTORY_TABLE = os.getenv(
    "RAPID7_SILVER_HISTORY_TABLE",
    "iceberg.silver.rapid7__assets__silver__history"
)

FORTI_SILVER_CURRENT_TABLE = os.getenv(
    "FORTI_SILVER_CURRENT_TABLE",
    "iceberg.silver.fortisiem__device__silver__current"
)
FORTI_SILVER_HISTORY_TABLE = os.getenv(
    "FORTI_SILVER_HISTORY_TABLE",
    "iceberg.silver.fortisiem__device__silver__history"
)

SENTINEL_SILVER_CURRENT_TABLE = os.getenv(
    "SENTINEL_SILVER_CURRENT_TABLE",
    "iceberg.silver.sentinalone__agents__silver__current"
)
SENTINEL_SILVER_HISTORY_TABLE = os.getenv(
    "SENTINEL_SILVER_HISTORY_TABLE",
    "iceberg.silver.sentinalone__agents__silver__history"
)

CONFORMED_CONTRACT_PATH = os.getenv(
    "CONFORMED_CONTRACT_PATH",
    "/opt/spark/scripts/bronze/contracts/assets_silver_contract.yaml"
)

COALESCE_PARTITIONS = int(os.getenv("COALESCE_PARTITIONS", "4"))
CONTRACT_CACHE_TTL_SEC = int(os.getenv("CONTRACT_CACHE_TTL_SEC", "300"))

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

def ensure_history_table(df, table_name: str):
    if not spark.catalog.tableExists(table_name):
        df.limit(0).writeTo(table_name).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    missing = [f for f in HISTORY_FIELDS if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")


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


def _parse_ingest_ts(col_name: str):
    raw = F.col(col_name)
    raw_str = raw.cast("string")
    raw_long = raw.cast("long")

    # Heuristic: treat large numeric values as epoch (ms/us/ns) and scale to seconds.
    epoch_seconds = (
        F.when(raw_long.isNull(), F.lit(None).cast("double"))
        .when(raw_long >= F.lit(10**18), raw_long / F.lit(1e9))   # nanos -> seconds
        .when(raw_long >= F.lit(10**15), raw_long / F.lit(1e6))   # micros -> seconds
        .when(raw_long >= F.lit(10**12), raw_long / F.lit(1e3))   # millis -> seconds
        .when(raw_long >= F.lit(10**9), raw_long.cast("double"))  # seconds
        .otherwise(F.lit(None).cast("double"))
    )
    epoch_ts = F.to_timestamp(F.from_unixtime(epoch_seconds))

    normalized = F.regexp_replace(raw_str, "T", " ")
    normalized = F.regexp_replace(normalized, "Z$", "")

    return F.coalesce(
        epoch_ts,
        F.to_timestamp(raw_str),
        F.to_timestamp(normalized)
    )


def _coerce_ingest_ts(df):
    if "ingest_ts" not in df.columns:
        return df
    field = next((f for f in df.schema.fields if f.name == "ingest_ts"), None)
    if field is not None and isinstance(field.dataType, TimestampType):
        return df
    return df.withColumn("ingest_ts", _parse_ingest_ts("ingest_ts"))


def merge_history_with_retry(
    incoming_df,
    current_table: str,
    history_table: str,
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
            ensure_history_table(history_inserts, history_table)

        if spark.catalog.tableExists(history_table):
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
                            MERGE INTO {history_table} h
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
                            MERGE INTO {history_table} h
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
# Batch: bronze_current -> silver (per source)
# ------------------------------------------------------------------------------

def _dedupe_latest(df):
    df = df.filter(col("entity_key_hash").isNotNull())
    order_col = F.coalesce(col("source_updated_at"), col("event_time"), col("ingest_ts"))
    w = Window.partitionBy("entity_key_hash").orderBy(order_col.desc(), col("ingest_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def process_source(source_name, input_table, current_table, history_table, normalize_fn, contract):
    if not spark.catalog.tableExists(input_table):
        print(f"[WARN] Source table not found: {input_table}")
        return

    raw_df = spark.table(input_table)
    raw_df = _coerce_ingest_ts(raw_df)
    normalized = normalize_fn(raw_df)

    if normalized.rdd.isEmpty():
        print(f"[INFO] No rows for {source_name}")
        return

    normalized = _dedupe_latest(normalized)

    try:
        conformed = conform_df(normalized, contract).coalesce(COALESCE_PARTITIONS)
        conformed = conformed.persist()
        try:
            if conformed.rdd.isEmpty():
                print(f"[INFO] No conformed rows for {source_name}")
                return
            merge_history_with_retry(
                conformed,
                current_table=current_table,
                history_table=history_table,
                materialized=True
            )
            merge_with_retry(conformed, current_table, materialized=True)
        finally:
            conformed.unpersist()
    except Exception as e:
        print(f"[ERROR] {source_name} conformance failed: {e}")


def main():
    contract = load_contract_cached(CONFORMED_CONTRACT_PATH)

    process_source(
        "rapid7",
        RAPID7_BRONZE_CURRENT_TABLE,
        RAPID7_SILVER_CURRENT_TABLE,
        RAPID7_SILVER_HISTORY_TABLE,
        normalize_rapid7,
        contract,
    )
    process_source(
        "fortisiem",
        FORTI_BRONZE_CURRENT_TABLE,
        FORTI_SILVER_CURRENT_TABLE,
        FORTI_SILVER_HISTORY_TABLE,
        normalize_fortisiem,
        contract,
    )
    process_source(
        "sentinelone",
        SENTINEL_BRONZE_CURRENT_TABLE,
        SENTINEL_SILVER_CURRENT_TABLE,
        SENTINEL_SILVER_HISTORY_TABLE,
        normalize_sentinel,
        contract,
    )


if __name__ == "__main__":
    main()
