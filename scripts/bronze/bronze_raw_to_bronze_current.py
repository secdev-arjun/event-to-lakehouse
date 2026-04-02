import os
import sys
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StringType, TimestampType, StructType, ArrayType, MapType

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"


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

RAW_SAMPLE_NAMESPACE = os.getenv("RAW_SAMPLE_NAMESPACE", "iceberg.elt")

RAPID7_BRONZE_TABLE = os.getenv(
    "RAPID7_BRONZE_TABLE", f"{RAW_SAMPLE_NAMESPACE}.cmdb__bronze__history__rapid7__assets"
)
FORTI_BRONZE_TABLE = os.getenv(
    "FORTI_BRONZE_TABLE", f"{RAW_SAMPLE_NAMESPACE}.cmdb__bronze__history__fortisiem__devices"
)
SENTINEL_BRONZE_TABLE = os.getenv(
    "SENTINEL_BRONZE_TABLE", f"{RAW_SAMPLE_NAMESPACE}.cmdb__bronze__history__sentinelone__agents"
)
RAPID7_SITE_BRONZE_TABLE = os.getenv(
    "RAPID7_SITE_BRONZE_TABLE", f"{RAW_SAMPLE_NAMESPACE}.cmdb__bronze__history__rapid7__sites"
)
FORTI_ORG_BRONZE_TABLE = os.getenv(
    "FORTI_ORG_BRONZE_TABLE", f"{RAW_SAMPLE_NAMESPACE}.cmdb__bronze__history__fortisiem__organization"
)
SENTINEL_SITE_BRONZE_TABLE = os.getenv(
    "SENTINEL_SITE_BRONZE_TABLE", f"{RAW_SAMPLE_NAMESPACE}.cmdb__bronze__history__sentinelone__sites"
)
FILESHARE_BRONZE_TABLE = os.getenv(
    "FILESHARE_BRONZE_TABLE", f"{RAW_SAMPLE_NAMESPACE}.cmdb__bronze__history__fileshare__assets"
)

RAPID7_CURRENT_TABLE = os.getenv(
    "RAPID7_CURRENT_TABLE",
    "iceberg.elt.cmdb__bronze__current__rapid7__assets"
)
FORTI_CURRENT_TABLE = os.getenv(
    "FORTI_CURRENT_TABLE",
    "iceberg.elt.cmdb__bronze__current__fortisiem__devices"
)
SENTINEL_CURRENT_TABLE = os.getenv(
    "SENTINEL_CURRENT_TABLE",
    "iceberg.elt.cmdb__bronze__current__sentinelone__agents"
)
RAPID7_SITE_CURRENT_TABLE = os.getenv(
    "RAPID7_SITE_CURRENT_TABLE",
    "iceberg.elt.cmdb__bronze__current__rapid7__sites"
)
FORTI_ORG_CURRENT_TABLE = os.getenv(
    "FORTI_ORG_CURRENT_TABLE",
    "iceberg.elt.cmdb__bronze__current__fortisiem__organization"
)
SENTINEL_SITE_CURRENT_TABLE = os.getenv(
    "SENTINEL_SITE_CURRENT_TABLE",
    "iceberg.elt.cmdb__bronze__current__sentinelone__sites"
)
FILESHARE_CURRENT_TABLE = os.getenv(
    "FILESHARE_CURRENT_TABLE",
    "iceberg.elt.cmdb__bronze__current__fileshare__assets"
)

ENTITY_ID_CONFIG_DEFAULTS = {
    "rapid7__assets": {"fields": ["site_id", "id"]},
    "sentinalone__agents": {"fields": ["siteId", "id"]},
    "fortisiem__device": {"fields": ["organization.attr_id", "naturalId"]},
    "fileshare__assets": {
        "required_fields": ["sl_no", "location"],
        "fallback_fields": ["mac_address", "ip_address", "hostname"],
    },
    "rapid7__site": {"fields": ["name"]},
    "fortisiem__organization": {"fields": ["name"]},
    "sentinelone__site": {"fields": ["name"]},
}
ENTITY_ID_DELIMITER = "__"
ENTITY_ID_NULL_TOKEN = "<null>"
# Escape delimiter occurrences inside values by prefixing with a backslash.
ENTITY_ID_ESCAPE_TOKEN = "\\__"
SOURCE_COLUMN = "source"

CHECKPOINT_TABLE = os.getenv(
    "BRONZE_CURRENT_CHECKPOINT_TABLE",
    "iceberg.elt.cmdb__bronze__current__checkpoint"
)
# POC default: no lookback for speed. For production, set to ~5 minutes.
CHECKPOINT_LOOKBACK_MINUTES = int(os.getenv("CHECKPOINT_LOOKBACK_MINUTES", "0"))
USE_INGEST_TS = os.getenv("USE_INGEST_TS", "true").lower() == "true"
ENABLED_SOURCES = {
    s.strip().lower()
    for s in os.getenv("ENABLED_SOURCES", "").split(",")
    if s.strip()
}

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

CHECKPOINT_FIELDS = [
    StructField("source_system", StringType(), False),
    StructField("last_ingest_ts", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
]


def _resolve_entity_id_config(runtime_override: dict | None = None) -> dict:
    # Copy defaults to avoid mutation.
    config = {k: dict(v) for k, v in ENTITY_ID_CONFIG_DEFAULTS.items()}
    if runtime_override:
        if not isinstance(runtime_override, dict):
            raise ValueError("runtime_override must be a dict of source -> config")
        for source_key, cfg in runtime_override.items():
            if not isinstance(cfg, dict):
                raise ValueError(f"Override for '{source_key}' must be a dict")
            base = dict(config.get(source_key, {}))
            base.update(cfg)
            config[source_key] = base
    return config


def _validate_field_path(schema: StructType, path: str) -> None:
    if not path or not isinstance(path, str):
        raise ValueError("entity_id field paths must be non-empty strings")

    parts = path.split(".")
    current = schema
    for idx, part in enumerate(parts):
        if not isinstance(current, StructType):
            raise ValueError(
                f"Unsupported nested field path '{path}': "
                f"segment '{part}' is not a struct"
            )
        field = next((f for f in current.fields if f.name == part), None)
        if field is None:
            raise ValueError(f"Configured entity_id field not found: '{path}'")

        if idx < len(parts) - 1:
            if isinstance(field.dataType, StructType):
                current = field.dataType
                continue
            if isinstance(field.dataType, (ArrayType, MapType)):
                raise ValueError(
                    f"Nested paths through arrays/maps are not supported: '{path}'"
                )
            raise ValueError(
                f"Unsupported nested field path '{path}': "
                f"segment '{part}' is not a struct"
            )


def _get_entity_id_definition(config: dict, source_key: str) -> tuple[str, object]:
    if source_key not in config:
        raise ValueError(f"Source '{source_key}' not found in entity_id config")

    source_cfg = config.get(source_key) or {}
    fields = source_cfg.get("fields")
    required_fields = source_cfg.get("required_fields")
    fallback_fields = source_cfg.get("fallback_fields")

    if fields is not None and (required_fields is not None or fallback_fields is not None):
        raise ValueError(
            f"Use either 'fields' or ('required_fields' + 'fallback_fields') for '{source_key}'"
        )

    if fields is not None:
        if not isinstance(fields, list) or not fields:
            raise ValueError(f"'fields' must be a non-empty list for '{source_key}'")
        if len(fields) != len(set(fields)):
            raise ValueError(f"Duplicate field names in fields for '{source_key}'")
        return "composite", fields

    if required_fields is None and fallback_fields is not None:
        raise ValueError(f"'required_fields' must also be set when using 'fallback_fields' for '{source_key}'")
    if required_fields is not None and fallback_fields is None:
        raise ValueError(f"'fallback_fields' must also be set when using 'required_fields' for '{source_key}'")

    if required_fields is not None and fallback_fields is not None:
        if not isinstance(required_fields, list) or not required_fields:
            raise ValueError(f"'required_fields' must be a non-empty list for '{source_key}'")
        if not isinstance(fallback_fields, list) or not fallback_fields:
            raise ValueError(f"'fallback_fields' must be a non-empty list for '{source_key}'")
        if len(required_fields) != len(set(required_fields)):
            raise ValueError(f"Duplicate field names in required_fields for '{source_key}'")
        if len(fallback_fields) != len(set(fallback_fields)):
            raise ValueError(f"Duplicate field names in fallback_fields for '{source_key}'")
        overlap = set(required_fields).intersection(set(fallback_fields))
        if overlap:
            raise ValueError(
                f"Same field(s) present in required_fields and fallback_fields for "
                f"'{source_key}': {sorted(overlap)}"
            )
        return "required_plus_fallback", {
            "required_fields": required_fields,
            "fallback_fields": fallback_fields,
        }

    raise ValueError(
        f"Invalid entity_id config for '{source_key}'; expected 'fields' or "
        f"('required_fields' + 'fallback_fields')"
    )


def _clean_entity_value_expr(field_path: str, lowercase: bool = False):
    col_expr = F.col(field_path).cast("string")
    col_expr = F.trim(col_expr)
    col_expr = F.when((col_expr.isNull()) | (col_expr == ""), F.lit(None)).otherwise(col_expr)
    if lowercase:
        col_expr = F.lower(col_expr)
    return col_expr


def _escape_entity_component_expr(col_expr):
    return F.regexp_replace(col_expr, ENTITY_ID_DELIMITER, ENTITY_ID_ESCAPE_TOKEN)


def _build_entity_id_expr(fields: list):
    parts = []
    for field_path in fields:
        col_expr = _clean_entity_value_expr(field_path)
        col_expr = F.when(col_expr.isNull(), F.lit(ENTITY_ID_NULL_TOKEN)).otherwise(col_expr)
        col_expr = _escape_entity_component_expr(col_expr)
        parts.append(col_expr)
    return F.concat_ws(ENTITY_ID_DELIMITER, *parts)


def _build_fallback_entity_id_expr(fields: list):
    # User-entered IDs can vary by casing/spacing; normalize for stable fallback identity.
    candidates = [_clean_entity_value_expr(field_path, lowercase=True) for field_path in fields]
    return F.coalesce(*candidates)


def _build_required_plus_fallback_entity_id_expr(required_fields: list, fallback_fields: list):
    required_exprs = [
        _clean_entity_value_expr(field_path, lowercase=True) for field_path in required_fields
    ]
    fallback_expr = _build_fallback_entity_id_expr(fallback_fields)

    valid_expr = fallback_expr.isNotNull()
    for req_expr in required_exprs:
        valid_expr = valid_expr & req_expr.isNotNull()

    parts = [_escape_entity_component_expr(req_expr) for req_expr in required_exprs]
    parts.append(_escape_entity_component_expr(fallback_expr))
    entity_id_expr = F.concat_ws(ENTITY_ID_DELIMITER, *parts)

    return entity_id_expr, valid_expr


def _apply_source_metadata(df, source_key: str):
    return df.withColumn(SOURCE_COLUMN, F.lit(source_key))


def _ensure_table_from_raw(raw_df, table_name: str):
    if not spark.catalog.tableExists(table_name):
        raw_df.limit(0).writeTo(table_name).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    missing = [f for f in raw_df.schema.fields if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")


def _needs_full_rebuild(current_table: str) -> bool:
    if not spark.catalog.tableExists(current_table):
        return False
    existing_fields = {f.name for f in spark.table(current_table).schema.fields}
    required = {"entity_id", SOURCE_COLUMN, "_ingest_ts"}
    missing = required - existing_fields
    if missing:
        print(f"[WARN] {current_table} missing columns {sorted(missing)}; forcing full rebuild")
        return True
    return False


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


def _is_source_enabled(source_system: str, source_key: str) -> bool:
    if not ENABLED_SOURCES:
        return True
    return (source_system or "").lower() in ENABLED_SOURCES or (source_key or "").lower() in ENABLED_SOURCES


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


def _with_ingest_ts(df):
    if USE_INGEST_TS and "ingest_ts" in df.columns:
        return df.withColumn("_ingest_ts", _parse_ingest_ts("ingest_ts"))

    for col_name in ("event_time", "source_updated_at", "updated_at", "timestamp"):
        if col_name in df.columns:
            return df.withColumn("_ingest_ts", F.to_timestamp(F.col(col_name).cast("string")))

    return df.withColumn("_ingest_ts", F.current_timestamp())


def _merge_current_raw(incoming_df, current_table: str):
    _ensure_table_from_raw(incoming_df, current_table)

    # Deduplicate incoming updates by latest ingest timestamp per entity_id
    incoming_latest = _latest_per_entity(incoming_df, "entity_id").persist()
    stats = incoming_latest.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.max("_ingest_ts").alias("max_ingest_ts"),
    ).collect()[0]
    incoming_latest_count = int(stats["row_count"] or 0)
    max_ingest_ts = stats["max_ingest_ts"]
    if incoming_latest_count == 0:
        incoming_latest.unpersist()
        return 0, None
    incoming_latest.createOrReplaceTempView("incoming_updates")

    all_cols = incoming_latest.columns
    update_set = ", ".join([f"{c} = s.{c}" for c in all_cols])
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"s.{c}" for c in all_cols])

    merge_sql = f"""
        MERGE INTO {current_table} t
        USING incoming_updates s
        ON t.entity_id = s.entity_id
        WHEN MATCHED AND (t._ingest_ts IS NULL OR s._ingest_ts >= t._ingest_ts) THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    spark.sql(merge_sql)
    incoming_latest.unpersist()
    return incoming_latest_count, max_ingest_ts


def _process_source(source_system, source_key, raw_table, current_table, entity_id_config):
    if not _is_source_enabled(source_system, source_key):
        print(f"[INFO] Skipping source '{source_key}' (not in ENABLED_SOURCES)")
        return

    if not spark.catalog.tableExists(raw_table):
        print(f"[WARN] Source table not found: {raw_table}")
        return

    print(f"[INFO] Processing source '{source_key}' from {raw_table}")
    raw_df = spark.table(raw_table)
    raw_df = _with_ingest_ts(raw_df)

    # Optional incremental filter using checkpoints. Apply this early so downstream
    # entity-id transforms only run on the candidate slice.
    force_full = _needs_full_rebuild(current_table)
    if USE_INGEST_TS and not force_full:
        last_ts = _get_checkpoint(source_system)
        if last_ts is not None:
            start_ts = last_ts - timedelta(minutes=CHECKPOINT_LOOKBACK_MINUTES)
            raw_df = raw_df.filter(F.col("_ingest_ts") > F.lit(start_ts))

    entity_id_strategy, entity_id_def = _get_entity_id_definition(entity_id_config, source_key)

    raw_df = _apply_source_metadata(raw_df, source_key)
    if entity_id_strategy == "required_plus_fallback":
        required_fields = entity_id_def["required_fields"]
        fallback_fields = entity_id_def["fallback_fields"]
        for field_path in required_fields + fallback_fields:
            _validate_field_path(raw_df.schema, field_path)

        print(
            f"[INFO] entity_id strategy for '{source_key}': "
            f"required_plus_fallback required={required_fields} fallback={fallback_fields}"
        )
        entity_id_expr, valid_expr = _build_required_plus_fallback_entity_id_expr(
            required_fields, fallback_fields
        )
        raw_df = raw_df.withColumn("entity_id", entity_id_expr).filter(valid_expr)
    elif entity_id_strategy == "fallback":
        entity_id_fields = entity_id_def
        for field_path in entity_id_fields:
            _validate_field_path(raw_df.schema, field_path)

        print(
            f"[INFO] entity_id strategy for '{source_key}': "
            f"fallback {entity_id_fields}"
        )
        raw_df = raw_df.withColumn(
            "entity_id",
            _escape_entity_component_expr(_build_fallback_entity_id_expr(entity_id_fields)),
        )
        raw_df = raw_df.filter(F.col("entity_id").isNotNull())
    else:
        entity_id_fields = entity_id_def
        for field_path in entity_id_fields:
            _validate_field_path(raw_df.schema, field_path)

        print(
            f"[INFO] entity_id strategy for '{source_key}': "
            f"composite {entity_id_fields}"
        )
        raw_df = raw_df.withColumn("entity_id", _build_entity_id_expr(entity_id_fields))

    incoming_df = raw_df.filter(F.col("_ingest_ts").isNotNull())
    merged_count, max_ts = _merge_current_raw(incoming_df, current_table)
    print(f"[INFO] {source_key}: rows merged into {current_table} = {merged_count}")
    if merged_count == 0:
        print(f"[INFO] No new rows for {source_key}")
        return

    if USE_INGEST_TS:
        _update_checkpoint(source_system, max_ts)


def main():
    entity_id_config = _resolve_entity_id_config()

    _process_source(
        "rapid7",
        "rapid7__assets",
        RAPID7_BRONZE_TABLE,
        RAPID7_CURRENT_TABLE,
        entity_id_config,
    )
    _process_source(
        "fortisiem",
        "fortisiem__device",
        FORTI_BRONZE_TABLE,
        FORTI_CURRENT_TABLE,
        entity_id_config,
    )
    _process_source(
        "sentinelone",
        "sentinalone__agents",
        SENTINEL_BRONZE_TABLE,
        SENTINEL_CURRENT_TABLE,
        entity_id_config,
    )
    _process_source(
        "fileshare",
        "fileshare__assets",
        FILESHARE_BRONZE_TABLE,
        FILESHARE_CURRENT_TABLE,
        entity_id_config,
    )
    _process_source(
        "rapid7_site",
        "rapid7__site",
        RAPID7_SITE_BRONZE_TABLE,
        RAPID7_SITE_CURRENT_TABLE,
        entity_id_config,
    )
    _process_source(
        "fortisiem_org",
        "fortisiem__organization",
        FORTI_ORG_BRONZE_TABLE,
        FORTI_ORG_CURRENT_TABLE,
        entity_id_config,
    )
    _process_source(
        "sentinelone_site",
        "sentinelone__site",
        SENTINEL_SITE_BRONZE_TABLE,
        SENTINEL_SITE_CURRENT_TABLE,
        entity_id_config,
    )


if __name__ == "__main__":
    main()
