import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"

from mapping.target import TARGET_FIELDS, ensure_columns, add_payload_hash


def _read_contract_text(path: str) -> str:
    if path.startswith("s3a://") or path.startswith("s3://"):
        spark = SparkSession.builder.getOrCreate()
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
            # Unknown op, no-op for forward compatibility
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


def ensure_gold_table(spark, df, table_name: str):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    if not spark.catalog.tableExists(table_name):
        df.limit(0).writeTo(table_name).create()
        return
    existing_fields = {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    missing = [f for f in TARGET_FIELDS if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")


def merge_into_gold(spark, df, table_name: str):
    df = df.persist()
    try:
        if not df.take(1):
            return

        ensure_gold_table(spark, df, table_name)
        df.count()
        df.createOrReplaceTempView("conformed_updates")

        all_cols = [f.name for f in TARGET_FIELDS]
        update_sql = ", ".join([f"{c} = s.{c}" for c in all_cols])
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([f"s.{c}" for c in all_cols])

        merge_sql = f"""
            MERGE INTO {table_name} t
            USING conformed_updates s
            ON t.entity_key_hash = s.entity_key_hash
            WHEN MATCHED THEN
              UPDATE SET {update_sql}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        spark.sql(merge_sql)
    finally:
        df.unpersist()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Silver -> Gold Assets Conformance")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/iceberg")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

    contract_path = os.getenv(
        "CONTRACT_PATH",
        "/opt/spark/scripts/gold/contracts/assets_gold_contract.yaml"
    )
    contract = load_contract(contract_path)
    source_table = os.getenv("SOURCE_TABLE", contract.get("source_table", "iceberg.silver.assets"))
    target_table = os.getenv("GOLD_TABLE", contract.get("target_table", "iceberg.gold.assets_conformed"))
    mode = os.getenv("WRITE_MODE", "merge").lower()

    df = spark.table(source_table)
    conformed = conform_df(df, contract)

    if mode == "overwrite":
        ensure_gold_table(spark, conformed, target_table)
        conformed.writeTo(target_table).overwritePartitions()
    else:
        merge_into_gold(spark, conformed, target_table)
