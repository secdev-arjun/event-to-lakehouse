from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ensure_table(df: DataFrame, table_name: str):
    spark = df.sparkSession
    if not spark.catalog.tableExists(table_name):
        df.limit(0).writeTo(table_name).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    missing = [f for f in df.schema.fields if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")


def write_gold_current(df: DataFrame, table_name: str):
    ensure_table(df, table_name)
    df.writeTo(table_name).overwrite(F.lit(True))
