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


def align_df_to_table(df: DataFrame, table_name: str) -> DataFrame:
    """
    Align DataFrame to target Iceberg table schema:
    - add missing table columns as null
    - cast shared columns to table data types
    - order table columns first, then any newly added DF columns
    """
    spark = df.sparkSession
    if not spark.catalog.tableExists(table_name):
        return df

    table_schema = spark.table(table_name).schema
    table_fields = {f.name: f.dataType for f in table_schema.fields}
    table_cols = [f.name for f in table_schema.fields]

    out = df
    for name, dtype in table_fields.items():
        if name not in out.columns:
            out = out.withColumn(name, F.lit(None).cast(dtype))
        else:
            out = out.withColumn(name, F.col(name).cast(dtype))

    ordered_cols = table_cols + [c for c in out.columns if c not in table_cols]
    return out.select(*ordered_cols)


def write_gold_current(df: DataFrame, table_name: str):
    ensure_table(df, table_name)
    aligned = align_df_to_table(df, table_name)
    aligned.writeTo(table_name).overwrite(F.lit(True))
