from pyspark.sql import SparkSession


def read_table(spark: SparkSession, table_name: str):
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Source table not found: {table_name}")
    return spark.table(table_name)
