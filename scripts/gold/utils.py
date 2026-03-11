from pyspark.sql import functions as F


def add_norm_fields(df):
    return (
        df
        .withColumn("norm_site", F.upper(F.trim(F.col("site_name"))))
        .withColumn("norm_host", F.upper(F.trim(F.col("primary_hostname"))))
        .withColumn(
            "norm_host_short",
            F.upper(F.trim(F.regexp_replace(F.col("primary_hostname"), "\\..*$", "")))
        )
    )


def prefix_columns(df, prefix: str, exclude: set | None = None):
    exclude = exclude or set()
    cols = []
    for c in df.columns:
        if c in exclude:
            cols.append(F.col(c))
        else:
            cols.append(F.col(c).alias(f"{prefix}{c}"))
    return df.select(cols)


def min_non_null(*cols):
    arr = F.array(*cols)
    filtered = F.filter(arr, lambda x: x.isNotNull())
    return F.array_min(filtered)


def max_non_null(*cols):
    arr = F.array(*cols)
    filtered = F.filter(arr, lambda x: x.isNotNull())
    return F.array_max(filtered)
