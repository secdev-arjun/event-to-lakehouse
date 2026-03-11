from pyspark.sql import functions as F

from .utils import add_norm_fields


def _prefixed_select(alias: str, cols, prefix: str):
    return [F.col(f"{alias}.{c}").alias(f"{prefix}{c}") for c in cols]


def match_sources(sentinel_df, rapid7_df, forti_df):
    # Normalize helper fields on each source
    s = add_norm_fields(sentinel_df).alias("s")
    r = add_norm_fields(rapid7_df).alias("r")
    f = add_norm_fields(forti_df).alias("f")

    # Build join conditions using qualified columns
    join_r = (
        (F.col("s.norm_site") == F.col("r.norm_site"))
        & (F.col("s.primary_ip") == F.col("r.primary_ip"))
    )

    join_f = (
        (F.col("s.norm_site") == F.col("f.norm_site"))
        & (
            (F.col("s.norm_host") == F.col("f.norm_host"))
            | (F.col("s.norm_host_short") == F.col("f.norm_host"))
            | (F.col("s.norm_host") == F.col("f.norm_host_short"))
        )
    )

    joined = (
        s
        .join(r, join_r, "left")
        .join(f, join_f, "left")
    )

    # Prefix all columns after joins for deterministic survivorship logic
    s_cols = s.columns
    r_cols = r.columns
    f_cols = f.columns

    return joined.select(
        *_prefixed_select("s", s_cols, "s_"),
        *_prefixed_select("r", r_cols, "r_"),
        *_prefixed_select("f", f_cols, "f_"),
    )
