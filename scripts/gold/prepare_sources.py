from pyspark.sql import DataFrame, functions as F

from mapping.target import TARGET_FIELDS
from .config import INVALID_MAC_REGEX, VIRTUAL_OUI_REGEX


TARGET_TYPE_BY_NAME = {f.name: f.dataType.simpleString() for f in TARGET_FIELDS}

# Keep all canonical Silver fields available for survivorship/provenance.
PREP_FIELDS = [f.name for f in TARGET_FIELDS]


def _ensure_column(df: DataFrame, name: str) -> DataFrame:
    if name in df.columns:
        return df
    dtype = TARGET_TYPE_BY_NAME.get(name, "string")
    return df.withColumn(name, F.lit(None).cast(dtype))


def _lower_trim(col_expr):
    val = F.trim(col_expr.cast("string"))
    return F.when(val == "", F.lit(None).cast("string")).otherwise(F.lower(val))


def _clean_string(col_expr):
    val = F.trim(col_expr.cast("string"))
    return F.when(val == "", F.lit(None).cast("string")).otherwise(val)


def _clean_array(arr_col):
    arr = F.coalesce(arr_col.cast("array<string>"), F.array().cast("array<string>"))
    arr = F.transform(arr, lambda x: _clean_string(x))
    arr = F.filter(arr, lambda x: x.isNotNull())
    return F.array_sort(F.array_distinct(arr))


def _normalize_mac_value(col_expr):
    value = F.lower(F.trim(col_expr.cast("string")))
    value = F.regexp_replace(value, "-", ":")
    value = F.regexp_replace(value, r"[^0-9a-f:]", "")
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def _normalize_mac_array(arr_col):
    arr = F.coalesce(arr_col.cast("array<string>"), F.array().cast("array<string>"))
    norm = F.transform(arr, lambda x: _normalize_mac_value(x))
    norm = F.filter(norm, lambda x: x.isNotNull() & (~x.rlike(INVALID_MAC_REGEX)))
    return F.array_sort(F.array_distinct(norm))


def prepare_source(df: DataFrame, source_name: str) -> DataFrame:
    out = df
    for name in PREP_FIELDS:
        out = _ensure_column(out, name)

    out = out.select([F.col(c) for c in PREP_FIELDS])

    source_system = F.lower(F.trim(F.coalesce(F.col("source_system"), F.lit(source_name))))
    primary_mac_norm = _normalize_mac_value(F.col("primary_mac"))
    primary_mac_norm = F.when(primary_mac_norm.rlike(INVALID_MAC_REGEX), F.lit(None).cast("string")).otherwise(
        primary_mac_norm
    )
    primary_mac_tier1 = F.when(primary_mac_norm.rlike(VIRTUAL_OUI_REGEX), F.lit(None).cast("string")).otherwise(
        primary_mac_norm
    )

    mac_norm = _normalize_mac_array(F.col("mac_addresses"))
    gateway_norm = _normalize_mac_array(F.col("gateway_mac_addresses"))
    virtual_norm = _normalize_mac_array(F.col("virtual_mac_addresses"))

    physical = F.array_sort(
        F.array_distinct(
            F.array_except(
                F.array_except(
                    F.coalesce(mac_norm, F.array().cast("array<string>")),
                    F.coalesce(gateway_norm, F.array().cast("array<string>")),
                ),
                F.coalesce(virtual_norm, F.array().cast("array<string>")),
            )
        )
    )
    physical_tier1 = F.filter(physical, lambda x: ~x.rlike(VIRTUAL_OUI_REGEX))

    return (
        out.withColumn("source_system", source_system)
        .withColumn("entity_id", _clean_string(F.col("entity_id")))
        .withColumn("source_record_id", _clean_string(F.col("source_record_id")))
        .withColumn("source_natural_id", _clean_string(F.col("source_natural_id")))
        .withColumn("serial_norm", _lower_trim(F.col("serial_number")))
        .withColumn("primary_hostname_norm", _lower_trim(F.col("primary_hostname")))
        .withColumn("site_name_norm", _lower_trim(F.col("site_name")))
        .withColumn("org_name_norm", _lower_trim(F.col("normalised_org_name")))
        .withColumn("os_family_norm", _lower_trim(F.col("os_family")))
        .withColumn("primary_ip_norm", _clean_string(F.col("primary_ip")))
        .withColumn("access_ip_norm", _clean_string(F.col("access_ip")))
        .withColumn("hostnames_norm", F.transform(_clean_array(F.col("hostnames")), lambda x: F.lower(x)))
        .withColumn("ip_addresses_norm", _clean_array(F.col("ip_addresses")))
        .withColumn("ip_addresses_raw_norm", _clean_array(F.col("ip_addresses_raw")))
        .withColumn("primary_mac_norm", primary_mac_norm)
        .withColumn("primary_mac_tier1", primary_mac_tier1)
        .withColumn("mac_addresses_norm", mac_norm)
        .withColumn("gateway_mac_addresses_norm", gateway_norm)
        .withColumn("virtual_mac_addresses_norm", virtual_norm)
        .withColumn("physical_mac_addresses", physical)
        .withColumn("physical_mac_addresses_tier1", physical_tier1)
    )


def prepare_sources(s1_df: DataFrame, r7_df: DataFrame, fsm_df: DataFrame):
    s1 = prepare_source(s1_df, "sentinalone")
    r7 = prepare_source(r7_df, "rapid7")
    fsm = prepare_source(fsm_df, "fortisiem")
    return s1, r7, fsm
