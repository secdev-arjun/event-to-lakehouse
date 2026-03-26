from pyspark.sql import DataFrame, functions as F

from mapping.target import TARGET_FIELDS
from .config import INVALID_MAC_REGEX, SOURCE_FSM, SOURCE_R7, SOURCE_S1, VIRTUAL_OUI_REGEX


TARGET_TYPE_BY_NAME = {field.name: field.dataType.simpleString() for field in TARGET_FIELDS}

# Keep all canonical Silver fields available for survivorship and unmatched outputs.
PREP_FIELDS = [field.name for field in TARGET_FIELDS]


def _ensure_column(df: DataFrame, name: str) -> DataFrame:
    if name in df.columns:
        return df
    return df.withColumn(name, F.lit(None).cast(TARGET_TYPE_BY_NAME.get(name, "string")))


def _clean_string(col_expr):
    value = F.trim(col_expr.cast("string"))
    return F.when(value == "", F.lit(None).cast("string")).otherwise(value)


def _lower_trim(col_expr):
    return F.lower(_clean_string(col_expr))


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


def _coalesce_non_empty(*col_exprs):
    result = F.lit(None).cast("string")
    for col_expr in reversed(col_exprs):
        result = F.coalesce(_clean_string(col_expr), result)
    return result


def _count_present(*col_exprs):
    total = F.lit(0)
    for col_expr in col_exprs:
        total = total + F.when(col_expr.isNotNull(), F.lit(1)).otherwise(F.lit(0))
    return total


def _build_ip_key(source_name: str):
    if source_name == SOURCE_R7:
        return _clean_string(F.col("primary_ip"))
    if source_name == SOURCE_FSM:
        return _clean_string(F.col("access_ip"))
    if source_name == SOURCE_S1:
        return F.coalesce(_clean_string(F.col("primary_ip")), _clean_string(F.col("access_ip")))
    raise ValueError(f"Unsupported source name: {source_name}")


def build_source_matching_view(df: DataFrame, source_name: str) -> DataFrame:
    out = df
    for name in PREP_FIELDS:
        out = _ensure_column(out, name)

    out = out.select([F.col(name) for name in PREP_FIELDS])

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

    physical_mac_addresses = F.array_sort(
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
    physical_mac_tier1 = F.filter(physical_mac_addresses, lambda x: ~x.rlike(VIRTUAL_OUI_REGEX))
    # Fold the surviving physical MAC evidence into one reusable exact-match array.
    physical_mac_keys = F.array_sort(
        F.array_distinct(
            F.concat(
                F.coalesce(physical_mac_tier1, F.array().cast("array<string>")),
                F.filter(F.array(primary_mac_tier1), lambda x: x.isNotNull()),
            )
        )
    )

    hostname_key = _lower_trim(
        _coalesce_non_empty(
            F.col("primary_hostname"),
            F.col("asset_name"),
            F.col("source_display_name"),
        )
    )
    ip_key = _build_ip_key(source_name)
    serial_key = _lower_trim(F.col("serial_number"))
    os_family_key = _lower_trim(F.col("os_family"))

    completeness_score = _count_present(
        hostname_key,
        ip_key,
        serial_key,
        _clean_string(F.col("os_name")),
        _clean_string(F.col("os_version")),
        F.when(F.size(physical_mac_keys) > F.lit(0), F.lit("physical_mac")).otherwise(F.lit(None).cast("string")),
    )

    return (
        out.withColumn("source_system", source_system)
        .withColumn("entity_id", _clean_string(F.col("entity_id")))
        .withColumn("source_record_id", _clean_string(F.col("source_record_id")))
        .withColumn("source_natural_id", _clean_string(F.col("source_natural_id")))
        .withColumn("serial_norm", serial_key)
        .withColumn("primary_hostname_norm", _lower_trim(F.col("primary_hostname")))
        .withColumn("site_name_norm", _lower_trim(F.col("site_name")))
        .withColumn("org_name_norm", _lower_trim(F.col("normalised_org_name")))
        .withColumn("os_family_norm", os_family_key)
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
        .withColumn("physical_mac_addresses", physical_mac_addresses)
        .withColumn("physical_mac_addresses_tier1", physical_mac_tier1)
        .withColumn("org_key", _lower_trim(F.col("normalised_org_name")))
        .withColumn("site_key", _lower_trim(F.col("site_name")))
        .withColumn("hostname_key", hostname_key)
        .withColumn("ip_key", ip_key)
        .withColumn("serial_key", serial_key)
        .withColumn("os_family_key", os_family_key)
        .withColumn("physical_mac_keys", physical_mac_keys)
        .withColumn("site_present_flag", F.when(_clean_string(F.col("site_name")).isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("evidence_completeness_score", completeness_score.cast("int"))
        .withColumn(
            "freshness_ts",
            F.coalesce(F.col("source_updated_at"), F.col("last_seen_at"), F.col("ingest_ts")),
        )
        .withColumn("matching_view_version", F.lit("gold.matching.v2"))
    )


def prepare_source(df: DataFrame, source_name: str) -> DataFrame:
    return build_source_matching_view(df, source_name)


def prepare_sources(s1_df: DataFrame, r7_df: DataFrame, fsm_df: DataFrame):
    s1 = build_source_matching_view(s1_df, SOURCE_S1)
    r7 = build_source_matching_view(r7_df, SOURCE_R7)
    fsm = build_source_matching_view(fsm_df, SOURCE_FSM)
    return s1, r7, fsm
