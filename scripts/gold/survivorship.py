from pyspark.sql import functions as F

from mapping.target import TARGET_FIELDS
from .utils import min_non_null, max_non_null
from .config import GOLD_HASH_COLUMNS


def build_gold_rows(df):
    # Normalize / survivorship helpers
    norm_site = F.coalesce(F.col("s_norm_site"), F.col("r_norm_site"), F.col("f_norm_site"))
    norm_host = F.coalesce(F.col("s_norm_host"), F.col("f_norm_host"), F.col("r_norm_host"))

    site_name = F.coalesce(F.col("s_site_name"), F.col("r_site_name"), F.col("f_site_name"))
    site_id = F.coalesce(F.col("s_site_id"), F.col("r_site_id"), F.col("f_site_id"))

    primary_ip = F.coalesce(F.col("s_primary_ip"), F.col("r_primary_ip"))
    primary_hostname = F.coalesce(F.col("s_primary_hostname"), F.col("f_primary_hostname"))
    host_domain = F.col("s_host_domain")
    os_name = F.coalesce(F.col("s_os_name"), F.col("r_os_name"), F.col("f_os_name"))
    risk_score = F.coalesce(F.col("r_risk_score"), F.col("s_risk_score"), F.col("f_risk_score"))

    first_seen_at = min_non_null(
        F.col("s_first_seen_at"), F.col("r_first_seen_at"), F.col("f_first_seen_at")
    )
    last_seen_at = max_non_null(
        F.col("s_last_seen_at"), F.col("r_last_seen_at"), F.col("f_last_seen_at")
    )
    source_updated_at = max_non_null(
        F.col("s_source_updated_at"),
        F.col("r_source_updated_at"),
        F.col("f_source_updated_at"),
    )

    gold_entity_key_str = F.when(
        primary_ip.isNotNull(),
        F.concat_ws("|", norm_site, primary_ip),
    ).otherwise(
        F.concat_ws("|", norm_site, norm_host)
    )
    gold_asset_id = F.sha2(gold_entity_key_str, 256)

    # Override/survivorship expressions for key fields
    overrides = {
        "source": F.lit("gold"),
        "entity_id": gold_asset_id,
        "entity_key_str": gold_entity_key_str,
        "payload_hash": F.lit(None),  # filled after gold_payload_hash is computed
        "source_system": F.lit("gold"),
        "site_name": site_name,
        "site_id": site_id,
        "primary_ip": primary_ip,
        "primary_hostname": primary_hostname,
        "host_domain": host_domain,
        "os_name": os_name,
        "risk_score": risk_score,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "source_updated_at": source_updated_at,
    }

    # Build base gold row with ALL normalized silver fields
    target_field_names = [f.name for f in TARGET_FIELDS]
    select_exprs = []
    for name in target_field_names:
        expr = overrides.get(name)
        if expr is not None:
            select_exprs.append(expr.alias(name))
        else:
            select_exprs.append(
                F.coalesce(F.col(f"s_{name}"), F.col(f"r_{name}"), F.col(f"f_{name}")).alias(name)
            )

    # Include gold + lineage columns directly so later logic doesn't depend on prefixed fields
    select_exprs.extend(
        [
            gold_asset_id.alias("gold_asset_id"),
            F.col("s_entity_id").alias("sentinalone_entity_id"),
            F.col("r_entity_id").alias("rapid7_entity_id"),
            F.col("f_entity_id").alias("fortisiem_entity_id"),
        ]
    )

    base = df.select(*select_exprs)

    seen_in_sentinalone = F.col("sentinalone_entity_id").isNotNull()
    seen_in_rapid7 = F.col("rapid7_entity_id").isNotNull()
    seen_in_fortisiem = F.col("fortisiem_entity_id").isNotNull()

    matched_sources_raw = F.array(
        F.when(seen_in_sentinalone, F.lit("sentinalone")),
        F.when(seen_in_rapid7, F.lit("rapid7")),
        F.when(seen_in_fortisiem, F.lit("fortisiem")),
    )
    matched_sources = F.filter(matched_sources_raw, lambda x: x.isNotNull())

    # Add gold lineage fields
    base = (
        base
        .withColumn("seen_in_sentinalone", seen_in_sentinalone)
        .withColumn("seen_in_rapid7", seen_in_rapid7)
        .withColumn("seen_in_fortisiem", seen_in_fortisiem)
        .withColumn("matched_sources", matched_sources)
    )

    # gold_payload_hash based on canonical fields
    hash_cols = [F.col(c) for c in GOLD_HASH_COLUMNS]
    base = base.withColumn("gold_payload_hash", F.sha2(F.to_json(F.struct(*hash_cols)), 256))
    base = base.withColumn("payload_hash", F.col("gold_payload_hash"))

    return base
