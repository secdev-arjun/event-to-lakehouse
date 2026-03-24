from pyspark.sql import DataFrame, functions as F

from .prepare_sources import prepare_sources
from .matching_pairs import build_r7_fsm_pairs, build_r7_s1_pairs, build_fsm_s1_pairs


MATCHING_PROJECTION_COLUMNS = [
    "entity_id",
    "source_updated_at",
    "serial_norm",
    "primary_hostname_norm",
    "primary_ip_norm",
    "access_ip_norm",
    "org_name_norm",
    "site_name_norm",
    "os_family_norm",
    "primary_mac_tier1",
    "physical_mac_addresses_tier1",
    "ip_addresses_norm",
]


def _materialize(df: DataFrame) -> DataFrame:
    """
    Cut lineage early so matching joins don't repeatedly re-expand normalization expressions.
    """
    try:
        return df.localCheckpoint(eager=True)
    except Exception:
        cached = df.cache()
        cached.count()
        return cached


def _project_for_matching(df: DataFrame) -> DataFrame:
    cols = [F.col(c) for c in MATCHING_PROJECTION_COLUMNS if c in df.columns]
    return df.select(*cols)


def match_sources(sentinel_df: DataFrame, rapid7_df: DataFrame, forti_df: DataFrame):
    """
    Build all pairwise source matches with normalized source projections.
    No anchor-source design is used here.
    """
    s1, r7, fsm = prepare_sources(sentinel_df, rapid7_df, forti_df)

    # Full prepared frames are kept for survivorship, but materialized once.
    s1 = _materialize(s1)
    r7 = _materialize(r7)
    fsm = _materialize(fsm)

    # Matching works from narrow projections to reduce scan/join memory footprint.
    s1_match = _materialize(_project_for_matching(s1))
    r7_match = _materialize(_project_for_matching(r7))
    fsm_match = _materialize(_project_for_matching(fsm))

    r7_fsm_pairs = _materialize(build_r7_fsm_pairs(r7_match, fsm_match))
    r7_s1_pairs = _materialize(build_r7_s1_pairs(r7_match, s1_match))
    fsm_s1_pairs = _materialize(build_fsm_s1_pairs(fsm_match, s1_match))

    all_pairs = _materialize(
        r7_fsm_pairs.unionByName(r7_s1_pairs, allowMissingColumns=True)
        .unionByName(fsm_s1_pairs, allowMissingColumns=True)
    )

    return s1, r7, fsm, r7_fsm_pairs, r7_s1_pairs, fsm_s1_pairs, all_pairs
