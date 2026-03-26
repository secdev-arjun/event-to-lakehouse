from __future__ import annotations

import os
from dataclasses import dataclass


# Input SILVER current tables
RAPID7_SILVER_CURRENT_TABLE = os.getenv(
    "RAPID7_SILVER_CURRENT_TABLE",
    "iceberg.silver.rapid7__assets__silver__current",
)
FORTI_SILVER_CURRENT_TABLE = os.getenv(
    "FORTI_SILVER_CURRENT_TABLE",
    "iceberg.silver.fortisiem__device__silver__current",
)
SENTINEL_SILVER_CURRENT_TABLE = os.getenv(
    "SENTINEL_SILVER_CURRENT_TABLE",
    "iceberg.silver.sentinalone__agents__silver__current",
)

# Output GOLD tables
GOLD_ASSETS_CURRENT_TABLE = os.getenv(
    "GOLD_ASSETS_CURRENT_TABLE",
    "iceberg.gold.assets_current",
)
GOLD_ASSETS_HISTORY_TABLE = os.getenv(
    "GOLD_ASSETS_HISTORY_TABLE",
    "iceberg.gold.assets_history",
)
GOLD_MATCH_CANDIDATES_TABLE = os.getenv(
    "GOLD_MATCH_CANDIDATES_TABLE",
    "iceberg.gold.match_candidates",
)
GOLD_MATCH_REVIEW_TABLE = os.getenv(
    "GOLD_MATCH_REVIEW_TABLE",
    "iceberg.gold.match_review",
)
GOLD_ASSETS_UNMATCHED_TABLE = os.getenv(
    "GOLD_ASSETS_UNMATCHED_TABLE",
    "iceberg.gold.assets_unmatched",
)
GOLD_MATCH_METRICS_TABLE = os.getenv(
    "GOLD_MATCH_METRICS_TABLE",
    "iceberg.gold.match_metrics",
)

SOURCE_R7 = "rapid7"
SOURCE_FSM = "fortisiem"
SOURCE_S1 = "sentinalone"

SOURCE_PAIR_R7_FSM = (SOURCE_R7, SOURCE_FSM)
SOURCE_PAIR_R7_S1 = (SOURCE_R7, SOURCE_S1)
SOURCE_PAIR_FSM_S1 = (SOURCE_FSM, SOURCE_S1)
PAIRWISE_SOURCE_PAIRS = (
    SOURCE_PAIR_R7_FSM,
    SOURCE_PAIR_R7_S1,
    SOURCE_PAIR_FSM_S1,
)

INVALID_MAC_REGEX = r"^(00:00:00:00:00:00|ff:ff:ff:ff:ff:ff)$"
VIRTUAL_OUI_REGEX = r"^(00:0c:29:|00:50:56:|00:15:5d:)"

MAX_COMPONENT_ITERATIONS = int(os.getenv("GOLD_MAX_COMPONENT_ITERATIONS", "12"))


@dataclass(frozen=True)
class MatchRuleDefinition:
    rule_name: str
    rule_rank: int
    key_parts: tuple[str, ...]
    key_columns_used: tuple[str, ...]
    applicable_pairs: tuple[tuple[str, str], ...]
    auto_accept: bool = True
    explode_array_column: str | None = None
    explode_alias: str | None = None
    description: str = ""


MATCH_RULES = (
    MatchRuleDefinition(
        rule_name="serial_org_exact",
        rule_rank=1,
        key_parts=("org_key", "serial_key"),
        key_columns_used=("normalised_org_name", "serial_number"),
        applicable_pairs=(SOURCE_PAIR_FSM_S1,),
        description="Exact canonical org plus serial number where both sources expose stable hardware serials.",
    ),
    MatchRuleDefinition(
        rule_name="physical_mac_org_exact",
        rule_rank=2,
        key_parts=("org_key", "physical_mac_key"),
        key_columns_used=("normalised_org_name", "mac_addresses"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        explode_array_column="physical_mac_keys",
        explode_alias="physical_mac_key",
        description="Exact canonical org plus filtered physical MAC evidence after removing gateway and virtual MACs.",
    ),
    MatchRuleDefinition(
        rule_name="org_site_ip_exact",
        rule_rank=3,
        key_parts=("org_key", "site_key", "ip_key"),
        key_columns_used=("normalised_org_name", "site_name", "primary_ip"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        description="Preferred business-aligned network key scoped by canonical org and canonical site.",
    ),
    MatchRuleDefinition(
        rule_name="org_site_hostname_exact",
        rule_rank=4,
        key_parts=("org_key", "site_key", "hostname_key"),
        key_columns_used=("normalised_org_name", "site_name", "primary_hostname"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        description="Preferred hostname fallback scoped by canonical org and canonical site.",
    ),
    MatchRuleDefinition(
        rule_name="org_ip_os_family_exact",
        rule_rank=5,
        key_parts=("org_key", "ip_key", "os_family_key"),
        key_columns_used=("normalised_org_name", "primary_ip", "os_family"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        description="Siteless fallback using exact org and IP with OS family as an extra discriminator.",
    ),
    MatchRuleDefinition(
        rule_name="org_hostname_os_family_exact",
        rule_rank=6,
        key_parts=("org_key", "hostname_key", "os_family_key"),
        key_columns_used=("normalised_org_name", "primary_hostname", "os_family"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        description="Siteless hostname fallback using OS family as an extra discriminator.",
    ),
    MatchRuleDefinition(
        rule_name="org_ip_exact",
        rule_rank=7,
        key_parts=("org_key", "ip_key"),
        key_columns_used=("normalised_org_name", "primary_ip"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        auto_accept=False,
        description="Exact canonical org plus IP kept as review-only until coverage and collision rates are validated.",
    ),
    MatchRuleDefinition(
        rule_name="org_hostname_exact",
        rule_rank=8,
        key_parts=("org_key", "hostname_key"),
        key_columns_used=("normalised_org_name", "primary_hostname"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        auto_accept=False,
        description="Exact canonical org plus hostname kept as review-only until ambiguity is better understood.",
    ),
)

# Canonical fields to hash for gold_payload_hash.
GOLD_HASH_COLUMNS = [
    "master_entity_id",
    "source_presence_label",
    "asset_name",
    "primary_hostname",
    "primary_ip",
    "primary_mac",
    "serial_number",
    "normalised_org_name",
    "site_name",
    "os_name",
    "risk_score",
    "match_method",
    "match_rule_summary",
    "min_match_rule_rank",
    "source_count",
    "edge_count",
    "first_seen_at",
    "last_seen_at",
    "source_updated_at",
]
