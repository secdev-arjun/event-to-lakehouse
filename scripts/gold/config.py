from __future__ import annotations

import os
from dataclasses import dataclass


# Input SILVER current tables
RAPID7_SILVER_CURRENT_TABLE = os.getenv(
    "RAPID7_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__rapid7__assets",
)
FORTI_SILVER_CURRENT_TABLE = os.getenv(
    "FORTI_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__fortisiem__devices",
)
SENTINEL_SILVER_CURRENT_TABLE = os.getenv(
    "SENTINEL_SILVER_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__silver__current__sentinelone__agents",
)

# Output GOLD tables
GOLD_ASSETS_CURRENT_TABLE = os.getenv(
    "GOLD_ASSETS_CURRENT_TABLE",
    "iceberg.cmdb.cmdb__gold__current",
)
GOLD_ASSETS_HISTORY_TABLE = os.getenv(
    "GOLD_ASSETS_HISTORY_TABLE",
    "iceberg.cmdb.cmdb__gold__history",
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
        auto_accept=True,
        description="Exact canonical org plus IP fallback rule auto-accepted when one-to-one and non-ambiguous.",
    ),
    MatchRuleDefinition(
        rule_name="org_hostname_exact",
        rule_rank=8,
        key_parts=("org_key", "hostname_key"),
        key_columns_used=("normalised_org_name", "primary_hostname"),
        applicable_pairs=PAIRWISE_SOURCE_PAIRS,
        auto_accept=True,
        description="Exact canonical org plus hostname fallback rule auto-accepted when one-to-one and non-ambiguous.",
    ),
)

# Canonical fields to hash for gold_payload_hash.
GOLD_HASH_COLUMNS = [
    "master_entity_id",
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
    "first_seen_at",
    "last_seen_at",
    "source_updated_at",
]
