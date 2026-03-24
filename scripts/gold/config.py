import os

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

# Output GOLD current table
GOLD_ASSETS_CURRENT_TABLE = os.getenv(
    "GOLD_ASSETS_CURRENT_TABLE",
    "iceberg.gold.assets_current",
)

# Optional history output table for future phase
GOLD_ASSETS_HISTORY_TABLE = os.getenv(
    "GOLD_ASSETS_HISTORY_TABLE",
    "iceberg.gold.assets_history",
)

SOURCE_R7 = "rapid7"
SOURCE_FSM = "fortisiem"
SOURCE_S1 = "sentinalone"

PRIVATE_IP_REGEX = r"^(10\\.|192\\.168\\.|172\\.(1[6-9]|2[0-9]|3[0-1])\\.)"
INVALID_MAC_REGEX = r"^(00:00:00:00:00:00|ff:ff:ff:ff:ff:ff)$"
VIRTUAL_OUI_REGEX = r"^(00:0c:29:|00:50:56:|00:15:5d:)"

MATCH_SCORES = {
    "serial_exact": 100,
    "primary_mac_exact": 100,
    "primary_mac_in_array": 98,
    "mac_overlap": 95,
    "hostname_org": 85,
    "hostname_site": 80,
    "hostname_os": 70,
    "ip_org": 80,
    "ip_site": 70,
    "access_ip_org": 75,
    "access_ip_site": 65,
    "ip_array_org": 65,
    "primary_ip_only": 30,
    "hostname_only": 40,
    "ip_array_only": 35,
    "virtual_mac_only": 45,
}

REVIEW_ONLY_METHODS = {
    "primary_ip_only",
    "hostname_only",
    "ip_array_only",
    "virtual_mac_only",
}

TIER1_METHODS = {
    "serial_exact",
    "primary_mac_exact",
    "primary_mac_in_array",
    "mac_overlap",
}

TIER2_METHODS = {
    "hostname_org",
    "hostname_site",
    "hostname_os",
    "ip_org",
    "ip_site",
    "access_ip_org",
    "access_ip_site",
    "ip_array_org",
}

AUTO_MERGE_TIER1_MIN_SCORE = 95
AUTO_MERGE_TIER2_MIN_SCORE = 65
REVIEW_MIN_SCORE = 40
REVIEW_MAX_SCORE = 64

# Canonical fields to hash for gold_payload_hash
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
    "match_score",
    "first_seen_at",
    "last_seen_at",
    "source_updated_at",
]
