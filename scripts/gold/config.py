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

# Canonical fields to hash for gold_payload_hash
GOLD_HASH_COLUMNS = [
    "gold_asset_id",
    "site_name",
    "primary_ip",
    "primary_hostname",
    "host_domain",
    "os_name",
    "risk_score",
    "first_seen_at",
    "last_seen_at",
    "source_updated_at",
]
