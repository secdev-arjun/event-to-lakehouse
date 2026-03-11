**Gold Assets Current Schema (silver -> gold)**

This document describes the schema of `iceberg.gold.assets_current` produced by the gold pipeline in `scripts/gold/`. The schema is derived from the normalized silver schema (`TARGET_FIELDS`) plus additional gold lineage fields.

**Stage**

silver -> gold (current only)

**Inputs**

- `iceberg.silver.rapid7__assets__silver__current`
- `iceberg.silver.fortisiem__device__silver__current`
- `iceberg.silver.sentinalone__agents__silver__current`

**Output**

- `iceberg.gold.assets_current`

**Grain**

One row per canonical asset built by anchoring on SentinelOne and enriching with Rapid7 and FortiSIEM when available. The row is emitted only if the SentinelOne record matches at least one other source (see filter in `build_gold_assets.py`).

**Row inclusion rule (current implementation)**

A gold row is written only if at least one of the following is true:

- `seen_in_rapid7 = true`
- `seen_in_fortisiem = true`

This means SentinelOne-only records are excluded.

---

**Schema reference**

The table contains:

1. All normalized fields from `TARGET_FIELDS` (same names as silver) populated via survivorship rules.
2. Gold-specific lineage and matching fields appended by `build_gold_rows`.

**Identifiers and lineage**

| Field | Type | Meaning | Population / derivation | Notes |
| --- | --- | --- | --- | --- |
| schema_version | string | Schema version label | Coalesce of silver `schema_version` fields | Typically `silver.asset_observation.v1` |
| source | string | Record source | Literal `gold` | Overwrites silver values |
| entity_id | string | Gold canonical identifier | `gold_asset_id` | sha2 of canonical key string |
| entity_key_str | string | Canonical key string | `norm_site|primary_ip` or `norm_site|norm_host` | Built in `build_gold_rows` |
| payload_hash | string | Gold payload hash | Set to `gold_payload_hash` | Overwrites silver payload_hash |
| topic_name | string | Source topic name | Coalesce s -> r -> f | Derived from silver inputs |
| vendor_id | string | Vendor/source identifier | Coalesce s -> r -> f | Derived from silver inputs |

**Timestamps and audit**

| Field | Type | Meaning | Population / derivation |
| --- | --- | --- | --- |
| ingest_ts | timestamp | Ingest timestamp | Coalesce s -> r -> f |
| first_seen_at | timestamp | Earliest first_seen | `min_non_null(s, r, f)` |
| last_seen_at | timestamp | Latest last_seen | `max_non_null(s, r, f)` |
| source_updated_at | timestamp | Latest source update time | `max_non_null(s, r, f)` |
| event_time | timestamp | Event time | Coalesce s -> r -> f (currently mostly null) |

**Organization / site fields**

| Field | Type | Meaning | Population / derivation |
| --- | --- | --- | --- |
| site_id | string | Canonical site identifier | Coalesce s -> r -> f |
| site_name | string | Canonical site name | Coalesce s -> r -> f |

**Asset identity fields**

| Field | Type | Meaning | Population / derivation | Notes |
| --- | --- | --- | --- | --- |
| asset_uid | string | Source-level asset hash | Coalesce s -> r -> f | Source-specific hash from silver |
| source_system | string | Source system label | Literal `gold` | Overwrites silver values |
| rapid7_id | string | Rapid7 identifier | Coalesce s -> r -> f | Typically from Rapid7 |
| fortisiem_id | string | FortiSIEM identifier | Coalesce s -> r -> f | Typically from FortiSIEM |
| asset_name | string | Asset name | Coalesce s -> r -> f | |
| primary_hostname | string | Primary hostname | Coalesce s -> f | SentinelOne preferred |
| primary_ip | string | Primary IP | Coalesce s -> r | SentinelOne preferred |
| access_ip | string | Access IP | Coalesce s -> r -> f | |
| natural_id | string | Natural ID | Coalesce s -> r -> f | |
| approved | boolean | Approved flag | Coalesce s -> r -> f | |
| unmanaged | boolean | Unmanaged flag | Coalesce s -> r -> f | |

**Device / hardware fields**

| Field | Type | Meaning | Population / derivation |
| --- | --- | --- | --- |
| device_vendor | string | Device vendor | Coalesce s -> r -> f |
| device_model | string | Device model | Coalesce s -> r -> f |
| device_version | string | Device version | Coalesce s -> r -> f |
| cpu_count | int | CPU count | Coalesce s -> r -> f |
| memory_bytes | bigint | Memory bytes | Coalesce s -> r -> f |

**Operating system fields**

| Field | Type | Meaning | Population / derivation | Notes |
| --- | --- | --- | --- | --- |
| os_name | string | OS name | Coalesce s -> r -> f | SentinelOne preferred |
| os_family | string | OS family | Coalesce s -> r -> f | |
| os_vendor | string | OS vendor | Coalesce s -> r -> f | |
| os_product | string | OS product | Coalesce s -> r -> f | |
| os_version | string | OS version | Coalesce s -> r -> f | |
| os_architecture | string | OS architecture | Coalesce s -> r -> f | |
| os_certainty | double | OS certainty | Coalesce s -> r -> f | |

**Vulnerability / risk fields**

| Field | Type | Meaning | Population / derivation | Notes |
| --- | --- | --- | --- | --- |
| assessed_for_policies | boolean | Policy assessment | Coalesce s -> r -> f | |
| assessed_for_vulnerabilities | boolean | Vulnerability assessment | Coalesce s -> r -> f | |
| risk_score | double | Risk score | Coalesce r -> s -> f | Rapid7 preferred |
| raw_risk_score | double | Raw risk score | Coalesce s -> r -> f | |
| vuln_total | int | Total vulnerabilities | Coalesce s -> r -> f | |
| vuln_critical | int | Critical vulnerabilities | Coalesce s -> r -> f | |
| vuln_severe | int | Severe vulnerabilities | Coalesce s -> r -> f | |
| vuln_moderate | int | Moderate vulnerabilities | Coalesce s -> r -> f | |
| vuln_exploits | int | Exploits | Coalesce s -> r -> f | |
| vuln_malware_kits | int | Malware kits | Coalesce s -> r -> f | |

**Network and posture fields**

| Field | Type | Meaning | Population / derivation |
| --- | --- | --- | --- |
| host_domain | string | Host domain | SentinelOne only (`s_host_domain`) |
| ip_addresses | array<string> | All IPs | Coalesce s -> r -> f |
| external_ip | string | External IP | Coalesce s -> r -> f |
| posture_is_active | boolean | Active posture | Coalesce s -> r -> f |
| posture_firewall_enabled | boolean | Firewall enabled | Coalesce s -> r -> f |
| posture_network_quarantine_enabled | boolean | Quarantine enabled | Coalesce s -> r -> f |
| posture_active_threats | int | Active threats | Coalesce s -> r -> f |
| tags | array<string> | Tags | Coalesce s -> r -> f |

**Raw lineage / payload fields**

| Field | Type | Meaning | Population / derivation |
| --- | --- | --- | --- |
| raw_payload | string | Raw JSON payload | Coalesce s -> r -> f |
| raw_json | string | Raw JSON payload | Coalesce s -> r -> f |

**Gold lineage and matching fields (additional)**

| Field | Type | Meaning | Population / derivation |
| --- | --- | --- | --- |
| gold_asset_id | string | Canonical gold asset id | sha2 of `gold_entity_key_str` |
| sentinalone_entity_id | string | SentinelOne entity id | `s_entity_id` |
| rapid7_entity_id | string | Rapid7 entity id | `r_entity_id` |
| fortisiem_entity_id | string | FortiSIEM entity id | `f_entity_id` |
| seen_in_sentinalone | boolean | Presence flag | `sentinalone_entity_id is not null` |
| seen_in_rapid7 | boolean | Presence flag | `rapid7_entity_id is not null` |
| seen_in_fortisiem | boolean | Presence flag | `fortisiem_entity_id is not null` |
| matched_sources | array<string> | Sources present | Array of `sentinalone`, `rapid7`, `fortisiem` filtered for non-null |
| gold_payload_hash | string | Hash of canonical gold fields | sha2 of `GOLD_HASH_COLUMNS` struct |

---

**Hashing and audit**

`gold_payload_hash` is computed from the columns in `GOLD_HASH_COLUMNS` (`scripts/gold/config.py`):

- gold_asset_id
- site_name
- primary_ip
- primary_hostname
- host_domain
- os_name
- risk_score
- first_seen_at
- last_seen_at
- source_updated_at

`payload_hash` in gold is set equal to `gold_payload_hash`.

---

**Limitations (from implementation)**

- Gold is current-only and is overwritten each run (`write_gold_current` uses overwrite).
- Gold rows are anchored on SentinelOne; Rapid7-only or FortiSIEM-only assets are not included.
- SentinelOne-only assets are excluded due to the filter in `build_gold_assets.py`.
