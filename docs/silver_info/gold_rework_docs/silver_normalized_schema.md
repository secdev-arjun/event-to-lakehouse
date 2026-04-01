**Silver Normalized Schema (bronze_current -> silver)**

This document describes the normalized schema produced by the bronze_current -> silver pipeline. The schema is defined in `scripts/mapping/target.py` as `TARGET_FIELDS` and is used by the per-source normalization functions in `scripts/mapping/sources/*.py`, conformance rules in `scripts/bronze/contracts/assets_silver_contract.yaml`, and the merge logic in `scripts/bronze/bronze_assets_to_silver_assets.py`.

**Stage**

bronze_current -> silver

**Inputs**

- `iceberg.bronze_current.rapid7__assets__current`
- `iceberg.bronze_current.fortisiem__device__current`
- `iceberg.bronze_current.sentinalone__agents__current`

**Outputs**

Silver current tables (one per source):

- `iceberg.silver.rapid7__assets__silver__current`
- `iceberg.silver.fortisiem__device__silver__current`
- `iceberg.silver.sentinalone__agents__silver__current`

Silver history tables (one per source):

- `iceberg.silver.rapid7__assets__silver__history`
- `iceberg.silver.fortisiem__device__silver__history`
- `iceberg.silver.sentinalone__agents__silver__history`

**Normalization goals**

- Provide a single, shared, source-agnostic schema for assets/devices/agents.
- Preserve source identity (`source`, `entity_id`) while standardizing common fields.
- Provide consistent change detection via `payload_hash`.
- Add stable metadata for downstream layers (gold).

**Naming conventions**

- All normalized fields are lower_snake_case.
- `*_id` fields are string-typed identifiers.
- `*_ts` / `*_at` are timestamps where available.
- Arrays use `array<string>` and are sorted/distinct when conformed.

**Canonical vs source-specific fields**

- Canonical fields are defined in `TARGET_FIELDS` and intended to be present for all sources.
- Many fields are source-specific in practice and are set to NULL when not provided by a source.
- Canonical identity in silver is `(source, entity_id)`. `entity_id` comes from bronze_current and is not globally unique across sources.

**Source-specific gaps**

- Rapid7 does not provide many posture fields and several hardware fields, so they are NULL.
- FortiSIEM does not provide IP as `primary_ip`, OS details, or vulnerability metrics, so they are NULL.
- SentinelOne does not provide vulnerability counts or Rapid7/ FortiSIEM IDs, so they are NULL.

---

**Schema reference (silver current tables)**

All fields are nullable in the current implementation because `StructField(..., nullable=True)` is used for every field in `TARGET_FIELDS`.

**Identifiers and lineage**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| schema_version | string | true | Schema version tag | Constant | `mapping.target.SCHEMA_VERSION` set by `add_common_fields` | Current value: `silver.asset_observation.v1` |
| source | string | true | Source name for the record | bronze_current | Passed through from bronze_current | Examples: `rapid7__assets`, `fortisiem__device`, `sentinalone__agents` |
| entity_id | string | true | Source-specific entity identifier | bronze_current | Passed through from bronze_current | Cast to string in normalization |
| entity_key_str | string | true | Silver business key | Computed | Overwritten in `process_source` as `concat_ws('|', source, entity_id)` | `add_common_fields` sets a different key first, but it is overwritten |
| payload_hash | string | true | Hash of normalized business payload | Computed | `add_payload_hash` over `PAYLOAD_HASH_COLUMNS` | Used for change detection |
| topic_name | string | true | Kafka topic identifier for the source | Constant | Set by `add_common_fields` per source | `rapid7.assets.raw`, `fortisiem.devices.raw`, `centinel.agents.raw` |
| vendor_id | string | true | Source-provided vendor key | Source-specific | Rapid7: `id` (as `rapid7_id`); Forti: `natural_id`; Sentinel: `uuid` or `id` | Used in asset_uid construction |

**Timestamps and audit**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| ingest_ts | timestamp | true | Ingestion time | bronze_current | Parsed/coerced by `_coerce_ingest_ts` | Supports numeric epoch (ms/us/ns) or string timestamps |
| first_seen_at | timestamp | true | First observed in silver | Computed | Set during MERGE (insert uses `ingest_ts`) | Updated only on initial insert |
| last_seen_at | timestamp | true | Last observed in silver | Computed | Updated during MERGE (same hash updates `last_seen_at`) | Uses `greatest(t.last_seen_at, s.ingest_ts)` for unchanged rows |
| source_updated_at | timestamp | true | Source’s update time | Source-specific | Sentinel: `updatedAt` via `_as_timestamp`; Rapid7/Forti: NULL | Also used in dedupe ordering |
| event_time | timestamp | true | Source event timestamp | Source-specific | Not set in current mappings; remains NULL | Present for future use |

**Organization / site fields**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| site_id | string | true | Site or org identifier | Rapid7: `site_id`; Forti: `organization.attr_id`; Sentinel: `siteId`/`siteid` | Conformed with trim | `site_id` and `site_name` are normalized in contract |
| site_name | string | true | Site or org name | Rapid7: `site_name`; Forti: `organization.attr_name`; Sentinel: `siteName`/`sitename` | Conformed with trim + regex replacements | Contract normalizes multiple CCED and Securado variants |

**Asset identity fields**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| asset_uid | string | true | Stable hash for asset identity within source | Computed per source | sha2 of hostname + IP + vendor_id/rapid7_id/fortisiem_id | Built in each normalize_* function |
| source_system | string | true | Friendly source system label | Constant | `rapid7`, `fortisiem`, `sentinelone` | Lowercased in conformance |
| rapid7_id | string | true | Rapid7 asset ID | Rapid7: `id`; others NULL | Cast to string | NULL for non-Rapid7 |
| fortisiem_id | string | true | FortiSIEM device ID | Forti: `_id.$oid` OR `id` OR `naturalId`; others NULL | Cast to string | NULL for non-FortiSIEM |
| asset_name | string | true | Display name of asset | Rapid7: `hostName`; Forti: `name`; Sentinel: `computerName` | Trim in conformance | |
| primary_hostname | string | true | Primary hostname | Rapid7: `hostName`; Forti: `name`; Sentinel: `computerName` | Trim in conformance | |
| primary_ip | string | true | Primary IP | Rapid7: `ip`; Forti: NULL; Sentinel: `lastIpToMgmt` | Trim in conformance | Forti uses `access_ip` instead |
| access_ip | string | true | Access IP (if applicable) | Rapid7: NULL; Forti: `accessIp`; Sentinel: `lastIpToMgmt` | Trim in conformance | |
| natural_id | string | true | Natural ID from source | Forti: `naturalId`; Rapid7/Sentinel: NULL | Direct | |
| approved | boolean | true | Approval status | Forti: `approved`; others NULL | Direct | |
| unmanaged | boolean | true | Unmanaged flag | Forti: `unmanaged`; others NULL | Direct | |

**Device / hardware fields**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| device_vendor | string | true | Device vendor | Forti: `deviceType.vendor`; others NULL | Trim in conformance | |
| device_model | string | true | Device model | Forti: `deviceType.model`; others NULL | Trim in conformance | |
| device_version | string | true | Device version | Forti: `deviceType.version`; others NULL | Trim in conformance | |
| cpu_count | int | true | CPU count | Sentinel: `cpuCount`; others NULL | Cast to int | |
| memory_bytes | bigint | true | Memory in bytes | Sentinel: `totalMemory * 1024 * 1024`; others NULL | Converted to bytes | |

**Operating system fields**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| os_name | string | true | OS name | Rapid7: `os`; Sentinel: `osName`; Forti: NULL | Trim in conformance | |
| os_family | string | true | OS family | Rapid7: `osFingerprint.family`; Sentinel: `osType`; Forti: NULL | Trim + lower + mapping in conformance | Contract maps `win/windows` -> `Windows`, `linux` -> `Linux`, `mac/macos/darwin` -> `MacOS` |
| os_vendor | string | true | OS vendor | Rapid7: `osFingerprint.vendor`; others NULL | Trim in conformance | |
| os_product | string | true | OS product | Rapid7: `osFingerprint.product`; others NULL | Trim in conformance | |
| os_version | string | true | OS version | Rapid7: `osFingerprint.cpe.version` OR `osFingerprint.version`; Sentinel: `osRevision`; Forti: NULL | Trim in conformance | |
| os_architecture | string | true | OS architecture | Rapid7: `osFingerprint.architecture`; Sentinel: `osArch`; Forti: NULL | Trim + lower + regex remove spaces + mapping | Contract maps `x64/amd64/64bit` -> `x86_64`, `arm64/aarch64` -> `arm64` |
| os_certainty | double | true | OS certainty score | Rapid7: `osCertainty`; others NULL | Cast to double | |

**Vulnerability / risk fields**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| assessed_for_policies | boolean | true | Policy assessment flag | Rapid7: `assessedForPolicies`; others NULL | Direct | |
| assessed_for_vulnerabilities | boolean | true | Vulnerability assessment flag | Rapid7: `assessedForVulnerabilities`; others NULL | Direct | |
| risk_score | double | true | Risk score | Rapid7: `riskScore`; others NULL | Cast to double, scaled if >10 | Contract scales values greater than 10 by 0.1 |
| raw_risk_score | double | true | Raw risk score | Rapid7: `rawRiskScore`; others NULL | Cast to double | |
| vuln_total | int | true | Total vulnerabilities | Rapid7: `vulnerabilities.total`; others NULL | Cast to int | |
| vuln_critical | int | true | Critical vulnerabilities | Rapid7: `vulnerabilities.critical`; others NULL | Cast to int | |
| vuln_severe | int | true | Severe vulnerabilities | Rapid7: `vulnerabilities.severe`; others NULL | Cast to int | |
| vuln_moderate | int | true | Moderate vulnerabilities | Rapid7: `vulnerabilities.moderate`; others NULL | Cast to int | |
| vuln_exploits | int | true | Exploits count | Rapid7: `vulnerabilities.exploits`; others NULL | Cast to int | |
| vuln_malware_kits | int | true | Malware kits count | Rapid7: `vulnerabilities.malwareKits`; others NULL | Cast to int | |

**Network and posture fields**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| host_domain | string | true | Domain name | Sentinel: `domain`; others NULL | Trim in conformance | |
| ip_addresses | array<string> | true | IP address list | Sentinel: networkInterfaces.inet + lastIpToMgmt | Distinct, null-filtered, sorted | Built in `normalize_sentinel` |
| external_ip | string | true | External IP | Sentinel: `externalIp`; others NULL | Trim in conformance | |
| posture_is_active | boolean | true | Endpoint active | Sentinel: `isActive`; others NULL | Direct | |
| posture_firewall_enabled | boolean | true | Firewall enabled | Sentinel: `firewallEnabled`; others NULL | Direct | |
| posture_network_quarantine_enabled | boolean | true | Network quarantine enabled | Sentinel: `networkQuarantineEnabled`; others NULL | Direct | |
| posture_active_threats | int | true | Active threats | Sentinel: `activeThreats`; others NULL | Cast to int | |
| tags | array<string> | true | Tags | Sentinel: `tags.sentinelone`; others NULL | Distinct, null-filtered, sorted | Uses `_array_sort_nullable` in normalization and array rules in contract |

**Raw lineage / payload fields**

| Field | Type | Nullable | Meaning | Source origin(s) | Derivation / rules | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| raw_payload | string | true | Raw JSON payload | All sources | Same as `raw_json` | Useful for debugging |
| raw_json | string | true | Raw JSON payload | All sources | `to_json(struct(*))` of source row | `_corrupt_record` excluded |

---

**History table fields**

History tables extend the base schema with these additional fields defined in `HISTORY_EXTRA_FIELDS` in `scripts/bronze/bronze_assets_to_silver_assets.py`:

| Field | Type | Nullable | Meaning | Derivation / rules |
| --- | --- | --- | --- | --- |
| valid_from | timestamp | true | Start of validity window | Set to `ingest_ts` when inserted into history |
| valid_to | timestamp | true | End of validity window | NULL for current rows, set on expiration |
| is_current | boolean | true | Current version flag | True for latest version, set to false when superseded |
| version_id | string | true | Unique version identifier | sha2 of `source|entity_id|payload_hash|valid_from` |
| change_ts | timestamp | true | Change timestamp | Set to `ingest_ts` at insertion |

---

**Payload hash columns**

`payload_hash` is computed by `mapping.target.add_payload_hash` over the ordered list in `PAYLOAD_HASH_COLUMNS`:

- asset_uid
- source_system
- site_id
- site_name
- rapid7_id
- fortisiem_id
- asset_name
- primary_hostname
- primary_ip
- access_ip
- natural_id
- approved
- unmanaged
- device_vendor
- device_model
- device_version
- os_name
- os_family
- os_vendor
- os_product
- os_version
- os_architecture
- os_certainty
- assessed_for_policies
- assessed_for_vulnerabilities
- risk_score
- raw_risk_score
- vuln_total
- vuln_critical
- vuln_severe
- vuln_moderate
- vuln_exploits
- vuln_malware_kits
- host_domain
- ip_addresses
- external_ip
- cpu_count
- memory_bytes
- posture_is_active
- posture_firewall_enabled
- posture_network_quarantine_enabled
- posture_active_threats
- tags

`payload_hash` intentionally excludes identity fields such as `source`, `entity_id`, and timestamps so that it reflects only the normalized business payload.
