**Bronze Current -> Silver Normalization (Mappings and Rules)**

This document describes how bronze_current source tables are transformed into the normalized silver schema. It is derived directly from:

- `scripts/bronze/bronze_assets_to_silver_assets.py`
- `scripts/mapping/sources/rapid7.py`
- `scripts/mapping/sources/fortisiem.py`
- `scripts/mapping/sources/sentinel.py`
- `scripts/mapping/target.py`
- `scripts/bronze/contracts/assets_silver_contract.yaml`

Raw source schema references used for redesign work:

- `docs/bronze_to_silver/raw_source_schemas.md`
- `docs/bronze_to_silver/raw_prod_bronze_describe_output.txt`

---

**A. Overview**

**What bronze_current contains**

Each bronze_current table contains raw source fields plus the required metadata columns `source`, `entity_id`, and `ingest_ts`. `entity_id` is created in the prior stage (bronze_raw -> bronze_current) and is considered the source-specific logical identifier for the object.

**What the normalization step does**

For each source table, the pipeline:

- Reads the entire bronze_current table.
- Coerces `ingest_ts` into a timestamp using `_coerce_ingest_ts`.
- Applies source-specific field mapping and normalization (`normalize_rapid7`, `normalize_fortisiem`, `normalize_sentinel`).
- Rewrites `entity_key_str` to be `source|entity_id`.
- Deduplicates to the latest record per `(source, entity_id)`.
- Applies conformance rules from `assets_silver_contract.yaml` (trim/lower/regex, mapping, array cleaning).
- Computes `payload_hash` based on `PAYLOAD_HASH_COLUMNS`.
- Upserts into silver current and silver history tables using MERGE logic.

**Why normalization is required before gold**

Gold expects a consistent schema across sources. The normalization step standardizes field names and data types, enabling deterministic matching and survivorship in the gold layer.

---

**B. Pipeline mechanics from code**

**Ingest timestamp coercion**

`_coerce_ingest_ts` attempts to parse `ingest_ts` as:

- Epoch seconds, milliseconds, microseconds, or nanoseconds based on magnitude.
- ISO-like strings (handles `T` and trailing `Z`).

**Deduping logic**

`_dedupe_latest` partitions by `(source, entity_id)` and orders by:

1. `source_updated_at`
2. `event_time`
3. `ingest_ts`

The latest row is retained using `row_number` over this window.

**Conformance rules**

All fields are passed through `conform_df` which applies rules defined in `assets_silver_contract.yaml` to normalize casing, trim whitespace, normalize site names, map OS families and architectures, sanitize arrays, and scale risk score.

**Merge logic into current**

`merge_with_retry` merges by `(source, entity_id)`:

- If `payload_hash` is unchanged, update `last_seen_at` and `ingest_ts`.
- If `payload_hash` changed, update all columns except `first_seen_at`; set `last_seen_at = ingest_ts`.
- If new, insert with `first_seen_at = ingest_ts` and `last_seen_at = ingest_ts`.

**Merge logic into history**

`merge_history_with_retry`:

- Inserts new and changed rows into history with `valid_from = ingest_ts`, `valid_to = NULL`, `is_current = true`.
- Expires previous versions by setting `valid_to = new.valid_from` and `is_current = false` for the same `(source, entity_id)` when `payload_hash` changes.
- `version_id` is sha2 of `source|entity_id|payload_hash|valid_from`.

---

**C. Source-by-source mapping sections**

The mapping tables below show the exact implementation for each source. All fields not explicitly populated in `normalize_*` are later created as NULL via `ensure_columns`.

**Rapid7 (scripts/mapping/sources/rapid7.py)**

Key identifiers

- `source`: passed through from bronze_current.
- `entity_id`: passed through from bronze_current.
- `rapid7_id`: `id` from Rapid7 payload.
- `vendor_id`: set to `rapid7_id` via `add_common_fields`.

Mapping table (Rapid7)

| Normalized field | Source field(s) / logic | Transformation / notes |
| --- | --- | --- |
| schema_version | constant | `silver.asset_observation.v1` via `add_common_fields` |
| source | bronze_current.source | passthrough |
| entity_id | bronze_current.entity_id | cast to string |
| entity_key_str | computed later | overwritten to `source|entity_id` in `process_source` |
| payload_hash | computed later | `add_payload_hash` over `PAYLOAD_HASH_COLUMNS` |
| topic_name | constant | `rapid7.assets.raw` |
| vendor_id | rapid7_id | via `add_common_fields` |
| ingest_ts | bronze_current.ingest_ts | parsed via `_coerce_ingest_ts` |
| first_seen_at | set in merge | uses `ingest_ts` on insert |
| last_seen_at | set in merge | updated on merge |
| source_updated_at | NULL | passed as `lit(None)` |
| event_time | NULL | not populated |
| asset_uid | sha2(hostname|ip|rapid7_id) | lower/trim applied in normalization |
| source_system | constant | `rapid7` |
| site_id | `site_id` | cast string, trimmed in contract |
| site_name | `site_name` | cast string, regex normalized in contract |
| rapid7_id | `id` | cast string |
| fortisiem_id | NULL | not provided |
| asset_name | `hostName` | trimmed in contract |
| primary_hostname | `hostName` | trimmed in contract |
| primary_ip | `ip` | trimmed in contract |
| access_ip | NULL | not provided |
| natural_id | NULL | not provided |
| approved | NULL | not provided |
| unmanaged | NULL | not provided |
| device_vendor | NULL | not provided |
| device_model | NULL | not provided |
| device_version | NULL | not provided |
| os_name | `os` | trimmed in contract |
| os_family | `osFingerprint.family` | normalized in contract |
| os_vendor | `osFingerprint.vendor` | trimmed in contract |
| os_product | `osFingerprint.product` | trimmed in contract |
| os_version | `osFingerprint.cpe.version` or `osFingerprint.version` | coalesce |
| os_architecture | `osFingerprint.architecture` | normalized in contract |
| os_certainty | `osCertainty` | cast double |
| assessed_for_policies | `assessedForPolicies` | direct |
| assessed_for_vulnerabilities | `assessedForVulnerabilities` | direct |
| risk_score | `riskScore` | cast double, scaled if > 10 |
| raw_risk_score | `rawRiskScore` | cast double |
| vuln_total | `vulnerabilities.total` | cast int |
| vuln_critical | `vulnerabilities.critical` | cast int |
| vuln_severe | `vulnerabilities.severe` | cast int |
| vuln_moderate | `vulnerabilities.moderate` | cast int |
| vuln_exploits | `vulnerabilities.exploits` | cast int |
| vuln_malware_kits | `vulnerabilities.malwareKits` | cast int |
| host_domain | NULL | not provided |
| ip_addresses | NULL | not provided |
| external_ip | NULL | not provided |
| cpu_count | NULL | not provided |
| memory_bytes | NULL | not provided |
| posture_is_active | NULL | not provided |
| posture_firewall_enabled | NULL | not provided |
| posture_network_quarantine_enabled | NULL | not provided |
| posture_active_threats | NULL | not provided |
| tags | NULL | not provided |
| raw_payload | raw_json | `raw_json` copied |
| raw_json | full source row | `to_json(struct(*))` excluding `_corrupt_record` |

**FortiSIEM (scripts/mapping/sources/fortisiem.py)**

Key identifiers

- `source`: passed through from bronze_current.
- `entity_id`: passed through from bronze_current.
- `fortisiem_id`: `coalesce(_id.$oid, id, naturalId)`.
- `vendor_id`: uses `natural_id` via `add_common_fields`.

Mapping table (FortiSIEM)

| Normalized field | Source field(s) / logic | Transformation / notes |
| --- | --- | --- |
| schema_version | constant | `silver.asset_observation.v1` |
| source | bronze_current.source | passthrough |
| entity_id | bronze_current.entity_id | cast string |
| entity_key_str | computed later | overwritten to `source|entity_id` |
| payload_hash | computed later | `add_payload_hash` |
| topic_name | constant | `fortisiem.devices.raw` |
| vendor_id | natural_id | from `naturalId` |
| ingest_ts | bronze_current.ingest_ts | parsed via `_coerce_ingest_ts` |
| first_seen_at | set in merge | uses `ingest_ts` on insert |
| last_seen_at | set in merge | updated on merge |
| source_updated_at | NULL | not provided |
| event_time | NULL | not populated |
| asset_uid | sha2(hostname|access_ip|fortisiem_id) | lower/trim applied in normalization |
| source_system | constant | `fortisiem` |
| site_id | `organization.attr_id` | cast string |
| site_name | `organization.attr_name` | cast string, regex normalized in contract |
| rapid7_id | NULL | not provided |
| fortisiem_id | `_id.$oid` or `id` or `naturalId` | coalesce |
| asset_name | `name` | trimmed in contract |
| primary_hostname | `name` | trimmed in contract |
| primary_ip | NULL | not provided |
| access_ip | `accessIp` | trimmed in contract |
| natural_id | `naturalId` | direct |
| approved | `approved` | direct |
| unmanaged | `unmanaged` | direct |
| device_vendor | `deviceType.vendor` | trimmed in contract |
| device_model | `deviceType.model` | trimmed in contract |
| device_version | `deviceType.version` | trimmed in contract |
| os_name | NULL | not provided |
| os_family | NULL | not provided |
| os_vendor | NULL | not provided |
| os_product | NULL | not provided |
| os_version | NULL | not provided |
| os_architecture | NULL | not provided |
| os_certainty | NULL | not provided |
| assessed_for_policies | NULL | not provided |
| assessed_for_vulnerabilities | NULL | not provided |
| risk_score | NULL | not provided |
| raw_risk_score | NULL | not provided |
| vuln_total | NULL | not provided |
| vuln_critical | NULL | not provided |
| vuln_severe | NULL | not provided |
| vuln_moderate | NULL | not provided |
| vuln_exploits | NULL | not provided |
| vuln_malware_kits | NULL | not provided |
| host_domain | NULL | not provided |
| ip_addresses | NULL | not provided |
| external_ip | NULL | not provided |
| cpu_count | NULL | not provided |
| memory_bytes | NULL | not provided |
| posture_is_active | NULL | not provided |
| posture_firewall_enabled | NULL | not provided |
| posture_network_quarantine_enabled | NULL | not provided |
| posture_active_threats | NULL | not provided |
| tags | NULL | not provided |
| raw_payload | raw_json | `raw_json` copied |
| raw_json | full source row | `to_json(struct(*))` excluding `_corrupt_record` |

**SentinelOne (scripts/mapping/sources/sentinel.py)**

Key identifiers

- `source`: passed through from bronze_current.
- `entity_id`: passed through from bronze_current.
- `vendor_id`: `coalesce(uuid, id)`.
- `topic_name`: `centinel.agents.raw` (note spelling).

Mapping table (SentinelOne)

| Normalized field | Source field(s) / logic | Transformation / notes |
| --- | --- | --- |
| schema_version | constant | `silver.asset_observation.v1` |
| source | bronze_current.source | passthrough |
| entity_id | bronze_current.entity_id | cast string |
| entity_key_str | computed later | overwritten to `source|entity_id` |
| payload_hash | computed later | `add_payload_hash` |
| topic_name | constant | `centinel.agents.raw` |
| vendor_id | `uuid` or `id` | coalesce |
| ingest_ts | bronze_current.ingest_ts | parsed via `_coerce_ingest_ts` |
| first_seen_at | set in merge | uses `ingest_ts` on insert |
| last_seen_at | set in merge | updated on merge |
| source_updated_at | `updatedAt` | via `_as_timestamp` |
| event_time | NULL | not populated |
| asset_uid | sha2(hostname|ip|vendor_id) | lower/trim applied in normalization |
| source_system | constant | `sentinelone` |
| site_id | `siteId`/`siteid`/`site_id` | coalesce, cast string |
| site_name | `siteName`/`sitename`/`site_name` | coalesce, cast string, regex normalized in contract |
| rapid7_id | NULL | not provided |
| fortisiem_id | NULL | not provided |
| asset_name | `computerName` | trimmed in contract |
| primary_hostname | `computerName` | trimmed in contract |
| primary_ip | `lastIpToMgmt` | trimmed in contract |
| access_ip | `lastIpToMgmt` | trimmed in contract |
| natural_id | NULL | not provided |
| approved | NULL | not provided |
| unmanaged | NULL | not provided |
| device_vendor | NULL | not provided |
| device_model | NULL | not provided |
| device_version | NULL | not provided |
| os_name | `osName` | trimmed in contract |
| os_family | `osType` | normalized in contract |
| os_vendor | NULL | not provided |
| os_product | NULL | not provided |
| os_version | `osRevision` | trimmed in contract |
| os_architecture | `osArch` | normalized in contract |
| os_certainty | NULL | not provided |
| assessed_for_policies | NULL | not provided |
| assessed_for_vulnerabilities | NULL | not provided |
| risk_score | NULL | not provided |
| raw_risk_score | NULL | not provided |
| vuln_total | NULL | not provided |
| vuln_critical | NULL | not provided |
| vuln_severe | NULL | not provided |
| vuln_moderate | NULL | not provided |
| vuln_exploits | NULL | not provided |
| vuln_malware_kits | NULL | not provided |
| host_domain | `domain` | trimmed in contract |
| ip_addresses | `networkInterfaces.inet` + `lastIpToMgmt` | flattened, unioned, distinct, null filtered, sorted |
| external_ip | `externalIp` | trimmed in contract |
| cpu_count | `cpuCount` | cast int |
| memory_bytes | `totalMemory * 1024 * 1024` | MB to bytes |
| posture_is_active | `isActive` | direct |
| posture_firewall_enabled | `firewallEnabled` | direct |
| posture_network_quarantine_enabled | `networkQuarantineEnabled` | direct |
| posture_active_threats | `activeThreats` | cast int |
| tags | `tags.sentinelone` | sorted array |
| raw_payload | raw_json | `raw_json` copied |
| raw_json | full source row | `to_json(struct(*))` excluding `_corrupt_record` |

---

**D. Data contract for Silver**

**Business key and identity**

- The silver business key is `(source, entity_id)`.
- `entity_id` is source-specific and originates in bronze_current.
- `entity_key_str` is always recomputed as `source|entity_id` before conformance.

**Change detection**

- `payload_hash` is the canonical change detector.
- Any change to the fields listed in `PAYLOAD_HASH_COLUMNS` will change `payload_hash`.

**Current table contract**

- One row per `(source, entity_id)`.
- Latest version is kept in `*_silver__current`.
- `first_seen_at` reflects first appearance in silver; `last_seen_at` updates on each run.

**History table contract**

- One row per version in `*_silver__history`.
- `valid_from` and `valid_to` represent SCD validity windows.
- `is_current` indicates the latest version.

**What Silver guarantees**

- A stable normalized schema across sources.
- Deterministic payload hashing.
- Standardized site and OS normalization.

**What Silver does not guarantee**

- Cross-source deduplication.
- Global uniqueness of `entity_id`.
- Completeness of all normalized fields across sources.

---

**E. Field-specific rules**

Rules are applied via `apply_rules` in `bronze_assets_to_silver_assets.py` based on `assets_silver_contract.yaml`.

Key rules include:

- `trim` on many string fields.
- `lower` on `source_system` and some OS values.
- `regex_replace` for site name normalization (CCED and Securado variants).
- `map` for `os_family` and `os_architecture` standardization.
- `scale_if_gt` for `risk_score` where values > 10 are scaled by 0.1.
- Array operations on `ip_addresses` and `tags`: filter nulls, distinct, sort.

---

**F. Exact conformance rules (selected examples)**

From `assets_silver_contract.yaml`:

`site_name` normalization examples: `"CCED Windows QUARTER"` -> `CCED`, `"CCEDORG"` -> `CCED`, `"CC Energy Development Oman"` -> `CCED`, and all of `"Securado"`, `"Securado HQ"`, `"Securado - HQ"`, `"Securado HO"`, `"Securado - HO"`, `"Securado HeadOffice"`, `"Securado - Head Office"`, `"Securado Head Office"`, `"Securado-IN"` -> `Securado`.

`os_family` mapping: `win/windows` -> `Windows`, `linux` -> `Linux`, `mac/macos/darwin` -> `MacOS`.

`os_architecture` mapping: `64bit/x64/amd64` -> `x86_64`, `arm64/aarch64` -> `arm64`.

---

**G. Inferred logic (explicitly marked)**

- The pipeline assumes `entity_id` has already been created in bronze_current. This is inferred from the fact that the normalization functions pass through `entity_id` and no new `entity_id` is generated here.
- `event_time` is present in the schema but is not populated in current mappings. This is inferred from the normalization code which never sets `event_time`.
