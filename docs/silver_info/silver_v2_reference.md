# Silver V2 Reference (Current Implementation)

## Purpose
This document describes the **actual** Silver V2 implementation in the codebase so downstream logic (especially Gold) can be updated safely.

Scope covered:
- Silver schema (`108` fields)
- Mapping behavior from all 3 source normalizers
- Transformation semantics that affect matching/survivorship
- What is intentionally null or intentionally not surfaced
- What Gold should use vs. avoid as match evidence

Code sources used:
- `scripts/mapping/target.py`
- `scripts/mapping/sources/rapid7.py`
- `scripts/mapping/sources/fortisiem.py`
- `scripts/mapping/sources/sentinel.py`
- `scripts/bronze/contracts/assets_silver_contract.yaml`
- `scripts/bronze/bronze_assets_to_silver_assets.py`

---

## Silver V2 Data Model

### Core design rules
1. One row per source observation (Silver does not cross-source merge).
2. Semantic field names (no source-specific prefixes in output schema).
3. Keep both canonicalized evidence and raw fidelity (`raw_json`, `raw_payload`).
4. Preserve multi-value evidence (`ip_addresses_raw`, arrays for hostnames/MACs/IPs).
5. Keep uncertain values as `null` instead of writing semantically wrong values.

### Bronze -> Silver flow (implemented)

| Source system | Bronze current input | Normalizer | Silver current output | Silver history output |
|---|---|---|---|---|
| Rapid7 | `iceberg.bronze_current.rapid7__assets__current` | `normalize_rapid7` | `iceberg.silver.rapid7__assets__silver__current` | `iceberg.silver.rapid7__assets__silver__history` |
| FortiSIEM | `iceberg.bronze_current.fortisiem__device__current` | `normalize_fortisiem` | `iceberg.silver.fortisiem__device__silver__current` | `iceberg.silver.fortisiem__device__silver__history` |
| SentinelOne | `iceberg.bronze_current.sentinalone__agents__current` | `normalize_sentinel` | `iceberg.silver.sentinalone__agents__silver__current` | `iceberg.silver.sentinalone__agents__silver__history` |

Rapid7 has one additional enrichment input:
- `iceberg.bronze_current.rapid7__site__current` (left-joined in `normalize_rapid7` by `site_id` -> `id`)

### Target tables
- `iceberg.silver.rapid7__assets__silver__current`
- `iceberg.silver.rapid7__assets__silver__history`
- `iceberg.silver.fortisiem__device__silver__current`
- `iceberg.silver.fortisiem__device__silver__history`
- `iceberg.silver.sentinalone__agents__silver__current`
- `iceberg.silver.sentinalone__agents__silver__history`
- `iceberg.silver.silver_current_checkpoint`

### Schema version
- `schema_version = silver.asset_observation.v2`

---

## Full Silver Schema (Grouped)

### 1) Provenance / Row Identity
- `schema_version` string
- `source` string (upstream source label such as `rapid7__assets`, `fortisiem__device`, `sentinalone__agents`)
- `entity_id` string
- `entity_key_str` string
- `payload_hash` string
- `topic_name` string
- `vendor_id` string
- `ingest_ts` timestamp
- `first_seen_at` timestamp
- `last_seen_at` timestamp
- `source_updated_at` timestamp
- `event_time` timestamp
- `asset_uid` string
- `source_system` string (`rapid7` / `fortisiem` / `sentinelone`)
- `source_record_id` string
- `source_natural_id` string
- `source_site_ref_id` string
- `source_display_name` string

### 2) Asset / Network Identity Evidence
- `asset_name` string
- `primary_hostname` string
- `hostnames` array<string>
- `host_domain` string
- `primary_ip` string
- `ip_addresses` array<string> (filtered canonical list)
- `ip_addresses_raw` array<string> (unfiltered evidence list)
- `ipv6_addresses` array<string>
- `primary_mac` string
- `mac_addresses` array<string>
- `gateway_mac_addresses` array<string>
- `virtual_mac_addresses` array<string>
- `serial_number` string
- `access_ip` string
- `external_ip` string

### 3) Ownership / Org / Site Context
- `approved` boolean
- `unmanaged` boolean
- `tags` array<string>
- `site_id` string
- `site_name` string
- `normalised_org_name` string
- `account_id` string
- `account_name` string
- `org_map_matched` boolean
- `site_description` string
- `site_type` string
- `site_importance` string
- `site_assets_count` bigint
- `site_risk_score` double
- `site_last_scan_time` timestamp
- `site_scan_engine` bigint
- `site_scan_template` string
- `site_vuln_total` bigint
- `site_vuln_critical` bigint
- `site_vuln_severe` bigint
- `site_vuln_moderate` bigint

### 4) Device / OS Attributes
- `device_vendor` string
- `device_model` string
- `device_version` string
- `platform_version` string
- `asset_type` string
- `os_name` string
- `os_family` string
- `os_vendor` string
- `os_product` string
- `os_version` string
- `os_architecture` string
- `os_edition` string
- `os_certainty` double
- `cpu_count` int
- `memory_bytes` bigint
- `system_uptime` bigint

### 5) Risk / Vulnerability
- `assessed_for_policies` boolean
- `assessed_for_vulnerabilities` boolean
- `risk_score` double
- `raw_risk_score` double
- `vuln_total` int
- `vuln_critical` int
- `vuln_severe` int
- `vuln_moderate` int
- `vuln_exploits` int
- `vuln_malware_kits` int

### 6) Posture / Operational Signals
- `posture_is_active` boolean
- `posture_firewall_enabled` boolean
- `posture_network_quarantine_enabled` boolean
- `posture_active_threats` int
- `scan_status` string
- `operational_state` string
- `operational_state_expiration` string
- `network_status` string
- `ranger_status` string
- `mitigation_mode` string
- `mitigation_mode_suspicious` string
- `machine_type` string
- `group_id` string
- `group_name` string
- `last_logged_in_user_name` string
- `active_protection_modes` array<string>
- `missing_permissions` array<string>
- `user_actions_needed` array<string>
- `location_names` array<string>
- `device_status` string
- `discover_method` string
- `event_log_status` string
- `perf_mon_status` string
- `update_method` string
- `services_count` int
- `software_count` int

### 7) Raw Fidelity
- `raw_payload` string
- `raw_json` string

---

## Shared Transformation Semantics

All three normalizers use helper logic from `target.py`.

### String cleaning
- `clean_string`: cast to string, trim, empty -> null
- `clean_string_array`: trim entries, remove null/empty, distinct, sort

### MAC normalization
- `normalize_mac_string`: lowercase, `-` -> `:`, remove non-hex chars except `:`
- `normalize_mac_array`: per-element normalize + clean array

### IP filtering
- `filter_matching_ip_addresses` removes:
  - loopback `127.*`
  - APIPA `169.254.*`
- `ip_addresses_raw` keeps pre-filter evidence

### Serial filtering
- `valid_hardware_serial` nulls likely placeholders:
  - `vmware-*`
  - `to be filled by o.e.m.`
  - `default string`, `n/a`, `na`, `unknown`, `null`

### Org normalization + matched flag
- `normalised_org_name` applies regex replacements defined in `ORG_NAME_NORMALIZATION_RULES`
- `org_map_matched = lower(clean(site_name)) != lower(clean(normalised_org_name))`

### Payload hash behavior
`payload_hash` is recalculated after conformance using all target fields **except**:
- `schema_version`, `source`, `entity_id`, `entity_key_str`, `payload_hash`, `topic_name`, `vendor_id`,
  `ingest_ts`, `first_seen_at`, `last_seen_at`, `source_updated_at`, `event_time`, `raw_payload`, `raw_json`

---

## Per-Source Mapping Details

## Rapid7 (`normalize_rapid7`)

### Identity / provenance
- `source_system = rapid7`
- `source_record_id <- id`
- `source_natural_id <- null`
- `source_site_ref_id <- coalesce(site_id, site_lookup.id)`
- `source_display_name <- null`
- `vendor_id <- source_record_id` (via `add_common_fields`)
- `topic_name = rapid7.assets.raw`
- `source_updated_at <- null`

### Host / IP / MAC
- `asset_name <- hostName`
- `primary_hostname <- hostName`
- `hostnames <- union([hostName], hostNames[].name)`
- `primary_ip <- ip`
- `ip_addresses_raw <- union([ip], addresses[].ip)`
- `ip_addresses <- filter_matching_ip_addresses(ip_addresses_raw)`
- `primary_mac <- mac (normalized)`
- `mac_addresses <- union([mac], addresses[].mac) (normalized)`
- `gateway_mac_addresses <- null`
- `virtual_mac_addresses <- null`

### Site enrichment
Primary join now uses site id, not name:
- Join key: `rapid7__assets.site_id` -> `rapid7__site.id`
- `site_name <- coalesce(site_lookup.name, assets.site_name)`
- `site_description/site_type/site_importance/... <- site lookup fields`
- `site_last_scan_time <- to_timestamp(site_lookup.lastScanTime)`

### OS / risk / counts
- `asset_type <- type`
- `os_name <- os`
- `os_family <- osFingerprint.family`
- `os_vendor <- osFingerprint.vendor`
- `os_product <- osFingerprint.product`
- `os_version <- coalesce(osFingerprint.cpe.version, osFingerprint.version)`
- `os_architecture <- osFingerprint.architecture`
- `os_certainty <- osCertainty`
- `assessed_for_policies <- assessedForPolicies`
- `assessed_for_vulnerabilities <- assessedForVulnerabilities`
- `risk_score <- riskScore`
- `raw_risk_score <- rawRiskScore`
- `vuln_* <- vulnerabilities.*`
- `services_count <- size(services)`
- `software_count <- size(software)`

### Raw fidelity
- `raw_json` includes full normalized source row plus site-enrichment intermediate columns
- `raw_payload = raw_json`

### asset_uid
`sha2(lower(trim(primary_hostname)) | lower(trim(primary_ip)) | source_record_id)`

---

## FortiSIEM (`normalize_fortisiem`)

### Identity / provenance
- `source_system = fortisiem`
- `source_record_id <- coalesce(_id.$oid, id, naturalId)`
- `source_natural_id <- naturalId`
- `source_site_ref_id <- organization.attr_id`
- `source_display_name <- name`
- `vendor_id <- coalesce(source_natural_id, source_record_id)`
- `topic_name = fortisiem.devices.raw`
- `source_updated_at <- null`

### Hostname rule (locked)
- `primary_hostname <- null`
- `hostnames <- null`
- `name` is represented as `source_display_name` and `asset_name`

### IP mapping
- `access_ip <- accessIp`
- `primary_ip <- null`
- `ip_addresses_raw <- union([accessIp], interfaces.networkinterface[].ipv4Addr)`
- `ip_addresses <- filter_matching_ip_addresses(ip_addresses_raw)`
- `ipv6_addresses <- interfaces.networkinterface[].ipv6Addr`

### MAC mapping
- `mac_addresses <- interfaces[].macAddr where macIsVirtual=false (normalized)`
- `virtual_mac_addresses <- interfaces[].macAddr where macIsVirtual=true (normalized)`
- `gateway_mac_addresses <- null`
- `primary_mac` deterministic selection:
  1. non-virtual, non-WAN, `operStatus=up`, non-empty MAC
  2. order by `snmpIndex` ascending
  3. first MAC
  4. fallback: same but without `operStatus=up`
  5. else null

### Device / operational
- `device_vendor/model/version <- deviceType.*`
- `platform_version <- version`
- `os_edition <- osEdition`
- `system_uptime <- systemUpTime`
- `approved <- approved`
- `unmanaged <- unmanaged`
- `device_status <- deviceStatus`
- `discover_method <- discoverMethod`
- `event_log_status <- eventLogStatus`
- `perf_mon_status <- perfMonStatus`
- `update_method <- updateMethod`

### Serial handling
- `serial_number <- valid_hardware_serial(hwSerialNum)`
- `bios` and `osSerialNum` are not surfaced as separate silver fields

### Raw fidelity
- `raw_json` and `raw_payload` preserved

### asset_uid
`sha2(lower(trim(source_display_name)) | lower(trim(access_ip)) | source_record_id)`

---

## SentinelOne (`normalize_sentinel`)

### Identity / provenance
- `source_system = sentinelone`
- `source_record_id <- id`
- `source_natural_id <- uuid`
- `source_site_ref_id <- coalesce(siteId, siteid, site_id)`
- `source_display_name <- null`
- `vendor_id <- coalesce(source_natural_id, source_record_id)`
- `topic_name = centinel.agents.raw` (note: literal value in code)
- `source_updated_at <- to_timestamp(updatedAt)`

### Host / IP / MAC
- `asset_name <- computerName`
- `primary_hostname <- computerName`
- `hostnames <- [computerName]`
- `host_domain <- domain`
- `primary_ip <- lastIpToMgmt`
- `access_ip <- lastIpToMgmt`
- `external_ip <- externalIp`
- `ip_addresses_raw <- union(networkInterfaces[].inet flattened, [lastIpToMgmt])`
- `ip_addresses <- filter_matching_ip_addresses(ip_addresses_raw)`
- `primary_mac <- null`
- `mac_addresses <- networkInterfaces[].physical (normalized)`
- `gateway_mac_addresses <- networkInterfaces[].gatewayMacAddress (normalized)`
- `virtual_mac_addresses <- null`

### Account / org / posture
- `site_id <- source_site_ref_id`
- `site_name <- coalesce(siteName, sitename, site_name)`
- `normalised_org_name <- normalize(site_name)`
- `account_id <- accountId`
- `account_name <- accountName`
- `org_map_matched <- site_name vs normalised_org_name`
- `posture_is_active <- isActive`
- `posture_firewall_enabled <- firewallEnabled`
- `posture_network_quarantine_enabled <- networkQuarantineEnabled`
- `posture_active_threats <- activeThreats`

### Endpoint operational fields
- `scan_status <- scanStatus`
- `operational_state <- operationalState`
- `operational_state_expiration <- operationalStateExpiration`
- `network_status <- networkStatus`
- `ranger_status <- rangerStatus`
- `mitigation_mode <- mitigationMode`
- `mitigation_mode_suspicious <- mitigationModeSuspicious`
- `machine_type <- machineType`
- `group_id <- groupId`
- `group_name <- groupName`
- `last_logged_in_user_name <- lastLoggedInUserName`
- `active_protection_modes <- activeProtection[]`
- `missing_permissions <- missingPermissions[]`
- `user_actions_needed <- userActionsNeeded[]`
- `location_names <- locations[].name`
- `tags <- tags.sentinelone[]`

### OS / hardware
- `serial_number <- valid_hardware_serial(serialNumber)`
- `os_name <- osName`
- `os_family <- osType`
- `os_version <- osRevision`
- `os_architecture <- osArch`
- `cpu_count <- cpuCount`
- `memory_bytes <- totalMemory * 1024 * 1024`

### Raw fidelity
- `raw_json` and `raw_payload` preserved

### asset_uid
`sha2(lower(trim(primary_hostname)) | lower(trim(primary_ip)) | source_record_id)`

---

## Contract-Layer Conformance

After source normalization, `conform_df` applies contract rules (`assets_silver_contract.yaml`):
- type casts
- trim/lower/regex/map where configured
- array clean/distinct/sort
- risk scaling rule (`scale_if_gt`) for `risk_score`

Contract policy metadata currently includes:
- prefix exception policy text
- no-unapproved-prefixes test policy
- single-source semantic whitelist
- informational `org_map_matched` test mode
- gold matching exclusions: `gateway_mac_addresses`, `virtual_mac_addresses`, `site_id`, `source_site_ref_id`

Job runtime also logs `org_map_matched=true` counts per source as informational.

---

## Cross-Source Mapping Matrix (Gold-Critical Fields)

This table is intentionally focused on the fields Gold typically uses for matching and survivorship.

| Silver field | Rapid7 | FortiSIEM | SentinelOne | Notes |
|---|---|---|---|---|
| `source_record_id` | `id` | `coalesce(_id.$oid, id, naturalId)` | `id` | canonical source row id |
| `source_natural_id` | `null` | `naturalId` | `uuid` | business-stable id where available |
| `source_site_ref_id` | `coalesce(site_id, site_lookup.id)` | `organization.attr_id` | `coalesce(siteId, siteid, site_id)` | source-local scope only |
| `source_display_name` | `null` | `name` | `null` | FSM display identifier |
| `asset_name` | `hostName` | `name` | `computerName` | semantic display name |
| `primary_hostname` | `hostName` | `null` | `computerName` | FSM intentionally null |
| `hostnames` | `union([hostName], hostNames[].name)` | `null` | `[computerName]` | array normalized |
| `primary_ip` | `ip` | `null` | `lastIpToMgmt` | S1 uses mgmt IP |
| `access_ip` | `null` | `accessIp` | `lastIpToMgmt` | mgmt/access semantics |
| `ip_addresses_raw` | `union([ip], addresses[].ip)` | `union([accessIp], interfaces[].ipv4Addr)` | `union(networkInterfaces[].inet, [lastIpToMgmt])` | pre-filter evidence |
| `ip_addresses` | filtered from `ip_addresses_raw` | filtered from `ip_addresses_raw` | filtered from `ip_addresses_raw` | strips `127.*` and `169.254.*` |
| `ipv6_addresses` | `null` | `interfaces[].ipv6Addr` | `null` | cleaned array |
| `primary_mac` | `mac` | deterministic selection from interfaces | `null` | FSM rule described below |
| `mac_addresses` | `union([mac], addresses[].mac)` | physical interface MACs only (`macIsVirtual=false`) | `networkInterfaces[].physical` | normalized array |
| `gateway_mac_addresses` | `null` | `null` | `networkInterfaces[].gatewayMacAddress` | do not use for identity |
| `virtual_mac_addresses` | `null` | virtual interface MACs (`macIsVirtual=true`) | `null` | do not use for identity |
| `serial_number` | `null` | `hwSerialNum` (filtered) | `serialNumber` (filtered) | placeholder serials nulled |
| `site_id` | `coalesce(site_id, site_lookup.id)` | `organization.attr_id` | `coalesce(siteId, siteid, site_id)` | source-local only |
| `site_name` | `coalesce(site_lookup.name, assets.site_name)` | `organization.attr_name` | `coalesce(siteName, sitename, site_name)` | canonical name in-row |
| `normalised_org_name` | normalized from `site_name` | normalized from `site_name` | normalized from `site_name` | regex normalization rules |
| `account_id` | `null` | `null` | `accountId` | tenant/account scope |
| `account_name` | `null` | `null` | `accountName` | tenant/account label |
| `org_map_matched` | `site_name != normalised_org_name` | same | same | informational test |
| `device_vendor` | `null` | `deviceType.vendor` | `null` | device metadata |
| `device_model` | `null` | `deviceType.model` | `null` | device metadata |
| `device_version` | `null` | `deviceType.version` | `null` | device metadata |
| `platform_version` | `null` | `version` | `null` | FSM platform/software version |
| `asset_type` | `type` | `null` | `null` | Rapid7 asset classification |
| `os_name` | `os` | `null` | `osName` | OS label |
| `os_family` | `osFingerprint.family` | `null` | `osType` | family/category |
| `os_vendor` | `osFingerprint.vendor` | `null` | `null` | vendor |
| `os_product` | `osFingerprint.product` | `null` | `null` | product |
| `os_version` | `coalesce(osFingerprint.cpe.version, osFingerprint.version)` | `null` | `osRevision` | version |
| `os_architecture` | `osFingerprint.architecture` | `null` | `osArch` | architecture |
| `os_edition` | `null` | `osEdition` | `null` | edition |
| `os_certainty` | `osCertainty` | `null` | `null` | Rapid7 confidence |
| `cpu_count` | `null` | `null` | `cpuCount` | integer |
| `memory_bytes` | `null` | `null` | `totalMemory * 1024 * 1024` | MB -> bytes |
| `system_uptime` | `null` | `systemUpTime` | `null` | uptime |
| `assessed_for_policies` | `assessedForPolicies` | `null` | `null` | Rapid7-only today |
| `assessed_for_vulnerabilities` | `assessedForVulnerabilities` | `null` | `null` | Rapid7-only today |
| `risk_score` | `riskScore` | `null` | `null` | conformance rules may scale |
| `raw_risk_score` | `rawRiskScore` | `null` | `null` | raw risk |
| `vuln_total` | `vulnerabilities.total` | `null` | `null` | vulnerability counts |
| `vuln_critical` | `vulnerabilities.critical` | `null` | `null` | vulnerability counts |
| `vuln_severe` | `vulnerabilities.severe` | `null` | `null` | vulnerability counts |
| `vuln_moderate` | `vulnerabilities.moderate` | `null` | `null` | vulnerability counts |
| `vuln_exploits` | `vulnerabilities.exploits` | `null` | `null` | vulnerability counts |
| `vuln_malware_kits` | `vulnerabilities.malwareKits` | `null` | `null` | vulnerability counts |
| `posture_is_active` | `null` | `null` | `isActive` | S1 endpoint posture |
| `posture_firewall_enabled` | `null` | `null` | `firewallEnabled` | S1 endpoint posture |
| `posture_network_quarantine_enabled` | `null` | `null` | `networkQuarantineEnabled` | S1 endpoint posture |
| `posture_active_threats` | `null` | `null` | `activeThreats` | S1 endpoint posture |
| `device_status` | `null` | `deviceStatus` | `null` | FSM operational |
| `discover_method` | `null` | `discoverMethod` | `null` | FSM operational |
| `event_log_status` | `null` | `eventLogStatus` | `null` | FSM operational |
| `perf_mon_status` | `null` | `perfMonStatus` | `null` | FSM operational |
| `update_method` | `null` | `updateMethod` | `null` | FSM operational |
| `scan_status` | `null` | `null` | `scanStatus` | S1 operational |
| `operational_state` | `null` | `null` | `operationalState` | S1 operational |
| `operational_state_expiration` | `null` | `null` | `operationalStateExpiration` | S1 operational |
| `network_status` | `null` | `null` | `networkStatus` | S1 operational |
| `ranger_status` | `null` | `null` | `rangerStatus` | S1 operational |
| `mitigation_mode` | `null` | `null` | `mitigationMode` | S1 operational |
| `mitigation_mode_suspicious` | `null` | `null` | `mitigationModeSuspicious` | S1 operational |
| `machine_type` | `null` | `null` | `machineType` | S1 operational |
| `group_id` | `null` | `null` | `groupId` | S1 grouping |
| `group_name` | `null` | `null` | `groupName` | S1 grouping |
| `last_logged_in_user_name` | `null` | `null` | `lastLoggedInUserName` | S1 identity |
| `active_protection_modes` | `null` | `null` | `activeProtection[]` | S1 arrays |
| `missing_permissions` | `null` | `null` | `missingPermissions[]` | S1 arrays |
| `user_actions_needed` | `null` | `null` | `userActionsNeeded[]` | S1 arrays |
| `location_names` | `null` | `null` | `locations[].name` | S1 arrays |
| `services_count` | `size(services[])` | `null` | `null` | Rapid7 inventory count |
| `software_count` | `size(software[])` | `null` | `null` | Rapid7 inventory count |

### FortiSIEM `primary_mac` exact derivation
1. Candidate set A: interface rows where `macIsVirtual=false`, `isWAN=false`, `operStatus=up`, MAC non-empty.
2. Sort set A by `snmpIndex` ascending.
3. Pick first MAC from sorted set A.
4. If A empty: repeat with set B where `operStatus` is not required.
5. If B empty: `primary_mac = null`.

---

## Fields Intentionally Not Surfaced as Dedicated Silver Columns

These are still available inside raw payload JSON:
- FortiSIEM `osSerialNum`
- FortiSIEM `bios`
- FortiSIEM deep component serials like `components[].serial`
- SentinelOne `networkInterfaces[].gatewayIp`

Reason: avoid semantically incorrect canonical placement while preserving raw evidence.

---

## Gold Migration Guidance (for Claude/GPT)

Use this section when redesigning Gold against Silver V2.

### Recommended matching evidence (high value)
- `primary_hostname`, `hostnames`
- `primary_ip`, `ip_addresses`, `ip_addresses_raw`
- `primary_mac`, `mac_addresses`
- `serial_number`
- `source_natural_id`, `source_record_id`
- `normalised_org_name`, `site_name`, `account_name`
- `device_vendor`, `device_model`, `os_family`, `os_name`

### Evidence to avoid as deterministic identity keys
- `gateway_mac_addresses`
- `virtual_mac_addresses`
- `site_id`
- `source_site_ref_id`

### Important null semantics
- `null` in Silver often means “not semantically supported by this source”, not “missing ETL”.
- Gold survivorship should treat source capability and nullability explicitly.

### Source asymmetry you must preserve
- FortiSIEM `name` is a display identifier (`source_display_name`), not canonical hostname in current design.
- SentinelOne has management IP behavior (`lastIpToMgmt` used as both `primary_ip` and `access_ip`).
- Rapid7 has richer vuln/risk and site-enrichment fields.

---

## Quick SQL Validation Snippets

```sql
-- Schema sanity
DESCRIBE TABLE iceberg.silver.rapid7__assets__silver__current;

-- Ensure no legacy prefixed columns remain
-- (manual check from DESCRIBE output should show none of rapid7_*, fortisiem_*, sentinelone_*)

-- Check IP raw vs filtered behavior
SELECT
  source_system,
  COUNT(*) AS rows,
  SUM(CASE WHEN ip_addresses_raw IS NOT NULL THEN 1 ELSE 0 END) AS raw_ip_rows,
  SUM(CASE WHEN ip_addresses IS NOT NULL THEN 1 ELSE 0 END) AS filtered_ip_rows
FROM iceberg.silver.rapid7__assets__silver__current
GROUP BY source_system;

-- Informational org-map count
SELECT source_system, org_map_matched, COUNT(*)
FROM iceberg.silver.sentinalone__agents__silver__current
GROUP BY source_system, org_map_matched;
```

---

## Change Control Note
If any source normalizer or `TARGET_FIELDS` changes, update this document immediately. Gold design assumptions should always be derived from this Silver reference, not from older prefixed schema docs.
