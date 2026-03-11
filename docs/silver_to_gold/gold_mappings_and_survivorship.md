**Silver -> Gold (Assets Current) Mappings and Survivorship**

This document describes how the gold pipeline merges and selects values from silver current tables to build `iceberg.gold.assets_current`. It is derived from:

- `scripts/gold/build_gold_assets.py`
- `scripts/gold/matching.py`
- `scripts/gold/survivorship.py`
- `scripts/gold/utils.py`
- `scripts/gold/config.py`

---

**A. Gold overview**

`gold.assets_current` is a canonical asset view built by anchoring on SentinelOne and enriching with Rapid7 and FortiSIEM where matches exist. It uses deterministic joins and survivorship rules to pick a single value for each field from the contributing sources.

The gold pipeline is current-only and overwrites the table on each run.

---

**B. Match / merge logic**

**Anchor strategy**

- SentinelOne is the anchor source.
- Rapid7 and FortiSIEM are LEFT JOINed onto SentinelOne.
- This means matching is “SentinelOne-centric” and the output grain is one row per SentinelOne asset (after filtering).

**Normalization for matching**

`add_norm_fields` (`scripts/gold/utils.py`) derives helper columns:

- `norm_site = upper(trim(site_name))`
- `norm_host = upper(trim(primary_hostname))`
- `norm_host_short = upper(trim(regexp_replace(primary_hostname, '\..*$', '')))`

**Join conditions (from `match_sources`)**

Rapid7 join condition: `s.norm_site = r.norm_site` AND `s.primary_ip = r.primary_ip`.

FortiSIEM join condition: `s.norm_site = f.norm_site` AND hostname match, where hostname match is true if any of these expressions are true: `s.norm_host = f.norm_host`, `s.norm_host_short = f.norm_host`, `s.norm_host = f.norm_host_short`.

**Row inclusion filter**

`build_gold_assets.py` filters out SentinelOne-only rows:

- A gold row is kept only when `seen_in_rapid7 OR seen_in_fortisiem` is true.

This is a strict cross-source requirement.

---

**C. Survivorship rules**

Survivorship logic is implemented in `build_gold_rows` (`scripts/gold/survivorship.py`).

**Key rules**

- Default rule for most fields: `coalesce(s_<field>, r_<field>, f_<field>)`.
- Certain fields use explicit precedence that differs from default.
- `min_non_null` and `max_non_null` are used for time fields.

**Helper functions**

`min_non_null(*cols)` builds an array of the inputs, filters out nulls, and returns `array_min`.

`max_non_null(*cols)` builds an array of the inputs, filters out nulls, and returns `array_max`.

These are used for:

- `first_seen_at = min_non_null(s_first_seen_at, r_first_seen_at, f_first_seen_at)`
- `last_seen_at = max_non_null(s_last_seen_at, r_last_seen_at, f_last_seen_at)`
- `source_updated_at = max_non_null(s_source_updated_at, r_source_updated_at, f_source_updated_at)`

**Canonical key construction**

- `gold_entity_key_str = norm_site | primary_ip` if `primary_ip` is not null.
- Otherwise `gold_entity_key_str = norm_site | norm_host`.
- `gold_asset_id = sha2(gold_entity_key_str, 256)`.

---

**D. Gold field mapping reference (comprehensive)**

The table below lists every gold field, its upstream source candidates, and precedence rules.

**Legend**

- `s_` = SentinelOne silver current
- `r_` = Rapid7 silver current
- `f_` = FortiSIEM silver current

| Gold field | Upstream fields | Precedence / derivation | Notes |
| --- | --- | --- | --- |
| schema_version | s_schema_version, r_schema_version, f_schema_version | coalesce s -> r -> f | |
| source | literal `gold` | override | |
| entity_id | gold_asset_id | override | canonical id |
| entity_key_str | gold_entity_key_str | override | `norm_site|primary_ip` else `norm_site|norm_host` |
| payload_hash | gold_payload_hash | override | copied from gold hash |
| topic_name | s_topic_name, r_topic_name, f_topic_name | coalesce s -> r -> f | |
| vendor_id | s_vendor_id, r_vendor_id, f_vendor_id | coalesce s -> r -> f | |
| ingest_ts | s_ingest_ts, r_ingest_ts, f_ingest_ts | coalesce s -> r -> f | |
| first_seen_at | s_first_seen_at, r_first_seen_at, f_first_seen_at | min_non_null | earliest across sources |
| last_seen_at | s_last_seen_at, r_last_seen_at, f_last_seen_at | max_non_null | latest across sources |
| source_updated_at | s_source_updated_at, r_source_updated_at, f_source_updated_at | max_non_null | latest source update |
| event_time | s_event_time, r_event_time, f_event_time | coalesce s -> r -> f | |
| asset_uid | s_asset_uid, r_asset_uid, f_asset_uid | coalesce s -> r -> f | |
| source_system | literal `gold` | override | |
| site_id | s_site_id, r_site_id, f_site_id | coalesce s -> r -> f | |
| site_name | s_site_name, r_site_name, f_site_name | coalesce s -> r -> f | normalized in silver |
| rapid7_id | s_rapid7_id, r_rapid7_id, f_rapid7_id | coalesce s -> r -> f | |
| fortisiem_id | s_fortisiem_id, r_fortisiem_id, f_fortisiem_id | coalesce s -> r -> f | |
| asset_name | s_asset_name, r_asset_name, f_asset_name | coalesce s -> r -> f | |
| primary_hostname | s_primary_hostname, f_primary_hostname | coalesce s -> f | Rapid7 is not used for hostname |
| primary_ip | s_primary_ip, r_primary_ip | coalesce s -> r | FortiSIEM not used |
| access_ip | s_access_ip, r_access_ip, f_access_ip | coalesce s -> r -> f | |
| natural_id | s_natural_id, r_natural_id, f_natural_id | coalesce s -> r -> f | |
| approved | s_approved, r_approved, f_approved | coalesce s -> r -> f | |
| unmanaged | s_unmanaged, r_unmanaged, f_unmanaged | coalesce s -> r -> f | |
| device_vendor | s_device_vendor, r_device_vendor, f_device_vendor | coalesce s -> r -> f | |
| device_model | s_device_model, r_device_model, f_device_model | coalesce s -> r -> f | |
| device_version | s_device_version, r_device_version, f_device_version | coalesce s -> r -> f | |
| os_name | s_os_name, r_os_name, f_os_name | coalesce s -> r -> f | SentinelOne preferred |
| os_family | s_os_family, r_os_family, f_os_family | coalesce s -> r -> f | |
| os_vendor | s_os_vendor, r_os_vendor, f_os_vendor | coalesce s -> r -> f | |
| os_product | s_os_product, r_os_product, f_os_product | coalesce s -> r -> f | |
| os_version | s_os_version, r_os_version, f_os_version | coalesce s -> r -> f | |
| os_architecture | s_os_architecture, r_os_architecture, f_os_architecture | coalesce s -> r -> f | |
| os_certainty | s_os_certainty, r_os_certainty, f_os_certainty | coalesce s -> r -> f | |
| assessed_for_policies | s_assessed_for_policies, r_assessed_for_policies, f_assessed_for_policies | coalesce s -> r -> f | |
| assessed_for_vulnerabilities | s_assessed_for_vulnerabilities, r_assessed_for_vulnerabilities, f_assessed_for_vulnerabilities | coalesce s -> r -> f | |
| risk_score | r_risk_score, s_risk_score, f_risk_score | coalesce r -> s -> f | Rapid7 preferred |
| raw_risk_score | s_raw_risk_score, r_raw_risk_score, f_raw_risk_score | coalesce s -> r -> f | |
| vuln_total | s_vuln_total, r_vuln_total, f_vuln_total | coalesce s -> r -> f | |
| vuln_critical | s_vuln_critical, r_vuln_critical, f_vuln_critical | coalesce s -> r -> f | |
| vuln_severe | s_vuln_severe, r_vuln_severe, f_vuln_severe | coalesce s -> r -> f | |
| vuln_moderate | s_vuln_moderate, r_vuln_moderate, f_vuln_moderate | coalesce s -> r -> f | |
| vuln_exploits | s_vuln_exploits, r_vuln_exploits, f_vuln_exploits | coalesce s -> r -> f | |
| vuln_malware_kits | s_vuln_malware_kits, r_vuln_malware_kits, f_vuln_malware_kits | coalesce s -> r -> f | |
| host_domain | s_host_domain | sentinel only | no fallback |
| ip_addresses | s_ip_addresses, r_ip_addresses, f_ip_addresses | coalesce s -> r -> f | |
| external_ip | s_external_ip, r_external_ip, f_external_ip | coalesce s -> r -> f | |
| cpu_count | s_cpu_count, r_cpu_count, f_cpu_count | coalesce s -> r -> f | |
| memory_bytes | s_memory_bytes, r_memory_bytes, f_memory_bytes | coalesce s -> r -> f | |
| posture_is_active | s_posture_is_active, r_posture_is_active, f_posture_is_active | coalesce s -> r -> f | |
| posture_firewall_enabled | s_posture_firewall_enabled, r_posture_firewall_enabled, f_posture_firewall_enabled | coalesce s -> r -> f | |
| posture_network_quarantine_enabled | s_posture_network_quarantine_enabled, r_posture_network_quarantine_enabled, f_posture_network_quarantine_enabled | coalesce s -> r -> f | |
| posture_active_threats | s_posture_active_threats, r_posture_active_threats, f_posture_active_threats | coalesce s -> r -> f | |
| tags | s_tags, r_tags, f_tags | coalesce s -> r -> f | |
| raw_payload | s_raw_payload, r_raw_payload, f_raw_payload | coalesce s -> r -> f | |
| raw_json | s_raw_json, r_raw_json, f_raw_json | coalesce s -> r -> f | |
| gold_asset_id | gold_entity_key_str | sha2 | canonical id |
| sentinalone_entity_id | s_entity_id | direct | lineage |
| rapid7_entity_id | r_entity_id | direct | lineage |
| fortisiem_entity_id | f_entity_id | direct | lineage |
| seen_in_sentinalone | sentinalone_entity_id | `isNotNull` | presence flag |
| seen_in_rapid7 | rapid7_entity_id | `isNotNull` | presence flag |
| seen_in_fortisiem | fortisiem_entity_id | `isNotNull` | presence flag |
| matched_sources | seen_in_* flags | array of present sources | nulls filtered |
| gold_payload_hash | GOLD_HASH_COLUMNS | sha2(to_json(struct(...))) | canonical hash |

---

**E. Match confidence and lineage fields**

`matched_sources` is built from the presence flags:

- `sentinalone_entity_id is not null` -> `sentinalone`
- `rapid7_entity_id is not null` -> `rapid7`
- `fortisiem_entity_id is not null` -> `fortisiem`

`matched_sources` is an array of non-null source labels and provides a simple match confidence signal.

---

**F. Data contract for Gold**

**Row grain**

One row per SentinelOne asset that matches at least one other source.

**What gold resolves**

- Merges cross-source records into a canonical view.
- Applies deterministic survivorship for key fields.
- Exposes lineage and match flags for transparency.

**What gold does not resolve**

- It does not merge Rapid7-only or FortiSIEM-only assets.
- It does not attempt fuzzy matching beyond the deterministic joins.
- It does not maintain history; each run overwrites the current table.

---

**G. Inferred details**

- The gold job relies on silver fields already normalized and conformed; no additional conformance rules are applied in gold beyond survivorship.
- SentinelOne is always the anchor because the join is built from `s` and left joins to `r` and `f`. This is inferred from the join order in `match_sources`.
