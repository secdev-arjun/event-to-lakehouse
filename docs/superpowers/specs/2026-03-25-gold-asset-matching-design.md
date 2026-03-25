# Gold Asset Matching Design

Date: 2026-03-25
Status: Approved design, pending implementation plan
Scope: Replace the current score-based gold matching core with a deterministic pairwise rule engine while keeping the existing gold entrypoint, naming conventions, and general survivorship/writer structure.

## 1. Architecture Summary

Gold will consume the existing trusted silver current tables for Rapid7, FortiSIEM, and SentinelOne and will perform deterministic pairwise matching only. Matching will run in ordered rule stages per source pair, with source-internal duplicate resolution before any cross-source join. Only safe one-to-one matches will be auto-accepted. Ambiguous and review-only candidates will be emitted separately. Accepted pairwise matches will then be consolidated into connected multi-source components, and only accepted components containing at least two distinct sources will be written into `gold_assets_current` in this phase.

This design preserves explainability and auditability by recording the rule, rank, source pair, keys used, ambiguity signals, and metrics for every rule stage. The framework will also support singleton entities, but singleton rows will remain in unmatched/staging outputs for now and will not flow into `gold_assets_current`.

## 2. Goals

- Match the same asset across Rapid7, FortiSIEM, and SentinelOne using deterministic, explainable rules.
- Prefer correctness over coverage.
- Preserve exact audit metadata for accepted and non-accepted pairwise candidates.
- Keep the implementation centralized and configurable rather than hiding rule logic inside ad hoc branches.
- Reuse the current codebase conventions where practical.

## 3. Non-Goals

- No redesign of the silver layer.
- No probabilistic or fuzzy entity resolution as the primary matching system.
- No forced merge of ambiguous or weak candidates.
- No singleton rows in `gold_assets_current` during this phase.

## 4. Existing Codebase Alignment

The implementation will keep these conventions:

- Entrypoint remains `scripts/gold/build_gold_assets.py`.
- Input table names remain driven by `scripts/gold/config.py`.
- Gold output continues to use the existing writer and survivorship shape, but the matching/grouping internals will be replaced.
- Silver remains the source of truth for canonical org and site values.

The implementation will replace the current score-based matching core in:

- `scripts/gold/matching.py`
- `scripts/gold/matching_pairs.py`
- `scripts/gold/grouping.py`

The implementation will adapt, not replace from scratch:

- `scripts/gold/survivorship.py`
- `scripts/gold/writer.py`

New helper modules may be added under `scripts/gold/` if that yields a cleaner design.

## 5. Source Matching Views

Each silver input will be projected into a source matching view containing only the fields needed for:

- rule evaluation
- source-internal ranking
- auditability
- survivorship

Each view will expose the following common helpers:

- `source_system`
- `entity_id`
- `source_record_id`
- `source_natural_id`
- `source_updated_at`
- `last_seen_at`
- `ingest_ts`
- `normalised_org_name`
- `site_name`
- `primary_hostname`
- `asset_name`
- `source_display_name`
- `primary_ip`
- `access_ip`
- `serial_number`
- `os_family`
- `os_name`
- `os_version`
- `mac_addresses`
- `gateway_mac_addresses`
- `virtual_mac_addresses`
- `ip_addresses`

Derived helper columns:

- `org_key`
  - normalized `normalised_org_name`
- `site_key`
  - normalized `site_name`
- `hostname_key`
  - normalized `coalesce(primary_hostname, asset_name, source_display_name)`
- `ip_key`
  - preferred scalar IP by source:
    - Rapid7: `primary_ip`
    - FortiSIEM: `access_ip`
    - SentinelOne: `coalesce(primary_ip, access_ip)`
- `serial_key`
  - normalized `serial_number`
- `os_family_key`
  - normalized `os_family`
- `physical_mac_keys`
  - normalized array of exact device MAC values from `mac_addresses` after excluding values found in `gateway_mac_addresses` and `virtual_mac_addresses`
  - this helper remains array-valued in the source matching view
  - MAC-based rules will explode this array into a stage-local scalar `physical_mac_key` so matching remains exact and auditable per MAC value
- `site_present_flag`
  - true when `site_key` is non-null
- `evidence_completeness_score`
  - count of populated evidence fields used to rank duplicates
  - field list:
    - `hostname_key`
    - `ip_key`
    - `serial_key`
    - `os_family_key`
    - `os_name`
    - `os_version`
    - `physical_mac_keys`
  - arrays contribute at most one point when non-empty
- `freshness_ts`
  - `coalesce(source_updated_at, last_seen_at, ingest_ts)`

`hostname_key`, `ip_key`, `serial_key`, `os_family_key`, and MAC helpers exist only for matching convenience. They do not redefine silver semantics.

## 6. Source-Internal Ranking and Uniqueness

For every rule stage and every source independently:

1. Build the rule key.
2. Partition rows by the rule key.
3. Rank rows inside that key using:
   - `site_present_flag DESC`
   - `evidence_completeness_score DESC`
   - `freshness_ts DESC`
   - stable final ordering by source identifiers for reproducibility only

Important guardrail:

- The stable final ordering is only to make results reproducible.
- A key is considered semantically ambiguous if multiple rows remain tied after the first three ranking dimensions.
- Such keys are not safe for auto-match even though a deterministic winner can be chosen for ordering purposes.

The rule engine will capture:

- duplicate counts per key
- whether the preferred row won because site was populated
- whether ambiguity remained after semantic ranking

## 7. Centralized Rule Hierarchy

Rules will be defined as centralized ordered config objects. Each rule definition will include:

- `rule_name`
- `rule_rank`
- `source_pair`
- `left_required_columns`
- `right_required_columns`
- `left_key_columns`
- `right_key_columns`
- `auto_accept`
- `emit_review`
- `description`

### 7.1 Shared Rule Families

1. `serial_org_exact`
   - key: `org_key + serial_key`
   - auto-accept: yes
   - rationale: strongest exact hardware identifier where available

2. `physical_mac_org_exact`
   - key: `org_key + physical_mac_key`
   - auto-accept: yes
   - rationale: strongest shared exact hardware evidence after excluding non-device MACs
   - implementation note: `physical_mac_key` is the scalar value produced by exploding `physical_mac_keys` for the current rule stage

3. `org_site_ip_exact`
   - key: `org_key + site_key + ip_key`
   - auto-accept: yes
   - rationale: strongest site-scoped business/network rule

4. `org_site_hostname_exact`
   - key: `org_key + site_key + hostname_key`
   - auto-accept: yes
   - rationale: site-scoped name-based fallback

5. `org_ip_os_family_exact`
   - key: `org_key + ip_key + os_family_key`
   - auto-accept: yes
   - rationale: siteless fallback with added discriminator

6. `org_hostname_os_family_exact`
   - key: `org_key + hostname_key + os_family_key`
   - auto-accept: yes
   - rationale: siteless hostname fallback with added discriminator

7. `org_ip_exact`
   - key: `org_key + ip_key`
   - auto-accept: no
   - emit review: yes
   - rationale: retained to measure possible future coverage

8. `org_hostname_exact`
   - key: `org_key + hostname_key`
   - auto-accept: no
   - emit review: yes
   - rationale: retained for review and metrics only

### 7.2 Pair Applicability

#### Rapid7 ↔ FortiSIEM

Enabled rules:

- `physical_mac_org_exact`
- `org_site_ip_exact`
- `org_site_hostname_exact`
- `org_ip_os_family_exact`
- `org_hostname_os_family_exact`
- `org_ip_exact`
- `org_hostname_exact`

Disabled rules:

- `serial_org_exact`

Notes:

- FortiSIEM name-side hostname evidence comes from `coalesce(primary_hostname, asset_name, source_display_name)`.
- IP-side comparison uses Rapid7 `primary_ip` and FortiSIEM `access_ip`.

#### Rapid7 ↔ SentinelOne

Enabled rules:

- `physical_mac_org_exact`
- `org_site_ip_exact`
- `org_site_hostname_exact`
- `org_ip_os_family_exact`
- `org_hostname_os_family_exact`
- `org_ip_exact`
- `org_hostname_exact`

Disabled rules:

- `serial_org_exact`

Notes:

- Both sides have useful hostname and IP coverage.
- MAC-based rules remain important because SentinelOne often carries MAC evidence in arrays rather than a single primary field.

#### FortiSIEM ↔ SentinelOne

Enabled rules:

- `serial_org_exact`
- `physical_mac_org_exact`
- `org_site_ip_exact`
- `org_site_hostname_exact`
- `org_ip_os_family_exact`
- `org_hostname_os_family_exact`
- `org_ip_exact`
- `org_hostname_exact`

Notes:

- `serial_org_exact` runs first for this pair because both sides can provide practical serial evidence.
- IP comparison uses FortiSIEM `access_ip` and SentinelOne `coalesce(primary_ip, access_ip)`.

## 8. Rule Stage Execution

For each pair and each ordered rule stage:

1. Build left and right rule keys.
2. Exclude rows with null keys from the stage.
   - Null key means not eligible for the stage, not a failed match.
3. Rank rows inside each source key using the source-internal ranking logic.
4. Determine whether the key is safe:
   - required columns exist
   - key is non-null
   - a preferred row exists
   - ambiguity does not remain after semantic ranking
5. Perform cross-source join only on safe preferred rows.
6. Classify outputs:
   - `auto_match`
   - `review`
   - `residue`
7. Remove only `auto_match` rows from later rule stages.
8. Preserve ambiguous and review-only candidates separately.

If a rule cannot run because required columns are missing for a pair, the engine will skip the rule and emit explicit zero-count metrics plus a rule-status note.

## 9. Output Datasets

### 9.1 `gold_match_candidates`

Stores accepted auto-match pairwise edges only.

Grain:

- one row per accepted pairwise edge
- a pairwise edge is emitted once, at the first rule stage that safely auto-accepts it
- because matched rows are removed from residue after acceptance, the same accepted edge must not be emitted again by lower-priority rules

Required fields:

- `source_pair`
- `rule_name`
- `rule_rank`
- `match_key`
- `left_source_system`
- `right_source_system`
- `left_entity_id`
- `right_entity_id`
- source-side record ids if available
- source-side natural ids if available
- `match_status = auto_match`
- audit flags describing duplicate resolution

### 9.2 `gold_match_review`

Stores ambiguous and review-only pairwise outputs only.

Grain:

- one row per rule-stage review event
- review rows are intentionally not deduplicated across rules
- the same source entities may appear more than once if different rules surface different ambiguity or review conditions
- this dataset is for diagnostics, analyst review, and rule-quality measurement

Required fields:

- `source_pair`
- `rule_name`
- `rule_rank`
- `match_key`
- candidate source entity ids
- `review_reason`
- ambiguity flags
- duplicate counts
- rule-status details

### 9.3 `gold_assets_unmatched`

Stores rows left unmatched after all rule stages.

Required behavior:

- source-row grain
- one row per source observation still unmatched after all rule stages complete
- singleton-ready structure, meaning each row carries enough lineage and deterministic identity to be promoted later into a singleton gold entity if that phase is enabled
- singleton-only components are represented here as unmatched source rows, not as consolidated multi-row component records in this phase
- no flow into `gold_assets_current` during this phase
- preserve enough lineage to enable later singleton promotion

### 9.4 `gold_match_metrics`

One row per `source_pair + rule_name + rule_rank`.

Required fields:

- `source_pair`
- `rule_name`
- `rule_rank`
- `rule_status`
- `left_candidate_rows`
- `right_candidate_rows`
- `left_duplicate_keys`
- `right_duplicate_keys`
- `left_rows_preferred_by_site`
- `right_rows_preferred_by_site`
- `auto_matches_count`
- `ambiguous_count`
- `unmatched_remaining_count`
- optional note field for skipped rules or special conditions

## 10. Consolidation into Gold Entities

Accepted pairwise matches will be treated as graph edges between source-specific entity ids.

Guardrails:

- connected-component construction uses only accepted edges from `gold_match_candidates`
- review-only edges never contribute to component construction
- a component is accepted for this phase only if it contains at least two distinct sources
- if a connected component contains more than one row from the same source, that component is routed to review and must not be written to `gold_assets_current`

The consolidation layer will still support singleton-ready construction in code, but singleton-only components remain routed to `gold_assets_unmatched` in this phase.

## 11. Survivorship and Deterministic Group Metadata

Final accepted multi-source components will be converted into `gold_assets_current` rows using field-level survivorship. Silver remains the source of canonical org/site truth.

Deterministic output metadata must include:

- `seen_in_rapid7`
- `seen_in_fortisiem`
- `seen_in_sentinalone`
- `matched_sources`
- source entity ids
- `source_count`
- `edge_count`
- `match_rule_summary`
  - ordered distinct accepted rules used in the component
- `min_match_rule_rank`
- provenance and audit columns needed to explain the accepted component

Survivorship principles:

- prefer the most complete/high-quality value for each field
- use field-specific precedence where needed
- union and de-duplicate multi-value fields such as hostnames, IPs, and MACs
- preserve source lineage and accepted-rule lineage

## 12. Error Handling and Safety Rules

- Missing required columns do not fail the entire job; the affected rule is skipped with explicit metrics.
- Null keys do not count as match failures; they are simply stage-ineligible.
- Ambiguous pairwise candidates never auto-merge.
- Components with duplicate representation from the same source never flow into `gold_assets_current`.
- Only accepted multi-source components are written to the current gold table in this phase.

## 13. Testing and Verification Requirements

Implementation verification must cover:

- source-internal ranking behavior
- `site not null` preference logic
- ambiguity detection after semantic ranking
- residue progression between rule stages
- review-only rules staying out of accepted edges
- connected-component construction using accepted edges only
- same-source duplicate component routing to review
- exclusion of singleton-only components from `gold_assets_current`
- presence of singleton-ready rows in unmatched output
- metrics emission for normal and skipped rule stages

## 14. Assumptions

- Silver current tables are already normalized and trusted.
- Canonical org and site values are already produced in silver.
- Current silver columns continue to match the existing codebase field names.
- Pairwise rule evaluation is sufficient for this phase; no direct three-way rule execution is required.

## 15. Extension Points

The framework is intentionally designed for later extension:

- promote approved singleton components into final gold output
- add new deterministic rules to the centralized config
- tune review-only rules into auto-accept rules if metrics justify it
- add optional fuzzy or analyst-assisted matching as a clearly isolated later phase
- extend current-only outputs into history tables if required later

## 16. Plan Gate

This spec is the approved design baseline for the implementation plan. The next step is to produce the implementation plan, mapped onto the existing `scripts/gold/` code layout, after explicit user approval of this written spec.
