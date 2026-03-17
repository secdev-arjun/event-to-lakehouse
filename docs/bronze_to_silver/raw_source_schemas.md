**Raw Bronze Source Schemas (prod__bronze)**

This document describes the raw source schemas currently landing in:

- `prod__bronze.prod__sentinelone__agents`
- `prod__bronze.prod__rapid7__assets`
- `prod__bronze.prod__fortisiem__devices`

The goal is to provide high-quality context for redesigning silver/gold normalization and for LLM-assisted analysis (GPT/Claude).

---

**Authoritative snapshots**

- Full raw `DESCRIBE TABLE` outputs: `docs/bronze_to_silver/raw_prod_bronze_describe_output.txt`
- Machine-readable summary for tooling/LLM prompts: `docs/bronze_to_silver/raw_source_schema_summary.json`
- Normalized target schema (current): `docs/bronze_to_silver/normalized_schema.md`
- Current mapping implementation: `docs/bronze_to_silver/mappings_and_rules.md`

---

**Table overview**

| Table | Rows in DESCRIBE | Raw grain (expected) | Notes |
| --- | --- | --- | --- |
| `prod__bronze.prod__sentinelone__agents` | 86 | One endpoint agent record | High boolean coverage, rich endpoint posture, deep arrays/structs |
| `prod__bronze.prod__rapid7__assets` | 26 | One asset record | Strong vuln/risk coverage, highly nested `software/services/files` |
| `prod__bronze.prod__fortisiem__devices` | 39 | One device/CMDB record | Very deep nested structs, operational CMDB-style fields |

---

**How this helps silver/gold redesign**

Use this document to drive explicit decisions for:

1. Entity identity strategy across sources (`id`, `uuid`, `naturalId`, `site_id`, hostname/IP fallbacks).
2. Timestamp handling (`string` vs `bigint` epochs) and strict event-time semantics.
3. Nested/array flattening policy (what becomes canonical columns vs retained in metadata JSON).
4. Cross-source survivorship precedence by domain (identity, network, OS, risk, posture).
5. Canonical hash design (what should and should not participate in `payload_hash` and gold hash).

---

**A. SentinelOne raw schema (`prod__bronze.prod__sentinelone__agents`)**

**Profile**

- Strengths:
  - Endpoint state + posture rich fields (`isActive`, `firewallEnabled`, `networkQuarantineEnabled`, `activeThreats`).
  - Useful operational booleans and scan state fields.
  - Multiple host/network descriptors (`computerName`, `domain`, `networkInterfaces`, `externalIp`, `lastIpToMgmt`).
- Weaknesses:
  - Many time fields stored as `string` (needs strict parsing rules).
  - Mixed identity fields (`id`, `uuid`, `externalId`) require deterministic hierarchy.
  - Deep nested objects need selective flattening.

**Identity and key candidates**

- Primary source key candidates: `id`, `uuid`.
- Secondary/key support: `externalId`, `computerName`, `siteId`, `accountId`.
- Recommended deterministic preference:
  - `entity_id = id` when present, else `uuid`, else fallback to stable hash over `siteId|computerName|lastIpToMgmt`.

**Timestamp and freshness fields**

- Strings requiring parse: `createdAt`, `lastActiveDate`, `registeredAt`, `updatedAt`, scan timestamps.
- Ingest metadata: `ingest_ts` (`bigint`) should be parsed with epoch heuristics.
- Recommended parse policy:
  - Parse to UTC timestamps in silver.
  - Preserve original raw value in lineage metadata for parse failures.

**Network and host modeling**

- Key fields: `lastIpToMgmt`, `externalIp`, `domain`, `networkInterfaces`.
- Recommended silver strategy:
  - Canonical `primary_ip` from `lastIpToMgmt`.
  - Build normalized `ip_addresses` from `networkInterfaces[].inet` plus `lastIpToMgmt`.
  - Keep interface-level details in a secondary nested column or JSON extension.

**Security/posture modeling**

- High-value fields:
  - `activeThreats`, `infected`, `mitigationMode`, `detectionState`, `networkQuarantineEnabled`, `firewallEnabled`.
- Recommended:
  - Treat these as first-class posture/risk contributors in gold survivorship.
  - Define explicit recency-based precedence if multiple sources provide analogous signals.

---

**B. Rapid7 raw schema (`prod__bronze.prod__rapid7__assets`)**

**Profile**

- Strengths:
  - Clear risk/vulnerability signal (`riskScore`, `rawRiskScore`, `vulnerabilities.*`).
  - Strong software/service inventory depth.
  - Useful identity references (`id`, `ip`, `hostName`, `addresses`, `ids`).
- Weaknesses:
  - Some certainty fields are strings (e.g., `osCertainty`).
  - Heavy nested arrays can inflate canonical payload/hashes if not controlled.

**Identity and key candidates**

- Primary key candidate: `id` (`bigint`).
- Supporting candidates: `hostName`, `ip`, `mac`, `site_id`, `site_name`.
- Recommended:
  - Convert `id` to string in silver.
  - Use `site_id + primary_ip` or `site_id + normalized_hostname` for cross-source matching compatibility.

**Risk and vulnerability modeling**

- Best source for risk:
  - `riskScore`, `rawRiskScore`, `vulnerabilities.{total,critical,severe,moderate,exploits,malwareKits}`.
- Recommended:
  - Keep Rapid7 precedence for canonical risk in gold where present.
  - Ensure deterministic numeric casts and null handling before hashing/survivorship.

**Nested inventory modeling**

- Deep fields:
  - `software`, `services`, `files`, `history`, `users`, `userGroups`, `hostNames`, `addresses`.
- Recommended:
  - Keep canonical silver fields focused on core identity/risk/posture.
  - Store optional detailed inventory in extension columns or side tables if needed.

---

**C. FortiSIEM raw schema (`prod__bronze.prod__fortisiem__devices`)**

**Profile**

- Strengths:
  - CMDB/device lifecycle style model.
  - Rich device metadata (`deviceType`, `organization`, `interfaces`, `components`, `properties`).
  - Operational state and management methods (`discoverMethod`, `updateMethod`, `status`).
- Weaknesses:
  - Very deep recursive-like `eventParserList` structure.
  - More operational CMDB focus, weaker direct vuln/risk coverage vs Rapid7.

**Identity and key candidates**

- Primary candidates: `naturalId`, `name`, `accessIp`.
- Secondary: `organization.attr_id`, `hwSerialNum`.
- Recommended:
  - Prefer `naturalId` for stable source identity.
  - Use `site/org + accessIp` and `site/org + normalized_hostname` as match aids.

**Timestamp handling**

- Numeric times: `discoverTime`, `systemUpTime`, software patch install times.
- Recommended:
  - Convert epoch-like numerics to timestamps only when semantically time-based.
  - Keep durations (`systemUpTime`) as numeric duration metrics, not timestamps.

**Deep nested structure policy**

- Most complex field: `eventParserList` (deep recursive struct chain).
- Recommended:
  - Do not flatten recursively into wide schema.
  - Preserve as raw lineage JSON unless a specific analytics use case requires a curated extraction.

---

**Cross-source compatibility matrix (key canonical dimensions)**

| Canonical concept | SentinelOne | Rapid7 | FortiSIEM | Suggested canonical strategy |
| --- | --- | --- | --- | --- |
| Source ID | `id`/`uuid` | `id` | `naturalId` | Keep source-specific `entity_id`; cast all to string |
| Hostname | `computerName` | `hostName` | `name` | Normalize case/whitespace; maintain original too |
| Primary IP | `lastIpToMgmt` | `ip` | `accessIp` | Use source precedence + quality checks |
| Site/Org ID | `siteId` | `site_id` | `organization.attr_id` | Normalize to string; map aliases |
| Site/Org Name | `siteName` | `site_name` | `organization.attr_name` | Normalize aliases; maintain standardized site taxonomy |
| Risk score | weak/native posture | `riskScore` | mostly absent | Prefer Rapid7 risk when present |
| Domain | `domain` | usually absent | limited | Source-specific fallback |
| Tags | `tags.sentinelone` | not primary | custom properties | Canonical tags as array<string> with source attribution |

---

**Recommended redesign work items (silver -> gold)**

1. Define explicit key hierarchy per source:
   - SentinelOne: `id -> uuid -> fallback hash`.
   - Rapid7: `id`.
   - FortiSIEM: `naturalId -> fallback`.
2. Standardize time parsing contracts:
   - Classify each raw time field as `event_time`, `source_updated_at`, lifecycle timestamp, or duration.
3. Separate canonical vs extension payload:
   - Keep gold canonical surface small and stable.
   - Move deep nested inventories to extension JSON or side tables.
4. Revisit matching keys for gold:
   - Match on `(normalized_site, primary_ip)` first, fallback `(normalized_site, normalized_hostname)`.
   - Add confidence scoring for multi-source joins.
5. Tighten hash inputs:
   - Exclude volatile/non-canonical nested blobs from canonical hash.
   - Include only stable business attributes needed for change detection.

---

**LLM prompt-ready context**

When asking GPT/Claude to improve silver/gold logic, include:

1. `docs/bronze_to_silver/raw_source_schemas.md`
2. `docs/bronze_to_silver/raw_prod_bronze_describe_output.txt`
3. `docs/bronze_to_silver/mappings_and_rules.md`
4. `docs/silver_to_gold/gold_mappings_and_survivorship.md`

And provide this instruction stub:

```text
Use raw source schema differences (SentinelOne/Rapid7/FortiSIEM) to propose a canonical silver and gold redesign.
Keep canonical fields stable, avoid flattening deeply recursive objects, define deterministic identity keys,
and specify survivorship precedence with rationale.
```
