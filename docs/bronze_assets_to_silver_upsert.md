# Bronze Current → Silver Assets

This document explains the **Bronze Current → Silver** batch job implemented in `scripts/bronze/bronze_assets_to_silver_assets.py`.

---

**What This Job Does (One‑Line)**

It reads the **bronze_current** tables for each source, normalizes them into a shared silver schema, computes a deterministic `payload_hash`, and **MERGEs** into **source‑specific silver current and history tables** using **(source, entity_id)** as the business key.

---

**Inputs (Bronze Current Tables)**

- `iceberg.bronze_current.rapid7__assets__current`
- `iceberg.bronze_current.fortisiem__device__current`
- `iceberg.bronze_current.sentinalone__agents__current`

Each bronze_current table already contains:
- `entity_id`
- `source`
- raw/source fields
- ingest metadata (`ingest_ts`)

---

**Outputs (Silver Tables)**

Current tables (SCD1‑style):
- `iceberg.silver.rapid7__assets__silver__current`
- `iceberg.silver.fortisiem__device__silver__current`
- `iceberg.silver.sentinalone__agents__silver__current`

History tables (SCD2‑style):
- `iceberg.silver.rapid7__assets__silver__history`
- `iceberg.silver.fortisiem__device__silver__history`
- `iceberg.silver.sentinalone__agents__silver__history`

---

**Business Key (Identity)**

Silver identity is based on the **pair**:
- `source`
- `entity_id`

This avoids collisions across sources. The job also computes:
- `entity_key_str = source | entity_id`

The merge logic uses **(source, entity_id)** directly. The `entity_key_str` is retained for readability and debugging.

---

**Change Detection**

The job computes `payload_hash` from the normalized business payload fields defined in `scripts/mapping/target.py` (see `PAYLOAD_HASH_COLUMNS`).

Rules:
1. Same `(source, entity_id)` and same `payload_hash` → unchanged
2. Same `(source, entity_id)` and different `payload_hash` → changed (new version)
3. No existing `(source, entity_id)` → new record

---

**High‑Level Flow**

1. Read the bronze_current table for the source.
2. Validate required columns `source` and `entity_id` exist.
3. Normalize the data using the source‑specific mapping function.
4. Recompute `entity_key_str` from `(source, entity_id)`.
5. Dedupe to the latest row per `(source, entity_id)`.
6. Apply the conformance contract and compute `payload_hash`.
7. Merge into silver **history** (SCD2).
8. Merge into silver **current** (SCD1).

---

**Normalization (Per Source)**

Each source has a mapping function that reshapes raw fields into the shared target schema:
- `normalize_rapid7` in `scripts/mapping/sources/rapid7.py`
- `normalize_fortisiem` in `scripts/mapping/sources/fortisiem.py`
- `normalize_sentinel` in `scripts/mapping/sources/sentinel.py`

Each normalization function preserves:
- `source`
- `entity_id`

---

**Conformance Contract**

The job uses the contract at:
- `scripts/bronze/contracts/assets_silver_contract.yaml`

The contract defines:
- expected fields
- per‑field rules (trim, regex_replace, cast, etc.)

This contract is loaded once and cached (TTL controlled by `CONTRACT_CACHE_TTL_SEC`).

---

**Current Table Merge (SCD1)**

The current table holds the latest version per `(source, entity_id)`.

Behavior:
1. New key → INSERT
2. Same key + same payload → update timestamps only
3. Same key + different payload → update all fields and timestamps

Merge key:
- `t.source = s.source AND t.entity_id = s.entity_id`

---

**History Table Merge (SCD2)**

History keeps every version of a record.

Behavior:
1. New key → insert history row (`is_current = true`)
2. Changed payload → expire previous history row (`valid_to`, `is_current = false`) and insert new row
3. Same payload → no history insert

A deterministic `version_id` is derived from:
- `source`
- `entity_id`
- `payload_hash`
- `valid_from`

---

**Key Environment Variables**

Inputs:
- `RAPID7_BRONZE_CURRENT_TABLE`
- `FORTI_BRONZE_CURRENT_TABLE`
- `SENTINEL_BRONZE_CURRENT_TABLE`

Outputs:
- `RAPID7_SILVER_CURRENT_TABLE`
- `RAPID7_SILVER_HISTORY_TABLE`
- `FORTI_SILVER_CURRENT_TABLE`
- `FORTI_SILVER_HISTORY_TABLE`
- `SENTINEL_SILVER_CURRENT_TABLE`
- `SENTINEL_SILVER_HISTORY_TABLE`

Contract:
- `CONFORMED_CONTRACT_PATH`

Performance:
- `COALESCE_PARTITIONS`
- `CONTRACT_CACHE_TTL_SEC`

---

**How to Run (Docker)**

If `spark-iceberg` is configured to run this job:

```bash
docker compose up -d spark-iceberg
```

Check logs:

```bash
docker compose logs -f spark-iceberg
```

---

**How to Verify**

Current table:

```sql
SELECT source, entity_id, payload_hash, last_seen_at
FROM iceberg.silver.rapid7__assets__silver__current
ORDER BY ingest_ts DESC
LIMIT 5;
```

History table:

```sql
SELECT source, entity_id, payload_hash, valid_from, valid_to, is_current
FROM iceberg.silver.rapid7__assets__silver__history
ORDER BY valid_from DESC
LIMIT 5;
```

---

**Common Issues**

- Missing required columns (`source`, `entity_id`) in bronze_current.
- Contract path not found or invalid format (JSON/YAML parsing fails).
- Unexpected schema changes in normalization functions.
