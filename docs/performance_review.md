# Kafka‑POC Performance Review Summary

Project: `kafka-poc` (Event‑to‑Lakehouse)  
Timeframe covered: from first commit to `45e7c80` (current `main`)

**Overview**
I delivered an end‑to‑end event‑to‑lakehouse pipeline that ingests Kafka events, lands raw data in bronze, infers schemas, normalizes to silver with SCD1/SCD2 semantics, and conforms to gold using contract‑driven rules. I stabilized the Docker stack (brokers, Spark connectivity, UI stability), improved observability and documentation, and added tooling that enables Schema Registry adoption for governance and evolution.

---

**Platform Components (Explicit Highlights)**
- **MinIO (S3‑compatible storage):** implemented event‑driven ingestion off MinIO object notifications; designed bucket layout for bronze data and warehouse metadata; used S3A endpoints across Spark, Iceberg, and schema inference jobs.
- **Apache Spark:** built streaming/batch jobs for schema inference, bronze→silver normalization, and silver conformance with structured transformations and scalable merge logic.
- **Apache Iceberg:** created and evolved Iceberg tables for bronze/silver/gold layers, used MERGE for SCD1/SCD2 semantics, and leveraged Iceberg snapshots for reproducibility and governance.
- **Kafka / Kafka Connect:** stabilized broker networking, connected Spark to Kafka for event‑driven ingestion, and prepared Connect for Iceberg sink + schema registry integration.

---

**Major Features**
- **Event‑driven schema inference service** (`scripts/schema_inferer.py`)  
  Built a Kafka‑driven schema inference pipeline that consumes MinIO object notifications, decodes and normalizes object keys, filters only valid topic paths, and persists a rolling window of recent files per topic in `iceberg.schema_registry.recent_files`. Implemented schema inference with Spark JSON reader (sampling support), hash‑based change detection to avoid unnecessary writes, and state metadata writes (schema hash, last attempt, success/failure) to S3. This yields fast, incremental schema discovery with auditability and reduced cost versus full scans.

- **Bronze → Silver normalization pipeline** (`scripts/bronze/bronze_assets_to_silver_assets.py`)  
  Implemented event‑driven ingestion from Kafka, used inferred schemas per topic to load JSON safely from MinIO, and built per‑source normalization functions that map Rapid7/FortiSIEM/Sentinel payloads into a shared target contract. Added deterministic identity (`entity_key_hash`) and payload hashing for idempotent merges, per‑batch deduplication by newest timestamp, and robust merge logic for both current (SCD1) and history (SCD2) Iceberg tables. Result: consistent asset model plus full change history.

- **Contract‑driven Silver → Gold conformance**  
  Built a silver conformance job that loads a YAML/JSON contract and applies ordered transformation rules (trim/lower/map/scale/clamp/regex/array cleanup/time parsing) to enforce consistent meaning across sources (e.g., risk score scaling, OS family mapping, IP list cleanup). Implemented type casting and payload hash recomputation, then merged into `iceberg.silver.assets_conformed` using Iceberg write semantics to ensure repeatable, governed outputs.

- **Schema Registry enablement (Docker + tooling)**  
  Integrated Schema Registry into the Docker stack, connected Kafbat UI for visibility, and built schema conversion tooling that exports Spark StructType JSON into Avro and JSON Schema. Added field‑name sanitization to satisfy Avro naming constraints (e.g., `v2.2` → `v2_2`) with a generated field‑map for traceability, enabling registry‑backed validation while preserving producer payloads.

---

**Minor Features**
- Implemented normalization mappings per source system (`scripts/mapping/sources/*`) with a shared target contract (`scripts/mapping/target.py`).
- Added consistent identity hashing (`entity_key_hash`) and payload hashing (`payload_hash`) to make merges deterministic and change‑aware.
- Added state metadata outputs for schema inference (schema hash, last attempt, sample files, change flags).
- Added/updated detailed docs for schema inference, bronze→silver, and silver→gold processing (`docs/`).
- Created reusable helper modules for Kafka notifications, MinIO reads, and normalization logic to keep pipeline code clean.

---

**Enhancements and Optimizations**
- **Event‑driven ingestion:** moved bronze ingestion to Kafka‑driven object notifications instead of directory scans to reduce latency and cost.
- **Rolling sampling for inference:** keep only latest N files in Iceberg to reduce infer‑time and ensure stable sampling.
- **Batch‑level deduplication:** resolve per‑batch duplicates by `entity_key_hash` to reduce conflicting updates.
- **Resilient reads:** enabled `ignoreMissingFiles` and `ignoreCorruptFiles` and added retry logic to tolerate transient S3 issues.
- **Idempotent SCD2:** deterministic `version_id` hashing ensures safe retries and exactly‑once history semantics.
- **Rule‑based conformance:** implemented operators like trim/lower/map/scale/clamp/array cleanup to standardize gold data with minimal manual code.

---

**Bug Fixes (Significant)**
- **Spark ↔ Kafka DNS resolution:** attached spark‑iceberg to the default Docker network and fixed bootstrap server configs to resolve broker DNS.
- **Kafbat UI stability:** fixed UI crashes by aligning broker access settings.
- **Silver merge stability:** hardened FortiSIEM and Sentinel ingestion to prevent unstable merges.
- **Schema conversion failures:** resolved Avro name legality errors (e.g., `v2.2`) by sanitizing field names and emitting field‑map files.
- **Container entrypoint fixes:** corrected Spark entrypoint execution issues (shell truncation), ensuring consistent startup.

---

**Refactoring / Architectural Improvements**
- **Modularized mapping logic:** moved source‑specific mappings into dedicated modules under `scripts/mapping/sources/`.
- **Clear medallion layering:** reorganized codebase into `scripts/bronze` and `scripts/mapping` to reinforce architecture.
- **Contract‑first gold layer:** decoupled business rules from code using a YAML/JSON data contract to improve change management.
- **Documentation refresh:** replaced scattered notes with structured, engineer‑readable docs in `docs/` that align to the actual code paths.

---

**Cross‑Team Collaboration / Support**
- No explicit cross‑team items are captured in this repo.  
  If needed, I can add concrete collaboration items here (e.g., coordinating topic schemas, reviewing producer payloads, helping with infra config).

---

**Research, POCs, and Innovation**
- **Schema inference POC → productionized flow:**  
  Iterated from manual schema inference to an automated, event‑driven, rolling‑window approach. Validated feasibility, then hardened into a reliable service.
- **Schema governance exploration:**  
  Assessed Schema Registry integration requirements and built conversion tooling (Spark StructType → Avro + JSON Schema) to support schema validation without changing producers.
- **Medallion architecture validation:**  
  Proved that bronze/silver/gold layering with Iceberg can support both auditability (history table) and standardized analytics (contract‑driven gold).
- **Normalization vs conformance separation:**  
  Introduced a two‑stage model where normalization aligns structure (silver) and conformance aligns meaning (gold). This separation reduces coupling and speeds future changes.
- **Kafka‑event‑based orchestration:**  
  Validated that MinIO object notifications can serve as a lightweight orchestration layer for Spark jobs, avoiding heavy schedulers for this POC.
