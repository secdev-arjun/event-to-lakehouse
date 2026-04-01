**Gold Rework Docs Pack**

This folder is a self-contained documentation bundle for redesigning bronze -> silver -> gold logic with LLM assistance.

**Recommended read order**

1. `raw_prod_bronze_describe_output.txt`
2. `raw_source_schemas.md`
3. `raw_source_schema_summary.json`
4. `silver_mappings_and_rules.md`
5. `silver_normalized_schema.md`
6. `silver_normalized_schema.json`

**File guide**

- `raw_prod_bronze_describe_output.txt`
  - Exact `DESCRIBE TABLE` snapshots for:
    - `prod__bronze.prod__sentinelone__agents`
    - `prod__bronze.prod__rapid7__assets`
    - `prod__bronze.prod__fortisiem__devices`
- `raw_source_schemas.md`
  - Detailed raw schema analysis and redesign implications.
- `raw_source_schema_summary.json`
  - Machine-readable schema summary for tooling and prompt pipelines.
- `silver_mappings_and_rules.md`
  - End-to-end silver normalization process and source mapping logic.
- `silver_normalized_schema.md`
  - Field-level specification of the normalized silver schema.
- `silver_normalized_schema.json`
  - Machine-readable representation of the normalized silver schema.

**Suggested prompt context for GPT/Claude**

Include all files in this folder and ask for:

- deterministic source identity strategy,
- timestamp normalization policy,
- canonical-vs-extension field split,
- survivorship precedence proposals for gold,
- hash/change-detection redesign recommendations.

