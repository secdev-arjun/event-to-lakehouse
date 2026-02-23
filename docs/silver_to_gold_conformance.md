# Silver to Gold Conformance

This document explains the **Gold conformance step** in very simple words, but with enough detail that an engineer can implement or review it. It includes **code snippets** and explanations of what those snippets do.

---

**Big Idea**

Imagine three friends give you toys. All the toys are in one big box (Silver). That is good, because everything is in one place.

But one friend calls a “big” toy **10 inches**, and another friend calls a “big” toy **1 inch**. Same word, different meaning.

The **Gold** step fixes that. It makes sure **“big” always means the same size**, so everyone agrees.

---

**Why We Need This Step**

Our Silver table already has the same columns for every source. That is **normalization**.

But normalization alone does not guarantee **consistent meaning**.

Examples of inconsistency:
- `risk_score` might be on a 0‑100 scale in one source and 0‑10 in another.
- `os_architecture` might be `x64` in one source and `64 bit` in another.
- `tags` might have duplicates or nulls.

**Gold conformance** fixes these differences using **rules**.

---

**What We Built**

We built a **contract‑driven Spark job** that:
1. Reads the Silver table
2. Applies rules (the contract)
3. Writes to a Gold table

The contract is a YAML file that lists the rules for every column.

---

**Files Involved**

- `scripts/gold/contracts/assets_gold_contract.yaml`  
  This is the **data contract** (rules for each column).

- `scripts/gold/silver_to_gold_conformance.py`  
  This is the **Spark job** that applies the contract.

- `docker-compose.yml`
  Adds a batch service `spark-conformance` to run the job.

---

**What the Contract Looks Like**

The contract file is **YAML**, but we wrote it in **JSON style** so it can be parsed by `json.loads` as well.

Here is a small snippet (real format):

```json
{
  "source_table": "iceberg.silver.assets",
  "target_table": "iceberg.gold.assets_conformed",
  "fields": {
    "risk_score": {
      "type": "double",
      "nullable": true,
      "rules": [
        {"op": "scale_if_gt", "value": 10, "factor": 0.1}
      ]
    }
  }
}
```

**Meaning**:
- `source_table` tells us where to read
- `target_table` tells us where to write
- `fields` contains **one entry per column**

Each field can define:
- `type`: final Spark type
- `nullable`: if null is allowed
- `rules`: transformations applied in order

---

**Rule Operators You Can Use**

These are the operators supported by the job:

- `trim` removes spaces at the start and end
- `lower` makes text lowercase
- `upper` makes text uppercase
- `regex_replace` replaces parts of a string
- `split` splits a string into an array
- `scale` multiplies by a factor
- `scale_if_gt` multiplies only if value is greater than a threshold
- `clamp` limits a value to a min and/or max
- `map` replaces values using a lookup map
- `array_distinct` removes duplicates in arrays
- `array_sort` sorts arrays
- `array_filter_nulls` removes nulls inside arrays
- `to_timestamp` parses string into timestamp

---

**Examples of Real Rules**

**Example 1: Risk score normalization**

```json
"risk_score": {
  "type": "double",
  "nullable": true,
  "rules": [
    {"op": "scale_if_gt", "value": 10, "factor": 0.1}
  ]
}
```

If `risk_score` is 0‑100, this rule scales it down to 0‑10.

**Example 2: OS family mapping**

```json
"os_family": {
  "type": "string",
  "nullable": true,
  "rules": [
    {"op": "trim"},
    {"op": "lower"},
    {"op": "map", "values": {"win": "Windows", "linux": "Linux"}, "default": null}
  ]
}
```

If a row says `win`, it becomes `Windows`.

**Example 3: IP array cleanup**

```json
"ip_addresses": {
  "type": "array<string>",
  "nullable": true,
  "rules": [
    {"op": "array_filter_nulls"},
    {"op": "array_distinct"},
    {"op": "array_sort"}
  ]
}
```

This removes nulls, removes duplicates, and sorts the list.

---

**How the Spark Job Works (Step‑By‑Step)**

The job lives in `scripts/gold/silver_to_gold_conformance.py`.

The flow is:
1. Load contract
2. Read the silver table
3. Apply rules to every column
4. Recompute `payload_hash`
5. Write to gold table using MERGE

---

**Code Walkthrough (With Explanation)**

**1) Load the contract file**

```python
def load_contract(path: str) -> dict:
    raw = _read_contract_text(path).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        import yaml
        return yaml.safe_load(raw)
```

Explanation:
- Reads the contract file text
- Tries JSON parsing first
- Falls back to YAML parsing

This lets the contract be **YAML** or **JSON‑style YAML**.

---

**2) Apply one rule at a time**

```python
def apply_rules(col_expr, rules):
    for rule in rules:
        op = rule.get("op")
        if op == "trim":
            expr = F.trim(expr)
        elif op == "scale_if_gt":
            expr = F.when(expr > lit(value), expr * lit(factor)).otherwise(expr)
```

Explanation:
- Takes a column expression and a list of rules
- Applies each rule in order
- Returns the transformed column

Rules are **sequenced**, so order matters.

---

**3) Build a conformed DataFrame**

```python
def conform_df(df, contract):
    for name, spec in contract.get("fields", {}).items():
        expr = col(name) if name in df.columns else lit(None)
        expr = apply_rules(expr, spec.get("rules"))
        if spec.get("type"):
            expr = expr.cast(spec.get("type"))
        df = df.withColumn(name, expr)
```

Explanation:
- Loops through each field in the contract
- If the column exists, use it; if not, create `NULL`
- Applies rules and casts to type

This guarantees **every field exists and follows the contract**.

---

**4) Ensure Gold table exists and has all columns**

```python
def ensure_gold_table(spark, df, table_name):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    if not spark.catalog.tableExists(table_name):
        df.limit(0).writeTo(table_name).create()
```

Explanation:
- Creates the namespace if missing
- Creates the table if missing (empty schema)
- This is safe to run repeatedly

---

**5) Merge into the Gold table**

```python
merge_sql = f"""
    MERGE INTO {table_name} t
    USING conformed_updates s
    ON t.entity_key_hash = s.entity_key_hash
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...
"""
```

Explanation:
- Uses `entity_key_hash` as the stable ID
- Updates existing rows
- Inserts new rows if not found

This makes the job **idempotent** and safe to re‑run.

---

**Important Detail: payload_hash**

After applying rules, the job recomputes `payload_hash` so that:
- It always reflects the **conformed values**
- Changes in Gold are tracked correctly

This happens here:

```python
from mapping.target import add_payload_hash

conformed = add_payload_hash(conformed)
```

---

**How to Run the Job**

Run the new service once:

```bash
docker compose up -d spark-conformance
```

View logs:

```bash
docker compose logs -f spark-conformance
```

---

**How to Verify the Result**

Check Gold table:

```sql
SELECT * FROM iceberg.gold.assets_conformed LIMIT 5;
```

Compare Silver vs Gold:

```sql
SELECT
  s.entity_key_hash,
  s.risk_score AS silver_risk,
  g.risk_score AS gold_risk
FROM iceberg.silver.assets s
JOIN iceberg.gold.assets_conformed g
  ON s.entity_key_hash = g.entity_key_hash
LIMIT 10;
```

If rules applied, you should see transformed values in Gold.

---

**Common Problems and What They Mean**

- `Failed to parse contract`
  The contract file is not valid YAML/JSON.

- `Column not found`
  The contract references a field that is missing in Silver.

- `Type cast failed`
  The contract expects a type that doesn’t match data. Add a rule or adjust the type.

---

**Summary (Short and Simple)**

- Silver gives us the same columns
- Gold gives us the same meaning
- The contract defines meaning rules
- The Spark job applies rules and merges into Gold
- Safe to re‑run

---

**Where to Edit When Rules Change**

- Edit rules in `scripts/gold/contracts/assets_gold_contract.yaml`
- Re‑run the batch job
- Gold will update in place

---

**End**

If you need this explained with a real data example from Rapid7, Sentinel, or FortiSIEM, ask and I’ll add a walkthrough.
