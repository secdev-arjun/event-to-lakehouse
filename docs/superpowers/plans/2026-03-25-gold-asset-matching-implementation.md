# Gold Asset Matching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current score-based gold matching core with a deterministic pairwise rule engine that emits accepted edges, review rows, unmatched rows, and rule metrics, while writing only accepted multi-source entities to `gold_assets_current` in this phase.

**Architecture:** Keep `scripts/gold/build_gold_assets.py` as the orchestration entrypoint and preserve the existing config/table conventions plus the survivorship/writer shape. Replace the matching core with centralized rule definitions, source matching views, ordered pairwise rule stages, and accepted-edge connected-component consolidation. Singletons remain supported in the architecture but stay routed to unmatched output instead of `gold_assets_current`.

**Tech Stack:** Python, PySpark, Spark SQL window functions, Apache Iceberg, pytest, local Spark test fixtures

---

## Planned File Map

**Create**

- `scripts/gold/rules.py`
  - centralized rule dataclass/config builder for all pairwise rule stages
- `tests/conftest.py`
  - repo-level pytest bootstrap and SparkSession fixture
- `tests/gold/test_rules.py`
  - rule-definition and config-surface tests
- `tests/gold/test_prepare_sources.py`
  - source matching view tests
- `tests/gold/test_matching_pairs.py`
  - source-internal ranking, qualification, and stage output tests
- `tests/gold/test_matching.py`
  - pairwise hierarchy orchestration and metrics tests
- `tests/gold/test_grouping.py`
  - accepted-edge component and review-routing tests
- `tests/gold/test_survivorship.py`
  - deterministic metadata and final row-shape tests
- `tests/gold/test_build_gold_assets.py`
  - entrypoint-level orchestration tests with mocked table writers

**Modify**

- `scripts/gold/config.py`
  - add output table constants and matching configuration defaults
- `scripts/gold/prepare_sources.py`
  - build source matching views and ranking helpers from silver
- `scripts/gold/matching_pairs.py`
  - replace score-based pair logic with rule-stage ranking/qualification engine
- `scripts/gold/matching.py`
  - orchestrate all pairwise hierarchies and combine outputs
- `scripts/gold/grouping.py`
  - replace current grouping logic with accepted-edge connected components and review routing
- `scripts/gold/survivorship.py`
  - build final gold rows from accepted multi-source components only
- `scripts/gold/writer.py`
  - add generic write helpers for staging/review/metrics/unmatched outputs while keeping current table alignment behavior
- `scripts/gold/build_gold_assets.py`
  - wire the new matching engine, component consolidation, output writes, and final current write

## Task 1: Test Harness and Config Surface

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/gold/test_rules.py`
- Create: `scripts/gold/rules.py`
- Modify: `scripts/gold/config.py`

- [ ] **Step 1: Write the failing tests for rule/config wiring**

```python
def test_conftest_bootstraps_gold_package_and_spark(spark):
    from gold.rules import build_rule_definitions

    assert spark is not None
    assert callable(build_rule_definitions)


def test_build_rule_definitions_returns_ranked_rules_per_pair():
    from gold.rules import build_rule_definitions

    rules = build_rule_definitions()

    assert ("rapid7", "fortisiem") in rules
    assert ("rapid7", "sentinalone") in rules
    assert ("fortisiem", "sentinalone") in rules
    assert [r.rule_name for r in rules[("fortisiem", "sentinalone")]][0] == "serial_org_exact"


def test_gold_config_exposes_new_output_tables():
    from gold import config

    assert config.GOLD_MATCH_CANDIDATES_TABLE.startswith("iceberg.gold.")
    assert config.GOLD_MATCH_REVIEW_TABLE.startswith("iceberg.gold.")
    assert config.GOLD_ASSETS_UNMATCHED_TABLE.startswith("iceberg.gold.")
    assert config.GOLD_MATCH_METRICS_TABLE.startswith("iceberg.gold.")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/gold/test_rules.py -q`

Expected: FAIL because `gold.rules` and the new config constants do not exist yet.

- [ ] **Step 3: Write minimal implementation for config and rule definitions**

```python
@dataclass(frozen=True)
class MatchRule:
    rule_name: str
    rule_rank: int
    source_pair: tuple[str, str]
    left_required_columns: tuple[str, ...]
    right_required_columns: tuple[str, ...]
    left_key_columns: tuple[str, ...]
    right_key_columns: tuple[str, ...]
    auto_accept: bool
    emit_review: bool
    description: str
```

`tests/conftest.py` responsibilities:

- prepend `<repo>/scripts` to `sys.path`
- expose a session-scoped local Spark fixture
- stop Spark cleanly after the test session

Rule matrix to encode explicitly in `build_rule_definitions()`:

- `("rapid7", "fortisiem")`
  - enable: `physical_mac_org_exact`, `org_site_ip_exact`, `org_site_hostname_exact`, `org_ip_os_family_exact`, `org_hostname_os_family_exact`, `org_ip_exact`, `org_hostname_exact`
  - disable: `serial_org_exact`
- `("rapid7", "sentinalone")`
  - enable: `physical_mac_org_exact`, `org_site_ip_exact`, `org_site_hostname_exact`, `org_ip_os_family_exact`, `org_hostname_os_family_exact`, `org_ip_exact`, `org_hostname_exact`
  - disable: `serial_org_exact`
- `("fortisiem", "sentinalone")`
  - enable: `serial_org_exact`, `physical_mac_org_exact`, `org_site_ip_exact`, `org_site_hostname_exact`, `org_ip_os_family_exact`, `org_hostname_os_family_exact`, `org_ip_exact`, `org_hostname_exact`

Add config constants:

- `GOLD_MATCH_CANDIDATES_TABLE`
- `GOLD_MATCH_REVIEW_TABLE`
- `GOLD_ASSETS_UNMATCHED_TABLE`
- `GOLD_MATCH_METRICS_TABLE`

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/gold/test_rules.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/conftest.py tests/gold/test_rules.py scripts/gold/rules.py scripts/gold/config.py
git commit -m "test: add gold rule config surface"
```

## Task 2: Build Source Matching Views

**Files:**
- Create: `tests/gold/test_prepare_sources.py`
- Modify: `scripts/gold/prepare_sources.py`

- [ ] **Step 1: Write the failing tests for source matching views**

```python
def test_prepare_source_builds_matching_helpers(spark):
    from gold.prepare_sources import prepare_source

    df = spark.createDataFrame(
        [("1", "rapid7", "Org A", "HQ", "Host1", "10.0.0.1", None, "SER1", "Windows", "10", ["aa:bb"], [], [], None, None, None)],
        "entity_id string, source_system string, normalised_org_name string, site_name string, primary_hostname string, primary_ip string, access_ip string, serial_number string, os_family string, os_version string, mac_addresses array<string>, gateway_mac_addresses array<string>, virtual_mac_addresses array<string>, asset_name string, source_display_name string, source_updated_at timestamp",
    )

    out = prepare_source(df, "rapid7")
    row = out.select("org_key", "site_key", "hostname_key", "ip_key", "serial_key", "physical_mac_keys", "site_present_flag", "evidence_completeness_score").first()

    assert row.org_key == "org a"
    assert row.site_key == "hq"
    assert row.hostname_key == "host1"
    assert row.ip_key == "10.0.0.1"
    assert row.serial_key == "ser1"
    assert row.physical_mac_keys == ["aa:bb"]
    assert row.site_present_flag is True
    assert row.evidence_completeness_score >= 5
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/gold/test_prepare_sources.py -q`

Expected: FAIL because the new helper columns and completeness scoring do not exist yet.

- [ ] **Step 3: Implement the source matching view helpers**

```python
def prepare_source(df: DataFrame, source_name: str) -> DataFrame:
    return (
        ...
        .withColumn("org_key", _lower_trim(F.col("normalised_org_name")))
        .withColumn("site_key", _lower_trim(F.col("site_name")))
        .withColumn("hostname_key", _lower_trim(F.coalesce(F.col("primary_hostname"), F.col("asset_name"), F.col("source_display_name"))))
        .withColumn("ip_key", _preferred_ip_expr(source_name))
        .withColumn("serial_key", _lower_trim(F.col("serial_number")))
        .withColumn("physical_mac_keys", _physical_mac_array(...))
        .withColumn("site_present_flag", F.col("site_key").isNotNull())
        .withColumn("evidence_completeness_score", _completeness_score(...))
        .withColumn("freshness_ts", F.coalesce("source_updated_at", "last_seen_at", "ingest_ts"))
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/gold/test_prepare_sources.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/gold/test_prepare_sources.py scripts/gold/prepare_sources.py
git commit -m "feat: add gold source matching views"
```

## Task 3: Implement Source-Internal Ranking and Rule Stage Qualification

**Files:**
- Create: `tests/gold/test_matching_pairs.py`
- Modify: `scripts/gold/matching_pairs.py`

- [ ] **Step 1: Write the failing tests for ranking and ambiguity handling**

```python
def test_rank_prefers_site_then_completeness_then_freshness(spark):
    from gold.matching_pairs import rank_source_candidates_for_rule
    from gold.rules import MatchRule

    rule = MatchRule(rule_name="org_ip_exact", rule_rank=7, source_pair=("rapid7", "fortisiem"), left_required_columns=("org_key", "ip_key"), right_required_columns=("org_key", "ip_key"), left_key_columns=("org_key", "ip_key"), right_key_columns=("org_key", "ip_key"), auto_accept=False, emit_review=True, description="review")
    df = spark.createDataFrame(
        [
            ("e1", "org a", None, "10.0.0.1", "h1", None, None, None, 2, None),
            ("e2", "org a", "hq", "10.0.0.1", "h1", None, None, None, 2, None),
        ],
        "entity_id string, org_key string, site_key string, ip_key string, hostname_key string, serial_key string, os_family_key string, source_record_id string, evidence_completeness_score int, freshness_ts timestamp",
    )

    ranked = rank_source_candidates_for_rule(df, rule, "left")
    winner = ranked.filter("row_rank = 1").first()

    assert winner.entity_id == "e2"
    assert winner.preferred_by_site is True


def test_stable_tie_breaker_does_not_remove_semantic_ambiguity(spark):
    from gold.matching_pairs import qualify_source_keys
    ...
    assert qualified.filter("is_semantically_ambiguous").count() == 1
    assert qualified.filter("is_safe_for_auto_match").count() == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/gold/test_matching_pairs.py -q`

Expected: FAIL because ranking/qualification helpers do not exist yet.

- [ ] **Step 3: Implement minimal ranking and qualification helpers**

```python
def rank_source_candidates_for_rule(df: DataFrame, rule: MatchRule, side: str) -> DataFrame:
    w = Window.partitionBy("rule_key").orderBy(
        F.col("site_present_flag").desc(),
        F.col("evidence_completeness_score").desc(),
        F.col("freshness_ts").desc(),
        F.col("entity_id").asc(),
    )
    ...


def qualify_source_keys(df: DataFrame, rule: MatchRule, side: str) -> DataFrame:
    return (
        ...
        .withColumn("is_semantically_ambiguous", F.col("top_semantic_count") > F.lit(1))
        .withColumn("is_safe_for_auto_match", (~F.col("is_semantically_ambiguous")) & F.col("rule_key").isNotNull())
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/gold/test_matching_pairs.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/gold/test_matching_pairs.py scripts/gold/matching_pairs.py
git commit -m "feat: add deterministic rule-stage ranking"
```

## Task 4: Implement Pairwise Rule Hierarchies, Residue Progression, and Metrics

**Files:**
- Create: `tests/gold/test_matching.py`
- Modify: `scripts/gold/matching_pairs.py`
- Modify: `scripts/gold/matching.py`

- [ ] **Step 1: Write the failing tests for pairwise stage execution**

```python
def test_run_pairwise_rule_hierarchy_emits_auto_review_and_residue(spark):
    from gold.matching import run_pairwise_rule_hierarchy

    result = run_pairwise_rule_hierarchy(left_df, right_df, rules)

    assert result.accepted_edges.filter("rule_name = 'org_site_ip_exact'").count() == 1
    assert result.review_rows.count() == 1
    assert result.left_residue.count() == 1
    assert result.metrics.filter("rule_name = 'org_ip_exact'").count() == 1


def test_skipped_rule_due_to_missing_columns_emits_zero_count_metric(spark):
    ...
    metric = result.metrics.filter("rule_name = 'serial_org_exact'").first()
    assert {"source_pair", "rule_name", "rule_rank", "rule_status", "left_candidate_rows", "right_candidate_rows", "left_duplicate_keys", "right_duplicate_keys", "left_rows_preferred_by_site", "right_rows_preferred_by_site", "auto_matches_count", "ambiguous_count", "unmatched_remaining_count"} <= set(result.metrics.columns)
    assert metric.rule_status == "skipped_missing_columns"
    assert metric.auto_matches_count == 0
    assert metric.left_candidate_rows == 0
    assert metric.right_candidate_rows == 0
    assert metric.left_duplicate_keys == 0
    assert metric.right_duplicate_keys == 0
    assert metric.left_rows_preferred_by_site == 0
    assert metric.right_rows_preferred_by_site == 0
    assert metric.ambiguous_count == 0
    assert metric.unmatched_remaining_count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/gold/test_matching.py -q`

Expected: FAIL because hierarchy execution and metrics outputs do not exist yet.

- [ ] **Step 3: Implement pairwise hierarchy execution**

```python
def match_pair_by_rule(left_df: DataFrame, right_df: DataFrame, rule: MatchRule) -> RuleStageResult:
    ...


def run_pairwise_rule_hierarchy(left_df: DataFrame, right_df: DataFrame, rules: list[MatchRule]) -> PairwiseHierarchyResult:
    current_left = left_df
    current_right = right_df
    for rule in rules:
        stage = match_pair_by_rule(current_left, current_right, rule)
        current_left = stage.left_residue
        current_right = stage.right_residue
        ...

    return PairwiseHierarchyResult(
        accepted_edges=accepted_edges,
        review_rows=review_rows,
        metrics=metrics,
        left_residue=current_left,
        right_residue=current_right,
    )


def build_unmatched_rows(
    rapid7_df: DataFrame,
    forti_df: DataFrame,
    sentinel_df: DataFrame,
    accepted_edges_df: DataFrame,
) -> DataFrame:
    # Build the final singleton-ready unmatched dataset from the original
    # prepared source views minus any entity_id present in accepted edges.
    ...


def match_sources(sentinel_df: DataFrame, rapid7_df: DataFrame, forti_df: DataFrame) -> GoldMatchingResult:
    sentinel_prepared, rapid7_prepared, forti_prepared = prepare_sources(sentinel_df, rapid7_df, forti_df)
    r7_fsm = run_pairwise_rule_hierarchy(rapid7_prepared, forti_prepared, build_rule_definitions()[("rapid7", "fortisiem")])
    r7_s1 = run_pairwise_rule_hierarchy(rapid7_prepared, sentinel_prepared, build_rule_definitions()[("rapid7", "sentinalone")])
    fsm_s1 = run_pairwise_rule_hierarchy(forti_prepared, sentinel_prepared, build_rule_definitions()[("fortisiem", "sentinalone")])
    return GoldMatchingResult(
        sentinel_prepared=sentinel_prepared,
        rapid7_prepared=rapid7_prepared,
        forti_prepared=forti_prepared,
        accepted_edges=r7_fsm.accepted_edges.unionByName(r7_s1.accepted_edges, allowMissingColumns=True).unionByName(fsm_s1.accepted_edges, allowMissingColumns=True),
        review_rows=r7_fsm.review_rows.unionByName(r7_s1.review_rows, allowMissingColumns=True).unionByName(fsm_s1.review_rows, allowMissingColumns=True),
        metrics=r7_fsm.metrics.unionByName(r7_s1.metrics, allowMissingColumns=True).unionByName(fsm_s1.metrics, allowMissingColumns=True),
    )
```

`build_unmatched_rows(...)` return contract:

- one row per source observation not present in any accepted edge
- required columns:
  - `source_system`
  - `entity_id`
  - `source_record_id`
  - `source_natural_id`
  - `singleton_basis`
  - `unmatched_reason`
  - `ingest_ts`
  - `last_seen_at`
  - `source_updated_at`
  - `normalised_org_name`
  - `site_name`
  - `primary_hostname`
  - `primary_ip`
  - `access_ip`
  - `asset_name`

Top-level orchestration contract:

```python
@dataclass
class GoldMatchingResult:
    sentinel_prepared: DataFrame
    rapid7_prepared: DataFrame
    forti_prepared: DataFrame
    accepted_edges: DataFrame
    review_rows: DataFrame
    metrics: DataFrame


def match_sources(sentinel_df: DataFrame, rapid7_df: DataFrame, forti_df: DataFrame) -> GoldMatchingResult:
    ...
```

`match_sources(...)` must:

- build all three prepared source matching views
- run the three pairwise hierarchies
- union accepted edges across pairs
- union review rows across pairs
- union rule metrics across pairs
- return the prepared source views needed later by `build_unmatched_rows(...)` and survivorship

`result.metrics` required columns:

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

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/gold/test_matching.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/gold/test_matching.py scripts/gold/matching_pairs.py scripts/gold/matching.py
git commit -m "feat: add pairwise gold rule hierarchies"
```

## Task 5: Implement Accepted-Edge Component Construction and Review Routing

**Files:**
- Create: `tests/gold/test_grouping.py`
- Modify: `scripts/gold/grouping.py`

- [ ] **Step 1: Write the failing tests for component consolidation**

```python
def test_build_entity_groups_uses_only_accepted_edges(spark):
    from gold.grouping import build_entity_groups

    groups, review = build_entity_groups(accepted_edges_df)

    assert groups.filter("source_count = 2").count() == 1
    assert review.filter("review_reason = 'duplicate_source_in_component'").count() == 0


def test_component_with_two_rows_from_same_source_routes_to_review(spark):
    ...
    assert groups.count() == 0
    assert review.filter("review_reason = 'duplicate_source_in_component'").count() == 1


def test_singleton_only_component_stays_out_of_accepted_groups(spark):
    ...
    assert groups.count() == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/gold/test_grouping.py -q`

Expected: FAIL because grouping still uses the old pair-score logic.

- [ ] **Step 3: Implement accepted-edge grouping**

```python
def build_entity_groups(accepted_edges_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    # Build component ids from accepted edges only.
    # Route invalid multi-row same-source components to review.
    # Keep only components with at least two distinct sources as accepted.
    ...
```

`build_entity_groups(...)` accepted-groups return contract:

- one row per accepted component
- required columns:
  - `component_id`
  - `rapid7_entity_id`
  - `fortisiem_entity_id`
  - `sentinalone_entity_id`
  - `source_count`
  - `edge_count`
  - `match_rule_summary`
  - `min_match_rule_rank`

`build_entity_groups(...)` review return contract:

- one row per rejected component
- required columns:
  - `component_id`
  - `review_reason`
  - `rapid7_entity_ids`
  - `fortisiem_entity_ids`
  - `sentinalone_entity_ids`
  - `source_count`
  - `edge_count`
  - `match_rule_summary`
  - `min_match_rule_rank`

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/gold/test_grouping.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/gold/test_grouping.py scripts/gold/grouping.py
git commit -m "feat: add accepted-edge gold components"
```

## Task 6: Adapt Survivorship for Accepted Multi-Source Components

**Files:**
- Create: `tests/gold/test_survivorship.py`
- Modify: `scripts/gold/survivorship.py`

- [ ] **Step 1: Write the failing tests for deterministic gold metadata**

```python
def test_build_gold_rows_includes_component_metadata(spark):
    from gold.survivorship import build_gold_rows

    gold_df = build_gold_rows(sentinel_df, rapid7_df, forti_df, accepted_groups_df, accepted_edges_df)
    row = gold_df.first()

    assert row.source_count == 2
    assert row.edge_count == 1
    assert row.match_rule_summary == ["org_site_ip_exact"]
    assert row.min_match_rule_rank == 3
    assert row.seen_in_rapid7 is True
    assert row.seen_in_fortisiem is True


def test_build_gold_rows_excludes_singletons_from_current_phase(spark):
    ...
    assert gold_df.count() == 0


def test_unmatched_row_shape_stays_singleton_ready(spark):
    from gold.matching import build_unmatched_rows

    unmatched = build_unmatched_rows(rapid7_df, forti_df, sentinel_df, accepted_edges_df)
    row = unmatched.first()

    assert row.source_system is not None
    assert row.entity_id is not None
    assert row.singleton_basis is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/gold/test_survivorship.py -q`

Expected: FAIL because survivorship still expects the old score-based grouping output.

- [ ] **Step 3: Implement minimal survivorship changes**

```python
def build_gold_rows(...):
    ...
    .withColumn("source_count", ...)
    .withColumn("edge_count", ...)
    .withColumn("match_rule_summary", ...)
    .withColumn("min_match_rule_rank", ...)
```

Keep:

- `seen_in_*` flags
- `matched_sources`
- source entity ids
- canonical field survivorship
- array unioning

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/gold/test_survivorship.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/gold/test_survivorship.py scripts/gold/survivorship.py
git commit -m "feat: adapt gold survivorship to accepted components"
```

## Task 7: Wire Output Tables and Entrypoint Orchestration

**Files:**
- Create: `tests/gold/test_build_gold_assets.py`
- Modify: `scripts/gold/writer.py`
- Modify: `scripts/gold/build_gold_assets.py`
- Modify: `scripts/gold/matching.py`

- [ ] **Step 1: Write the failing tests for output orchestration**

```python
def test_build_gold_assets_writes_staging_outputs_and_current(monkeypatch, spark):
    from gold.build_gold_assets import main

    writes = []
    monkeypatch.setattr("gold.build_gold_assets.read_table", lambda *args, **kwargs: spark.createDataFrame([], "entity_id string"))
    monkeypatch.setattr("gold.build_gold_assets.match_sources", lambda *args, **kwargs: pairwise_result)
    monkeypatch.setattr("gold.build_gold_assets.build_entity_groups", lambda *args, **kwargs: (accepted_groups_df, component_review_df))
    monkeypatch.setattr("gold.build_gold_assets.build_unmatched_rows", lambda *args, **kwargs: unmatched_df)
    monkeypatch.setattr("gold.build_gold_assets.build_gold_rows", lambda *args, **kwargs: gold_df)
    monkeypatch.setattr("gold.writer.write_gold_table", lambda df, table_name: writes.append(table_name))
    monkeypatch.setattr("gold.writer.write_gold_current", lambda df, table_name: writes.append(table_name))

    main()

    assert "iceberg.gold.match_candidates" in writes
    assert "iceberg.gold.match_review" in writes
    assert "iceberg.gold.assets_unmatched" in writes
    assert "iceberg.gold.match_metrics" in writes
    assert "iceberg.gold.assets_current" in writes
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/gold/test_build_gold_assets.py -q`

Expected: FAIL because the entrypoint still writes only `gold_assets_current`.

- [ ] **Step 3: Implement writer helpers and orchestration**

```python
def write_gold_table(df: DataFrame, table_name: str):
    ensure_table(df, table_name)
    align_df_to_table(df, table_name).writeTo(table_name).overwrite(F.lit(True))


def main():
    ...
    pairwise = match_sources(...)
    accepted_groups, component_review = build_entity_groups(pairwise.accepted_edges)
    unmatched_rows = build_unmatched_rows(pairwise.rapid7_prepared, pairwise.forti_prepared, pairwise.sentinel_prepared, pairwise.accepted_edges)
    gold_df = build_gold_rows(..., accepted_groups, pairwise.accepted_edges)
    write_gold_table(pairwise.accepted_edges, GOLD_MATCH_CANDIDATES_TABLE)
    write_gold_table(pairwise.review_rows.unionByName(component_review, allowMissingColumns=True), GOLD_MATCH_REVIEW_TABLE)
    write_gold_table(unmatched_rows, GOLD_ASSETS_UNMATCHED_TABLE)
    write_gold_table(pairwise.metrics, GOLD_MATCH_METRICS_TABLE)
    write_gold_current(gold_df, GOLD_ASSETS_CURRENT_TABLE)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/gold/test_build_gold_assets.py -q`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/gold/test_build_gold_assets.py scripts/gold/writer.py scripts/gold/build_gold_assets.py scripts/gold/matching.py
git commit -m "feat: wire deterministic gold outputs"
```

## Task 8: Full Verification

**Files:**
- Modify: `docs/superpowers/plans/2026-03-25-gold-asset-matching-implementation.md`

- [ ] **Step 1: Run focused gold tests**

Run: `python -m pytest tests/gold -q`

Expected: PASS

- [ ] **Step 2: Run a targeted smoke test for the gold package**

Run: `python -m pytest tests/gold/test_matching.py tests/gold/test_grouping.py tests/gold/test_survivorship.py -q`

Expected: PASS

- [ ] **Step 3: Run the full verification command one more time from a clean tree**

Run: `git status --short`

Expected: only intended gold/test/plan changes remain

- [ ] **Step 4: Record verification notes in the final handoff**

Capture:

- tests run
- whether staging outputs and current output were exercised
- any remaining risks, especially around Spark/Iceberg integration tests not executed in Docker

- [ ] **Step 5: Commit**

```bash
git add scripts/gold tests/gold
git commit -m "feat: replace gold matching engine"
```

## Implementation Notes

- Keep the rule hierarchy centralized in `scripts/gold/rules.py`; do not bury rule ordering inside `if/else` branches.
- Keep null-key handling explicit: null means not eligible for that rule stage.
- Ensure rule metrics are emitted even when a rule is skipped due to missing columns.
- Keep connected-component construction restricted to accepted edges only.
- Keep same-source duplicate components out of `gold_assets_current`.
- Keep singleton-ready outputs in `gold_assets_unmatched`, but do not promote them into `gold_assets_current`.

## Verification Commands Summary

- `python -m pytest tests/gold/test_rules.py -q`
- `python -m pytest tests/gold/test_prepare_sources.py -q`
- `python -m pytest tests/gold/test_matching_pairs.py -q`
- `python -m pytest tests/gold/test_matching.py -q`
- `python -m pytest tests/gold/test_grouping.py -q`
- `python -m pytest tests/gold/test_survivorship.py -q`
- `python -m pytest tests/gold/test_build_gold_assets.py -q`
- `python -m pytest tests/gold -q`
