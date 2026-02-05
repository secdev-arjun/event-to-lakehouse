# Automatic Schema Generator (Notion‑Ready)

This document explains `scripts/schema_inferer.py` in simple, step‑by‑step language. The goal is to make the script easy to understand, easy to operate, and easy to troubleshoot.

## One‑Line Summary

The script scans every topic under `s3a://bronze/topics/`, infers a JSON schema from the newest files, and stores that schema under `s3a://warehouse/schemas/<topic>/schema/` along with metadata under `.../_state/`.

## What It Produces

For each topic:

- Schema: `s3a://warehouse/schemas/<topic>/schema/` (contains a `part-*.txt` with the schema JSON)
- State: `s3a://warehouse/schemas/<topic>/_state/` (metadata JSON written as text)

If you open `_state/`, you will see metadata, not the schema. The schema is only in `schema/`.

## The Big Idea (kid‑friendly)

Think of each topic as a shelf of books. Each book is a JSON file.

1. The script looks at every shelf.
2. It grabs the newest few books.
3. It asks Spark, “What is the shape of the stories inside?”
4. It saves that shape as the topic’s schema.
5. It remembers what it did last time so it doesn’t repeat work.

## How It Works (Step‑by‑Step)

1. Find all topic folders under `s3a://bronze/topics/`.
2. For each topic, list all files inside (recursively).
3. Compare newest file time with the last processed time.
4. If nothing new, skip that topic.
5. If new files exist, pick the newest N files.
6. Spark reads those files and infers a schema.
7. The schema is saved only if it changed.
8. State metadata is always updated.

## Configuration Knobs

You can set these using environment variables.

| Variable | Default | Meaning |
| --- | --- | --- |
| `BRONZE_ROOT` | `s3a://bronze/topics/` | Where to read topics from |
| `SCHEMA_ROOT` | `s3a://warehouse/schemas/` | Where to write schemas |
| `MAX_FILES_FOR_INFERENCE` | `50` | How many newest files to sample |
| `SAMPLING_RATIO` | `0.2` | Spark sampling ratio for schema inference |
| `LOOP_INTERVAL_SEC` | `0` | Run once if 0, loop every N seconds otherwise |
| `COUNT_SAMPLE_RECORDS` | `false` | If true, counts records in sample |
| `DROP_ALL_NULL_FIELDS` | `false` | If true, drops fields that are always null |
| `JSON_MULTILINE` | `true` | Support multi‑line JSON |
| `JSON_MODE` | `PERMISSIVE` | Keep going if some records are malformed |
| `CORRUPT_RECORD_COL` | `_corrupt_record` | Column name for corrupt JSON |

## What the State File Means

The `_state/` file contains metadata to help debugging. Typical fields:

- `last_processed_mtime`: newest file time seen last run
- `sample_files`: which files were used to infer the schema
- `schema_hash`: hash of the schema JSON
- `schema_changed`: true if schema changed this run
- `last_success_ts`: time of last successful inference
- `last_attempt_ts`: time of last attempt
- `failure_reason`: error message if inference failed

This is not the schema itself.

## How to Check a Schema

Use Spark to read the schema folder:

```python
spark.read.text("s3a://warehouse/schemas/fortisiem.devices.raw/schema/") \
     .show(truncate=False)
```

The schema JSON will look like:

```
{"fields":[...],"type":"struct"}
```

## Why It Sometimes “Looks Like an Error”

You likely opened `_state/` instead of `schema/`. The state file is metadata, so it does not look like a schema. The schema always lives under `.../schema/`.

## Code Walkthrough (Plain English)

`_with_trailing_slash(path)`

- Ensures every root path ends with `/` so path joins are consistent.

`_fs_for(path)`

- Picks the correct Hadoop filesystem for that path (important for S3A).

`_exists(path)`

- Safe “does this exist?” check using the right filesystem.

`list_dirs(path)`

- Returns only real topic folders and skips hidden folders.

`list_files_recursive(path)`

- Returns all files under a topic with their modification time and size.

`read_state(topic)`

- Reads the topic’s last saved metadata from `_state/`.

`write_state(topic, state)`

- Writes a new metadata JSON into `_state/` as a text file.

`write_schema(topic, schema_json)`

- Writes the schema JSON into `schema/` as a text file.

`infer_schema(sample_files)`

- Reads JSON files in Spark, drops corrupt record column, and returns `df.schema.json()`.

`run_once()`

- The main job. It does the discovery, change detection, schema inference, and writes outputs.

`while True: ...`

- Repeats `run_once()` every `LOOP_INTERVAL_SEC` seconds if looping is enabled.

## Short Example (Behavior)

If you add a new file to:

```
s3a://bronze/topics/fortisiem.devices.raw/
```

Then on the next run:

1. The script sees a newer file.
2. It reads the newest N files.
3. It infers the schema.
4. It writes schema to:

```
s3a://warehouse/schemas/fortisiem.devices.raw/schema/
```

## Confirming It’s Working

If you can see a real schema JSON for both Rapid7 and FortiSIEM, the script is working correctly.

If you want me to validate live, tell me how you want to check (Spark, MinIO UI, or `mc`).
