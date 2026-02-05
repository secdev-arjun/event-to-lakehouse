# Schema Inferer Explained (friendly)

This document explains `scripts/schema_inferer.py` in simple, step‑by‑step language.

## Big Picture

Imagine a library with many shelves. Each shelf is a **topic** under:

```
s3a://bronze/topics/<topic_name>/
```

Inside each shelf are JSON files. The script’s job is to:

1. Look at each shelf (topic).
2. Read some of the newest files.
3. Ask Spark “what is the shape of these JSON objects?”
4. Save that **shape (schema)** here:

```
s3a://warehouse/schemas/<topic_name>/schema/
```

It also saves a **state file** here so it knows what it did last time:

```
s3a://warehouse/schemas/<topic_name>/_state/
```

## Inputs and Outputs

Inputs

- Bronze root: `s3a://bronze/topics/`
- JSON files inside each topic folder

Outputs per topic

- Schema folder: `s3a://warehouse/schemas/<topic>/schema/`
- State folder: `s3a://warehouse/schemas/<topic>/_state/`

The schema is a JSON string stored inside a `part-*.txt` file in the schema folder.

## Key Ideas in the Code

### 1. Configuration (easy knobs)

These let you control behavior without changing the code:

- `BRONZE_ROOT` and `SCHEMA_ROOT`: where to read and write.
- `MAX_FILES_FOR_INFERENCE`: how many newest files to use.
- `SAMPLING_RATIO`: how much of each file to sample while inferring.
- `LOOP_INTERVAL_SEC`: how often to repeat (0 = run once).

### 2. Hadoop FS helpers (S3A safe)

S3A sometimes throws “Wrong FS” errors if you use the wrong filesystem object. The code fixes that by doing:

- “Use the filesystem that matches this path”

That’s why you see `_fs_for(path)` and `_exists(path)`.

### 3. Topic discovery

The script lists all folders under the bronze root and treats each folder as a topic. It ignores hidden folders like `_temporary` or `.spark-staging`.

### 4. File discovery and change check

For each topic:

1. List all files under the topic (recursive).
2. Find the newest modification time (`newest_mtime`).
3. Compare it to the last time we processed this topic (`last_processed_mtime`).
4. If nothing is newer, skip this topic.

This prevents re‑inferring the schema on every run.

### 5. Sampling

Instead of reading every file, it takes the newest N files:

- N = `MAX_FILES_FOR_INFERENCE`

This keeps inference fast, but still fresh.

### 6. Schema inference

Spark reads those files and infers the JSON structure. The script drops Spark’s `_corrupt_record` column if it exists so the schema is cleaner.

Then it converts the schema to JSON:

```
 schema_json = df.schema.json()
```

### 7. Schema change detection

The schema JSON is hashed with SHA‑256. If the hash is different from last time, the schema is written again. If it’s the same, the schema file is left as‑is.

This avoids pointless rewrites and gives you a clean “drift detection” signal.

### 8. State writing

The state file stores useful debug info such as:

- `last_processed_mtime`
- `sample_files`
- `schema_hash`
- `schema_changed`
- `last_success_ts` and `last_attempt_ts`
- `failure_reason`

That’s why the state file you saw looks like metadata instead of schema.

### 9. Looping

At the bottom of the file:

```
while True:
    run_once()
    if LOOP_INTERVAL_SEC <= 0:
        break
    time.sleep(LOOP_INTERVAL_SEC)
```

If you set `LOOP_INTERVAL_SEC=60`, it will run every minute.

## Function‑by‑Function (Plain English)

`_with_trailing_slash(path)`

- Makes sure a path ends with `/` so folder paths are consistent.

`_fs_for(path)`

- Picks the correct Hadoop filesystem for the given path (important for S3A).

`_exists(path)`

- Checks if a path exists using the right filesystem.

`list_dirs(path)`

- Returns all subfolders that are not hidden.

`list_files_recursive(path)`

- Returns a list of files with size and modified time.

`read_state(topic_name)`

- Reads the last stored state for the topic (if it exists).

`write_state(topic_name, state)`

- Writes state into `_state/` as a text file.

`write_schema(topic_name, schema_json)`

- Writes the schema JSON into `schema/` as a text file.

`infer_schema(sample_files)`

- Reads JSON files with Spark and returns schema JSON plus small metadata.

`run_once()`

- The main job. It loops topics, detects changes, infers schema, writes schema and state.

## Why your FortiSIEM looked “wrong”

You were looking at the **state file**, not the schema file.

Correct locations:

- Schema: `s3a://warehouse/schemas/fortisiem.devices.raw/schema/`
- State: `s3a://warehouse/schemas/fortisiem.devices.raw/_state/`

The schema is the JSON with `fields: [...]`.

## “Does it work?”

Based on your results:

- You saw a real schema for `rapid7.assets.raw`.
- You now see a real schema for `fortisiem.devices.raw`.
- That means the script is doing what it’s supposed to.

If you want me to validate live, tell me how you want to check (Spark, mc, or MinIO UI) and I’ll give you exact commands.
