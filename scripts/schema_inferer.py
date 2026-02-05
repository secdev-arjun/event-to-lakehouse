import json
import os
import time
import hashlib
from datetime import datetime, timezone

from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------
# Spark session (docker spark-submit provides Iceberg + s3a configs)
# ------------------------------------------------------------------------------

spark = SparkSession.builder.appName("infer-schemas-all-sources").getOrCreate()

# ------------------------------------------------------------------------------
# Config (override via env vars)
# ------------------------------------------------------------------------------

def _with_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"

BRONZE_ROOT = _with_trailing_slash(os.getenv("BRONZE_ROOT", "s3a://bronze/topics/"))
SCHEMA_ROOT = _with_trailing_slash(os.getenv("SCHEMA_ROOT", "s3a://warehouse/schemas/"))

MAX_FILES_FOR_INFERENCE = int(os.getenv("MAX_FILES_FOR_INFERENCE", "50"))
SAMPLING_RATIO = float(os.getenv("SAMPLING_RATIO", "0.2"))
COUNT_SAMPLE_RECORDS = os.getenv("COUNT_SAMPLE_RECORDS", "false").lower() == "true"
DROP_ALL_NULL_FIELDS = os.getenv("DROP_ALL_NULL_FIELDS", "false").lower() == "true"

LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "60"))

CORRUPT_RECORD_COL = os.getenv("CORRUPT_RECORD_COL", "_corrupt_record")
JSON_READ_OPTS = {
    "multiLine": os.getenv("JSON_MULTILINE", "true"),
    "mode": os.getenv("JSON_MODE", "PERMISSIVE"),
    "columnNameOfCorruptRecord": CORRUPT_RECORD_COL,
}
if DROP_ALL_NULL_FIELDS:
    JSON_READ_OPTS["dropFieldIfAllNull"] = "true"

# ------------------------------------------------------------------------------
# Hadoop FS helpers (MinIO/S3A)
# ------------------------------------------------------------------------------

jvm = spark._jvm
hconf = spark._jsc.hadoopConfiguration()
Path = jvm.org.apache.hadoop.fs.Path

def _fs_for(path: str):
    # Ensure we use the filesystem for the path scheme (avoids Wrong FS errors).
    return Path(path).getFileSystem(hconf)


def _exists(path: str) -> bool:
    fs = _fs_for(path)
    return fs.exists(Path(path))


def _is_hidden_name(name: str) -> bool:
    return name.startswith("_") or name.startswith(".")


def list_dirs(path: str):
    if not _exists(path):
        return []
    fs = _fs_for(path)
    statuses = fs.listStatus(Path(path))
    out = []
    for s in statuses:
        if not s.isDirectory():
            continue
        p = s.getPath().toString().rstrip("/")
        name = p.split("/")[-1]
        if _is_hidden_name(name):
            continue
        out.append(p + "/")
    return out


def list_files_recursive(path: str):
    """
    Uses listFiles(recursive=true) which is recommended for object stores.
    Returns list of (file_path, modification_time_ms, size_bytes).
    """
    if not _exists(path):
        return []
    fs = _fs_for(path)
    it = fs.listFiles(Path(path), True)
    out = []
    while it.hasNext():
        st = it.next()
        p = st.getPath().toString()
        name = p.rstrip("/").split("/")[-1]
        if _is_hidden_name(name):
            continue
        out.append((p, st.getModificationTime(), st.getLen()))
    return out

# ------------------------------------------------------------------------------
# State helpers
# ------------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_schema(schema_json: str) -> str:
    return hashlib.sha256(schema_json.encode("utf-8")).hexdigest()


def read_state(topic_name: str) -> dict:
    state_dir = f"{SCHEMA_ROOT}{topic_name}/_state/"
    if not _exists(state_dir):
        return {}

    # state_dir is a folder written by Spark; read its text content
    rows = spark.read.text(state_dir).collect()
    for r in rows:
        val = r["value"]
        if not val or not val.strip():
            continue
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            continue
    return {}


def write_state(topic_name: str, state: dict):
    state_dir = f"{SCHEMA_ROOT}{topic_name}/_state/"
    payload = json.dumps(state, sort_keys=True)
    (
        spark.createDataFrame([(payload,)], ["value"])
        .coalesce(1)
        .write.mode("overwrite")
        .text(state_dir)
    )


def write_schema(topic_name: str, schema_json: str):
    # Write schema as a folder (not a single renamed file) to avoid S3A rename/copy issues.
    schema_dir = f"{SCHEMA_ROOT}{topic_name}/schema/"
    (
        spark.createDataFrame([(schema_json,)], ["value"])
        .coalesce(1)
        .write.mode("overwrite")
        .text(schema_dir)
    )

# ------------------------------------------------------------------------------
# Inference
# ------------------------------------------------------------------------------

def infer_schema(sample_files):
    reader = spark.read.options(**JSON_READ_OPTS)
    if SAMPLING_RATIO < 1.0:
        reader = reader.option("samplingRatio", SAMPLING_RATIO)

    df_raw = reader.json(sample_files)
    if CORRUPT_RECORD_COL in df_raw.columns:
        df = df_raw.drop(CORRUPT_RECORD_COL)
    else:
        df = df_raw

    if not df.schema.fields:
        return None, {"sample_record_count": 0, "sample_column_count": 0}

    record_count = None
    if COUNT_SAMPLE_RECORDS:
        record_count = df.count()

    meta = {
        "sample_record_count": record_count,
        "sample_column_count": len(df.schema.fields),
    }
    return df.schema.json(), meta

# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------

def run_once():
    if not _exists(BRONZE_ROOT):
        print(f"[SKIP] Bronze root not found: {BRONZE_ROOT}")
        return

    topic_dirs = list_dirs(BRONZE_ROOT)
    if not topic_dirs:
        print(f"[SKIP] No topics found under {BRONZE_ROOT}")
        return

    for topic_path in topic_dirs:
        topic_name = topic_path.rstrip("/").split("/")[-1]
        state = {}

        try:
            state = read_state(topic_name)
            last_mtime = int(state.get("last_processed_mtime", 0) or 0)

            files = list_files_recursive(topic_path)
            if not files:
                print(f"[SKIP] {topic_name}: no files")
                continue

            newest_mtime = max(m for _, m, _ in files)

            # If nothing changed since last run, skip
            if newest_mtime <= last_mtime:
                print(f"[SKIP] {topic_name}: no new files since last run")
                continue

            files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
            if MAX_FILES_FOR_INFERENCE <= 0:
                sample = files_sorted
            else:
                sample = files_sorted[:MAX_FILES_FOR_INFERENCE]

            sample_files = [p for p, _, _ in sample]
            if not sample_files:
                print(f"[SKIP] {topic_name}: no usable files for inference")
                continue

            sample_bytes = sum(sz for _, _, sz in sample)

            schema_json, meta = infer_schema(sample_files)
            if not schema_json:
                raise RuntimeError("empty schema (no readable records)")

            schema_hash = _hash_schema(schema_json)
            prev_hash = state.get("schema_hash")
            schema_changed = prev_hash is None or prev_hash != schema_hash

            if schema_changed:
                write_schema(topic_name, schema_json)

            new_state = {
                "topic": topic_name,
                "last_processed_mtime": newest_mtime,
                "sample_files": sample_files,
                "sample_file_count": len(sample_files),
                "sample_bytes": sample_bytes,
                "schema_hash": schema_hash,
                "previous_schema_hash": prev_hash,
                "schema_changed": schema_changed,
                "last_success_ts": _now_iso(),
                "last_attempt_ts": _now_iso(),
                "failure_reason": None,
            }
            new_state.update(meta)
            write_state(topic_name, new_state)

            if schema_changed:
                print(f"[OK] {topic_name}: schema updated from {len(sample_files)} newest files")
            else:
                print(f"[OK] {topic_name}: schema unchanged; state refreshed")

        except Exception as e:
            failure_state = {
                "topic": topic_name,
                "last_processed_mtime": state.get("last_processed_mtime", 0),
                "sample_files": state.get("sample_files", []),
                "schema_hash": state.get("schema_hash"),
                "last_success_ts": state.get("last_success_ts"),
                "last_attempt_ts": _now_iso(),
                "failure_reason": str(e),
            }
            try:
                write_state(topic_name, failure_state)
            except Exception:
                pass
            print(f"[FAIL] {topic_name}: {e}")


if __name__ == "__main__":
    while True:
        run_once()
        if LOOP_INTERVAL_SEC <= 0:
            break
        time.sleep(LOOP_INTERVAL_SEC)