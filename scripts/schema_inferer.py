import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("infer-schemas-all-sources").getOrCreate()

BRONZE_ROOT = "s3a://bronze/topics/"
SCHEMA_ROOT = "s3a://warehouse/schemas/"

JSON_READ_OPTS = {
    "multiLine": "true",
    "mode": "PERMISSIVE",
    # optional: "dropFieldIfAllNull": "true"  # can reduce noise if fields are always null
}

MAX_FILES_FOR_INFERENCE = 50   # rolling window size (tune this)
SAMPLING_RATIO = 0.2           # 1.0 safest, 0.1–0.3 common for speed  :contentReference[oaicite:5]{index=5}

# ----------------------------
# Hadoop FS helpers (MinIO/S3A)
# ----------------------------

jvm = spark._jvm
hconf = spark._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
Path = jvm.org.apache.hadoop.fs.Path

def exists(path: str) -> bool:
    return fs.exists(Path(path))

def list_dirs(path: str):
    statuses = fs.listStatus(Path(path))
    return [s.getPath().toString().rstrip("/") + "/" for s in statuses if s.isDirectory()]

def list_files_recursive(path: str):
    """
    Uses listFiles(recursive=true) which is recommended for object stores. :contentReference[oaicite:6]{index=6}
    Returns (file_path, modification_time_ms) list.
    """
    it = fs.listFiles(Path(path), True)
    out = []
    while it.hasNext():
        st = it.next()
        out.append((st.getPath().toString(), st.getModificationTime()))
    return out

def read_state(topic_name: str):
    state_dir = f"{SCHEMA_ROOT}{topic_name}/_state/"
    if not exists(state_dir):
        return {"last_processed_mtime": 0, "sample_files": []}

    # state_dir is a folder written by Spark; read its text content
    rows = spark.read.text(state_dir).collect()
    # pick first non-empty line
    for r in rows:
        if r["value"] and r["value"].strip():
            return json.loads(r["value"])
    return {"last_processed_mtime": 0, "sample_files": []}

def write_state(topic_name: str, state: dict):
    state_dir = f"{SCHEMA_ROOT}{topic_name}/_state/"
    spark.createDataFrame([(json.dumps(state),)], ["value"]) \
         .coalesce(1) \
         .write.mode("overwrite") \
         .text(state_dir)

def write_schema(topic_name: str, schema_json: str):
    # Write schema as a folder (not a single renamed file) to avoid S3A rename/copy issues. :contentReference[oaicite:7]{index=7}
    schema_dir = f"{SCHEMA_ROOT}{topic_name}/schema/"
    spark.createDataFrame([(schema_json,)], ["value"]) \
         .coalesce(1) \
         .write.mode("overwrite") \
         .text(schema_dir)

# ----------------------------
# Main loop
# ----------------------------
topic_dirs = list_dirs(BRONZE_ROOT)

for topic_path in topic_dirs:
    topic_name = topic_path.rstrip("/").split("/")[-1]

    try:
        state = read_state(topic_name)
        last_mtime = state.get("last_processed_mtime", 0)

        files = list_files_recursive(topic_path)
        if not files:
            print(f"[SKIP] {topic_name}: no files")
            continue

        newest_mtime = max(m for _, m in files)

        # If nothing changed since last run, skip
        if newest_mtime <= last_mtime:
            print(f"[SKIP] {topic_name}: no new files since last run")
            continue

        # Sort by mtime desc, take newest N files for inference
        files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
        sample_files = [p for p, _ in files_sorted[:MAX_FILES_FOR_INFERENCE]]

        df = (spark.read
              .options(**JSON_READ_OPTS)
              .option("samplingRatio", SAMPLING_RATIO)
              .json(sample_files))

        schema_json = df.schema.json()
        write_schema(topic_name, schema_json)

        # update state
        new_state = {
            "last_processed_mtime": newest_mtime,
            "sample_files": sample_files
        }
        write_state(topic_name, new_state)

        print(f"[OK] {topic_name}: inferred schema from {len(sample_files)} newest files")

    except Exception as e:
        print(f"[FAIL] {topic_name}: {e}")