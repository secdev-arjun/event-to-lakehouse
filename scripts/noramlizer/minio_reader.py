import json
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def filter_existing_paths(spark, paths, retries: int, sleep_sec: float):
    """
    Filter out paths that are not yet visible in object storage.
    Returns (existing_paths, missing_paths).
    """
    hconf = spark._jsc.hadoopConfiguration()
    Path = spark._jvm.org.apache.hadoop.fs.Path

    def _fs_for(path: str):
        return Path(path).getFileSystem(hconf)

    def _exists(path: str) -> bool:
        try:
            return _fs_for(path).exists(Path(path))
        except Exception:
            return False

    remaining = list(paths)
    missing = []

    for attempt in range(max(1, retries)):
        missing = [p for p in remaining if not _exists(p)]
        if not missing:
            return remaining, []
        if attempt < retries - 1:
            time.sleep(sleep_sec)
            remaining = [p for p in remaining if p not in missing]
            remaining = remaining + missing

    existing = [p for p in paths if p not in missing]
    return existing, missing


def load_latest_schema(spark, schema_root: str, topic_name: str):
    schema_dir = f"{schema_root}{topic_name}/schema/"
    try:
        rows = spark.read.text(schema_dir).collect()
    except Exception as e:
        print(f"[WARN] {topic_name}: unable to read schema at {schema_dir}: {e}")
        return None

    for r in rows:
        val = r["value"]
        if val and val.strip():
            try:
                return StructType.fromJson(json.loads(val))
            except Exception as e:
                print(f"[WARN] {topic_name}: invalid schema JSON in {schema_dir}: {e}")
                return None

    print(f"[WARN] {topic_name}: no schema content found in {schema_dir}")
    return None


def read_topic_files(spark, entries, schema, json_options, batch_ts):
    """
    entries: list of (file_path, event_time, ingest_ts)
    """
    meta_by_path = {p: (et, it) for p, et, it in entries}
    dfs = []
    for path in meta_by_path:
        event_time, ingest_ts = meta_by_path.get(path, (None, batch_ts))
        df_path = (
            spark.read.schema(schema).options(**json_options).json(path)
            .withColumn("file_path", F.lit(path))
            .withColumn("event_time", F.lit(event_time))
            .withColumn("ingest_ts", F.lit(ingest_ts if ingest_ts is not None else batch_ts))
        )
        dfs.append(df_path)
    if not dfs:
        return None
    df = dfs[0]
    for other in dfs[1:]:
        df = df.unionByName(other)
    return df
