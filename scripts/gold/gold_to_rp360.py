import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"


# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

GOLD_TABLE = os.getenv("GOLD_TABLE", "iceberg.gold.assets_current")

RP360_BASE_URL = os.getenv("RP360_BASE_URL", "http://172.16.232.51:4000/")
RP360_USERNAME = os.getenv("RP360_USERNAME", "admin")
RP360_PASSWORD = os.getenv("RP360_PASSWORD", "admin")
RP360_TIMEOUT_SECONDS = int(os.getenv("RP360_TIMEOUT_SECONDS", "30"))

RP360_TYPE_TITLE = os.getenv("RP360_TYPE_TITLE", "gold_asset").strip().lower()
RP360_SCHEMA_TITLE = os.getenv("RP360_SCHEMA_TITLE", "gold_asset")
RP360_SCHEMA_VERBOSE_NAME = os.getenv("RP360_SCHEMA_VERBOSE_NAME", "Gold Asset")
RP360_SCHEMA_DESCRIPTION = os.getenv(
    "RP360_SCHEMA_DESCRIPTION",
    "Canonical gold asset generated from iceberg.gold.assets_current.",
)
RP360_ENSURE_TYPE_SCHEMA = os.getenv("RP360_ENSURE_TYPE_SCHEMA", "true").lower() == "true"

SYNC_STATE_TABLE = os.getenv("RP360_SYNC_STATE_TABLE", "iceberg.gold.rp360_sync_state")
SYNC_ERROR_TABLE = os.getenv("RP360_SYNC_ERROR_TABLE", "iceberg.gold.rp360_sync_errors")

MAX_ROWS_PER_RUN = int(os.getenv("MAX_ROWS_PER_RUN", "0"))  # 0 = no limit
FAIL_ON_ROW_ERROR = os.getenv("FAIL_ON_ROW_ERROR", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

REQUIRED_FIELDS = ["gold_asset_id", "gold_payload_hash"]
UNIQUE_CONSTRAINTS = [["gold_asset_id"]]
FORCED_EXCLUDED_FIELDS = {"raw_json", "raw_payload"}
EXCLUDED_FIELDS = FORCED_EXCLUDED_FIELDS.union(
    {f.strip() for f in os.getenv("RP360_EXCLUDED_FIELDS", "").split(",") if f.strip()}
)


# ------------------------------------------------------------------------------
# Spark session
# ------------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Gold -> RP360 CI Sync")
    .config("spark.executorEnv.PYTHONPATH", os.environ.get("PYTHONPATH", ""))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")


# ------------------------------------------------------------------------------
# Schema helpers
# ------------------------------------------------------------------------------


def _type_meta(dt: DataType) -> Dict[str, Any]:
    if isinstance(dt, (StringType, DateType, TimestampType)):
        return {"x-fieldType": "text", "x-ui-widget": "text"}
    if isinstance(dt, (IntegerType, LongType, ShortType, FloatType, DoubleType, DecimalType)):
        return {"x-fieldType": "number", "x-ui-widget": "number"}
    if isinstance(dt, BooleanType):
        return {"x-fieldType": "checkbox", "x-ui-widget": "checkbox"}
    if isinstance(dt, ArrayType):
        return {"x-ui-widget": "multiselect"}
    return {"x-fieldType": "text", "x-ui-widget": "text"}


def _json_type_for_spark_type(dt: DataType) -> Dict[str, Any]:
    if isinstance(dt, StringType):
        return {"type": "string"}
    if isinstance(dt, (IntegerType, LongType, ShortType)):
        return {"type": "integer"}
    if isinstance(dt, (FloatType, DoubleType, DecimalType)):
        return {"type": "number"}
    if isinstance(dt, BooleanType):
        return {"type": "boolean"}
    if isinstance(dt, TimestampType):
        return {"type": "string", "format": "date-time"}
    if isinstance(dt, DateType):
        return {"type": "string", "format": "date"}
    if isinstance(dt, ArrayType):
        item_schema = _json_type_for_spark_type(dt.elementType)
        return {"type": "array", "items": item_schema}
    # RP360 CI schemas are object-focused and usually flat for this use case.
    # For unsupported complex nested types, preserve as JSON string.
    return {"type": "string"}


def _human_title(col_name: str) -> str:
    return col_name.replace("_", " ").strip().title()


def build_rp360_schema(df_schema: StructType) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    cols = [f.name for f in df_schema.fields if f.name not in EXCLUDED_FIELDS]

    for field in df_schema.fields:
        if field.name in EXCLUDED_FIELDS:
            continue
        schema_piece = _json_type_for_spark_type(field.dataType)
        schema_piece["title"] = _human_title(field.name)
        schema_piece.update(_type_meta(field.dataType))
        properties[field.name] = schema_piece

    required = [c for c in REQUIRED_FIELDS if c in cols]
    return {
        "type": "object",
        "title": RP360_SCHEMA_TITLE,
        "description": RP360_SCHEMA_DESCRIPTION,
        "verbose_name": RP360_SCHEMA_VERBOSE_NAME,
        "required": required,
        "properties": properties,
        "unique_constraints": UNIQUE_CONSTRAINTS if "gold_asset_id" in cols else [],
    }


# ------------------------------------------------------------------------------
# JSON value helpers
# ------------------------------------------------------------------------------


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    return value


def build_ci_data(row_dict: Dict[str, Any], schema_props: Dict[str, Any], required: List[str]) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for key in schema_props.keys():
        if key in FORCED_EXCLUDED_FIELDS:
            continue
        val = _to_jsonable(row_dict.get(key))
        expected_type = schema_props.get(key, {}).get("type")
        if expected_type == "string" and isinstance(val, (dict, list)):
            val = json.dumps(val, default=str, sort_keys=True)
        if val is not None:
            data[key] = val

    for key in required:
        if key not in data:
            raise ValueError(f"Required field '{key}' is null/missing in source row")
    return data


def _stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


# ------------------------------------------------------------------------------
# RP360 API client
# ------------------------------------------------------------------------------


class RP360Client:
    def __init__(self, base_url: str, username: str, password: str, timeout_seconds: int = 30):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.access_token: Optional[str] = None

    def _url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        return f"{self.base_url}/{path_or_url.lstrip('/')}"

    def _request(
        self,
        method: str,
        path_or_url: str,
        payload: Optional[Dict[str, Any]] = None,
        auth: bool = True,
    ) -> Tuple[int, Any]:
        url = self._url(path_or_url)
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")

        req = urlrequest.Request(url=url, method=method.upper(), data=body)
        req.add_header("Accept", "application/json")
        if payload is not None:
            req.add_header("Content-Type", "application/json")
        if auth and self.access_token:
            req.add_header("Authorization", f"Bearer {self.access_token}")

        try:
            with urlrequest.urlopen(req, timeout=self.timeout_seconds) as resp:
                text = resp.read().decode("utf-8")
                data = _safe_json_loads(text)
                return int(resp.status), data
        except urlerror.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            data = _safe_json_loads(text)
            return int(exc.code), data
        except urlerror.URLError as exc:
            raise RuntimeError(f"Network error calling RP360: {exc}") from exc

    def authenticate(self) -> None:
        status, data = self._request(
            "POST",
            "/api/auth/token/",
            {"username": self.username, "password": self.password},
            auth=False,
        )
        if status < 200 or status >= 300:
            raise RuntimeError(f"Auth failed ({status}): {data}")
        if not isinstance(data, dict):
            raise RuntimeError(f"Auth response was not JSON object: {data}")
        token = data.get("access") or data.get("token")
        if not token:
            raise RuntimeError(f"Auth succeeded but no access token found: {data}")
        self.access_token = str(token)

    def list_types(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        next_url: Optional[str] = "/api/types/"
        while next_url:
            status, data = self._request("GET", next_url)
            if status < 200 or status >= 300:
                raise RuntimeError(f"Failed to list types ({status}): {data}")

            if isinstance(data, dict) and isinstance(data.get("results"), list):
                out.extend([x for x in data["results"] if isinstance(x, dict)])
                nxt = data.get("next")
                next_url = str(nxt) if nxt else None
            elif isinstance(data, list):
                out.extend([x for x in data if isinstance(x, dict)])
                next_url = None
            else:
                # Some APIs return a single object on non-paginated list edge cases.
                if isinstance(data, dict):
                    out.append(data)
                next_url = None
        return out

    def ensure_type(self, title: str, schema: Dict[str, Any], ensure_schema: bool = True) -> Optional[str]:
        type_title = title.strip().lower()
        types = self.list_types()
        existing = next((t for t in types if str(t.get("title", "")).lower() == type_title), None)

        if existing is None:
            payload = {"title": type_title, "schema": schema}
            status, data = self._request("POST", "/api/types/", payload)
            if status < 200 or status >= 300:
                raise RuntimeError(f"Failed to create type '{type_title}' ({status}): {data}")
            if isinstance(data, dict):
                return str(data.get("id")) if data.get("id") is not None else None
            return None

        type_id = existing.get("id")
        if not ensure_schema or type_id is None:
            return str(type_id) if type_id is not None else None

        existing_schema = existing.get("schema")
        if _stable_json(existing_schema) == _stable_json(schema):
            return str(type_id)

        # Preferred schema endpoint.
        patch_status, patch_data = self._request("PATCH", f"/api/types/schema/{type_id}/", schema)
        if 200 <= patch_status < 300:
            return str(type_id)

        # Fallback endpoint style.
        patch_status, patch_data = self._request(
            "PATCH",
            f"/api/types/schema/{type_id}/",
            {"schema": schema},
        )
        if 200 <= patch_status < 300:
            return str(type_id)

        # Final fallback to type detail patch.
        patch_status, patch_data = self._request(
            "PATCH",
            f"/api/types/{type_id}/",
            {"schema": schema},
        )
        if 200 <= patch_status < 300:
            return str(type_id)

        raise RuntimeError(
            f"Failed to update schema for type '{type_title}' ({patch_status}): {patch_data}"
        )

    def search_ci_by_asset_id(self, type_title: str, gold_asset_id: str) -> Optional[str]:
        escaped = gold_asset_id.replace("\\", "\\\\").replace('"', '\\"')
        payload = {
            "searchMode": "field",
            "filterText": f'gold_asset_id = "{escaped}"',
            "properties": ["id", "gold_asset_id", "gold_payload_hash"],
        }
        encoded_type = urlparse.quote(type_title, safe="")
        status, data = self._request("POST", f"/api/CIs/Type/{encoded_type}/", payload)
        if status < 200 or status >= 300:
            return None
        for item in _extract_items(data):
            ci_id = item.get("id")
            if ci_id is not None:
                return str(ci_id)
        return None

    def create_ci(self, type_title: str, data: Dict[str, Any]) -> Tuple[bool, Optional[str], str]:
        payload = {"type": type_title, "data": data}
        status, resp = self._request("POST", "/api/CIs/", payload)
        if 200 <= status < 300:
            ci_id = str(resp.get("id")) if isinstance(resp, dict) and resp.get("id") is not None else None
            return True, ci_id, ""
        return False, None, f"create failed ({status}): {resp}"

    def patch_ci(self, ci_id: str, data: Dict[str, Any]) -> Tuple[bool, str]:
        payload = {"data": data}
        status, resp = self._request("PATCH", f"/api/CIs/{ci_id}/", payload)
        if 200 <= status < 300:
            return True, ""
        return False, f"patch failed ({status}): {resp}"


def _safe_json_loads(text: str) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _extract_items(payload: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return

    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            for item in payload["results"]:
                if isinstance(item, dict):
                    yield item
            return
        # Handle single object responses.
        if "id" in payload:
            yield payload


# ------------------------------------------------------------------------------
# Sync table helpers
# ------------------------------------------------------------------------------


STATE_FIELDS = StructType(
    [
        StructField("gold_asset_id", StringType(), False),
        StructField("gold_payload_hash", StringType(), True),
        StructField("rp360_ci_id", StringType(), True),
        StructField("synced_at", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("last_error", StringType(), True),
    ]
)

ERROR_FIELDS = StructType(
    [
        StructField("gold_asset_id", StringType(), True),
        StructField("error", StringType(), True),
        StructField("payload_json", StringType(), True),
        StructField("attempted_at", TimestampType(), True),
    ]
)


def ensure_table_with_schema(table_name: str, schema: StructType) -> None:
    if not spark.catalog.tableExists(table_name):
        spark.createDataFrame([], schema=schema).writeTo(table_name).create()
        return

    existing_fields = {f.name: f.dataType for f in spark.table(table_name).schema.fields}
    missing = [f for f in schema.fields if f.name not in existing_fields]
    if missing:
        cols_sql = ", ".join([f"{f.name} {f.dataType.simpleString()}" for f in missing])
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({cols_sql})")


def dedupe_by_gold_asset_id(df):
    if "gold_asset_id" not in df.columns:
        return df

    order_exprs = []
    if "_rp360_ci_id" in df.columns:
        order_exprs.append(
            F.when(F.col("_rp360_ci_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)).desc()
        )
    for col_name in ("source_updated_at", "last_seen_at", "ingest_ts", "first_seen_at"):
        if col_name in df.columns:
            order_exprs.append(F.col(col_name).desc_nulls_last())
    if not order_exprs:
        order_exprs = [F.lit(1)]

    w = Window.partitionBy("gold_asset_id").orderBy(*order_exprs)
    return (
        df.filter(F.col("gold_asset_id").isNotNull())
        .withColumn("_rn_asset", F.row_number().over(w))
        .filter(F.col("_rn_asset") == 1)
        .drop("_rn_asset")
    )


def compute_delta_df(gold_table: str, state_table: str):
    if not spark.catalog.tableExists(gold_table):
        raise ValueError(f"Gold table not found: {gold_table}")

    gold_df = spark.table(gold_table)
    needed = {"gold_asset_id", "gold_payload_hash"}
    missing_needed = [c for c in needed if c not in gold_df.columns]
    if missing_needed:
        raise ValueError(
            f"Gold table missing required columns for sync: {missing_needed}"
        )

    if not spark.catalog.tableExists(state_table):
        return dedupe_by_gold_asset_id(
            gold_df.withColumn("_rp360_ci_id", F.lit(None).cast("string"))
        )

    state_df = (
        spark.table(state_table)
        .select("gold_asset_id", "gold_payload_hash", "rp360_ci_id", "status")
        .alias("s")
    )
    joined = gold_df.alias("g").join(state_df, on="gold_asset_id", how="left")

    delta = joined.filter(
        F.col("s.gold_payload_hash").isNull()
        | (F.col("g.gold_payload_hash") != F.col("s.gold_payload_hash"))
        | (F.col("s.status").isNull())
        | (F.col("s.status") != F.lit("success"))
    )
    delta = delta.select("g.*", F.col("s.rp360_ci_id").alias("_rp360_ci_id"))
    return dedupe_by_gold_asset_id(delta)


def merge_state_updates(rows: List[Tuple[str, str, Optional[str], datetime, str, str]]) -> None:
    if not rows:
        return
    updates_df = spark.createDataFrame(
        rows,
        [
            "gold_asset_id",
            "gold_payload_hash",
            "rp360_ci_id",
            "synced_at",
            "status",
            "last_error",
        ],
    )
    updates_df = updates_df.filter(F.col("gold_asset_id").isNotNull())
    status_rank = F.when(F.col("status") == F.lit("success"), F.lit(1)).otherwise(F.lit(0))
    w = Window.partitionBy("gold_asset_id").orderBy(
        status_rank.desc(),
        F.col("synced_at").desc_nulls_last(),
    )
    updates_df = (
        updates_df
        .withColumn("_rn_state", F.row_number().over(w))
        .filter(F.col("_rn_state") == 1)
        .drop("_rn_state")
    )
    updates_df.createOrReplaceTempView("rp360_sync_state_updates")
    spark.sql(
        f"""
        MERGE INTO {SYNC_STATE_TABLE} t
        USING rp360_sync_state_updates s
        ON t.gold_asset_id = s.gold_asset_id
        WHEN MATCHED THEN UPDATE SET
            gold_payload_hash = s.gold_payload_hash,
            rp360_ci_id = s.rp360_ci_id,
            synced_at = s.synced_at,
            status = s.status,
            last_error = s.last_error
        WHEN NOT MATCHED THEN INSERT (
            gold_asset_id,
            gold_payload_hash,
            rp360_ci_id,
            synced_at,
            status,
            last_error
        ) VALUES (
            s.gold_asset_id,
            s.gold_payload_hash,
            s.rp360_ci_id,
            s.synced_at,
            s.status,
            s.last_error
        )
        """
    )


def append_error_rows(rows: List[Tuple[Optional[str], str, str, datetime]]) -> None:
    if not rows:
        return
    err_df = spark.createDataFrame(rows, ["gold_asset_id", "error", "payload_json", "attempted_at"])
    err_df.writeTo(SYNC_ERROR_TABLE).append()


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------


def _assert_required_config() -> None:
    if not RP360_BASE_URL:
        raise ValueError("RP360_BASE_URL is required")
    if not RP360_TYPE_TITLE:
        raise ValueError("RP360_TYPE_TITLE is required")
    parsed = urlparse.urlparse(RP360_BASE_URL)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"RP360_BASE_URL is not a valid URL: {RP360_BASE_URL}")


def main() -> None:
    _assert_required_config()
    ensure_table_with_schema(SYNC_STATE_TABLE, STATE_FIELDS)
    ensure_table_with_schema(SYNC_ERROR_TABLE, ERROR_FIELDS)

    gold_df = spark.table(GOLD_TABLE)
    rp360_schema = build_rp360_schema(gold_df.schema)

    client = RP360Client(
        base_url=RP360_BASE_URL,
        username=RP360_USERNAME,
        password=RP360_PASSWORD,
        timeout_seconds=RP360_TIMEOUT_SECONDS,
    )
    client.authenticate()
    type_id = client.ensure_type(RP360_TYPE_TITLE, rp360_schema, ensure_schema=RP360_ENSURE_TYPE_SCHEMA)
    print(f"[INFO] RP360 type ready: title={RP360_TYPE_TITLE}, id={type_id}")

    delta_df = compute_delta_df(GOLD_TABLE, SYNC_STATE_TABLE)
    if MAX_ROWS_PER_RUN > 0:
        delta_df = delta_df.limit(MAX_ROWS_PER_RUN)

    schema_props = rp360_schema.get("properties", {})
    required_fields = rp360_schema.get("required", [])

    success_updates: List[Tuple[str, str, Optional[str], datetime, str, str]] = []
    error_rows: List[Tuple[Optional[str], str, str, datetime]] = []

    total = 0
    success = 0
    failed = 0

    for row in delta_df.toLocalIterator():
        total += 1
        now_ts = datetime.now(timezone.utc)
        row_dict = row.asDict(recursive=True)
        hinted_ci_id = row_dict.pop("_rp360_ci_id", None)
        row_dict.pop("raw_json", None)
        row_dict.pop("raw_payload", None)

        gold_asset_id = str(row_dict.get("gold_asset_id")) if row_dict.get("gold_asset_id") is not None else None
        gold_payload_hash = str(row_dict.get("gold_payload_hash")) if row_dict.get("gold_payload_hash") is not None else ""

        try:
            ci_data = build_ci_data(row_dict, schema_props, required_fields)
            payload = {"type": RP360_TYPE_TITLE, "data": ci_data}

            if DRY_RUN:
                success_updates.append(
                    (
                        gold_asset_id or "",
                        gold_payload_hash,
                        str(hinted_ci_id) if hinted_ci_id is not None else None,
                        now_ts,
                        "success",
                        "",
                    )
                )
                success += 1
                continue

            ci_id = str(hinted_ci_id) if hinted_ci_id is not None else None
            if ci_id:
                ok, msg = client.patch_ci(ci_id, ci_data)
                if ok:
                    success_updates.append((gold_asset_id or "", gold_payload_hash, ci_id, now_ts, "success", ""))
                    success += 1
                    continue
                ci_id = None

            if not ci_id and gold_asset_id:
                ci_id = client.search_ci_by_asset_id(RP360_TYPE_TITLE, gold_asset_id)
                if ci_id:
                    ok, msg = client.patch_ci(ci_id, ci_data)
                    if ok:
                        success_updates.append(
                            (gold_asset_id or "", gold_payload_hash, ci_id, now_ts, "success", "")
                        )
                        success += 1
                        continue

            ok, created_ci_id, err_msg = client.create_ci(RP360_TYPE_TITLE, ci_data)
            if ok:
                success_updates.append(
                    (gold_asset_id or "", gold_payload_hash, created_ci_id, now_ts, "success", "")
                )
                success += 1
                continue

            failed += 1
            err_text = err_msg or "unknown create error"
            error_rows.append((gold_asset_id, err_text, json.dumps(payload, default=str), now_ts))
            success_updates.append(
                (
                    gold_asset_id or "",
                    gold_payload_hash,
                    ci_id,
                    now_ts,
                    "failed",
                    err_text[:2000],
                )
            )
            if FAIL_ON_ROW_ERROR:
                raise RuntimeError(err_text)

        except Exception as exc:
            failed += 1
            err_text = str(exc)
            fallback_payload = {
                "type": RP360_TYPE_TITLE,
                "data": {
                    k: _to_jsonable(v)
                    for k, v in row_dict.items()
                    if k not in {"_rp360_ci_id", "raw_json", "raw_payload"}
                },
            }
            error_rows.append((gold_asset_id, err_text, json.dumps(fallback_payload, default=str), now_ts))
            success_updates.append(
                (
                    gold_asset_id or "",
                    gold_payload_hash,
                    str(hinted_ci_id) if hinted_ci_id is not None else None,
                    now_ts,
                    "failed",
                    err_text[:2000],
                )
            )
            if FAIL_ON_ROW_ERROR:
                raise

    merge_state_updates(success_updates)
    append_error_rows(error_rows)

    print(
        "[INFO] RP360 sync completed "
        f"(dry_run={DRY_RUN}) total={total}, success={success}, failed={failed}, "
        f"state_table={SYNC_STATE_TABLE}, error_table={SYNC_ERROR_TABLE}"
    )

    if failed > 0 and FAIL_ON_ROW_ERROR:
        raise RuntimeError(f"RP360 sync failed for {failed} rows")


if __name__ == "__main__":
    main()
