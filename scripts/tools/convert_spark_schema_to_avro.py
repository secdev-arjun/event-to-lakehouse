import argparse
import json
import os
import re
from typing import Dict, List, Tuple, Union

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from py4j.java_gateway import JavaPackage


def _sanitize_record_name(name: str) -> str:
    # Avro record name must match [A-Za-z_][A-Za-z0-9_]*
    base = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not base:
        base = "record"
    if re.match(r"^[0-9]", base):
        base = f"r_{base}"
    return base


def _sanitize_field_name(name: str) -> str:
    base = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not base:
        base = "field"
    if re.match(r"^[0-9]", base):
        base = f"f_{base}"
    return base


def _sanitize_struct_fields(fields: List[dict], name_map: Dict[str, str]) -> List[dict]:
    seen: Dict[str, int] = {}
    out = []
    for field in fields:
        orig = field.get("name", "")
        sanitized = _sanitize_field_name(orig)
        if sanitized in seen:
            seen[sanitized] += 1
            sanitized = f"{sanitized}_{seen[sanitized]}"
        else:
            seen[sanitized] = 0
        if orig and orig != sanitized:
            name_map[orig] = sanitized
        field = dict(field)
        field["name"] = sanitized
        field["type"] = _sanitize_schema_type(field.get("type"), name_map)
        out.append(field)
    return out


def _sanitize_schema_type(schema_type, name_map: Dict[str, str]):
    if isinstance(schema_type, dict):
        t = schema_type.get("type")
        if t == "struct":
            schema_type = dict(schema_type)
            schema_type["fields"] = _sanitize_struct_fields(schema_type.get("fields", []), name_map)
            return schema_type
        if t == "array":
            schema_type = dict(schema_type)
            schema_type["elementType"] = _sanitize_schema_type(schema_type.get("elementType"), name_map)
            return schema_type
    return schema_type


def _sanitize_schema_json(schema_json: dict) -> Tuple[dict, Dict[str, str]]:
    name_map: Dict[str, str] = {}
    schema_json = dict(schema_json)
    if schema_json.get("type") == "struct":
        schema_json["fields"] = _sanitize_struct_fields(schema_json.get("fields", []), name_map)
    return schema_json, name_map


def _parse_decimal_type(type_str: str) -> Union[None, Dict[str, Union[str, int, float]]]:
    match = re.match(r"decimal\((\d+),(\d+)\)", type_str)
    if not match:
        return None
    # JSON Schema has no native decimal; use number.
    return {"type": "number"}


def _parse_char_type(type_str: str) -> Union[None, Dict[str, Union[str, int]]]:
    match = re.match(r"(var)?char\((\d+)\)", type_str)
    if not match:
        return None
    max_len = int(match.group(2))
    return {"type": "string", "maxLength": max_len}


def _json_schema_from_type_string(type_str: str) -> Dict:
    decimal_schema = _parse_decimal_type(type_str)
    if decimal_schema:
        return decimal_schema

    char_schema = _parse_char_type(type_str)
    if char_schema:
        return char_schema

    type_map = {
        "string": {"type": "string"},
        "boolean": {"type": "boolean"},
        "byte": {"type": "integer"},
        "short": {"type": "integer"},
        "integer": {"type": "integer"},
        "long": {"type": "integer"},
        "float": {"type": "number"},
        "double": {"type": "number"},
        "binary": {"type": "string"},
        "date": {"type": "string", "format": "date"},
        "timestamp": {"type": "string", "format": "date-time"},
        "timestamp_ntz": {"type": "string", "format": "date-time"},
    }
    return type_map.get(type_str, {"type": "string"})


def _add_nullable(schema: Dict) -> Dict:
    schema = dict(schema)
    schema_type = schema.get("type")
    if schema_type is None:
        return {"anyOf": [{"type": "null"}, schema]}
    if isinstance(schema_type, list):
        if "null" not in schema_type:
            schema_type = list(schema_type) + ["null"]
        schema["type"] = schema_type
        return schema
    schema["type"] = ["null", schema_type]
    return schema


def _json_schema_from_type(schema_type, nullable: bool) -> Dict:
    if isinstance(schema_type, dict):
        t = schema_type.get("type")
        if t == "struct":
            properties: Dict[str, Dict] = {}
            required: List[str] = []
            for field in schema_type.get("fields", []):
                field_name = field.get("name", "")
                field_schema = _json_schema_from_type(field.get("type"), field.get("nullable", True))
                properties[field_name] = field_schema
                if field.get("nullable", True) is False:
                    required.append(field_name)
            obj_schema: Dict[str, Union[str, Dict, List]] = {
                "type": "object",
                "properties": properties,
            }
            if required:
                obj_schema["required"] = required
            return _add_nullable(obj_schema) if nullable else obj_schema
        if t == "array":
            element_schema = _json_schema_from_type(
                schema_type.get("elementType"), schema_type.get("containsNull", True)
            )
            arr_schema: Dict[str, Union[str, Dict]] = {"type": "array", "items": element_schema}
            return _add_nullable(arr_schema) if nullable else arr_schema
        if t == "map":
            value_schema = _json_schema_from_type(
                schema_type.get("valueType"), schema_type.get("valueContainsNull", True)
            )
            map_schema: Dict[str, Union[str, Dict]] = {
                "type": "object",
                "additionalProperties": value_schema,
            }
            return _add_nullable(map_schema) if nullable else map_schema
        if isinstance(t, str):
            base_schema = _json_schema_from_type_string(t)
            return _add_nullable(base_schema) if nullable else base_schema
    if isinstance(schema_type, str):
        base_schema = _json_schema_from_type_string(schema_type)
        return _add_nullable(base_schema) if nullable else base_schema
    return _add_nullable({"type": "string"}) if nullable else {"type": "string"}


def _to_json_schema(schema_json: dict, title: str) -> Dict:
    base_schema = _json_schema_from_type(schema_json, False)
    base_schema = dict(base_schema)
    base_schema["$schema"] = "http://json-schema.org/draft-07/schema#"
    base_schema["title"] = title
    return base_schema


def _list_json_files(input_dir: str) -> List[str]:
    return sorted(
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.lower().endswith(".json")
    )


def _to_avro(spark: SparkSession, schema_json: dict, name: str, namespace: str) -> str:
    struct = StructType.fromJson(schema_json)
    jvm = spark._jvm
    schema_converters = jvm.org.apache.spark.sql.avro.SchemaConverters
    if isinstance(schema_converters, JavaPackage):
        raise RuntimeError(
            "spark-avro is not on the classpath. Re-run with "
            "--packages org.apache.spark:spark-avro_2.12:3.5.5"
        )
    jvm_schema = jvm.org.apache.spark.sql.types.DataType.fromJson(struct.json())
    avro_schema = schema_converters.toAvroType(jvm_schema, False, name, namespace)
    return avro_schema.toString()


def main():
    parser = argparse.ArgumentParser(description="Convert Spark StructType JSON to Avro schema")
    parser.add_argument("--input-dir", default="/opt/spark/scripts/schemas/spark")
    parser.add_argument("--output-dir", default="/opt/spark/scripts/schemas/avro")
    parser.add_argument("--json-schema-dir", default="/opt/spark/scripts/schemas/jsonschema")
    parser.add_argument("--namespace", default="bronze")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("spark-schema-to-avro").getOrCreate()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.json_schema_dir, exist_ok=True)

    files = _list_json_files(args.input_dir)
    if not files:
        raise SystemExit(f"No .json schema files found in {args.input_dir}")

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            raw_schema_json = json.load(f)

        schema_json, field_map = _sanitize_schema_json(raw_schema_json)

        base = os.path.splitext(os.path.basename(path))[0]
        record_name = _sanitize_record_name(base)
        avro_text = _to_avro(spark, schema_json, record_name, args.namespace)

        out_path = os.path.join(args.output_dir, f"{record_name}.avsc")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(avro_text)

        if field_map:
            map_path = os.path.join(args.output_dir, f"{record_name}.field_map.json")
            with open(map_path, "w", encoding="utf-8") as f:
                json.dump(field_map, f, indent=2, sort_keys=True)
        print(f"Converted {path} -> {out_path}")

        json_schema = _to_json_schema(raw_schema_json, base)
        json_path = os.path.join(args.json_schema_dir, f"{base}.schema.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_schema, f, indent=2, sort_keys=True)
        print(f"Converted {path} -> {json_path}")

    spark.stop()


if __name__ == "__main__":
    main()
