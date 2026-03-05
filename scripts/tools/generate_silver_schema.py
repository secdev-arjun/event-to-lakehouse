import argparse
import json
import os
import re
import sys
from typing import Dict, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = f"{BASE_DIR}:{os.environ.get('PYTHONPATH', '')}"

from mapping.target import TARGET_FIELDS, PAYLOAD_HASH_COLUMNS  # noqa: E402
from tools.convert_spark_schema_to_avro import _to_avro, _to_json_schema  # noqa: E402


def _load_contract(path: str) -> dict:
    raw = ""
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore
            return yaml.safe_load(raw)
        except Exception as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"Failed to parse contract at {path}: {exc}")


def _split_top_level(args: str) -> Tuple[str, str]:
    depth = 0
    in_str = False
    str_char = ""
    escape = False
    for i, ch in enumerate(args):
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == str_char:
                in_str = False
        else:
            if ch in ("'", '"'):
                in_str = True
                str_char = ch
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                return args[:i], args[i + 1 :]
    return args, ""


def _extract_withcolumns(text: str) -> Dict[str, str]:
    mappings: Dict[str, str] = {}
    idx = 0
    while True:
        idx = text.find(".withColumn(", idx)
        if idx == -1:
            break

        open_paren = text.find("(", idx)
        if open_paren == -1:
            break

        i = open_paren + 1
        depth = 1
        in_str = False
        str_char = ""
        escape = False
        while i < len(text) and depth > 0:
            ch = text[i]
            if in_str:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == str_char:
                    in_str = False
            else:
                if ch in ("'", '"'):
                    in_str = True
                    str_char = ch
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
            i += 1

        args = text[open_paren + 1 : i - 1].strip()
        field_expr, value_expr = _split_top_level(args)
        field_expr = field_expr.strip()
        value_expr = value_expr.strip()

        m = re.match(r"""['"]([^'"]+)['"]""", field_expr)
        if m and value_expr:
            mappings[m.group(1)] = value_expr

        idx = i
    return mappings


def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _build_mapping(base_dir: str) -> Dict[str, Dict[str, str]]:
    mapping_dir = os.path.join(base_dir, "mapping", "sources")
    rapid7_path = os.path.join(mapping_dir, "rapid7.py")
    forti_path = os.path.join(mapping_dir, "fortisiem.py")
    sentinel_path = os.path.join(mapping_dir, "sentinel.py")

    mapping = {
        "rapid7": _extract_withcolumns(_read_file(rapid7_path)),
        "fortisiem": _extract_withcolumns(_read_file(forti_path)),
        "sentinelone": _extract_withcolumns(_read_file(sentinel_path)),
        "common_fields": {
            "schema_version": "lit(SCHEMA_VERSION)",
            "topic_name": "lit(topic_name)",
            "vendor_id": "vendor_id_col.cast('string')",
            "entity_key_str": "concat_ws('|', topic_name, vendor_id)",
            "entity_key_hash": "sha2(entity_key_str, 256)",
            "source_updated_at": "source_updated_at_col",
            "first_seen_at": "lit(None).cast('timestamp')",
            "last_seen_at": "lit(None).cast('timestamp')",
            "payload_hash": f"sha2(to_json(struct({', '.join(PAYLOAD_HASH_COLUMNS)})), 256)",
        },
        "notes": {
            "description": "Mappings extracted from normalize_* functions. "
                           "common_fields are applied via add_common_fields/add_payload_hash.",
        },
    }
    return mapping


def _build_ui_mapping(mapping: dict, contract: dict) -> dict:
    field_specs = contract.get("fields", {})
    sources = ["rapid7", "fortisiem", "sentinelone"]
    targets = []

    for field in TARGET_FIELDS:
        name = field.name
        contract_spec = field_specs.get(name, {})
        rule_list = contract_spec.get("rules", [])
        field_type = contract_spec.get("type") or field.dataType.simpleString()
        nullable = contract_spec.get("nullable", True)

        source_map = {}
        for src in sources:
            expr = mapping.get(src, {}).get(name)
            if expr is not None:
                source_map[src] = expr

        derived_expr = mapping.get("common_fields", {}).get(name)

        targets.append(
            {
                "name": name,
                "type": field_type,
                "nullable": nullable,
                "rules": rule_list,
                "sources": source_map,
                "derived": derived_expr,
            }
        )

    return {
        "target_table": contract.get("target_table"),
        "source_table": contract.get("source_table"),
        "sources": sources,
        "targets": targets,
        "notes": mapping.get("notes", {}),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate silver schema outputs")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--namespace", default="silver")
    parser.add_argument("--record-name", default="assets_silver_conformed")
    parser.add_argument("--base-name", default="assets_silver_conformed")
    parser.add_argument(
        "--contract-path",
        default="/opt/spark/scripts/bronze/contracts/assets_silver_contract.yaml",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    spark = SparkSession.builder.appName("silver-schema-export").getOrCreate()

    struct = StructType(TARGET_FIELDS)
    schema_json = struct.jsonValue()

    spark_json_path = os.path.join(args.output_dir, f"{args.base_name}.spark.json")
    avro_path = os.path.join(args.output_dir, f"{args.base_name}.avsc")
    jsonschema_path = os.path.join(args.output_dir, f"{args.base_name}.schema.json")
    mapping_path = os.path.join(args.output_dir, f"{args.base_name}.mapping.json")
    mapping_ui_path = os.path.join(args.output_dir, f"{args.base_name}.mapping.ui.json")

    with open(spark_json_path, "w", encoding="utf-8") as f:
        json.dump(schema_json, f, indent=2, sort_keys=True)

    json_schema = _to_json_schema(schema_json, title=args.record_name)
    with open(jsonschema_path, "w", encoding="utf-8") as f:
        json.dump(json_schema, f, indent=2, sort_keys=True)

    avro_text = _to_avro(spark, schema_json, args.record_name, args.namespace)
    with open(avro_path, "w", encoding="utf-8") as f:
        f.write(avro_text)

    mapping = _build_mapping(BASE_DIR)
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, sort_keys=True)

    contract = _load_contract(args.contract_path)
    mapping_ui = _build_ui_mapping(mapping, contract)
    with open(mapping_ui_path, "w", encoding="utf-8") as f:
        json.dump(mapping_ui, f, indent=2, sort_keys=True)

    print(f"Wrote Spark schema: {spark_json_path}")
    print(f"Wrote JSON Schema: {jsonschema_path}")
    print(f"Wrote Avro schema: {avro_path}")
    print(f"Wrote mapping: {mapping_path}")
    print(f"Wrote UI mapping: {mapping_ui_path}")


if __name__ == "__main__":
    main()
