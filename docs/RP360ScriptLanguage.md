# RP360 Script Language Reference (AI-Focused)

This document explains the custom runtime language available to RP360 scripts, including:

- runtime objects injected into each script (`input`, `p`, `CI`, `headers`, `token`)
- CI CRUD/search operations (`CI.get`, `CI.post`, `CI.patch`, `CI.delete`, `CI.search`)
- input/output contract enforcement (`input_format`, `output_format`)
- flow input/output mapping mini-language (`manual` / `flow` / `both`, mapped edges)

It is based on current source behavior in:

- `Django/scripts/helpers.py`
- `Django/scripts/uds_client.py`
- `Django/scripts/tasks.py`
- `Django/scripts/models.py`
- `Django/CI/views.py`
- `Django/CI/serializers.py`
- `Django/CI/schema.py`

---

## 1. Execution Model

Each script run executes Python code inside the configured container using:

- command: `python -u -c "<generated_code>"`
- stdin: JSON payload (`input_json`)
- stdout: must contain valid JSON output
- stderr: treated as logs

The runner prepends helper code before your script content. That helper defines the custom RP360 script language primitives.

Important:

- The script `language` model field can be `python` or `react`, but runtime execution is Python (`python -c ...`).
- If your script exits non-zero, run status becomes `FAILED`.
- If stdout is not valid JSON, run status becomes `FAILED`.

---

## 2. Injected Runtime Objects

The runner injects these globals into your script.

## 2.1 `input` (read-only mapping)

`input` wraps incoming `input_json` and behaves like a mapping object:

- dict-style access: `input["key"]`
- attribute-style access: `input.key`
- iteration supported: `for k in input: ...`
- conversion back to plain dict: `input.to_dict()`

Rules:

- `input` is read-only (assignment/deletion raises `AttributeError`)
- only top-level/nested dict values are wrapped as `Input`; lists stay normal lists
- missing attributes raise `AttributeError("'input' has no attribute '...')`

Example:

```python
name = input.name
threshold = input["threshold"]
payload = input.to_dict()
```

## 2.2 `p` helper (`p.log`, `p.output`, `p.error`)

`p` is a read-only helper object.

- `p.log(*args)`: writes to stderr (captured as run logs)
- `p.output(obj)`: writes JSON to stdout, flushes, then exits process with code `0`
- `p.error(*args)`: writes to stderr, exits process with code `1`

Important behavior:

- `p.output(...)` ends script execution immediately (`sys.exit(0)`)
- stdout must be JSON only; avoid normal `print(...)` to stdout
- `p.error(...)` marks run as failed

## 2.3 `token` and `headers`

- `token`: DRF token string for run user (or `None` if no user context)
- `headers`:
  - `{"Authorization": f"Token {token}"}` when token exists
  - `{}` otherwise

Use `headers=headers` in `CI.*` calls when endpoint permissions require auth.

---

## 3. `CI` Client API (CRUD + Search)

The custom client class is imported as `from uds_client import CI`.

It communicates over the Unix socket to RP360 API endpoints rooted at:

- base: `/api/CIs/`

The helper is static/frozen:

- cannot instantiate: `CI()`
- cannot subclass/monkey-patch

All methods return parsed JSON (`response.json()`), or raise `HTTPError` for non-2xx responses.

## 3.1 Method Reference

## `CI.get`

```python
CI.get(id=<int>, params=None, headers=None, timeout=None)
```

- HTTP: `GET /api/CIs/<id>/`
- typical use: read one CI by ID

## `CI.post` (Create)

```python
CI.post(payload=<dict>, headers=None, timeout=None)
```

- HTTP: `POST /api/CIs/`
- payload shape:

```json
{
  "type": "<schema title>",
  "data": { "...": "..." }
}
```

## `CI.patch` (Update)

```python
CI.patch(id=<int>, payload=<dict>, headers=None, timeout=None)
```

- HTTP: `PATCH /api/CIs/<id>/`
- partial update payload typically:

```json
{
  "data": { "...": "..." }
}
```

`type` is read-only on patch.

## `CI.delete`

```python
CI.delete(id=<int>, headers=None, timeout=None, params=None)
```

- HTTP: `DELETE /api/CIs/<id>/`
- returns:
  - JSON body if server sends one
  - otherwise fallback `{"status": <http_status_code>}` (commonly 204)

## `CI.search` (Query by CI type)

```python
CI.search(ci_type=<str>, payload=None, params=None, headers=None, timeout=None)
```

- HTTP: `POST /api/CIs/Type/<ci_type>/`
- supports query body fields:
  - `searchMode`: `"field"` or `"global"`
  - `filterText`: query expression or full-text string
  - `properties`: list or comma-separated string
  - `page`, `page_size`

Response is paginated and typically includes:

- `total_count`
- `current_page_count`
- `current_page`
- `total_pages`
- `next_uri`, `next_page`
- `previous_uri`, `previous_page`
- `results`: list of `{id, data}`

Projection behavior:

- if `properties` is omitted/empty, `data` is full CI JSON
- if `properties` is provided, response is projected/flattened
- missing projected keys are omitted (no `includeMissing` option in `CI.search` API)

## 3.2 CRUD Mapping (conceptual)

If you think in classical CRUD naming:

- Create -> `CI.post`
- Read -> `CI.get` / `CI.search`
- Update -> `CI.patch`
- Delete -> `CI.delete`

There is no `CI.create`/`CI.update` method currently.

## 3.3 Error and Timeout Behavior

- every request calls `response.raise_for_status()`
- unhandled HTTP errors fail the script run
- timeout parameter is accepted by method signatures, but current implementation always uses `SYSAPI_TIMEOUT` env value (default `10s`)

Safe pattern:

```python
try:
    item = CI.get(id=123, headers=headers)
except Exception as e:
    p.error(f"CI.get failed: {e}")
```

---

## 4. CI Payload Contracts (What CRUD Must Respect)

Server-side CI validation enforces schema and relationship rules.

## 4.1 Create (`POST /api/CIs/`)

Expected body:

- `type`: existing `CISchema.title`
- `data`: object matching schema definition

Validation highlights:

- unknown keys in `data` are rejected
- schema validation uses JSON Schema (Draft 2020-12)
- uniqueness constraints (`x-uniqueAcross`) enforced
- relationship constraints (`x-relatedTo`) enforced

## 4.2 Patch (`PATCH /api/CIs/<id>/`)

Behavior:

- partial merge into existing data
- type cannot be changed
- merged result is revalidated using same schema/relationship checks

Edge behavior to know:

- empty dict/list values in incoming `data` are removed before validation merge

---

## 5. Search Language via `CI.search(... searchMode="field")`

When `searchMode="field"`, `filterText` is parsed by RP360 field grammar.

Supported operators:

- `=`
- `>`
- `>=`
- `<`
- `<=`
- `REGEX`
- `STARTSWITH`
- `ENDSWITH`
- `CONTAINS`
- `IN`

Logical operators:

- `NOT`
- `AND`
- `OR`

Literals:

- quoted strings (`"..."` or `'...'`)
- numbers
- booleans (`true`, `false`)
- `null`

Examples:

```text
name = "server-01"
version >= 10
NOT status = "retired"
owner.email ENDSWITH "@example.com"
tags IN "prod"
```

Notes:

- `CONTAINS` is implemented using case-insensitive regex matching.
- `STARTSWITH` / `ENDSWITH` use case-sensitive lookups.
- boolean values are only supported with `=` or `IN`.
- For complete query guide and examples, see `Documentation/Query.md`.

---

## 6. Script Input Contract (`Script.input_format`)

`Script.input_format` is a Cerberus schema for run input normalization/validation.

At run time:

- input payload is validated with `Validator(input_format, allow_unknown=False)`
- defaults can be injected by Cerberus during normalization
- invalid input causes run creation/validation failure

Allowed rule keys in `input_format` entries:

- `type`
- `required`
- `default`
- `meta`
- `regex`
- `allowed`

Example:

```json
{
  "ci_id": { "type": "integer", "required": true },
  "new_owner": { "type": "string", "required": true, "regex": "^[a-z0-9_.-]+$" },
  "dry_run": { "type": "boolean", "required": false, "default": true }
}
```

---

## 7. Script Output Contract (`Script.output_format`)

Script stdout must be valid JSON. Then optional output schema validation runs.

If `output_format` is configured:

- must be a Cerberus schema object
- validated with `Validator(schema, allow_unknown=False, require_all=True)`
- unknown output fields are rejected
- missing required fields fail validation

Allowed rule keys in `output_format` entries:

- `type`
- `required`
- `meta`

Example:

```json
{
  "status": { "type": "string", "required": true },
  "updated_id": { "type": "integer", "required": true },
  "details": { "type": "dict", "required": false }
}
```

If output is invalid, run status becomes `FAILED` with validation error text.

---

## 8. Flow Input/Output Mapping Mini-Language

When scripts run inside flows, node-level input/output behavior adds another contract.

## 8.1 `FlowNode.input_format` values

`FlowNode.input_format` maps input keys to one of:

- `"manual"`: value must come from node static `input`
- `"flow"`: value must come from upstream flow payload
- `"both"`: prefer flow payload, fallback to node static `input`

Validation rules:

- first node cannot request `"flow"` or `"both"`
- `"manual"` keys must exist in node static `input`

## 8.2 Edge key mapping (`source_key` / `target_key`)

Mapped edges transfer selected keys:

- `source_key` must exist in source node `output_format`
- `target_key` must exist in target node `input_format`
- target key must be configured as `"flow"`
- both keys must be set together (or both blank for general edge)

General edges:

- copy full source output object into downstream payload (merge)
- cannot coexist with mapped edges between same source/target pair
- current engine applies the first general incoming edge for a node when multiple are present

---

## 9. AI Script Authoring Rules (Recommended)

Use these conventions when generating RP360 scripts:

1. Always finish with exactly one `p.output({...})` on success.
2. Never print normal text to stdout; use `p.log(...)` for diagnostics.
3. Wrap all `CI.*` calls in `try/except` and fail with `p.error(...)`.
4. Pass `headers=headers` to `CI.*` unless endpoint is intentionally public.
5. Return stable, schema-compliant JSON keys for `output_format`.
6. Keep output JSON small and deterministic.
7. Treat `input` as immutable; copy using `input.to_dict()` if needed.

---

## 10. Reference Templates

## 10.1 Minimal safe script template

```python
try:
    data = input.to_dict()
    p.log("starting", data)

    # business logic...
    result = {"status": "ok"}

    p.output(result)
except Exception as e:
    p.error(f"script failed: {e}")
```

## 10.2 CRUD template with `CI`

```python
try:
    # Create
    created = CI.post(
        payload={"type": input.ci_type, "data": input.data},
        headers=headers,
    )
    ci_id = created["id"]

    # Read
    fetched = CI.get(id=ci_id, headers=headers)

    # Update
    updated = CI.patch(
        id=ci_id,
        payload={"data": {"status": "active"}},
        headers=headers,
    )

    # Optional search
    search_result = CI.search(
        ci_type=input.ci_type,
        payload={
            "searchMode": "field",
            "filterText": f"id = {ci_id}",
            "properties": ["id", "status"],
            "page": 1,
            "page_size": 20,
        },
        headers=headers,
    )

    # Optional delete (if requested)
    deleted = None
    if getattr(input, "delete_after", False):
        deleted = CI.delete(id=ci_id, headers=headers)

    p.output({
        "status": "success",
        "created_id": ci_id,
        "fetched": fetched,
        "updated": updated,
        "search": search_result,
        "deleted": deleted,
    })
except Exception as e:
    p.error(f"CRUD flow failed: {e}")
```

---

## 11. Common Failure Modes

- Missing/invalid auth:
  - `CI.*` raises 401/403 HTTP error.
- Non-JSON stdout:
  - run fails with `Script did not emit valid JSON on stdout.`
- Output schema mismatch:
  - run fails with Cerberus error object string.
- Writing debug text to stdout before `p.output`:
  - breaks JSON parse.
- Using unsupported `CI` method names:
  - only `get/post/search/patch/delete` exist.

---

## 12. Quick Checklist for AI-Generated RP360 Scripts

- input contract defined (`input_format`) and respected
- exactly one success `p.output(...)`
- all logs via `p.log(...)`
- all API calls use `headers=headers`
- all API calls guarded with `try/except`
- output keys/types match `output_format`
