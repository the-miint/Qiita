"""JSON-Schema validation for `work_ticket.action_context`.

Each `qiita.action` row carries a `context_schema` (JSON Schema fragment,
default `{}` = accept any object). At submission time the route handler
passes the action's stored schema and the request's `action_context` to
`validate_context` — every validation error becomes one entry in the 422
response body so the client can fix everything in one round-trip rather
than one error at a time.

`check_schema` runs at action-sync time and refuses to upsert a row
whose own `context_schema` is malformed. Catching that at deploy time
prevents a runtime 500 on the first submission against a broken action.

Pinned to Draft 2020-12 — the latest stable JSON Schema dialect when
this module was written. Action authors should write schemas that
validate against that dialect.
"""

from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

# Re-exported so callers don't need to import jsonschema directly when
# they only want to catch a malformed-schema error.
__all__ = ["check_schema", "validate_context", "SchemaError"]


def check_schema(schema: dict[str, Any]) -> None:
    """Raise `jsonschema.exceptions.SchemaError` if `schema` is not a
    well-formed Draft-2020-12 JSON Schema. Callers should let the
    exception propagate so the operator sees the schema-level message
    rather than a generic 500."""
    Draft202012Validator.check_schema(schema)


def validate_context(schema: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate `context` against `schema`. Returns a list of error
    dicts; empty list means valid.

    Each error dict has:

    - `path`: the JSON Pointer to the offending value in `context`
      (e.g. `"/sample_count"`, or `""` for an error at the root).
    - `message`: the human-readable reason — pulled directly from
      jsonschema's error.
    - `schema_path`: the JSON Pointer to the failing rule inside
      `schema` (e.g. `"/properties/sample_count/type"`). Useful for
      a client that wants to highlight the broken rule.

    `validator_value`: the value the schema asserted. Helpful for
    client-side error prettifying ("must be one of [a, b, c]").

    `Draft202012Validator(schema).iter_errors(context)` is the cheapest
    multi-error API jsonschema offers; constructing the validator here
    means we re-parse the schema on every submission. That's fine —
    submission isn't a hot path, and per-action schema caching can be
    added later if profiling says otherwise.
    """
    validator = Draft202012Validator(schema)
    return [_format_error(err) for err in validator.iter_errors(context)]


def _format_error(err) -> dict[str, Any]:
    return {
        "path": _pointer(err.absolute_path),
        "message": err.message,
        "schema_path": _pointer(err.absolute_schema_path),
        "validator_value": err.validator_value,
    }


def _pointer(parts) -> str:
    """Render a deque of path components as a JSON Pointer string.
    Empty deque → `""` (root). Components are stringified so integer
    array indices come out as `"/0"`, `"/1"`, etc."""
    if not parts:
        return ""
    return "/" + "/".join(str(p) for p in parts)
