"""Unit tests for qiita_control_plane.actions.context_validator.

No DB needed; pure JSON-Schema behavior. The route-level wiring
(submission-time validation against the action row's stored schema)
is exercised by the DB-bound tests in tests/routes/test_work_ticket.py.
"""

from __future__ import annotations

import pytest

from qiita_control_plane.actions.context_validator import (
    SchemaError,
    check_schema,
    validate_context,
)


# ---------------------------------------------------------------------------
# validate_context
# ---------------------------------------------------------------------------


def test_empty_schema_accepts_anything():
    """Standard JSON-Schema: `{}` is the always-valid schema. An action
    that doesn't declare a context_schema must accept any object."""
    assert validate_context({}, {}) == []
    assert validate_context({}, {"sample_count": 42}) == []
    assert validate_context({}, {"nested": {"a": [1, 2, 3]}}) == []


def test_valid_context_returns_empty_list():
    schema = {
        "type": "object",
        "properties": {"n": {"type": "integer"}},
        "required": ["n"],
    }
    assert validate_context(schema, {"n": 42}) == []


def test_missing_required_field_reports_error():
    schema = {
        "type": "object",
        "properties": {"n": {"type": "integer"}},
        "required": ["n"],
    }
    errs = validate_context(schema, {})
    assert len(errs) == 1
    assert errs[0]["path"] == ""  # error is at the root (the missing key)
    assert "'n'" in errs[0]["message"]
    assert errs[0]["validator_value"] == ["n"]


def test_type_mismatch_reports_error_with_path():
    schema = {"type": "object", "properties": {"n": {"type": "integer"}}}
    errs = validate_context(schema, {"n": "not-an-int"})
    assert len(errs) == 1
    assert errs[0]["path"] == "/n"
    assert "integer" in errs[0]["message"]


def test_multiple_errors_returned_in_one_call():
    """The whole point of using `iter_errors` over `validate`: clients
    fix everything in one round-trip. Two invalid fields should yield
    two errors, not one."""
    schema = {
        "type": "object",
        "properties": {
            "a": {"type": "integer"},
            "b": {"type": "string"},
        },
        "required": ["a", "b"],
    }
    errs = validate_context(schema, {"a": "bad", "b": 999})
    assert len(errs) == 2
    paths = {e["path"] for e in errs}
    assert paths == {"/a", "/b"}


def test_nested_path_renders_as_json_pointer():
    schema = {
        "type": "object",
        "properties": {
            "outer": {
                "type": "object",
                "properties": {"inner": {"type": "integer"}},
            }
        },
    }
    errs = validate_context(schema, {"outer": {"inner": "wrong"}})
    assert len(errs) == 1
    assert errs[0]["path"] == "/outer/inner"


def test_array_index_renders_in_pointer():
    schema = {"type": "array", "items": {"type": "integer"}}
    errs = validate_context(schema, [1, 2, "three", 4])
    assert len(errs) == 1
    assert errs[0]["path"] == "/2"


def test_additional_properties_false_rejects_extras():
    """An action wanting 'no context allowed' declares additionalProperties:
    false explicitly. Verify that pattern works."""
    schema = {"type": "object", "additionalProperties": False}
    assert validate_context(schema, {}) == []
    errs = validate_context(schema, {"surprise": "value"})
    assert len(errs) == 1


def test_schema_path_points_at_failing_rule():
    schema = {"type": "object", "properties": {"n": {"type": "integer"}}}
    errs = validate_context(schema, {"n": "x"})
    assert errs[0]["schema_path"] == "/properties/n/type"


# ---------------------------------------------------------------------------
# check_schema
# ---------------------------------------------------------------------------


def test_check_schema_accepts_well_formed_schema():
    # Empty schema is valid.
    check_schema({})
    # Realistic schema is valid.
    check_schema({"type": "object", "properties": {"n": {"type": "integer"}}})


def test_check_schema_rejects_bad_type_string():
    """`type: this-is-not-a-real-type` is not a valid JSON-Schema type
    keyword and should be rejected at sync time, not at submission."""
    with pytest.raises(SchemaError):
        check_schema({"type": "this-is-not-a-real-type"})


def test_check_schema_rejects_bad_property_name():
    """`required` must be an array of strings, not an object."""
    with pytest.raises(SchemaError):
        check_schema({"type": "object", "required": {"x": True}})
