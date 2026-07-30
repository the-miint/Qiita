"""Unit tests for qiita_control_plane.actions.context_validator.

No DB needed; pure JSON-Schema behavior. The route-level wiring
(submission-time validation against the action row's stored schema)
is exercised by the DB-bound tests in tests/routes/test_work_ticket.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

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


# ---------------------------------------------------------------------------
# Real-world schema guards — negative tests
# ---------------------------------------------------------------------------


def _load_workflow_schema(yaml_relpath):
    """Load a workflow YAML's context_schema for direct validation.

    Lets tests assert that the YAML `if/then` guards actually reject
    invalid submissions, without spinning up the full route + DB stack.
    """
    workflows_root = Path(__file__).parent.parent.parent / "workflows"
    schema_path = workflows_root / yaml_relpath
    with schema_path.open() as f:
        doc = yaml.safe_load(f)
    return doc["context_schema"]


def test_reference_add_shard_index_requires_genome_map_upload_idx():
    """`shard_index: true` without `genome_map_upload_idx` is rejected by the
    YAML `if/then` guard — plan-shards derives the per-shard feature set from
    qiita.feature_genome, which mint-features populates only when a genome map
    is supplied. Without this guard, a sharded reference runs the full ingest
    (hours) then fails at plan-shards with N=0."""
    schema = _load_workflow_schema("reference-add/1.0.0.yaml")
    context = {
        "fasta_upload_idx": 1,
        "taxonomy_upload_idx": 2,
        "shard_index": True,
        # genome_map_upload_idx deliberately absent
    }
    errs = validate_context(schema, context)
    assert errs, "expected validation to reject shard_index without genome_map"
    assert any("genome_map_upload_idx" in e["message"] for e in errs)


def test_local_reference_add_shard_index_requires_genome_map_path():
    """`local-reference-add` carries the same guard: `shard_index: true`
    without `genome_map_path` is rejected by the YAML `if/then`."""
    schema = _load_workflow_schema("local-reference-add/1.0.0.yaml")
    context = {
        "fasta_manifest_path": "/shared/fastas/manifest.txt",
        "taxonomy_path": "/shared/tax/tax.parquet",
        "shard_index": True,
        # genome_map_path deliberately absent
    }
    errs = validate_context(schema, context)
    assert errs, "expected validation to reject shard_index without genome_map"
    assert any("genome_map_path" in e["message"] for e in errs)
