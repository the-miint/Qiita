"""Unit tests for the read-ingest / staged-read runner resolvers.

Pure-function coverage (no DB / no orchestrator) for the bindings the
read-storage-from-masking split added:
  - `_resolve_sample_map` materializes the action_context roster to a Parquet.
  - `_resolve_staged_reads` binds `reads` from the durable staging copy, or falls
    back to the data-plane `export_read` DoAction (stubbed here) when that copy is
    gone, failing BAD_INPUT when neither source has the sample's reads.
  - `_workflow_needs_staged_reads` / `_workflow_declares_input` gate logic.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import duckdb
import pytest
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind

from qiita_control_plane.runner import (
    SAMPLE_MAP_BINDING,
    STAGED_READS_BINDING,
    _resolve_sample_map,
    _resolve_staged_reads,
    _workflow_declares_input,
    _workflow_needs_staged_reads,
)


def _step(**kw) -> SimpleNamespace:
    """A minimal WorkflowStep stand-in: inputs / optional_inputs / outputs."""
    return SimpleNamespace(
        inputs=kw.get("inputs", []),
        optional_inputs=kw.get("optional_inputs", []),
        outputs=kw.get("outputs", []),
    )


def test_resolve_sample_map_writes_parquet(tmp_path):
    """The action_context roster is written to sample_map.parquet with the
    (prep_sample_idx, pool_item_id) columns the ingest step reads."""
    action_context = {
        SAMPLE_MAP_BINDING: [
            {"prep_sample_idx": 81, "pool_item_id": "1"},
            {"prep_sample_idx": 82, "pool_item_id": "2"},
        ]
    }
    bound = asyncio.run(_resolve_sample_map(action_context, tmp_path / "ws"))
    out = bound[SAMPLE_MAP_BINDING]
    assert out.exists()
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, pool_item_id FROM read_parquet('{out}') "
            "ORDER BY prep_sample_idx"
        ).fetchall()
    assert rows == [(81, "1"), (82, "2")]


def test_resolve_sample_map_rejects_empty_roster(tmp_path):
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(_resolve_sample_map({SAMPLE_MAP_BINDING: []}, tmp_path / "ws"))
    assert exc.value.kind == FailureKind.BAD_INPUT


_EXPORT_READ = "qiita_control_plane.runner._do_action_export_read"


def _staged_kwargs(tmp_path):
    return {
        "data_plane_url": "grpc://unused",
        "hmac_secret": b"x" * 16,
        "workspace": tmp_path / "ticket" / "804",
    }


def test_resolve_staged_reads_binds_existing(tmp_path, monkeypatch):
    """When the durable staging copy exists, `reads` binds to it and the data
    plane is NOT called."""
    staging_root = tmp_path / "staging"
    reads = compute_reads_staging_path(staging_root, 42)
    reads.parent.mkdir(parents=True)
    reads.write_text("parquet-bytes")

    def _boom(_url, _token):
        raise AssertionError("export_read must not fire when the durable copy exists")

    monkeypatch.setattr(_EXPORT_READ, _boom)

    bound = asyncio.run(
        _resolve_staged_reads({"prep_sample_idx": 42}, staging_root, **_staged_kwargs(tmp_path))
    )
    assert bound[STAGED_READS_BINDING] == reads


def test_resolve_staged_reads_export_fallback_binds_workspace_parquet(tmp_path, monkeypatch):
    """Durable copy absent → the data-plane `export_read` action writes the
    per-ticket reads.parquet, which `reads` binds to."""
    workspace = tmp_path / "ticket" / "804"
    dest = workspace / "reads.parquet"

    def _fake_export(_url, _token):
        # The real data plane writes the file; the stub mirrors that + its shape.
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("parquet-bytes")
        return {"count": 5, "dest": str(dest)}

    monkeypatch.setattr(_EXPORT_READ, _fake_export)

    bound = asyncio.run(
        _resolve_staged_reads(
            {"prep_sample_idx": 42},
            tmp_path / "staging",
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 16,
            workspace=workspace,
        )
    )
    assert bound[STAGED_READS_BINDING] == dest
    assert dest.exists()


def test_resolve_staged_reads_empty_export_fails_must_be_ingested(tmp_path, monkeypatch):
    """Durable absent and the data plane returns 0 rows → BAD_INPUT 'must be
    ingested' — the no-stored-reads semantics are preserved."""
    monkeypatch.setattr(_EXPORT_READ, lambda _u, _t: {"count": 0, "dest": "x"})
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads(
                {"prep_sample_idx": 7}, tmp_path / "staging", **_staged_kwargs(tmp_path)
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "must be ingested" in exc.value.reason


def test_resolve_staged_reads_export_failure_is_bad_input(tmp_path, monkeypatch):
    """A Flight failure from the export action is wrapped as BAD_INPUT (it never
    escapes as an untyped exception)."""

    def _boom(_url, _token):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(_EXPORT_READ, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads(
                {"prep_sample_idx": 7}, tmp_path / "staging", **_staged_kwargs(tmp_path)
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "data plane" in exc.value.reason


def test_resolve_staged_reads_missing_file_is_bad_input(tmp_path, monkeypatch):
    """count>0 but no file landed at dest (a DP bug / full disk) → BAD_INPUT at
    submission, not a downstream FileNotFoundError."""
    workspace = tmp_path / "ticket" / "804"
    dest = workspace / "reads.parquet"
    # Reports reads but writes NO file.
    monkeypatch.setattr(_EXPORT_READ, lambda _u, _t: {"count": 5, "dest": str(dest)})
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads(
                {"prep_sample_idx": 7},
                tmp_path / "staging",
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=workspace,
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "wrote no file" in exc.value.reason


def test_workflow_needs_staged_reads_gate():
    """`reads` consumed but not produced → needs external staged binding (the
    read-mask workflow). Produced by a step → not external (bcl-convert)."""
    mask_steps = [_step(inputs=["reads", "qc_mask"], outputs=["read_mask"])]
    assert _workflow_needs_staged_reads(mask_steps) is True

    ingest_steps = [_step(inputs=["convert_dir"], outputs=["reads", "read_staging_dir"])]
    assert _workflow_needs_staged_reads(ingest_steps) is False

    no_reads = [_step(inputs=["bcl_input_dir"], outputs=["convert_dir"])]
    assert _workflow_needs_staged_reads(no_reads) is False


def test_workflow_declares_input_checks_optional_too():
    steps = [_step(inputs=["reads"], optional_inputs=["host_rype_path"])]
    assert _workflow_declares_input(steps, "host_rype_path") is True
    assert _workflow_declares_input(steps, "sample_map") is False
