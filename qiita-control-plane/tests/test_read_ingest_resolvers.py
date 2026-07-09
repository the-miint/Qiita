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
    _resolve_staged_masked_reads_block,
    _resolve_staged_reads,
    _resolve_staged_reads_block,
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


# --- block-export resolver (_resolve_staged_reads_block) -------------------

_EXPORT_READ_BLOCK = "qiita_control_plane.runner._do_action_export_read_block"


def _decode_token_payload(token: bytes) -> dict:
    """Extract the canonical-JSON payload from a signed action token. Wire
    format: <1B version><4B big-endian len><payload><32B hmac><8B expiry>."""
    import json
    import struct

    (plen,) = struct.unpack(">I", token[1:5])
    return json.loads(token[5 : 5 + plen])


_BLOCK_MEMBERS = [
    {"prep_sample_idx": 101, "sequence_idx_start": 100, "sequence_idx_stop": 109},
    {"prep_sample_idx": 103, "sequence_idx_start": 300, "sequence_idx_stop": 309},
]


def test_resolve_staged_reads_block_binds_workspace_parquet_and_signs_members(
    tmp_path, monkeypatch
):
    """A block always sources from the DP `export_read_block` action (a block may
    hold a partial sample, so no per-sample durable copy serves it). `reads` binds
    to the written file, and the signed token carries the members verbatim."""
    workspace = tmp_path / "ticket" / "900"
    dest = workspace / "reads.parquet"
    captured: dict = {}

    def _fake_export(_url, token):
        captured["payload"] = _decode_token_payload(token)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("parquet-bytes")
        return {"count": 5, "dest": str(dest)}

    monkeypatch.setattr(_EXPORT_READ_BLOCK, _fake_export)

    bound = asyncio.run(
        _resolve_staged_reads_block(
            _BLOCK_MEMBERS,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 16,
            workspace=workspace,
        )
    )
    assert bound[STAGED_READS_BINDING] == dest
    assert dest.exists()

    payload = captured["payload"]
    assert payload["action"] == "export_read_block"
    assert payload["dest"] == str(dest)
    assert payload["members"] == _BLOCK_MEMBERS


def test_resolve_staged_reads_block_empty_members_is_bad_input(tmp_path, monkeypatch):
    """An empty block is a planning bug → BAD_INPUT, and the DP is never called."""

    def _boom(_url, _token):
        raise AssertionError("export_read_block must not fire for an empty block")

    monkeypatch.setattr(_EXPORT_READ_BLOCK, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads_block(
                [],
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=tmp_path / "ws",
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT


def test_resolve_staged_reads_block_zero_count_is_bad_input(tmp_path, monkeypatch):
    """The block selected zero reads (its members' ranges match nothing) →
    BAD_INPUT: a planning bug, since blocks are tiled from sequence_range bounds."""
    monkeypatch.setattr(_EXPORT_READ_BLOCK, lambda _u, _t: {"count": 0, "dest": "x"})
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads_block(
                _BLOCK_MEMBERS,
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=tmp_path / "ws",
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT


def test_resolve_staged_reads_block_export_failure_is_bad_input(tmp_path, monkeypatch):
    """A Flight failure is wrapped as BAD_INPUT, never an untyped exception."""

    def _boom(_url, _token):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(_EXPORT_READ_BLOCK, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads_block(
                _BLOCK_MEMBERS,
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=tmp_path / "ws",
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "data plane" in exc.value.reason


def test_resolve_staged_reads_block_malformed_member_is_bad_input(tmp_path, monkeypatch):
    """A member missing a key (a planner bug) fails BAD_INPUT — not an untyped
    KeyError that would strand the ticket in PROCESSING. The DP is never called."""

    def _boom(_url, _token):
        raise AssertionError("export_read_block must not fire for a malformed member")

    monkeypatch.setattr(_EXPORT_READ_BLOCK, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads_block(
                [{"prep_sample_idx": 101, "sequence_idx_start": 100}],  # missing stop
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=tmp_path / "ws",
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT


def test_resolve_staged_reads_block_missing_file_is_bad_input(tmp_path, monkeypatch):
    """count>0 but no file landed → BAD_INPUT at submission, not a downstream
    FileNotFoundError."""
    workspace = tmp_path / "ticket" / "900"
    dest = workspace / "reads.parquet"
    monkeypatch.setattr(_EXPORT_READ_BLOCK, lambda _u, _t: {"count": 5, "dest": str(dest)})
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads_block(
                _BLOCK_MEMBERS,
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=workspace,
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "wrote no file" in exc.value.reason


# --- masked block-export resolver (_resolve_staged_masked_reads_block) ------

_EXPORT_MASKED_BLOCK = "qiita_control_plane.runner._do_action_export_read_masked_block"


def test_resolve_staged_masked_reads_block_binds_and_signs_mask_and_members(tmp_path, monkeypatch):
    """An align block sources the DP `export_read_masked_block` action; `reads`
    binds to the written file, and the signed token carries the mask_idx + the
    members verbatim (the MASKED-reads twin of the raw block resolver)."""
    workspace = tmp_path / "ticket" / "901"
    dest = workspace / "reads.parquet"
    captured: dict = {}

    def _fake_export(_url, token):
        captured["payload"] = _decode_token_payload(token)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("parquet-bytes")
        return {"count": 3, "dest": str(dest)}

    monkeypatch.setattr(_EXPORT_MASKED_BLOCK, _fake_export)

    bound = asyncio.run(
        _resolve_staged_masked_reads_block(
            _BLOCK_MEMBERS,
            mask_idx=42,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 16,
            workspace=workspace,
        )
    )
    assert bound[STAGED_READS_BINDING] == dest
    assert dest.exists()

    payload = captured["payload"]
    assert payload["action"] == "export_read_masked_block"
    assert payload["dest"] == str(dest)
    assert payload["mask_idx"] == 42
    assert payload["members"] == _BLOCK_MEMBERS


def test_resolve_staged_masked_reads_block_empty_members_is_bad_input(tmp_path, monkeypatch):
    def _boom(_url, _token):
        raise AssertionError("export_read_masked_block must not fire for an empty block")

    monkeypatch.setattr(_EXPORT_MASKED_BLOCK, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_masked_reads_block(
                [],
                mask_idx=1,
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=tmp_path / "ws",
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT


def test_resolve_staged_masked_reads_block_zero_count_binds_empty_parquet(tmp_path, monkeypatch):
    """Zero MASKED reads is a LEGITIMATE no-op, NOT a failure — a completed mask
    can carry 0 passing reads (a blank/control or fully host/QC-filtered sample the
    planner still tiles). The resolver binds an empty (schema-correct)
    reads.parquet (the DP writes no file for an empty selection) so the block runs
    to a clean no-op completion instead of permanently wedging its ticket."""
    import duckdb

    workspace = tmp_path / "ws"
    # The DP reports 0 and writes NO file (matches export_select_to_parquet).
    monkeypatch.setattr(_EXPORT_MASKED_BLOCK, lambda _u, _t: {"count": 0, "dest": "x"})
    bound = asyncio.run(
        _resolve_staged_masked_reads_block(
            _BLOCK_MEMBERS,
            mask_idx=1,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 16,
            workspace=workspace,
        )
    )
    dest = bound[STAGED_READS_BINDING]
    assert dest == workspace / "reads.parquet"
    assert dest.exists()
    # A valid, 0-row parquet in the export_read_block column shape align_sharded binds.
    with duckdb.connect(":memory:") as conn:
        desc = conn.execute(f"SELECT * FROM read_parquet('{dest}') LIMIT 0").description
        cols = [c[0] for c in desc]
        (n,) = conn.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()
    assert n == 0
    assert cols == [
        "prep_sample_idx",
        "sequence_idx",
        "read_id",
        "sequence1",
        "qual1",
        "sequence2",
        "qual2",
    ]


def test_resolve_staged_masked_reads_block_export_failure_is_bad_input(tmp_path, monkeypatch):
    def _boom(_url, _token):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(_EXPORT_MASKED_BLOCK, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_masked_reads_block(
                _BLOCK_MEMBERS,
                mask_idx=1,
                data_plane_url="grpc://unused",
                hmac_secret=b"x" * 16,
                workspace=tmp_path / "ws",
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "masked reads" in exc.value.reason
