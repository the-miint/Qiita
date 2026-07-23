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
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData

from qiita_control_plane.runner import (
    RUN_MAP_BINDING,
    SAMPLE_MAP_BINDING,
    STAGED_MASKED_READS_BINDING,
    STAGED_READS_BINDING,
    _resolve_sample_map,
    _resolve_staged_masked_reads,
    _resolve_staged_reads,
    _stage_ena_run_roster,
    _workflow_declares_input,
    _workflow_needs_staged_masked_reads,
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


# --- ENA run roster (_stage_ena_run_roster) ---------------------------------


class _FakeRosterPool:
    """Minimal asyncpg.Pool stand-in: `.fetch()` returns canned
    (prep_sample_idx, ena_run_accession) rows regardless of the query text —
    the resolver's own SQL shape is exercised by
    repositories/tests/test_sequenced_sample.py; this fake only needs to hand
    back rows in a stable, asserted order."""

    def __init__(self, rows: list[tuple[int, str | None]]):
        self._rows = [{"prep_sample_idx": p, "ena_run_accession": a} for p, a in rows]

    async def fetch(self, *_args, **_kwargs):
        return self._rows


def test_stage_ena_run_roster_writes_ordered_parquet(tmp_path):
    """The pool's (prep_sample_idx, ena_run_accession) rows are materialized
    to `run_map.parquet`, ordered by prep_sample_idx (the repo fetch's own
    ORDER BY — this asserts the resolver preserves it verbatim)."""
    pool = _FakeRosterPool([(82, "ERR002"), (81, "ERR001")])
    bound = asyncio.run(_stage_ena_run_roster(pool, 5, workspace=tmp_path / "ws"))
    out = bound[RUN_MAP_BINDING]
    assert out.exists()
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, ena_run_accession FROM read_parquet('{out}') "
            "ORDER BY prep_sample_idx"
        ).fetchall()
    assert rows == [(81, "ERR001"), (82, "ERR002")]


def test_stage_ena_run_roster_rejects_empty_pool(tmp_path):
    """An empty pool fails loud (BAD_INPUT) — there is nothing to download,
    and this must never silently produce a 0-row run_map."""
    pool = _FakeRosterPool([])
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(_stage_ena_run_roster(pool, 5, workspace=tmp_path / "ws"))
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "no sequenced_samples" in exc.value.reason


def test_stage_ena_run_roster_rejects_missing_accession(tmp_path):
    """A prep_sample with no ena_run_accession is a misconfiguration (a
    non-ENA sample sharing the pool) — fails loud rather than silently
    dropping it from the roster."""
    pool = _FakeRosterPool([(81, "ERR001"), (82, None)])
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(_stage_ena_run_roster(pool, 5, workspace=tmp_path / "ws"))
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "82" in exc.value.reason


def test_workflow_declares_run_map_binding_gate():
    """`_workflow_declares_input` recognizes RUN_MAP_BINDING like any other
    declared input — the runner's dispatch branch in `_workflow.py` gates on
    exactly this, not on scope-kind, so it never fires for bcl-convert's
    (also sequenced_pool-scoped) ticket."""
    ena_steps = [_step(inputs=["run_map", "reads_staging_root"], outputs=["read_staging_dir"])]
    assert _workflow_declares_input(ena_steps, RUN_MAP_BINDING) is True

    bcl_steps = [_step(inputs=["convert_dir", "sample_map"], outputs=["read_staging_dir"])]
    assert _workflow_declares_input(bcl_steps, RUN_MAP_BINDING) is False


_EXPORT_READ = "qiita_control_plane.runner._do_action_export_read"
# The zero-read control-split lookup, monkeypatched so these pure-unit tests
# exercise the routing decision without a DB. The seam's real DB behavior
# (prep_sample -> biosample -> control marker) is pinned by the DB-bound tests
# in test_host_filter_resolver.py.
_CONTROL_LOOKUP = "qiita_control_plane.runner._read_ingest._prep_sample_is_expected_empty_control"
# `_resolve_staged_reads` now takes a pool first; the branches these tests reach
# either don't touch it or monkeypatch the one helper that would, so a sentinel
# stands in for it.
_FAKE_POOL = object()


def _control_lookup(is_control: bool):
    async def _fn(_pool, _prep_sample_idx):
        return is_control

    return _fn


def _staged_kwargs(tmp_path):
    return {
        "data_plane_url": "grpc://unused",
        "signing_key": b"x" * 32,
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
        _resolve_staged_reads(
            _FAKE_POOL, {"prep_sample_idx": 42}, staging_root, **_staged_kwargs(tmp_path)
        )
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
            _FAKE_POOL,
            {"prep_sample_idx": 42},
            tmp_path / "staging",
            data_plane_url="grpc://unused",
            signing_key=b"x" * 32,
            workspace=workspace,
        )
    )
    assert bound[STAGED_READS_BINDING] == dest
    assert dest.exists()


def test_resolve_staged_reads_empty_data_well_fails_must_be_ingested(tmp_path, monkeypatch):
    """Durable absent, the data plane returns 0 rows, and the well is NOT a control
    → BAD_INPUT 'must be ingested' — an unexpected-empty data well is a real
    failure, the no-stored-reads semantics preserved."""
    monkeypatch.setattr(_EXPORT_READ, lambda _u, _t: {"count": 0, "dest": "x"})
    monkeypatch.setattr(_CONTROL_LOOKUP, _control_lookup(False))
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads(
                _FAKE_POOL, {"prep_sample_idx": 7}, tmp_path / "staging", **_staged_kwargs(tmp_path)
            )
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "must be ingested" in exc.value.reason


def test_resolve_staged_reads_empty_control_well_is_no_data(tmp_path, monkeypatch):
    """Durable absent, the data plane returns 0 rows, and the well IS an
    expected-empty control (blank / NTC) → StepNoData (terminal no_data), NOT a
    failure — an empty control must not land in the pool's samples_failed."""
    monkeypatch.setattr(_EXPORT_READ, lambda _u, _t: {"count": 0, "dest": "x"})
    monkeypatch.setattr(_CONTROL_LOOKUP, _control_lookup(True))
    with pytest.raises(StepNoData) as exc:
        asyncio.run(
            _resolve_staged_reads(
                _FAKE_POOL, {"prep_sample_idx": 7}, tmp_path / "staging", **_staged_kwargs(tmp_path)
            )
        )
    assert "expected-empty control" in exc.value.reason
    assert "7" in exc.value.reason


def test_resolve_staged_reads_export_failure_is_bad_input(tmp_path, monkeypatch):
    """A Flight failure from the export action is wrapped as BAD_INPUT (it never
    escapes as an untyped exception)."""

    def _boom(_url, _token):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(_EXPORT_READ, _boom)
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(
            _resolve_staged_reads(
                _FAKE_POOL, {"prep_sample_idx": 7}, tmp_path / "staging", **_staged_kwargs(tmp_path)
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
                _FAKE_POOL,
                {"prep_sample_idx": 7},
                tmp_path / "staging",
                data_plane_url="grpc://unused",
                signing_key=b"x" * 32,
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


# --- masked-read resolver (_resolve_staged_masked_reads) -------------------

_STREAM_MASKED = "qiita_control_plane.runner._stream_masked_reads_to_fastq"


class _FakePool:
    """Minimal asyncpg.Pool stand-in: `fetchval` returns a fixed mask_sample
    gate state (None = no gate row = allowed)."""

    def __init__(self, gate_state: str | None = None):
        self._gate_state = gate_state

    async def fetchval(self, *_args, **_kwargs):
        return self._gate_state


def _run_masked(pool, prep_sample_idx, workspace, mask_idx=77):
    return asyncio.run(
        _resolve_staged_masked_reads(
            pool,
            {"prep_sample_idx": prep_sample_idx},
            mask_idx,
            data_plane_url="grpc://unused",
            signing_key=b"x" * 32,
            workspace=workspace,
        )
    )


def test_workflow_needs_staged_masked_reads_gate():
    """`masked_reads_fastq` consumed but not produced → needs the masked staged
    binding (assembly). The raw-`reads` gate must NOT fire on it, and vice-versa."""
    assembly = [_step(inputs=["masked_reads_fastq"], outputs=["genomes_dir"])]
    assert _workflow_needs_staged_masked_reads(assembly) is True
    assert _workflow_needs_staged_reads(assembly) is False

    raw = [_step(inputs=["reads", "qc_mask"], outputs=["read_mask"])]
    assert _workflow_needs_staged_masked_reads(raw) is False


def test_resolve_staged_masked_reads_streams_fastq_and_binds(tmp_path, monkeypatch):
    """No gate row + count>0: the runner streams read_masked to a gzip FASTQ (miint
    COPY FORMAT FASTQ), which `masked_reads_fastq` binds to. No parquet, no
    DoAction."""
    workspace = tmp_path / "ticket" / "804"
    dest = workspace / "masked_reads.fastq.gz"

    def _fake_stream(_url, _ticket, out):
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("fastq-bytes")
        return 9

    monkeypatch.setattr(_STREAM_MASKED, _fake_stream)

    bound = _run_masked(_FakePool(), 42, workspace)
    assert bound[STAGED_MASKED_READS_BINDING] == dest
    assert dest.exists()


def test_resolve_staged_masked_reads_incomplete_mask_is_bad_input(tmp_path, monkeypatch):
    """A mask_sample gate row that is not 'completed' (a covering block still
    masking) → BAD_INPUT before any stream — never assemble a partial pass-set."""
    monkeypatch.setattr(_STREAM_MASKED, lambda _u, _t, _d: pytest.fail("must not stream"))
    with pytest.raises(BackendFailure) as exc:
        _run_masked(_FakePool("processing"), 7, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "not completed" in exc.value.reason


def test_resolve_staged_masked_reads_empty_stream_is_no_data(tmp_path, monkeypatch):
    """0 passing reads under the mask is a COMMON outcome (heavy filtering removed
    everything) → terminal StepNoData, NOT a failure; the empty fastq is removed."""
    workspace = tmp_path / "ws"

    def _fake_stream(_url, _ticket, out):
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("")  # COPY may create an empty file
        return 0

    monkeypatch.setattr(_STREAM_MASKED, _fake_stream)
    with pytest.raises(StepNoData) as exc:
        _run_masked(_FakePool(), 7, workspace)
    assert "nothing to assemble" in exc.value.reason
    assert not (workspace / "masked_reads.fastq.gz").exists()


def test_resolve_staged_masked_reads_stream_failure_is_bad_input(tmp_path, monkeypatch):
    """A Flight/stream failure is wrapped as BAD_INPUT (never an untyped exception)."""

    def _boom(_url, _ticket, _dest):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(_STREAM_MASKED, _boom)
    with pytest.raises(BackendFailure) as exc:
        _run_masked(_FakePool(), 7, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "data plane" in exc.value.reason


def test_workflow_declares_input_checks_optional_too():
    steps = [_step(inputs=["reads"], optional_inputs=["host_rype_path"])]
    assert _workflow_declares_input(steps, "host_rype_path") is True
    assert _workflow_declares_input(steps, "sample_map") is False


# --- block reads: no resolver by design -------------------------------------
#
# `_resolve_staged_reads_block` / `_resolve_staged_masked_reads_block` are GONE.
# A block's steps stream their reads from the data plane at runtime
# (POST /read/ticket/doget), so the control plane no longer materializes a
# per-ticket reads.parquet onto shared scratch at submit time. What those
# resolvers tested moved, it was not dropped:
#
#   * the raw-vs-masked decision (including the ON DELETE SET NULL trap) ->
#     tests/test_block_read.py, on the shared rule both boundaries use;
#   * the signed member/mask scope -> tests/routes/test_read_doget.py;
#   * the member selector's row semantics (gap excluded, split sub-range exact)
#     -> the block-read DoGet tests in qiita-data-plane/src/flight_service.rs.
#
# The empty-block case changed MEANING and is worth stating: the masked block
# export used to write a schema-correct 0-row parquet so an all-masked-out block
# ran to a clean no-op. A zero-row Arrow stream carries its schema, so the job
# now binds a valid empty relation with no special case at all
# (tests/test_read_source.py::test_empty_stream_is_not_an_error in the
# orchestrator). An empty MEMBER LIST is a different thing — a planning bug — and
# is refused at three boundaries: the route, sign_ticket, and the data plane.


def test_block_read_resolvers_are_gone():
    """Pin the removal so a future change re-adding submit-time block staging is
    a deliberate act, not an accident."""
    from qiita_control_plane import runner

    for name in (
        "_resolve_staged_reads_block",
        "_resolve_staged_masked_reads_block",
        "_do_action_export_read_block",
        "_do_action_export_read_masked_block",
        "_write_empty_reads_parquet",
    ):
        assert not hasattr(runner, name), f"{name} should have been removed"
