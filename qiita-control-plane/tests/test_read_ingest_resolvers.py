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
import concurrent.futures
import gzip
import logging
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData

from qiita_control_plane.auth import tickets
from qiita_control_plane.auth.tickets import run_signed_flight_call, token_expiry
from qiita_control_plane.preflight import (
    AmpliconBarcode,
    AmpliconPreflightError,
    amplicon_barcode_from_blob,
)
from qiita_control_plane.repositories import INT4_MASK
from qiita_control_plane.repositories.sequencing_run import (
    POOL_LOCK_WAIT_TIMEOUT_S,
    POOL_RESOLVE_LOCK_CLASS,
)
from qiita_control_plane.runner import (
    BARCODE_MAP_BINDING,
    ENA_RUN_MAP_BINDING,
    SAMPLE_MAP_BINDING,
    STAGED_MASKED_READS_BINDING,
    STAGED_READS_BINDING,
    _resolve_barcode_map,
    _resolve_sample_map,
    _resolve_staged_masked_reads,
    _resolve_staged_reads,
    _stage_ena_run_roster,
    _stage_ena_run_roster_binding,
    _workflow_declares_input,
    _workflow_needs_staged_masked_reads,
    _workflow_needs_staged_reads,
    _write_reference_fasta,
)
from qiita_control_plane.runner._read_ingest import (
    _barcode_roster_mismatches,
    _preflight_barcode_roster,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

_AMPLICON_GZ = Path(__file__).resolve().parent / "cli" / "data" / "good_amplicon_v1.sqlite.gz"


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


class _NoopAsyncCtx:
    """Async context manager yielding its value; lets the fake pool stand in
    for `acquire()` / `transaction()` without a DB."""

    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, *_exc):
        return False


class _FakeRosterPool:
    """Minimal asyncpg.Pool stand-in: `.fetch()` returns canned
    (prep_sample_idx, ena_run_accession) rows regardless of the query text —
    the resolver's own SQL shape is exercised by
    repositories/tests/test_sequenced_sample.py; this fake only needs to hand
    back rows in a stable, asserted order. `acquire`/`transaction` hand back
    no-op context managers; `execute` and `fetch` RECORD their calls instead
    of running SQL, so a test can pin which key the advisory-lock statement
    locked (and with what timeout), or that nothing ran at all.
    `in_transaction` is what `require_transaction` sees, to pin that guard."""

    def __init__(self, rows: list[tuple[int, str | None]], *, in_transaction: bool = True):
        self._rows = [{"prep_sample_idx": p, "ena_run_accession": a} for p, a in rows]
        self.in_transaction = in_transaction
        self.execute_calls: list[tuple[tuple, dict]] = []
        self.fetch_calls: list[tuple] = []

    def acquire(self):
        return _NoopAsyncCtx(self)

    def transaction(self):
        return _NoopAsyncCtx(self)

    def is_in_transaction(self):
        return self.in_transaction

    async def execute(self, *args, **kwargs):
        self.execute_calls.append((args, kwargs))
        return "SET"

    async def fetch(self, *args, **kwargs):
        self.fetch_calls.append(args)
        return self._rows


def test_stage_ena_run_roster_writes_ordered_parquet(tmp_path):
    """The pool's (prep_sample_idx, ena_run_accession) rows are materialized
    to `ena_run_map.parquet`, ordered by prep_sample_idx (the repo fetch's own
    ORDER BY — this asserts the resolver preserves it verbatim)."""
    pool = _FakeRosterPool([(82, "ERR002"), (81, "ERR001")])
    bound = asyncio.run(
        _stage_ena_run_roster(pool, 5, sequencing_run_idx=7, workspace=tmp_path / "ws")
    )
    out = bound[ENA_RUN_MAP_BINDING]
    assert out.exists()
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, ena_run_accession FROM read_parquet('{out}') "
            "ORDER BY prep_sample_idx"
        ).fetchall()
    assert rows == [(81, "ERR001"), (82, "ERR002")]


def test_stage_ena_run_roster_rejects_empty_pool(tmp_path):
    """An empty pool fails loud (BAD_INPUT) — there is nothing to download,
    and this must never silently produce a 0-row ena_run_map."""
    pool = _FakeRosterPool([])
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(_stage_ena_run_roster(pool, 5, sequencing_run_idx=7, workspace=tmp_path / "ws"))
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "no sequenced_samples" in exc.value.reason


def test_stage_ena_run_roster_rejects_missing_accession(tmp_path):
    """A prep_sample with no ena_run_accession is a misconfiguration (a
    non-ENA sample sharing the pool) — fails loud rather than silently
    dropping it from the roster."""
    pool = _FakeRosterPool([(81, "ERR001"), (82, None)])
    with pytest.raises(BackendFailure) as exc:
        asyncio.run(_stage_ena_run_roster(pool, 5, sequencing_run_idx=7, workspace=tmp_path / "ws"))
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "82" in exc.value.reason


def test_stage_ena_run_roster_locks_the_run_key_with_the_bounded_wait(tmp_path):
    """The lock goes to the *sequencing_run* key (7 — distinct from the pool
    idx 5 passed alongside it), under POOL_RESOLVE_LOCK_CLASS and the
    deliberate POOL_LOCK_WAIT_TIMEOUT_S bound rather than the pool's
    inherited 10s command_timeout."""
    pool = _FakeRosterPool([(81, "ERR001")])
    asyncio.run(_stage_ena_run_roster(pool, 5, sequencing_run_idx=7, workspace=tmp_path / "ws"))
    (args, kwargs) = pool.execute_calls[0]
    assert args == (
        "SELECT pg_advisory_xact_lock($1, $2)",
        POOL_RESOLVE_LOCK_CLASS,
        7 & INT4_MASK,
    )
    assert kwargs["timeout"] == POOL_LOCK_WAIT_TIMEOUT_S


def test_lock_sequencing_run_refuses_without_a_transaction(tmp_path):
    """In autocommit the xact-lock dies with the statement, silently
    protecting nothing — a caller that forgot its transaction must fail
    loudly instead, before any SQL runs."""
    pool = _FakeRosterPool([(81, "ERR001")], in_transaction=False)
    with pytest.raises(RuntimeError, match="outside a transaction"):
        asyncio.run(_stage_ena_run_roster(pool, 5, sequencing_run_idx=7, workspace=tmp_path / "ws"))
    assert pool.execute_calls == []


def test_stage_ena_run_roster_binding_stages_for_declared_workflow(tmp_path):
    """run_workflow's pre-loop wiring: a workflow declaring `ena_run_map` gets
    the roster staged from the ticket's scope — pool idx 5 and sequencing_run
    idx 7 travel as separate arguments, so a swap fails on the recorded lock
    key here rather than on a live DB."""
    pool = _FakeRosterPool([(81, "ERR001")])
    steps = [_step(inputs=["ena_run_map"], outputs=["read_staging_dir"])]
    scope_target = {
        "kind": "sequenced_pool",
        "sequenced_pool_idx": 5,
        "sequencing_run_idx": 7,
    }
    bound = asyncio.run(
        _stage_ena_run_roster_binding(
            pool, action_steps=steps, scope_target=scope_target, workspace=tmp_path / "ws"
        )
    )
    assert bound is not None
    assert bound[ENA_RUN_MAP_BINDING].exists()
    assert pool.fetch_calls  # the live roster read ran
    (args, _kwargs) = pool.execute_calls[0]
    assert args == (
        "SELECT pg_advisory_xact_lock($1, $2)",
        POOL_RESOLVE_LOCK_CLASS,
        7 & INT4_MASK,
    )


def test_stage_ena_run_roster_binding_skips_undeclared_workflow(tmp_path):
    """A workflow that declares no `ena_run_map` (bcl-convert: also
    sequenced_pool-scoped) stages nothing — no lock taken, no read run."""
    pool = _FakeRosterPool([(81, "ERR001")])
    steps = [_step(inputs=["convert_dir", "sample_map"], outputs=["read_staging_dir"])]
    scope_target = {
        "kind": "sequenced_pool",
        "sequenced_pool_idx": 5,
        "sequencing_run_idx": 7,
    }
    bound = asyncio.run(
        _stage_ena_run_roster_binding(
            pool, action_steps=steps, scope_target=scope_target, workspace=tmp_path / "ws"
        )
    )
    assert bound is None
    assert pool.execute_calls == []
    assert pool.fetch_calls == []


def test_workflow_declares_run_map_binding_gate():
    """`_workflow_declares_input` recognizes ENA_RUN_MAP_BINDING like any other
    declared input — the runner's dispatch branch in `_workflow.py` gates on
    exactly this, not on scope-kind, so it never fires for bcl-convert's
    (also sequenced_pool-scoped) ticket."""
    ena_steps = [_step(inputs=["ena_run_map", "reads_staging_root"], outputs=["read_staging_dir"])]
    assert _workflow_declares_input(ena_steps, ENA_RUN_MAP_BINDING) is True

    bcl_steps = [_step(inputs=["convert_dir", "sample_map"], outputs=["read_staging_dir"])]
    assert _workflow_declares_input(bcl_steps, ENA_RUN_MAP_BINDING) is False


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
    gate state. Under the first-class completion contract, only 'completed' is
    allowed; None (no gate row) and any other state are rejected."""

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
    """Completed gate + count>0: the runner streams read_masked to a gzip FASTQ
    (miint COPY FORMAT FASTQ), which `masked_reads_fastq` binds to. No parquet, no
    DoAction."""
    workspace = tmp_path / "ticket" / "804"
    dest = workspace / "masked_reads.fastq.gz"

    def _fake_stream(_url, _ticket, out):
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("fastq-bytes")
        return 9

    monkeypatch.setattr(_STREAM_MASKED, _fake_stream)

    bound = _run_masked(_FakePool("completed"), 42, workspace)
    assert bound[STAGED_MASKED_READS_BINDING] == dest
    assert dest.exists()


def test_resolve_staged_masked_reads_incomplete_mask_is_bad_input(tmp_path, monkeypatch):
    """A mask_sample gate row that is not 'completed' (a covering block still
    masking) → BAD_INPUT before any stream — never assemble a partial pass-set."""
    monkeypatch.setattr(_STREAM_MASKED, lambda _u, _t, _d: pytest.fail("must not stream"))
    with pytest.raises(BackendFailure) as exc:
        _run_masked(_FakePool("processing"), 7, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "not masked-complete" in exc.value.reason


def test_resolve_staged_masked_reads_no_gate_row_is_bad_input(tmp_path, monkeypatch):
    """No gate row (None) means no read-mask has completed for this pair. Under the
    first-class completion contract that is NOT exempt: reject before any stream,
    same as a non-completed row (both masking paths now write the gate)."""
    monkeypatch.setattr(_STREAM_MASKED, lambda _u, _t, _d: pytest.fail("must not stream"))
    with pytest.raises(BackendFailure) as exc:
        _run_masked(_FakePool(None), 7, tmp_path / "ws")
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "not masked-complete" in exc.value.reason


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
        _run_masked(_FakePool("completed"), 7, workspace)
    assert "nothing to assemble" in exc.value.reason
    assert not (workspace / "masked_reads.fastq.gz").exists()


def test_resolve_staged_masked_reads_stream_failure_is_bad_input(tmp_path, monkeypatch):
    """A Flight/stream failure is wrapped as BAD_INPUT (never an untyped exception)."""

    def _boom(_url, _ticket, _dest):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(_STREAM_MASKED, _boom)
    with pytest.raises(BackendFailure) as exc:
        _run_masked(_FakePool("completed"), 7, tmp_path / "ws")
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


# --- barcode_map (golay-demux) ----------------------------------------------


_READ_INGEST = "qiita_control_plane.runner._read_ingest"


def _pool_with_preflight(monkeypatch, blob: bytes | None, members: dict[str, int]) -> None:
    """Stand in for the pool's two reads: its stored pre-flight and its
    `{sequenced_pool_item_id: prep_sample_idx}` membership."""

    async def preflight(_pool, *, sequencing_run_idx, sequenced_pool_idx):
        assert (sequencing_run_idx, sequenced_pool_idx) == (3, 7)
        return None if blob is None else {"run_preflight_blob": blob}

    async def item_prep_sample_idxs(_pool, sequenced_pool_idx):
        assert sequenced_pool_idx == 7
        return members

    monkeypatch.setattr(f"{_READ_INGEST}.fetch_sequenced_pool_preflight", preflight)
    monkeypatch.setattr(
        f"{_READ_INGEST}.fetch_sequenced_pool_item_prep_sample_idxs", item_prep_sample_idxs
    )


def _real_pool(monkeypatch, build_amplicon_preflight, **build):
    """The committed amplicon pre-flight as the pool's blob, its samples as the
    pool's members (prep_sample_idx = 1000 + prepped_sample_idx), and the roster
    the CLI would submit for it."""
    blob = build_amplicon_preflight(**build).read_bytes()
    by_item = amplicon_barcode_from_blob(blob)
    members = {item_id: 1000 + int(item_id) for item_id in by_item}
    _pool_with_preflight(monkeypatch, blob, members)
    roster = [
        {
            "prep_sample_idx": members[item_id],
            "barcode": b.barcode,
            "barcodes_are_rc": b.barcodes_are_rc,
        }
        for item_id, b in by_item.items()
    ]
    return roster


def _resolve(roster, tmp_path):
    return asyncio.run(
        _resolve_barcode_map(
            None,
            {BARCODE_MAP_BINDING: roster},
            tmp_path / "ws",
            sequencing_run_idx=3,
            sequenced_pool_idx=7,
        )
    )


def _bad_input(roster, tmp_path) -> str:
    with pytest.raises(BackendFailure) as exc:
        _resolve(roster, tmp_path)
    assert exc.value.kind == FailureKind.BAD_INPUT
    return exc.value.reason


@pytest.mark.parametrize("barcodes_are_rc", [True, False])
def test_resolve_barcode_map_writes_a_roster_the_preflight_confirms(
    monkeypatch, tmp_path, build_amplicon_preflight, barcodes_are_rc
):
    """A roster that matches the pool's stored pre-flight is written with the
    (prep_sample_idx, barcode, barcodes_are_rc) columns the golay_demux step reads."""
    roster = _real_pool(monkeypatch, build_amplicon_preflight, barcodes_are_rc=barcodes_are_rc)
    out = _resolve(roster, tmp_path)[BARCODE_MAP_BINDING]
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, barcode, barcodes_are_rc FROM read_parquet('{out}')"
        ).fetchall()
    assert sorted(rows) == sorted(
        (e["prep_sample_idx"], e["barcode"], e["barcodes_are_rc"]) for e in roster
    )


def test_resolve_barcode_map_refuses_a_transposed_roster(
    monkeypatch, tmp_path, build_amplicon_preflight
):
    """Two samples' barcodes swapped -- the failure the check exists for: each
    would demultiplex the other's reads, and nothing downstream would notice."""
    roster = _real_pool(monkeypatch, build_amplicon_preflight)
    roster[0]["barcode"], roster[1]["barcode"] = roster[1]["barcode"], roster[0]["barcode"]
    reason = _bad_input(roster, tmp_path)
    assert "does not match sequenced_pool 7's stored pre-flight" in reason
    for entry in roster[:2]:
        assert f"prep_sample_idx {entry['prep_sample_idx']} has barcode" in reason


def test_resolve_barcode_map_refuses_the_wrong_orientation(
    monkeypatch, tmp_path, build_amplicon_preflight
):
    roster = _real_pool(monkeypatch, build_amplicon_preflight)
    roster[0]["barcodes_are_rc"] = not roster[0]["barcodes_are_rc"]
    assert "barcodes_are_rc" in _bad_input(roster, tmp_path)


def test_resolve_barcode_map_refuses_missing_extra_and_repeated_samples(
    monkeypatch, tmp_path, build_amplicon_preflight
):
    roster = _real_pool(monkeypatch, build_amplicon_preflight)
    dropped = roster.pop()
    roster.append({**roster[0]})  # repeated
    roster.append({"prep_sample_idx": 9, "barcode": "ACGT", "barcodes_are_rc": True})  # extra
    reason = _bad_input(roster, tmp_path)
    assert f"prep_sample_idx {roster[0]['prep_sample_idx']} appears more than once" in reason
    assert "prep_sample_idx 9 is not a sample of this pool" in reason
    assert f"prep_sample_idx {dropped['prep_sample_idx']} is missing from barcode_map" in reason


def test_resolve_barcode_map_needs_the_pools_preflight(monkeypatch, tmp_path):
    _pool_with_preflight(monkeypatch, None, {})
    roster = [{"prep_sample_idx": 5, "barcode": "ACGT", "barcodes_are_rc": True}]
    assert "carries no run pre-flight" in _bad_input(roster, tmp_path)


def test_resolve_barcode_map_refuses_a_preflight_the_cli_would_refuse(
    monkeypatch, tmp_path, build_amplicon_preflight
):
    blob = build_amplicon_preflight(populate_accessions=False).read_bytes()
    _pool_with_preflight(monkeypatch, blob, {"1": 5})
    roster = [{"prep_sample_idx": 5, "barcode": "ACGT", "barcodes_are_rc": True}]
    assert "cannot supply a barcode roster" in _bad_input(roster, tmp_path)


def test_resolve_barcode_map_refuses_a_pool_sample_the_preflight_lacks(
    monkeypatch, tmp_path, build_amplicon_preflight
):
    blob = build_amplicon_preflight().read_bytes()
    _pool_with_preflight(monkeypatch, blob, {"no-such-item": 5})
    roster = [{"prep_sample_idx": 5, "barcode": "ACGT", "barcodes_are_rc": True}]
    assert "does not list (sequenced_pool_item_id no-such-item)" in _bad_input(roster, tmp_path)


def test_resolve_barcode_map_rejects_empty_roster(tmp_path):
    with pytest.raises(BackendFailure) as exc:
        _resolve([], tmp_path)
    assert exc.value.kind == FailureKind.BAD_INPUT


def test_barcode_roster_mismatches_names_five_and_counts_the_rest():
    expected = {i: AmpliconBarcode("ACGT", True) for i in range(1, 10)}
    problems = _barcode_roster_mismatches([], expected)
    assert problems[:5] == [f"prep_sample_idx {i} is missing from barcode_map" for i in range(1, 6)]
    assert problems[5:] == ["and 4 more"]


def test_resolve_barcode_map_pins_each_barcode_to_its_own_sample(
    monkeypatch, tmp_path, build_amplicon_preflight
):
    """The written roster pairs each prep_sample_idx with ITS OWN barcode, checked
    against the blob's amplicon_sample table read directly -- so a barcode<->sample
    swap inside `amplicon_samples` (which both sides of the check call, so they would
    still agree) is caught. The other tests compare the check against itself."""
    blob_path = build_amplicon_preflight()
    with sqlite3.connect(blob_path) as truth_conn:
        truth = {
            1000 + int(prepped): barcode
            for prepped, barcode in truth_conn.execute(
                "SELECT prepped_sample_idx, barcode FROM amplicon_sample"
            )
        }
    blob = blob_path.read_bytes()
    by_item = amplicon_barcode_from_blob(blob)
    members = {item_id: 1000 + int(item_id) for item_id in by_item}
    _pool_with_preflight(monkeypatch, blob, members)
    roster = [
        {"prep_sample_idx": members[i], "barcode": b.barcode, "barcodes_are_rc": b.barcodes_are_rc}
        for i, b in by_item.items()
    ]
    out = _resolve(roster, tmp_path)[BARCODE_MAP_BINDING]
    with duckdb.connect(":memory:") as conn:
        written = dict(
            conn.execute(f"SELECT prep_sample_idx, barcode FROM read_parquet('{out}')").fetchall()
        )
    assert written
    for prep_sample_idx, barcode in written.items():
        assert truth[prep_sample_idx] == barcode


def test_resolve_barcode_map_accepts_a_lower_cased_roster(
    monkeypatch, tmp_path, build_amplicon_preflight
):
    """Barcode comparison is case-insensitive: the golay-demux job upper-cases
    barcodes before decoding, so a lower-cased roster still matches the pre-flight."""
    roster = _real_pool(monkeypatch, build_amplicon_preflight)
    for entry in roster:
        entry["barcode"] = entry["barcode"].lower()
    out = _resolve(roster, tmp_path)[BARCODE_MAP_BINDING]  # does not raise
    assert out.exists()


def test_resolve_barcode_map_propagates_an_unreadable_blob_error(monkeypatch, tmp_path):
    """A stored blob that cannot be READ (e.g. written against a newer pre-flight
    schema than this deployment ships -> ValueError from open_blob) is a deployment
    fault, not the submitter's bad input: it propagates rather than becoming BAD_INPUT."""
    _pool_with_preflight(monkeypatch, b"blob", {"1": 5})

    def _raise(_blob):
        raise ValueError("pre-flight written against a newer schema than this deployment ships")

    monkeypatch.setattr(f"{_READ_INGEST}.amplicon_barcode_from_blob", _raise)
    roster = [{"prep_sample_idx": 5, "barcode": "ACGT", "barcodes_are_rc": True}]
    with pytest.raises(ValueError, match="newer schema"):
        _resolve(roster, tmp_path)


def test_resolve_barcode_map_maps_bad_content_to_bad_input(monkeypatch, tmp_path):
    """An AmpliconPreflightError (the pre-flight's CONTENT cannot supply a roster)
    IS the submitter's bad input, unlike an unreadable blob."""
    _pool_with_preflight(monkeypatch, b"blob", {"1": 5})

    def _raise(_blob):
        raise AmpliconPreflightError("prepped_sample_idx 1 carries no Golay barcode")

    monkeypatch.setattr(f"{_READ_INGEST}.amplicon_barcode_from_blob", _raise)
    roster = [{"prep_sample_idx": 5, "barcode": "ACGT", "barcodes_are_rc": True}]
    assert "cannot supply a barcode roster" in _bad_input(roster, tmp_path)


def test_resolve_barcode_map_accepts_a_pool_subset_of_the_preflight(
    monkeypatch, tmp_path, build_amplicon_preflight, caplog
):
    """A pool holding only SOME of the pre-flight's samples is accepted (a well may
    be dropped before pooling); the pre-flight's absent samples are surfaced in the
    log, not silently ignored."""
    blob = build_amplicon_preflight().read_bytes()
    by_item = amplicon_barcode_from_blob(blob)
    one = next(iter(by_item))
    members = {one: 1000 + int(one)}  # the pool holds a single sample
    _pool_with_preflight(monkeypatch, blob, members)
    roster = [
        {
            "prep_sample_idx": members[one],
            "barcode": by_item[one].barcode,
            "barcodes_are_rc": by_item[one].barcodes_are_rc,
        }
    ]
    with caplog.at_level(logging.INFO):
        out = _resolve(roster, tmp_path)[BARCODE_MAP_BINDING]
    assert out.exists()
    assert "absent from the pool" in caplog.text


def _three_sample_blob(tmp_path) -> bytes:
    """The committed amplicon pre-flight trimmed to prepped_sample_idx 1-3 with
    accessions populated, so a three-sample pool can be seeded against it."""
    db = tmp_path / "preflight.db"
    db.write_bytes(gzip.decompress(_AMPLICON_GZ.read_bytes()))
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM amplicon_sample WHERE prepped_sample_idx > 3")
    conn.execute("UPDATE input_sample SET biosample_accession = 'BIO_' || sample_name")
    conn.execute("UPDATE project SET bioproject_accession = 'PRJNA' || external_project_id")
    conn.commit()
    conn.close()
    return db.read_bytes()


async def _seed_amplicon_pool(pool, owner_idx, item_ids, blob):
    run_idx = pool_idx = None
    prep_by_item: dict[str, int] = {}
    for item in item_ids:
        _bio, prep = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner_idx)
        run_idx, pool_idx, _ss = await seed_sequenced_sample_subtype(
            pool,
            prep_sample_idx=prep,
            owner_idx=owner_idx,
            sequenced_pool_item_id=item,
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )
        prep_by_item[item] = prep
    if blob is not None:
        await pool.execute(
            "UPDATE qiita.sequenced_pool SET run_preflight_blob = $1,"
            " run_preflight_filename = 'preflight.db' WHERE idx = $2",
            blob,
            pool_idx,
        )
    return run_idx, pool_idx, prep_by_item


@pytest.mark.db
async def test_resolve_barcode_map_over_real_sql_drops_retired_samples(
    postgres_pool, human_admin_session, tmp_path
):
    """End-to-end over the REAL pool SQL (not the monkeypatched reads): the runner's
    expected roster joins the stored blob to the pool's ACTIVE members, so a retired
    sample is out of the expected set -- `retire` means the same here as for the CLI.
    Exercises `_preflight_barcode_roster` + the active-set filter on a seeded pool."""
    owner = human_admin_session["principal_idx"]
    blob = _three_sample_blob(tmp_path)
    run_idx, pool_idx, prep_by_item = await _seed_amplicon_pool(
        postgres_pool, owner, ["1", "2", "3"], blob
    )

    async def _resolve_db(roster):
        return await _resolve_barcode_map(
            postgres_pool,
            {BARCODE_MAP_BINDING: roster},
            tmp_path / "ws",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )

    async def _bad_input_db(roster) -> str:
        with pytest.raises(BackendFailure) as exc:
            await _resolve_db(roster)
        assert exc.value.kind == FailureKind.BAD_INPUT
        return exc.value.reason

    expected = await _preflight_barcode_roster(
        postgres_pool, sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
    )
    assert set(expected) == set(prep_by_item.values())  # all three active
    full = [
        {"prep_sample_idx": p, "barcode": b.barcode, "barcodes_are_rc": b.barcodes_are_rc}
        for p, b in expected.items()
    ]
    await _resolve_db(full)  # matching -> accepted (no raise)

    # Retire sample "3": the active set is now {1, 2} on both sides.
    await postgres_pool.execute(
        "UPDATE qiita.prep_sample SET retired = true, retired_at = now(), retired_by_idx = $2"
        " WHERE idx = $1",
        prep_by_item["3"],
        owner,
    )
    active = {p for item, p in prep_by_item.items() if item != "3"}
    expected_active = await _preflight_barcode_roster(
        postgres_pool, sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
    )
    assert set(expected_active) == active  # the retired sample is gone

    active_roster = [e for e in full if e["prep_sample_idx"] in active]
    await _resolve_db(active_roster)  # active-only roster -> accepted

    # A roster that still names the retired sample is refused: it is not in the set.
    assert (
        f"prep_sample_idx {prep_by_item['3']} is not a sample of this pool"
        in await _bad_input_db(full)
    )
    # A roster missing an active sample is refused.
    assert "is missing from barcode_map" in await _bad_input_db(active_roster[:1])


@pytest.mark.db
async def test_resolve_barcode_map_over_real_sql_drops_ena_flagged_samples(
    postgres_pool, human_admin_session, tmp_path
):
    """An `ena_status`-flagged sequenced_sample leaves the pool's ACTIVE set the same
    way a retired prep_sample does: the membership read filters BOTH
    (`ss.ena_status IS NULL`), so the expected roster drops it. Pins the ena_status
    half of the filter, which the retired-sample test leaves NULL."""
    owner = human_admin_session["principal_idx"]
    blob = _three_sample_blob(tmp_path)
    run_idx, pool_idx, prep_by_item = await _seed_amplicon_pool(
        postgres_pool, owner, ["1", "2", "3"], blob
    )
    assert set(
        await _preflight_barcode_roster(
            postgres_pool, sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
        )
    ) == set(prep_by_item.values())  # all three active

    # Flag sample "3" as unavailable at ENA (any non-NULL ena_status): now out.
    await postgres_pool.execute(
        "UPDATE qiita.sequenced_sample SET ena_status = 'suppressed' WHERE prep_sample_idx = $1",
        prep_by_item["3"],
    )
    assert set(
        await _preflight_barcode_roster(
            postgres_pool, sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
        )
    ) == {prep_by_item["1"], prep_by_item["2"]}


@pytest.mark.db
async def test_resolve_barcode_map_over_real_sql_refuses_a_pool_with_a_null_blob(
    postgres_pool, human_admin_session, tmp_path
):
    """Over the REAL query, a pool whose row exists but stores a NULL blob is bad
    input. The monkeypatched reader returns no row; the DB returns a row with a
    NULL `run_preflight_blob` -- the `row["run_preflight_blob"] is None` half of the
    guard, which the stub-backed `needs_the_pools_preflight` test cannot reach."""
    owner = human_admin_session["principal_idx"]
    run_idx, pool_idx, prep_by_item = await _seed_amplicon_pool(
        postgres_pool, owner, ["1", "2", "3"], None
    )
    roster = [
        {"prep_sample_idx": p, "barcode": "CTACAGGGTCTC", "barcodes_are_rc": True}
        for p in prep_by_item.values()
    ]
    with pytest.raises(BackendFailure) as exc:
        await _resolve_barcode_map(
            postgres_pool,
            {BARCODE_MAP_BINDING: roster},
            tmp_path / "ws",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )
    assert exc.value.kind == FailureKind.BAD_INPUT
    assert "carries no run pre-flight" in exc.value.reason


# --- SortMeRNA reference FASTA writer ----------------------------------------


def test_write_reference_fasta_reassembles_chunks(tmp_path):
    """Chunks are grouped by feature_idx, ordered by chunk_index, concatenated,
    and written one FASTA record per feature (header = feature_idx) via miint's
    FORMAT FASTA writer. The client connect helper stands in for the service-side
    staged one (identical FASTA bytes)."""
    from qiita_control_plane.miint import connect_with_miint

    rows = [
        (2, 1, "CGA"),  # deliberately out of feature + chunk order
        (1, 0, "ACG"),
        (2, 0, "TT"),
        (1, 1, "GGA"),
    ]
    out = tmp_path / "ref.fasta"
    with connect_with_miint() as con:
        n = _write_reference_fasta(rows, out, con)
    assert n == 2
    assert out.read_text() == ">1\nACGGGA\n>2\nTTCGA\n"
    # the intermediate .partial is renamed away, never left behind.
    assert not (tmp_path / "ref.fasta.partial").exists()


def test_write_reference_fasta_empty_raises(tmp_path):
    from qiita_control_plane.miint import connect_with_miint

    with connect_with_miint() as con, pytest.raises(ValueError, match="no sequences"):
        _write_reference_fasta([], tmp_path / "ref.fasta", con)


# =============================================================================
# Token minting happens inside the executor worker
# =============================================================================
#
# `auth.tickets` reads the wall clock through its module-level `time`, so
# replacing that one reference gives these tests a clock they drive: an executor
# that "queues" a call past DEFAULT_TTL_SECONDS costs no real wait.


class _FakeClock:
    """Stands in for the `time` module in `auth.tickets` — only `time()` is read."""

    def __init__(self, start: float = 1_700_000_000.0):
        self.now = start

    def time(self) -> float:
        return self.now


class _DelayedExecutor(concurrent.futures.ThreadPoolExecutor):
    """Advances `clock` by `delay` on submit, then runs the call — the queue wait a
    fan-out wider than the pool imposes, with no real one. The advance happens on
    the submitting thread, before the worker runs, so the ordering is fixed."""

    def __init__(self, clock: _FakeClock, delay: float):
        super().__init__(max_workers=1)
        self._clock = clock
        self._delay = delay

    def submit(self, fn, /, *args, **kwargs):
        self._clock.now += self._delay
        return super().submit(fn, *args, **kwargs)


async def _with_delayed_executor(clock, delay, coro_fn):
    """Run `coro_fn()` with the loop's default executor delaying every submit."""
    executor = _DelayedExecutor(clock, delay)
    asyncio.get_running_loop().set_default_executor(executor)
    try:
        return await coro_fn()
    finally:
        executor.shutdown()


def test_run_signed_flight_call_signs_after_the_queue_wait(monkeypatch):
    """The shared seam the read-ingest resolvers call through: `sign` runs on the
    worker, so a token minted for a queued call carries a TTL measured from when
    the worker started, not from when the call was submitted."""
    clock = _FakeClock()
    monkeypatch.setattr(tickets, "time", clock)
    queue_wait = 10 * 60  # comfortably past DEFAULT_TTL_SECONDS
    submitted_at = clock.now

    def _sign() -> bytes:
        return tickets.sign_action(
            action="export_read", payload={"prep_sample_idx": 1}, secret=b"x" * 32
        )

    token = asyncio.run(
        _with_delayed_executor(
            clock,
            queue_wait,
            lambda: run_signed_flight_call(_sign, lambda t: t),
        )
    )
    assert token_expiry(token) == int(submitted_at + queue_wait) + tickets.DEFAULT_TTL_SECONDS


def test_resolve_staged_reads_token_survives_a_queue_wait_past_the_ttl(tmp_path, monkeypatch):
    """The reported failure: a read-mask fan-out wide enough to queue behind the
    default executor expired its own export_read tokens, and the queued calls
    reached the data plane already dead ("ticket expired"). The token the stub
    receives is minted after the wait, so it is still valid on arrival."""

    clock = _FakeClock()
    monkeypatch.setattr(tickets, "time", clock)
    queue_wait = 10 * 60

    seen: list[bytes] = []
    workspace = tmp_path / "ticket" / "804"
    dest = workspace / "reads.parquet"

    def _fake_export(_url, token):
        seen.append(token)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("parquet-bytes")
        return {"count": 5, "dest": str(dest)}

    monkeypatch.setattr(_EXPORT_READ, _fake_export)

    bound = asyncio.run(
        _with_delayed_executor(
            clock,
            queue_wait,
            lambda: _resolve_staged_reads(
                _FAKE_POOL,
                {"prep_sample_idx": 42},
                tmp_path / "staging",
                data_plane_url="grpc://unused",
                signing_key=b"x" * 32,
                workspace=workspace,
            ),
        )
    )
    assert bound[STAGED_READS_BINDING] == dest
    # What the data plane checks on arrival: expiry still ahead of "now". Minting
    # before the executor hop would put it `queue_wait - TTL` seconds in the past.
    assert token_expiry(seen[0]) > clock.now
