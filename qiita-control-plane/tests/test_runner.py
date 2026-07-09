"""Tests for qiita_control_plane.runner.run_workflow.

Lives in the control plane: the runner reads/writes work_ticket and
reference rows via direct DB access, and calls LIBRARY primitives
in-process. The orchestrator's ComputeBackend is reached over HTTP via
ComputeBackendClient — stubbed here.

Coverage strategy:
  * State transitions (work_ticket, reference) are observed against the
    real DB.
  * `step:` dispatch is faked at the ComputeBackendClient layer so we
    don't need a running orchestrator.
  * `action:` dispatch is monkeypatched at the LIBRARY dict so we record
    invocations without needing real DuckDB / data-plane plumbing — the
    LIBRARY primitives themselves are exercised by tests/integration/
    test_action_library.py.
"""

import json
import uuid
from pathlib import Path

import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.models import (
    ComputeTarget,
    FoundJobWire,
    StepHandleWire,
    StepProgressState,
    StepStatus,
    StepStatusWire,
    UploadStatus,
    WorkTicketFailureStage,
)
from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER, REFERENCE_LOAD_CONTAINER

from qiita_control_plane import step_progress

pytestmark = pytest.mark.db


# =============================================================================
# Fakes
# =============================================================================


class _LocalLikeBackendMixin:
    """Adapts a `run_step`-style fake (synchronous: returns the outputs map or
    raises BackendFailure) onto the decoupled submit/status/result trio the
    runner now drives.

    Models the LocalBackend's synchronous path: `submit_step` runs the step
    in-process and returns a terminal handle (`compute_target=local`,
    `terminal_outputs` set), or propagates the BackendFailure the fake raised.
    The runner short-circuits on `terminal_outputs is not None`, so it never
    calls `status_step` / `result_step` on these handles — those assert if
    reached, catching a runner regression that would poll a synchronous job.

    Subclasses keep `run_step` as the single customization point; the trio
    above is inherited, so the existing fakes only define their step behavior
    once."""

    async def submit_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        scope_target: dict,
        work_ticket_idx: int,
        attempt: int = 0,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> StepHandleWire:
        outputs = await self.run_step(
            step_name=step_name,
            inputs=inputs,
            workspace=workspace,
            scope_target=scope_target,
            work_ticket_idx=work_ticket_idx,
            container=container,
            module=module,
            entrypoint=entrypoint,
            baseline_resources=baseline_resources,
        )
        return StepHandleWire(
            compute_target=ComputeTarget.LOCAL,
            step_name=step_name,
            terminal_outputs={k: str(v) for k, v in outputs.items()},
        )

    async def status_step(self, handle: StepHandleWire) -> StepStatusWire:
        raise AssertionError(
            "local-like fake: status_step must not be called for a terminal handle"
        )

    async def result_step(self, handle: StepHandleWire, status: StepStatusWire) -> dict[str, Path]:
        raise AssertionError(
            "local-like fake: result_step must not be called for a terminal handle"
        )


class FakeBackendClient(_LocalLikeBackendMixin):
    """Stand-in for a synchronous (local) backend. Records calls and
    returns scripted outputs (which the runner expects to be Path
    objects keyed by the step's declared output names)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Path], Path, dict]] = []
        self.outputs_for: dict[str, dict[str, Path]] = {}

    async def run_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        scope_target: dict,
        work_ticket_idx: int,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> dict[str, Path]:
        # Accepted for protocol parity (the runner now forwards container
        # metadata for SlurmBackend); the LocalBackend-shaped fake here
        # ignores them.
        del work_ticket_idx, container, module, entrypoint, baseline_resources
        self.calls.append((step_name, dict(inputs), workspace, scope_target))
        outputs = self.outputs_for.get(step_name, {})
        for path in outputs.values():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        return outputs


@pytest.fixture
def library_spy(monkeypatch):
    """Patch every LIBRARY entry with a recording stub.

    Each spy materialises any 'output' Parquet the runner expects on disk
    so downstream entries' input checks pass. Returns the call log; tests
    set `fail_on` to make a primitive raise to exercise failure paths.
    """
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import library as _lib

    calls: list[tuple] = []
    state = {"fail_on": None}

    async def mint_features(pool, manifest_path, output_dir, *, genome_map_path=None):
        calls.append(("mint-features", manifest_path, output_dir, genome_map_path))
        if state["fail_on"] == LibraryPrimitive.MINT_FEATURES:
            raise RuntimeError("simulated mint-features failure")
        feature_map = output_dir / "feature_map.parquet"
        feature_map.parent.mkdir(parents=True, exist_ok=True)
        feature_map.touch(exist_ok=True)
        return feature_map, 0, 0

    async def write_membership(pool, reference_idx, feature_map_path):
        calls.append(("write-membership", reference_idx, feature_map_path))
        if state["fail_on"] == LibraryPrimitive.WRITE_MEMBERSHIP:
            raise RuntimeError("simulated write-membership failure")
        return 0, 0

    async def register_files(*, staging_dir, files, work_ticket_idx, hmac_secret, data_plane_url):
        # work_ticket_idx is keyword-required: if the runner stops threading it
        # (it rides in the signed payload so the data plane can mint unique,
        # ticket-traceable lake filenames), this stub raises TypeError and the
        # test fails.
        calls.append(("register-files", staging_dir, dict(files), work_ticket_idx))
        if state["fail_on"] == LibraryPrimitive.REGISTER_FILES:
            raise RuntimeError("simulated register-files failure")
        return [f"{staging_dir}/{name}" for name in files]

    monkeypatch.setitem(_lib.LIBRARY, LibraryPrimitive.MINT_FEATURES, mint_features)
    monkeypatch.setitem(_lib.LIBRARY, LibraryPrimitive.WRITE_MEMBERSHIP, write_membership)
    monkeypatch.setitem(_lib.LIBRARY, LibraryPrimitive.REGISTER_FILES, register_files)

    return type("LibrarySpy", (), {"calls": calls, "state": state})


# =============================================================================
# Action / reference / ticket fixtures
# =============================================================================


_REFERENCE_ADD_STEPS = [
    {
        "kind": "step",
        "name": "hash",
        "step_type": "singleton",
        "container": REFERENCE_HASH_CONTAINER,
        "target_status": "hashing",
        "inputs": ["fasta_path"],
        "outputs": ["manifest"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "action",
        "name": "mint-features",
        "target_status": "minting",
        "inputs": ["manifest"],
        "outputs": ["feature_map"],
    },
    {
        "kind": "action",
        "name": "write-membership",
        "inputs": ["feature_map"],
        "outputs": [],
    },
    {
        "kind": "step",
        "name": "load",
        "step_type": "singleton",
        "container": REFERENCE_LOAD_CONTAINER,
        "target_status": "loading",
        "inputs": ["fasta_path", "manifest", "feature_map"],
        "outputs": ["staging_dir"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "action",
        "name": "register-files",
        "inputs": ["staging_dir"],
        "outputs": [],
    },
]


@pytest.fixture
async def reference_idx(postgres_pool, human_admin_session) -> int:
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"runner-test-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


@pytest.fixture
async def second_reference_idx(postgres_pool, human_admin_session) -> int:
    """A second, independent reference — for the two-reference host filter
    (fastq-to-parquet/1.2.0), where rype and minimap2 come from different
    references."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"runner-test-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


@pytest.fixture
async def reference_add_action(postgres_pool):
    action_id = "reference-add"
    version = f"runner-test-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, "
        "  context_schema, steps, "
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, "
        "  success_status, failure_status"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "  $5::jsonb, $6::jsonb, 1, 1, '1 minute', $7, $8)",
        action_id,
        version,
        ["feature:mint", "reference:write", "reference:register_files"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps(_REFERENCE_ADD_STEPS),
        "active",
        "failed",
    )
    yield action_id, version
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )


@pytest.fixture
async def pending_work_ticket(postgres_pool, reference_add_action, reference_idx, tmp_path):
    action_id, version = reference_add_action
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">seq1\nACGT\n")
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps({"fasta_path": str(fasta)}),
    )
    yield {
        "work_ticket_idx": idx,
        "reference_idx": reference_idx,
        "fasta_path": fasta,
        "action": (action_id, version),
    }
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)


async def test_fetch_work_ticket_decodes_resource_override(
    postgres_pool, reference_add_action, reference_idx
):
    """The dispatch seam: `_fetch_work_ticket` must decode the
    `resource_override` JSONB column (asyncpg returns JSONB as a *string*, no
    codec is registered) into the dict `run_workflow` indexes for `mem_gb`, and
    leave a NULL override as None. Pins the shape so a future JSONB-codec
    registration (which would make the `isinstance(str)` guard skip) can't
    silently change what `run_workflow` sees."""
    from qiita_control_plane.runner import _fetch_work_ticket

    action_id, version = reference_add_action

    async def _insert(resource_override_json: str | None) -> int:
        # One at a time: the work_ticket_one_in_flight_per_reference unique
        # index forbids two pending tickets for the same (action, reference).
        return await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, reference_idx, action_context, resource_override"
            ") VALUES ($1, $2, 1, 'reference', $3, '{}'::jsonb, $4::jsonb)"
            " RETURNING work_ticket_idx",
            action_id,
            version,
            reference_idx,
            resource_override_json,
        )

    # WITH override: the JSONB string asyncpg returns is decoded to the dict
    # run_workflow indexes — mem_gb_override = _override.get("mem_gb").
    with_idx = await _insert(json.dumps({"mem_gb": 48}))
    try:
        with_row = await _fetch_work_ticket(postgres_pool, with_idx)
        assert with_row["resource_override"] == {"mem_gb": 48}
        assert with_row["resource_override"].get("mem_gb") == 48
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", with_idx
        )

    # WITHOUT override (NULL column) stays None.
    without_idx = await _insert(None)
    try:
        without_row = await _fetch_work_ticket(postgres_pool, without_idx)
        assert without_row["resource_override"] is None
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", without_idx
        )


def _populate_step_outputs(backend: FakeBackendClient, workspace: Path) -> Path:
    """Configure the fake backend so hash/load step outputs land on disk
    and a single Parquet sits in the staging dir for register-files."""
    backend.outputs_for["hash"] = {"manifest": workspace / "manifest.parquet"}
    backend.outputs_for["load"] = {"staging_dir": workspace / "staging"}
    staging = workspace / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "reference_sequences.parquet").touch(exist_ok=True)
    return staging


async def _run(
    work_ticket_idx: int,
    pool,
    backend: FakeBackendClient,
    workspace_root: Path,
    *,
    upload_staging_root: Path | None = None,
    default_adapter_reference_idx: int | None = None,
    resume: bool = False,
) -> None:
    from qiita_control_plane.runner import run_workflow

    # Tests that don't exercise upload-handle resolution still need to pass
    # something for upload_staging_root (no default on the kwarg). The path
    # only matters if a step actually reads from it.
    effective_upload_root = (
        upload_staging_root if upload_staging_root is not None else workspace_root / "uploads"
    )

    await run_workflow(
        work_ticket_idx,
        pool,
        backend,  # type: ignore[arg-type]  # protocol-shaped duck
        hmac_secret=b"unused",
        data_plane_url="grpc://unused:0",
        work_ticket_workspace_root=workspace_root,
        upload_staging_root=effective_upload_root,
        default_adapter_reference_idx=default_adapter_reference_idx,
        # 0 so any (accidental) poll loop spins instantly; the local-like
        # fakes complete synchronously at submit and never poll anyway.
        poll_interval_seconds=0,
        resume=resume,
    )


# =============================================================================
# Tests
# =============================================================================


async def test_success_path_advances_state_and_status(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        pending_work_ticket["reference_idx"],
    )
    assert ref_status == "active"

    # Backend ran two steps in declared order.
    assert [c[0] for c in backend.calls] == ["hash", "load"]

    # LIBRARY actions ran in declared order.
    assert [c[0] for c in library_spy.calls] == [
        "mint-features",
        "write-membership",
        "register-files",
    ]


async def test_rerun_advances_to_fresh_attempt_dir(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """A step that re-runs with no prior progress row lands in a FRESH attempt
    dir, leaving the orphaned one untouched. This is the update-lane → invalidate
    → `ticket run` path: the prep step's COMPLETED row was dropped so it re-runs
    against the corrected blob, but its prior attempt-0 dir still holds the stale
    (read-only 0o440) output + manifest. The runner must NOT reuse that dir (it
    would trip the output verifier / read-only overwrite) and must NOT delete it
    (a container step's output is owned by the SLURM job user, so the control
    plane can't unlink or chmod it). Instead it advances to attempt-1."""
    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    # A prior run's leftover in the first step's attempt-0 dir. pending_work_ticket
    # records no work_ticket_step rows, so the step re-runs fresh (no adoption).
    stale = workspace_root / str(work_ticket_idx) / "hash" / "attempt-0" / "stale.txt"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("stale output from a prior run")

    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    # The orphaned attempt-0 dir is left intact (we never delete SLURM-owned
    # output) — the stale file survives for postmortem.
    assert stale.exists()
    # The re-run advanced to a fresh attempt-1 dir instead of reusing attempt-0.
    assert (workspace_root / str(work_ticket_idx) / "hash" / "attempt-1").is_dir()
    # ...and the run completed cleanly.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"


async def test_failure_marks_ticket_and_reference_failed(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    from qiita_common.api_paths import LibraryPrimitive

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    library_spy.state["fail_on"] = LibraryPrimitive.WRITE_MEMBERSHIP

    with pytest.raises(RuntimeError, match="simulated write-membership failure"):
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "failed"
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        pending_work_ticket["reference_idx"],
    )
    assert ref_status == "failed"


class _NoDataBackendClient(_LocalLikeBackendMixin):
    """Backend stub whose first step raises StepNoData — the empty-well outcome.
    StepNoData is NOT a BackendFailure, so it propagates straight through
    `_run_entry_with_retry` (which only catches BackendFailure) to run_workflow's
    StepNoData arm."""

    async def run_step(self, *, step_name, **kwargs):
        raise StepNoData(
            step_name=step_name, reason=f"FASTQ file contains no records ({step_name})"
        )


async def test_no_data_transitions_ticket_to_no_data(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """A StepNoData from a step transitions the ticket PROCESSING → NO_DATA with
    all failure_* columns NULL, WITHOUT advancing the resource's success_status
    or PATCHing its failure_status, and clears any transient-retry marker. The
    runner returns cleanly (no re-raise) — no_data is a terminal success-ish
    outcome, not a task error."""
    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    # Seed a stale transient marker so we can assert it's cleared.
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET transient_reason = 'stuck', transient_since = now()"
        " WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )

    # Returns cleanly — StepNoData is not re-raised by run_workflow.
    await _run(work_ticket_idx, postgres_pool, _NoDataBackendClient(), workspace_root)

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type, failure_stage, failure_step_name, failure_reason,"
        " transient_reason, transient_since"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "no_data"
    # Not a failure: every failure_* column is NULL.
    assert row["failure_type"] is None
    assert row["failure_stage"] is None
    assert row["failure_step_name"] is None
    assert row["failure_reason"] is None
    # Transient-retry marker cleared.
    assert row["transient_reason"] is None
    assert row["transient_since"] is None

    # success_status NOT applied (reference did not reach 'active') and
    # failure_status NOT applied — the reference stays where it was, not failed.
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        pending_work_ticket["reference_idx"],
    )
    assert ref_status not in ("active", "failed")


async def test_refuses_non_pending_ticket(postgres_pool, pending_work_ticket, tmp_path):
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'processing' WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    with pytest.raises(RuntimeError, match="must be 'pending'"):
        await _run(work_ticket_idx, postgres_pool, FakeBackendClient(), tmp_path)


async def test_refuses_disabled_action(postgres_pool, pending_work_ticket, tmp_path):
    """An action disabled between submit and dispatch must FAIL the ticket
    (SUBMISSION stage, NULL step name) and re-raise. Previously the action
    pre-fetch ran ABOVE the try, so the raise stranded the ticket in PENDING
    with no failure recorded (and a misleading "marked FAILED" dispatch log)."""
    action_id, version = pending_work_ticket["action"]
    await postgres_pool.execute(
        "UPDATE qiita.action SET enabled = false, disabled_at = now(), disabled_by_idx = 1"
        " WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )
    with pytest.raises(RuntimeError, match="not found or disabled"):
        await _run(
            pending_work_ticket["work_ticket_idx"],
            postgres_pool,
            FakeBackendClient(),
            tmp_path,
        )
    row = await postgres_pool.fetchrow(
        "SELECT state, failure_stage, failure_step_name, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        pending_work_ticket["work_ticket_idx"],
    )
    assert row["state"] == "failed"
    assert row["failure_stage"] == "submission"
    assert row["failure_step_name"] is None
    assert "disabled" in row["failure_reason"]


async def test_refuses_unknown_ticket(postgres_pool, tmp_path):
    with pytest.raises(RuntimeError, match="not found"):
        await _run(999_999_999, postgres_pool, FakeBackendClient(), tmp_path)


async def test_register_files_globs_staging_dir(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """register-files picks up both flat *.parquet files (single-file
    tables, table name = stem) and top-level subdirs of part_*.parquet
    (multi-file tables, table name = subdir name). The multi-file form
    exists for `reference_sequence_chunks` — see
    qiita_compute_orchestrator.jobs.reference_load."""
    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    workspace = workspace_root / str(work_ticket_idx)
    backend.outputs_for["hash"] = {"manifest": workspace / "manifest.parquet"}
    backend.outputs_for["load"] = {"staging_dir": workspace / "staging"}
    staging = workspace / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    for name in (
        "reference_sequences.parquet",
        "reference_membership.parquet",
        "reference_taxonomy.parquet",
    ):
        (staging / name).touch(exist_ok=True)
    chunks_dir = staging / "reference_sequence_chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    (chunks_dir / "part_00000.parquet").touch()
    (chunks_dir / "part_00001.parquet").touch()

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    register_call = next(c for c in library_spy.calls if c[0] == "register-files")
    # ("register-files", staging_dir, files, work_ticket_idx)
    assert register_call[3] == work_ticket_idx, (
        "runner must thread work_ticket_idx into register-files so the data "
        "plane can mint unique, ticket-traceable lake filenames"
    )
    files = register_call[2]
    assert files == {
        "reference_sequences.parquet": "reference_sequences",
        "reference_membership.parquet": "reference_membership",
        "reference_taxonomy.parquet": "reference_taxonomy",
        "reference_sequence_chunks/part_00000.parquet": "reference_sequence_chunks",
        "reference_sequence_chunks/part_00001.parquet": "reference_sequence_chunks",
    }


async def test_threads_inputs_through_workspace(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    workspace = workspace_root / str(work_ticket_idx)
    _populate_step_outputs(backend, workspace)

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    # hash got fasta_path from action_context.
    hash_call = next(c for c in backend.calls if c[0] == "hash")
    assert hash_call[1]["fasta_path"] == pending_work_ticket["fasta_path"]

    # load got fasta_path + manifest (hash output) + feature_map (mint output).
    # `manifest` comes from FakeBackendClient.outputs_for, which the test
    # scripted at `<workspace>/manifest.parquet`. `feature_map` is written
    # by the mint-features library spy into the entry's per-attempt
    # workspace (workspace/<entry-name>/attempt-<N>/ — runner.py owns this
    # layout so retries don't leak stale artifacts at the verifier).
    load_call = next(c for c in backend.calls if c[0] == "load")
    assert load_call[1]["fasta_path"] == pending_work_ticket["fasta_path"]
    assert load_call[1]["manifest"] == workspace / "manifest.parquet"
    assert (
        load_call[1]["feature_map"]
        == workspace / "mint-features" / "attempt-0" / "feature_map.parquet"
    )


async def test_creates_workspace_directory(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    workspace_root = tmp_path / "ws_does_not_exist"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    assert (workspace_root / str(work_ticket_idx)).is_dir()


async def test_skips_redundant_status_patches(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """write-membership and register-files don't declare target_status,
    so the runner should reuse the previous status (minting / loading
    respectively) instead of re-PATCHing the same value. With the new
    transition_reference_status, a same-status PATCH would raise
    IllegalStatusTransition; a regression here would crash the workflow.
    Reaching COMPLETED proves the short-circuit fired."""
    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"


async def test_swallows_failure_status_patch_error(
    postgres_pool, pending_work_ticket, library_spy, tmp_path, monkeypatch
):
    """If the best-effort failure_status PATCH itself raises, the runner
    still marks the work_ticket FAILED and re-raises the *original*
    primitive-level exception (not the secondary PATCH failure)."""
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import reference as _ref

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    library_spy.state["fail_on"] = LibraryPrimitive.MINT_FEATURES

    real_transition = _ref.transition_reference_status

    async def transition_then_fail(pool, reference_idx, target):
        if str(target) == "failed":
            raise RuntimeError("simulated failure_status PATCH failure")
        return await real_transition(pool, reference_idx, target)

    monkeypatch.setattr(
        "qiita_control_plane.runner.transition_reference_status", transition_then_fail
    )

    with pytest.raises(RuntimeError, match="simulated mint-features failure"):
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "failed"


# =============================================================================
# Retry-loop tests
# =============================================================================


class _RetryingBackendClient(_LocalLikeBackendMixin):
    """Backend stub that raises BackendFailure on the first N attempts of
    a named step, then succeeds. Used to drive the retry loop without
    needing a real orchestrator. Each call increments the per-step
    counter so an instance can fail one step transiently while another
    succeeds first try.

    The raise models the LocalBackend failing in-process at submit time —
    the mixin's `submit_step` calls `run_step` and lets the BackendFailure
    propagate, exactly as a synchronous backend would."""

    def __init__(
        self,
        *,
        fail_step: str,
        fail_n_times: int,
        kind,  # FailureKind
        outputs_on_success: dict[str, Path] | None = None,
    ) -> None:
        self.fail_step = fail_step
        self.fail_n_times = fail_n_times
        self.kind = kind
        self.outputs_on_success = outputs_on_success or {}
        self.attempts: dict[str, int] = {}

    async def run_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        scope_target: dict,
        work_ticket_idx: int,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> dict[str, Path]:
        from qiita_common.backend_failure import BackendFailure
        from qiita_common.models import WorkTicketFailureStage

        del scope_target, work_ticket_idx, container, module, entrypoint, baseline_resources
        self.attempts[step_name] = self.attempts.get(step_name, 0) + 1
        if step_name == self.fail_step and self.attempts[step_name] <= self.fail_n_times:
            raise BackendFailure(
                kind=self.kind,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=f"simulated {self.kind.value} attempt {self.attempts[step_name]}",
            )
        for path in self.outputs_on_success.values():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        return self.outputs_on_success


async def test_retry_succeeds_after_transient_failure(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """A transient BackendFailure on the hash step is retried; the
    second attempt succeeds and the workflow completes. retry_count is
    bumped to 1 in the process."""
    from qiita_common.backend_failure import FailureKind

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    # Backend fails the FIRST hash attempt with NODE_FAIL (retriable),
    # succeeds on the second. Load step has its own success output.
    class _Backend(_RetryingBackendClient):
        async def run_step(
            self, *, step_name, inputs, workspace, scope_target, work_ticket_idx, **kw
        ):
            self.attempts[step_name] = self.attempts.get(step_name, 0) + 1
            if step_name == "load":
                workspace.mkdir(parents=True, exist_ok=True)
                (workspace / "staging").mkdir(parents=True, exist_ok=True)
                (workspace / "staging" / "reference_sequences.parquet").touch()
                return {"staging_dir": workspace / "staging"}
            # Reset the counter increment super does (we already counted)
            # then delegate so the fail-N-times logic keeps working.
            self.attempts[step_name] -= 1
            return await super().run_step(
                step_name=step_name,
                inputs=inputs,
                workspace=workspace,
                scope_target=scope_target,
                work_ticket_idx=work_ticket_idx,
                **kw,
            )

    backend = _Backend(
        fail_step="hash",
        fail_n_times=1,
        kind=FailureKind.NODE_FAIL,
        outputs_on_success={"manifest": workspace / "manifest.parquet"},
    )

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    # Workflow completed despite the retry.
    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count, failure_type, failure_stage, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "completed"
    assert row["retry_count"] == 1
    # COMPLETED tickets carry no failure_* (DB CHECK enforces).
    assert row["failure_type"] is None
    assert row["failure_stage"] is None
    assert row["failure_reason"] is None
    # Hash step ran twice (one fail + one success); load ran once.
    assert backend.attempts == {"hash": 2, "load": 1}


async def test_retry_uses_isolated_per_attempt_workspace(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """Each attempt of a retried entry gets its own workspace dir, and
    failed-attempt dirs persist on disk (postmortem-friendly). Prevents
    stale outputs from a failed attempt #0 from leaking into the
    verifier's "every file under output_path must be in manifest" check
    on attempt #1."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import WorkTicketFailureStage

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    class _RecordingBackend(_LocalLikeBackendMixin):
        """Records the workspace path the runner hands the backend on each
        call. Hash fails once (drops a stale file in its given workspace
        first to simulate a partial container write), then succeeds."""

        def __init__(self) -> None:
            self.workspaces: list[tuple[str, Path]] = []
            self.attempts: dict[str, int] = {}

        async def run_step(
            self, *, step_name, inputs, workspace, scope_target, work_ticket_idx, **_kw
        ):
            self.workspaces.append((step_name, workspace))
            self.attempts[step_name] = self.attempts.get(step_name, 0) + 1
            if step_name == "hash" and self.attempts["hash"] == 1:
                workspace.mkdir(parents=True, exist_ok=True)
                (workspace / "partial.parquet").write_bytes(b"stale junk")
                raise BackendFailure(
                    kind=FailureKind.NODE_FAIL,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=step_name,
                    reason="simulated transient failure",
                )
            if step_name == "load":
                workspace.mkdir(parents=True, exist_ok=True)
                staging = workspace / "staging"
                staging.mkdir(parents=True, exist_ok=True)
                (staging / "reference_sequences.parquet").touch()
                return {"staging_dir": staging}
            manifest = workspace / "manifest.parquet"
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.touch()
            return {"manifest": manifest}

    backend = _RecordingBackend()
    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    hash_calls = [ws for (name, ws) in backend.workspaces if name == "hash"]
    workspace = workspace_root / str(work_ticket_idx)
    assert hash_calls == [
        workspace / "hash" / "attempt-0",
        workspace / "hash" / "attempt-1",
    ]
    # Failed attempt's artifacts are preserved on disk for postmortem.
    assert (workspace / "hash" / "attempt-0" / "partial.parquet").exists()
    # Successful attempt has its own clean dir — no `partial.parquet` from #0.
    assert (workspace / "hash" / "attempt-1").exists()
    assert not (workspace / "hash" / "attempt-1" / "partial.parquet").exists()


async def test_retry_exhausted_marks_failed_with_retriable_type(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """Transient failures keep retrying until retry_count == max_retries,
    then transition to FAILED with failure_type='retriable' (the
    distinguishing post-mortem signal: retries-exhausted vs
    permanent-on-first-attempt)."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    # max_retries default is 3 → 4 total attempts (1 initial + 3 retries).
    # Backend always fails the hash step transiently.
    backend = _RetryingBackendClient(
        fail_step="hash",
        fail_n_times=999,  # never succeed
        kind=FailureKind.NODE_FAIL,
    )

    with pytest.raises(BackendFailure):
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count, max_retries,"
        "       failure_type, failure_stage, failure_step_name, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "failed"
    assert row["retry_count"] == row["max_retries"] == 3
    assert row["failure_type"] == "retriable"
    assert row["failure_stage"] == "step_run"
    assert row["failure_step_name"] == "hash"
    assert "node_fail" in row["failure_reason"]
    # 4 total attempts: 1 initial + 3 retries.
    assert backend.attempts["hash"] == 4


async def test_permanent_failure_skips_retry_loop(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """A permanent BackendFailure (BAD_INPUT) does NOT retry — straight
    to FAILED with failure_type='permanent', retry_count=0."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    backend = _RetryingBackendClient(
        fail_step="hash",
        fail_n_times=999,
        kind=FailureKind.BAD_INPUT,
    )

    with pytest.raises(BackendFailure):
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count, failure_type, failure_step_name"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "failed"
    assert row["retry_count"] == 0  # no retries attempted
    assert row["failure_type"] == "permanent"
    assert row["failure_step_name"] == "hash"
    # Exactly one attempt — permanent failures don't loop.
    assert backend.attempts["hash"] == 1


async def test_oom_at_memory_ceiling_fails_without_retry(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """An OOM_KILLED step that is ALREADY at the action memory ceiling does not
    retry: memory escalation has no headroom (a re-run would OOM identically),
    so the runner reclassifies it as a permanent RESOURCE_CEILING_EXHAUSTED and
    fails immediately — instead of burning the retry budget on a guaranteed
    repeat. This fixture's action has mem_ceiling_gb == the hash step's baseline
    mem_gb (1), so the very first OOM is at the ceiling."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    backend = _RetryingBackendClient(
        fail_step="hash",
        fail_n_times=999,  # would never succeed if it kept retrying
        kind=FailureKind.OOM_KILLED,
    )

    with pytest.raises(BackendFailure) as exc_info:
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)
    assert exc_info.value.kind is FailureKind.RESOURCE_CEILING_EXHAUSTED

    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count, failure_type, failure_stage,"
        "       failure_step_name, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "failed"
    assert row["retry_count"] == 0  # fail-fast: no retry consumed
    assert row["failure_type"] == "permanent"
    assert row["failure_stage"] == "step_run"
    assert row["failure_step_name"] == "hash"
    # The kind is asserted on the raised exception above; failure_reason carries
    # the human explanation (exc.reason), which names the exhausted ceiling.
    assert "memory ceiling" in row["failure_reason"]
    # Exactly one attempt — the at-ceiling OOM does not loop.
    assert backend.attempts["hash"] == 1


async def test_timeout_at_walltime_ceiling_fails_without_retry(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """Mirror of the OOM case for walltime: a TIMEOUT_BEFORE_START step already
    at the action walltime ceiling fails-fast as RESOURCE_CEILING_EXHAUSTED
    rather than re-running at the same limit. This fixture's walltime ceiling
    (1 minute) equals the hash step's baseline walltime (PT1M), so the first
    timeout is at the ceiling."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    backend = _RetryingBackendClient(
        fail_step="hash",
        fail_n_times=999,
        kind=FailureKind.TIMEOUT_BEFORE_START,
    )

    with pytest.raises(BackendFailure) as exc_info:
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)
    assert exc_info.value.kind is FailureKind.RESOURCE_CEILING_EXHAUSTED

    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count, failure_type, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "failed"
    assert row["retry_count"] == 0
    assert row["failure_type"] == "permanent"
    assert "walltime ceiling" in row["failure_reason"]
    assert backend.attempts["hash"] == 1


async def test_unwrapped_exception_marks_failed_as_permanent(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """A LIBRARY primitive raising plain Python (not BackendFailure)
    becomes failure_type='permanent' with kind UNKNOWN_PERMANENT in the
    reason. The retry loop only fires on BackendFailure; plain
    exceptions skip it because there's no classification to dispatch on."""
    from qiita_common.api_paths import LibraryPrimitive

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    library_spy.state["fail_on"] = LibraryPrimitive.MINT_FEATURES

    with pytest.raises(RuntimeError, match="simulated mint-features failure"):
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count, failure_type, failure_stage, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "failed"
    assert row["retry_count"] == 0
    assert row["failure_type"] == "permanent"
    assert row["failure_stage"] == "step_run"
    assert "RuntimeError" in row["failure_reason"]


async def test_transient_db_error_marks_failed_as_retriable(
    postgres_pool, pending_work_ticket, library_spy, tmp_path, monkeypatch
):
    """A transient CP-DB error on one of the runner's OWN DB calls — modelled
    here as a bare `asyncio.TimeoutError` (exactly how asyncpg surfaces a
    `command_timeout`) from the step's write-ahead `record_submitting` — is
    recorded `failure_type='retriable'`, NOT 'permanent'. That is the #214 fix:
    a transient DB hiccup must not abandon a healthy in-flight job as a
    deterministic failure; RETRIABLE lets a `/run` redrive re-attempt. Contrast
    `test_unwrapped_exception_marks_failed_as_permanent` (a real Python bug stays
    permanent)."""
    from qiita_control_plane import runner as _runner

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()

    async def boom_record_submitting(*_a, **_k):
        # Bare TimeoutError (empty args) — what a 10s asyncpg command_timeout
        # raises, and `asyncio.TimeoutError is TimeoutError` in 3.11+.
        raise TimeoutError

    monkeypatch.setattr(_runner.step_progress, "record_submitting", boom_record_submitting)

    with pytest.raises(TimeoutError):
        await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type, failure_stage, failure_step_name, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert row["state"] == "failed"
    assert row["failure_type"] == "retriable"  # the fix — not 'permanent'
    assert row["failure_stage"] == "step_run"
    assert row["failure_step_name"] == "hash"  # first step, where the write-ahead fired
    assert "transient control-plane DB error" in row["failure_reason"]
    assert "TimeoutError" in row["failure_reason"]


async def test_retry_observable_via_state_transitions(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """Each transient retry transitions PROCESSING → QUEUED → PROCESSING.
    Verified by observing the work_ticket state through a `before_each`
    hook installed on the backend stub: when run_step is invoked, the
    ticket must be in PROCESSING (not QUEUED — the runner re-transitions
    before each attempt).

    Uses NODE_FAIL (a transient infra kind that retries at the same
    allocation), NOT OOM_KILLED: this fixture's action has mem baseline ==
    ceiling (1 GB), so an OOM would saturate memory escalation on the first
    attempt and fail-fast as RESOURCE_CEILING_EXHAUSTED — see
    test_oom_at_memory_ceiling_fails_without_retry."""
    from qiita_common.backend_failure import FailureKind

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    observed_states: list[str] = []

    class _Backend(_RetryingBackendClient):
        async def run_step(
            self, *, step_name, inputs, workspace, scope_target, work_ticket_idx, **kw
        ):
            state = await postgres_pool.fetchval(
                "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            observed_states.append(state)
            self.attempts[step_name] = self.attempts.get(step_name, 0) + 1
            if step_name == "load":
                workspace.mkdir(parents=True, exist_ok=True)
                (workspace / "staging").mkdir(parents=True, exist_ok=True)
                (workspace / "staging" / "reference_sequences.parquet").touch()
                return {"staging_dir": workspace / "staging"}
            self.attempts[step_name] -= 1
            return await super().run_step(
                step_name=step_name,
                inputs=inputs,
                workspace=workspace,
                scope_target=scope_target,
                work_ticket_idx=work_ticket_idx,
                **kw,
            )

    backend = _Backend(
        fail_step="hash",
        fail_n_times=2,  # two transient failures, then succeeds
        kind=FailureKind.NODE_FAIL,
        outputs_on_success={"manifest": workspace / "manifest.parquet"},
    )

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    # Three hash attempts + one load attempt.
    # Every attempt observed PROCESSING (the runner transitions to
    # PROCESSING before invoking the backend, even when retrying).
    assert observed_states == ["processing", "processing", "processing", "processing"]
    final = await postgres_pool.fetchrow(
        "SELECT state, retry_count FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert final["state"] == "completed"
    assert final["retry_count"] == 2


# =============================================================================
# Upload handle resolution
# =============================================================================
#
# At workflow start the runner walks `action_context` for `*_upload_idx`
# keys, looks each up in `qiita.upload`, asserts (status='ready',
# created_by == work_ticket originator), and injects the resolved staging
# path under the matching `*_path` key in the binding map. On success the
# upload rows transition ready → consumed atomically.


@pytest.fixture
async def upload_staging_root(tmp_path):
    root = tmp_path / "staging"
    root.mkdir()
    return root


async def _insert_upload(
    pool, *, principal_idx: int, status: UploadStatus = UploadStatus.READY
) -> int:
    """Insert a qiita.upload row at the given status. Status transitions
    on this domain are CHECK-gated; insert direct so tests can set up
    pending/ready/consumed/failed without going through the route."""
    completed_at = "now()" if status != UploadStatus.PENDING else "NULL"
    return await pool.fetchval(
        f"INSERT INTO qiita.upload (status, created_by_idx, completed_at)"
        f" VALUES ($1, $2, {completed_at})"
        " RETURNING upload_idx",
        status.value,
        principal_idx,
    )


@pytest.fixture
async def upload_work_ticket(
    postgres_pool, reference_add_action, reference_idx, upload_staging_root
):
    """Variant of `pending_work_ticket` whose action_context carries
    `fasta_upload_idx` rather than `fasta_path`. The principal idx is 1
    (the system principal) to match the existing fixture's hardcoded
    originator."""
    action_id, version = reference_add_action
    upload_idx = await _insert_upload(postgres_pool, principal_idx=1, status=UploadStatus.READY)
    # Materialize the staged file on disk at the canonical layout so the
    # runner's existence check (if any) wouldn't trip; the FakeBackendClient
    # doesn't actually read it.
    from qiita_common.api_paths import compute_upload_staging_path

    staging_path = compute_upload_staging_path(upload_staging_root, upload_idx)
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path.write_bytes(b"")

    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps({"fasta_upload_idx": upload_idx}),
    )
    yield {
        "work_ticket_idx": idx,
        "reference_idx": reference_idx,
        "fasta_upload_idx": upload_idx,
        "staging_path": staging_path,
        "action": (action_id, version),
    }
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)
    await postgres_pool.execute("DELETE FROM qiita.upload WHERE upload_idx = $1", upload_idx)


async def test_runner_resolves_upload_handles_and_consumes_on_success(
    postgres_pool, upload_work_ticket, library_spy, tmp_path, upload_staging_root
):
    """Happy path: a `_upload_idx` key resolves to the canonical staging
    path under the matching `_path` binding, the step sees the resolved
    Path, and the upload transitions ready → consumed on workflow success."""
    workspace_root = tmp_path / "ws"
    work_ticket_idx = upload_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    await _run(
        work_ticket_idx,
        postgres_pool,
        backend,
        workspace_root,
        upload_staging_root=upload_staging_root,
    )

    hash_call = next(c for c in backend.calls if c[0] == "hash")
    assert hash_call[1]["fasta_path"] == upload_work_ticket["staging_path"]

    status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.upload WHERE upload_idx = $1",
        upload_work_ticket["fasta_upload_idx"],
    )
    assert status == UploadStatus.CONSUMED.value


async def test_runner_rejects_unready_upload(
    postgres_pool, reference_add_action, reference_idx, tmp_path, upload_staging_root
):
    """An upload still at status='pending' fails the workflow before any
    step runs. The upload stays pending; the work_ticket transitions to
    FAILED."""
    action_id, version = reference_add_action
    pending_upload = await _insert_upload(
        postgres_pool, principal_idx=1, status=UploadStatus.PENDING
    )
    try:
        work_ticket_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, reference_idx, action_context"
            ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
            action_id,
            version,
            reference_idx,
            json.dumps({"fasta_upload_idx": pending_upload}),
        )
        try:
            backend = FakeBackendClient()
            with pytest.raises(Exception) as ei:
                await _run(
                    work_ticket_idx,
                    postgres_pool,
                    backend,
                    tmp_path,
                    upload_staging_root=upload_staging_root,
                )
            assert "pending" in str(ei.value).lower() or "ready" in str(ei.value).lower()
            # Backend never invoked — resolution gate ran before the step loop.
            assert backend.calls == []
            state = await postgres_pool.fetchval(
                "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            assert state == "failed"
            status = await postgres_pool.fetchval(
                "SELECT status FROM qiita.upload WHERE upload_idx = $1", pending_upload
            )
            assert status == UploadStatus.PENDING.value, (
                "unready upload must not be moved by a rejected workflow"
            )
        finally:
            await postgres_pool.execute(
                "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
            )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.upload WHERE upload_idx = $1", pending_upload
        )


async def test_runner_rejects_consumed_upload(
    postgres_pool, reference_add_action, reference_idx, tmp_path, upload_staging_root
):
    """A consumed upload cannot be re-claimed by a second work ticket.
    One-shot semantics: status='consumed' is terminal for the consume
    path. Mint a fresh upload if you need to retry."""
    action_id, version = reference_add_action
    spent = await _insert_upload(postgres_pool, principal_idx=1, status=UploadStatus.CONSUMED)
    try:
        work_ticket_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket ("
            "  action_id, action_version, originator_principal_idx,"
            "  scope_target_kind, reference_idx, action_context"
            ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
            action_id,
            version,
            reference_idx,
            json.dumps({"fasta_upload_idx": spent}),
        )
        try:
            backend = FakeBackendClient()
            with pytest.raises(Exception):
                await _run(
                    work_ticket_idx,
                    postgres_pool,
                    backend,
                    tmp_path,
                    upload_staging_root=upload_staging_root,
                )
            assert backend.calls == []
            state = await postgres_pool.fetchval(
                "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            assert state == "failed"
        finally:
            await postgres_pool.execute(
                "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
            )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.upload WHERE upload_idx = $1", spent)


async def test_runner_rejects_unknown_upload(
    postgres_pool, reference_add_action, reference_idx, tmp_path, upload_staging_root
):
    """An upload_idx that doesn't exist in qiita.upload fails the workflow
    fast — no step runs, ticket FAILED."""
    action_id, version = reference_add_action
    bogus_upload_idx = 999_999_999
    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps({"fasta_upload_idx": bogus_upload_idx}),
    )
    try:
        backend = FakeBackendClient()
        with pytest.raises(Exception):
            await _run(
                work_ticket_idx,
                postgres_pool,
                backend,
                tmp_path,
                upload_staging_root=upload_staging_root,
            )
        assert backend.calls == []
        state = await postgres_pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        assert state == "failed"
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )


async def test_runner_rejects_upload_owned_by_other_principal(
    postgres_pool, reference_add_action, reference_idx, tmp_path, upload_staging_root
):
    """An upload created by principal A cannot be consumed by a work
    ticket whose originator is principal B. The runner enforces owner
    parity defensively even though the upload domain audience is
    admin-only today; a future tightening (per-row creator gates on /done)
    leans on the same invariant."""
    action_id, version = reference_add_action
    # Seed a second principal (idx != 1) to own the upload.
    other_pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ('upload-owner-other', 'user', 1) RETURNING idx"
    )
    try:
        foreign_upload = await _insert_upload(
            postgres_pool, principal_idx=other_pidx, status=UploadStatus.READY
        )
        try:
            work_ticket_idx = await postgres_pool.fetchval(
                "INSERT INTO qiita.work_ticket ("
                "  action_id, action_version, originator_principal_idx,"
                "  scope_target_kind, reference_idx, action_context"
                ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
                action_id,
                version,
                reference_idx,
                json.dumps({"fasta_upload_idx": foreign_upload}),
            )
            try:
                backend = FakeBackendClient()
                with pytest.raises(Exception) as ei:
                    await _run(
                        work_ticket_idx,
                        postgres_pool,
                        backend,
                        tmp_path,
                        upload_staging_root=upload_staging_root,
                    )
                msg = str(ei.value).lower()
                assert "owner" in msg or "principal" in msg or "created_by" in msg
                assert backend.calls == []
                state = await postgres_pool.fetchval(
                    "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                    work_ticket_idx,
                )
                assert state == "failed"
                # Owner-rejected upload is untouched.
                status = await postgres_pool.fetchval(
                    "SELECT status FROM qiita.upload WHERE upload_idx = $1", foreign_upload
                )
                assert status == UploadStatus.READY.value
            finally:
                await postgres_pool.execute(
                    "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
                )
        finally:
            await postgres_pool.execute(
                "DELETE FROM qiita.upload WHERE upload_idx = $1", foreign_upload
            )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", other_pidx)


# =============================================================================
# register-index dispatch
# =============================================================================


async def test_dispatch_register_index_writes_row(postgres_pool, reference_idx, tmp_path):
    """The register-index action arm reads the meta JSON named by its single
    `entry.inputs[0]` binding (native step outputs are path strings, so build
    params ride a file) and the reference_idx from scope_target, then records a
    qiita.reference_index row carrying the builder's index_type / fs_path /
    params. The binding name is NOT hardcoded — two register-index steps in one
    workflow (rype + minimap2) target different metas via their own `inputs:`."""
    from qiita_common.actions import WorkflowAction

    # Targets the per-primitive dispatch arm directly (no work_ticket /
    # progress-row plumbing); the progress-recording wrapper `_dispatch_action`
    # is exercised through the full run_workflow path elsewhere.
    from qiita_control_plane.runner import _run_action_primitive

    fs_path = f"/srv/qiita/references/{reference_idx}/rype/index.ryxdi"
    meta_path = tmp_path / "rype_index_meta.json"
    meta_path.write_text(
        json.dumps(
            {
                "index_type": "rype",
                "fs_path": fs_path,
                "params": {"k": 64, "w": 25, "bucket_name": f"reference_{reference_idx}"},
            }
        )
    )
    bound = {"rype_index_meta": str(meta_path)}
    entry = WorkflowAction(
        kind="action", name="register-index", inputs=["rype_index_meta"], outputs=[]
    )

    out = await _run_action_primitive(
        postgres_pool,
        entry,
        bound,
        tmp_path,
        {"kind": "reference", "reference_idx": reference_idx},
        work_ticket_idx=1,  # register-index ignores it; required by the signature
        hmac_secret=b"unused",
        data_plane_url="grpc://unused:50051",
    )
    assert out == {}

    row = await postgres_pool.fetchrow(
        "SELECT index_type, fs_path, params FROM qiita.reference_index WHERE reference_idx = $1",
        reference_idx,
    )
    assert row is not None
    assert row["index_type"] == "rype"
    assert row["fs_path"].endswith("index.ryxdi")
    assert json.loads(row["params"])["k"] == 64
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
    )


async def test_dispatch_register_index_minimap2_meta(postgres_pool, reference_idx, tmp_path):
    """A second register-index step in the same workflow targets the
    `minimap2_index_meta` binding via `entry.inputs[0]` and records a row with
    index_type='minimap2' and the minimap2 params — proving the arm reads the
    named input, not a hardcoded `rype_index_meta`."""
    from qiita_common.actions import WorkflowAction

    from qiita_control_plane.runner import _run_action_primitive

    fs_path = f"/srv/qiita/references/{reference_idx}/minimap2/index.mmi"
    meta_path = tmp_path / "minimap2_index_meta.json"
    meta_path.write_text(
        json.dumps(
            {
                "index_type": "minimap2",
                "fs_path": fs_path,
                "params": {
                    "preset": "sr",
                    "source_chunks": "/data/host/grch38.chunks",
                    "num_subjects": 1,
                },
            }
        )
    )
    bound = {"minimap2_index_meta": str(meta_path)}
    entry = WorkflowAction(
        kind="action", name="register-index", inputs=["minimap2_index_meta"], outputs=[]
    )

    out = await _run_action_primitive(
        postgres_pool,
        entry,
        bound,
        tmp_path,
        {"kind": "reference", "reference_idx": reference_idx},
        work_ticket_idx=1,  # register-index ignores it; required by the signature
        hmac_secret=b"unused",
        data_plane_url="grpc://unused:50051",
    )
    assert out == {}

    row = await postgres_pool.fetchrow(
        "SELECT index_type, fs_path, params FROM qiita.reference_index"
        " WHERE reference_idx = $1 AND index_type = 'minimap2'",
        reference_idx,
    )
    assert row is not None
    assert row["fs_path"].endswith("index.mmi")
    assert json.loads(row["params"])["preset"] == "sr"
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
    )


# =============================================================================
# Block-scope wiring (bulk-block read-mask): scope target + reconcile-block arm
# =============================================================================


def test_build_scope_target_block():
    """A block-scoped work_ticket row maps to the {kind: block, block_idx} shape
    the pre-loop / dispatch code reads (matching BlockScopeTarget)."""
    from qiita_control_plane.runner import _build_scope_target

    assert _build_scope_target({"scope_target_kind": "block", "block_idx": 42}) == {
        "kind": "block",
        "block_idx": 42,
    }


async def test_run_action_primitive_reconcile_block_dispatches(monkeypatch, tmp_path):
    """The reconcile-block arm calls the RECONCILE_BLOCK primitive with block_idx
    from the scope target and mask_idx from the runner-bound `bound` (the ticket's
    mask_idx), plus the hmac_secret / data_plane_url for the mask_metrics read."""
    from qiita_common.actions import WorkflowAction
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import library
    from qiita_control_plane.runner import _run_action_primitive

    recorded: dict = {}

    async def fake_reconcile(pool, *, block_idx, mask_idx, hmac_secret, data_plane_url):
        recorded.update(
            block_idx=block_idx,
            mask_idx=mask_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {"block_idx": block_idx, "finalized_samples": []}

    monkeypatch.setitem(library.LIBRARY, LibraryPrimitive.RECONCILE_BLOCK, fake_reconcile)

    entry = WorkflowAction(kind="action", name="reconcile-block", inputs=[], outputs=[])
    out = await _run_action_primitive(
        None,  # pool — the fake ignores it
        entry,
        {"mask_idx": 77},
        tmp_path,
        {"kind": "block", "block_idx": 42},
        work_ticket_idx=9,
        hmac_secret=b"sekret",
        data_plane_url="grpc://dp:50051",
    )
    assert out == {}
    assert recorded == {
        "block_idx": 42,
        "mask_idx": 77,
        "hmac_secret": b"sekret",
        "data_plane_url": "grpc://dp:50051",
    }


async def test_run_action_primitive_reconcile_block_rejects_non_block_scope(tmp_path):
    """reconcile-block is only meaningful for a block-scoped ticket; a mis-scoped
    workflow YAML is a contract error, surfaced loudly."""
    from qiita_common.actions import WorkflowAction

    from qiita_control_plane.runner import _run_action_primitive

    entry = WorkflowAction(kind="action", name="reconcile-block", inputs=[], outputs=[])
    with pytest.raises(RuntimeError, match="block-scoped"):
        await _run_action_primitive(
            None,
            entry,
            {"mask_idx": 1},
            tmp_path,
            {"kind": "prep_sample", "prep_sample_idx": 5},
            work_ticket_idx=1,
            hmac_secret=b"x",
            data_plane_url="grpc://x",
        )


async def test_run_action_primitive_delete_block_mask_dispatches(monkeypatch, tmp_path):
    """The delete-block-mask arm calls the DELETE_READ_MASK_BLOCK primitive with
    block_idx from the scope target and mask_idx from the runner-bound `bound`,
    plus the hmac_secret / data_plane_url for the footprint delete DoAction."""
    from qiita_common.actions import WorkflowAction
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import library
    from qiita_control_plane.runner import _run_action_primitive

    recorded: dict = {}

    async def fake_delete(pool, *, block_idx, mask_idx, hmac_secret, data_plane_url):
        recorded.update(
            block_idx=block_idx,
            mask_idx=mask_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {"block_idx": block_idx, "rows_deleted": 0}

    monkeypatch.setitem(library.LIBRARY, LibraryPrimitive.DELETE_READ_MASK_BLOCK, fake_delete)

    entry = WorkflowAction(kind="action", name="delete-block-mask", inputs=[], outputs=[])
    out = await _run_action_primitive(
        None,  # pool — the fake ignores it
        entry,
        {"mask_idx": 77},
        tmp_path,
        {"kind": "block", "block_idx": 42},
        work_ticket_idx=9,
        hmac_secret=b"sekret",
        data_plane_url="grpc://dp:50051",
    )
    assert out == {}
    assert recorded == {
        "block_idx": 42,
        "mask_idx": 77,
        "hmac_secret": b"sekret",
        "data_plane_url": "grpc://dp:50051",
    }


async def test_run_action_primitive_delete_block_mask_rejects_non_block_scope(tmp_path):
    """delete-block-mask is only meaningful for a block-scoped ticket; a mis-scoped
    workflow YAML is a contract error, surfaced loudly."""
    from qiita_common.actions import WorkflowAction

    from qiita_control_plane.runner import _run_action_primitive

    entry = WorkflowAction(kind="action", name="delete-block-mask", inputs=[], outputs=[])
    with pytest.raises(RuntimeError, match="block-scoped"):
        await _run_action_primitive(
            None,
            entry,
            {"mask_idx": 1},
            tmp_path,
            {"kind": "prep_sample", "prep_sample_idx": 5},
            work_ticket_idx=1,
            hmac_secret=b"x",
            data_plane_url="grpc://x",
        )


# =============================================================================
# WorkflowEntry.when (conditional gate) + WorkflowStep.params (scalar params)
# =============================================================================
#
# Mirrors the host-reference-add shape: two gated native build steps (each
# carrying its own `params`) and a gated register-index per builder. Lets one
# submission build rype-only / minimap2-only / both via action_context, and
# tune the rype `w` / minimap2 `preset` the builders receive.

_INDEX_SELECT_STEPS = [
    {
        "kind": "step",
        "name": "build_rype_index",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.build_rype_index",
        "inputs": [],
        "params": {"rype_w": "w"},
        "when": "build_rype",
        "outputs": ["rype_index_meta"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "step",
        "name": "build_minimap2_index",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.build_minimap2_index",
        "inputs": [],
        "params": {"minimap2_preset": "preset"},
        "when": "build_minimap2",
        "outputs": ["minimap2_index_meta"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "action",
        "name": "register-index",
        "inputs": ["rype_index_meta"],
        "when": "build_rype",
        "outputs": [],
    },
    {
        "kind": "action",
        "name": "register-index",
        "inputs": ["minimap2_index_meta"],
        "when": "build_minimap2",
        "outputs": [],
    },
]


async def _make_index_select_ticket(pool, reference_idx, action_context: dict) -> tuple[int, str]:
    """Insert an index-selection action (the _INDEX_SELECT_STEPS shape) plus a
    pending work_ticket carrying `action_context`. Returns (work_ticket_idx,
    version) for cleanup."""
    version = f"index-select-{uuid.uuid4()}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps, cpu_ceiling, mem_ceiling_gb, walltime_ceiling,"
        "  success_status, failure_status"
        ") VALUES ('host-reference-add', $1, 'reference', $2::text[], $3::jsonb,"
        "  '{}'::jsonb, $4::jsonb, 4, 32, '1 hour', NULL, 'failed')",
        version,
        ["feature:mint", "reference:write", "reference:register_files"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps(_INDEX_SELECT_STEPS),
    )
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ('host-reference-add', $1, 1, 'reference', $2, $3::jsonb)"
        " RETURNING work_ticket_idx",
        version,
        reference_idx,
        json.dumps(action_context),
    )
    return wt_idx, version


@pytest.fixture
def register_index_spy(monkeypatch):
    """Record every REGISTER_INDEX primitive call (index_type, params) without
    a DB write, so a test can assert which index registrations fired."""
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import library as _lib

    calls: list[tuple[str, dict]] = []

    async def register_index(pool, *, reference_idx, index_type, fs_path, params):
        calls.append((index_type, params))

    monkeypatch.setitem(_lib.LIBRARY, LibraryPrimitive.REGISTER_INDEX, register_index)
    return calls


def _write_index_meta(path: Path, index_type: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"index_type": index_type, "fs_path": f"/srv/{index_type}", "params": {}})
    )


async def test_when_gate_skips_disabled_build_and_param_merges(
    postgres_pool, reference_idx, register_index_spy, tmp_path
):
    """`build_minimap2: false` in action_context skips the minimap2 builder AND
    its register-index (the `when:` gate fires for both step and action
    entries), while the rype builder runs and receives its `rype_w` -> `w`
    param merged into the native step inputs."""
    backend = FakeBackendClient()
    rype_meta = tmp_path / "rype_index_meta.json"
    _write_index_meta(rype_meta, "rype")
    backend.outputs_for["build_rype_index"] = {"rype_index_meta": rype_meta}

    wt_idx, version = await _make_index_select_ticket(
        postgres_pool, reference_idx, {"build_minimap2": False, "rype_w": 35}
    )
    try:
        await _run(wt_idx, postgres_pool, backend, tmp_path)

        # Only the rype builder was dispatched — minimap2 was gated off.
        step_calls = [name for name, *_ in backend.calls]
        assert step_calls == ["build_rype_index"]

        # rype_w (35) reached the native step as `w`, merged un-Path-coerced
        # (sent as a string the job's Inputs model re-coerces to int).
        _name, inputs, _ws, _scope = backend.calls[0]
        assert inputs == {"w": "35"}

        # Only the rype register-index fired; the minimap2 one was skipped.
        assert register_index_spy == [("rype", {})]

        # No progress row for the skipped step.
        rows = await step_progress.load_step_progress(postgres_pool, wt_idx)
        assert all(r.step_name != "build_minimap2_index" for r in rows)

        state = await postgres_pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )
        assert state == "completed"
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = 'host-reference-add' AND version = $1",
            version,
        )


async def test_when_gate_runs_both_when_flags_absent(
    postgres_pool, reference_idx, register_index_spy, tmp_path
):
    """An empty action_context (no selection flags) builds BOTH indexes — the
    `when:` gate defaults ON, so today's behavior is preserved. Omitted params
    are not forwarded (the builders fall back to their Inputs defaults)."""
    backend = FakeBackendClient()
    rype_meta = tmp_path / "rype_index_meta.json"
    mm2_meta = tmp_path / "minimap2_index_meta.json"
    _write_index_meta(rype_meta, "rype")
    _write_index_meta(mm2_meta, "minimap2")
    backend.outputs_for["build_rype_index"] = {"rype_index_meta": rype_meta}
    backend.outputs_for["build_minimap2_index"] = {"minimap2_index_meta": mm2_meta}

    wt_idx, version = await _make_index_select_ticket(postgres_pool, reference_idx, {})
    try:
        await _run(wt_idx, postgres_pool, backend, tmp_path)

        step_calls = [name for name, *_ in backend.calls]
        assert step_calls == ["build_rype_index", "build_minimap2_index"]
        # No param keys present in action_context -> none merged.
        assert backend.calls[0][1] == {}
        assert backend.calls[1][1] == {}
        assert {t for t, _ in register_index_spy} == {"rype", "minimap2"}
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = 'host-reference-add' AND version = $1",
            version,
        )


# =============================================================================
# _resolve_reference_index_path
# =============================================================================
#
# The on-disk path a future host-filter compute job is injected with: the
# newest generation of a given index_type for an ACTIVE reference. Built and
# tested now (the host-filter *processing* workflow itself is out of scope) so
# the resolution contract — newest-generation selection + active-status gate —
# is locked against the reference_index table the host-reference-add workflow
# populates.


async def _insert_reference_index(pool, reference_idx, fs_path, *, index_type="rype"):
    return await pool.fetchval(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
        " VALUES ($1, $2, $3, $4::jsonb) RETURNING reference_index_idx",
        reference_idx,
        index_type,
        fs_path,
        json.dumps({"k": 64, "w": 25}),
    )


async def test_resolve_reference_index_path_returns_latest(postgres_pool, reference_idx):
    """When a reference is active and has multiple index generations of the
    same type, the newest (highest reference_index_idx) fs_path is returned —
    the "grow a reference appends a generation" path the table's lack of a
    UNIQUE(reference_idx, index_type) deliberately allows."""
    from qiita_control_plane.runner import _resolve_reference_index_path

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/qiita/old.ryxdi")
    newest = "/srv/qiita/new.ryxdi"
    await _insert_reference_index(postgres_pool, reference_idx, newest)
    try:
        resolved = await _resolve_reference_index_path(postgres_pool, reference_idx, "rype")
        assert resolved == newest
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_reference_index_path_raises_when_reference_absent(postgres_pool):
    from qiita_control_plane.actions.reference import ReferenceNotFound
    from qiita_control_plane.runner import _resolve_reference_index_path

    with pytest.raises(ReferenceNotFound):
        await _resolve_reference_index_path(postgres_pool, 999_999_999, "rype")


async def test_resolve_reference_index_path_raises_when_not_active(postgres_pool, reference_idx):
    """A reference still in `indexing` (or any non-active state) must not have
    its index served — the build may be mid-flight or have failed."""
    from qiita_control_plane.runner import _resolve_reference_index_path

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'indexing' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/qiita/x.ryxdi")
    try:
        with pytest.raises(ValueError, match="active"):
            await _resolve_reference_index_path(postgres_pool, reference_idx, "rype")
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_reference_index_path_raises_when_no_index(postgres_pool, reference_idx):
    """Active reference, but no index of the requested type built yet."""
    from qiita_control_plane.runner import _resolve_reference_index_path

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    with pytest.raises(ValueError, match="no 'rype' index"):
        await _resolve_reference_index_path(postgres_pool, reference_idx, "rype")


# =============================================================================
# _resolve_host_filter_indexes — legacy single-reference (fastq-to-parquet/1.1.0)
# =============================================================================
#
# `host_reference_idx` names ONE reference; whichever of its rype/minimap2
# indexes exist are bound (>=1 required, a missing one skips that stage). The
# two-reference layout (1.2.0) has its own section below.


async def test_resolve_host_filter_indexes_binds_both_when_enabled(postgres_pool, reference_idx):
    """Enabled + an ACTIVE host reference with both indexes → host_rype_path and
    host_minimap2_path bound to the newest generation of each."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/m.mmi", index_type="minimap2")
    try:
        out = await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={"host_filter_enabled": True, "host_reference_idx": reference_idx},
        )
        assert {k: str(v) for k, v in out.items()} == {
            "host_rype_path": "/srv/r.ryxdi",
            "host_minimap2_path": "/srv/m.mmi",
        }
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_host_filter_indexes_disabled_returns_empty(postgres_pool, reference_idx):
    """Flag false or absent → {} (host_filter runs as a pass-through). No DB
    lookup is even attempted."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    assert (
        await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={"host_filter_enabled": False, "host_reference_idx": reference_idx},
        )
        == {}
    )
    assert await _resolve_host_filter_indexes(postgres_pool, action_context={}) == {}


async def test_resolve_host_filter_indexes_enabled_requires_a_layout(postgres_pool):
    """Enabled but NO reference key at all → SUBMISSION BAD_INPUT naming BOTH
    layouts (a legacy caller who dropped host_reference_idx must not be pointed at
    host_rype_reference_idx, a key they never set)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    with pytest.raises(BackendFailure) as ei:
        await _resolve_host_filter_indexes(
            postgres_pool, action_context={"host_filter_enabled": True}
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
    assert ei.value.step_name is None
    assert "host_reference_idx" in ei.value.reason
    assert "host_rype_reference_idx" in ei.value.reason


async def test_resolve_host_filter_indexes_enabled_requires_reference_idx(postgres_pool):
    """Legacy layout, non-positive / wrong-typed host_reference_idx → SUBMISSION
    BAD_INPUT naming that field."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    for ctx in (
        {"host_filter_enabled": True, "host_reference_idx": 0},  # non-positive
        {"host_filter_enabled": True, "host_reference_idx": True},  # bool, not int
    ):
        with pytest.raises(BackendFailure) as ei:
            await _resolve_host_filter_indexes(postgres_pool, action_context=ctx)
        assert ei.value.kind == FailureKind.BAD_INPUT
        assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
        assert ei.value.step_name is None
        assert "host_reference_idx" in ei.value.reason


async def test_resolve_host_filter_indexes_unknown_reference(postgres_pool):
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    with pytest.raises(BackendFailure) as ei:
        await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={"host_filter_enabled": True, "host_reference_idx": 999_999_999},
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert ei.value.stage == WorkTicketFailureStage.SUBMISSION


async def test_resolve_host_filter_indexes_non_active_reference(postgres_pool, reference_idx):
    """A host reference still `indexing` (build mid-flight) must not be served."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'indexing' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/m.mmi", index_type="minimap2")
    try:
        with pytest.raises(BackendFailure) as ei:
            await _resolve_host_filter_indexes(
                postgres_pool,
                action_context={"host_filter_enabled": True, "host_reference_idx": reference_idx},
            )
        assert ei.value.kind == FailureKind.BAD_INPUT
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_host_filter_indexes_rype_only(postgres_pool, reference_idx):
    """A rype-only host reference (built --no-minimap2-index): bind only
    host_rype_path; the minimap2 stage is skipped (its path stays unbound, so
    host_filter's Inputs default it to None)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    try:
        out = await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={"host_filter_enabled": True, "host_reference_idx": reference_idx},
        )
        assert {k: str(v) for k, v in out.items()} == {"host_rype_path": "/srv/r.ryxdi"}
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_host_filter_indexes_minimap2_only(postgres_pool, reference_idx):
    """Symmetric: a minimap2-only host reference (built --no-rype-index) binds
    only host_minimap2_path. rype is resolved first, so this also proves a
    missing rype doesn't abort before the minimap2 lookup runs."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/m.mmi", index_type="minimap2")
    try:
        out = await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={"host_filter_enabled": True, "host_reference_idx": reference_idx},
        )
        assert {k: str(v) for k, v in out.items()} == {"host_minimap2_path": "/srv/m.mmi"}
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_host_filter_indexes_neither_index(postgres_pool, reference_idx):
    """An active host reference with NEITHER index can't filter anything →
    BAD_INPUT (the one remaining hard error among the single-index cases)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    with pytest.raises(BackendFailure) as ei:
        await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={"host_filter_enabled": True, "host_reference_idx": reference_idx},
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
    assert "neither" in ei.value.reason


# =============================================================================
# _resolve_host_filter_indexes — two-reference (fastq-to-parquet/1.2.0)
# =============================================================================
#
# An independent reference per tool: host_rype_reference_idx (REQUIRED) for the
# rype .ryxdi, host_minimap2_reference_idx (OPTIONAL) for the minimap2 .mmi. A
# designated reference MUST be active and MUST carry its named index — a missing
# index is a hard error here, NOT a skipped stage as in the legacy layout.


async def test_resolve_host_filter_two_reference_rype_only(postgres_pool, reference_idx):
    """rype reference only (minimap2 omitted) → just host_rype_path bound."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    try:
        out = await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={
                "host_filter_enabled": True,
                "host_rype_reference_idx": reference_idx,
            },
        )
        assert {k: str(v) for k, v in out.items()} == {"host_rype_path": "/srv/r.ryxdi"}
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_host_filter_two_reference_rype_and_minimap2(
    postgres_pool, reference_idx, second_reference_idx
):
    """Two distinct references: rype from one, minimap2 from the other — each
    index resolved from its OWN reference."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    for idx in (reference_idx, second_reference_idx):
        await postgres_pool.execute(
            "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", idx
        )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    await _insert_reference_index(
        postgres_pool, second_reference_idx, "/srv/m.mmi", index_type="minimap2"
    )
    try:
        out = await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={
                "host_filter_enabled": True,
                "host_rype_reference_idx": reference_idx,
                "host_minimap2_reference_idx": second_reference_idx,
            },
        )
        assert {k: str(v) for k, v in out.items()} == {
            "host_rype_path": "/srv/r.ryxdi",
            "host_minimap2_path": "/srv/m.mmi",
        }
    finally:
        for idx in (reference_idx, second_reference_idx):
            await postgres_pool.execute(
                "DELETE FROM qiita.reference_index WHERE reference_idx = $1", idx
            )


async def test_resolve_host_filter_two_reference_requires_rype(postgres_pool, second_reference_idx):
    """minimap2 set but rype absent → BAD_INPUT (rype is REQUIRED in this layout,
    unlike the legacy >=1-of-either rule)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1",
        second_reference_idx,
    )
    await _insert_reference_index(
        postgres_pool, second_reference_idx, "/srv/m.mmi", index_type="minimap2"
    )
    try:
        with pytest.raises(BackendFailure) as ei:
            await _resolve_host_filter_indexes(
                postgres_pool,
                action_context={
                    "host_filter_enabled": True,
                    "host_minimap2_reference_idx": second_reference_idx,
                },
            )
        assert ei.value.kind == FailureKind.BAD_INPUT
        assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
        assert "host_rype_reference_idx" in ei.value.reason
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", second_reference_idx
        )


async def test_resolve_host_filter_two_reference_rype_missing_its_index(
    postgres_pool, reference_idx
):
    """The designated rype reference is active but carries NO rype index → hard
    BAD_INPUT (a missing index on a designated reference is fatal here, not a
    skipped stage)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    # Only a minimap2 index exists on this reference, but it's named as the rype
    # reference → the rype lookup must fail rather than silently skip.
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/m.mmi", index_type="minimap2")
    try:
        with pytest.raises(BackendFailure) as ei:
            await _resolve_host_filter_indexes(
                postgres_pool,
                action_context={
                    "host_filter_enabled": True,
                    "host_rype_reference_idx": reference_idx,
                },
            )
        assert ei.value.kind == FailureKind.BAD_INPUT
        assert "rype" in ei.value.reason
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_host_filter_two_reference_minimap2_missing_its_index(
    postgres_pool, reference_idx, second_reference_idx
):
    """The designated minimap2 reference is active but carries NO minimap2 index →
    hard BAD_INPUT (symmetric to the rype case)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    for idx in (reference_idx, second_reference_idx):
        await postgres_pool.execute(
            "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", idx
        )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    # second_reference_idx has no minimap2 index built.
    try:
        with pytest.raises(BackendFailure) as ei:
            await _resolve_host_filter_indexes(
                postgres_pool,
                action_context={
                    "host_filter_enabled": True,
                    "host_rype_reference_idx": reference_idx,
                    "host_minimap2_reference_idx": second_reference_idx,
                },
            )
        assert ei.value.kind == FailureKind.BAD_INPUT
        assert "minimap2" in ei.value.reason
    finally:
        for idx in (reference_idx, second_reference_idx):
            await postgres_pool.execute(
                "DELETE FROM qiita.reference_index WHERE reference_idx = $1", idx
            )


async def test_resolve_host_filter_two_reference_unknown_rype_reference(postgres_pool):
    """An unknown host_rype_reference_idx → BAD_INPUT naming the field."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    with pytest.raises(BackendFailure) as ei:
        await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={
                "host_filter_enabled": True,
                "host_rype_reference_idx": 999_999_999,
            },
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert "host_rype_reference_idx" in ei.value.reason


async def test_resolve_host_filter_two_reference_rejects_mixed_layouts(
    postgres_pool, reference_idx
):
    """Supplying BOTH the legacy host_reference_idx and a two-reference key is a
    contract error → BAD_INPUT (no silent precedence)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    with pytest.raises(BackendFailure) as ei:
        await _resolve_host_filter_indexes(
            postgres_pool,
            action_context={
                "host_filter_enabled": True,
                "host_reference_idx": reference_idx,
                "host_rype_reference_idx": reference_idx,
            },
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert ei.value.stage == WorkTicketFailureStage.SUBMISSION


async def test_resolve_host_filter_two_reference_minimap2_must_be_positive(
    postgres_pool, reference_idx
):
    """A present-but-invalid host_minimap2_reference_idx (0 / bool) → BAD_INPUT,
    even though minimap2 is otherwise optional."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    try:
        for bad in (0, True):
            with pytest.raises(BackendFailure) as ei:
                await _resolve_host_filter_indexes(
                    postgres_pool,
                    action_context={
                        "host_filter_enabled": True,
                        "host_rype_reference_idx": reference_idx,
                        "host_minimap2_reference_idx": bad,
                    },
                )
            assert ei.value.kind == FailureKind.BAD_INPUT
            assert "host_minimap2_reference_idx" in ei.value.reason
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


async def test_resolve_host_filter_two_reference_rype_must_be_positive(postgres_pool):
    """A non-positive / wrong-typed host_rype_reference_idx (the REQUIRED field) →
    BAD_INPUT naming it — symmetric with the minimap2 validation."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    for bad in (0, True):
        with pytest.raises(BackendFailure) as ei:
            await _resolve_host_filter_indexes(
                postgres_pool,
                action_context={"host_filter_enabled": True, "host_rype_reference_idx": bad},
            )
        assert ei.value.kind == FailureKind.BAD_INPUT
        assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
        assert "host_rype_reference_idx" in ei.value.reason


async def test_resolve_host_filter_two_reference_non_active_rype(postgres_pool, reference_idx):
    """A reference still `indexing` (build mid-flight) designated as the rype
    reference must not be served → BAD_INPUT (the two-reference path maps the
    non-active ValueError to a SUBMISSION failure)."""
    from qiita_control_plane.runner import _resolve_host_filter_indexes

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'indexing' WHERE reference_idx = $1", reference_idx
    )
    await _insert_reference_index(postgres_pool, reference_idx, "/srv/r.ryxdi", index_type="rype")
    try:
        with pytest.raises(BackendFailure) as ei:
            await _resolve_host_filter_indexes(
                postgres_pool,
                action_context={
                    "host_filter_enabled": True,
                    "host_rype_reference_idx": reference_idx,
                },
            )
        assert ei.value.kind == FailureKind.BAD_INPUT
        assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )


# =============================================================================
# _resolve_qc_adapters (QC adapter-set materialization)
# =============================================================================


async def _make_adapter_reference(pool, reference_idx) -> None:
    """Turn the test reference into an ACTIVE artifact_sequence_set."""
    await pool.execute(
        "UPDATE qiita.reference SET kind = 'artifact_sequence_set', status = 'active'"
        " WHERE reference_idx = $1",
        reference_idx,
    )


async def test_resolve_qc_adapters_writes_parquet(
    postgres_pool, reference_idx, tmp_path, monkeypatch
):
    """Active artifact_sequence_set + DoGet'd chunks → adapters.parquet in the
    workspace, with chunks reassembled in chunk_index order per feature. The
    Parquet has (feature_idx, sequence) rows sorted by feature_idx."""
    import duckdb

    from qiita_control_plane import runner

    await _make_adapter_reference(postgres_pool, reference_idx)
    # Out-of-order chunks + a two-chunk feature, to pin the ORDER BY chunk_index
    # reassembly and the per-feature record split.
    monkeypatch.setattr(
        runner,
        "_do_get_reference_sequence_chunks",
        lambda _url, _ticket: [(7, 1, "GGGG"), (7, 0, "AGAT"), (9, 0, "CTGTCTC")],
    )
    out = await runner._resolve_qc_adapters(
        postgres_pool,
        default_adapter_reference_idx=reference_idx,
        data_plane_url="grpc://unused",
        hmac_secret=b"x" * 32,
        workspace=tmp_path,
    )
    adapter_parquet = tmp_path / "adapters.parquet"
    assert out == {"adapter_parquet": adapter_parquet}
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT feature_idx, sequence FROM read_parquet('{adapter_parquet}') "
            "ORDER BY feature_idx"
        ).fetchall()
    assert rows == [(7, "AGATGGGG"), (9, "CTGTCTC")]


async def test_resolve_qc_adapters_unconfigured(postgres_pool, tmp_path):
    """No configured default → SUBMISSION BAD_INPUT (no DB lookup needed)."""
    from qiita_control_plane import runner

    with pytest.raises(BackendFailure) as ei:
        await runner._resolve_qc_adapters(
            postgres_pool,
            default_adapter_reference_idx=None,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 32,
            workspace=tmp_path,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
    assert "QIITA_DEFAULT_ADAPTER_REFERENCE_IDX" in ei.value.reason


async def test_resolve_qc_adapters_unknown_reference(postgres_pool, tmp_path):
    from qiita_control_plane import runner

    with pytest.raises(BackendFailure) as ei:
        await runner._resolve_qc_adapters(
            postgres_pool,
            default_adapter_reference_idx=999_999_999,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 32,
            workspace=tmp_path,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT


async def test_resolve_qc_adapters_wrong_kind(postgres_pool, reference_idx, tmp_path):
    """A non-artifact_sequence_set reference is rejected — fail fast rather than
    DoGet a (possibly huge) sequence_reference as 'adapters'."""
    from qiita_control_plane import runner

    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'active' WHERE reference_idx = $1", reference_idx
    )  # leaves kind = 'sequence_reference'
    with pytest.raises(BackendFailure) as ei:
        await runner._resolve_qc_adapters(
            postgres_pool,
            default_adapter_reference_idx=reference_idx,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 32,
            workspace=tmp_path,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert "artifact_sequence_set" in ei.value.reason


async def test_resolve_qc_adapters_non_active(postgres_pool, reference_idx, tmp_path):
    from qiita_control_plane import runner

    await postgres_pool.execute(
        "UPDATE qiita.reference SET kind = 'artifact_sequence_set', status = 'loading'"
        " WHERE reference_idx = $1",
        reference_idx,
    )
    with pytest.raises(BackendFailure) as ei:
        await runner._resolve_qc_adapters(
            postgres_pool,
            default_adapter_reference_idx=reference_idx,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 32,
            workspace=tmp_path,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert "active" in ei.value.reason


async def test_resolve_qc_adapters_empty_set(postgres_pool, reference_idx, tmp_path, monkeypatch):
    """An adapter reference that DoGets zero sequences is a misconfiguration →
    BAD_INPUT, and no partial adapters.parquet is left behind."""
    from qiita_control_plane import runner

    await _make_adapter_reference(postgres_pool, reference_idx)
    monkeypatch.setattr(runner, "_do_get_reference_sequence_chunks", lambda _url, _t: [])
    with pytest.raises(BackendFailure) as ei:
        await runner._resolve_qc_adapters(
            postgres_pool,
            default_adapter_reference_idx=reference_idx,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 32,
            workspace=tmp_path,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert not (tmp_path / "adapters.parquet").exists()


async def test_resolve_qc_adapters_dataplane_failure_is_submission_failure(
    postgres_pool, reference_idx, tmp_path, monkeypatch
):
    """A Flight DoGet failure (data plane unreachable/errored) is wrapped as a
    SUBMISSION BackendFailure — never allowed to escape as an untyped exception
    (which run_workflow's bare handler would mis-record as STEP_RUN/step_name=None,
    violating the failure-step-name CHECK and stranding the ticket)."""
    from qiita_control_plane import runner

    await _make_adapter_reference(postgres_pool, reference_idx)

    def _boom(_url, _ticket):
        raise RuntimeError("Flight: connection refused")

    monkeypatch.setattr(runner, "_do_get_reference_sequence_chunks", _boom)
    with pytest.raises(BackendFailure) as ei:
        await runner._resolve_qc_adapters(
            postgres_pool,
            default_adapter_reference_idx=reference_idx,
            data_plane_url="grpc://unused",
            hmac_secret=b"x" * 32,
            workspace=tmp_path,
        )
    assert ei.value.kind == FailureKind.BAD_INPUT
    assert ei.value.stage == WorkTicketFailureStage.SUBMISSION
    assert ei.value.step_name is None
    assert "data plane" in ei.value.reason


async def test_workflow_needs_adapters_detects_adapter_input():
    """The gate fires only when an entry declares adapter_parquet as an input."""
    from types import SimpleNamespace

    from qiita_control_plane.runner import _workflow_needs_adapters

    no_qc = [SimpleNamespace(inputs=["reads"], optional_inputs=["host_rype_path"])]
    with_qc = [SimpleNamespace(inputs=["reads", "adapter_parquet"], optional_inputs=[])]
    assert _workflow_needs_adapters(no_qc) is False
    assert _workflow_needs_adapters(with_qc) is True


# =============================================================================
# fastq-to-parquet/1.2.0 step wiring (fastq -> qc -> host_filter)
# =============================================================================
#
# The 1.2.0 chain inserts an always-on `qc` step between fastq and host_filter.
# Each stage re-emits the `reads` binding it consumes (a transform in place), so
# host_filter is identical to 1.1.0 and consumes qc's QC'd `reads`. The qc step
# takes the runner-materialized `adapter_parquet` as a PATH input and the sequencing
# `instrument_model` as a scalar `params` value.

_FASTQ_TO_PARQUET_V12_STEPS = [
    {
        "kind": "step",
        "name": "fastq",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.fastq_to_parquet",
        "inputs": ["fastq_path"],
        "optional_inputs": ["reverse_fastq_path"],
        "outputs": ["reads"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "step",
        "name": "qc",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.qc",
        "inputs": ["reads", "adapter_parquet"],
        "params": {"instrument_model": "instrument_model"},
        "outputs": ["reads"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "step",
        "name": "host_filter",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.host_filter",
        "inputs": ["reads"],
        "optional_inputs": ["host_rype_path", "host_minimap2_path"],
        "outputs": ["filtered_reads"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
]


async def _make_v12_ticket(
    pool, prep_sample_idx: int, principal_idx: int, action_context: dict
) -> tuple[int, str]:
    """Insert a fastq-to-parquet/1.2.0-shaped action (the _FASTQ_TO_PARQUET_V12_STEPS
    shape) + a pending prep_sample-scoped work_ticket. Returns (work_ticket_idx,
    version) for cleanup."""
    version = f"fastq-to-parquet-v12-{uuid.uuid4()}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps, cpu_ceiling, mem_ceiling_gb, walltime_ceiling,"
        "  success_status, failure_status"
        ") VALUES ('fastq-to-parquet', $1, 'prep_sample', $2::text[], $3::jsonb,"
        "  '{}'::jsonb, $4::jsonb, 8, 16, '4 hours', NULL, 'failed')",
        version,
        [],
        json.dumps({"service": False, "human_roles": ["user"]}),
        json.dumps(_FASTQ_TO_PARQUET_V12_STEPS),
    )
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context"
        ") VALUES ('fastq-to-parquet', $1, $2, 'prep_sample', $3, $4::jsonb)"
        " RETURNING work_ticket_idx",
        version,
        principal_idx,
        prep_sample_idx,
        json.dumps(action_context),
    )
    return wt_idx, version


async def test_fastq_to_parquet_v12_qc_binds_adapter_and_instrument_model(
    postgres_pool, reference_idx, human_admin_session, tmp_path, monkeypatch
):
    """End-to-end 1.2.0 wiring: the runner dispatches fastq → qc → host_filter in
    order; qc receives `reads` (fastq's output) and the runner-materialized
    `adapter_parquet` as PATH inputs plus `instrument_model` via params; and
    host_filter consumes the `reads` binding qc re-emitted (the QC'd output, not
    fastq's raw reads)."""
    from qiita_control_plane import runner
    from qiita_control_plane.testing.db_seeds import seed_biosample_with_sequenced_prep_sample

    principal_idx = human_admin_session["principal_idx"]
    _bio_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )

    # Configured artifact_sequence_set adapter reference + a stubbed DoGet so the
    # pre-loop adapter materialization writes a real adapters.parquet.
    await _make_adapter_reference(postgres_pool, reference_idx)
    monkeypatch.setattr(
        runner,
        "_do_get_reference_sequence_chunks",
        lambda _url, _t: [(1, 0, "AGATCGGAAGAGC")],
    )

    backend = FakeBackendClient()
    backend.outputs_for["fastq"] = {"reads": tmp_path / "fastq_out" / "reads.parquet"}
    backend.outputs_for["qc"] = {"reads": tmp_path / "qc_out" / "qc_reads.parquet"}
    backend.outputs_for["host_filter"] = {
        "filtered_reads": tmp_path / "hf_out" / "filtered_reads.parquet"
    }

    # host_filter_enabled absent → host filtering off (no index paths bound).
    wt_idx, version = await _make_v12_ticket(
        postgres_pool,
        prep_sample_idx,
        principal_idx,
        {"fastq_path": "/data/sample.fastq", "instrument_model": "NextSeq 550"},
    )
    try:
        await _run(
            wt_idx,
            postgres_pool,
            backend,
            tmp_path / "ws",
            default_adapter_reference_idx=reference_idx,
        )

        # Step order.
        assert [name for name, *_ in backend.calls] == ["fastq", "qc", "host_filter"]

        # qc inputs: `reads` is fastq's output path; `adapter_parquet` is the
        # runner-materialized canonical set (a real file on disk); `instrument_model`
        # rides through `params` as a string (not Path-coerced).
        qc_inputs = next(inp for name, inp, *_ in backend.calls if name == "qc")
        assert qc_inputs["reads"] == backend.outputs_for["fastq"]["reads"]
        assert qc_inputs["adapter_parquet"].name == "adapters.parquet"
        assert qc_inputs["adapter_parquet"].exists()
        assert qc_inputs["instrument_model"] == "NextSeq 550"

        # host_filter consumes the `reads` binding qc re-emitted (the QC'd output),
        # NOT fastq's raw reads — proving the in-place transform chaining. Host
        # filtering is off, so no index paths are bound.
        hf_inputs = next(inp for name, inp, *_ in backend.calls if name == "host_filter")
        assert hf_inputs["reads"] == backend.outputs_for["qc"]["reads"]
        assert "host_rype_path" not in hf_inputs
        assert "host_minimap2_path" not in hf_inputs

        state = await postgres_pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )
        assert state == "completed"
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = 'fastq-to-parquet' AND version = $1",
            version,
        )
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", _bio_idx)


async def test_run_workflow_fails_ticket_on_host_filter_resolution_error(
    postgres_pool, reference_add_action, reference_idx, tmp_path
):
    """End-to-end wiring: run_workflow calls _resolve_host_filter_indexes INSIDE
    its try block, so an enabled host filter pointing at an unknown
    host_reference_idx FAILs the ticket (SUBMISSION / BAD_INPUT) rather than
    leaving it stuck in PROCESSING or running any step. Guards against the
    resolver being unwired or moved outside the try. (The disabled → {} path is
    exercised by every other run_workflow test, where the flag is absent.)"""
    action_id, version = reference_add_action
    fasta = tmp_path / "in.fasta"
    fasta.write_text(">s\nACGT\n")
    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps(
            {
                "fasta_path": str(fasta),
                "host_filter_enabled": True,
                "host_reference_idx": 999_999_999,
            }
        ),
    )
    try:
        backend = FakeBackendClient()  # no step should run; resolution fails first
        # run_workflow marks the ticket FAILED AND re-raises (same as any
        # submission-stage failure), so assert both the raise and the row.
        with pytest.raises(BackendFailure, match="999999999"):
            await _run(work_ticket_idx, postgres_pool, backend, tmp_path / "ws")

        row = await postgres_pool.fetchrow(
            "SELECT state, failure_type, failure_stage, failure_step_name, failure_reason"
            " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        assert row["state"] == "failed"
        # failure_type is the retry classification — BAD_INPUT is permanent.
        assert row["failure_type"] == "permanent"
        assert row["failure_stage"] == "submission"
        assert row["failure_step_name"] is None
        assert "999999999" in row["failure_reason"]
        # Resolution failed before the step loop — no step was dispatched.
        assert backend.calls == []
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )


# =============================================================================
# Decoupled SLURM-path dispatch (submit → poll → result)
# =============================================================================
#
# These exercise the asynchronous compute path the runner drives now: submit
# returns a handle immediately, the runner polls status_step until terminal,
# then fetches the verified result via result_step. No connection is held for
# the job's duration — the durable proof the 600s-timeout bug is gone.


class FakeSlurmBackendClient:
    """Async (SLURM-shaped) backend stub. `submit_step` returns a non-terminal
    handle carrying a job id; `status_step` replays `status_script` (each item
    a StepStatus to return, or a BackendFailure to raise — e.g. an
    ORCHESTRATOR_UNREACHABLE to simulate the CO being down mid-poll);
    `result_step` replays `result_script` (a `{output_name: filename}` dict to
    materialise + return, or a BackendFailure to raise — e.g. a job that ended
    FAILED). Drives the runner's poll loop without a real orchestrator."""

    def __init__(
        self,
        *,
        status_script: list,
        result_script: list,
        submit_unreachable_times: int = 0,
        slurm_job_id: int = 4242,
        found_jobs: list | None = None,
    ) -> None:
        self.status_script = list(status_script)
        self.result_script = list(result_script)
        self._submit_unreachable_times = submit_unreachable_times
        self._slurm_job_id = slurm_job_id
        # find-by-name: `found_jobs` is what the orphan-adoption lookup
        # returns (a list of FoundJobWire, or a BackendFailure to raise);
        # `find_by_name_calls` records the names looked up.
        self._found_jobs = found_jobs if found_jobs is not None else []
        self.find_by_name_calls: list[str] = []
        self.submit_calls = 0
        self.status_calls = 0
        self.result_calls = 0

    async def submit_step(
        self,
        *,
        step_name,
        inputs,
        workspace,
        scope_target,
        work_ticket_idx,
        attempt=0,
        container=None,
        module=None,
        entrypoint=None,
        baseline_resources=None,
    ):
        self.submit_calls += 1
        if self.submit_calls <= self._submit_unreachable_times:
            raise BackendFailure(
                kind=FailureKind.ORCHESTRATOR_UNREACHABLE,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason="orchestrator unreachable (simulated submit)",
            )
        return StepHandleWire(
            compute_target=ComputeTarget.SLURM,
            step_name=step_name,
            slurm_job_id=self._slurm_job_id,
            job_name=f"qiita-wt{work_ticket_idx}-{step_name}-a{attempt}",
            output_path=str(workspace / "output"),
            logs_path=str(workspace / "logs"),
        )

    async def status_step(self, handle):
        self.status_calls += 1
        item = self.status_script.pop(0) if self.status_script else StepStatus.COMPLETED
        if isinstance(item, BackendFailure):
            raise item
        return StepStatusWire(status=item, raw_state=item.value.upper())

    async def result_step(self, handle, status):
        self.result_calls += 1
        item = self.result_script.pop(0) if self.result_script else {}
        if isinstance(item, BackendFailure):
            raise item
        base = Path(handle.output_path)
        base.mkdir(parents=True, exist_ok=True)
        out = {}
        for name, filename in item.items():
            p = base / filename
            p.touch(exist_ok=True)
            out[name] = p
        return out

    async def find_jobs_by_name(self, job_name):
        self.find_by_name_calls.append(job_name)
        if isinstance(self._found_jobs, BackendFailure):
            raise self._found_jobs
        return list(self._found_jobs)


_SINGLE_STEP_WORKFLOW = [
    {
        "kind": "step",
        "name": "compute",
        "step_type": "singleton",
        "container": REFERENCE_HASH_CONTAINER,
        "inputs": ["fasta_path"],
        "outputs": ["result"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
]


@pytest.fixture
async def slurm_action(postgres_pool):
    """A minimal one-`step:` action with no success/failure status PATCH —
    isolates the compute poll loop from reference-status transitions."""
    action_id = "slurm-single-step"
    version = f"runner-test-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps, cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "          $5::jsonb, $6::jsonb, 1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps(_SINGLE_STEP_WORKFLOW),
    )
    yield action_id, version
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )


@pytest.fixture
async def slurm_ticket(postgres_pool, slurm_action, reference_idx, tmp_path):
    action_id, version = slurm_action
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">seq1\nACGT\n")
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps({"fasta_path": str(fasta)}),
    )
    yield idx
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)


async def _progress_rows(pool, work_ticket_idx):
    return await step_progress.load_step_progress(pool, work_ticket_idx)


async def test_long_running_step_completes_without_timeout(postgres_pool, slurm_ticket, tmp_path):
    """The headline 600s-fix proof: a job observed RUNNING across dozens of
    status polls still completes. Impossible under the old held-connection
    model (capped at a 600s client timeout)."""
    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.PENDING] + [StepStatus.RUNNING] * 30 + [StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws")

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    # 32 polls (1 pending + 30 running + 1 completed) — far past any single
    # held-connection call could survive.
    assert backend.status_calls == 32
    assert backend.submit_calls == 1
    rows = await _progress_rows(postgres_pool, slurm_ticket)
    assert len(rows) == 1
    assert rows[0].state is StepProgressState.COMPLETED
    assert rows[0].compute_target is ComputeTarget.SLURM
    assert rows[0].slurm_job_id == 4242


async def test_co_unreachable_mid_poll_keeps_polling(postgres_pool, slurm_ticket, tmp_path):
    """status_step raising ORCHESTRATOR_UNREACHABLE (CO down) must NOT fail
    the ticket — the runner keeps polling and completes when CO returns."""
    unreachable = BackendFailure(
        kind=FailureKind.ORCHESTRATOR_UNREACHABLE,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="compute",
        reason="co down (simulated poll)",
    )
    backend = FakeSlurmBackendClient(
        status_script=[unreachable] * 5 + [StepStatus.RUNNING, StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws")

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        slurm_ticket,
    )
    assert row["state"] == "completed"  # NOT failed
    assert row["failure_type"] is None
    # All 5 unreachable polls happened, then RUNNING + COMPLETED.
    assert backend.status_calls == 7


async def test_co_unreachable_during_submit_eventually_submits(
    postgres_pool, slurm_ticket, tmp_path
):
    """submit_step raising ORCHESTRATOR_UNREACHABLE is retried in place until
    the orchestrator returns — the ticket never fails on a CO outage."""
    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
        submit_unreachable_times=3,
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws")

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    # 3 unreachable submits + 1 that landed.
    assert backend.submit_calls == 4


async def test_idempotent_adopt_does_not_resubmit(postgres_pool, slurm_ticket, tmp_path):
    """If a job is already recorded for this (idx, step, attempt) — e.g. a
    restart resuming the attempt — the runner adopts it (resumes polling)
    instead of submitting a duplicate."""
    # Pre-seed the progress row as already submitted with a job id.
    await step_progress.record_submitting(
        postgres_pool,
        work_ticket_idx=slurm_ticket,
        step_index=0,
        attempt=0,
        step_name="compute",
        compute_target=ComputeTarget.SLURM,
        job_name=f"qiita-wt{slurm_ticket}-compute-a0",
    )
    await step_progress.record_submitted(
        postgres_pool, work_ticket_idx=slurm_ticket, step_index=0, attempt=0, slurm_job_id=777
    )
    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.RUNNING, StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws")

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    # Adopted the existing job — no submit at all.
    assert backend.submit_calls == 0
    assert backend.status_calls == 2
    rows = await _progress_rows(postgres_pool, slurm_ticket)
    assert rows[0].slurm_job_id == 777  # the pre-seeded job, untouched
    assert rows[0].state is StepProgressState.COMPLETED


async def test_job_failure_records_failed_then_retries_new_attempt(
    postgres_pool, slurm_ticket, tmp_path
):
    """A SLURM job that ends FAILED with a transient kind (NODE_FAIL) marks
    this attempt's progress row failed, then the runner retries as a NEW
    attempt that succeeds. Two attempt rows result; the ticket completes."""
    node_fail = BackendFailure(
        kind=FailureKind.NODE_FAIL,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="compute",
        reason="node died (simulated)",
    )
    backend = FakeSlurmBackendClient(
        # attempt 0: poll → FAILED, result raises NODE_FAIL.
        # attempt 1: poll → COMPLETED, result returns outputs.
        status_script=[StepStatus.FAILED, StepStatus.COMPLETED],
        result_script=[node_fail, {"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws")

    row = await postgres_pool.fetchrow(
        "SELECT state, retry_count FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        slurm_ticket,
    )
    assert row["state"] == "completed"
    assert row["retry_count"] == 1
    assert backend.submit_calls == 2  # one per attempt
    rows = await _progress_rows(postgres_pool, slurm_ticket)
    # Two attempt rows for step 0: attempt 0 failed, attempt 1 completed.
    by_attempt = {r.attempt: r for r in rows}
    assert by_attempt[0].state is StepProgressState.FAILED
    assert by_attempt[0].failure_kind == "node_fail"
    assert by_attempt[1].state is StepProgressState.COMPLETED


async def test_local_step_records_compute_target_local(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """A step run on the synchronous local backend is recorded with
    compute_target=local (corrected from the optimistic slurm write-ahead),
    and an in-process action: entry is recorded as control_plane."""
    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    backend = FakeBackendClient()
    _populate_step_outputs(backend, workspace_root / str(work_ticket_idx))

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    rows = await _progress_rows(postgres_pool, work_ticket_idx)
    by_name = {r.step_name: r for r in rows}
    # `hash` / `load` are container steps → local backend here.
    assert by_name["hash"].compute_target is ComputeTarget.LOCAL
    assert by_name["hash"].slurm_job_id is None
    assert by_name["hash"].job_name is None
    # `mint-features` / `write-membership` / `register-files` are action: entries.
    assert by_name["mint-features"].compute_target is ComputeTarget.CONTROL_PLANE
    assert by_name["write-membership"].compute_target is ComputeTarget.CONTROL_PLANE
    # Every recorded entry completed.
    assert all(r.state is StepProgressState.COMPLETED for r in rows)


# =============================================================================
# Restart recovery (resume = re-attach, never blanket-fail)
# =============================================================================
#
# On CP startup, reconcile_inflight_tickets re-drives each non-terminal ticket
# through run_workflow(resume=True): completed entries fast-forward (outputs
# rebuilt from the shared workspace, not re-run), and the first incomplete
# entry resumes — re-attaching to a live SLURM job by its persisted id,
# finalizing one that succeeded while the CP was down, or deciding a purged job
# from its on-disk manifest. A CO outage during reconcile never fails the
# ticket. These drive run_workflow(resume=True) directly (reconcile's selection
# is covered in tests/routes/test_work_ticket.py).


async def _mark_processing(pool, work_ticket_idx):
    await pool.execute(
        "UPDATE qiita.work_ticket SET state = 'processing' WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )


async def _seed_submitted_step(pool, work_ticket_idx, *, step_name, slurm_job_id, step_index=0):
    """Write the progress rows for a step that was submitted (job id persisted)
    but not yet terminal — the in-flight shape a crash leaves behind."""
    await step_progress.record_submitting(
        pool,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=0,
        step_name=step_name,
        compute_target=ComputeTarget.SLURM,
        job_name=f"qiita-wt{work_ticket_idx}-{step_name}-a0",
    )
    await step_progress.record_submitted(
        pool,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=0,
        slurm_job_id=slurm_job_id,
    )


async def test_resume_reattaches_running_job_and_completes(postgres_pool, slurm_ticket, tmp_path):
    """A ticket that crashed with a SLURM job in flight (id persisted) resumes
    by adopting that job — no resubmit — and completes when it finishes."""
    await _mark_processing(postgres_pool, slurm_ticket)
    await _seed_submitted_step(postgres_pool, slurm_ticket, step_name="compute", slurm_job_id=900)

    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.RUNNING, StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    assert backend.submit_calls == 0  # adopted the persisted job
    rows = await step_progress.load_step_progress(postgres_pool, slurm_ticket)
    assert rows[0].slurm_job_id == 900
    assert rows[0].state is StepProgressState.COMPLETED


async def test_resume_finalizes_job_that_succeeded_during_outage(
    postgres_pool, slurm_ticket, tmp_path
):
    """The job finished while the CP was down: the first poll on resume reports
    COMPLETED, result is fetched, the ticket finalizes."""
    await _mark_processing(postgres_pool, slurm_ticket)
    await _seed_submitted_step(postgres_pool, slurm_ticket, step_name="compute", slurm_job_id=901)

    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    assert backend.submit_calls == 0
    assert backend.result_calls == 1


async def test_resume_purged_job_with_valid_output_completes(postgres_pool, slurm_ticket, tmp_path):
    """The job aged out of slurmrestd (status read raises UNKNOWN_PERMANENT),
    but its output manifest is on disk — the filesystem tiebreaker decides
    COMPLETED via result_step."""
    await _mark_processing(postgres_pool, slurm_ticket)
    await _seed_submitted_step(postgres_pool, slurm_ticket, step_name="compute", slurm_job_id=902)

    purged = BackendFailure(
        kind=FailureKind.UNKNOWN_PERMANENT,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="compute",
        reason="slurmrestd 404 (job purged)",
    )
    backend = FakeSlurmBackendClient(
        status_script=[purged],
        result_script=[{"result": "result.parquet"}],  # valid manifest on disk
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    assert backend.result_calls == 1  # tiebreaker consulted the filesystem


async def test_resume_purged_job_without_output_fails(postgres_pool, slurm_ticket, tmp_path):
    """The job aged out AND its output is gone/invalid — result_step's verify
    raises CONTRACT_VIOLATION (permanent), so the resumed ticket fails."""
    await _mark_processing(postgres_pool, slurm_ticket)
    await _seed_submitted_step(postgres_pool, slurm_ticket, step_name="compute", slurm_job_id=903)

    purged = BackendFailure(
        kind=FailureKind.UNKNOWN_PERMANENT,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="compute",
        reason="slurmrestd 404 (job purged)",
    )
    contract = BackendFailure(
        kind=FailureKind.CONTRACT_VIOLATION,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="compute",
        reason="output manifest missing on shared scratch",
    )
    backend = FakeSlurmBackendClient(status_script=[purged], result_script=[contract])
    with pytest.raises(BackendFailure):
        await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        slurm_ticket,
    )
    assert row["state"] == "failed"
    assert row["failure_type"] == "permanent"


async def test_resume_never_started_runs_from_scratch(postgres_pool, slurm_ticket, tmp_path):
    """A ticket orphaned right after PENDING → PROCESSING with no progress rows
    runs from step 0 — resume degrades to a normal dispatch."""
    await _mark_processing(postgres_pool, slurm_ticket)  # no progress rows seeded

    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    assert backend.submit_calls == 1  # fresh submit, nothing to adopt


async def _seed_submitting_no_id(pool, work_ticket_idx, *, step_name, step_index=0):
    """Write only the write-ahead 'submitting' row (no job id) — the exact
    shape the duplicate-job gap leaves behind: a prior process recorded intent
    (and the deterministic job_name) but crashed before persisting the id."""
    await step_progress.record_submitting(
        pool,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=0,
        step_name=step_name,
        compute_target=ComputeTarget.SLURM,
        job_name=f"qiita-wt{work_ticket_idx}-{step_name}-a0",
    )


async def test_resume_adopts_orphan_job_by_name(postgres_pool, slurm_ticket, tmp_path):
    """The write-ahead gap closer: a 'submitting' row with no persisted id but
    a recorded job_name resumes by finding the orphaned SLURM job by name and
    adopting it — NOT re-submitting a duplicate. The adopted id is persisted
    and the ticket completes."""
    await _mark_processing(postgres_pool, slurm_ticket)
    await _seed_submitting_no_id(postgres_pool, slurm_ticket, step_name="compute")

    job_name = f"qiita-wt{slurm_ticket}-compute-a0"
    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.RUNNING, StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
        found_jobs=[
            FoundJobWire(
                slurm_job_id=950,
                job_name=job_name,
                status=StepStatusWire(status=StepStatus.RUNNING, raw_state="RUNNING"),
            )
        ],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    # Adopted by name — no duplicate submit; the lookup keyed on the recorded name.
    assert backend.submit_calls == 0
    assert backend.find_by_name_calls == [job_name]
    rows = await step_progress.load_step_progress(postgres_pool, slurm_ticket)
    assert rows[0].slurm_job_id == 950  # the adopted orphan's id, now persisted
    assert rows[0].state is StepProgressState.COMPLETED


async def test_resume_submits_when_orphan_not_found(postgres_pool, slurm_ticket, tmp_path):
    """If slurmrestd has no job under the name (the submit never reached SLURM,
    or the job was purged), the find-by-name lookup is empty and the runner
    falls through to a fresh submit — exactly once."""
    await _mark_processing(postgres_pool, slurm_ticket)
    await _seed_submitting_no_id(postgres_pool, slurm_ticket, step_name="compute")

    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
        found_jobs=[],  # no orphan to adopt
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    assert backend.find_by_name_calls == [f"qiita-wt{slurm_ticket}-compute-a0"]
    assert backend.submit_calls == 1  # nothing to adopt → fresh submit


async def test_fresh_dispatch_does_not_look_up_by_name(postgres_pool, slurm_ticket, tmp_path):
    """A fresh (non-resume) dispatch wrote its own 'submitting' row this
    process, so there is no orphan to find — the (cluster-wide) find-by-name
    lookup is skipped entirely and the step submits normally."""
    backend = FakeSlurmBackendClient(
        status_script=[StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
        found_jobs=[
            FoundJobWire(  # would be adopted if the lookup ran — it must not
                slurm_job_id=999,
                job_name=f"qiita-wt{slurm_ticket}-compute-a0",
                status=StepStatusWire(status=StepStatus.RUNNING, raw_state="RUNNING"),
            )
        ],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws")  # resume=False

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", slurm_ticket
    )
    assert state == "completed"
    assert backend.find_by_name_calls == []  # never looked up on a fresh dispatch
    assert backend.submit_calls == 1
    rows = await step_progress.load_step_progress(postgres_pool, slurm_ticket)
    assert rows[0].slurm_job_id == 4242  # the fresh submit's id, not the orphan's 999


async def test_resume_co_unreachable_does_not_fail_ticket(postgres_pool, slurm_ticket, tmp_path):
    """A CO outage during reconcile keeps the resumed poll loop retrying and
    completes when the orchestrator returns — the ticket is never failed."""
    await _mark_processing(postgres_pool, slurm_ticket)
    await _seed_submitted_step(postgres_pool, slurm_ticket, step_name="compute", slurm_job_id=904)

    unreachable = BackendFailure(
        kind=FailureKind.ORCHESTRATOR_UNREACHABLE,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="compute",
        reason="co down (simulated reconcile)",
    )
    backend = FakeSlurmBackendClient(
        status_script=[unreachable] * 4 + [StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        slurm_ticket,
    )
    assert row["state"] == "completed"
    assert row["failure_type"] is None


# --- infra-retry escapability, backoff, visibility -------------------


def test_infra_backoff_delay_is_capped_exponential():
    """The in-place infra-retry backoff grows geometrically from the base
    interval and is clamped at the cap — and base=0 (the test cadence)
    stays 0 so suites never sleep."""
    from qiita_control_plane.runner import _infra_backoff_delay

    assert _infra_backoff_delay(0, base=1.0, cap=60.0) == 1.0
    assert _infra_backoff_delay(1, base=1.0, cap=60.0) == 2.0
    assert _infra_backoff_delay(2, base=1.0, cap=60.0) == 4.0
    # Geometric growth is clamped at the cap, never unbounded.
    assert _infra_backoff_delay(20, base=1.0, cap=60.0) == 60.0
    # base=0 (tests pass poll_interval_seconds=0) → never sleeps.
    assert _infra_backoff_delay(5, base=0.0, cap=60.0) == 0.0
    # A very long outage (huge n) must still yield a real, sleepable number —
    # the exponent clamp prevents 2**n -> inf and 0.0*inf -> nan (which would
    # crash asyncio.sleep). Holds for a real base and the base=0 test cadence.
    assert _infra_backoff_delay(5000, base=1.0, cap=60.0) == 60.0
    assert _infra_backoff_delay(5000, base=0.0, cap=60.0) == 0.0


async def test_infra_retry_bails_when_ticket_force_failed(postgres_pool, slurm_ticket, tmp_path):
    """An operator `force-fail` (a direct-DB FAILED transition) must stop the
    runner's in-place infra-unreachable retry loop, which otherwise spins
    forever. The runner re-checks the ticket's DB state each iteration
    and bails when it has gone terminal — WITHOUT clobbering the operator's
    failure surface."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import WorkTicketFailureStage

    class _ForceFailMidSubmit:
        """submit_step simulates the force-fail landing while the runner is
        mid-outage: flip the ticket terminal, then raise the unreachable
        failure the runner would otherwise retry forever."""

        def __init__(self, pool, idx):
            self._pool = pool
            self._idx = idx
            self.submit_calls = 0

        async def submit_step(self, *, step_name, **kwargs):
            self.submit_calls += 1
            await self._pool.execute(
                "UPDATE qiita.work_ticket"
                " SET state = 'failed', failure_type = 'permanent',"
                "     failure_stage = 'submission', failure_reason = $2"
                " WHERE work_ticket_idx = $1",
                self._idx,
                "operator force-fail",
            )
            raise BackendFailure(
                kind=FailureKind.ORCHESTRATOR_UNREACHABLE,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason="co down (simulated outage)",
            )

    backend = _ForceFailMidSubmit(postgres_pool, slurm_ticket)
    # Must RETURN (bail), not hang in the retry loop or raise. The wait_for
    # bound turns a regression (the pre-fix unbounded loop) into a fast test
    # failure instead of a hung suite — poll_interval_seconds=0 yields at the
    # backoff sleep, so the cancellation lands.
    import asyncio

    await asyncio.wait_for(_run(slurm_ticket, postgres_pool, backend, tmp_path / "ws"), timeout=10)

    # Bailed on the first post-force-fail iteration — did not spin.
    assert backend.submit_calls == 1
    row = await postgres_pool.fetchrow(
        "SELECT state, failure_type, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        slurm_ticket,
    )
    # The operator's failure surface is preserved, not overwritten.
    assert row["state"] == "failed"
    assert row["failure_reason"] == "operator force-fail"


async def test_infra_retry_surfaces_then_clears_transient_reason(
    postgres_pool, slurm_ticket, tmp_path
):
    """While the runner retries an unreachable orchestrator in place, it
    surfaces *why* on the ticket (transient_reason / transient_since) so the
    status route doesn't show a silently-wedged ticket; once it makes
    progress the marker is cleared."""

    class _CapturingBackend(FakeSlurmBackendClient):
        """Captures the ticket's transient marker on the submit that finally
        succeeds — i.e. after the unreachable retries set it."""

        def __init__(self, *, pool, idx, **kw):
            super().__init__(**kw)
            self._pool = pool
            self._idx = idx
            self.seen_transient: str | None = None
            self.seen_since = None

        async def submit_step(self, **kw):
            if self.submit_calls >= self._submit_unreachable_times:
                row = await self._pool.fetchrow(
                    "SELECT transient_reason, transient_since"
                    " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                    self._idx,
                )
                self.seen_transient = row["transient_reason"]
                self.seen_since = row["transient_since"]
            return await super().submit_step(**kw)

    backend = _CapturingBackend(
        pool=postgres_pool,
        idx=slurm_ticket,
        submit_unreachable_times=2,
        status_script=[StepStatus.COMPLETED],
        result_script=[{"result": "result.parquet"}],
    )
    await _run(slurm_ticket, postgres_pool, backend, tmp_path / "ws")

    # The retries surfaced the reason + a since-timestamp before recovery.
    assert backend.seen_transient is not None
    assert "orchestrator_unreachable" in backend.seen_transient
    assert backend.seen_since is not None
    # Recovery (and completion) cleared the marker — not left stale.
    row = await postgres_pool.fetchrow(
        "SELECT state, transient_reason, transient_since"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        slurm_ticket,
    )
    assert row["state"] == "completed"
    assert row["transient_reason"] is None
    assert row["transient_since"] is None


# --- multi-step resume: bound rebuild + action fast-forward -----------------


_MULTI_STEP_WORKFLOW = [
    {
        "kind": "step",
        "name": "compute",
        "step_type": "singleton",
        "container": REFERENCE_HASH_CONTAINER,
        "inputs": ["fasta_path"],
        "outputs": ["manifest"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "action",
        "name": "mint-features",
        "inputs": ["manifest"],
        "outputs": ["feature_map"],
    },
    {
        "kind": "step",
        "name": "finish",
        "step_type": "singleton",
        "container": REFERENCE_LOAD_CONTAINER,
        "inputs": ["feature_map"],
        "outputs": ["result"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
]


@pytest.fixture
async def multi_step_ticket(postgres_pool, reference_idx, tmp_path):
    action_id = "slurm-multi-step"
    version = f"runner-test-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps, cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "          $5::jsonb, $6::jsonb, 1, 1, '1 minute')",
        action_id,
        version,
        ["feature:mint", "reference:write"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps(_MULTI_STEP_WORKFLOW),
    )
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">seq1\nACGT\n")
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps({"fasta_path": str(fasta)}),
    )
    yield idx
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )


async def test_resume_multi_step_rebuilds_bound_and_skips_completed_action(
    postgres_pool, multi_step_ticket, library_spy, tmp_path
):
    """Resume of a 3-entry workflow where step 0 (compute) and the action
    (mint-features) already completed: both are fast-forwarded (the action is
    NOT re-run — not idempotent), their outputs are rebuilt into `bound`, and
    the final step runs with the rebuilt feature_map input."""
    await _mark_processing(postgres_pool, multi_step_ticket)
    # Step 0: completed SLURM step.
    await _seed_submitted_step(
        postgres_pool, multi_step_ticket, step_name="compute", slurm_job_id=950, step_index=0
    )
    await step_progress.record_running(
        postgres_pool, work_ticket_idx=multi_step_ticket, step_index=0, attempt=0
    )
    await step_progress.record_completed(
        postgres_pool, work_ticket_idx=multi_step_ticket, step_index=0, attempt=0
    )
    # Step 1: completed in-process action.
    await step_progress.record_submitting(
        postgres_pool,
        work_ticket_idx=multi_step_ticket,
        step_index=1,
        attempt=0,
        step_name="mint-features",
        compute_target=ComputeTarget.CONTROL_PLANE,
    )
    await step_progress.record_completed(
        postgres_pool, work_ticket_idx=multi_step_ticket, step_index=1, attempt=0
    )

    backend = FakeSlurmBackendClient(
        # result_script[0] = compute's reconstruct (fast-forward via result_step);
        # result_script[1] = finish's fresh result. status_script drives only
        # the fresh `finish` poll (the reconstruct doesn't poll).
        status_script=[StepStatus.COMPLETED],
        result_script=[{"manifest": "manifest.parquet"}, {"result": "result.parquet"}],
    )
    await _run(multi_step_ticket, postgres_pool, backend, tmp_path / "ws", resume=True)

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", multi_step_ticket
    )
    assert state == "completed"
    # mint-features was fast-forwarded, NOT re-run.
    assert [c[0] for c in library_spy.calls] == []
    # Only the final step submitted fresh; compute was reconstructed, not resubmitted.
    assert backend.submit_calls == 1
    # All three entries end completed in the progress table.
    rows = await step_progress.load_step_progress(postgres_pool, multi_step_ticket)
    by_index = {r.step_index: r for r in rows}
    assert by_index[0].state is StepProgressState.COMPLETED
    assert by_index[1].state is StepProgressState.COMPLETED
    assert by_index[2].state is StepProgressState.COMPLETED


_TARGET_STATUS_WORKFLOW = [
    {
        "kind": "step",
        "name": "s0",
        "step_type": "singleton",
        "container": REFERENCE_HASH_CONTAINER,
        "target_status": "hashing",
        "inputs": ["fasta_path"],
        "outputs": ["manifest"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "step",
        "name": "s1",
        "step_type": "singleton",
        "container": REFERENCE_LOAD_CONTAINER,
        "target_status": "minting",
        "inputs": ["manifest"],
        "outputs": ["result"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
]


async def test_resume_with_target_status_does_not_re_patch_or_go_backward(
    postgres_pool, reference_idx, tmp_path
):
    """Resume of a workflow whose entries declare `target_status`: a crash
    after step 1's status PATCH fired (resource at 'minting') but before it
    completed must NOT re-issue that PATCH (redundant 'minting'→'minting') nor
    the already-completed step 0's PATCH (backward 'minting'→'hashing') — both
    raise IllegalStatusTransition and would wrongly fail a healthy ticket. The
    PATCH is keyed off the resource's actual status, so both are skipped."""
    action_id = "slurm-target-status"
    version = f"runner-test-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps, cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "          $5::jsonb, $6::jsonb, 1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps(_TARGET_STATUS_WORKFLOW),
    )
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">seq1\nACGT\n")
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context, state"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb, 'processing'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps({"fasta_path": str(fasta)}),
    )
    try:
        # The crash left the reference at 'minting' (step 1's PATCH fired) with
        # step 0 completed and step 1 in flight (job id persisted).
        await postgres_pool.execute(
            "UPDATE qiita.reference SET status = 'minting' WHERE reference_idx = $1",
            reference_idx,
        )
        await _seed_submitted_step(
            postgres_pool, idx, step_name="s0", slurm_job_id=960, step_index=0
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=idx, step_index=0, attempt=0
        )
        await _seed_submitted_step(
            postgres_pool, idx, step_name="s1", slurm_job_id=961, step_index=1
        )

        backend = FakeSlurmBackendClient(
            status_script=[StepStatus.COMPLETED],  # s1 re-attach → completed
            result_script=[{"manifest": "manifest.parquet"}, {"result": "result.parquet"}],
        )
        # No success_status on the action → finalize issues no reference PATCH,
        # isolating the in-loop resume PATCH behavior under test.
        await _run(idx, postgres_pool, backend, tmp_path / "ws", resume=True)

        state = await postgres_pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
        )
        assert state == "completed"
        assert backend.submit_calls == 0  # s1 adopted, s0 reconstructed
    finally:
        await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )


_REDRIVE_REWALK_WORKFLOW = [
    {
        "kind": "step",
        "name": "s0",
        "step_type": "singleton",
        "container": REFERENCE_HASH_CONTAINER,
        "target_status": "hashing",
        "inputs": ["fasta_path"],
        "outputs": ["manifest"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "step",
        "name": "s1",
        "step_type": "singleton",
        "container": REFERENCE_LOAD_CONTAINER,
        "target_status": "minting",
        "inputs": ["manifest"],
        "outputs": ["feature_map"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "step",
        "name": "s2",
        "step_type": "singleton",
        "container": REFERENCE_LOAD_CONTAINER,
        "target_status": "loading",
        "inputs": ["feature_map"],
        "outputs": ["result"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
]


async def test_redrive_rewalks_status_fsm_for_completed_steps(
    postgres_pool, reference_idx, tmp_path
):
    """A `/run` redrive of a FAILED multi-transition reference workflow resets
    the reference to `pending` (its only legal exit from `failed`) while keeping
    the completed step rows. The runner must RE-WALK the FSM as it fast-forwards
    those completed steps — `pending → hashing` (s0) then `hashing → minting`
    (s1) — so the first re-run step (s2) can legally advance `minting → loading`.
    Without the re-walk the reference is stuck at `pending` and s2's
    `pending → loading` raises IllegalStatusTransition, dead-ending the redrive
    (the bug). s0/s1 are fast-forwarded (reconstructed, not resubmitted); only s2
    submits fresh."""
    action_id = "slurm-redrive-rewalk"
    version = f"runner-test-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience,"
        "  context_schema, steps, cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "          $5::jsonb, $6::jsonb, 1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps(_REDRIVE_REWALK_WORKFLOW),
    )
    fasta = tmp_path / "input.fasta"
    fasta.write_text(">seq1\nACGT\n")
    # The redrive already reset the ticket to `pending` and the reference to
    # `pending`, keeping the completed step rows. Reproduce that exact shape.
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context, state"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb, 'pending'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps({"fasta_path": str(fasta)}),
    )
    try:
        await postgres_pool.execute(
            "UPDATE qiita.reference SET status = 'pending' WHERE reference_idx = $1",
            reference_idx,
        )
        await _seed_submitted_step(
            postgres_pool, idx, step_name="s0", slurm_job_id=970, step_index=0
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=idx, step_index=0, attempt=0
        )
        await _seed_submitted_step(
            postgres_pool, idx, step_name="s1", slurm_job_id=971, step_index=1
        )
        await step_progress.record_completed(
            postgres_pool, work_ticket_idx=idx, step_index=1, attempt=0
        )

        backend = FakeSlurmBackendClient(
            status_script=[StepStatus.COMPLETED],  # s2's only poll
            # s0 + s1 reconstruct (fast-forward via result_step), then s2's fresh result.
            result_script=[
                {"manifest": "manifest.parquet"},
                {"feature_map": "feature_map.parquet"},
                {"result": "result.parquet"},
            ],
        )
        await _run(idx, postgres_pool, backend, tmp_path / "ws", resume=False)

        state = await postgres_pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
        )
        assert state == "completed"
        # The FSM was re-walked through the completed steps and on into s2: the
        # reference ends at `loading` (no success_status on this action), proving
        # `pending → hashing → minting → loading` all fired.
        ref_status = await postgres_pool.fetchval(
            "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
        )
        assert ref_status == "loading"
        assert backend.submit_calls == 1  # only s2 ran fresh; s0/s1 reconstructed
    finally:
        await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)
        await postgres_pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )


# =============================================================================
# Local (--local) by-path ingest — runner passthrough
# =============================================================================
#
# The local workflows (local-reference-add / local-host-reference-add) prepend a
# `stage_local_fasta` step that reads a raw `fasta_manifest_path` and produces
# `fasta_path`; companions (taxonomy / genome_map / ...) ride in action_context
# as raw absolute `*_path` strings rather than DoPut `*_upload_idx` handles. The
# whole point of the design is that the runner needs ZERO code change to support
# this: `_resolve_upload_handles` only touches `*_upload_idx` keys, so the raw
# `*_path` keys flow through `bound` untouched, and the existing output-threading
# (`bound.update(outputs)`) wires `stage_local_fasta.fasta_path` into the next
# step's `inputs:[fasta_path]`. These tests lock that passthrough.

_LOCAL_REFERENCE_ADD_STEPS = [
    {
        # The one new step: manifest path in, fasta_path out. Module step,
        # like the real YAML; the fake backend ignores container/module.
        "kind": "step",
        "name": "stage_local_fasta",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.stage_local_fasta",
        "inputs": ["fasta_manifest_path"],
        "outputs": ["fasta_path"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        # Unchanged from reference-add: consumes the staged fasta_path.
        "kind": "step",
        "name": "hash_sequences",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.hash_sequences",
        "target_status": "hashing",
        "inputs": ["fasta_path"],
        "outputs": ["manifest"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "action",
        "name": "mint-features",
        "target_status": "minting",
        "inputs": ["manifest"],
        "outputs": ["feature_map"],
    },
    {
        "kind": "action",
        "name": "write-membership",
        "inputs": ["feature_map"],
        "outputs": [],
    },
    {
        # taxonomy_path is an optional_input — for the local path it comes from
        # action_context as a raw path, exactly like the remote path's resolved
        # taxonomy_path.
        "kind": "step",
        "name": "load",
        "step_type": "singleton",
        "module": "qiita_compute_orchestrator.jobs.reference_load",
        "target_status": "loading",
        "inputs": ["manifest", "feature_map"],
        "optional_inputs": ["taxonomy_path"],
        "outputs": ["staging_dir"],
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    },
    {
        "kind": "action",
        "name": "register-files",
        "inputs": ["staging_dir"],
        "outputs": [],
    },
]


@pytest.fixture
async def local_reference_add_action(postgres_pool):
    """A `local-reference-add` action row whose first step is stage_local_fasta."""
    action_id = "local-reference-add"
    version = f"runner-test-{uuid.uuid4()}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, "
        "  context_schema, steps, "
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, "
        "  success_status, failure_status"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb,"
        "  $5::jsonb, $6::jsonb, 1, 1, '1 minute', $7, $8)",
        action_id,
        version,
        ["feature:mint", "reference:write", "reference:register_files"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps(_LOCAL_REFERENCE_ADD_STEPS),
        "active",
        "failed",
    )
    yield action_id, version
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )


@pytest.fixture
async def local_pending_work_ticket(
    postgres_pool, local_reference_add_action, reference_idx, tmp_path
):
    """A pending work_ticket whose action_context carries raw `*_path` keys
    (manifest + companions) instead of `*_upload_idx` handles."""
    action_id, version = local_reference_add_action
    manifest = tmp_path / "manifest.txt"
    fasta = tmp_path / "g1.fa"
    fasta.write_text(">g1\nACGT\n")
    manifest.write_text(f"{fasta}\n")
    taxonomy = tmp_path / "tax.parquet"
    taxonomy.write_text("taxonomy-bytes")
    genome_map = tmp_path / "gmap.parquet"
    genome_map.write_text("genome-map-bytes")

    action_context = {
        "fasta_manifest_path": str(manifest),
        "taxonomy_path": str(taxonomy),
        "genome_map_path": str(genome_map),
    }
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, 1, 'reference', $3, $4::jsonb) RETURNING work_ticket_idx",
        action_id,
        version,
        reference_idx,
        json.dumps(action_context),
    )
    yield {
        "work_ticket_idx": idx,
        "reference_idx": reference_idx,
        "manifest_path": manifest,
        "taxonomy_path": taxonomy,
        "genome_map_path": genome_map,
        "action": (action_id, version),
    }
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx)


def _populate_local_step_outputs(backend: FakeBackendClient, workspace: Path) -> None:
    """Configure the fake backend so stage_local_fasta/hash_sequences/load
    outputs land on disk and a staging Parquet exists for register-files."""
    backend.outputs_for["stage_local_fasta"] = {"fasta_path": workspace / "fasta.parquet"}
    backend.outputs_for["hash_sequences"] = {"manifest": workspace / "manifest.parquet"}
    backend.outputs_for["load"] = {"staging_dir": workspace / "staging"}
    staging = workspace / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "reference_sequences.parquet").touch(exist_ok=True)


async def test_resolve_upload_handles_leaves_raw_paths_untouched(
    postgres_pool, upload_staging_root
):
    """`_resolve_upload_handles` only resolves `*_upload_idx` keys. An
    action_context of pure raw `*_path` keys (the local-ingest shape) resolves
    to nothing and consumes no uploads — so the keys flow through `bound`
    verbatim and no DB upload row is touched."""
    from qiita_control_plane.runner import _resolve_upload_handles

    resolved, to_consume = await _resolve_upload_handles(
        postgres_pool,
        action_context={
            "fasta_manifest_path": "/data/refs/manifest.txt",
            "taxonomy_path": "/data/refs/tax.parquet",
            "genome_map_path": "/data/refs/gmap.parquet",
        },
        originator_principal_idx=1,
        upload_staging_root=upload_staging_root,
    )
    assert resolved == {}
    assert to_consume == []


async def test_runner_local_passthrough_threads_paths(
    postgres_pool, local_pending_work_ticket, library_spy, tmp_path
):
    """End-to-end runner passthrough for the local workflow with ZERO runner
    code change:

      * raw `fasta_manifest_path` reaches stage_local_fasta as an input;
      * stage_local_fasta's `fasta_path` output threads into hash_sequences'
        `inputs:[fasta_path]`;
      * the raw `taxonomy_path` reaches the load step via optional_inputs;
      * the raw `genome_map_path` reaches mint-features via bound.get(...);
      * the ticket completes and the reference goes active.
    """
    workspace_root = tmp_path / "ws"
    wt = local_pending_work_ticket
    work_ticket_idx = wt["work_ticket_idx"]

    backend = FakeBackendClient()
    workspace = workspace_root / str(work_ticket_idx)
    _populate_local_step_outputs(backend, workspace)

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    # Ticket + reference terminal state.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
    )
    assert state == "completed"
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", wt["reference_idx"]
    )
    assert ref_status == "active"

    # Steps ran in declared order: the stager first, then the unchanged pipeline.
    assert [c[0] for c in backend.calls] == ["stage_local_fasta", "hash_sequences", "load"]

    by_step = {c[0]: c[1] for c in backend.calls}
    # stage_local_fasta received the RAW manifest path straight from action_context.
    assert by_step["stage_local_fasta"]["fasta_manifest_path"] == wt["manifest_path"]
    # hash_sequences received the stage step's fasta_path output (threaded via bound),
    # NOT anything from action_context.
    assert by_step["hash_sequences"]["fasta_path"] == workspace / "fasta.parquet"
    # The raw taxonomy_path reached the load step as an optional input.
    assert by_step["load"]["taxonomy_path"] == wt["taxonomy_path"]

    # mint-features picked up the raw genome_map_path from bound (4th tuple slot
    # in the library_spy record).
    mint_call = next(c for c in library_spy.calls if c[0] == "mint-features")
    assert mint_call[3] == wt["genome_map_path"]

    # No upload rows were consumed — the local path mints/uploads nothing.
    assert [c[0] for c in library_spy.calls] == [
        "mint-features",
        "write-membership",
        "register-files",
    ]
