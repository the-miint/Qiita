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

pytestmark = pytest.mark.db


# =============================================================================
# Fakes
# =============================================================================


class FakeBackendClient:
    """Stand-in for ComputeBackendClient.run_step. Records calls and
    returns scripted outputs (which the runner expects to be Path
    objects keyed by the step's declared output names)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Path], Path, int]] = []
        self.outputs_for: dict[str, dict[str, Path]] = {}

    async def run_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        reference_idx: int,
    ) -> dict[str, Path]:
        self.calls.append((step_name, dict(inputs), workspace, reference_idx))
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

    async def register_files(*, staging_dir, files, hmac_secret, data_plane_url):
        calls.append(("register-files", staging_dir, dict(files)))
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
        "container": "qiita/reference-hash:1.0.0",
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
        "container": "qiita/reference-load:1.0.0",
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
) -> None:
    from qiita_control_plane.runner import run_workflow

    await run_workflow(
        work_ticket_idx,
        pool,
        backend,  # type: ignore[arg-type]  # protocol-shaped duck
        hmac_secret=b"unused",
        data_plane_url="grpc://unused:0",
        workspace_root=workspace_root,
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


async def test_refuses_non_pending_ticket(postgres_pool, pending_work_ticket, tmp_path):
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'processing' WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    with pytest.raises(RuntimeError, match="must be 'pending'"):
        await _run(work_ticket_idx, postgres_pool, FakeBackendClient(), tmp_path)


async def test_refuses_disabled_action(postgres_pool, pending_work_ticket, tmp_path):
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
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        pending_work_ticket["work_ticket_idx"],
    )
    assert state == "pending"


async def test_refuses_unknown_ticket(postgres_pool, tmp_path):
    with pytest.raises(RuntimeError, match="not found"):
        await _run(999_999_999, postgres_pool, FakeBackendClient(), tmp_path)


async def test_register_files_globs_staging_dir(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
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
        "reference_sequence_chunks.parquet",
        "reference_membership.parquet",
        "reference_taxonomy.parquet",
    ):
        (staging / name).touch(exist_ok=True)

    await _run(work_ticket_idx, postgres_pool, backend, workspace_root)

    register_call = next(c for c in library_spy.calls if c[0] == "register-files")
    files = register_call[2]  # ("register-files", staging_dir, files)
    assert files == {
        "reference_sequences.parquet": "reference_sequences",
        "reference_sequence_chunks.parquet": "reference_sequence_chunks",
        "reference_membership.parquet": "reference_membership",
        "reference_taxonomy.parquet": "reference_taxonomy",
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
    load_call = next(c for c in backend.calls if c[0] == "load")
    assert load_call[1]["fasta_path"] == pending_work_ticket["fasta_path"]
    assert load_call[1]["manifest"] == workspace / "manifest.parquet"
    assert load_call[1]["feature_map"] == workspace / "feature_map.parquet"


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


class _RetryingBackendClient:
    """Backend stub that raises BackendFailure on the first N attempts of
    a named step, then succeeds. Used to drive the retry loop without
    needing a real orchestrator. Each call increments the per-step
    counter so an instance can fail one step transiently while another
    succeeds first try."""

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
        self, *, step_name: str, inputs: dict[str, Path], workspace: Path, reference_idx: int
    ) -> dict[str, Path]:
        from qiita_common.backend_failure import BackendFailure
        from qiita_common.models import WorkTicketFailureStage

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
        async def run_step(self, *, step_name, inputs, workspace, reference_idx):
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
                step_name=step_name, inputs=inputs, workspace=workspace, reference_idx=reference_idx
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


async def test_retry_observable_via_state_transitions(
    postgres_pool, pending_work_ticket, library_spy, tmp_path
):
    """Each transient retry transitions PROCESSING → QUEUED → PROCESSING.
    Verified by observing the work_ticket state through a `before_each`
    hook installed on the backend stub: when run_step is invoked, the
    ticket must be in PROCESSING (not QUEUED — the runner re-transitions
    before each attempt)."""
    from qiita_common.backend_failure import FailureKind

    workspace_root = tmp_path / "ws"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    observed_states: list[str] = []

    class _Backend(_RetryingBackendClient):
        async def run_step(self, *, step_name, inputs, workspace, reference_idx):
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
                step_name=step_name, inputs=inputs, workspace=workspace, reference_idx=reference_idx
            )

    backend = _Backend(
        fail_step="hash",
        fail_n_times=2,  # two transient failures, then succeeds
        kind=FailureKind.OOM_KILLED,
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
