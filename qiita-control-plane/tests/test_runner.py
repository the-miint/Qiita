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
from qiita_common.models import UploadStatus
from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER, REFERENCE_LOAD_CONTAINER

pytestmark = pytest.mark.db


# =============================================================================
# Fakes
# =============================================================================


class FakeBackendClient:
    """Stand-in for ComputeBackendClient.run_step. Records calls and
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
    *,
    upload_staging_root: Path | None = None,
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
    files = register_call[2]  # ("register-files", staging_dir, files)
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

    class _RecordingBackend:
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
    """The register-index action arm reads the build step's `rype_index_meta`
    JSON from `bound` (native step outputs are path strings, so build params
    ride a file) and the reference_idx from scope_target, then records a
    qiita.reference_index row carrying the builder's index_type / fs_path /
    params."""
    from qiita_common.actions import WorkflowAction

    from qiita_control_plane.runner import _dispatch_action

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
    bound = {"rype_index_path": fs_path, "rype_index_meta": str(meta_path)}
    entry = WorkflowAction(kind="action", name="register-index", inputs=[], outputs=[])

    out = await _dispatch_action(
        postgres_pool,
        entry,
        bound,
        tmp_path,
        {"kind": "reference", "reference_idx": reference_idx},
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
