"""Integration tests for qiita_compute_orchestrator.runner.run_workflow.

Exercises the runner against a live qiita.action / qiita.work_ticket
schema. Backend and ControlPlaneClient are stubbed so the tests assert
runner behaviour (sequencing, status PATCHes, state transitions, error
paths) without needing a running orchestrator or control-plane process.
"""

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from qiita_common.models import (
    FeatureMintResponse,
    ReferenceMembershipResponse,
    RegisterFilesResponse,
)


# =============================================================================
# Fakes
# =============================================================================


class FakeBackend:
    """Records every run_step call and returns scripted outputs.

    Tests pre-load `outputs_for[step_name]` with a dict that the backend
    returns for that step, optionally `touch()`ing the path so subsequent
    entries that read it find a real file."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], Path, int]] = []
        self.outputs_for: dict[str, dict[str, Path]] = {}

    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        reference_idx: int,
    ) -> dict[str, Path]:
        self.calls.append((name, dict(inputs), workspace, reference_idx))
        outputs = self.outputs_for.get(name, {})
        # Materialise output paths if they live under the workspace, so
        # the runner's downstream-input check sees a real file.
        for path in outputs.values():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        return outputs


class FakeControlPlaneClient:
    """Records every status PATCH and library call. Library responses
    are scripted via the `mint_*` / `register_*` attributes."""

    def __init__(self, workspace: Path) -> None:
        self.status_patches: list[tuple[int, str]] = []
        self.action_calls: list[tuple[str, ...]] = []
        self._workspace = workspace
        self.fail_on: str | None = None  # set to a primitive name to make it raise
        self.fail_status_patches: bool = False

    async def update_reference_status(self, reference_idx: int, status: str) -> Any:
        self.status_patches.append((reference_idx, status))
        if self.fail_status_patches:
            raise RuntimeError("simulated status PATCH failure")
        return None

    async def mint_features(
        self,
        reference_idx: int,
        manifest_path: Path,
        output_dir: Path,
        genome_map_path: Path | None = None,
    ) -> FeatureMintResponse:
        self.action_calls.append(
            ("mint-features", reference_idx, manifest_path, output_dir, genome_map_path)
        )
        if self.fail_on == "mint-features":
            raise RuntimeError("simulated mint-features failure")
        # Materialise feature_map.parquet so the next entry's input check
        # passes through reality, even though we're a fake.
        feature_map = output_dir / "feature_map.parquet"
        feature_map.parent.mkdir(parents=True, exist_ok=True)
        feature_map.touch(exist_ok=True)
        return FeatureMintResponse(
            feature_map_path=str(feature_map), minted=0, reused=0
        )

    async def write_membership(
        self, reference_idx: int, feature_map_path: Path
    ) -> ReferenceMembershipResponse:
        self.action_calls.append(("write-membership", reference_idx, feature_map_path))
        if self.fail_on == "write-membership":
            raise RuntimeError("simulated write-membership failure")
        return ReferenceMembershipResponse(linked=0, already_linked=0)

    async def register_files(
        self, reference_idx: int, staging_dir: str, files: dict[str, str]
    ) -> RegisterFilesResponse:
        self.action_calls.append(
            ("register-files", reference_idx, staging_dir, dict(files))
        )
        if self.fail_on == "register-files":
            raise RuntimeError("simulated register-files failure")
        return RegisterFilesResponse(registered=[])


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
async def reference_idx(postgres_pool, human_admin_session) -> int:
    """Create a reference at status='pending' and clean up after."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"runner-test-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


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
async def reference_add_action(postgres_pool):
    """Insert a reference-add row in qiita.action; clean up after.

    Cleanup is just the action row — the pending_work_ticket fixture is
    responsible for deleting any tickets it creates."""
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
async def pending_work_ticket(
    postgres_pool, reference_add_action, reference_idx, tmp_path
):
    """Insert a PENDING work_ticket pointing at the reference-add action and
    a fresh reference. The action_context carries a stub fasta_path.

    Teardown deletes the work_ticket so the dependent action and reference
    fixtures can drop their RESTRICT-protected rows after."""
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
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
    )


# =============================================================================
# Tests
# =============================================================================


async def test_run_workflow_success_path(postgres_pool, pending_work_ticket, tmp_path):
    """Successful run: walks every entry, drives status hashing → minting →
    loading → active, marks ticket COMPLETED."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    backend = FakeBackend()
    backend.outputs_for["hash"] = {"manifest": workspace / "manifest.parquet"}
    backend.outputs_for["load"] = {"staging_dir": workspace / "staging"}
    # Materialise a stand-in Parquet inside staging so register-files'
    # glob finds something to register.
    (workspace / "staging").mkdir(parents=True, exist_ok=True)
    (workspace / "staging" / "reference_sequences.parquet").touch(exist_ok=True)

    client = FakeControlPlaneClient(workspace=workspace)

    await run_workflow(
        work_ticket_idx,
        backend,
        client,
        postgres_pool,
        workspace_root=workspace_root,
    )

    # State transitions in order.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    # Status PATCHes in declared order: hashing, minting, loading, active.
    patches = [s for _, s in client.status_patches]
    assert patches == ["hashing", "minting", "loading", "active"]

    # Backend ran two steps.
    step_names = [name for name, *_ in backend.calls]
    assert step_names == ["hash", "load"]

    # Library actions ran in order.
    action_names = [call[0] for call in client.action_calls]
    assert action_names == ["mint-features", "write-membership", "register-files"]


async def test_run_workflow_skips_redundant_status_patches(
    postgres_pool, pending_work_ticket, tmp_path
):
    """write-membership and register-files don't declare target_status,
    so they should reuse the previous status (minting / loading
    respectively) — runner should not re-PATCH the same value."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    backend = FakeBackend()
    backend.outputs_for["hash"] = {"manifest": workspace / "manifest.parquet"}
    backend.outputs_for["load"] = {"staging_dir": workspace / "staging"}
    (workspace / "staging").mkdir(parents=True, exist_ok=True)
    (workspace / "staging" / "reference_sequences.parquet").touch(exist_ok=True)

    client = FakeControlPlaneClient(workspace=workspace)

    await run_workflow(
        work_ticket_idx, backend, client, postgres_pool, workspace_root=workspace_root
    )

    # 4 status patches total: hashing, minting, loading, active. Not 5
    # (write-membership doesn't re-patch minting). Not 6 (register-files
    # doesn't re-patch loading).
    assert len(client.status_patches) == 4


async def test_run_workflow_failure_marks_ticket_failed(
    postgres_pool, pending_work_ticket, tmp_path
):
    """A library-call failure mid-workflow marks the ticket FAILED, best-effort
    PATCHes the resource to failure_status, and re-raises."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    backend = FakeBackend()
    backend.outputs_for["hash"] = {"manifest": workspace / "manifest.parquet"}

    client = FakeControlPlaneClient(workspace=workspace)
    client.fail_on = "write-membership"

    with pytest.raises(RuntimeError, match="simulated write-membership failure"):
        await run_workflow(
            work_ticket_idx,
            backend,
            client,
            postgres_pool,
            workspace_root=workspace_root,
        )

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "failed"

    # Last status PATCH should be the failure_status ('failed') — applied
    # on the failure path.
    assert client.status_patches[-1] == (pending_work_ticket["reference_idx"], "failed")


async def test_run_workflow_swallows_failure_status_patch_error(
    postgres_pool, pending_work_ticket, tmp_path
):
    """If the best-effort failure_status PATCH itself fails, the runner
    still marks the ticket FAILED and re-raises the *original* failure
    (the PATCH-failure exception is swallowed, not propagated)."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    backend = FakeBackend()
    backend.outputs_for["hash"] = {"manifest": workspace / "manifest.parquet"}

    # Subclass FakeClient: succeed for the entry-level target_status PATCHes
    # (so the workflow runs to where mint-features fails), but fail the
    # *failure_status* PATCH at the end — the value the runner tries to
    # apply on the failure path.
    class FailingFailureStatusClient(FakeControlPlaneClient):
        async def update_reference_status(self, reference_idx, status):
            self.status_patches.append((reference_idx, status))
            if status == "failed":  # the action's failure_status
                raise RuntimeError("simulated failure_status PATCH failure")
            return None

    client = FailingFailureStatusClient(workspace=workspace_root / str(work_ticket_idx))
    client.fail_on = "mint-features"

    with pytest.raises(RuntimeError, match="simulated mint-features failure"):
        await run_workflow(
            work_ticket_idx,
            backend,
            client,
            postgres_pool,
            workspace_root=workspace_root,
        )

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "failed"

    # The runner attempted the failure_status PATCH (it appears in the
    # log of patches) even though it raised — the swallow means the
    # original exception is what bubbles, not the PATCH failure.
    assert ("failed",) in [(s,) for _, s in client.status_patches]


async def test_run_workflow_refuses_non_pending_ticket(
    postgres_pool, pending_work_ticket, tmp_path
):
    """A leftover PROCESSING (e.g. orchestrator crashed mid-run) is
    operator-recovery territory; the runner refuses to re-run silently."""
    from qiita_compute_orchestrator.runner import run_workflow

    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'processing' WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )

    backend = FakeBackend()
    client = FakeControlPlaneClient(workspace=tmp_path)

    with pytest.raises(RuntimeError, match="must be 'pending'"):
        await run_workflow(
            work_ticket_idx, backend, client, postgres_pool, workspace_root=tmp_path
        )


async def test_run_workflow_refuses_disabled_action(
    postgres_pool, pending_work_ticket, tmp_path
):
    """Disabled actions are filtered at fetch time — runner refuses with a
    clear error rather than silently running stale logic."""
    from qiita_compute_orchestrator.runner import run_workflow

    action_id, version = pending_work_ticket["action"]
    await postgres_pool.execute(
        "UPDATE qiita.action SET enabled = false, disabled_at = now(), disabled_by_idx = 1 "
        "WHERE action_id = $1 AND version = $2",
        action_id,
        version,
    )

    backend = FakeBackend()
    client = FakeControlPlaneClient(workspace=tmp_path)

    with pytest.raises(RuntimeError, match="not found or disabled"):
        await run_workflow(
            pending_work_ticket["work_ticket_idx"],
            backend,
            client,
            postgres_pool,
            workspace_root=tmp_path,
        )

    # Ticket stays PENDING — runner couldn't load the action so nothing
    # transitioned.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        pending_work_ticket["work_ticket_idx"],
    )
    assert state == "pending"


async def test_run_workflow_refuses_unknown_ticket(postgres_pool, tmp_path):
    """An idx pointing at no row is a hard error."""
    from qiita_compute_orchestrator.runner import run_workflow

    backend = FakeBackend()
    client = FakeControlPlaneClient(workspace=tmp_path)

    with pytest.raises(RuntimeError, match="not found"):
        await run_workflow(
            999_999_999, backend, client, postgres_pool, workspace_root=tmp_path
        )


async def test_run_workflow_register_files_globs_staging_dir(
    postgres_pool, pending_work_ticket, tmp_path
):
    """The runner derives register-files' filename → table mapping by
    globbing the staging_dir for *.parquet and using the stem as the
    table name."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    backend = FakeBackend()
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

    client = FakeControlPlaneClient(workspace=workspace)

    await run_workflow(
        work_ticket_idx, backend, client, postgres_pool, workspace_root=workspace_root
    )

    register_call = next(c for c in client.action_calls if c[0] == "register-files")
    files = register_call[3]  # (name, ref_idx, staging_dir, files)
    assert files == {
        "reference_sequences.parquet": "reference_sequences",
        "reference_sequence_chunks.parquet": "reference_sequence_chunks",
        "reference_membership.parquet": "reference_membership",
        "reference_taxonomy.parquet": "reference_taxonomy",
    }


async def test_run_workflow_threads_inputs_through_workspace(
    postgres_pool, pending_work_ticket, tmp_path
):
    """Each entry's `inputs` resolve from the workspace bound dict — outputs
    of earlier entries become inputs of later ones, including the initial
    work_ticket.action_context."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]
    workspace = workspace_root / str(work_ticket_idx)

    backend = FakeBackend()
    backend.outputs_for["hash"] = {"manifest": workspace / "manifest.parquet"}
    backend.outputs_for["load"] = {"staging_dir": workspace / "staging"}
    (workspace / "staging").mkdir(parents=True, exist_ok=True)
    (workspace / "staging" / "reference_sequences.parquet").touch(exist_ok=True)

    client = FakeControlPlaneClient(workspace=workspace)

    await run_workflow(
        work_ticket_idx, backend, client, postgres_pool, workspace_root=workspace_root
    )

    # hash got fasta_path from action_context (set in pending_work_ticket).
    hash_call = next(c for c in backend.calls if c[0] == "hash")
    hash_inputs = hash_call[1]
    assert hash_inputs["fasta_path"] == pending_work_ticket["fasta_path"]

    # load got fasta_path (action_context) + manifest (hash output) +
    # feature_map (mint-features output).
    load_call = next(c for c in backend.calls if c[0] == "load")
    load_inputs = load_call[1]
    assert load_inputs["fasta_path"] == pending_work_ticket["fasta_path"]
    assert load_inputs["manifest"] == workspace / "manifest.parquet"
    assert load_inputs["feature_map"] == workspace / "feature_map.parquet"


async def test_run_workflow_creates_workspace_directory(
    postgres_pool, pending_work_ticket, tmp_path
):
    """The runner mkdirs `<workspace_root>/<work_ticket_idx>/` before any
    entry runs so steps and primitives have a place to write outputs."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace_does_not_exist_yet"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    backend = FakeBackend()
    # Backend's hash output writes into the workspace; this also exercises
    # that the runner created it.
    backend.outputs_for["hash"] = {
        "manifest": workspace_root / str(work_ticket_idx) / "manifest.parquet"
    }
    backend.outputs_for["load"] = {
        "staging_dir": workspace_root / str(work_ticket_idx) / "staging"
    }
    (workspace_root / str(work_ticket_idx) / "staging").mkdir(
        parents=True, exist_ok=True
    )
    (workspace_root / str(work_ticket_idx) / "staging" / "x.parquet").touch(
        exist_ok=True
    )

    client = FakeControlPlaneClient(workspace=workspace_root / str(work_ticket_idx))

    await run_workflow(
        work_ticket_idx, backend, client, postgres_pool, workspace_root=workspace_root
    )

    workspace = workspace_root / str(work_ticket_idx)
    assert workspace.is_dir()


async def test_run_workflow_uses_action_failure_status_when_set(
    postgres_pool, pending_work_ticket, tmp_path
):
    """Failure handler PATCHes the action's declared failure_status, not
    a hardcoded value."""
    from qiita_compute_orchestrator.runner import run_workflow

    workspace_root = tmp_path / "workspace"
    work_ticket_idx = pending_work_ticket["work_ticket_idx"]

    backend = FakeBackend()
    backend.outputs_for["hash"] = {
        "manifest": workspace_root / str(work_ticket_idx) / "manifest.parquet"
    }

    client = FakeControlPlaneClient(workspace=workspace_root / str(work_ticket_idx))
    client.fail_on = "mint-features"

    with pytest.raises(RuntimeError):
        await run_workflow(
            work_ticket_idx,
            backend,
            client,
            postgres_pool,
            workspace_root=workspace_root,
        )

    # The reference-add fixture sets failure_status='failed' (matching
    # the qiita.reference status enum).
    assert client.status_patches[-1][1] == "failed"
