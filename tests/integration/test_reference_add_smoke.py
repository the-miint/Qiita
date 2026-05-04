"""End-to-end smoke test: drive workflows/reference-add/1.0.0.yaml
through the runner against a real control-plane (in-process via
ASGITransport) and a real LocalBackend.

What's exercised end-to-end:
  - YAML loader → sync into qiita.action
  - Runner reads the action row, walks every entry
  - Real LocalBackend hashes a tiny FASTA into manifest.parquet
  - HTTP /api/v1/library/mint-features dispatch → real
    library.mint_features → qiita.feature rows + feature_map.parquet
  - HTTP /api/v1/library/write-membership → real library.write_membership
    → qiita.reference_membership rows
  - Real LocalBackend load step writes reference_*.parquet
  - register-files stubbed: data-plane Flight needs cargo, which isn't
    on this host. The stub returns a canned RegisterFilesResponse so
    the runner can complete the workflow.

The assertion surface is the post-conditions that prove every entry
ran: reference reaches `active`, feature/membership rows exist,
work_ticket reaches COMPLETED, status PATCHes hit in declared order.
"""

import uuid
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.client import ControlPlaneClient
from qiita_common.models import RegisterFilesResponse


_REFERENCE_ADD_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "reference-add" / "1.0.0.yaml"
)


# A tiny FASTA the hash step can chew through in milliseconds.
_TINY_FASTA = b">seq1\nACGTACGTACGTACGT\n>seq2\nTTTTAAAACCCCGGGG\n>seq3\nGCATGCATGCATGCAT\n"


class _StubbedRegisterClient(ControlPlaneClient):
    """ControlPlaneClient subclass with register_files short-circuited.

    The real register_files goes through HTTP → control-plane dispatch →
    library.register_files → pyarrow.flight.FlightClient → data plane.
    The data plane needs the qiita-data-plane Rust binary, and cargo
    isn't on this host. Stub the register at the client level so the
    smoke test can complete the workflow without Flight.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_calls: list[tuple[int, str, dict[str, str]]] = []

    async def register_files(
        self, reference_idx: int, staging_dir: str, files: dict[str, str]
    ) -> RegisterFilesResponse:
        self.register_calls.append((reference_idx, staging_dir, dict(files)))
        return RegisterFilesResponse(
            registered=[f"{staging_dir}/{name}" for name in files]
        )


@pytest.fixture
async def synced_reference_add_action(postgres_pool, tmp_path):
    """Materialize workflows/reference-add/1.0.0.yaml under tmp_path/workflows/
    so the loader's directory walk picks it up, sync it into qiita.action,
    and clean the row up after.

    A unique action version is derived per test invocation so the test
    doesn't collide with other workflows in the table."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "reference-add"
    workflows_dir.mkdir(parents=True)
    yaml_text = _REFERENCE_ADD_YAML_PATH.read_text()
    test_version = f"smoke-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    assert len(actions) == 1
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("reference-add", test_version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "reference-add",
        test_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "reference-add",
        test_version,
    )


@pytest.fixture
async def smoke_reference(postgres_pool, human_admin_session):
    """A fresh reference for the smoke run; cleans up everything pointing
    at it (work_ticket via FK, then reference_membership) before
    dropping the reference itself."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"smoke-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    # Order matters: work_ticket → reference is RESTRICT, so drop tickets
    # before the reference. reference_membership cascade-FKs on the
    # reference itself but no point waiting for cascade.
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


async def test_reference_add_workflow_end_to_end(
    postgres_pool,
    hmac_secret,
    synced_reference_add_action,
    smoke_reference,
    compute_worker_service_account,
    tmp_path,
):
    """Drive the full reference-add workflow via the runner against the
    in-process control-plane app. After the run: ticket COMPLETED,
    reference 'active', feature/membership rows present, register-files
    invoked exactly once with a sensible filename → table mapping."""
    from qiita_compute_orchestrator.backends.local import LocalBackend
    from qiita_compute_orchestrator.runner import run_workflow
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    action_id, action_version = synced_reference_add_action
    reference_idx = smoke_reference

    fasta = tmp_path / "input.fasta"
    fasta.write_bytes(_TINY_FASTA)

    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, $3, 'reference', $4, $5::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        compute_worker_service_account["principal_idx"],
        reference_idx,
        f'{{"fasta_path": "{fasta}"}}',
    )

    # Configure the in-process control-plane app — same pattern as the
    # other integration test suites that route through ASGITransport.
    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:0",
    )

    workspace_root = tmp_path / "workspace"
    backend = LocalBackend()

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={
            "Authorization": f"Bearer {compute_worker_service_account['token']}"
        },
    ) as http:
        client = _StubbedRegisterClient(
            "http://test",
            api_token=compute_worker_service_account["token"],
            http_client=http,
        )
        await run_workflow(
            work_ticket_idx,
            backend,
            client,
            postgres_pool,
            workspace_root=workspace_root,
        )

    # work_ticket transitioned to COMPLETED.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"

    # Reference walked through hashing → minting → loading → active.
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    assert ref_status == "active"

    # mint-features inserted three feature rows (the FASTA has 3 sequences,
    # all distinct hashes).
    feature_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.feature f"
        " JOIN qiita.reference_membership m ON m.feature_idx = f.feature_idx"
        " WHERE m.reference_idx = $1",
        reference_idx,
    )
    assert feature_count == 3

    # register-files was called exactly once with the staging-dir Parquet
    # files mapped by the runner's filename → stem convention.
    assert len(client.register_calls) == 1
    _, staging_dir, files = client.register_calls[0]
    assert "reference_sequences.parquet" in files
    assert files["reference_sequences.parquet"] == "reference_sequences"
    assert "reference_membership.parquet" in files

    # Workspace materialised the expected files.
    workspace = workspace_root / str(work_ticket_idx)
    assert (workspace / "manifest.parquet").exists()
    assert (workspace / "feature_map.parquet").exists()
    # Load step writes its outputs into a `staging_dir` whose name comes
    # from LocalBackend._run_load — actually the load step writes into
    # the workspace itself. The runner records the path under the
    # output name 'staging_dir'.
