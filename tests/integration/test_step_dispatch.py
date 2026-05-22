"""End-to-end test for the CP → CO step-dispatch contract.

Exercises every layer of the new private path:

  * `ComputeBackendClient` (qiita-common) — request envelope serialization
    (StepRunRequest), Authorization header, response parsing back into
    Path objects.
  * `POST /api/v1/step/run` (qiita-compute-orchestrator) — bearer-token
    compare, route-level dispatch into `app.state.backend`.
  * Real `LocalBackend` (DuckDB + miint) — actually hashes the FASTA
    and writes manifest.parquet under the workspace.

In-process: the orchestrator FastAPI app is reached via httpx
`ASGITransport`, not a uvicorn subprocess. Lifespan is bypassed in
favour of setting `app.state` directly — same pattern the control-plane
integration tests use to wire `app.state.pool`.
"""

import duckdb
import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER
from qiita_compute_orchestrator.backends.local import LocalBackend
from qiita_compute_orchestrator.main import app as orch_app

_SHARED_TOKEN = "step-dispatch-test-token"


@pytest.fixture
def orchestrator_app():
    """Configure the orchestrator app's state without going through
    lifespan. The /step/run route reads from `request.app.state` so
    setting it directly is sufficient and avoids re-running lifespan
    (which would re-resolve `Settings.from_env()` on every test)."""
    orch_app.state.backend = LocalBackend()
    orch_app.state.cp_to_co_token = _SHARED_TOKEN
    return orch_app


async def test_step_dispatch_hash_end_to_end(orchestrator_app, tmp_path):
    """A real ComputeBackendClient → /step/run → LocalBackend hash:
    the manifest Parquet must end up on disk with the expected shape."""
    fasta = tmp_path / "test.fa"
    fasta.write_text(">seq1\nACGTACGTACGTACGT\n>seq2\nTTTTAAAACCCCGGGG\n")
    workspace = tmp_path / "workspace"

    # Caller-supplied http_client takes its auth from its own headers
    # (ComputeBackendClient honours that path); set Authorization here
    # so the orchestrator's bearer compare passes.
    async with AsyncClient(
        transport=ASGITransport(app=orchestrator_app),
        base_url="http://orch",
        headers={"Authorization": f"Bearer {_SHARED_TOKEN}"},
    ) as http:
        client = ComputeBackendClient(
            "http://orch",
            api_token=_SHARED_TOKEN,
            http_client=http,
        )
        outputs = await client.run_step(
            step_name="hash",
            inputs={"fasta_path": fasta},
            workspace=workspace,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            # Required by StepRunRequest's exactly-one(container, module)
            # validator. LocalBackend ignores the value for the hash step
            # (name-dispatch into _run_hash); the field declares the runtime.
            container=REFERENCE_HASH_CONTAINER,
        )

    manifest = outputs["manifest"]
    assert manifest.exists()
    assert manifest == workspace / "manifest.parquet"

    with duckdb.connect(":memory:") as conn:
        cols = [
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{manifest}')"
            ).fetchall()
        ]
        assert cols == ["read_id", "sequence_hash", "length"]
        n = conn.execute(f"SELECT count(*) FROM '{manifest}'").fetchone()[0]
        assert n == 2


async def test_step_dispatch_rejects_wrong_token(orchestrator_app, tmp_path):
    """A request with the wrong bearer token must hit the orchestrator's
    401 — proves the route-level auth check survives the round trip."""
    async with AsyncClient(
        transport=ASGITransport(app=orchestrator_app),
        base_url="http://orch",
        headers={"Authorization": "Bearer wrong-token"},
    ) as http:
        client = ComputeBackendClient(
            "http://orch",
            api_token="wrong-token",
            http_client=http,
        )
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client.run_step(
                step_name="hash",
                inputs={"fasta_path": tmp_path / "x.fa"},
                workspace=tmp_path,
                scope_target={"kind": "reference", "reference_idx": 1},
                work_ticket_idx=1,
                container=REFERENCE_HASH_CONTAINER,
            )
        assert exc_info.value.response.status_code == 401
