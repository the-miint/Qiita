"""End-to-end test for the CP → CO step-dispatch contract.

Exercises every layer of the private CP → CO step path:

  * `ComputeBackendClient` (qiita-common) — request envelope serialization
    (StepRunRequest), Authorization header, response parsing back into
    Path objects.
  * `POST /api/v1/step/run` (qiita-compute-orchestrator) — bearer-token
    compare, route-level dispatch into `app.state.backend`.
  * Real `LocalBackend` running the `hash_sequences` native module —
    actually canonicalizes the upload Parquet and writes manifest.parquet
    plus the two reference_sequence* outputs under the workspace.

In-process: the orchestrator FastAPI app is reached via httpx
`ASGITransport`, not a uvicorn subprocess. Lifespan is bypassed in
favour of setting `app.state` directly — same pattern the control-plane
integration tests use to wire `app.state.pool`.
"""

from pathlib import Path

import duckdb
import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_compute_orchestrator.backends.local import LocalBackend
from qiita_compute_orchestrator.main import app as orch_app

_SHARED_TOKEN = "step-dispatch-test-token"
_HASH_SEQUENCES_MODULE = "qiita_compute_orchestrator.jobs.hash_sequences"


def _write_upload_parquet(path: Path, reads: list[tuple[str, str]]) -> Path:
    """Synthesize an `upload.parquet` matching the DoPut writer's shape:
    `(read_id VARCHAR, sequence VARCHAR)`. Two short sequences are enough
    to exercise the step's full pipeline without inflating I/O."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TEMP TABLE t (read_id VARCHAR, sequence VARCHAR)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", reads)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    return path


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
    """A real ComputeBackendClient → /step/run → LocalBackend → native
    hash_sequences run: the manifest Parquet must end up on disk with the
    expected shape. Verifies the CP-side envelope (StepRunRequest) carries
    the YAML's module path correctly through to run_native_job dispatch."""
    fasta = _write_upload_parquet(
        tmp_path / "upload.parquet",
        [("seq1", "ACGTACGTACGTACGT"), ("seq2", "TTTTAAAACCCCGGGG")],
    )
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
            step_name="hash_sequences",
            inputs={"fasta_path": fasta},
            workspace=workspace,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            # StepRunRequest validates exactly-one(container, module);
            # LocalBackend rejects container steps wholesale, so native
            # dispatch under `module:` is the only LocalBackend path.
            module=_HASH_SEQUENCES_MODULE,
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
        # `manifest` and `reference_sequence` share the
        # `sequence_length_bp` column so downstream JOINs don't trip on
        # naming drift.
        assert cols == ["read_id", "sequence_hash", "sequence_length_bp"]
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
                step_name="hash_sequences",
                inputs={"fasta_path": tmp_path / "x.parquet"},
                workspace=tmp_path,
                scope_target={"kind": "reference", "reference_idx": 1},
                work_ticket_idx=1,
                module=_HASH_SEQUENCES_MODULE,
            )
        assert exc_info.value.response.status_code == 401
