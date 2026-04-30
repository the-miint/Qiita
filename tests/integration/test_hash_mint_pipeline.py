"""Integration test: orchestrator hash job → control plane mint — first cross-service test."""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    """AsyncClient authenticated as the session admin. Service-only routes
    (mint) take a worker token via per-request `headers=worker_headers`."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        yield ac


@pytest.fixture
def worker_headers(compute_worker_service_account):
    """Authorization header for the compute worker service account."""
    return {"Authorization": f"Bearer {compute_worker_service_account['token']}"}


@pytest.fixture
async def ref_for_pipeline(client, postgres_pool):
    """Create a reference in pending status and clean up after."""
    resp = await client.post(
        "/api/v1/references",
        json={
            "name": f"pipeline-test-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    yield idx
    # Cleanup in FK dependency order
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.references WHERE reference_idx = $1", idx
    )


async def test_hash_then_mint_pipeline(
    client, fasta_file, ref_for_pipeline, tmp_path, worker_headers
):
    """Full round-trip: create reference → transition → hash → mint → verify."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, seqs = fasta_file
    ref_idx = ref_for_pipeline

    # Transition to hashing
    status_resp = await client.patch(
        f"/api/v1/references/{ref_idx}/status",
        json={"status": "hashing"},
    )
    assert status_resp.status_code == 200

    # Run hash job
    backend = LocalBackend()
    output_dir = tmp_path / "hash_output"
    manifest_path = await backend.run_hash_job(
        fasta_path=fasta_path,
        output_dir=output_dir,
        reference_idx=ref_idx,
    )
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["entries"]) == 5

    # Mint features
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        f"/api/v1/references/{ref_idx}/features/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    assert mint_resp.status_code == 200
    mint_body = mint_resp.json()
    assert mint_body["minted"] + mint_body["reused"] == 5
    assert len(mint_body["mapping"]) == 5

    # Verify membership rows
    from qiita_control_plane.main import app

    pool = app.state.pool
    count = await pool.fetchval(
        "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1",
        ref_idx,
    )
    assert count == 5


async def test_status_transition_pending_to_hashing(client, ref_for_pipeline):
    """PATCH /api/v1/references/{id}/status: pending → hashing must succeed."""
    resp = await client.patch(
        f"/api/v1/references/{ref_for_pipeline}/status",
        json={"status": "hashing"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "hashing"


async def test_status_transition_rejects_invalid(client, ref_for_pipeline):
    """PATCH /api/v1/references/{id}/status: pending → active must fail."""
    resp = await client.patch(
        f"/api/v1/references/{ref_for_pipeline}/status",
        json={"status": "active"},
    )
    assert resp.status_code == 409
