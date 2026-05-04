"""Integration test: orchestrator hash job → control plane mint — first cross-service test."""

import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    LOOPBACK_HOST,
    URL_LIBRARY_NAME,
    URL_REFERENCE_PREFIX,
    URL_REFERENCE_STATUS,
    LibraryPrimitive,
)


@pytest.fixture
async def client(postgres_pool, hmac_secret, human_admin_session):
    """AsyncClient authenticated as the session admin. Service-only routes
    (mint, membership) take a worker token via per-request
    `headers=worker_headers`. Settings are initialised so the library
    dispatch endpoint's hmac/data-plane dependencies resolve; the
    data_plane_url is a stub since this module never dispatches
    register-files (the only primitive that talks to the data plane)."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:0",
    )
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
        URL_REFERENCE_PREFIX,
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
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


async def test_hash_then_mint_pipeline(
    client, fasta_file, ref_for_pipeline, tmp_path, worker_headers
):
    """Full round-trip: create reference → transition → hash → mint → link → verify."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    fasta_path, seqs = fasta_file
    ref_idx = ref_for_pipeline

    status_url = URL_REFERENCE_STATUS.format(reference_idx=ref_idx)

    # pending → hashing
    status_resp = await client.patch(status_url, json={"status": "hashing"})
    assert status_resp.status_code == 200

    # Hash step
    backend = LocalBackend()
    output_dir = tmp_path / "hash_output"
    result = await backend.run_step(
        "hash", {"fasta_path": fasta_path}, output_dir, reference_idx=ref_idx
    )
    manifest_path = result["manifest"]
    manifest = json.loads(manifest_path.read_text())
    assert len(manifest["entries"]) == 5

    # hashing → minting (orchestrator-driven; no longer implicit in the mint call)
    await client.patch(status_url, json={"status": "minting"})

    # mint-features primitive via /library/{name}
    entries = [{"sequence_hash": e["sequence_hash"]} for e in manifest["entries"]]
    mint_resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": {"kind": "reference", "reference_idx": ref_idx},
            "inputs": {"entries": entries},
        },
        headers=worker_headers,
    )
    assert mint_resp.status_code == 200, mint_resp.text
    mint_outputs = mint_resp.json()["outputs"]
    assert mint_outputs["minted"] + mint_outputs["reused"] == 5
    assert len(mint_outputs["mapping"]) == 5

    # write-membership primitive — links the minted feature_idxs
    feature_idxs = list(mint_outputs["mapping"].values())
    membership_resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.WRITE_MEMBERSHIP),
        json={
            "scope_target": {"kind": "reference", "reference_idx": ref_idx},
            "inputs": {"feature_idxs": feature_idxs},
        },
        headers=worker_headers,
    )
    assert membership_resp.status_code == 200, membership_resp.text
    assert membership_resp.json()["outputs"]["linked"] == 5

    # Verify membership rows
    from qiita_control_plane.main import app

    pool = app.state.pool
    count = await pool.fetchval(
        "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1",
        ref_idx,
    )
    assert count == 5


async def test_status_transition_pending_to_hashing(client, ref_for_pipeline):
    """PATCH /api/v1/reference/{id}/status: pending → hashing must succeed."""
    resp = await client.patch(
        URL_REFERENCE_STATUS.format(reference_idx=ref_for_pipeline),
        json={"status": "hashing"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "hashing"


async def test_status_transition_rejects_invalid(client, ref_for_pipeline):
    """PATCH /api/v1/reference/{id}/status: pending → active must fail."""
    resp = await client.patch(
        URL_REFERENCE_STATUS.format(reference_idx=ref_for_pipeline),
        json={"status": "active"},
    )
    assert resp.status_code == 409
