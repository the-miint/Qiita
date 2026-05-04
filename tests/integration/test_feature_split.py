"""Integration tests for the split feature-mint / reference-membership routes.

The deprecated POST /reference/{idx}/feature/mint endpoint is exercised by
test_feature_minting.py; this file covers the new shape:
    POST /feature/mint                             (no reference context)
    POST /reference/{reference_idx}/membership     (link minted features)
plus the orchestrator-driven flow that ties them together via PATCH
/reference/{idx}/status between calls.
"""

import hashlib
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

_TEST_SALT = uuid.uuid4().hex


def _md5_uuid(seq: str) -> str:
    return str(uuid.UUID(hashlib.md5(f"{_TEST_SALT}{seq}".encode()).hexdigest()))


@pytest.fixture
async def client(postgres_pool, human_admin_session):
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
    return {"Authorization": f"Bearer {compute_worker_service_account['token']}"}


@pytest.fixture
async def admin_headers(human_admin_session):
    """Headers for the session admin — used to PATCH reference status
    (scope reference:write, not service-only)."""
    return {"Authorization": f"Bearer {human_admin_session['token']}"}


@pytest.fixture
async def minting_reference(client, postgres_pool):
    """Create a reference and walk it to status='minting' via PATCH."""
    resp = await client.post(
        "/api/v1/reference",
        json={
            "name": f"split-test-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    # pending → hashing → minting via the existing PATCH /status endpoint.
    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'minting' WHERE reference_idx = $1", idx
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx IN "
        "(SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1)",
        idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_feature_mint_returns_mapping_no_reference_context(client, worker_headers):
    """POST /feature/mint with novel hashes returns a feature_idx mapping
    and writes nothing to qiita.reference_membership."""
    hashes = [_md5_uuid(f"SPLIT{i}") for i in range(4)]
    resp = await client.post(
        "/api/v1/feature/mint",
        json={"entries": [{"sequence_hash": h} for h in hashes]},
        headers=worker_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["minted"] == 4
    assert body["reused"] == 0
    assert set(body["mapping"].keys()) == set(hashes)
    assert all(isinstance(v, int) and v > 0 for v in body["mapping"].values())


async def test_feature_mint_dedupes_across_calls(client, worker_headers):
    """Calling /feature/mint twice with the same hashes returns identical
    feature_idx values; the second call reports reused=N."""
    hashes = [_md5_uuid(f"DEDUP-SPLIT{i}") for i in range(3)]
    body = {"entries": [{"sequence_hash": h} for h in hashes]}

    first = (await client.post("/api/v1/feature/mint", json=body, headers=worker_headers)).json()
    second = (await client.post("/api/v1/feature/mint", json=body, headers=worker_headers)).json()

    assert second["minted"] == 0
    assert second["reused"] == 3
    assert first["mapping"] == second["mapping"]


async def test_feature_mint_writes_genome_associations(client, postgres_pool, worker_headers):
    """When entries carry genome data, /feature/mint writes feature_genome
    rows. Reference-agnostic — feature_genome is not reference-scoped."""
    h = _md5_uuid("GENOME-SPLIT-1")
    resp = await client.post(
        "/api/v1/feature/mint",
        json={
            "entries": [
                {
                    "sequence_hash": h,
                    "genome_source": "ncbi",
                    "genome_source_id": "GCA_000001-split",
                }
            ]
        },
        headers=worker_headers,
    )
    assert resp.status_code == 200
    feature_idx = resp.json()["mapping"][h]

    row = await postgres_pool.fetchrow(
        "SELECT g.source, g.source_id FROM qiita.feature_genome fg"
        " JOIN qiita.genome g ON g.genome_idx = fg.genome_idx"
        " WHERE fg.feature_idx = $1",
        feature_idx,
    )
    assert row is not None
    assert row["source"] == "ncbi"
    assert row["source_id"] == "GCA_000001-split"


async def test_feature_mint_rejects_human_caller(client, admin_headers):
    """Service-only — a human PAT is rejected with 403."""
    resp = await client.post(
        "/api/v1/feature/mint",
        json={"entries": [{"sequence_hash": _md5_uuid("HUMAN")}]},
        headers=admin_headers,
    )
    assert resp.status_code == 403


async def test_membership_links_minted_features(
    client, postgres_pool, minting_reference, worker_headers
):
    """End-to-end orchestrator-shaped flow: mint then link."""
    hashes = [_md5_uuid(f"E2E{i}") for i in range(3)]
    mint = await client.post(
        "/api/v1/feature/mint",
        json={"entries": [{"sequence_hash": h} for h in hashes]},
        headers=worker_headers,
    )
    feature_idxs = list(mint.json()["mapping"].values())

    link = await client.post(
        f"/api/v1/reference/{minting_reference}/membership",
        json={"feature_idxs": feature_idxs},
        headers=worker_headers,
    )
    assert link.status_code == 201, link.text
    body = link.json()
    assert body["linked"] == 3
    assert body["already_linked"] == 0

    rows = await postgres_pool.fetch(
        "SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1",
        minting_reference,
    )
    assert sorted(r["feature_idx"] for r in rows) == sorted(feature_idxs)


async def test_membership_is_idempotent(client, minting_reference, worker_headers):
    """Re-linking the same feature_idxs reports already_linked=N and writes
    nothing new — pre-existing rows are skipped via ON CONFLICT DO NOTHING."""
    hashes = [_md5_uuid(f"IDEM{i}") for i in range(3)]
    mint = await client.post(
        "/api/v1/feature/mint",
        json={"entries": [{"sequence_hash": h} for h in hashes]},
        headers=worker_headers,
    )
    feature_idxs = list(mint.json()["mapping"].values())

    first = await client.post(
        f"/api/v1/reference/{minting_reference}/membership",
        json={"feature_idxs": feature_idxs},
        headers=worker_headers,
    )
    second = await client.post(
        f"/api/v1/reference/{minting_reference}/membership",
        json={"feature_idxs": feature_idxs},
        headers=worker_headers,
    )
    assert first.json() == {"linked": 3, "already_linked": 0}
    assert second.json() == {"linked": 0, "already_linked": 3}


async def test_membership_rejects_wrong_status(client, postgres_pool, worker_headers):
    """A reference not in 'minting' status must reject /membership with 409."""
    resp = await client.post(
        "/api/v1/reference",
        json={
            "name": f"split-bad-status-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    try:
        # status='pending' (default for fresh reference)
        link = await client.post(
            f"/api/v1/reference/{idx}/membership",
            json={"feature_idxs": [1]},
            headers=worker_headers,
        )
        assert link.status_code == 409
        assert "must be 'minting'" in link.json()["detail"]
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_membership_404_on_unknown_reference(client, worker_headers):
    """A reference_idx with no matching row returns 404."""
    resp = await client.post(
        "/api/v1/reference/999999999/membership",
        json={"feature_idxs": [1]},
        headers=worker_headers,
    )
    assert resp.status_code == 404


async def test_membership_rejects_human_caller(client, minting_reference, admin_headers):
    """Service-only — a human PAT is rejected with 403."""
    resp = await client.post(
        f"/api/v1/reference/{minting_reference}/membership",
        json={"feature_idxs": [1]},
        headers=admin_headers,
    )
    assert resp.status_code == 403


async def test_membership_rejects_unknown_feature_idx(
    client, minting_reference, worker_headers
):
    """A feature_idx that doesn't exist in qiita.feature returns 422 —
    callers are expected to mint via /feature/mint first."""
    resp = await client.post(
        f"/api/v1/reference/{minting_reference}/membership",
        json={"feature_idxs": [9999999999]},
        headers=worker_headers,
    )
    assert resp.status_code == 422
    assert "feature_idx" in resp.json()["detail"]
