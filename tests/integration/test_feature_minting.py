"""Integration tests for bulk feature minting endpoint."""

import hashlib
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

_TEST_SALT = uuid.uuid4().hex  # unique per test session to avoid cross-run collisions


def _md5_uuid(seq: str) -> str:
    """Compute MD5 hash of a salted sequence and return as UUID string."""
    return str(uuid.UUID(hashlib.md5(f"{_TEST_SALT}{seq}".encode()).hexdigest()))


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    """AsyncClient authenticated as the session admin. Service-only endpoints
    (mint, register, doget tickets) are exercised by passing `worker_headers`
    per-request to override the default Authorization."""
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
    """Authorization header for the compute worker service account — required
    by service-only routes (POST /references/{id}/features/mint)."""
    return {"Authorization": f"Bearer {compute_worker_service_account['token']}"}


@pytest.fixture
async def reference_idx(client, postgres_pool):
    """Create a reference in 'hashing' status and return its idx."""
    resp = await client.post(
        "/api/v1/references",
        json={
            "name": f"mint-test-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    await postgres_pool.execute(
        "UPDATE qiita.references SET status = 'hashing' WHERE reference_idx = $1", idx
    )
    yield idx
    # Cleanup in FK dependency order
    await postgres_pool.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx IN "
        "(SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1)",
        idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.references WHERE reference_idx = $1", idx
    )


async def test_mint_five_new_features(client, reference_idx, worker_headers):
    """Minting 5 novel hashes should return 5 feature_idx values."""
    hashes = [_md5_uuid(f"ATCG{i}") for i in range(5)]
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": [{"sequence_hash": h} for h in hashes]},
        headers=worker_headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["minted"] == 5
    assert body["reused"] == 0
    assert len(body["mapping"]) == 5
    for feature_idx in body["mapping"].values():
        assert isinstance(feature_idx, int)
        assert feature_idx > 0


async def test_mint_deduplicates(client, reference_idx, worker_headers):
    """Minting the same hashes twice should reuse all feature_idx values."""
    hashes = [_md5_uuid(f"DEDUP{i}") for i in range(3)]
    entries = [{"sequence_hash": h} for h in hashes]

    resp1 = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    assert resp1.status_code == 200
    mapping1 = resp1.json()["mapping"]

    resp2 = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert body2["reused"] == 3
    assert body2["minted"] == 0
    assert body2["mapping"] == mapping1


async def test_mint_mixed_new_and_existing(client, reference_idx, worker_headers):
    """Minting a mix of existing and new hashes should reuse and mint correctly."""
    existing = [_md5_uuid(f"EXIST{i}") for i in range(3)]
    resp1 = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": [{"sequence_hash": h} for h in existing]},
        headers=worker_headers,
    )
    assert resp1.status_code == 200

    new = [_md5_uuid(f"NEW{i}") for i in range(2)]
    all_hashes = existing + new
    resp2 = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": [{"sequence_hash": h} for h in all_hashes]},
        headers=worker_headers,
    )
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["reused"] == 3
    assert body["minted"] == 2
    assert len(body["mapping"]) == 5


async def test_mint_with_genome_association(
    client, reference_idx, postgres_pool, worker_headers
):
    """Entries with genome_source/genome_source_id should create genome + junction rows."""
    source_id = f"GCF_ASSOC_{uuid.uuid4().hex[:8]}"
    h = _md5_uuid("GENOME_SEQ")
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={
            "entries": [
                {
                    "sequence_hash": h,
                    "genome_source": "genbank",
                    "genome_source_id": source_id,
                }
            ]
        },
        headers=worker_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["minted"] == 1
    feature_idx = resp.json()["mapping"][h]

    genome = await postgres_pool.fetchrow(
        "SELECT * FROM qiita.genomes WHERE source = 'genbank' AND source_id = $1",
        source_id,
    )
    assert genome is not None

    junction = await postgres_pool.fetchrow(
        "SELECT * FROM qiita.feature_genome WHERE feature_idx = $1", feature_idx
    )
    assert junction is not None
    assert junction["genome_idx"] == genome["genome_idx"]


async def test_mint_reuses_existing_genome(
    client, reference_idx, postgres_pool, worker_headers
):
    """If a genome already exists (same source+source_id), reuse it."""
    source_id = f"GCF_REUSE_{uuid.uuid4().hex[:8]}"
    genome_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.genomes (source, source_id) VALUES ($1, $2)"
        " ON CONFLICT (source, source_id) DO UPDATE SET source = EXCLUDED.source"
        " RETURNING genome_idx",
        "genbank",
        source_id,
    )

    h = _md5_uuid("GENOME_REUSE_SEQ")
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={
            "entries": [
                {
                    "sequence_hash": h,
                    "genome_source": "genbank",
                    "genome_source_id": source_id,
                }
            ]
        },
        headers=worker_headers,
    )
    assert resp.status_code == 200
    feature_idx = resp.json()["mapping"][h]

    junction = await postgres_pool.fetchrow(
        "SELECT * FROM qiita.feature_genome WHERE feature_idx = $1", feature_idx
    )
    assert junction["genome_idx"] == genome_idx


async def test_mint_rejects_wrong_status(client, postgres_pool, worker_headers):
    """Minting against a reference in 'pending' status should fail."""
    resp = await client.post(
        "/api/v1/references",
        json={
            "name": f"wrong-status-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]

    mint_resp = await client.post(
        f"/api/v1/references/{idx}/features/mint",
        json={"entries": [{"sequence_hash": _md5_uuid("X")}]},
        headers=worker_headers,
    )
    assert mint_resp.status_code == 409

    await postgres_pool.execute(
        "DELETE FROM qiita.references WHERE reference_idx = $1", idx
    )


async def test_mint_rejects_empty_batch(client, reference_idx, worker_headers):
    """Minting with an empty entries list should return 422."""
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": []},
        headers=worker_headers,
    )
    assert resp.status_code == 422


async def test_mint_rejects_duplicate_hashes_in_request(
    client, reference_idx, worker_headers
):
    """Submitting duplicate sequence_hash values in one request should return 422."""
    h = _md5_uuid("DUP_IN_REQ")
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": [{"sequence_hash": h}, {"sequence_hash": h}]},
        headers=worker_headers,
    )
    assert resp.status_code == 422


async def test_mint_rejects_genome_source_without_id(
    client, reference_idx, worker_headers
):
    """genome_source set without genome_source_id should return 422."""
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={
            "entries": [
                {"sequence_hash": _md5_uuid("GS_NO_ID"), "genome_source": "genbank"}
            ]
        },
        headers=worker_headers,
    )
    assert resp.status_code == 422


async def test_mint_rejects_genome_id_without_source(
    client, reference_idx, worker_headers
):
    """genome_source_id set without genome_source should return 422."""
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={
            "entries": [
                {
                    "sequence_hash": _md5_uuid("GID_NO_SRC"),
                    "genome_source_id": "GCF_123",
                }
            ]
        },
        headers=worker_headers,
    )
    assert resp.status_code == 422


async def test_cross_reference_deduplication(client, postgres_pool, worker_headers):
    """Same hash minted against two references should get the same feature_idx."""
    # Create two references in hashing status
    refs = []
    for i in range(2):
        resp = await client.post(
            "/api/v1/references",
            json={
                "name": f"xref-dedup-{uuid.uuid4()}",
                "version": "1.0",
                "kind": "sequence_reference",
            },
        )
        idx = resp.json()["reference_idx"]
        await postgres_pool.execute(
            "UPDATE qiita.references SET status = 'hashing' WHERE reference_idx = $1",
            idx,
        )
        refs.append(idx)

    h = _md5_uuid("SHARED_ACROSS_REFS")
    entries = [{"sequence_hash": h}]

    resp1 = await client.post(
        f"/api/v1/references/{refs[0]}/features/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    resp2 = await client.post(
        f"/api/v1/references/{refs[1]}/features/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    assert resp1.status_code == 200
    assert resp2.status_code == 200

    # Same feature_idx for both references
    assert resp1.json()["mapping"][h] == resp2.json()["mapping"][h]

    # Both references have membership rows
    for ref_idx in refs:
        count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1",
            ref_idx,
        )
        assert count == 1

    # Cleanup
    for ref_idx in refs:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", ref_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.references WHERE reference_idx = $1", ref_idx
        )


async def test_mint_10k_batch_performance(client, reference_idx, worker_headers):
    """Minting 10,000 features should complete in < 10 seconds."""
    import time

    hashes = [_md5_uuid(f"PERF{i}") for i in range(10_000)]
    entries = [{"sequence_hash": h} for h in hashes]

    start = time.monotonic()
    resp = await client.post(
        f"/api/v1/references/{reference_idx}/features/mint",
        json={"entries": entries},
        headers=worker_headers,
    )
    elapsed = time.monotonic() - start

    assert resp.status_code == 200
    body = resp.json()
    assert body["minted"] == 10_000
    assert len(body["mapping"]) == 10_000
    assert elapsed < 10.0, f"10K mint took {elapsed:.1f}s (limit: 10s)"
