"""Integration tests for the generic /api/v1/library/{name} dispatch route."""

import hashlib
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    LOOPBACK_HOST,
    URL_LIBRARY_NAME,
    URL_REFERENCE_PREFIX,
    LibraryPrimitive,
)

_TEST_SALT = uuid.uuid4().hex


def _md5_uuid(seq: str) -> str:
    return str(uuid.UUID(hashlib.md5(f"{_TEST_SALT}{seq}".encode()).hexdigest()))


@pytest.fixture
async def client(postgres_pool, hmac_secret, human_admin_session):
    """AsyncClient with stub data-plane URL — register-files isn't dispatched
    in this module, only mint and membership."""
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
    return {"Authorization": f"Bearer {compute_worker_service_account['token']}"}


@pytest.fixture
async def admin_headers(human_admin_session):
    return {"Authorization": f"Bearer {human_admin_session['token']}"}


@pytest.fixture
async def minting_reference(client, postgres_pool):
    """Create a reference and walk it to status='minting' so write-membership
    accepts feature_idxs."""
    resp = await client.post(
        URL_REFERENCE_PREFIX,
        json={
            "name": f"lib-dispatch-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    await postgres_pool.execute(
        "UPDATE qiita.reference SET status = 'minting' WHERE reference_idx = $1",
        idx,
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


def _ref_target(reference_idx: int) -> dict:
    return {"kind": "reference", "reference_idx": reference_idx}


async def test_unknown_primitive_returns_404(client, worker_headers):
    """A primitive name that isn't in LIBRARY surfaces as 404, not 500."""
    resp = await client.post(
        URL_LIBRARY_NAME.format(name="nonexistent"),
        json={"scope_target": _ref_target(1), "inputs": {}},
        headers=worker_headers,
    )
    assert resp.status_code == 404
    assert "Unknown library primitive" in resp.json()["detail"]


async def test_dispatch_rejects_human_caller(client, minting_reference, admin_headers):
    """All primitives are service-only; a human PAT is rejected with 403."""
    resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"entries": [{"sequence_hash": _md5_uuid("X")}]},
        },
        headers=admin_headers,
    )
    assert resp.status_code == 403


async def test_dispatch_mint_features_round_trip(
    client, minting_reference, worker_headers
):
    """mint-features dispatch returns the same shape as the underlying
    library function — mapping + minted + reused under `outputs`."""
    hashes = [_md5_uuid(f"DISPATCH{i}") for i in range(3)]
    resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"entries": [{"sequence_hash": h} for h in hashes]},
        },
        headers=worker_headers,
    )
    assert resp.status_code == 200, resp.text
    outputs = resp.json()["outputs"]
    assert outputs["minted"] == 3
    assert outputs["reused"] == 0
    assert set(outputs["mapping"].keys()) == set(hashes)


async def test_dispatch_write_membership_then_again_is_idempotent(
    client, minting_reference, worker_headers
):
    """Two calls of write-membership for the same feature_idxs report
    linked=N then linked=0 / already_linked=N — idempotent like the
    underlying library function."""
    hashes = [_md5_uuid(f"IDEM-DISPATCH{i}") for i in range(3)]
    mint = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"entries": [{"sequence_hash": h} for h in hashes]},
        },
        headers=worker_headers,
    )
    feature_idxs = list(mint.json()["outputs"]["mapping"].values())

    first = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.WRITE_MEMBERSHIP),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"feature_idxs": feature_idxs},
        },
        headers=worker_headers,
    )
    second = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.WRITE_MEMBERSHIP),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"feature_idxs": feature_idxs},
        },
        headers=worker_headers,
    )
    assert first.json()["outputs"] == {"linked": 3, "already_linked": 0}
    assert second.json()["outputs"] == {"linked": 0, "already_linked": 3}


async def test_dispatch_write_membership_status_check(
    client, postgres_pool, worker_headers
):
    """write-membership rejects with 409 when the reference isn't in
    'minting' status — the dispatch handler enforces this so workflow
    runners surface a useful error rather than silently FK-failing."""
    resp = await client.post(
        URL_REFERENCE_PREFIX,
        json={
            "name": f"bad-status-{uuid.uuid4()}",
            "version": "1.0",
            "kind": "sequence_reference",
        },
    )
    idx = resp.json()["reference_idx"]
    try:
        # status='pending' (fresh reference)
        membership = await client.post(
            URL_LIBRARY_NAME.format(name=LibraryPrimitive.WRITE_MEMBERSHIP),
            json={
                "scope_target": _ref_target(idx),
                "inputs": {"feature_idxs": [1]},
            },
            headers=worker_headers,
        )
        assert membership.status_code == 409
        assert "must be 'minting'" in membership.json()["detail"]
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
        )


async def test_dispatch_validates_input_shape(
    client, minting_reference, worker_headers
):
    """mint-features without inputs.entries is 422 — the dispatch handler
    parses per-primitive input shape inside the route since the generic
    envelope doesn't constrain it."""
    resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {},  # missing entries
        },
        headers=worker_headers,
    )
    assert resp.status_code == 422
    assert "entries" in resp.json()["detail"]
