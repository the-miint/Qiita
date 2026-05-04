"""Integration tests for the generic /api/v1/library/{name} dispatch route."""

import hashlib
import uuid

import duckdb
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


def _write_manifest(path, hashes: list[str]) -> None:
    """Materialize a manifest.parquet — sequence_hash column drives mint-features."""
    rows = [(f"seq{i}", h, 32 + i) for i, h in enumerate(hashes)]
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE m (read_id VARCHAR, sequence_hash UUID, length BIGINT)"
        )
        conn.executemany("INSERT INTO m VALUES (?, ?::uuid, ?)", rows)
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")


def _read_outputs_mapping(feature_map_path: str) -> dict[str, int]:
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            "SELECT CAST(sequence_hash AS VARCHAR), feature_idx FROM read_parquet(?)",
            [feature_map_path],
        ).fetchall()
    return {r[0]: r[1] for r in rows}


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
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


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


async def test_dispatch_rejects_human_caller(
    client, minting_reference, admin_headers, tmp_path
):
    """All primitives are service-only; a human PAT is rejected with 403."""
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, [_md5_uuid("X")])
    resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"manifest_path": str(manifest), "output_dir": str(tmp_path)},
        },
        headers=admin_headers,
    )
    assert resp.status_code == 403


async def test_dispatch_mint_features_round_trip(
    client, minting_reference, worker_headers, tmp_path
):
    """mint-features dispatch returns the file path of feature_map.parquet
    plus minted/reused counts under `outputs`. The Parquet file contains
    one row per input hash."""
    hashes = [_md5_uuid(f"DISPATCH{i}") for i in range(3)]
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, hashes)
    resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"manifest_path": str(manifest), "output_dir": str(tmp_path)},
        },
        headers=worker_headers,
    )
    assert resp.status_code == 200, resp.text
    outputs = resp.json()["outputs"]
    assert outputs["minted"] == 3
    assert outputs["reused"] == 0
    feature_map_path = outputs["feature_map_path"]
    assert feature_map_path == str(tmp_path / "feature_map.parquet")
    mapping = _read_outputs_mapping(feature_map_path)
    assert set(mapping.keys()) == set(hashes)


async def test_dispatch_write_membership_then_again_is_idempotent(
    client, minting_reference, worker_headers, tmp_path
):
    """Two calls of write-membership for the same feature_map report
    linked=N then linked=0 / already_linked=N."""
    hashes = [_md5_uuid(f"IDEM-DISPATCH{i}") for i in range(3)]
    manifest = tmp_path / "manifest.parquet"
    _write_manifest(manifest, hashes)
    mint = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"manifest_path": str(manifest), "output_dir": str(tmp_path)},
        },
        headers=worker_headers,
    )
    feature_map_path = mint.json()["outputs"]["feature_map_path"]

    first = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.WRITE_MEMBERSHIP),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"feature_map_path": feature_map_path},
        },
        headers=worker_headers,
    )
    second = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.WRITE_MEMBERSHIP),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {"feature_map_path": feature_map_path},
        },
        headers=worker_headers,
    )
    assert first.json()["outputs"] == {"linked": 3, "already_linked": 0}
    assert second.json()["outputs"] == {"linked": 0, "already_linked": 3}


async def test_dispatch_write_membership_status_check(
    client, postgres_pool, worker_headers, tmp_path
):
    """write-membership rejects with 409 when the reference isn't in
    'minting' status — the dispatch handler enforces this so workflow
    runners surface a useful error rather than silently FK-failing.
    The Parquet body never matters because the status check fails first."""
    feature_map = tmp_path / "fm.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE fm (sequence_hash UUID, feature_idx BIGINT)"
        )
        conn.execute(
            "INSERT INTO fm VALUES ('00000000-0000-0000-0000-000000000001'::uuid, 1)"
        )
        conn.execute(f"COPY fm TO '{feature_map}' (FORMAT PARQUET)")

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
                "inputs": {"feature_map_path": str(feature_map)},
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
    """mint-features without inputs.manifest_path is 422 — the dispatch
    handler parses per-primitive input shape inside the route since the
    generic envelope doesn't constrain it."""
    resp = await client.post(
        URL_LIBRARY_NAME.format(name=LibraryPrimitive.MINT_FEATURES),
        json={
            "scope_target": _ref_target(minting_reference),
            "inputs": {},  # missing manifest_path / output_dir
        },
        headers=worker_headers,
    )
    assert resp.status_code == 422
    assert "manifest_path" in resp.json()["detail"]
