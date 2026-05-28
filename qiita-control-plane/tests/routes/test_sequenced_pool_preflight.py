"""Integration tests for GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/preflight.

Exercises the SA-only read surface added in commit 2: the
compute-worker SA (Scope.SEQUENCED_POOL_PREFLIGHT_READ) happy path,
the auth matrix (HumanUser 403, SA-without-scope 403, anonymous 401),
the membership and presence 404/422 paths (unknown run, unknown pool,
pool-in-wrong-run, pool with no preflight populated), and a base64
round-trip that pins byte-equality between the on-disk blob and the
deserialised response body.
"""

import base64
import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SEQUENCED_POOL_PREFLIGHT
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.main import app

from .conftest import delete_idxs, unique_instrument_id

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# FK-reverse cleanup
# ---------------------------------------------------------------------------


async def _cleanup_tracked(pool, created: dict) -> None:
    await delete_idxs(pool, "sequenced_pool", created["sequenced_pool"])
    await delete_idxs(pool, "sequencing_run", created["sequencing_run"])


@pytest_asyncio.fixture
async def ctx(
    postgres_pool,
    compute_worker_service_account,
    wet_lab_admin_session,
    regular_user_session,
):
    """Yield a route-test context with the SA client (preflight:read scope),
    a wet_lab_admin client (used to seed runs/pools as a non-SA principal),
    a regular-user client (used for the human-403 path), and the FK-reverse
    `created` tracker."""
    app.state.pool = postgres_pool
    transport = ASGITransport(app=app)
    created: dict = {"sequencing_run": [], "sequenced_pool": []}
    async with (
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
        ) as sa,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
        ) as wet,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {regular_user_session['token']}"},
        ) as user,
    ):
        yield {
            "pool": postgres_pool,
            "sa": sa,
            "wet": wet,
            "user": user,
            "sa_session": compute_worker_service_account,
            "wet_session": wet_lab_admin_session,
            "user_session": regular_user_session,
            "created": created,
        }
    await _cleanup_tracked(postgres_pool, created)


@pytest_asyncio.fixture
async def sa_no_preflight_read_scope_client(postgres_pool, compute_worker_service_account):
    """A bearer-auth client whose SA token carries a worker scope OTHER than
    sequenced_pool:preflight:read, so the require_service_with_scope guard's
    403 path is exercised."""
    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=compute_worker_service_account["principal_idx"],
        label=f"sa-no-preflight-{secrets.token_hex(4)}",
        scopes=[Scope.SEQUENCE_RANGE_MINT],
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# DB seeding helpers
# ---------------------------------------------------------------------------


async def _seed_run(ctx, suffix: str) -> int:
    """Insert a minimal sequencing_run, track for cleanup, return its idx."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        unique_instrument_id(suffix),
        ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["sequencing_run"].append(idx)
    return idx


async def _seed_pool(ctx, *, run_idx: int, blob: bytes | None, filename: str | None) -> int:
    """Insert a sequenced_pool against `run_idx` with the given preflight
    pair (both may be None for the no-preflight case), track for cleanup,
    return its idx."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.sequenced_pool ("
        "    sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
        "    created_by_idx"
        ") VALUES ($1, $2, $3, $4) RETURNING idx",
        run_idx,
        blob,
        filename,
        ctx["wet_session"]["principal_idx"],
    )
    ctx["created"]["sequenced_pool"].append(idx)
    return idx


def _url(run_idx: int, pool_idx: int) -> str:
    return URL_SEQUENCED_POOL_PREFLIGHT.format(
        sequencing_run_idx=run_idx,
        sequenced_pool_idx=pool_idx,
    )


# ===========================================================================
# Happy path
# ===========================================================================


async def test_get_preflight_sa_happy_path_round_trips_bytes(ctx):
    # SA with Scope.SEQUENCED_POOL_PREFLIGHT_READ reads the (blob, filename)
    # pair. Blob round-trips byte-identical via base64 → BYTEA → base64.
    run_idx = await _seed_run(ctx, "ok")
    blob = b"\x00SQLite header\x01\x02\xff and trailing magic\xfe\xfd"
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=blob, filename="preflight.db")

    resp = await ctx["sa"].get(_url(run_idx, pool_idx))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    expected = {
        "run_preflight_blob": base64.b64encode(blob).decode("ascii"),
        "run_preflight_filename": "preflight.db",
    }
    assert body == expected
    # Decode the base64 surface and compare to the seeded blob byte-for-byte.
    assert base64.b64decode(body["run_preflight_blob"]) == blob


# ===========================================================================
# Auth matrix
# ===========================================================================


async def test_get_preflight_anonymous_401(ctx):
    run_idx = await _seed_run(ctx, "anon")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=b"X", filename="f.db")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_url(run_idx, pool_idx))
    assert resp.status_code == 401


async def test_get_preflight_human_user_403(ctx):
    # A HumanUser (no SA kind) gets 403 from require_service_with_scope
    # regardless of scope set. The route is service-only.
    run_idx = await _seed_run(ctx, "human")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=b"X", filename="f.db")
    resp = await ctx["user"].get(_url(run_idx, pool_idx))
    assert resp.status_code == 403


async def test_get_preflight_sa_without_scope_403(ctx, sa_no_preflight_read_scope_client):
    run_idx = await _seed_run(ctx, "noscope")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=b"X", filename="f.db")
    resp = await sa_no_preflight_read_scope_client.get(_url(run_idx, pool_idx))
    assert resp.status_code == 403
    assert "sequenced_pool:preflight:read" in resp.json()["detail"]


# ===========================================================================
# Membership and presence
# ===========================================================================


async def test_get_preflight_unknown_run_404(ctx):
    # require_sequencing_run_exists fires the 404 before the pool lookup.
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.sequencing_run")
    resp = await ctx["sa"].get(_url(max_idx + 100_000, 1))
    assert resp.status_code == 404
    assert "sequencing_run" in resp.json()["detail"]


async def test_get_preflight_unknown_pool_404(ctx):
    # The run exists; require_sequenced_pool_in_run resolves no pool row
    # and surfaces 404 naming the pool idx.
    run_idx = await _seed_run(ctx, "nopool")
    max_pool_idx = await ctx["pool"].fetchval(
        "SELECT COALESCE(MAX(idx), 0) FROM qiita.sequenced_pool"
    )
    resp = await ctx["sa"].get(_url(run_idx, max_pool_idx + 100_000))
    assert resp.status_code == 404
    assert "sequenced_pool" in resp.json()["detail"]


async def test_get_preflight_pool_in_wrong_run_422(ctx):
    # The pool exists but belongs to a different sequencing_run.
    # require_sequenced_pool_in_run maps that to 422 (existing convention
    # for parent-child consistency mismatches; not 404).
    run_a = await _seed_run(ctx, "a")
    run_b = await _seed_run(ctx, "b")
    pool_in_a = await _seed_pool(ctx, run_idx=run_a, blob=b"X", filename="f.db")

    resp = await ctx["sa"].get(_url(run_b, pool_in_a))
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert f"sequenced_pool {pool_in_a}" in detail
    assert f"sequencing_run {run_b}" in detail


async def test_get_preflight_pool_has_no_preflight_404(ctx):
    # The row exists, membership is correct, but the pool was created
    # without a preflight pair (both blob and filename NULL). Distinct
    # 404 with a message naming the pool.
    run_idx = await _seed_run(ctx, "nopre")
    pool_idx = await _seed_pool(ctx, run_idx=run_idx, blob=None, filename=None)
    resp = await ctx["sa"].get(_url(run_idx, pool_idx))
    assert resp.status_code == 404
    assert f"sequenced_pool {pool_idx}" in resp.json()["detail"]
    assert "no preflight" in resp.json()["detail"]
