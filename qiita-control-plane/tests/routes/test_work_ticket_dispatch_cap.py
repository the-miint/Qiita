"""Route tests for GET/PATCH /api/v1/work-ticket/dispatch-cap.

GET reads the process-wide dispatch cap; PATCH sets or clears its runtime override.
Both are gated on `work_ticket:cancel` (system_admin).
"""

import asyncio
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_WORK_TICKET_DISPATCH_CAP
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import DISPATCH_MAX_INFLIGHT_CHOICES

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def ctx(postgres_pool, monkeypatch):
    """App with a fresh dispatch semaphore at the default cap, a system_admin token
    carrying work_ticket:cancel, and a scopeless user token for the 403s."""
    from qiita_control_plane.auth.token import mint_api_token
    from qiita_control_plane.config import Settings
    from qiita_control_plane.dispatch import build_dispatch_semaphore
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )
    # The app is shared across the worker's tests; monkeypatch restores the semaphore
    # so an override set here does not leak into the next test.
    semaphore = build_dispatch_semaphore()
    monkeypatch.setattr(app.state, "dispatch_semaphore", semaphore, raising=False)

    suffix = uuid.uuid4().hex[:8]

    async def _principal(role, label, scopes):
        idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
            " VALUES ($1, $2, 1) RETURNING idx",
            f"{label}-{suffix}",
            role,
        )
        await postgres_pool.execute(
            "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
            " VALUES ($1, $2, 'X', 'Y', 'Z')",
            idx,
            f"{label}-{suffix}@example.com",
        )
        tok, _ = await mint_api_token(postgres_pool, principal_idx=idx, label=label, scopes=scopes)
        return idx, tok

    admin_idx, admin_tok = await _principal(
        SystemRole.SYSTEM_ADMIN, "wtdc-admin", [Scope.WORK_TICKET_CANCEL]
    )
    user_idx, user_tok = await _principal(SystemRole.USER, "wtdc-user", [Scope.SELF_PROFILE])

    yield {"admin_tok": admin_tok, "user_tok": user_tok, "semaphore": semaphore}

    for pidx in (admin_idx, user_idx):
        await postgres_pool.execute("DELETE FROM qiita.api_token WHERE principal_idx = $1", pidx)
        await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", pidx)
        await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", pidx)


def _client():
    from qiita_control_plane.main import app

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _auth(tok):
    return {"Authorization": f"Bearer {tok}"}


async def test_get_reports_the_default_cap(ctx):
    async with _client() as c:
        resp = await c.get(URL_WORK_TICKET_DISPATCH_CAP, headers=_auth(ctx["admin_tok"]))
    assert resp.status_code == 200
    assert resp.json() == {
        "default": 8,
        "override": None,
        "max_inflight": 8,
        "running": 0,
        "waiting": 0,
        "max_settable": 12,
        "choices": list(DISPATCH_MAX_INFLIGHT_CHOICES),
    }


async def test_a_ceiling_above_the_default_is_settable_and_reported(ctx, monkeypatch):
    """Booted at 12 with DISPATCH_MAX_INFLIGHT_CEILING=24: GET reports 24 as
    `max_settable`, and PATCH to 24 is accepted rather than refused as above the
    boot value."""
    from qiita_control_plane.dispatch import build_dispatch_semaphore
    from qiita_control_plane.main import app

    monkeypatch.setattr(app.state, "dispatch_semaphore", build_dispatch_semaphore(12, 24))
    async with _client() as c:
        got = await c.get(URL_WORK_TICKET_DISPATCH_CAP, headers=_auth(ctx["admin_tok"]))
        patched = await c.patch(
            URL_WORK_TICKET_DISPATCH_CAP,
            headers=_auth(ctx["admin_tok"]),
            json={"max_inflight": 24},
        )
    assert got.status_code == 200
    assert (got.json()["default"], got.json()["max_settable"]) == (12, 24)
    assert patched.status_code == 200
    assert (patched.json()["default"], patched.json()["max_inflight"]) == (12, 24)


async def test_get_counts_running_and_waiting(ctx):
    sem = ctx["semaphore"]
    for _ in range(8):
        await sem.acquire()
    waiter = asyncio.create_task(sem.acquire())
    await asyncio.sleep(0)
    try:
        async with _client() as c:
            body = (
                await c.get(URL_WORK_TICKET_DISPATCH_CAP, headers=_auth(ctx["admin_tok"]))
            ).json()
        assert (body["running"], body["waiting"]) == (8, 1)
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_patch_raises_the_cap_and_starts_waiters(ctx):
    sem = ctx["semaphore"]
    for _ in range(8):
        await sem.acquire()
    waiter = asyncio.create_task(sem.acquire())
    await asyncio.sleep(0)
    async with _client() as c:
        resp = await c.patch(
            URL_WORK_TICKET_DISPATCH_CAP,
            headers=_auth(ctx["admin_tok"]),
            json={"max_inflight": 12},
        )
    assert resp.status_code == 200
    await asyncio.wait_for(waiter, timeout=5)
    body = resp.json()
    assert (body["override"], body["max_inflight"], body["running"]) == (12, 12, 9)
    assert sem.limit == 12


async def test_patch_null_clears_the_override(ctx):
    ctx["semaphore"].set_override(12)
    async with _client() as c:
        resp = await c.patch(
            URL_WORK_TICKET_DISPATCH_CAP,
            headers=_auth(ctx["admin_tok"]),
            json={"max_inflight": None},
        )
    assert resp.status_code == 200
    assert (resp.json()["override"], resp.json()["max_inflight"]) == (None, 8)


async def test_patch_above_what_the_boot_pool_carries_is_422(ctx):
    """Booted at 8, the pool is 25, so 16 is refused and the cap is unchanged."""
    async with _client() as c:
        resp = await c.patch(
            URL_WORK_TICKET_DISPATCH_CAP,
            headers=_auth(ctx["admin_tok"]),
            json={"max_inflight": 16},
        )
    assert resp.status_code == 422
    assert "DISPATCH_MAX_INFLIGHT_CEILING" in resp.json()["detail"]
    assert ctx["semaphore"].override is None


@pytest.mark.parametrize("body", [{"max_inflight": 10}, {"max_inflight": 0}, {}, {"x": 1}])
async def test_patch_rejects_a_value_outside_the_choices_or_a_bad_body(ctx, body):
    async with _client() as c:
        resp = await c.patch(
            URL_WORK_TICKET_DISPATCH_CAP, headers=_auth(ctx["admin_tok"]), json=body
        )
    assert resp.status_code == 422
    assert ctx["semaphore"].override is None


async def test_both_routes_require_the_cancel_scope(ctx):
    async with _client() as c:
        get = await c.get(URL_WORK_TICKET_DISPATCH_CAP, headers=_auth(ctx["user_tok"]))
        patch = await c.patch(
            URL_WORK_TICKET_DISPATCH_CAP,
            headers=_auth(ctx["user_tok"]),
            json={"max_inflight": 12},
        )
    assert (get.status_code, patch.status_code) == (403, 403)
    assert ctx["semaphore"].override is None
