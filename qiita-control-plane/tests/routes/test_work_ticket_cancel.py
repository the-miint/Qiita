"""Route + reap-helper tests for POST /api/v1/work-ticket/cancel and
`work_ticket_cancel.cancel_work_ticket`.

Covers: the terminal-first flip (non-terminal → cancelled) + scancel reap, the
already-terminal no-op (still reaps), the not-found path, the action_id (+pool)
filter, the missing-scope 403, and the reap_error surface (flip lands, scancel
fails). A fake backend client records the reap and scripts its result / failure.
"""

import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_WORK_TICKET_CANCEL
from qiita_common.auth_constants import Scope, SystemRole

pytestmark = pytest.mark.db


class _FakeBackendClient:
    """Records cancel() calls and scripts the response. Set `raise_on` to a ticket
    idx to simulate a reap failure after the flip."""

    def __init__(self) -> None:
        self.cancel_calls: list[int] = []
        self.result_ids: list[int] = [518235]
        self.raise_on: int | None = None

    async def cancel(self, work_ticket_idx: int) -> list[int]:
        self.cancel_calls.append(work_ticket_idx)
        if self.raise_on == work_ticket_idx:
            raise RuntimeError("orchestrator unreachable")
        return list(self.result_ids)


@pytest_asyncio.fixture
async def ctx(postgres_pool):
    """App wired with a fake backend client + a system_admin token carrying
    work_ticket:cancel, plus seed_ticket(state) / seed_action helpers. FK-reverse
    cleanup."""
    from qiita_control_plane.auth.token import mint_api_token
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )
    backend = _FakeBackendClient()
    app.state.compute_backend_client = backend
    app.state.running_dispatches = set()

    suffix = uuid.uuid4().hex[:8]
    admin_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        f"wtc-admin-{suffix}",
        SystemRole.SYSTEM_ADMIN,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, 'X', 'Y', 'Z')",
        admin_idx,
        f"wtc-admin-{suffix}@example.com",
    )
    admin_tok, _ = await mint_api_token(
        postgres_pool,
        principal_idx=admin_idx,
        label="wtc-admin",
        scopes=[Scope.WORK_TICKET_CANCEL],
    )
    # A user-role principal WITHOUT the cancel scope, for the 403 test.
    user_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        f"wtc-user-{suffix}",
        SystemRole.USER,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, 'X', 'Y', 'Z')",
        user_idx,
        f"wtc-user-{suffix}@example.com",
    )
    user_tok, _ = await mint_api_token(
        postgres_pool, principal_idx=user_idx, label="wtc-user", scopes=[Scope.SELF_PROFILE]
    )

    action_id = "read-mask"
    action_version = f"v-{suffix}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, target_processing_kinds,"
        "  scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status"
        ") VALUES ($1, $2, 'reference'::qiita.scope_target_kind,"
        "          ARRAY[]::qiita.processing_kind[], ARRAY['reference:write']::text[],"
        '          \'{"service": false, "human_roles": ["user"]}\'::jsonb,'
        "          '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        action_id,
        action_version,
    )

    tickets: list[int] = []
    refs: list[int] = []

    async def seed_ticket(state: str = "processing") -> int:
        ref_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
            " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2) RETURNING reference_idx",
            f"wtc-{uuid.uuid4()}",
            admin_idx,
        )
        refs.append(ref_idx)
        failed = state == "failed"
        idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            "  (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "   reference_idx, state, failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'reference', $4, $5::qiita.work_ticket_state, $6, $7, $8)"
            " RETURNING work_ticket_idx",
            action_id,
            action_version,
            admin_idx,
            ref_idx,
            state,
            "permanent" if failed else None,
            "finalize" if failed else None,
            "boom" if failed else None,
        )
        tickets.append(idx)
        return idx

    yield {
        "pool": postgres_pool,
        "backend": backend,
        "admin_tok": admin_tok,
        "user_tok": user_tok,
        "seed_ticket": seed_ticket,
        "action_id": action_id,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])", tickets
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])", refs
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, action_version
    )
    for pidx in (admin_idx, user_idx):
        await postgres_pool.execute("DELETE FROM qiita.api_token WHERE principal_idx = $1", pidx)
        await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", pidx)
        await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", pidx)


def _client():
    from qiita_control_plane.main import app

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def _state(pool, idx) -> str:
    return await pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", idx
    )


async def test_cancel_flips_terminal_then_reaps(ctx):
    idx = await ctx["seed_ticket"]("processing")
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["requested"] == 1
    assert body["cancelled"] == 1
    (r,) = body["results"]
    assert r["work_ticket_idx"] == idx
    assert r["previous_state"] == "processing"
    assert r["state"] == "cancelled"
    assert r["cancelled"] is True
    assert r["cancelled_job_ids"] == [518235]
    # State really flipped, and the reap ran AFTER (terminal-first).
    assert await _state(ctx["pool"], idx) == "cancelled"
    assert ctx["backend"].cancel_calls == [idx]


async def test_cancel_already_terminal_is_no_op_but_still_reaps(ctx):
    """An already-terminal ticket is not re-flipped (cancelled=False) but its jobs
    are still reaped — the defensive orphan-reap property."""
    idx = await ctx["seed_ticket"]("failed")
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    (r,) = resp.json()["results"]
    assert r["cancelled"] is False
    assert r["previous_state"] == "failed"
    assert r["state"] == "failed"  # unchanged
    assert await _state(ctx["pool"], idx) == "failed"
    assert ctx["backend"].cancel_calls == [idx]  # reap still ran


async def test_cancel_not_found_idx(ctx):
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [999_999_999]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    (r,) = resp.json()["results"]
    assert r["not_found"] is True
    assert r["cancelled"] is False
    assert ctx["backend"].cancel_calls == []  # no reap for a missing ticket


async def test_cancel_filter_by_action_id(ctx):
    """The action_id filter selects the non-terminal tickets for that action; a
    terminal ticket for the same action is excluded from the filter."""
    a = await ctx["seed_ticket"]("processing")
    b = await ctx["seed_ticket"]("queued")
    done = await ctx["seed_ticket"]("completed")  # terminal → excluded from filter
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"action_id": ctx["action_id"]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    got = {r["work_ticket_idx"] for r in body["results"]}
    assert {a, b} <= got
    assert done not in got
    assert await _state(ctx["pool"], a) == "cancelled"
    assert await _state(ctx["pool"], b) == "cancelled"


async def test_cancel_reap_error_surfaces_but_flip_stands(ctx):
    """A reap that fails AFTER the flip lands comes back with reap_error, and the
    ticket is still terminal (the important half landed)."""
    idx = await ctx["seed_ticket"]("processing")
    ctx["backend"].raise_on = idx
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    (r,) = resp.json()["results"]
    assert r["cancelled"] is True
    assert r["reap_error"] is not None
    assert r["cancelled_job_ids"] == []
    assert await _state(ctx["pool"], idx) == "cancelled"  # flip stands


async def test_cancel_requires_work_ticket_cancel_scope(ctx):
    idx = await ctx["seed_ticket"]("processing")
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['user_tok']}"},
        )
    assert resp.status_code == 403
    assert await _state(ctx["pool"], idx) == "processing"  # untouched


async def test_cancel_anonymous_401(ctx):
    idx = await ctx["seed_ticket"]("processing")
    async with _client() as c:
        resp = await c.post(URL_WORK_TICKET_CANCEL, json={"work_ticket_idxs": [idx]})
    assert resp.status_code == 401
