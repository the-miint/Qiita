"""Route + reap-helper tests for POST /api/v1/work-ticket/cancel and
`work_ticket_cancel.cancel_work_ticket`.

Covers: the terminal-first flip (non-terminal or failed → cancelled) + scancel
reap, the completed / no_data / cancelled no-op (still reaps), the not-found path,
the action_id (+pool) filter, the missing-scope 403, and the reap_error surface
(flip lands, scancel fails). A fake backend client records the reap and scripts
its result / failure.
"""

import asyncio
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_WORK_TICKET_CANCEL
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import WorkTicketState

from qiita_control_plane.ticket_redrive import ReapPending, reset_ticket_to_pending
from qiita_control_plane.work_ticket_cancel import cancel_work_ticket

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
    from qiita_control_plane.dispatch import build_dispatch_semaphore
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )
    backend = _FakeBackendClient()
    saved_compute_backend_client = getattr(app.state, "compute_backend_client", None)
    app.state.compute_backend_client = backend
    app.state.running_dispatches = set()
    app.state.dispatch_semaphore = build_dispatch_semaphore()

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

    app.state.compute_backend_client = saved_compute_backend_client
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


@pytest.mark.parametrize(
    "state",
    [WorkTicketState.COMPLETED, WorkTicketState.NO_DATA, WorkTicketState.CANCELLED],
)
async def test_cancel_completed_no_data_or_cancelled_is_no_op_but_still_reaps(ctx, state):
    """A completed, no_data or cancelled ticket is not flipped (cancelled=False) but
    its jobs are still reaped — the defensive orphan-reap property."""
    idx = await ctx["seed_ticket"](state.value)
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    (r,) = resp.json()["results"]
    assert r["cancelled"] is False
    assert r["previous_state"] == state.value
    assert r["state"] == state.value  # unchanged
    assert await _state(ctx["pool"], idx) == state.value
    assert ctx["backend"].cancel_calls == [idx]  # reap still ran


async def test_cancel_failed_ticket_flips_clears_failure_and_reaps(ctx):
    """A failed ticket named by idx is flipped to cancelled, its failure_* columns
    are cleared (work_ticket_failure_consistent allows them only on failed), and the
    reap runs after the flip."""
    idx = await ctx["seed_ticket"](WorkTicketState.FAILED.value)
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cancelled"] == 1
    (r,) = body["results"]
    assert (r["previous_state"], r["state"], r["cancelled"]) == (
        WorkTicketState.FAILED.value,
        WorkTicketState.CANCELLED.value,
        True,
    )
    assert r["cancelled_job_ids"] == [518235]
    assert r["reap_error"] is None
    row = await ctx["pool"].fetchrow(
        "SELECT state, failure_type, failure_stage, failure_step_name, failure_reason"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        idx,
    )
    assert dict(row) == {
        "state": WorkTicketState.CANCELLED.value,
        "failure_type": None,
        "failure_stage": None,
        "failure_step_name": None,
        "failure_reason": None,
    }
    assert ctx["backend"].cancel_calls == [idx]

    # A second cancel is a no-op on the now-cancelled ticket and reaps again.
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    (r,) = resp.json()["results"]
    assert (r["previous_state"], r["cancelled"]) == (WorkTicketState.CANCELLED.value, False)
    assert ctx["backend"].cancel_calls == [idx, idx]


async def test_cancel_failed_ticket_reap_error_keeps_the_flip(ctx):
    """A failed ticket whose reap fails is still cancelled; reap_error says so."""
    idx = await ctx["seed_ticket"](WorkTicketState.FAILED.value)
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
    assert await _state(ctx["pool"], idx) == WorkTicketState.CANCELLED.value


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
    terminal ticket for the same action, failed included, is excluded from the
    filter."""
    a = await ctx["seed_ticket"]("processing")
    b = await ctx["seed_ticket"]("queued")
    done = await ctx["seed_ticket"]("completed")  # terminal → excluded from filter
    failed = await ctx["seed_ticket"](WorkTicketState.FAILED.value)
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
    assert failed not in got
    assert await _state(ctx["pool"], failed) == WorkTicketState.FAILED.value
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


async def _reap_mark(pool, idx):
    row = await pool.fetchrow(
        "SELECT reap_pending_since, reap_note FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        idx,
    )
    return row["reap_pending_since"], row["reap_note"]


@pytest.mark.parametrize("state", ["processing", "failed", "completed"])
async def test_a_failed_reap_is_recorded_and_a_later_reap_clears_it(ctx, state):
    """Any cancel whose reap fails marks the ticket, whatever its state; re-running the
    cancel is the retry, and a reap without error clears the mark."""
    idx = await ctx["seed_ticket"](state)
    ctx["backend"].raise_on = idx
    h = {"Authorization": f"Bearer {ctx['admin_tok']}"}
    async with _client() as c:
        resp = await c.post(URL_WORK_TICKET_CANCEL, json={"work_ticket_idxs": [idx]}, headers=h)
        assert resp.status_code == 200, resp.text
        pending_since, failure = await _reap_mark(ctx["pool"], idx)
        assert pending_since is not None
        assert failure == "RuntimeError: orchestrator unreachable"

        ctx["backend"].raise_on = None
        resp = await c.post(URL_WORK_TICKET_CANCEL, json={"work_ticket_idxs": [idx]}, headers=h)
        assert resp.status_code == 200, resp.text
        assert resp.json()["results"][0]["reap_error"] is None
    assert await _reap_mark(ctx["pool"], idx) == (None, None)


class _BlockingBackendClient(_FakeBackendClient):
    """A reap that waits on `release` before returning or raising."""

    def __init__(self, *, fail: bool) -> None:
        super().__init__()
        self.reaping = asyncio.Event()
        self.release = asyncio.Event()
        self.fail = fail

    async def cancel(self, work_ticket_idx: int) -> list[int]:
        self.cancel_calls.append(work_ticket_idx)
        self.reaping.set()
        await self.release.wait()
        if self.fail:
            raise RuntimeError("orchestrator unreachable")
        return list(self.result_ids)


async def _redrive(pool, idx) -> bool:
    async with pool.acquire() as conn, conn.transaction():
        return await reset_ticket_to_pending(
            conn, work_ticket_idx=idx, from_state=WorkTicketState.CANCELLED.value
        )


@pytest.mark.parametrize("fail", [True, False])
async def test_a_redrive_during_the_reap_is_refused(ctx, fail):
    """The flip commits with the reap mark set, so a redrive that lands while the reap
    is still running is refused rather than dispatching a ticket whose jobs the reap
    has yet to reach. A failed reap keeps the ticket refused; a clean one clears it."""
    from qiita_control_plane.main import app

    idx = await ctx["seed_ticket"]("processing")
    backend = _BlockingBackendClient(fail=fail)
    app.state.compute_backend_client = backend
    h = {"Authorization": f"Bearer {ctx['admin_tok']}"}
    async with _client() as c:
        cancel = asyncio.create_task(
            c.post(URL_WORK_TICKET_CANCEL, json={"work_ticket_idxs": [idx]}, headers=h)
        )
        await asyncio.wait_for(backend.reaping.wait(), timeout=10)
        assert await _state(ctx["pool"], idx) == "cancelled"
        with pytest.raises(ReapPending, match="has not finished") as refused:
            await _redrive(ctx["pool"], idx)
        assert "failed" not in str(refused.value)
        backend.release.set()
        assert (await cancel).status_code == 200
    if fail:
        with pytest.raises(ReapPending, match="failed: RuntimeError: orchestrator unreachable"):
            await _redrive(ctx["pool"], idx)
    else:
        assert await _reap_mark(ctx["pool"], idx) == (None, None)
        assert await _redrive(ctx["pool"], idx) is True


async def test_an_earlier_cancels_clean_reap_leaves_a_later_cancels_mark(ctx):
    """Two cancels of one ticket overlap: A's reap finishing cleanly does not clear B's
    mark while B's reap still runs, so a redrive stays refused until B's reap ends."""
    idx = await ctx["seed_ticket"]("processing")
    first = _BlockingBackendClient(fail=False)
    second = _BlockingBackendClient(fail=False)
    a = asyncio.create_task(cancel_work_ticket(ctx["pool"], first, idx))
    await asyncio.wait_for(first.reaping.wait(), timeout=10)
    b = asyncio.create_task(cancel_work_ticket(ctx["pool"], second, idx))
    await asyncio.wait_for(second.reaping.wait(), timeout=10)

    first.release.set()
    assert (await a)["reap_error"] is None
    with pytest.raises(ReapPending, match="has not finished"):
        await _redrive(ctx["pool"], idx)

    second.release.set()
    assert (await b)["reap_error"] is None
    assert await _reap_mark(ctx["pool"], idx) == (None, None)
    assert await _redrive(ctx["pool"], idx) is True


async def test_a_cancel_whose_reap_succeeds_leaves_no_mark(ctx):
    idx = await ctx["seed_ticket"]("processing")
    async with _client() as c:
        resp = await c.post(
            URL_WORK_TICKET_CANCEL,
            json={"work_ticket_idxs": [idx]},
            headers={"Authorization": f"Bearer {ctx['admin_tok']}"},
        )
    assert resp.status_code == 200, resp.text
    assert await _reap_mark(ctx["pool"], idx) == (None, None)
