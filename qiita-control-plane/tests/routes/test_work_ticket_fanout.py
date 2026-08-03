"""Route tests for the fan-out control surface under /api/v1/work-ticket/fanout.

GET lists active cohorts with their throttle state; PATCH retunes one cohort's cap
and pumps it in the same call; POST .../pump re-triggers a pump without changing the
cap. All three are gated on `work_ticket:cancel` (system_admin).

The pump is edge-triggered — nothing re-evaluates a cohort on its own — so a PATCH
that only recorded the cap would appear to do nothing until an unrelated ticket
finished. Pumping inline is the feature, not a convenience, and the tests pin it.
"""

import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_WORK_TICKET_FANOUT,
    URL_WORK_TICKET_FANOUT_COHORT,
    URL_WORK_TICKET_FANOUT_COHORT_PUMP,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import MAX_FANOUT_OVERRIDE, FanoutCohortKind

pytestmark = pytest.mark.db


@pytest.fixture(autouse=True)
def _clear_fanout_overrides():
    """`_OVERRIDES` is a process-global dict, and these tests set overrides through
    PATCH. Without this, one test's override leaks into the next through the module
    registry and the failure lands in whichever test happens to run second — an
    ordering-dependent break that says nothing about the code. Mirrors the fixture in
    `test_fanout_dispatch.py`, which reaches the same registry directly."""
    from qiita_control_plane.fanout_dispatch import _OVERRIDES

    _OVERRIDES.clear()
    yield
    _OVERRIDES.clear()


def _pump_url(kind: str, key: int) -> str:
    return URL_WORK_TICKET_FANOUT_COHORT_PUMP.format(kind=kind, key=key)


def _cohort_url(kind: str, key: int) -> str:
    return URL_WORK_TICKET_FANOUT_COHORT.format(kind=kind, key=key)


@pytest_asyncio.fixture
async def ctx(postgres_pool, monkeypatch):
    """App + a system_admin token carrying work_ticket:cancel, a scopeless user
    token for the 403s, and a shard cohort of held tickets. `schedule_dispatch` is
    stubbed to record idxs rather than start real workflow tasks."""
    from qiita_control_plane.auth.token import mint_api_token
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app
    from qiita_control_plane.routes import work_ticket as wt_routes

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused", flight_signing_key=b"\x00" * 32, data_plane_url="unused"
    )
    app.state.compute_backend_client = object()
    app.state.running_dispatches = set()

    dispatched: list[int] = []
    monkeypatch.setattr(
        wt_routes, "schedule_dispatch", lambda _app, idx, **_kw: dispatched.append(idx)
    )

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
        SystemRole.SYSTEM_ADMIN, "wtf-admin", [Scope.WORK_TICKET_CANCEL]
    )
    user_idx, user_tok = await _principal(SystemRole.USER, "wtf-user", [Scope.SELF_PROFILE])

    action_id, action_version = "build-shard-index", f"v-{suffix}"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'reference', '{}'::text[],"
        '          \'{"service": false, "human_roles": ["system_admin"]}\'::jsonb,'
        "         '{}'::jsonb, '[]'::jsonb, 1, 1, '1 minute', NULL, 'failed')",
        action_id,
        action_version,
    )

    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', 'indexing', $2) RETURNING reference_idx",
        f"wtf-{suffix}",
        admin_idx,
    )

    async def seed_shards(n: int) -> list[int]:
        idxs = []
        for shard_id in range(n):
            idxs.append(
                await postgres_pool.fetchval(
                    "INSERT INTO qiita.work_ticket ("
                    "  action_id, action_version, originator_principal_idx,"
                    "  scope_target_kind, reference_idx, shard_id, dispatch_held"
                    ") VALUES ($1, $2, $3, 'reference', $4, $5, true)"
                    " RETURNING work_ticket_idx",
                    action_id,
                    action_version,
                    admin_idx,
                    reference_idx,
                    shard_id,
                )
            )
        return idxs

    yield {
        "pool": postgres_pool,
        "admin_tok": admin_tok,
        "user_tok": user_tok,
        "reference_idx": reference_idx,
        "seed_shards": seed_shards,
        "dispatched": dispatched,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
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


def _auth(tok):
    return {"Authorization": f"Bearer {tok}"}


# ---------------------------------------------------------------------------
# GET /work-ticket/fanout
# ---------------------------------------------------------------------------


async def test_get_lists_the_active_cohort_with_its_status(ctx):
    await ctx["seed_shards"](5)
    async with _client() as c:
        resp = await c.get(URL_WORK_TICKET_FANOUT, headers=_auth(ctx["admin_tok"]))
    assert resp.status_code == 200
    mine = [
        row
        for row in resp.json()["cohorts"]
        if row["kind"] == FanoutCohortKind.SHARD and row["key"] == ctx["reference_idx"]
    ]
    assert len(mine) == 1
    assert mine[0]["held"] == 5
    assert mine[0]["running"] == 0
    assert mine[0]["fail_stopped"] is False
    assert mine[0]["override"] is None


async def test_get_requires_the_cancel_scope(ctx):
    async with _client() as c:
        resp = await c.get(URL_WORK_TICKET_FANOUT, headers=_auth(ctx["user_tok"]))
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# PATCH /work-ticket/fanout/{kind}/{key}
# ---------------------------------------------------------------------------


async def test_patch_sets_the_cap_and_pumps_in_the_same_call(ctx):
    idxs = await ctx["seed_shards"](5)
    async with _client() as c:
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": 3},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["released"] == idxs[:3]
    assert body["status"]["max_inflight"] == 3
    assert body["status"]["override"] == 3
    assert body["status"]["held"] == 2
    assert body["status"]["running"] == 3
    # Released tickets are actually handed to the dispatcher, not just un-held.
    assert ctx["dispatched"] == idxs[:3]


async def test_patch_null_clears_the_override(ctx):
    await ctx["seed_shards"](5)
    async with _client() as c:
        await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": 3},
            headers=_auth(ctx["admin_tok"]),
        )
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": None},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 200
    assert resp.json()["status"]["override"] is None


@pytest.mark.parametrize("bad", [0, -1, MAX_FANOUT_OVERRIDE + 1])
async def test_patch_rejects_a_cap_outside_the_ceiling(ctx, bad):
    await ctx["seed_shards"](2)
    async with _client() as c:
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": bad},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 422


async def test_patch_rejects_an_unknown_kind(ctx):
    async with _client() as c:
        resp = await c.patch(
            _cohort_url("not_a_kind", 1),
            json={"max_inflight": 4},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 422


async def test_patch_on_a_cohort_with_no_tickets_is_404(ctx):
    """A typo'd key must not silently record an override against nothing."""
    async with _client() as c:
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, 999_999_999),
            json={"max_inflight": 4},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 404


async def test_patch_succeeds_on_a_fully_released_cohort(ctx):
    """Existence is `total`, not `held`. A cohort that has released everything still
    exists and must stay retunable — an `held == 0` check would 404 it."""
    await ctx["seed_shards"](3)
    async with _client() as c:
        await c.post(
            _pump_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            headers=_auth(ctx["admin_tok"]),
        )
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": 4},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 200
    assert resp.json()["status"]["held"] == 0
    assert resp.json()["status"]["running"] == 3


async def test_patch_succeeds_on_a_finished_cohort(ctx):
    """All-terminal is still a cohort that existed: `total` counts terminal tickets,
    so this is a 200 with everything zero but `total`, not a 404."""
    await ctx["seed_shards"](3)
    await ctx["pool"].execute(
        "UPDATE qiita.work_ticket SET state='completed', dispatch_held=false"
        " WHERE reference_idx=$1",
        ctx["reference_idx"],
    )
    async with _client() as c:
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": 4},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"]["total"] == 3
    assert (body["status"]["held"], body["status"]["running"]) == (0, 0)
    assert body["released"] == []


async def test_get_still_lists_a_drained_cohort_that_kept_its_override(ctx):
    """An override outlives its cohort, so the list must too.

    Nothing evicts from the registry except an explicit clear, and `active_cohorts`
    drops a cohort the moment its last child goes terminal. Listing only active cohorts
    made a set override unenumerable at exactly the point it became a surprise — it
    still reapplies if that (kind, key) is ever re-run, and an operator who set three
    during an incident had no way to ask "what have I set?".
    """
    await ctx["seed_shards"](3)
    async with _client() as c:
        patch = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": 2},
            headers=_auth(ctx["admin_tok"]),
        )
        assert patch.status_code == 200

        # Drain it completely: every child terminal, nothing held or in flight.
        await ctx["pool"].execute(
            "UPDATE qiita.work_ticket SET state='completed', dispatch_held=false"
            " WHERE reference_idx=$1",
            ctx["reference_idx"],
        )

        resp = await c.get(URL_WORK_TICKET_FANOUT, headers=_auth(ctx["admin_tok"]))

    assert resp.status_code == 200
    mine = [
        row
        for row in resp.json()["cohorts"]
        if row["kind"] == FanoutCohortKind.SHARD and row["key"] == ctx["reference_idx"]
    ]
    assert len(mine) == 1, "a drained cohort with an override set must still be listed"
    # Zero counts with a non-null override is the shape that reads as "set, with nothing
    # left to apply it to" — which is the state an operator needs to see to clear it.
    assert (mine[0]["held"], mine[0]["running"]) == (0, 0)
    assert mine[0]["override"] == 2
    assert mine[0]["max_inflight"] == 2


async def test_get_does_not_list_a_drained_cohort_with_no_override(ctx):
    """The union is overrides only — a finished cohort with nothing set still drops out,
    so the list doesn't accumulate every fan-out the deploy has ever run."""
    await ctx["seed_shards"](3)
    await ctx["pool"].execute(
        "UPDATE qiita.work_ticket SET state='completed', dispatch_held=false"
        " WHERE reference_idx=$1",
        ctx["reference_idx"],
    )
    async with _client() as c:
        resp = await c.get(URL_WORK_TICKET_FANOUT, headers=_auth(ctx["admin_tok"]))

    assert resp.status_code == 200
    mine = [
        row
        for row in resp.json()["cohorts"]
        if row["kind"] == FanoutCohortKind.SHARD and row["key"] == ctx["reference_idx"]
    ]
    assert mine == []


async def test_get_lists_an_active_cohort_once_even_with_an_override(ctx):
    """The union dedupes on (kind, key): a cohort that is both active and overridden
    must appear once, not twice."""
    await ctx["seed_shards"](5)
    async with _client() as c:
        patch = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": 2},
            headers=_auth(ctx["admin_tok"]),
        )
        assert patch.status_code == 200
        resp = await c.get(URL_WORK_TICKET_FANOUT, headers=_auth(ctx["admin_tok"]))

    mine = [
        row
        for row in resp.json()["cohorts"]
        if row["kind"] == FanoutCohortKind.SHARD and row["key"] == ctx["reference_idx"]
    ]
    assert len(mine) == 1
    assert mine[0]["override"] == 2


async def test_fanout_list_is_not_shadowed_by_the_int_path_param(ctx):
    """`GET /work-ticket/fanout` must be declared before `GET /{work_ticket_idx}`.

    Starlette matches in definition order, so reordering makes this route 422 on
    int("fanout"). This pins the ordering explicitly rather than leaving it to be
    caught incidentally by an unrelated-looking assertion elsewhere.
    """
    async with _client() as c:
        resp = await c.get(URL_WORK_TICKET_FANOUT, headers=_auth(ctx["admin_tok"]))
    assert resp.status_code != 422, "GET /fanout was captured by GET /{work_ticket_idx}"
    assert resp.status_code == 200


async def test_patch_reports_fail_stopped_instead_of_a_bare_zero(ctx):
    """The incident case: raising the cap on a fail-stopped cohort releases nothing,
    and the operator must be told why rather than seeing an empty list."""
    idxs = await ctx["seed_shards"](5)
    await ctx["pool"].execute(
        "UPDATE qiita.work_ticket SET state='failed', dispatch_held=false,"
        " failure_type='permanent', failure_stage='submission', failure_reason='boom'"
        " WHERE work_ticket_idx=$1",
        idxs[0],
    )
    async with _client() as c:
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": MAX_FANOUT_OVERRIDE},
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["released"] == []
    assert body["status"]["fail_stopped"] is True
    assert body["status"]["failed"] == 1


async def test_patch_requires_the_cancel_scope(ctx):
    async with _client() as c:
        resp = await c.patch(
            _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            json={"max_inflight": 4},
            headers=_auth(ctx["user_tok"]),
        )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /work-ticket/fanout/{kind}/{key}/pump
# ---------------------------------------------------------------------------


async def test_pump_releases_without_changing_the_cap(ctx):
    """The recovery path for a cohort stranded by a failed completion-hook pump."""
    idxs = await ctx["seed_shards"](5)
    async with _client() as c:
        resp = await c.post(
            _pump_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            headers=_auth(ctx["admin_tok"]),
        )
    assert resp.status_code == 200
    body = resp.json()
    # Default cap is 8, so all 5 release; no override is recorded.
    assert body["released"] == idxs
    assert body["status"]["override"] is None
    assert ctx["dispatched"] == idxs


@pytest.mark.parametrize("verb", ["patch", "pump"])
async def test_mutating_routes_503_without_an_orchestrator(ctx, verb):
    """Without a dispatch path, a pump would strand everything it releases.

    `schedule_dispatch` raises when `compute_backend_client` is None, the pump's
    per-ticket guard swallows it, and the release is already committed — so a 200 here
    would report tickets as `running` that are un-held, undispatched, and beyond the
    reach of any later pump. 503 before touching anything instead.
    """
    from qiita_control_plane.main import app

    await ctx["seed_shards"](3)
    saved = app.state.compute_backend_client
    app.state.compute_backend_client = None
    try:
        async with _client() as c:
            if verb == "patch":
                resp = await c.patch(
                    _cohort_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
                    json={"max_inflight": 4},
                    headers=_auth(ctx["admin_tok"]),
                )
            else:
                resp = await c.post(
                    _pump_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
                    headers=_auth(ctx["admin_tok"]),
                )
        assert resp.status_code == 503
    finally:
        app.state.compute_backend_client = saved
    # Nothing was released, so the cohort is untouched and still recoverable.
    held = await ctx["pool"].fetchval(
        "SELECT count(*) FROM qiita.work_ticket"
        " WHERE reference_idx = $1 AND shard_id IS NOT NULL AND dispatch_held",
        ctx["reference_idx"],
    )
    assert held == 3


async def test_get_still_works_without_an_orchestrator(ctx):
    """The read-only route is not gated: inspecting a stuck fan-out is exactly what
    an operator needs when the orchestrator is the thing that is misconfigured."""
    from qiita_control_plane.main import app

    await ctx["seed_shards"](3)
    saved = app.state.compute_backend_client
    app.state.compute_backend_client = None
    try:
        async with _client() as c:
            resp = await c.get(URL_WORK_TICKET_FANOUT, headers=_auth(ctx["admin_tok"]))
        assert resp.status_code == 200
    finally:
        app.state.compute_backend_client = saved


async def test_pump_requires_the_cancel_scope(ctx):
    async with _client() as c:
        resp = await c.post(
            _pump_url(FanoutCohortKind.SHARD, ctx["reference_idx"]),
            headers=_auth(ctx["user_tok"]),
        )
    assert resp.status_code == 403
