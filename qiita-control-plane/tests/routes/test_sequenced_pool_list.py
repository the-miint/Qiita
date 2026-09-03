"""Route tests for GET /sequencing-run/{run}/sequenced-pool — the pool listing.

This is the read that makes a `sequenced_pool_idx` obtainable, so the cases that
matter are the gate and the scoping: who may list a run's pools, and that a
listing returns that run's pools and no others.

The gate is `require_caller_owns_run()`, as it is on the POST on this path and on
the aggregate reads under it. `test_list_pools_creator_user_can_read` is what pins
that choice; without it the route would pass every other test here at a
wet_lab_admin floor.

Uses the shared `ctx` (role-keyed clients + db pool) fixture from
tests/routes/conftest.py.
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SEQUENCING_RUN_SEQUENCED_POOL

from qiita_control_plane.main import app

pytestmark = pytest.mark.db


@pytest.fixture
def ctx(role_keyed_clients):
    return role_keyed_clients


async def _seed_run(db, owner_idx: int, *, pools: int) -> tuple[int, list[int]]:
    """A run owned by `owner_idx` with `pools` pools, each with a distinct
    preflight filename so the listing's discriminator is exercised."""
    import secrets

    run_idx = await db.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"list-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    pool_idxs = []
    for lane in range(pools):
        pool_idxs.append(
            await db.fetchval(
                "INSERT INTO qiita.sequenced_pool"
                "  (sequencing_run_idx, run_preflight_blob, run_preflight_filename,"
                "   created_by_idx)"
                " VALUES ($1, $2, $3, $4) RETURNING idx",
                run_idx,
                # The blob/filename pair is co-populated or both NULL
                # (sequenced_pool_run_preflight_pair_consistent), and a run's pools
                # must differ in BOTH content and filename, so vary both.
                f"preflight-bytes-{lane}".encode(),
                f"lane-{lane}.db",
                owner_idx,
            )
        )
    return run_idx, pool_idxs


async def _drop_run(db, run_idx: int) -> None:
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE sequencing_run_idx = $1", run_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)


@pytest_asyncio.fixture
async def wet_run(ctx):
    """A two-pool run owned by the wet-admin principal, plus a second run with one
    pool so the listing has something it must NOT return."""
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    run_idx, pool_idxs = await _seed_run(db, owner, pools=2)
    other_run_idx, other_pool_idxs = await _seed_run(db, owner, pools=1)
    yield {
        "run_idx": run_idx,
        "pool_idxs": pool_idxs,
        "other_run_idx": other_run_idx,
        "other_pool_idx": other_pool_idxs[0],
    }
    await _drop_run(db, other_run_idx)
    await _drop_run(db, run_idx)


@pytest_asyncio.fixture
async def user_run(ctx):
    """A one-pool run owned by the PLAIN-USER principal — the creator case."""
    db = ctx["pool"]
    run_idx, pool_idxs = await _seed_run(db, ctx["user_session"]["principal_idx"], pools=1)
    yield {"run_idx": run_idx, "pool_idx": pool_idxs[0]}
    await _drop_run(db, run_idx)


def _url(run_idx: int) -> str:
    return URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=run_idx)


async def test_list_pools_returns_the_runs_pools_in_idx_order(ctx, wet_run):
    resp = await ctx["wet"].get(_url(wet_run["run_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequencing_run_idx"] == wet_run["run_idx"]
    assert body["count"] == 2
    assert body["truncated"] is False
    assert [p["sequenced_pool_idx"] for p in body["sequenced_pool"]] == sorted(wet_run["pool_idxs"])
    # The discriminator the listing exists to carry.
    assert [p["run_preflight_filename"] for p in body["sequenced_pool"]] == [
        "lane-0.db",
        "lane-1.db",
    ]


async def test_list_pools_excludes_other_runs_pools(ctx, wet_run):
    """A pool of a DIFFERENT run must not appear — the filter is the point of the
    route, and a listing that ignored it would still pass the shape test above."""
    resp = await ctx["wet"].get(_url(wet_run["run_idx"]))
    returned = {p["sequenced_pool_idx"] for p in resp.json()["sequenced_pool"]}
    assert wet_run["other_pool_idx"] not in returned


async def test_list_pools_omits_the_preflight_blob_and_read_metrics(ctx, wet_run):
    """The BYTEA blob never crosses the wire, and the per-sample rollup is the
    single-pool read's job — a list would pay it per row."""
    resp = await ctx["wet"].get(_url(wet_run["run_idx"]))
    row = resp.json()["sequenced_pool"][0]
    assert "run_preflight_blob" not in row
    assert "read_metrics" not in row


async def test_list_pools_creator_user_can_read(ctx, user_run):
    """The gate choice: a plain `user` who CREATED the run may list its pools.

    The per-sample reads (QC report, exceptions) still require wet_lab_admin —
    naming a run's pools discloses nothing about whose samples are on them.
    """
    resp = await ctx["user"].get(_url(user_run["run_idx"]))
    assert resp.status_code == 200, resp.text
    assert [p["sequenced_pool_idx"] for p in resp.json()["sequenced_pool"]] == [
        user_run["pool_idx"]
    ]


async def test_list_pools_non_creator_user_403(ctx, wet_run):
    """The other half of the gate: ownership is what admits a plain user, not the
    route being a read. A run they did not create stays closed."""
    resp = await ctx["user"].get(_url(wet_run["run_idx"]))
    assert resp.status_code == 403


async def test_list_pools_wet_lab_admin_bypasses_ownership(ctx, user_run):
    """`require_caller_owns_run()`'s default bypass_role — a wet-lab admin lists a
    run they did not create."""
    resp = await ctx["wet"].get(_url(user_run["run_idx"]))
    assert resp.status_code == 200, resp.text


async def test_list_pools_unknown_run_404(ctx):
    """`require_sequencing_run_exists` fires before the gate, so a typo'd run 404s
    rather than reading as an empty listing."""
    resp = await ctx["wet"].get(_url(999_999_999))
    assert resp.status_code == 404


async def test_list_pools_run_with_no_pools_is_empty_not_404(ctx):
    db = ctx["pool"]
    run_idx, _ = await _seed_run(db, ctx["wet_session"]["principal_idx"], pools=0)
    try:
        resp = await ctx["wet"].get(_url(run_idx))
        assert resp.status_code == 200, resp.text
        assert resp.json()["sequenced_pool"] == []
        assert resp.json()["count"] == 0
    finally:
        await _drop_run(db, run_idx)


async def test_list_pools_anonymous_401(ctx, wet_run):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_url(wet_run["run_idx"]))
    assert resp.status_code == 401


async def test_list_pools_missing_scope_403(wet_run, no_prep_sample_read_client):
    resp = await no_prep_sample_read_client.get(_url(wet_run["run_idx"]))
    assert resp.status_code == 403
