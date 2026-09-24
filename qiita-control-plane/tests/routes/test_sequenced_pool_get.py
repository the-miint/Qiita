"""Route tests for GET /sequencing-run/{run}/sequenced-pool/{pool} — the pool
read-metric rollup.

Covers the happy path (rollup shape + computed fraction) and the read gate
(404 missing pool, 422 pool-not-in-run, 401 anonymous, 403 missing scope /
regular user). Aggregation correctness across many samples lives in the
repositories test; here a single processed sample exercises the route wiring,
response model, and auth. Uses the shared `ctx` (role-keyed clients + db pool)
fixture from tests/routes/conftest.py.
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SEQUENCED_POOL_BY_IDX

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

from .conftest import make_caller_own_run

pytestmark = pytest.mark.db


@pytest.fixture
def ctx(role_keyed_clients):
    """Alias the shared role-keyed clients ({pool, wet, user, wet_session, ...});
    this route needs no per-test `created` tracker (seeded_pool owns cleanup)."""
    return role_keyed_clients


@pytest_asyncio.fixture
async def seeded_pool(ctx):
    """Seed a run + pool + one processed sequenced_sample (raw=1000, bio=900,
    qf=850) owned by the wet-admin principal; FK-reverse cleanup."""
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps_idx, owner_idx=owner, sequenced_pool_item_id="rollup-item-1"
    )
    await db.execute(
        "UPDATE qiita.sequenced_sample SET raw_read_count_r1r2 = 1000,"
        " biological_read_count_r1r2 = 900, quality_filtered_read_count_r1r2 = 850,"
        " spikein_read_count_r1r2 = 40 WHERE idx = $1",
        ss_idx,
    )
    yield {"run_idx": run_idx, "pool_idx": pool_idx, "ss_idx": ss_idx}
    await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
    await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)


def _url(run_idx, pool_idx):
    return URL_SEQUENCED_POOL_BY_IDX.format(sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx)


async def test_get_pool_returns_rollup(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded_pool["pool_idx"]
    assert body["sequencing_run_idx"] == seeded_pool["run_idx"]
    rm = body["read_metrics"]
    assert rm["raw_read_count_r1r2"] == 1000
    assert rm["biological_read_count_r1r2"] == 900
    assert rm["quality_filtered_read_count_r1r2"] == 850
    # The spikein SUM + the route's .pop() — a NULL-only fixture would pass with
    # the rollup column missing entirely.
    assert rm["spikein_read_count_r1r2"] == 40
    assert rm["sample_count"] == 1
    assert rm["samples_with_metrics"] == 1
    assert rm["fraction_passing_quality_filter"] == pytest.approx(0.85)


async def test_get_pool_unknown_pool_404(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], 999_999_999))
    assert resp.status_code == 404


async def test_get_pool_wrong_run_422(ctx, seeded_pool):
    # The pool exists but the path's run doesn't own it → require_sequenced_pool_in_run 422.
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"] + 10_000, seeded_pool["pool_idx"]))
    assert resp.status_code == 422


async def test_get_pool_anonymous_401(ctx, seeded_pool):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 401


async def test_get_pool_missing_scope_403(seeded_pool, no_prep_sample_read_client):
    resp = await no_prep_sample_read_client.get(
        _url(seeded_pool["run_idx"], seeded_pool["pool_idx"])
    )
    assert resp.status_code == 403


async def test_get_pool_regular_user_403(ctx, seeded_pool):
    # Non-owner: the run was created by the wet-admin principal, so the plain user
    # is refused for not owning it rather than for lacking a role.
    resp = await ctx["user"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 403


async def test_get_pool_owner_unknown_pool_404_and_wrong_run_403(ctx, seeded_pool):
    """The guard runs BEFORE `require_sequenced_pool_in_run`, so a non-admin owner
    sees 404/422 only for pools under a run they own.

    The admin cases above pin 404/422 through the guard's role bypass, which
    returns before any DB lookup. This is the other branch: the owner of run R
    gets 404 for a pool that does not exist under R, and 403 — not 422 — when the
    path names a run they do not own, because ownership is decided first.
    """
    await make_caller_own_run(
        ctx, seeded_pool["run_idx"], principal_idx=ctx["user_session"]["principal_idx"]
    )
    missing = await ctx["user"].get(_url(seeded_pool["run_idx"], 999_999_999))
    assert missing.status_code == 404, missing.text

    foreign = await ctx["user"].get(_url(seeded_pool["run_idx"] + 10_000, seeded_pool["pool_idx"]))
    assert foreign.status_code in (403, 404), foreign.text


async def test_get_pool_run_creator_can_read(ctx, seeded_pool):
    """A plain user who created the RUN reads its pool's rollup.

    Ownership, not role, is what admits them: the same principal is refused above
    on the identical URL while the run belongs to someone else.
    """
    await make_caller_own_run(
        ctx, seeded_pool["run_idx"], principal_idx=ctx["user_session"]["principal_idx"]
    )
    resp = await ctx["user"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    assert resp.json()["sequenced_pool_idx"] == seeded_pool["pool_idx"]
