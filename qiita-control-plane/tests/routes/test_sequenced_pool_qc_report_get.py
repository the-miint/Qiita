"""Route tests for GET /sequencing-run/{run}/sequenced-pool/{pool}/qc-report —
the pool's merged (multiqc-equivalent) QC report.

Covers the happy path (merged aggregate + per-sample detail + read-metric
rollup), the empty pool, and the read gate (404 missing pool, 422 pool-not-in-
run, 401 anonymous, 403 missing scope / regular user). Merge arithmetic across
samples lives in qiita-common's test_qc_report_merge; here the wiring, response
model, JSONB decode, and auth are exercised. Uses the shared role-keyed clients.
"""

import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_SEQUENCED_POOL_QC_REPORT

from qiita_control_plane.main import app
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

pytestmark = pytest.mark.db


def _mate(reads, total_bases, mean_quality):
    return {
        "reads": reads,
        "total_bases": total_bases,
        "mean_quality": mean_quality,
        "gc_content": 0.5,
        "n_content": 0.0,
        "min_length": 100,
        "max_length": 100,
        "mean_length": 100.0,
        "quality_histogram": {str(int(mean_quality)): reads},
        "gc_histogram": {"50": reads},
        "length_histogram": {"100": reads},
    }


def _report(point, read_pairs, mate):
    return {
        "point": point,
        "layout": "single",
        "read_pairs": read_pairs,
        "mates": {"r1": mate, "r2": None},
    }


@pytest.fixture
def ctx(role_keyed_clients):
    """Alias the shared role-keyed clients ({pool, wet, user, wet_session, ...})."""
    return role_keyed_clients


async def _attach_sample_with_reports(db, *, owner, pool_idx, item_id, raw_mate, filt_mate):
    """Seed a biosample + sequenced prep_sample, attach it to an EXISTING pool as
    a sequenced_sample, and write its raw/filtered QC reports. Returns
    (biosample_idx, prep_sample_idx, sequenced_sample_idx)."""
    bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    ss_idx = await db.fetchval(
        "INSERT INTO qiita.sequenced_sample"
        "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx,"
        "   raw_qc_report, filtered_qc_report)"
        " VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb) RETURNING idx",
        ps_idx,
        pool_idx,
        item_id,
        owner,
        json.dumps(_report("raw", raw_mate["reads"], raw_mate)),
        json.dumps(_report("filtered", filt_mate["reads"], filt_mate)),
    )
    return bs_idx, ps_idx, ss_idx


@pytest_asyncio.fixture
async def seeded_pool(ctx):
    """Seed a run + pool with two processed sequenced_samples carrying QC reports
    (raw mean_quality 30 over 1000 bases, 20 over 3000 → pooled 22.5); FK-reverse
    cleanup. The first sample establishes the run + pool via the shared helper;
    the second attaches to that same pool with a direct insert."""
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    created = []

    # First sample: helper creates the run + pool.
    bs0, ps0 = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, ss0 = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps0, owner_idx=owner, sequenced_pool_item_id="qc-item-0"
    )
    await db.execute(
        "UPDATE qiita.sequenced_sample SET raw_qc_report = $2::jsonb,"
        " filtered_qc_report = $3::jsonb WHERE idx = $1",
        ss0,
        json.dumps(_report("raw", 10, _mate(10, 1000, 30.0))),
        json.dumps(_report("filtered", 9, _mate(9, 900, 31.0))),
    )
    created.append((bs0, ps0, ss0))

    # Second sample: same pool, distinct item id.
    created.append(
        await _attach_sample_with_reports(
            db,
            owner=owner,
            pool_idx=pool_idx,
            item_id="qc-item-1",
            raw_mate=_mate(30, 3000, 20.0),
            filt_mate=_mate(27, 2700, 21.0),
        )
    )

    yield {"run_idx": run_idx, "pool_idx": pool_idx, "samples": created}

    for _bs, _ps, ss_idx in created:
        await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    for _bs, ps_idx, _ss in created:
        await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
    for bs_idx, _ps, _ss in created:
        await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)


def _url(run_idx, pool_idx):
    return URL_SEQUENCED_POOL_QC_REPORT.format(
        sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
    )


async def test_get_qc_report_merges_samples(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded_pool["pool_idx"]
    assert body["sequencing_run_idx"] == seeded_pool["run_idx"]
    assert body["sample_count"] == 2
    assert body["samples_with_qc_report"] == 2
    assert len(body["samples"]) == 2
    # raw r1 base-weighted mean: (30*1000 + 20*3000)/4000 = 22.5
    raw_r1 = body["merged"]["raw"]["mates"]["r1"]
    assert raw_r1["reads"] == 40
    assert raw_r1["mean_quality"] == pytest.approx(22.5)
    assert raw_r1["quality_histogram"] == {"20": 30, "30": 10}
    assert body["merged"]["raw"]["mates"]["r2"] is None
    # filtered point present and independent
    assert body["merged"]["filtered"]["read_pairs"] == 9 + 27


async def test_get_qc_report_empty_pool(ctx, seeded_pool):
    """A pool whose samples carry no QC reports yields merged None at both
    points and an empty samples list — seed a fresh pool with no reports."""
    db = ctx["pool"]
    owner = ctx["wet_session"]["principal_idx"]
    bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        db, prep_sample_idx=ps_idx, owner_idx=owner, sequenced_pool_item_id="empty-1"
    )
    try:
        resp = await ctx["wet"].get(_url(run_idx, pool_idx))
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["sample_count"] == 1
        assert body["samples_with_qc_report"] == 0
        assert body["merged"]["raw"] is None
        assert body["merged"]["filtered"] is None
        assert body["samples"][0]["raw_qc_report"] is None
    finally:
        await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
        await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
        await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
        await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)


async def test_get_qc_report_unknown_pool_404(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"], 999_999_999))
    assert resp.status_code == 404


async def test_get_qc_report_wrong_run_422(ctx, seeded_pool):
    resp = await ctx["wet"].get(_url(seeded_pool["run_idx"] + 10_000, seeded_pool["pool_idx"]))
    assert resp.status_code == 422


async def test_get_qc_report_anonymous_401(ctx, seeded_pool):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 401


async def test_get_qc_report_missing_scope_403(seeded_pool, no_prep_sample_read_client):
    resp = await no_prep_sample_read_client.get(
        _url(seeded_pool["run_idx"], seeded_pool["pool_idx"])
    )
    assert resp.status_code == 403


async def test_get_qc_report_regular_user_403(ctx, seeded_pool):
    resp = await ctx["user"].get(_url(seeded_pool["run_idx"], seeded_pool["pool_idx"]))
    assert resp.status_code == 403
