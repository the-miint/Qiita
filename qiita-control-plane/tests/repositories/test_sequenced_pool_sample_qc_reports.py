"""DB tests for fetch_sequenced_pool_sample_qc_reports — the per-sample QC-report
rows the merged pool report aggregates.

The repo function returns one row per NON-retired sequenced_sample in the pool,
carrying the two persisted QC-report JSONBs plus prep_sample_idx / item id,
ordered by prep_sample_idx. Its retired exclusion must match
fetch_sequenced_pool_read_metrics' so `sample_count` (the rollup) and the length
of this list agree. Each test seeds one principal + run + pool and attaches
samples via `pool_ctx.add_sample`; cleanup is FK-reverse on postgres_pool.
"""

import json
import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_sample_qc_reports,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


def _report(point):
    return {"point": point, "layout": "single", "read_pairs": 1, "mates": {"r1": None, "r2": None}}


@pytest_asyncio.fixture
async def pool_ctx(postgres_pool):
    """Seed a principal + one sequencing_run + one sequenced_pool; yield a context
    whose `add_sample(...)` attaches a sequenced_sample (with optional QC reports /
    retirement) to the pool. FK-reverse cleanup."""
    owner_idx = await seed_user_principal(postgres_pool, prefix="poolqc", suffix="owner")
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"pq-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner_idx,
    )
    samples: list[tuple[int, int, int]] = []

    async def add_sample(*, with_reports=False, retired=False):
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=owner_idx
        )
        ss_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
            " VALUES ($1, $2, $3, $4) RETURNING idx",
            ps_idx,
            pool_idx,
            f"item-{secrets.token_hex(4)}",
            owner_idx,
        )
        if with_reports:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET raw_qc_report = $2::jsonb,"
                " filtered_qc_report = $3::jsonb WHERE idx = $1",
                ss_idx,
                json.dumps(_report("raw")),
                json.dumps(_report("filtered")),
            )
        if retired:
            await postgres_pool.execute(
                "UPDATE qiita.prep_sample SET retired = true, retired_by_idx = $2,"
                " retired_at = now(), retire_reason = 'test' WHERE idx = $1",
                ps_idx,
                owner_idx,
            )
        samples.append((bs_idx, ps_idx, ss_idx))
        return ps_idx

    yield {"pool": postgres_pool, "pool_idx": pool_idx, "add_sample": add_sample}

    for _bs, _ps, ss_idx in samples:
        await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    for _bs, ps_idx, _ss in samples:
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
    for bs_idx, _ps, _ss in samples:
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", owner_idx)


async def test_empty_pool_returns_no_rows(pool_ctx):
    rows = await fetch_sequenced_pool_sample_qc_reports(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert rows == []


async def test_returns_processed_and_unprocessed_ordered(pool_ctx):
    """Both a sample with reports and one without come back (the latter with NULL
    blobs), ordered by prep_sample_idx."""
    ps_a = await pool_ctx["add_sample"](with_reports=True)
    ps_b = await pool_ctx["add_sample"](with_reports=False)
    rows = await fetch_sequenced_pool_sample_qc_reports(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == sorted([ps_a, ps_b])
    by_ps = {r["prep_sample_idx"]: r for r in rows}
    assert by_ps[ps_a]["raw_qc_report"] is not None
    assert by_ps[ps_b]["raw_qc_report"] is None
    assert by_ps[ps_b]["filtered_qc_report"] is None


async def test_retired_sample_excluded(pool_ctx):
    """A retired prep_sample is omitted entirely — matching the read-metric
    rollup's retired exclusion so sample_count and this list agree."""
    ps_live = await pool_ctx["add_sample"](with_reports=True)
    await pool_ctx["add_sample"](with_reports=True, retired=True)
    rows = await fetch_sequenced_pool_sample_qc_reports(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == [ps_live]
