"""DB tests for fetch_sequenced_pool_sample_qc_reports — the per-sample QC-report
rows the merged pool report aggregates.

The repo function returns one row per NON-retired sequenced_sample in the pool,
carrying the two persisted QC-report JSONBs plus prep_sample_idx / item id,
ordered by prep_sample_idx. Its retired exclusion must match
fetch_sequenced_pool_read_metrics' so `sample_count` (the rollup) and the length
of this list agree. Each test attaches samples to the shared `pool_ctx`
fixture's pool via its `add_sample`.
"""

import pytest

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_sample_qc_reports,
)

pytestmark = pytest.mark.db


async def test_empty_pool_returns_no_rows(pool_ctx):
    rows = await fetch_sequenced_pool_sample_qc_reports(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert rows == []


async def test_returns_processed_and_unprocessed_ordered(pool_ctx):
    """Both a sample with reports and one without come back (the latter with NULL
    blobs), ordered by prep_sample_idx."""
    ps_a = (await pool_ctx["add_sample"](with_reports=True)).prep_sample_idx
    ps_b = (await pool_ctx["add_sample"](with_reports=False)).prep_sample_idx
    rows = await fetch_sequenced_pool_sample_qc_reports(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == sorted([ps_a, ps_b])
    by_ps = {r["prep_sample_idx"]: r for r in rows}
    assert by_ps[ps_a]["raw_qc_report"] is not None
    assert by_ps[ps_b]["raw_qc_report"] is None
    assert by_ps[ps_b]["filtered_qc_report"] is None


async def test_retired_sample_excluded(pool_ctx):
    """A retired prep_sample is omitted entirely — matching the read-metric
    rollup's retired exclusion so sample_count and this list agree."""
    ps_live = (await pool_ctx["add_sample"](with_reports=True)).prep_sample_idx
    await pool_ctx["add_sample"](with_reports=True, retired=True)
    rows = await fetch_sequenced_pool_sample_qc_reports(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == [ps_live]


async def test_flagged_sample_excluded(pool_ctx):
    """An ENA-flagged sequenced_sample is omitted entirely, the same as a
    retired one."""
    ps_live = (await pool_ctx["add_sample"](with_reports=True)).prep_sample_idx
    await pool_ctx["add_sample"](with_reports=True, ena_status="suppressed")
    rows = await fetch_sequenced_pool_sample_qc_reports(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert [r["prep_sample_idx"] for r in rows] == [ps_live]
