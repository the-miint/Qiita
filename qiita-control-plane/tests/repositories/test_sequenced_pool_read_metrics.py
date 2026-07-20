"""DB tests for fetch_sequenced_pool_read_metrics — the compute-on-read pool
rollup.

The repo function SUMs the per-stage read counts over a pool's non-retired
sequenced_samples and reports the sample total / with-metrics count. Each test
seeds one principal + one run + one pool and attaches samples with controllable
metrics (and optional retirement) via `pool_ctx.add_sample`; cleanup is
FK-reverse on the shared postgres_pool fixture.
"""

import secrets

import pytest
import pytest_asyncio

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_read_metrics,
    fetch_sequencing_run_read_metrics,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def pool_ctx(postgres_pool):
    """Seed a principal + one sequencing_run + one sequenced_pool; yield a
    context whose `add_sample(...)` attaches a sequenced_sample (with optional
    read metrics / retirement) to the pool. FK-reverse cleanup."""
    owner_idx = await seed_user_principal(postgres_pool, prefix="poolmetrics", suffix="owner")
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"pm-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    pool_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx",
        run_idx,
        owner_idx,
    )
    samples: list[tuple[int, int, int]] = []  # (biosample, prep_sample, sequenced_sample)

    async def add_sample(
        *,
        raw=None,
        biological=None,
        quality_filtered=None,
        spikein=None,
        retired=False,
        biosample_accession=None,
        ena_sample_accession=None,
        ena_experiment_accession=None,
        ena_run_accession=None,
    ):
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
        if biosample_accession is not None or ena_sample_accession is not None:
            await postgres_pool.execute(
                "UPDATE qiita.biosample SET biosample_accession = $2,"
                " ena_sample_accession = $3 WHERE idx = $1",
                bs_idx,
                biosample_accession,
                ena_sample_accession,
            )
        if ena_experiment_accession is not None or ena_run_accession is not None:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET ena_experiment_accession = $2,"
                " ena_run_accession = $3 WHERE idx = $1",
                ss_idx,
                ena_experiment_accession,
                ena_run_accession,
            )
        if raw is not None:
            await postgres_pool.execute(
                "UPDATE qiita.sequenced_sample SET raw_read_count_r1r2 = $2,"
                " biological_read_count_r1r2 = $3, quality_filtered_read_count_r1r2 = $4,"
                " spikein_read_count_r1r2 = $5"
                " WHERE idx = $1",
                ss_idx,
                raw,
                biological,
                quality_filtered,
                spikein,
            )
        if retired:
            await postgres_pool.execute(
                "UPDATE qiita.prep_sample SET retired = true, retired_by_idx = $2,"
                " retired_at = now(), retire_reason = 'test' WHERE idx = $1",
                ps_idx,
                owner_idx,
            )
        samples.append((bs_idx, ps_idx, ss_idx))
        return ss_idx

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


async def test_empty_pool_is_null_sums_zero_counts(pool_ctx):
    """A pool with no samples: sums NULL, all counts 0 (LEFT JOINs keep the pool
    row, and count(ss.idx) ignores the all-NULL phantom row for every bucket)."""
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["raw_read_count_r1r2"] is None
    assert row["biological_read_count_r1r2"] is None
    assert row["quality_filtered_read_count_r1r2"] is None
    assert row["sample_count"] == 0
    assert row["samples_with_metrics"] == 0
    # None of the new breakdown / coverage buckets are inflated by the phantom row.
    for col in (
        "samples_unprocessed",
        "samples_zero_reads",
        "samples_with_reads",
        "samples_with_biosample_accession",
        "samples_with_ena_sample_accession",
        "samples_with_ena_experiment_accession",
        "samples_with_ena_run_accession",
        "samples_fully_submitted_to_ena",
    ):
        assert row[col] == 0, col


async def test_read_outcome_breakdown_partitions_sample_count(pool_ctx):
    """The three read-outcome buckets partition sample_count: an unprocessed
    sample (no raw), a processed-but-zero-survived sample (raw>0, qf=0), and a
    processed-with-reads sample (raw>0, qf>0). unprocessed == sample_count -
    samples_with_metrics, and the three sum to sample_count."""
    await pool_ctx["add_sample"]()  # unprocessed
    await pool_ctx["add_sample"](raw=1000, biological=0, quality_filtered=0)  # zero survived
    await pool_ctx["add_sample"](raw=2000, biological=1800, quality_filtered=1700)  # with reads
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 3
    assert row["samples_with_metrics"] == 2
    assert row["samples_unprocessed"] == 1
    assert row["samples_zero_reads"] == 1
    assert row["samples_with_reads"] == 1
    assert (
        row["samples_unprocessed"] + row["samples_zero_reads"] + row["samples_with_reads"]
        == row["sample_count"]
    )
    assert row["samples_unprocessed"] == row["sample_count"] - row["samples_with_metrics"]


async def test_read_outcome_processed_with_null_qf_counts_as_zero_reads(pool_ctx):
    """A sample processed (raw set) but whose quality_filtered count is NULL lands
    in zero_reads (COALESCE NULL->0), so the three buckets still sum to
    sample_count rather than dropping it."""
    await pool_ctx["add_sample"](raw=1000, biological=None, quality_filtered=None)
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 1
    assert row["samples_with_metrics"] == 1
    assert row["samples_unprocessed"] == 0
    assert row["samples_zero_reads"] == 1
    assert row["samples_with_reads"] == 0


async def test_accession_coverage_counts(pool_ctx):
    """Coverage counts per accession, and samples_fully_submitted_to_ena requires
    all four (biosample + ENA-sample on the biosample, ENA experiment + run on the
    sequenced_sample). Retired samples are excluded like the read sums."""
    # Fully submitted: all four accessions.
    await pool_ctx["add_sample"](
        biosample_accession="SAMEA00000000",
        ena_sample_accession="ERS0000000",
        ena_experiment_accession="ERX0000000",
        ena_run_accession="ERR0000000",
    )
    # Partial: biosample + ena-sample only (no ENA experiment/run yet).
    await pool_ctx["add_sample"](
        biosample_accession="SAMEA00000001",
        ena_sample_accession="ERS0000001",
    )
    # None: no accessions at all.
    await pool_ctx["add_sample"]()
    # Retired fully-submitted sample must not inflate any coverage count.
    await pool_ctx["add_sample"](
        retired=True,
        biosample_accession="SAMEA00000002",
        ena_sample_accession="ERS0000002",
        ena_experiment_accession="ERX0000002",
        ena_run_accession="ERR0000002",
    )
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["sample_count"] == 3  # retired excluded
    assert row["samples_with_biosample_accession"] == 2
    assert row["samples_with_ena_sample_accession"] == 2
    assert row["samples_with_ena_experiment_accession"] == 1
    assert row["samples_with_ena_run_accession"] == 1
    assert row["samples_fully_submitted_to_ena"] == 1


async def test_sums_across_processed_samples(pool_ctx):
    """Two processed samples: per-stage counts sum; the ::bigint cast yields
    plain ints (not Decimal). The spikein column sums too — a PacBio absquant
    sample carries one, an Illumina sample carries 0, and the rollup adds both."""
    await pool_ctx["add_sample"](raw=1000, biological=900, quality_filtered=850, spikein=40)
    await pool_ctx["add_sample"](raw=2000, biological=1800, quality_filtered=1700, spikein=0)
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["raw_read_count_r1r2"] == 3000
    assert row["biological_read_count_r1r2"] == 2700
    assert row["quality_filtered_read_count_r1r2"] == 2550
    assert row["spikein_read_count_r1r2"] == 40
    assert isinstance(row["raw_read_count_r1r2"], int)
    assert isinstance(row["spikein_read_count_r1r2"], int)
    assert row["sample_count"] == 2
    assert row["samples_with_metrics"] == 2


async def test_spikein_sums_only_over_non_retired_samples(pool_ctx):
    """The spikein SUM carries the same `FILTER (WHERE ps.retired IS NOT TRUE)`
    as its three siblings — a retired sample's spike-ins must not inflate the
    pool's spike-in masking total (NOT the cell-count model's input — that
    is per-insert coverage depth)."""
    await pool_ctx["add_sample"](raw=1000, biological=900, quality_filtered=850, spikein=40)
    await pool_ctx["add_sample"](
        raw=500, biological=400, quality_filtered=300, spikein=99, retired=True
    )
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["spikein_read_count_r1r2"] == 40
    assert row["raw_read_count_r1r2"] == 1000


async def test_partial_pool_counts_only_processed(pool_ctx):
    """One processed + one unprocessed sample: sums reflect only the processed
    one, sample_count counts both, samples_with_metrics counts one."""
    await pool_ctx["add_sample"](raw=1000, biological=900, quality_filtered=850)
    await pool_ctx["add_sample"]()  # unprocessed → NULL counts
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["raw_read_count_r1r2"] == 1000
    assert row["sample_count"] == 2
    assert row["samples_with_metrics"] == 1


async def test_retired_sample_excluded_from_sums_and_counts(pool_ctx):
    """A retired prep_sample contributes to neither the sums nor either count."""
    await pool_ctx["add_sample"](raw=1000, biological=900, quality_filtered=850)
    await pool_ctx["add_sample"](raw=5000, biological=4000, quality_filtered=3000, retired=True)
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["raw_read_count_r1r2"] == 1000  # retired 5000 excluded
    assert row["sample_count"] == 1
    assert row["samples_with_metrics"] == 1


async def test_fraction_recomputes_from_sums_not_mean_of_fractions(pool_ctx):
    """Sample A (100/100 = 1.0) and B (900 raw, 0 qf = 0.0): a mean of per-sample
    fractions would be 0.5, but the pool rollup sums first — 100/1000 = 0.1. We
    assert the SUMS here; the 0.1 fraction is derived in PoolReadMetrics."""
    await pool_ctx["add_sample"](raw=100, biological=100, quality_filtered=100)
    await pool_ctx["add_sample"](raw=900, biological=100, quality_filtered=0)
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["raw_read_count_r1r2"] == 1000
    assert row["quality_filtered_read_count_r1r2"] == 100  # → fraction 0.1, not mean 0.5


async def test_unknown_pool_returns_none(pool_ctx):
    assert await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], 999_999_999) is None


async def test_run_level_rollup_sums_across_pools(postgres_pool):
    """fetch_sequencing_run_read_metrics reports the identical PoolReadMetrics
    shape, summed across every pool in the run — same aggregate expressions as the
    pool rollup (shared _READ_METRIC_AGGREGATE_COLUMNS), just rooted at the run."""
    owner_idx = await seed_user_principal(postgres_pool, prefix="runmetrics", suffix="owner")
    run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"rm-run-{secrets.token_hex(4)}",
        owner_idx,
    )
    made: list[tuple[int, int, int]] = []  # (biosample, prep_sample, sequenced_sample)
    pool_idxs: list[int] = []

    async def add_pool_sample(*, raw, quality_filtered, biosample_accession=None):
        pool_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.sequenced_pool (sequencing_run_idx, created_by_idx)"
            " VALUES ($1, $2) RETURNING idx",
            run_idx,
            owner_idx,
        )
        pool_idxs.append(pool_idx)
        bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
            postgres_pool, owner_idx=owner_idx
        )
        ss_idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.sequenced_sample"
            "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id,"
            "   raw_read_count_r1r2, quality_filtered_read_count_r1r2, created_by_idx)"
            " VALUES ($1, $2, $3, $4, $5, $6) RETURNING idx",
            ps_idx,
            pool_idx,
            f"item-{secrets.token_hex(4)}",
            raw,
            quality_filtered,
            owner_idx,
        )
        if biosample_accession is not None:
            await postgres_pool.execute(
                "UPDATE qiita.biosample SET biosample_accession = $2 WHERE idx = $1",
                bs_idx,
                biosample_accession,
            )
        made.append((bs_idx, ps_idx, ss_idx))

    try:
        # Two pools, one sample each: with-reads (has biosample accession) and
        # zero-reads (no accession).
        await add_pool_sample(raw=1000, quality_filtered=800, biosample_accession="SAMEA0")
        await add_pool_sample(raw=500, quality_filtered=0)
        row = await fetch_sequencing_run_read_metrics(postgres_pool, run_idx)
        assert row["sample_count"] == 2  # summed across both pools
        assert row["raw_read_count_r1r2"] == 1500
        assert row["quality_filtered_read_count_r1r2"] == 800
        assert row["samples_with_reads"] == 1
        assert row["samples_zero_reads"] == 1
        assert row["samples_with_biosample_accession"] == 1
        # A run with no samples still returns a row (0 counts), not None; only an
        # unknown run idx is None.
        assert await fetch_sequencing_run_read_metrics(postgres_pool, 999_999_999) is None
    finally:
        for _bs, _ps, ss_idx in made:
            await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
        for pool_idx in pool_idxs:
            await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
        for _bs, ps_idx, _ss in made:
            await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", ps_idx)
        for bs_idx, _ps, _ss in made:
            await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", bs_idx)
        await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", owner_idx)
        await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", owner_idx)
