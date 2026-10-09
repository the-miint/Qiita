"""DB tests for fetch_sequenced_pool_read_metrics — the compute-on-read pool
rollup.

The repo function SUMs the per-stage read counts over a pool's non-retired
sequenced_samples and reports the sample total / with-metrics count. Each test
seeds one principal + one run + one pool and attaches samples with controllable
metrics (and optional retirement) via the shared `pool_ctx` fixture's
`add_sample`. The run-level test seeds several pools, so it builds its own.
"""

import secrets

import pytest

from qiita_control_plane.repositories.sequencing_run import (
    fetch_sequenced_pool_read_metrics,
    fetch_sequencing_run_read_metrics,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)

pytestmark = pytest.mark.db


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


async def test_flagged_sample_excluded_from_sums_and_counts(pool_ctx):
    """An ENA-flagged sequenced_sample contributes to neither the sums nor
    either count, the same as a retired one."""
    await pool_ctx["add_sample"](raw=1000, biological=900, quality_filtered=850)
    await pool_ctx["add_sample"](
        raw=5000, biological=4000, quality_filtered=3000, ena_status="suppressed"
    )
    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])
    assert row["raw_read_count_r1r2"] == 1000  # flagged 5000 excluded
    assert row["sample_count"] == 1
    assert row["samples_with_metrics"] == 1


async def test_all_retired_pool_still_returns_a_zeroed_row(pool_ctx):
    """A pool whose EVERY sample is retired still returns a row of zeros — it does
    not read as a missing pool.

    This is the one case that distinguishes the per-aggregate
    `FILTER (WHERE ps.retired IS NOT TRUE)` from the same predicate hoisted into a
    plain `WHERE`. With `WHERE`, every joined row is eliminated, the `GROUP BY sp.idx`
    group disappears, `fetchrow` returns None, and the route reports the pool as
    missing (404) rather than empty. With `FILTER`, the pool row survives and the
    aggregates zero out. A pool that exists but has been fully retired is the former,
    not the latter — so the FILTER form is load-bearing, not stylistic.

    (For a pool with NO samples the two forms agree: `ps.retired` is NULL there and
    `NULL IS NOT TRUE` is true, so the LEFT-JOIN phantom row survives a WHERE too.
    That case is `test_empty_pool_is_null_sums_zero_counts`.)"""
    await pool_ctx["add_sample"](raw=1000, biological=900, quality_filtered=850, retired=True)
    await pool_ctx["add_sample"](raw=2000, biological=1800, quality_filtered=1700, retired=True)

    row = await fetch_sequenced_pool_read_metrics(pool_ctx["pool"], pool_ctx["pool_idx"])

    assert row is not None, "an all-retired pool must not read as a missing pool"
    assert row["idx"] == pool_ctx["pool_idx"]
    assert row["raw_read_count_r1r2"] is None  # SUM over zero rows
    assert row["sample_count"] == 0
    assert row["samples_with_metrics"] == 0


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
        await teardown_entity_graph(
            postgres_pool,
            study_idxs=[],
            biosample_idxs=[bs for bs, _ps, _ss in made],
            prep_sample_idxs=[ps for _bs, ps, _ss in made],
        )
        await delete_idxs(postgres_pool, "sequenced_pool", pool_idxs)
        await delete_idxs(postgres_pool, "sequencing_run", run_idx)
        await delete_principal(postgres_pool, [owner_idx])
