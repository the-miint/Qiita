"""Unit tests for the pool-level QC-report merge (pure, no DB).

`merge_qc_reports` folds a pool's per-sample qc_report.json documents into the
run-level MergedQCAggregate: histograms sum per bucket, counts sum, and the
per-sample means pool weighted (by total_bases for content/quality, by reads for
length). Raw and filtered points merge independently. These tests construct
SampleQCReport lists directly so the arithmetic is checked without a workspace.
"""

import pytest

from qiita_common.models import SampleQCReport, merge_qc_reports


def _mate(
    *,
    reads,
    total_bases,
    mean_quality=None,
    gc_content=None,
    n_content=None,
    min_length=None,
    max_length=None,
    mean_length=None,
    quality_histogram=None,
    gc_histogram=None,
    length_histogram=None,
):
    return {
        "reads": reads,
        "total_bases": total_bases,
        "mean_quality": mean_quality,
        "gc_content": gc_content,
        "n_content": n_content,
        "min_length": min_length,
        "max_length": max_length,
        "mean_length": mean_length,
        "quality_histogram": quality_histogram or {},
        "gc_histogram": gc_histogram or {},
        "length_histogram": length_histogram or {},
    }


def _report(point, *, read_pairs, r1, r2=None):
    return {
        "point": point,
        "layout": "paired" if r2 is not None else "single",
        "read_pairs": read_pairs,
        "mates": {"r1": r1, "r2": r2},
    }


def test_merge_two_paired_samples_sums_and_weights():
    """Two paired-end samples: counts sum, histograms sum per bucket, means pool
    weighted (quality/content by total_bases, length by reads), min/max fold."""
    a_r1 = _mate(
        reads=10,
        total_bases=1000,
        mean_quality=30.0,
        gc_content=0.5,
        n_content=0.0,
        min_length=90,
        max_length=110,
        mean_length=100.0,
        quality_histogram={"30": 10},
        gc_histogram={"50": 10},
        length_histogram={"100": 10},
    )
    b_r1 = _mate(
        reads=30,
        total_bases=3000,
        mean_quality=20.0,
        gc_content=0.4,
        n_content=0.0,
        min_length=40,
        max_length=60,
        mean_length=50.0,
        quality_histogram={"20": 30},
        gc_histogram={"40": 30},
        length_histogram={"50": 30},
    )
    samples = [
        SampleQCReport(
            prep_sample_idx=1,
            sequenced_pool_item_id="a",
            raw_qc_report=_report("raw", read_pairs=10, r1=a_r1, r2=a_r1),
            filtered_qc_report=None,
        ),
        SampleQCReport(
            prep_sample_idx=2,
            sequenced_pool_item_id="b",
            raw_qc_report=_report("raw", read_pairs=30, r1=b_r1, r2=b_r1),
            filtered_qc_report=None,
        ),
    ]

    merged = merge_qc_reports(samples)
    assert merged.filtered is None
    raw = merged.raw
    assert raw.samples == 2
    assert raw.read_pairs == 40
    r1 = raw.mates["r1"]
    assert r1.reads == 40
    assert r1.total_bases == 4000
    # base-weighted: (30*1000 + 20*3000) / 4000 = 22.5
    assert r1.mean_quality == pytest.approx(22.5)
    # base-weighted: (0.5*1000 + 0.4*3000) / 4000 = 0.425
    assert r1.gc_content == pytest.approx(0.425)
    assert r1.min_length == 40
    assert r1.max_length == 110
    # read-weighted: (100*10 + 50*30) / 40 = 62.5
    assert r1.mean_length == pytest.approx(62.5)
    # histograms sum per bucket, ordered by numeric bucket key
    assert r1.quality_histogram == {"20": 30, "30": 10}
    assert list(r1.gc_histogram) == ["40", "50"]
    # both samples carried r2 (same block), so the pooled r2 is present too
    assert raw.mates["r2"] is not None
    assert raw.mates["r2"].reads == 40


def test_merge_single_end_pool_has_no_r2():
    """No sample carried an r2 block → the pooled r2 is None (single-end pool)."""
    r1 = _mate(reads=5, total_bases=500, mean_quality=35.0, quality_histogram={"35": 5})
    samples = [
        SampleQCReport(
            prep_sample_idx=1,
            sequenced_pool_item_id="a",
            raw_qc_report=_report("raw", read_pairs=5, r1=r1, r2=None),
            filtered_qc_report=None,
        )
    ]
    merged = merge_qc_reports(samples)
    assert merged.raw.mates["r1"] is not None
    assert merged.raw.mates["r2"] is None


def test_merge_empty_pool_is_all_none():
    """A pool with no processed samples (or all reports None) merges to None at
    both points."""
    samples = [
        SampleQCReport(
            prep_sample_idx=1,
            sequenced_pool_item_id="a",
            raw_qc_report=None,
            filtered_qc_report=None,
        )
    ]
    merged = merge_qc_reports(samples)
    assert merged.raw is None
    assert merged.filtered is None
    assert merge_qc_reports([]).raw is None


def test_merge_points_are_independent():
    """A sample with only a raw report contributes to merged.raw but not
    merged.filtered; a sample with only filtered does the reverse."""
    r1 = _mate(reads=4, total_bases=400, mean_quality=30.0)
    samples = [
        SampleQCReport(
            prep_sample_idx=1,
            sequenced_pool_item_id="a",
            raw_qc_report=_report("raw", read_pairs=4, r1=r1),
            filtered_qc_report=None,
        ),
        SampleQCReport(
            prep_sample_idx=2,
            sequenced_pool_item_id="b",
            raw_qc_report=None,
            filtered_qc_report=_report("filtered", read_pairs=2, r1=r1),
        ),
    ]
    merged = merge_qc_reports(samples)
    assert merged.raw.samples == 1
    assert merged.filtered.samples == 1


def test_merge_skips_missing_means_in_weighting():
    """A sample whose mate carries no mean_quality (e.g. FASTA, None) is excluded
    from the weighted mean rather than poisoning it; counts still sum."""
    with_q = _mate(reads=10, total_bases=1000, mean_quality=40.0)
    without_q = _mate(reads=10, total_bases=1000, mean_quality=None)
    samples = [
        SampleQCReport(
            prep_sample_idx=1,
            sequenced_pool_item_id="a",
            raw_qc_report=_report("raw", read_pairs=10, r1=with_q),
            filtered_qc_report=None,
        ),
        SampleQCReport(
            prep_sample_idx=2,
            sequenced_pool_item_id="b",
            raw_qc_report=_report("raw", read_pairs=10, r1=without_q),
            filtered_qc_report=None,
        ),
    ]
    r1 = merge_qc_reports(samples).raw.mates["r1"]
    assert r1.reads == 20
    # only the sample with a mean contributes → 40.0, not diluted toward 0
    assert r1.mean_quality == pytest.approx(40.0)
