"""Pure-unit tests for the in-job Golay (12,11,8) decode-cloud generator.

The demux no longer reads a vendored `(raw, corrected, errors)` Parquet — it
generates the extended-binary-Golay [24,12] decode cloud from a baked systematic
generator (`golay_demux._golay_cloud_rows`). These tests pin the code's
correctness invariants WITHOUT the 36 MB vendored table (min distance 8 → k≤3
neighbours are uniquely correctable; the counts are exactly combinatorial), and
— when the vendored table happens to be present locally — assert the generated
cloud reproduces it byte-for-byte for errors≤3. No infrastructure; runs in the
pure-unit tier.
"""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from qiita_compute_orchestrator.jobs.golay_demux import (
    _bits_to_dna,
    _correctable_radius,
    _golay_cloud_rows,
    _golay_codeword,
)

# The vendored table, if a dev checkout has it — used only by the optional
# exact-match test, skipped in CI where it is absent.
_VENDORED_GOLAY = Path(__file__).resolve().parents[3] / "ref" / "golay_corrected_ordered.parquet"


def _hamming24(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def test_4096_distinct_codewords_min_distance_8():
    """The code is a linear [24,12] code (4096 distinct codewords) with minimum
    distance 8 — the property that makes k≤3 corrections unambiguous."""
    codewords = [_golay_codeword(m) for m in range(4096)]
    assert len(set(codewords)) == 4096
    # For a linear code the minimum distance is the minimum nonzero codeword
    # weight (distance to the all-zero codeword, which is message 0 -> all 'C').
    assert _golay_codeword(0) == 0
    min_weight = min(bin(cw).count("1") for cw in codewords if cw != 0)
    assert min_weight == 8


def test_codeword_zero_is_all_c():
    """The 2-bit map is C=00, so the zero codeword renders as the all-C barcode."""
    assert _bits_to_dna(_golay_codeword(0)) == "C" * 12


def test_radius_1_cloud_shape():
    """Default threshold 1.5 → radius 1: 4096 codewords + 4096·24 single-bit
    neighbours = 102,400 rows, and every raw maps to exactly one codeword (no
    collisions — proof the neighbours stay inside disjoint Hamming balls)."""
    rows = _golay_cloud_rows(1)
    assert len(rows) == 4096 + 4096 * 24 == 102400
    exact = [r for r in rows if r[2] == 0]
    assert len(exact) == 4096
    assert all(raw == corrected for raw, corrected, _ in exact)
    # raw is unique across the whole cloud → the decode is a function.
    assert len({raw for raw, _, _ in rows}) == len(rows)


@pytest.mark.parametrize("k", [0, 1, 2, 3])
def test_neighbour_counts_are_combinatorial(k):
    """errors=k rows number exactly 4096·C(24,k) — the disjoint-Hamming-ball
    count, confirming no two codewords share a k-neighbour for k≤3."""
    rows = [r for r in _golay_cloud_rows(k) if r[2] == k]
    assert len(rows) == 4096 * math.comb(24, k)


def test_correctable_radius():
    """`errors < threshold` over integer error counts, capped at 3."""
    assert _correctable_radius(1.5) == 1
    assert _correctable_radius(2.0) == 1  # errors < 2 -> {0, 1}
    assert _correctable_radius(4.0) == 3  # capped (errors>=4 are ambiguous)
    assert _correctable_radius(0.5) == 0


@pytest.mark.skipif(not _VENDORED_GOLAY.exists(), reason="vendored golay table not present")
def test_matches_vendored_table_for_correctable_errors():
    """When the vendored duckdb-miint table is available, the generated cloud
    reproduces it EXACTLY for errors≤3 (the correctable range the demux uses)."""
    import duckdb  # noqa: PLC0415

    gen = {(raw, corrected, errors) for raw, corrected, errors in _golay_cloud_rows(3)}
    with duckdb.connect(":memory:") as conn:
        vendored = set(
            conn.execute(
                "SELECT raw, corrected, errors FROM read_parquet(?) WHERE errors <= 3",
                [str(_VENDORED_GOLAY)],
            ).fetchall()
        )
    assert gen == vendored
