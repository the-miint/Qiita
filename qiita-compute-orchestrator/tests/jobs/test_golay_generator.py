"""tests for the in-job Golay decode-cloud generator.

pin the correctness invariants without the vendored table (min distance 8, so
k<=3 neighbours are unique and the counts are combinatorial), then assert the
generator reproduces the canonical duckdb-miint table EXACTLY on a committed
golden subset (always runs). when the full 16.7M-row vendored table is present
locally, also assert the exhaustive match for errors<=3.
"""

from __future__ import annotations

import math
from pathlib import Path

import pytest
from qiita_common.illumina import AMPLICON_PLACEHOLDER_INDEX

from qiita_compute_orchestrator.jobs.golay_demux import (
    _BITS_TO_NT,
    _MAX_CORRECTABLE,
    _bits_to_dna,
    _correctable_radius,
    _golay_cloud_rows,
    _golay_codeword,
)

# A committed deterministic subset of the canonical table: all 4096 codewords
# plus strided samples of correctable neighbours (errors 1-3) and
# non-correctable 12-mers (errors >3). Gzipped (it ships in CI) and read via
# duckdb, whose CSV reader handles the compression natively.
_GOLDEN_SUBSET = Path(__file__).resolve().parent / "data" / "golay_golden_subset.csv.gz"

# The full 16.7M-row table (every 12-mer), if a dev checkout has it — used only
# by the optional exhaustive test, skipped in CI where it is absent.
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


def test_the_dummy_sheet_placeholder_index_is_never_decodable():
    """The placeholder's decoded form (its reverse complement) is beyond
    _MAX_CORRECTABLE of every codeword (see AMPLICON_PLACEHOLDER_INDEX for why)."""
    complement = str.maketrans("ACGT", "TGCA")
    decoded_form = AMPLICON_PLACEHOLDER_INDEX[::-1].translate(complement)
    word = 0
    for nt in decoded_form:
        word = (word << 2) | _BITS_TO_NT.index(nt)
    nearest = min(bin(word ^ _golay_codeword(m)).count("1") for m in range(4096))
    assert nearest > _MAX_CORRECTABLE


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


def test_matches_canonical_golden_subset():
    """The generated cloud reproduces the canonical duckdb-miint table EXACTLY on
    the committed golden subset: every correctable (errors≤3) sample maps to the
    same corrected codeword and error count, and every non-correctable (errors>3)
    sample is absent from the cloud (the decoder must not over-correct). Always
    runs — this is the load-bearing "matches prior implementations" assertion."""
    import duckdb  # noqa: PLC0415

    # raw -> (corrected, errors); unique for errors≤3 (min distance 8).
    gen = {raw: (corrected, errors) for raw, corrected, errors in _golay_cloud_rows(3)}

    # duckdb's CSV reader decompresses the .gz natively.
    with duckdb.connect(":memory:") as conn:
        golden = conn.execute(
            "SELECT raw, corrected, errors FROM read_csv(?)", [str(_GOLDEN_SUBSET)]
        ).fetchall()

    correctable = noncorrectable = 0
    for raw, corrected, errors in golden:
        if errors <= 3:
            assert gen.get(raw) == (corrected, errors), raw
            correctable += 1
        else:
            assert raw not in gen, raw
            noncorrectable += 1
    # guard against an empty/mis-generated fixture silently passing.
    assert correctable >= 4096  # at least all codewords
    assert noncorrectable > 0


@pytest.mark.skipif(not _VENDORED_GOLAY.exists(), reason="full vendored golay table not present")
def test_matches_vendored_table_for_correctable_errors():
    """Exhaustive local check: when the full 16.7M-row table is available, the
    generated cloud reproduces it EXACTLY for errors≤3 (the correctable range the
    demux uses). The committed golden subset covers the CI case."""
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
