"""Real-miint smoke test for `syndna` (the `align_minimap2` seam NOT stubbed).

Builds a real `.mmi` from two synthetic spike-in inserts and runs the actual
`align_minimap2`. What this pins that the stubbed unit tests cannot:

  - a real `map-hifi` `.mmi` (built exactly as the operator is told to build it:
    `qiita reference load --host --no-rype-index --minimap2-preset map-hifi`)
    identifies spike-in reads;
  - **the identity threshold is load-bearing.** A read that ALIGNS to a spike-in but
    below `MIN_IDENTITY` is NOT a spike-in. This is the whole reason syndna does not
    just reuse host_filter's "any alignment = hit" rule: a spike-in call is a claim
    about a read's ORIGIN, and a false positive silently removes a genuine biological
    read from `biological`. Without this case the threshold could be deleted and every
    test would still pass;
  - **the primary-only predicate is load-bearing.** A read whose only at-or-above-floor
    alignment is SUPPLEMENTARY is NOT a spike-in — the local-alignment false positive a
    chimeric read produces, and what coverm's read counts do too. Delete
    `alignment_is_primary` from the seam and the chimera test fails;
  - **that chimera test cannot pass vacuously.** A separate guard asserts against the
    RAW aligner that the fixture really does elicit a below-floor primary AND an
    at-or-above-floor supplementary, so a minimap2 / miint bump that stops emitting the
    supplementary record fails loudly instead of leaving the predicate untested;
  - `read_id` round-trips BIGINT through `align_minimap2` into the job's accumulator;
  - a biological read matching no spike-in emits no alignment at all and is left alone.

The unit tests own the merge semantics; this owns the miint contract.
"""

from __future__ import annotations

import asyncio
import random
from pathlib import Path

import duckdb
from qiita_common.duckdb_miint import miint_connect_config, miint_install_sql
from qiita_common.models import ReadMaskReason

from qiita_compute_orchestrator.miint import open_miint_conn

_PASS = ReadMaskReason.PASS.value
_SPIKEIN = ReadMaskReason.SPIKEIN_SYNDNA.value

# feature_idx of each spike-in insert (the subject sequences of the .mmi).
_FEATURE_A = 77
_FEATURE_B = 88


def _seq(rng: random.Random, n: int) -> str:
    return "".join(rng.choice("ACGT") for _ in range(n))


def _mutate(rng: random.Random, s: str, n: int) -> str:
    """Substitute `n` bases — so identity over the aligned region is ~1 - n/len."""
    out = list(s)
    for pos in rng.sample(range(len(out)), n):
        out[pos] = rng.choice([c for c in "ACGT" if c != out[pos]])
    return "".join(out)


# Kilobase-scale, as real HiFi is: `map-hifi` is tuned for long reads and will not
# align a short toy sequence. Seeded, so the identities below are deterministic.
_RNG = random.Random(20260713)
_SPIKEIN_A = _seq(_RNG, 2000)
_SPIKEIN_B = _seq(_RNG, 2000)
_BIOLOGICAL = _seq(_RNG, 2000)

_READ_EXACT_A = _SPIKEIN_A  # identity 1.00  -> spike-in
_READ_NEAR_B = _mutate(_RNG, _SPIKEIN_B, 20)  # identity ~0.99 -> spike-in
_READ_FAR_A = _mutate(_RNG, _SPIKEIN_A, 200)  # identity ~0.90 -> ALIGNS, but NOT a spike-in

# A chimera: a long, LOW-identity stretch of spike-in A followed by a short but PERFECT
# stretch of spike-in B. minimap2 chains these into two separate alignments and — the
# long one scoring higher — makes A the PRIMARY (identity ~0.90, below the floor) and B a
# SUPPLEMENTARY (identity 1.00, above it). That is the local-alignment false positive:
# scoring identity per row and DISTINCT-ing to a read would mark this read a spike-in on
# the strength of its supplementary segment alone.
_READ_CHIMERA = _READ_FAR_A + _SPIKEIN_B[:600]


def _build_syndna_index(tmp_path: Path) -> Path:
    """A real minimap2 `.mmi` over the spike-in inserts, built with the same preset
    the job aligns with (`map-hifi`) — mismatching them is a silent accuracy loss."""
    conn = duckdb.connect(":memory:", config=miint_connect_config())
    conn.execute(miint_install_sql())
    conn.execute("LOAD miint;")
    conn.execute(
        "CREATE TABLE subjects AS SELECT * FROM (VALUES "
        "  (CAST(? AS BIGINT), CAST(? AS VARCHAR)), "
        "  (CAST(? AS BIGINT), CAST(? AS VARCHAR))"
        ") AS t(read_id, sequence1)",
        [_FEATURE_A, _SPIKEIN_A, _FEATURE_B, _SPIKEIN_B],
    )
    mmi = tmp_path / "syndna.mmi"
    success, _path, num_subjects = conn.execute(
        "SELECT success, index_path, num_subjects FROM save_minimap2_index(?, ?, preset := ?)",
        ["subjects", str(mmi), "map-hifi"],
    ).fetchone()
    assert success, "minimap2 index build failed"
    assert num_subjects == 2
    conn.close()
    return mmi


def _write_reads(path: Path, rows: list[tuple[int, str]]) -> Path:
    values = ", ".join(
        "(CAST(5 AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(NULL AS UTINYINT[]), CAST(NULL AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, seq in rows:
        params.extend([sidx, f"r{sidx}", seq])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _run(tmp_path: Path, reads: Path, index: Path) -> dict:
    from qiita_compute_orchestrator.jobs import syndna

    return asyncio.run(
        syndna.execute(
            syndna.Inputs(reads=reads, syndna_minimap2_path=index, work_ticket_idx=1),
            tmp_path / "ws",
        )
    )


def _reasons(out: dict) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT sequence_idx, reason FROM read_parquet('{out['partial_mask']}') "
            "ORDER BY sequence_idx"
        ).fetchall()


def test_syndna_smoke_marks_spikeins_from_a_real_minimap2_index(tmp_path):
    """Both spike-in reads flagged on the RAW reads; the biological read is `pass`."""
    reads = _write_reads(
        tmp_path / "reads.parquet",
        [(1, _READ_EXACT_A), (2, _READ_NEAR_B), (3, _BIOLOGICAL)],
    )
    out = _run(tmp_path, reads, _build_syndna_index(tmp_path))
    assert _reasons(out) == [(1, _SPIKEIN), (2, _SPIKEIN), (3, _PASS)]


def test_syndna_smoke_low_identity_alignment_is_not_a_spikein(tmp_path):
    """THE threshold test. `_READ_FAR_A` aligns to spike-in A (so host_filter's
    "any alignment = hit" rule would call it a spike-in) but at ~0.90 identity, below
    `MIN_IDENTITY` — so syndna must leave it `pass`. Delete the identity floor and
    this is the test that fails."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, _READ_EXACT_A), (2, _READ_FAR_A)])
    out = _run(tmp_path, reads, _build_syndna_index(tmp_path))
    assert _reasons(out) == [(1, _SPIKEIN), (2, _PASS)]


def test_syndna_smoke_no_spikeins_leaves_the_mask_untouched(tmp_path):
    """A sample with no spike-ins: the mask passes through unchanged."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, _BIOLOGICAL)])
    out = _run(tmp_path, reads, _build_syndna_index(tmp_path))
    assert _reasons(out) == [(1, _PASS)]


def test_syndna_smoke_the_chimera_really_does_produce_a_high_identity_supplementary(tmp_path):
    """Guards the test below from passing vacuously.

    Asserts against the RAW aligner (no job, no predicate) that `_READ_CHIMERA` actually
    elicits what it is supposed to: a PRIMARY alignment BELOW `MIN_IDENTITY` and a
    SUPPLEMENTARY one AT OR ABOVE it. If a minimap2 or miint bump ever stops emitting
    that supplementary record, this fails loudly — rather than leaving the false-positive
    test green for the wrong reason.
    """
    from qiita_compute_orchestrator.jobs._coverage import MIN_IDENTITY
    from qiita_compute_orchestrator.jobs.syndna import (
        _IDENTITY_METHOD,
        _MM2_PRESET,
    )

    index = _build_syndna_index(tmp_path)
    # The job's own connection helper, deliberately: this test's whole value is that it
    # probes the SAME miint the job loads. A hand-rolled connect() could drift from
    # `open_miint_conn` (settings, extension resolution) and quietly probe a different one.
    with open_miint_conn() as conn:
        conn.execute(
            "CREATE TABLE q AS SELECT * FROM (VALUES (CAST(1 AS BIGINT), CAST(? AS VARCHAR)))"
            " AS t(read_id, sequence1)",
            [_READ_CHIMERA],
        )
        rows = conn.execute(
            "SELECT alignment_is_primary(flags), alignment_is_supplementary(flags), "
            "       alignment_seq_identity(cigar, tag_nm, tag_md, ?) "
            "FROM align_minimap2('q', index_path := ?, preset := ?, max_secondary := 0) "
            "WHERE NOT alignment_is_unmapped(flags)",
            [_IDENTITY_METHOD, str(index), _MM2_PRESET],
        ).fetchall()

    primary = [ident for is_primary, _, ident in rows if is_primary]
    supplementary = [ident for _, is_supp, ident in rows if is_supp]
    assert primary, f"expected a primary alignment, got {rows}"
    assert supplementary, f"expected a supplementary alignment, got {rows}"
    assert all(i < MIN_IDENTITY for i in primary), f"primary should be below the floor: {rows}"
    assert any(i >= MIN_IDENTITY for i in supplementary), (
        f"supplementary should be at/above the floor: {rows}"
    )


def test_syndna_smoke_high_identity_supplementary_alignment_is_not_a_spikein(tmp_path):
    """THE supplementary test, and a behaviour change: `_READ_CHIMERA`'s only
    at-or-above-threshold alignment is SUPPLEMENTARY, so the read is NOT a spike-in.

    Scoring identity per row and DISTINCT-ing to a read would mark it — a short local
    alignment is enough to claim a whole read came from a spike-in. coverm does not do
    that (measured against coverm 0.8.0: a read whose only alignment to a contig is
    supplementary contributes 0 to that contig's read count), and neither do we.
    Delete `alignment_is_primary` from the seam and this is the test that fails.
    """
    reads = _write_reads(tmp_path / "reads.parquet", [(1, _READ_EXACT_A), (2, _READ_CHIMERA)])
    out = _run(tmp_path, reads, _build_syndna_index(tmp_path))
    assert _reasons(out) == [(1, _SPIKEIN), (2, _PASS)]


def test_mapped_primary_predicate():
    """`_coverage.MAPPED_PRIMARY_EXPR` (the shared gate's mapped-primary filter) uses
    miint's `alignment_is_mapped_primary`. Pin it equivalent to the explicit
    `alignment_is_primary AND NOT alignment_is_unmapped` form across the SAM flag space,
    so a mirror build that changes the function's semantics is caught here — not silently
    in which reads get masked as spike-in / counted toward depth.

    `alignment_is_primary` alone is TRUE for an unmapped read (SAM makes unmapped
    implicitly primary), so the single-call form must still exclude the unmapped bit.
    """
    from qiita_compute_orchestrator.jobs._coverage import MAPPED_PRIMARY_EXPR

    # 0 mapped-primary, 4 unmapped, 16 mapped-primary(reverse), 20 unmapped(reverse),
    # 256 secondary, 260 unmapped+secondary, 2048 supplementary, 2052 unmapped+supp,
    # 2304 secondary+supplementary.
    flags = [0, 4, 16, 20, 256, 260, 2048, 2052, 2304]
    reference = "alignment_is_primary(flags) AND NOT alignment_is_unmapped(flags)"
    with open_miint_conn() as conn:
        rows = conn.execute(
            f"SELECT ({MAPPED_PRIMARY_EXPR}) AS got, ({reference}) AS want "
            "FROM (SELECT UNNEST(?::USMALLINT[]) AS flags)",
            [flags],
        ).fetchall()
    assert rows, "probe produced no rows"
    assert all(got == want for got, want in rows), (
        f"alignment_is_mapped_primary diverged from primary-AND-mapped: {rows}"
    )
    # And it must actually discriminate — only the two mapped-primary flags are TRUE.
    assert [got for got, _ in rows] == [True, False, True, False, False, False, False, False, False]
