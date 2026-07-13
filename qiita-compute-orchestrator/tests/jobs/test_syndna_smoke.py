"""Real-miint smoke test for `syndna` (the `align_minimap2` seam NOT stubbed).

Builds a real `.mmi` from two synthetic spike-in inserts and runs the actual
`align_minimap2`. What this pins that the stubbed unit tests cannot:

  - a real `map-hifi` `.mmi` (built exactly as the operator is told to build it:
    `qiita reference load --host --no-rype-index --minimap2-preset map-hifi`)
    identifies spike-in reads;
  - **the identity threshold is load-bearing.** A read that ALIGNS to a spike-in but
    below `_MIN_IDENTITY` is NOT a spike-in. This is the whole reason syndna does not
    just reuse host_filter's "any alignment = hit" rule: a spike-in call is a
    QUANTITATIVE claim, and a false positive both removes a real read from
    `biological` AND inflates the count the cell-count model divides by. Without this
    case the threshold could be deleted and every test would still pass;
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
    `_MIN_IDENTITY` — so syndna must leave it `pass`. Delete the identity floor and
    this is the test that fails."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, _READ_EXACT_A), (2, _READ_FAR_A)])
    out = _run(tmp_path, reads, _build_syndna_index(tmp_path))
    assert _reasons(out) == [(1, _SPIKEIN), (2, _PASS)]


def test_syndna_smoke_no_spikeins_leaves_the_mask_untouched(tmp_path):
    """A sample with no spike-ins: the mask passes through unchanged."""
    reads = _write_reads(tmp_path / "reads.parquet", [(1, _BIOLOGICAL)])
    out = _run(tmp_path, reads, _build_syndna_index(tmp_path))
    assert _reasons(out) == [(1, _PASS)]
