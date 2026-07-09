"""Real-miint smoke test for `syndna` (the rype classify seam NOT stubbed).

Builds a real per-feature-bucket `.ryxdi` from two synthetic spike-in sequences and
runs the actual `rype_classify`. What this pins that the stubbed unit tests cannot:

  - `rype_classify` really returns a `bucket_name` and a `score` alongside
    `read_id`, and a `bucket_per_feature` index makes that name the `feature_idx`;
  - `arg_max(bucket_name, score)` is valid over that output, so a read attributes
    to exactly one spike-in;
  - rype's `read_id` output type (build-dependent — a BIGINT input has come back
    VARCHAR) coerces into the job's BIGINT accumulator column.

The unit tests own the merge semantics; this owns the miint contract.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
from qiita_common.duckdb_miint import miint_connect_config, miint_install_sql
from qiita_common.models import ReadMaskReason

# Two unrelated synthetic "spike-in" sequences, long enough for k=64 minimizers.
_SPIKEIN_A = "ACGGTTACGATCGGATCACTGACTGCATTAGCC" * 12
_SPIKEIN_B = "TTGCAAGCTTGGACCATATCGGCAAGTTCAAGG" * 12
# A read drawn from each spike-in, and one sharing no motif with either.
_READ_A = _SPIKEIN_A[100:300]
_READ_B = _SPIKEIN_B[100:300]
_BIOLOGICAL = "GCGCATATCGCGTATAGCGCATAT" * 9

_MASK_IDX = 4242
_PASS = ReadMaskReason.PASS.value
_SPIKEIN = ReadMaskReason.SPIKEIN_SYNDNA.value

# feature_idx of each spike-in; a bucket_per_feature index names its buckets these.
_FEATURE_A = 77
_FEATURE_B = 88


def _build_syndna_index(tmp_path: Path) -> Path:
    """A real `.ryxdi` with ONE BUCKET PER FEATURE, as build_rype_index emits when
    `bucket_per_feature=True` — that mapping is what makes bucket_name meaningful."""
    conn = duckdb.connect(":memory:", config=miint_connect_config())
    conn.execute(miint_install_sql())
    conn.execute("LOAD miint;")
    conn.execute(
        "CREATE TABLE chunks AS SELECT * FROM (VALUES "
        "  (CAST(? AS BIGINT), CAST(0 AS INTEGER), CAST(? AS VARCHAR)), "
        "  (CAST(? AS BIGINT), CAST(0 AS INTEGER), CAST(? AS VARCHAR))"
        ") AS t(feature_idx, chunk_index, chunk_data)",
        [_FEATURE_A, _SPIKEIN_A, _FEATURE_B, _SPIKEIN_B],
    )
    conn.execute(
        "CREATE TABLE bucket_map AS SELECT DISTINCT feature_idx, "
        "CAST(feature_idx AS VARCHAR) AS bucket_name FROM chunks"
    )
    ryxdi = tmp_path / "syndna.ryxdi"
    status = conn.execute(
        "SELECT status FROM rype_index_create(?, ?, mapping_table := 'bucket_map', "
        "k := 64, w := 25, orient := TRUE)",
        ["chunks", str(ryxdi)],
    ).fetchone()[0]
    assert status == "ok", f"rype index build failed: {status!r}"
    conn.close()
    return ryxdi


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


def _write_mask(path: Path, sequence_idxs: list[int]) -> Path:
    values = ", ".join(
        "(CAST(? AS BIGINT), CAST(5 AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), "
        "CAST(0 AS UINTEGER), CAST(0 AS UINTEGER), "
        "CAST(NULL AS UINTEGER), CAST(NULL AS UINTEGER))"
        for _ in sequence_idxs
    )
    params: list = []
    for sidx in sequence_idxs:
        params.extend([_MASK_IDX, sidx, _PASS])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "mask_idx, prep_sample_idx, sequence_idx, reason, "
            "left_trim1, right_trim1, left_trim2, right_trim2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def test_syndna_smoke_marks_spikeins_and_names_them_by_feature(tmp_path):
    from qiita_compute_orchestrator.jobs import syndna

    reads = _write_reads(
        tmp_path / "reads.parquet",
        [(1, _READ_A), (2, _READ_B), (3, _BIOLOGICAL)],
    )
    mask = _write_mask(tmp_path / "mask.parquet", [1, 2, 3])
    index = _build_syndna_index(tmp_path)

    out = asyncio.run(
        syndna.execute(
            syndna.Inputs(reads=reads, read_mask=mask, syndna_rype_path=index, work_ticket_idx=1),
            tmp_path / "ws",
        )
    )

    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT sequence_idx, reason FROM read_parquet('{out['read_mask']}') "
            "ORDER BY sequence_idx"
        ).fetchall()
        counts = conn.execute(
            f"SELECT spikein, read_count FROM read_parquet('{out['spikein_counts']}') "
            "ORDER BY spikein"
        ).fetchall()

    # Both spike-in reads flagged; the biological read untouched.
    assert rows == [(1, _SPIKEIN), (2, _SPIKEIN), (3, _PASS)]
    # ...and each is attributed to the feature it came from — the real bucket_name.
    assert counts == [(str(_FEATURE_A), 1), (str(_FEATURE_B), 1)]


def test_syndna_smoke_no_spikeins_leaves_the_mask_untouched(tmp_path):
    """A sample with no spike-ins: the mask passes through and the counts table is
    empty (not absent — the parquet is always written)."""
    from qiita_compute_orchestrator.jobs import syndna

    reads = _write_reads(tmp_path / "reads.parquet", [(1, _BIOLOGICAL)])
    mask = _write_mask(tmp_path / "mask.parquet", [1])
    index = _build_syndna_index(tmp_path)

    out = asyncio.run(
        syndna.execute(
            syndna.Inputs(reads=reads, read_mask=mask, syndna_rype_path=index, work_ticket_idx=1),
            tmp_path / "ws",
        )
    )
    with duckdb.connect(":memory:") as conn:
        assert conn.execute(
            f"SELECT reason FROM read_parquet('{out['read_mask']}')"
        ).fetchall() == [(_PASS,)]
        assert (
            conn.execute(
                f"SELECT count(*) FROM read_parquet('{out['spikein_counts']}')"
            ).fetchone()[0]
            == 0
        )
