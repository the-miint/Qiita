"""Shared fixtures for native-job tests that read/write the reads.parquet shape.

`write_reads` and `read_survivors` are used by both the stubbed
`test_host_filter.py` and the real-miint `test_host_filter_smoke.py`, so the
fastq_to_parquet 7-column schema (prep_sample_idx + the 6 read columns, the
export-side shape) lives in exactly one place (a change to that schema updates
one writer, not two that silently drift).

`write_reads_q` is the QC variant: same 7-column schema but carrying REAL
`UTINYINT[]` quals (host filtering is sequence-only and so writes NULL quals,
but QC's `filter_read` / `trim_polyg` need decoded phred), used by
`test_qc.py` / `test_qc_smoke.py`.

Both take a `prep_sample_idx` keyword: an int broadcast to every row (per-sample,
default 5) or a list (one per row) for a multi-sample BLOCK reads parquet."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def write_reads():
    """Factory writing a reads.parquet with the fastq_to_parquet 7-col schema
    (prep_sample_idx first, then the read columns — the export-side shape).
    `rows` are (sequence_idx, read_id, sequence1, sequence2|None); quals are NULL
    (FASTA shape — irrelevant to the sequence-only host filter). `prep_sample_idx`
    is an int broadcast to every row (default 5) or a list (one per row) for a
    multi-sample block."""

    def _write(
        path: Path,
        rows: list[tuple[int, str, str, str | None]],
        *,
        prep_sample_idx: int | list[int] = 5,
    ) -> Path:
        ps = prep_sample_idx if isinstance(prep_sample_idx, list) else [prep_sample_idx] * len(rows)
        with duckdb.connect(":memory:") as conn:
            values = ", ".join(
                "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
                "CAST(NULL AS UTINYINT[]), CAST(? AS VARCHAR), CAST(NULL AS UTINYINT[]))"
                for _ in rows
            )
            params: list = []
            for (sidx, rid, s1, s2), p in zip(rows, ps, strict=True):
                params.extend([p, sidx, rid, s1, s2])
            conn.execute(
                f"COPY (SELECT * FROM (VALUES {values}) "
                "AS t(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
                f"TO '{path}' (FORMAT PARQUET)",
                params,
            )
        return path

    return _write


@pytest.fixture
def write_reads_q():
    """Factory writing a reads.parquet with the fastq_to_parquet 7-col schema
    (prep_sample_idx first) and REAL `UTINYINT[]` quals. `rows` are
    `(sequence_idx, read_id, sequence1, qual1, sequence2|None, qual2|None)` where
    `qual1`/`qual2` are `list[int] | None` — QC needs decoded phred, unlike the
    sequence-only host filter (see `write_reads`). `prep_sample_idx` is an int
    broadcast to every row (default 5) or a list (one per row); QC keys the mask
    on sequence_idx and does not read the column, but a block's reads carry it."""

    def _write(
        path: Path,
        rows: list[tuple[int, str, str, list[int] | None, str | None, list[int] | None]],
        *,
        prep_sample_idx: int | list[int] = 5,
    ) -> Path:
        ps = prep_sample_idx if isinstance(prep_sample_idx, list) else [prep_sample_idx] * len(rows)
        with duckdb.connect(":memory:") as conn:
            values = ", ".join(
                "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
                "CAST(? AS UTINYINT[]), CAST(? AS VARCHAR), CAST(? AS UTINYINT[]))"
                for _ in rows
            )
            params: list = []
            for (sidx, rid, s1, q1, s2, q2), p in zip(rows, ps, strict=True):
                params.extend([p, sidx, rid, s1, q1, s2, q2])
            conn.execute(
                f"COPY (SELECT * FROM (VALUES {values}) "
                "AS t(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
                f"TO '{path}' (FORMAT PARQUET)",
                params,
            )
        return path

    return _write


@pytest.fixture
def write_partial_mask():
    """Factory writing a 6-column partial mask parquet — the shape `qc` emits and
    the shape `qc` optionally CONSUMES as `adapter_mask`:
    `(sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)`.

    `rows` are `(sequence_idx, reason, left_trim1, right_trim1)`; the mate trims
    are written NULL (single-end — the only layout an incoming mask supports).
    Used by `test_qc.py` / `test_qc_smoke.py`, so the incoming-mask schema lives
    in exactly one place."""

    def _write(path: Path, rows: list[tuple[int, str, int, int]]) -> Path:
        with duckdb.connect(":memory:") as conn:
            values = ", ".join(
                "(CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS UINTEGER), "
                "CAST(? AS UINTEGER), CAST(NULL AS UINTEGER), CAST(NULL AS UINTEGER))"
                for _ in rows
            )
            params: list = []
            for sidx, reason, lt1, rt1 in rows:
                params.extend([sidx, reason, lt1, rt1])
            conn.execute(
                f"COPY (SELECT * FROM (VALUES {values}) AS t("
                "sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)) "
                f"TO '{path}' (FORMAT PARQUET)",
                params,
            )
        return path

    return _write


@pytest.fixture
def read_survivors():
    """Return the sorted `sequence_idx` list remaining in a filtered_reads.parquet."""

    def _read(path: Path) -> list[int]:
        with duckdb.connect(":memory:") as conn:
            return [
                r[0]
                for r in conn.execute(
                    f"SELECT sequence_idx FROM read_parquet('{path}') ORDER BY sequence_idx"
                ).fetchall()
            ]

    return _read
