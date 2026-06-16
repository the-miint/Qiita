"""Shared fixtures for native-job tests that read/write the reads.parquet shape.

`write_reads` and `read_survivors` are used by both the stubbed
`test_host_filter.py` and the real-miint `test_host_filter_smoke.py`, so the
fastq_to_parquet 6-column schema lives in exactly one place (a change to that
schema updates one writer, not two that silently drift)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def write_reads():
    """Factory writing a reads.parquet with the fastq_to_parquet 6-col schema.
    `rows` are (sequence_idx, read_id, sequence1, sequence2|None); quals are NULL
    (FASTA shape — irrelevant to the sequence-only host filter)."""

    def _write(path: Path, rows: list[tuple[int, str, str, str | None]]) -> Path:
        with duckdb.connect(":memory:") as conn:
            values = ", ".join(
                "(CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
                "CAST(NULL AS UTINYINT[]), CAST(? AS VARCHAR), CAST(NULL AS UTINYINT[]))"
                for _ in rows
            )
            params: list = []
            for sidx, rid, s1, s2 in rows:
                params.extend([sidx, rid, s1, s2])
            conn.execute(
                f"COPY (SELECT * FROM (VALUES {values}) "
                "AS t(sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
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
