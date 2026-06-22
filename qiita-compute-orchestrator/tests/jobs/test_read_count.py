"""Unit tests for the shared read-count sidecar helper.

`write_read_count` counts reads in a reads.parquet and writes `read_count.json`
recording the `*_r1r2` total (both mates), the pair/row count, and the layout.
The r1r2 formula is `count(*) + count(sequence2)`: every row's R1 plus the
non-null R2s — correct for single-end, paired-end, mixed, and an empty (fully
filtered) file, with no SE/PE branching.

The `write_reads` fixture (tests/jobs/conftest.py) owns the fastq_to_parquet
6-col schema. `read_count.py` is a sibling of `jobs/`, but its test lives here so
it can reuse that shared reads writer.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from qiita_compute_orchestrator.read_count import (
    READ_COUNT_FILENAME,
    ReadCount,
    write_read_count,
)


def _emit(reads: Path, workspace: Path) -> ReadCount:
    """Run the helper on a fresh connection and parse the written sidecar."""
    workspace.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        out = write_read_count(conn, reads, workspace)
    assert out == workspace / READ_COUNT_FILENAME
    return ReadCount.model_validate_json(out.read_text())


def _empty_reads(path: Path) -> Path:
    """A 0-row reads.parquet with the 6-col schema (a fully-filtered sample)."""
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT CAST(NULL AS BIGINT) AS sequence_idx, "
            "CAST(NULL AS VARCHAR) AS read_id, CAST(NULL AS VARCHAR) AS sequence1, "
            "CAST(NULL AS UTINYINT[]) AS qual1, CAST(NULL AS VARCHAR) AS sequence2, "
            "CAST(NULL AS UTINYINT[]) AS qual2 WHERE false) "
            f"TO '{path}' (FORMAT PARQUET)"
        )
    return path


def test_single_end_counts_r1_only(tmp_path, write_reads):
    """SE rows (sequence2 NULL): r1r2 == row count, layout 'single'."""
    reads = write_reads(
        tmp_path / "reads.parquet",
        [(1, "a", "AAA", None), (2, "b", "CCC", None), (3, "c", "GGG", None)],
    )
    rc = _emit(reads, tmp_path / "ws")
    assert rc.read_pairs == 3
    assert rc.read_count_r1r2 == 3
    assert rc.layout == "single"


def test_paired_end_counts_both_mates(tmp_path, write_reads):
    """PE rows (sequence2 present): r1r2 == 2 * pairs, layout 'paired'."""
    reads = write_reads(
        tmp_path / "reads.parquet",
        [(1, "a", "AAA", "TTT"), (2, "b", "CCC", "GGG")],
    )
    rc = _emit(reads, tmp_path / "ws")
    assert rc.read_pairs == 2
    assert rc.read_count_r1r2 == 4
    assert rc.layout == "paired"


def test_mixed_layout_counts_non_null_r2(tmp_path, write_reads):
    """Mixed SE+PE: r1r2 == count(*) + count(sequence2) (3 rows, 2 R2s = 5).
    A within-sample mix shouldn't occur, but the formula stays correct and any
    R2 present marks the layout 'paired'."""
    reads = write_reads(
        tmp_path / "reads.parquet",
        [(1, "a", "AAA", "TTT"), (2, "b", "CCC", None), (3, "c", "GGG", "AAA")],
    )
    rc = _emit(reads, tmp_path / "ws")
    assert rc.read_pairs == 3
    assert rc.read_count_r1r2 == 5
    assert rc.layout == "paired"


def test_empty_reads_is_zero(tmp_path):
    """A fully-filtered sample (0 rows) yields a well-formed 0/0 sidecar."""
    reads = _empty_reads(tmp_path / "reads.parquet")
    rc = _emit(reads, tmp_path / "ws")
    assert rc.read_pairs == 0
    assert rc.read_count_r1r2 == 0
    assert rc.layout == "single"


def test_sidecar_is_valid_json_with_exact_keys(tmp_path, write_reads):
    """The sidecar is plain JSON with exactly the three contract keys (a
    consumer reads it without DuckDB)."""
    reads = write_reads(tmp_path / "reads.parquet", [(1, "a", "AAA", "TTT")])
    workspace = tmp_path / "ws"
    workspace.mkdir()
    with duckdb.connect(":memory:") as conn:
        out = write_read_count(conn, reads, workspace)
    payload = json.loads(out.read_text())
    assert payload == {"read_count_r1r2": 2, "read_pairs": 1, "layout": "paired"}
