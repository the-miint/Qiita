"""Unit tests for the submit-time plan() helpers in `job_resource_plan`.

`count_read_pairs` (a Parquet footer count) and `linear_walltime` (a
base + linear-in-cardinality walltime estimate) are pure, cheap helpers
native jobs' `plan()` compose — no miint, no infrastructure.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb

from qiita_compute_orchestrator.job_resource_plan import count_read_pairs, linear_walltime


def _reads(path: Path, n: int) -> Path:
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT i AS sequence_idx FROM range({n}) t(i)) TO '{path}' (FORMAT PARQUET)"
        )
    return path


def test_count_read_pairs_counts_rows(tmp_path):
    assert count_read_pairs(_reads(tmp_path / "r.parquet", 12345)) == 12345


def test_count_read_pairs_empty(tmp_path):
    assert count_read_pairs(_reads(tmp_path / "r.parquet", 0)) == 0


def test_linear_walltime_base_plus_linear():
    # 2M pairs at 30 s/M = 60 s over a 300 s base.
    assert linear_walltime(2_000_000, base_seconds=300, seconds_per_million_pairs=30) == timedelta(
        seconds=360
    )


def test_linear_walltime_rounds_up_fractional_millions():
    # 1 read: ceil(1/1e6 * 30) = 1 s over the base (never rounds a nonzero
    # contribution down to 0).
    assert linear_walltime(1, base_seconds=300, seconds_per_million_pairs=30) == timedelta(
        seconds=301
    )


def test_linear_walltime_zero_reads_is_base_only():
    assert linear_walltime(0, base_seconds=300, seconds_per_million_pairs=30) == timedelta(
        seconds=300
    )
