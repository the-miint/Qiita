"""Submit-time resource-planning helpers for native jobs' optional `plan()`.

A SIBLING of `jobs/` (not inside it): every non-dunder file under `jobs/` must
be a valid native job (`Inputs` + `execute`), so shared `plan()` helpers live
out here — same rationale as `read_count.py` (see `jobs/__init__.py`'s scan
docstring).

These run in the ORCHESTRATOR process at submit time, never on a compute node.
They must stay cheap: a Parquet footer / metadata read, not a data scan.
`plan()` is advisory — the control plane clamps a hint to at most the YAML
baseline (down-sizing only) and escalation is the backstop for an
under-estimate, so a wrong coefficient costs at most a retry, never
correctness.
"""

from __future__ import annotations

import math
from datetime import timedelta
from pathlib import Path

import duckdb
from qiita_common.parquet import validate_parquet_path


def count_read_pairs(reads_parquet: Path) -> int:
    """Row count of a reads Parquet (one row == one single-end read or one
    R1/R2 pair), read from the Parquet FOOTER — metadata only, no data scan.
    A `count(*)` over `read_parquet` is answered from row-group statistics, so
    this stays cheap enough to run inline at submit time.

    A plain `read_parquet` count needs no miint extension, so a bare
    connection suffices. The path is inlined via `validate_parquet_path`
    (fail-fast on quote/backslash/control chars), matching the sibling jobs'
    `read_parquet`/COPY literal convention — a filesystem path is the only
    surface here, and a rejected path degrades to the baseline like any other
    `plan()` failure."""
    path_sql = validate_parquet_path(reads_parquet)
    with duckdb.connect() as conn:
        row = conn.execute(f"SELECT count(*) FROM read_parquet('{path_sql}')").fetchone()
    return int(row[0])


def linear_walltime(
    read_pairs: int, *, base_seconds: int, seconds_per_million_pairs: float
) -> timedelta:
    """A `base + linear-in-cardinality` WALLTIME estimate:
    `base_seconds + ceil(read_pairs / 1e6 * seconds_per_million_pairs)`.

    Walltime — not memory — is the axis to size from read count for a STREAMING
    job (per-row transform + a spill-to-disk sort): peak RAM is bounded by the
    operator working set and DuckDB's `memory_limit` (roughly flat in row
    count), while runtime scales ~linearly with rows at a ~constant throughput.
    `base_seconds` covers fixed per-job overhead (process + DuckDB init, the
    read/scan/write that isn't row-count-dominated).

    The coefficients are per-job (a job passes its own) and are conservative
    INITIAL estimates to refine against production telemetry — NOT a guarantee.
    The control plane only ever LOWERS a step to this value (never above its
    YAML baseline), and TIMEOUT escalation raises walltime again on an
    under-estimate, so a too-low coefficient costs at most a retry, never
    correctness."""
    total = base_seconds + math.ceil(read_pairs / 1_000_000 * seconds_per_million_pairs)
    return timedelta(seconds=total)
