"""Shared read-storage helpers for native jobs that store a prep_sample's
reads into the DuckLake `read` table, once.

Lives at the sibling level (NOT inside `jobs/`) for the same reason
`miint.py` does — every non-dunder file under `jobs/` must export an
`Inputs` model and an `execute` coroutine (enforced by `scan_native_jobs`),
so shared helpers go alongside, not inside (see `docs/writing-a-job.md`).

`ingest_reads` (the bcl-convert workflow's read-storage step) and
`ingest_ena_reads` (the download-ena-study workflow's read-storage step)
both write the durable `read.parquet` via the same
mint-then-sort-and-assign-sequence_idx pipeline and the same
hardlink-into-the-register-dir tail; this module is the one place that
logic lives so the two jobs cannot silently diverge.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from qiita_common.parquet import validate_parquet_path

from .miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_headroom_gb,
    open_conn,
    slurm_alloc_gb,
)


def per_slot_caps(concurrency: int, *, threads: int, fallback_memory_gb: int) -> tuple[int, int]:
    """Per-slot DuckDB ``(memory_gb, threads)`` for `concurrency` samples/runs in
    flight at once. `threads` is the fixed per-slot DuckDB thread count the
    caller wants (kept the sort's parallelism reasonable without starving other
    slots). Under SLURM the memory is the cgroup allocation minus headroom for
    all ``concurrency * threads`` threads, split evenly across slots — so a
    per-run ``--mem-gb`` override reaches each slot. Off SLURM
    (`slurm_alloc_gb()` is None — tests / local backend) it falls back to
    `fallback_memory_gb`, the caller's YAML-baseline-derived literal."""
    alloc = slurm_alloc_gb()
    if alloc is None:
        return fallback_memory_gb, threads
    usable = alloc - duckdb_headroom_gb(concurrency * threads)
    return max(1, usable // concurrency), threads


def write_sorted_reads(
    intermediate_path: Path,
    prep_sample_idx: int,
    sequence_idx_start: int,
    out_path: Path,
    duckdb_tmp: Path,
    memory_gb: int,
    threads: int,
) -> None:
    """Second pass: read the staged intermediate, assign the minted
    `sequence_idx`, and write the durable `read.parquet` at `out_path` sorted by
    `sequence_idx`. No FASTQ/ENA re-fetch — the heavy fetch already happened
    when the caller staged `intermediate_path`. `sequence_idx_start` is the
    inclusive mint start; `sequence_index` is the staged intermediate's
    1-based per-run/per-file row index.

    Sorts by `sequence_idx` alone: `prep_sample_idx` is a constant literal for
    the whole sample (cardinality 1), so adding it to the sort key orders
    nothing — the output is identical to sorting by `(prep_sample_idx,
    sequence_idx)`. The explicit ORDER BY is load-bearing: the read happens
    with `preserve_insertion_order=false`, which lets DuckDB write rows out of
    order, so only the sort guarantees `sequence_idx` is ordered at rest (for
    DuckLake pruning / row-group pushdown).

    **Atomic publish.** The sorted COPY lands in a `.partial` sibling, then
    `os.replace`s into `out_path` (atomic on the same filesystem). This is
    load-bearing for idempotency: `out_path` is ALSO the retry sentinel (the
    caller skips a sample/run whose durable copy already exists), so it must
    only ever appear complete — DuckDB `COPY ... TO` is not atomic, and an
    OOM-kill / walltime cut mid-COPY would otherwise leave a truncated
    `read.parquet` that the next attempt skips and registers as the full read
    set."""
    partial_path = out_path.parent / f"{out_path.name}.partial"
    partial = validate_parquet_path(partial_path)
    try:
        with open_conn() as conn:
            apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)
            conn.execute(
                "COPY ( SELECT "
                "  ?::BIGINT AS prep_sample_idx,"
                "  sequence_index + ? - 1 AS sequence_idx,"
                "  read_id, sequence1, qual1, sequence2, qual2 "
                "FROM read_parquet(?) "
                "ORDER BY sequence_idx ) "
                f"TO '{partial}' ({PARQUET_OPTS})",
                [prep_sample_idx, sequence_idx_start, str(intermediate_path)],
            )
        # Publish atomically: the durable path only ever appears complete.
        os.replace(partial_path, out_path)
    finally:
        # If the COPY died before the replace, drop the half-written partial so
        # a retry re-derives instead of finding stale bytes.
        partial_path.unlink(missing_ok=True)


def hardlink(src: Path, dst: Path) -> None:
    """Hardlink `src` -> `dst` (same scratch filesystem), replacing an
    existing dst. Falls back to a copy across filesystems (defensive — the
    durable copy and the workspace are both under PATH_SCRATCH)."""
    dst.unlink(missing_ok=True)
    try:
        os.link(src, dst)
    except OSError:
        shutil.copyfile(src, dst)
