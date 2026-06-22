"""Shared helper: emit a per-step read-count sidecar.

Each parquet stage of the prep-generation pipeline (`fastq` -> `qc` ->
`host_filter`) emits a small `read_count.json` recording how many reads
survive that stage, so the three SPP boundary counts are captured per
`prep_sample`:

  - `fastq`        -> raw reads (post bcl-convert, pre-filtering)
  - `qc`           -> biological reads (adapter/quality-filtered)
  - `host_filter`  -> quality-filtered reads (host-depleted)

This module is the WRITE side only (emission); persisting the counts onto
`sequenced_sample` and rolling them up to the pool are future work. The
sidecar lives next to the stage's reads Parquet, is declared as a step
output, and the runner forwards it in `bound` for a future consumer.

It is a SIBLING of `jobs/` (not inside it): every non-dunder file under
`jobs/` must be a valid native job (Inputs + execute); shared helpers live
out here (see jobs/__init__.py's scan docstring).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel

# Sidecar filename, written into each stage's own workspace. The binding
# NAME disambiguates the three stages (raw_/biological_/quality_filtered_),
# so the on-disk filename can be the same in every step's output dir.
READ_COUNT_FILENAME = "read_count.json"


class ReadCount(BaseModel):
    """Payload of a stage's `read_count.json`.

    `read_count_r1r2` is total reads counting BOTH mates (R1 + R2) — the
    `*_r1r2` convention SPP's prep file uses. For paired-end that is
    `2 * read_pairs`; for single-end it equals `read_pairs` (R1 only).
    `read_pairs` is the Parquet row count (one row == one pair (PE) or one
    single-end read). `layout` is "paired" when any R2 is present, else
    "single".

    Lives here (orchestrator-side) for the step that only WRITES the sidecar.
    When the CP-side consumer is added, lift this model to qiita-common so
    both services share one contract.
    """

    read_count_r1r2: int
    read_pairs: int
    layout: str


def write_read_count(conn: duckdb.DuckDBPyConnection, reads_parquet: Path, workspace: Path) -> Path:
    """Count the reads in `reads_parquet` and write `read_count.json` into
    `workspace`; return the sidecar path.

    r1r2 = `count(*) + count(sequence2)`: `count(*)` is every row's R1 and
    the non-null `sequence2` count is the R2s, so the sum is total reads
    across both mates — correct for single-end (sequence2 all NULL),
    paired-end, AND a host_filter pass-through, with no SE/PE branching.
    `count(*)` is answered from the Parquet footer; `count(sequence2)` reads
    only that column's row-group null-counts — both cheap, no full data scan.

    The caller passes its already-open DuckDB connection (every stage has
    one); a plain `read_parquet` count needs no miint extension, so any
    connection works. The path is inline single-quote-escaped to match the
    sibling jobs' COPY/read_parquet literals (a filesystem path, no other
    injection surface); this helper joins qc/host_filter on the inline-escape
    side of the validate_parquet_path convergence."""
    path_sql = str(reads_parquet).replace("'", "''")
    read_pairs, r2_reads = conn.execute(
        f"SELECT count(*), count(sequence2) FROM read_parquet('{path_sql}')"
    ).fetchone()
    payload = ReadCount(
        read_count_r1r2=read_pairs + r2_reads,
        read_pairs=read_pairs,
        layout="paired" if r2_reads > 0 else "single",
    )
    out = workspace / READ_COUNT_FILENAME
    out.write_text(payload.model_dump_json())
    return out
