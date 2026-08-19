"""Golay-barcode demultiplex a pool's multiplexed 16S run into the DuckLake `read`
table — the EMP-style analogue of bcl-convert+ingest_reads for runs that arrive as
one multiplexed FASTQ set (R1 forward + R2 + I1 Golay index) rather than a per-sample
BCL demux. Ported from duckdb-miint's demux_qiita.sql.

Two halves in one sequenced_pool-scoped job:
  1. demux: build the prep barcodes' Golay error-correction cloud, pair the I1 index
     stream against R1/R2 by record order (I1 reverse-complemented — Illumina submits
     it RC to the prep), and assign each read to the prep_sample its corrected index
     matches. R2 is carried through (EMP includes it; historically unused downstream).
  2. ingest: per sample, mint a contiguous sequence_idx range (the shared idempotent
     mint_or_reuse_sequence_range) and write the sorted read.parquet — a durable copy
     the read-mask workflow consumes plus a register-files part for the `read` table.

TODO(converge): the per-sample read.parquet write duplicates fastq_to_parquet /
ingest_reads' two-pass shape — promote it to a shared read-ingest core (and consider
packing I1 into a `comment` column) rather than three copies.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from pydantic import BaseModel
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import StepNoData
from qiita_common.parquet import validate_parquet_path

from ..cp_client import make_cp_client
from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_INTERMEDIATE,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_conn,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ..sequence_range_retry import mint_or_reuse_sequence_range

YAML_STEP_NAME = "golay_demux"

# read_fastx over a whole multiplexed lane is the heavy serial part; per-sample writes
# run sequentially (one COPY at a time — no cgroup-memory division needed).
_DUCKDB_THREADS = 4
_DUCKDB_FALLBACK_MEMORY_GB = 8


class Inputs(BaseModel):
    """Typed input contract for golay_demux.

    index_reads_path: multiplexed I1 barcode FASTQ (12-nt Golay codes; longer index
        reads are sliced to the first 12).
    forward_reads_path: multiplexed R1 forward FASTQ (paired to I1 by record order).
    reverse_reads_path: multiplexed R2 FASTQ (optional; carried as sequence2/qual2).
    golay_table_path: the Golay (12,11,8) error-correction Parquet (raw/corrected/errors).
    barcode_map: runner-staged roster Parquet
        (prep_sample_idx BIGINT, barcode VARCHAR, barcodes_are_rc BOOLEAN) — each
        barcode's RC flag is a per-sample sample-sheet fact, not a uniform knob.
    golay_error_threshold: max Golay errors to accept a barcode match (EMP: 1.5).
    reads_staging_root: scratch root the durable per-sample read.parquet copies hang under.
    """

    index_reads_path: Path
    forward_reads_path: Path
    reverse_reads_path: Path | None = None
    golay_table_path: Path
    barcode_map: Path
    golay_error_threshold: float = 1.5
    reads_staging_root: Path
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def _run_demux(inputs: Inputs, demuxed_out: Path, duckdb_tmp: Path, *, memory_gb: int) -> None:
    """Demux the multiplexed FASTQ to an intermediate Parquet keyed by prep_sample_idx.
    Paths are inlined (sanitised) — DuckDB rejects bound params in CREATE VIEW / SET."""
    i1 = validate_parquet_path(inputs.index_reads_path)
    r1 = validate_parquet_path(inputs.forward_reads_path)
    golay = validate_parquet_path(inputs.golay_table_path)
    bc = validate_parquet_path(inputs.barcode_map)
    out = validate_parquet_path(demuxed_out)
    threshold = float(inputs.golay_error_threshold)
    fr_clause = f"read_fastx('{r1}')"
    if inputs.reverse_reads_path is not None:
        r2 = validate_parquet_path(inputs.reverse_reads_path.resolve())
        fr_clause = f"read_fastx('{r1}', sequence2 := '{r2}')"

    with open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        # prep barcodes, each RC'd per its own barcodes_are_rc flag; expand against the
        # Golay cloud (unique index on raw makes the demux join a hash lookup).
        conn.execute(
            "CREATE OR REPLACE VIEW prep_bc AS SELECT prep_sample_idx, "
            "IF(barcodes_are_rc, sequence_dna_reverse_complement(barcode), barcode) AS barcode "
            f"FROM read_parquet('{bc}')"
        )
        conn.execute(
            "CREATE OR REPLACE TABLE golay_codes AS "
            "SELECT p.prep_sample_idx, g.raw FROM prep_bc p "
            f"JOIN read_parquet('{golay}') g ON p.barcode = g.corrected "
            f"WHERE g.errors < {threshold}"
        )
        conn.execute("CREATE UNIQUE INDEX gc_idx ON golay_codes(raw)")
        # I1 length varies; EMP V4 is 12 nt, but some runs ship longer index reads.
        conn.execute(
            "SET VARIABLE demux_i1_12 = "
            f"(SELECT length(sequence1) = 12 FROM read_fastx('{i1}') LIMIT 1)"
        )
        # per-record RC'd index read, keyed by record order.
        conn.execute(
            "CREATE OR REPLACE VIEW idx_reads AS SELECT sequence_index, "
            "sequence_dna_reverse_complement("
            "  IF(getvariable('demux_i1_12'), sequence1, sequence1[:12])) AS index_read "
            f"FROM read_fastx('{i1}')"
        )
        # per-record R1(+R2), keyed by record order (matches I1's order).
        conn.execute(
            "CREATE OR REPLACE VIEW fr_reads AS "
            "SELECT sequence_index, read_id, sequence1, qual1, sequence2, qual2 "
            f"FROM {fr_clause}"
        )
        # assign prep_sample_idx by the Golay match; non-matching reads drop.
        conn.execute(
            "COPY (SELECT gc.prep_sample_idx, fr.sequence_index, fr.read_id, "
            "             fr.sequence1, fr.qual1, fr.sequence2, fr.qual2 "
            "      FROM idx_reads ir JOIN golay_codes gc ON gc.raw = ir.index_read "
            "      JOIN fr_reads fr USING (sequence_index)) "
            f"TO '{out}' ({PARQUET_OPTS_INTERMEDIATE})"
        )


def _sample_counts(demuxed_out: Path, duckdb_tmp: Path, *, memory_gb: int) -> list[tuple[int, int]]:
    """(prep_sample_idx, read_count) per demuxed sample, ascending. Empty → no match."""
    with open_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        rows = conn.execute(
            "SELECT prep_sample_idx, COUNT(*) FROM read_parquet(?) "
            "GROUP BY prep_sample_idx ORDER BY prep_sample_idx",
            [str(demuxed_out)],
        ).fetchall()
    return [(int(r[0]), int(r[1])) for r in rows]


def _write_sample_reads(
    demuxed_out: Path,
    prep_sample_idx: int,
    start: int,
    out_path: Path,
    duckdb_tmp: Path,
    *,
    memory_gb: int,
) -> None:
    """Write one sample's read.parquet from its slice of the demuxed intermediate:
    re-number 1..N (a scattered subset of lane indices), assign sequence_idx, sort by it.
    Atomic publish via a .partial sibling (the durable copy doubles as the retry sentinel)."""
    partial = out_path.parent / f"{out_path.name}.partial"
    safe_partial = validate_parquet_path(partial)
    try:
        with open_conn() as conn:
            apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
            conn.execute(
                "COPY (SELECT ?::BIGINT AS prep_sample_idx, "
                "  ROW_NUMBER() OVER (ORDER BY sequence_index) + ? - 1 AS sequence_idx, "
                "  read_id, sequence1, qual1, sequence2, qual2 "
                "FROM read_parquet(?) WHERE prep_sample_idx = ? ORDER BY sequence_idx) "
                f"TO '{safe_partial}' ({PARQUET_OPTS})",
                [prep_sample_idx, start, str(demuxed_out), prep_sample_idx],
            )
        os.replace(partial, out_path)
    finally:
        partial.unlink(missing_ok=True)


def _hardlink(src: Path, dst: Path) -> None:
    dst.unlink(missing_ok=True)
    try:
        os.link(src, dst)
    except OSError:
        import shutil  # noqa: PLC0415

        shutil.copyfile(src, dst)


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Golay-demultiplex the pool's multiplexed FASTQ and ingest per-sample reads.
    Returns {"read_staging_dir": workspace} for register-files. StepNoData when no
    read matches any prep barcode."""
    workspace = workspace.resolve()
    inputs.index_reads_path = inputs.index_reads_path.resolve()
    inputs.forward_reads_path = inputs.forward_reads_path.resolve()
    inputs.golay_table_path = inputs.golay_table_path.resolve()
    inputs.barcode_map = inputs.barcode_map.resolve()
    for p in (
        inputs.index_reads_path,
        inputs.forward_reads_path,
        inputs.golay_table_path,
        inputs.barcode_map,
    ):
        if not p.exists():
            raise FileNotFoundError(f"golay_demux input not found: {p}")

    workspace.mkdir(parents=True, exist_ok=True)
    register_dir = workspace / "read"
    register_dir.mkdir(parents=True, exist_ok=True)
    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS)

    with duckdb_tmp_dir(workspace) as duckdb_tmp:
        demuxed = workspace / "_demuxed.parquet"
        _run_demux(inputs, demuxed, duckdb_tmp, memory_gb=memory_gb)
        counts = _sample_counts(demuxed, duckdb_tmp, memory_gb=memory_gb)
        if not counts:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason=(
                    f"sequenced_pool {inputs.sequenced_pool_idx}: no read matched any prep "
                    f"barcode within the Golay error threshold"
                ),
            )

        async with make_cp_client() as http:
            for prep_sample_idx, count in counts:
                durable = compute_reads_staging_path(inputs.reads_staging_root, prep_sample_idx)
                durable.parent.mkdir(parents=True, exist_ok=True)
                start = await mint_or_reuse_sequence_range(
                    http,
                    prep_sample_idx,
                    count,
                    work_ticket_idx=inputs.work_ticket_idx,
                    step_name=YAML_STEP_NAME,
                )
                await asyncio.to_thread(
                    _write_sample_reads,
                    demuxed,
                    prep_sample_idx,
                    start,
                    durable,
                    duckdb_tmp,
                    memory_gb=memory_gb,
                )
                _hardlink(durable, register_dir / f"{prep_sample_idx}.parquet")

        demuxed.unlink(missing_ok=True)

    return {"read_staging_dir": workspace}
