"""Golay-barcode demux of a pool's multiplexed 16S run into the DuckLake `read`
table — the analogue of bcl-convert+ingest_reads for runs that arrive as one
multiplexed FASTQ set (R1 + R2 + I1) rather than a per-sample BCL demux. Ported
from duckdb-miint's demux_qiita.sql.

Two halves: demux (build the Golay cloud, pair I1 against R1/R2 by record order with
I1 reverse-complemented, assign each read to its prep_sample) and ingest (mint a
sequence_idx range per sample, write the sorted read.parquet). R2 rides through,
unused downstream.

TODO(converge): the per-sample write duplicates fastq_to_parquet/ingest_reads; fold
into a shared read-ingest core.
"""

from __future__ import annotations

import asyncio
import itertools
import math
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

# per-sample writes run one at a time, so each gets the full budget.
_DUCKDB_THREADS = 4
_DUCKDB_FALLBACK_MEMORY_GB = 8

# extended binary Golay [24,12], the EMP 16S barcode code. a 12-nt barcode is
# 24 bits at 2 bits/nt (A=11 C=00 T=10 G=01); the 4096 codewords are the valid
# barcodes. `errors` is bit-distance to the nearest codeword; <=3 correctable,
# >=4 ambiguous. generated in-job (matches the vendored table for errors<=3,
# pinned by a test), so no fixed operator path.
#
# systematic generator: codeword(m) = (m<<12) | parity(m); the parity basis was
# extracted from the code itself.
_GOLAY_PARITY = (
    0b011111111111,
    0b111011100010,
    0b110111000101,
    0b101110001011,
    0b111100010110,
    0b111000101101,
    0b110001011011,
    0b100010110111,
    0b100101101110,
    0b101011011100,
    0b110110111000,
    0b101101110001,
)
_BITS_TO_NT = ("C", "G", "T", "A")  # 2-bit value: 00->C 01->G 10->T 11->A
_MAX_CORRECTABLE = 3  # errors>=4 are ambiguous, never joined


def _golay_codeword(message: int) -> int:
    """the 24-bit codeword for a 12-bit message (systematic)."""
    parity = 0
    for i in range(12):
        if message & (1 << (11 - i)):
            parity ^= _GOLAY_PARITY[i]
    return (message << 12) | parity


def _bits_to_dna(word: int) -> str:
    """a 24-bit word to its 12-nt barcode (2 bits/nt)."""
    return "".join(_BITS_TO_NT[(word >> (2 * (11 - p))) & 0b11] for p in range(12))


def _golay_cloud_rows(max_errors: int) -> list[tuple[str, str, int]]:
    """(raw, corrected, errors) for every codeword and its neighbours within
    `max_errors` flips. neighbours are unique for max_errors<=3, so no dedup.
    default threshold 1.5 gives radius 1 and 102,400 rows."""
    rows: list[tuple[str, str, int]] = []
    for message in range(4096):
        codeword = _golay_codeword(message)
        corrected = _bits_to_dna(codeword)
        for k in range(max_errors + 1):
            for combo in itertools.combinations(range(24), k):
                word = codeword
                for bit in combo:
                    word ^= 1 << bit
                rows.append((_bits_to_dna(word), corrected, k))
    return rows


def _correctable_radius(threshold: float) -> int:
    """largest error count below `threshold`, capped at _MAX_CORRECTABLE."""
    below = math.ceil(threshold) - 1 if float(threshold).is_integer() else math.floor(threshold)
    return max(0, min(_MAX_CORRECTABLE, below))


def _build_golay_cloud(conn, max_errors: int) -> None:
    """build the `golay_cloud(raw, corrected, errors)` table the demux joins."""
    import pyarrow as pa  # noqa: PLC0415

    rows = _golay_cloud_rows(max_errors)
    src = pa.table(
        {
            "raw": pa.array([r[0] for r in rows], pa.string()),
            "corrected": pa.array([r[1] for r in rows], pa.string()),
            "errors": pa.array([r[2] for r in rows], pa.int32()),
        }
    )
    conn.register("_golay_cloud_src", src)
    conn.execute("CREATE OR REPLACE TABLE golay_cloud AS SELECT * FROM _golay_cloud_src")
    conn.unregister("_golay_cloud_src")


class Inputs(BaseModel):
    """input contract for golay_demux.

    index_reads_path: multiplexed I1 barcode FASTQ (12-nt Golay codes).
    forward_reads_path: multiplexed R1 FASTQ (paired to I1 by record order).
    reverse_reads_path: multiplexed R2 FASTQ (optional; kept as sequence2/qual2).
    barcode_map: runner-staged roster (prep_sample_idx, barcode, barcodes_are_rc);
        the RC flag is per-sample provenance.
    golay_error_threshold: max Golay errors to accept a match (EMP: 1.5). the
        decode cloud is generated in-job; this bounds its radius.
    reads_staging_root: scratch root for the durable per-sample copies.
    """

    index_reads_path: Path
    forward_reads_path: Path
    reverse_reads_path: Path | None = None
    barcode_map: Path
    golay_error_threshold: float = 1.5
    reads_staging_root: Path
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def _run_demux(inputs: Inputs, demuxed_out: Path, duckdb_tmp: Path, *, memory_gb: int) -> None:
    """demux the FASTQ to an intermediate parquet keyed by prep_sample_idx.
    paths are inlined (sanitised); DuckDB rejects bound params in CREATE VIEW/SET."""
    i1 = validate_parquet_path(inputs.index_reads_path)
    r1 = validate_parquet_path(inputs.forward_reads_path)
    bc = validate_parquet_path(inputs.barcode_map)
    out = validate_parquet_path(demuxed_out)
    threshold = float(inputs.golay_error_threshold)
    fr_clause = f"read_fastx('{r1}')"
    if inputs.reverse_reads_path is not None:
        r2 = validate_parquet_path(inputs.reverse_reads_path.resolve())
        fr_clause = f"read_fastx('{r1}', sequence2 := '{r2}')"

    with open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        # build the decode cloud in-job, bounded by the threshold.
        _build_golay_cloud(conn, _correctable_radius(threshold))
        # prep barcodes, RC'd per their flag; expand against the cloud.
        conn.execute(
            "CREATE OR REPLACE VIEW prep_bc AS SELECT prep_sample_idx, "
            "IF(barcodes_are_rc, sequence_dna_reverse_complement(barcode), barcode) AS barcode "
            f"FROM read_parquet('{bc}')"
        )
        conn.execute(
            "CREATE OR REPLACE TABLE golay_codes AS "
            "SELECT p.prep_sample_idx, g.raw FROM prep_bc p "
            "JOIN golay_cloud g ON p.barcode = g.corrected "
            f"WHERE g.errors < {threshold}"
        )
        conn.execute("CREATE UNIQUE INDEX gc_idx ON golay_codes(raw)")
        # I1 length varies; some runs ship longer index reads than 12 nt.
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
    """(prep_sample_idx, read_count) per sample, ascending; empty if no match."""
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
    """write one sample's read.parquet from its slice: re-number the reads,
    assign sequence_idx, sort by it. publish via a .partial sibling."""
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
    """demux the pool's FASTQ and ingest per-sample reads. returns
    {"read_staging_dir": workspace}; StepNoData when no read matches a barcode."""
    workspace = workspace.resolve()
    inputs.index_reads_path = inputs.index_reads_path.resolve()
    inputs.forward_reads_path = inputs.forward_reads_path.resolve()
    inputs.barcode_map = inputs.barcode_map.resolve()
    for p in (inputs.index_reads_path, inputs.forward_reads_path, inputs.barcode_map):
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
                reason=f"pool {inputs.sequenced_pool_idx}: no read matched a barcode",
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
