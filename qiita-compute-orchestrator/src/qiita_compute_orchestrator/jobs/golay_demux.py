"""Golay-barcode demux of a pool's multiplexed 16S run into the DuckLake `read`
table — the amplicon analogue of ingest_reads. It runs after bcl_convert, which
sent every read to Undetermined (the dummy no-index sheet) and emitted the Golay
I1 index, and reads that convert_dir's I1/R1/R2 FASTQs directly.

Two halves: demux (build the Golay cloud, pair I1 against R1/R2 by record order with
I1 reverse-complemented, assign each read to its prep_sample) and ingest (mint a
sequence_idx range per sample, write the sorted read.parquet). The I1 index keys the
record, so R1 and R2 land under the same prep_sample; R2 is persisted as
sequence2/qual2 in `read` (deblur is R1-only, matching the GG2 V4 catalog).

The per-sample mint + sorted write is the shared `read_staging` core `ingest_reads`
and `ingest_ena_reads` use; golay only differs in re-numbering each sample's slice of
the pooled demux intermediate (see the `write_sorted_reads` call below).
"""

from __future__ import annotations

import asyncio
import itertools
import math
from pathlib import Path

from pydantic import BaseModel
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import StepNoData
from qiita_common.parquet import validate_parquet_path

from ..cp_client import make_cp_client
from ..miint import (
    PARQUET_OPTS_INTERMEDIATE,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_conn,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ..read_staging import hardlink, write_sorted_reads
from ..sequence_range_retry import mint_or_reuse_sequence_range

YAML_STEP_NAME = "golay_demux"

# per-sample writes run one at a time, so each gets the full budget.
_DUCKDB_THREADS = 4
_DUCKDB_FALLBACK_MEMORY_GB = 8

# extended binary Golay [24,12,8], the EMP 16S barcode code. a 12-nt barcode is
# 24 bits at 2 bits/nt (A=11 C=00 T=10 G=01); the 4096 codewords are the valid
# barcodes. `errors` is bit-distance to the nearest codeword; <=3 correctable,
# >=4 ambiguous. generated in-job from the systematic generator below, so no
# vendored table or fixed operator path.
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

    convert_dir: the bcl_convert step's output dir, holding the pool's Undetermined
        I1/R1/R2 FASTQs (no per-sample demux happened upstream — the dummy sheet
        sent everything to Undetermined and emitted the Golay I1 index).
    barcode_map: runner-staged roster (prep_sample_idx, barcode, barcodes_are_rc);
        the RC flag is per-sample provenance.
    golay_error_threshold: max Golay errors to accept a match (EMP: 1.5). the
        decode cloud is generated in-job; this bounds its radius.
    reads_staging_root: scratch root for the durable per-sample copies.
    """

    convert_dir: Path
    barcode_map: Path
    golay_error_threshold: float = 1.5
    reads_staging_root: Path
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


class _UndeterminedReads(BaseModel):
    """The I1/R1/R2 FASTQs bcl_convert wrote for the pool's Undetermined reads."""

    index_reads_path: Path
    forward_reads_path: Path
    reverse_reads_path: Path | None = None


def _find_undetermined(convert_dir: Path) -> _UndeterminedReads:
    """Locate the Undetermined I1/R1/R2 FASTQs in a bcl_convert output dir.

    Requires exactly one I1 and one R1 (single-lane MiSeq Rapid 16S); a multi-lane
    run would need per-lane concatenation, which is not supported here, so it fails
    loud rather than silently reading one lane. R2 is optional but, if present,
    must also be single.
    """

    def one(tag: str, *, required: bool) -> Path | None:
        hits = sorted(convert_dir.glob(f"Undetermined_S*_{tag}_*.fastq.gz"))
        if len(hits) == 1:
            return hits[0]
        if not hits and not required:
            return None
        raise ValueError(
            f"expected exactly one Undetermined {tag} FASTQ in {convert_dir}, found {len(hits)}"
        )

    return _UndeterminedReads(
        index_reads_path=one("I1", required=True),
        forward_reads_path=one("R1", required=True),
        reverse_reads_path=one("R2", required=False),
    )


def _run_demux(
    inputs: Inputs,
    reads: _UndeterminedReads,
    demuxed_out: Path,
    duckdb_tmp: Path,
    *,
    memory_gb: int,
) -> None:
    """demux the FASTQ to an intermediate parquet keyed by prep_sample_idx.
    paths are inlined (sanitised); DuckDB rejects bound params in CREATE VIEW/SET."""
    i1 = validate_parquet_path(reads.index_reads_path)
    r1 = validate_parquet_path(reads.forward_reads_path)
    bc = validate_parquet_path(inputs.barcode_map)
    out = validate_parquet_path(demuxed_out)
    threshold = float(inputs.golay_error_threshold)
    fr_clause = f"read_fastx('{r1}')"
    if reads.reverse_reads_path is not None:
        r2 = validate_parquet_path(reads.reverse_reads_path.resolve())
        fr_clause = f"read_fastx('{r1}', sequence2 := '{r2}')"

    with open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        # build the decode cloud in-job, bounded by the threshold.
        _build_golay_cloud(conn, _correctable_radius(threshold))
        # prep barcodes, upper-cased then RC'd per their flag; expand against the cloud.
        conn.execute(
            "CREATE OR REPLACE VIEW prep_bc AS SELECT prep_sample_idx, "
            "IF(barcodes_are_rc, sequence_dna_reverse_complement(upper(barcode)), upper(barcode)) "
            f"AS barcode FROM read_parquet('{bc}')"
        )
        conn.execute(
            "CREATE OR REPLACE TABLE golay_codes AS "
            "SELECT p.prep_sample_idx, g.raw FROM prep_bc p "
            "JOIN golay_cloud g ON p.barcode = g.corrected "
            f"WHERE g.errors < {threshold}"
        )
        conn.execute("CREATE UNIQUE INDEX gc_idx ON golay_codes(raw)")
        # fail loud on any barcode that decodes to no codeword (a typo, the wrong
        # barcodes_are_rc, or a non-Golay barcode) — else that sample silently drops.
        bad = conn.execute(
            "SELECT prep_sample_idx, barcode FROM prep_bc "
            "WHERE prep_sample_idx NOT IN (SELECT prep_sample_idx FROM golay_codes) "
            "ORDER BY prep_sample_idx"
        ).fetchall()
        if bad:
            named = ", ".join(f"{r[0]}:{r[1]}" for r in bad)
            raise ValueError(f"barcodes decode to no Golay codeword: {named}")
        # per-record RC'd 12-nt index read, keyed by record order. sequence1[:12] is
        # correct at any length (a shorter I1 just won't match a 12-mer codeword).
        conn.execute(
            "CREATE OR REPLACE VIEW idx_reads AS SELECT sequence_index, "
            "sequence_dna_reverse_complement(upper(sequence1[:12])) AS index_read "
            f"FROM read_fastx('{i1}')"
        )
        # per-record R1(+R2), keyed by record order (matches I1's order).
        conn.execute(
            "CREATE OR REPLACE VIEW fr_reads AS "
            "SELECT sequence_index, read_id, sequence1, qual1, sequence2, qual2 "
            f"FROM {fr_clause}"
        )
        # I1 and R1 pair positionally; a record-count mismatch means one was filtered
        # independently, which would mis-assign every read past the divergence.
        n_i1, n_fr = conn.execute(
            f"SELECT (SELECT count(*) FROM read_fastx('{i1}')), (SELECT count(*) FROM {fr_clause})"
        ).fetchone()
        if n_i1 != n_fr:
            raise ValueError(f"I1 and R1 record counts differ ({n_i1} vs {n_fr})")
        # assign prep_sample_idx by the Golay match; non-matching reads drop. sorted by
        # prep_sample_idx so each per-sample write prunes instead of full-scanning.
        conn.execute(
            "COPY (SELECT gc.prep_sample_idx, fr.sequence_index, fr.read_id, "
            "             fr.sequence1, fr.qual1, fr.sequence2, fr.qual2 "
            "      FROM idx_reads ir JOIN golay_codes gc ON gc.raw = ir.index_read "
            "      JOIN fr_reads fr USING (sequence_index) "
            "      ORDER BY gc.prep_sample_idx) "
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


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """demux the pool's FASTQ and ingest per-sample reads. returns
    {"read_staging_dir": workspace}; StepNoData when no read matches a barcode."""
    workspace = workspace.resolve()
    inputs.convert_dir = inputs.convert_dir.resolve()
    inputs.barcode_map = inputs.barcode_map.resolve()
    if not inputs.convert_dir.is_dir():
        raise FileNotFoundError(f"golay_demux convert_dir not found: {inputs.convert_dir}")
    reads = _find_undetermined(inputs.convert_dir)
    reads.index_reads_path = reads.index_reads_path.resolve()
    reads.forward_reads_path = reads.forward_reads_path.resolve()
    required = [reads.index_reads_path, reads.forward_reads_path, inputs.barcode_map]
    if reads.reverse_reads_path is not None:
        reads.reverse_reads_path = reads.reverse_reads_path.resolve()
        required.append(reads.reverse_reads_path)
    for p in required:
        if not p.exists():
            raise FileNotFoundError(f"golay_demux input not found: {p}")

    workspace.mkdir(parents=True, exist_ok=True)
    register_dir = workspace / "read"
    register_dir.mkdir(parents=True, exist_ok=True)
    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS)

    with duckdb_tmp_dir(workspace) as duckdb_tmp:
        # the demux intermediate is a full read copy; always remove it, including on
        # StepNoData and any mid-loop failure, so failed attempts don't strand it.
        demuxed = workspace / "_demuxed.parquet"
        try:
            _run_demux(inputs, reads, demuxed, duckdb_tmp, memory_gb=memory_gb)
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
                    # The demux intermediate numbers reads across ALL samples, so
                    # re-number this sample's slice 1..count with ROW_NUMBER before
                    # applying the mint offset (ingest_reads' source is already
                    # per-sample and passes sequence_index verbatim).
                    await asyncio.to_thread(
                        write_sorted_reads,
                        demuxed,
                        prep_sample_idx=prep_sample_idx,
                        sequence_idx_start=start,
                        out_path=durable,
                        duckdb_tmp=duckdb_tmp,
                        memory_gb=memory_gb,
                        threads=_DUCKDB_THREADS,
                        local_index_sql="ROW_NUMBER() OVER (ORDER BY sequence_index)",
                        where_sql=f"prep_sample_idx = {int(prep_sample_idx)}",
                    )
                    hardlink(durable, register_dir / f"{prep_sample_idx}.parquet")
        finally:
            demuxed.unlink(missing_ok=True)

    return {"read_staging_dir": workspace}
