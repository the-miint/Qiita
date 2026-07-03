"""Native job: Golay-barcode demultiplex a pool's multiplexed 16S run into the
DuckLake `read` table, once. Ports demux_qiita.sql as the `golay-demux` workflow's
EMP-style read-storage head; the barcode map arrives as a runner-staged Parquet."""

from __future__ import annotations

import asyncio
import csv
import os
from pathlib import Path

import httpx
from pydantic import BaseModel
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.models import WorkTicketFailureStage
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
from ..sequence_range import (
    PrepSampleNotEligibleForSequenceRange,
    SequenceRangeAlreadyExists,
    mint_sequence_range,
)
from ..sequence_range_retry import cp_call_failure, cp_call_with_retry

YAML_STEP_NAME = "golay_demux"

# read_fastx over a whole multiplexed lane is the heavy serial part; keep DuckDB
# modest and let the per-sample ingest run a few samples at once.
_DUCKDB_THREADS = 4
_DUCKDB_FALLBACK_MEMORY_GB = 8
_CONCURRENCY = 4


class Inputs(BaseModel):
    """Typed input contract for golay_demux.

    barcode_map: runner-staged Parquet `(prep_sample_idx, barcode, barcodes_are_rc)`;
        barcodes_are_rc is a per-barcode sample-sheet fact (replaces the old uniform
        revcomp_barcodes param), so the demux RC's each barcode by its own flag.
    """

    index_reads_path: Path
    forward_reads_path: Path
    golay_table_path: Path
    barcode_map: Path
    # 1.5 is the EMP Golay error tolerance — a real algorithm knob, so a step param.
    golay_error_threshold: float = 1.5
    reads_staging_root: Path
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def _run_demux(inputs: Inputs, demuxed_out: Path, duckdb_tmp: Path, *, memory_gb: int) -> None:
    """Run the ported demux_qiita.sql over the multiplexed FASTQ, writing one demuxed
    row per R1 read to an intermediate Parquet.
    Paths are inlined (sanitised) because DuckDB rejects a bound param in CREATE VIEW /
    SET VARIABLE.
    """
    i1 = validate_parquet_path(inputs.index_reads_path)
    r1 = validate_parquet_path(inputs.forward_reads_path)
    golay = validate_parquet_path(inputs.golay_table_path)
    bc_map = validate_parquet_path(inputs.barcode_map)
    out = validate_parquet_path(demuxed_out)
    threshold = float(inputs.golay_error_threshold)

    with open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)

        # prep barcodes, each RC'd per its own `barcodes_are_rc` flag.
        conn.execute(
            "CREATE OR REPLACE VIEW prep_metadata AS "
            "SELECT prep_sample_idx, "
            "       IF(barcodes_are_rc, sequence_dna_reverse_complement(barcode), barcode) "
            "         AS barcode "
            f"FROM read_parquet('{bc_map}')"
        )
        # expand each prep barcode over the Golay correction cloud; unique index on
        # `raw` makes the demux join a hash lookup.
        conn.execute(
            "CREATE OR REPLACE TABLE golay_codes AS "
            "SELECT pm.prep_sample_idx, gc.raw, gc.errors "
            "FROM prep_metadata pm "
            f"JOIN read_parquet('{golay}') gc ON pm.barcode = gc.corrected "
            f"WHERE gc.errors < {threshold}"
        )
        conn.execute("CREATE UNIQUE INDEX gc_idx ON golay_codes(raw)")

        # some runs ship index reads longer than the 12-nt Golay code; detect once.
        conn.execute(
            "SET VARIABLE demux_i1_is_12nt = ("
            f"  SELECT length(sequence1) = 12 FROM read_fastx('{i1}') LIMIT 1)"
        )
        # pair I1 against R1 by record order; I1 is RC'd (Illumina submits it RC vs
        # the prep). R1 (sequence2/qual2) is carried as sequence1/qual1.
        conn.execute(
            "CREATE OR REPLACE VIEW raw_reads AS "
            "SELECT sequence_dna_reverse_complement("
            "         IF(getvariable('demux_i1_is_12nt'), sequence1, sequence1[:12])"
            "       ) AS index_read, "
            "       sequence_index, read_id, sequence2 AS sequence1, qual2 AS qual1 "
            f"FROM read_fastx('{i1}', sequence2 := '{r1}')"
        )
        # assign prep_sample_idx by joining the index_read against the Golay cloud;
        # non-matching reads are dropped.
        conn.execute(
            "COPY (SELECT gc.prep_sample_idx, r.sequence_index, r.read_id, "
            "             r.sequence1, r.qual1 "
            "      FROM golay_codes gc JOIN raw_reads r ON gc.raw = r.index_read) "
            f"TO '{out}' ({PARQUET_OPTS_INTERMEDIATE})"
        )


def _write_demux_stats(counts: list[tuple[int, int]], stats_out: Path) -> None:
    """Per-sample read-count sidecar (workspace diagnostic, not registered), written
    from the already-computed counts so no second pass over the demuxed lane."""
    with stats_out.open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(("prep_sample_idx", "demultiplexed_read_count"))
        writer.writerows(counts)


def _sample_counts(demuxed_out: Path, duckdb_tmp: Path, *, memory_gb: int) -> list[tuple[int, int]]:
    """(prep_sample_idx, read_count) for every sample with >=1 demuxed read, ascending;
    empty list means no read matched any barcode."""
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
    sequence_idx_start: int,
    out_path: Path,
    duckdb_tmp: Path,
    *,
    memory_gb: int,
) -> None:
    """Write one sample's `read.parquet` from its slice of the demuxed intermediate,
    re-numbering `sequence_idx` from `start`; R1-only. Publish atomically via a
    `.partial` sibling — the durable path doubles as the retry sentinel."""
    partial_path = out_path.parent / f"{out_path.name}.partial"
    partial = validate_parquet_path(partial_path)
    try:
        with open_conn() as conn:
            apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
            conn.execute(
                "COPY (SELECT "
                "  ?::BIGINT AS prep_sample_idx, "
                "  ROW_NUMBER() OVER (ORDER BY sequence_index) + ? - 1 AS sequence_idx, "
                "  read_id, sequence1, qual1, "
                "  NULL::VARCHAR AS sequence2, NULL::UTINYINT[] AS qual2 "
                "FROM read_parquet(?) WHERE prep_sample_idx = ? "
                "ORDER BY sequence_idx) "
                f"TO '{partial}' ({PARQUET_OPTS})",
                [prep_sample_idx, sequence_idx_start, str(demuxed_out), prep_sample_idx],
            )
        os.replace(partial_path, out_path)
    finally:
        partial_path.unlink(missing_ok=True)


async def _mint_range(http: httpx.AsyncClient, prep_sample_idx: int, count: int) -> int:
    """Mint a contiguous sequence_idx range for one sample; return its inclusive start.
    Maps the typed mint exceptions to BackendFailures (the dispatcher only wraps bare
    ValueError / FileNotFoundError). A 409 is permanent here — unlike ingest_reads this
    job doesn't reuse a partial range, so delete the prep_sample to re-mint."""
    try:
        rng = await cp_call_with_retry(
            lambda: mint_sequence_range(http=http, prep_sample_idx=prep_sample_idx, count=count)
        )
        return rng.sequence_idx_start
    except SequenceRangeAlreadyExists as exc:
        raise BackendFailure(
            kind=FailureKind.UNKNOWN_PERMANENT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=YAML_STEP_NAME,
            reason=(
                f"prep_sample {prep_sample_idx} already has a sequence_range — a prior "
                f"demux attempt minted it; delete the prep_sample to re-mint, then resubmit"
            ),
        ) from exc
    except PrepSampleNotEligibleForSequenceRange as exc:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=YAML_STEP_NAME,
            reason=str(exc),
        ) from exc
    except (httpx.HTTPStatusError, httpx.TransportError) as exc:
        raise cp_call_failure(prep_sample_idx, exc, step_name=YAML_STEP_NAME) from exc


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Golay-demultiplex the pool's multiplexed FASTQ and ingest per-sample reads,
    returning `{"read_staging_dir": workspace}` for a register-files step. Raises
    StepNoData when no read matches any prep barcode — a whole-pool no-data outcome."""
    for path in (
        inputs.index_reads_path,
        inputs.forward_reads_path,
        inputs.golay_table_path,
        inputs.barcode_map,
    ):
        if not path.exists():
            raise FileNotFoundError(f"golay_demux input not found: {path}")

    workspace.mkdir(parents=True, exist_ok=True)
    register_dir = workspace / "read"
    register_dir.mkdir(parents=True, exist_ok=True)
    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS)

    with duckdb_tmp_dir(workspace) as duckdb_tmp:
        demuxed = workspace / "_demuxed.parquet"
        _run_demux(inputs, demuxed, duckdb_tmp, memory_gb=memory_gb)
        counts = _sample_counts(demuxed, duckdb_tmp, memory_gb=memory_gb)
        _write_demux_stats(counts, workspace / "demultiplex-stats.tsv")

        if not counts:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason=(
                    f"sequenced_pool {inputs.sequenced_pool_idx}: no read matched any "
                    f"prep barcode within the Golay error threshold"
                ),
            )

        sem = asyncio.Semaphore(_CONCURRENCY)

        async def _ingest_sample(http: httpx.AsyncClient, prep_sample_idx: int, count: int) -> None:
            async with sem:
                durable = compute_reads_staging_path(inputs.reads_staging_root, prep_sample_idx)
                durable.parent.mkdir(parents=True, exist_ok=True)
                part = register_dir / f"{prep_sample_idx}.parquet"
                sample_tmp = duckdb_tmp / str(prep_sample_idx)
                sample_tmp.mkdir(parents=True, exist_ok=True)
                start = await _mint_range(http, prep_sample_idx, count)
                await asyncio.to_thread(
                    _write_sample_reads,
                    demuxed,
                    prep_sample_idx,
                    start,
                    durable,
                    sample_tmp,
                    memory_gb=memory_gb,
                )
                _hardlink(durable, part)

        async with make_cp_client() as http:
            await asyncio.gather(*(_ingest_sample(http, psi, n) for psi, n in counts))

        # drop the large demuxed intermediate; the per-sample reads are published.
        demuxed.unlink(missing_ok=True)

    return {"read_staging_dir": workspace}


def _hardlink(src: Path, dst: Path) -> None:
    """Hardlink src to dst on the shared scratch filesystem, replacing dst."""
    dst.unlink(missing_ok=True)
    try:
        os.link(src, dst)
    except OSError:
        import shutil  # noqa: PLC0415

        shutil.copyfile(src, dst)
