"""Native job: convert a prep_sample's MASKED reads into a FASTQ the pacbio
assembler consumes, plus a small run-config carrying the chosen assembler.

This is the head of the pacbio-processing workflow. The runner materializes the
sample's masked reads (the `read_masked` pass-set for a `mask_idx`, via the
data-plane `export_read_masked` DoAction) into a per-ticket `reads.parquet` and
binds it as the `reads` input. The heavy tools downstream want FASTQ, not
Parquet, so this native step:

  1. Footer-counts the reads Parquet. Zero passing reads is a terminal NO_DATA
     outcome (StepNoData), NOT a failure — nothing to assemble.
  2. Streams the reads to `reads.fastq.gz` (bounded memory via batched fetch).
     Reads are single-end long reads, so only sequence1/qual1 are used. A read
     that carries no quality (`qual1` NULL) is rejected as BAD_INPUT — HiFi
     assembly input is FASTQ with per-base quality; a quality-less read is a
     contract violation, surfaced loudly rather than fabricated.
  3. Writes `run_config.json` carrying the `assembler` choice. The scalar can't
     ride a container step's `params:` (the runner treats a container input as a
     bind-mount path), so it flows through this native step into a file the
     `assemble` container reads with `jq`.

Both outputs are plain files consumed by the next (container) step: `reads_fastq`
and `run_config`.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.models import WorkTicketFailureStage

from ..miint import (
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_conn,
    resolve_duckdb_memory_gb,
)

# YAML step name this module implements. Hard-coded because execute() raises
# BackendFailures/StepNoData itself (which need a step_name); a rename here that
# diverges from the `- step: pacbio_export_reads` YAML entry fails loudly.
YAML_STEP_NAME = "pacbio_export_reads"

# Output basenames the next (container) step reads via params.json `.inputs.*`.
_FASTQ_NAME = "reads.fastq.gz"
_RUN_CONFIG_NAME = "run_config.json"

# Rows fetched per batch when streaming reads to FASTQ — bounds peak memory
# independent of sample size (a HiFi metagenome can be millions of reads).
_FETCH_ROWS = 50_000

# Off-SLURM DuckDB fallbacks; under SLURM the real cgroup drives the cap. This is
# a light streaming read, so the literals are only a floor.
_DUCKDB_FALLBACK_GB = 4
_DUCKDB_THREADS = 2


class Inputs(BaseModel):
    """Typed input contract for pacbio_export_reads.

    `masked_reads` is the per-ticket masked `reads.parquet` (binding name
    `masked_reads`, materialized by the runner via export_read_masked).
    `assembler` selects the step-1 tool and is stamped into run_config.json.
    `prep_sample_idx`/`work_ticket_idx` are framework-injected scope scalars.
    """

    masked_reads: Path
    assembler: Literal["hifiasm_meta", "myloasm"] = "hifiasm_meta"
    prep_sample_idx: int
    work_ticket_idx: int


def _phred_to_ascii(qual: list[int]) -> str:
    """Encode PHRED integer qualities as a Sanger FASTQ quality string
    (offset 33). HiFi qualities fit the 0..93 printable range."""
    return "".join(chr(q + 33) for q in qual)


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    reads = inputs.masked_reads
    if not reads.exists():
        raise FileNotFoundError(
            f"masked reads Parquet not found: {reads} "
            f"(the runner's export_read_masked did not materialize it)"
        )

    workspace.mkdir(parents=True, exist_ok=True)
    fastq_out = workspace / _FASTQ_NAME
    run_config_out = workspace / _RUN_CONFIG_NAME

    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_GB, threads=_DUCKDB_THREADS)

    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)

        # Count via the Parquet footer (no scan). 0 reads → terminal NO_DATA
        # before any write, mirroring the other read-ingest jobs' empty handling.
        count = conn.execute("SELECT count(*) FROM read_parquet(?)", [str(reads)]).fetchone()[0]
        if count == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason=f"no masked reads to assemble: {reads}",
            )

        # Stream reads to FASTQ. Order is irrelevant to assembly, so no ORDER BY
        # (avoids a full sort of a multi-million-read sample). Only sequence1 /
        # qual1 — these are single-end long reads.
        conn.execute("SELECT read_id, sequence1, qual1 FROM read_parquet(?)", [str(reads)])
        with gzip.open(fastq_out, "wt") as fq:
            while True:
                rows = conn.fetchmany(_FETCH_ROWS)
                if not rows:
                    break
                for read_id, sequence1, qual1 in rows:
                    if qual1 is None:
                        raise BackendFailure(
                            kind=FailureKind.BAD_INPUT,
                            stage=WorkTicketFailureStage.STEP_RUN,
                            step_name=YAML_STEP_NAME,
                            reason=(
                                f"read {read_id!r} has no quality — FASTQ assembly "
                                f"input requires per-base quality (masked reads "
                                f"Parquet: {reads})"
                            ),
                        )
                    fq.write(f"@{read_id}\n{sequence1}\n+\n{_phred_to_ascii(qual1)}\n")

    run_config_out.write_text(json.dumps({"assembler": inputs.assembler}) + "\n")

    return {"reads_fastq": fastq_out, "run_config": run_config_out}
