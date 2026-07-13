"""Native job: convert a BAM/SAM/CRAM into a Parquet of reads, keyed by a
CP-minted `sequence_idx` BIGINT.

The BAM analogue of `fastq_to_parquet`, and structurally near-identical to it:
miint's `read_sequences_sam` emits the SAME read_fastx-compatible schema
(`sequence_index, read_id, sequence1, qual1, sequence2, qual2`) that `read_fastx`
does, so this job is fastq_to_parquet with the reader swapped — same B-staged
pipeline (parse → intermediate → count → mint range → rewrite with sequence_idx),
same `sequence_index + start - 1` assignment (no hand-rolled ordinal), same
column shape into the DuckLake `read` table.

It is a plain **read loader**: it takes only the read payload and DISCARDS all
alignment information and every aux tag — base-modification (MM/ML methylation)
and kinetics (ipd/pw) included. If per-read methylation ever needs preserving,
that is a separate reader and a separate table, not this job.

**Unaligned uBAM, declared.** The target input is a basecaller uBAM (PacBio HiFi
/ ONT dorado), where every record is unaligned — `read_sequences_sam` returns
`SEQ` verbatim, which for an unaligned read IS the sequenced orientation. An
ALIGNED BAM (e.g. pbmm2/minimap2 output) carries reverse-strand records whose
`SEQ` is reference-forward — reverse-complemented relative to the original read —
which this loader would store mis-oriented. The caller DECLARES the input is
unaligned via `expect_unaligned` (the sequence-load step sets it True); this job
TRUSTS that declaration — it does not yet VERIFY it. Verifying would need a
flags-only `read_alignments` pass (`read_sequences_sam` exposes no FLAG column),
deferred until there's a reason to pay for the extra scan; other input
restrictions can be layered on the same flag later. `expect_unaligned=False` (a
caller asserting an aligned BAM) is not supported and is rejected outright.

**One record per read.** Even within an unaligned BAM, a paired uBAM flags both
mates primary with the same QNAME, and `read_sequences_sam` emits one row per
record — so the two mates would each get a distinct `sequence_idx`, two
identifiers for one molecular event (the invariant `fastq_to_parquet` preserves
by emitting one row per pair). So after the count we ALSO reject (BAD_INPUT) any
input whose read_id is not unique (`count(DISTINCT read_id) != count(*)`). The
single-end uBAM this targets has unique QNAMEs and passes.

Input-immutability assumption, same as fastq_to_parquet: `bam_path` MUST NOT be
modified between work_ticket submission and step execution — the retry-recovery
path (`PreMintedRange`) relies on the read count being stable across attempts.
"""

from __future__ import annotations

import math
from datetime import timedelta
from pathlib import Path

from pydantic import BaseModel
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
from ..sequence_range import PreMintedRange
from ..sequence_range_retry import mint_or_reuse_sequence_range
from . import JobPlan, JobResourcePlan

# YAML step name this module implements. Hard-coded because execute() raises
# BackendFailures itself (which need a step_name); the integration smoke asserts
# work_ticket.failure_step_name, so a rename here that diverges from the
# `- step: bam` YAML entry fails loudly.
YAML_STEP_NAME = "bam"

# DuckDB resource caps. The limit is sized from the REAL SLURM cgroup via
# `resolve_duckdb_memory_gb`, not a literal: a hardcoded cap silently ignores both
# the per-run `--mem-gb` override and the runner's OOM memory escalation, so an
# escalated retry would re-OOM at the same in-process limit no matter how much
# SLURM granted it. `_DUCKDB_FALLBACK_MEMORY_GB` applies only off SLURM (local
# backend / tests), where it tracks the YAML baseline minus headroom.
#
# A BAM record is far heavier than a FASTQ read: a PacBio HiFi / ONT uBAM record
# carries per-base modification (MM/ML) and kinetics (ipd/pw) aux tags that htslib
# materializes with the whole record BEFORE read_sequences_sam projects them away,
# and the reads themselves are ~15-25 kB of seq+qual against Illumina's ~150 bp.
# The blocking operator is the final sorted COPY, whose payload is the full
# seq+qual: a 2M-read HiFi sample is tens of GB to sort. That is why the YAML
# baseline is long-read-sized rather than inherited from fastq_to_parquet.
_DUCKDB_FALLBACK_MEMORY_GB = 7
_DUCKDB_THREADS = 2

# plan() memory model. The dominant cost scales with the BAM's on-disk size (the
# sorted COPY's payload is the decompressed seq+qual it carries), so we size from
# `st_size` — a stat(), no scan, per job_resource_plan's "must stay cheap" rule.
# The multiplier is deliberately generous: plan() can only ever LOWER the step
# below its YAML baseline (the CP composes it down-only), so an over-estimate is a
# no-op that leaves the baseline in place, while an under-estimate would starve a
# real sample. Its whole job is to stop a control-sized BAM from reserving the
# long-read baseline. Refine against real MaxRSS telemetry.
_PLAN_BASE_MEM_GB = 4
_PLAN_MEM_GB_PER_BAM_GB = 4.0
_PLAN_BASE_WALLTIME_SECONDS = 900
_PLAN_WALLTIME_SECONDS_PER_BAM_GB = 600.0


class Inputs(BaseModel):
    """Typed input contract for bam_to_parquet.

    `bam_path` is the workflow-declared input (the action_context's bam_path
    flows through here). `expect_unaligned` is the caller's alignment-state
    declaration (threaded from action_context via the step's `params:`; the
    sequence-load step sets it True). Defaults True — a hand-submitted ticket that
    omits it still gets the unaligned verification. `prep_sample_idx` and
    `work_ticket_idx` are framework-injected scope scalars; `prep_sample_idx` is
    also the key the CP's sequence-range allocator uses. `pre_minted_range` is the
    optional E-operator recovery hook — set only on a retry where a prior attempt
    minted then failed after the mint (see `PreMintedRange`).
    """

    bam_path: Path
    expect_unaligned: bool = True
    prep_sample_idx: int
    work_ticket_idx: int
    pre_minted_range: PreMintedRange | None = None


def plan(inputs: Inputs) -> JobPlan:
    """Size memory + walltime from the BAM's on-disk size (a stat(), never a scan).

    Runs in the ORCHESTRATOR process at submit time, so it must stay cheap — see
    `job_resource_plan`. The YAML baseline is sized for a real long-read sample
    (millions of HiFi reads, tens of GB to sort); this exists so a control-sized
    BAM does not reserve that whole envelope and sit in the SLURM queue behind it.

    Down-only by construction: the CP applies a hint only when it is BELOW the
    baseline (and leaves an axis alone when baseline == ceiling), so this can
    never raise a step's allocation — escalation remains the only up-sizing path.
    An over-estimate is therefore a no-op, which is why the coefficients lean
    generous.
    """
    bam_gb = inputs.bam_path.stat().st_size / (1024**3)
    mem_gb = _PLAN_BASE_MEM_GB + math.ceil(_PLAN_MEM_GB_PER_BAM_GB * bam_gb)
    walltime = timedelta(
        seconds=_PLAN_BASE_WALLTIME_SECONDS + math.ceil(_PLAN_WALLTIME_SECONDS_PER_BAM_GB * bam_gb)
    )
    return JobPlan(resources=JobResourcePlan(mem_gb=mem_gb, walltime=walltime))


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """B-staged-Parquet pipeline. See module docstring for the full description."""
    if not inputs.bam_path.exists():
        raise FileNotFoundError(f"BAM file not found: {inputs.bam_path}")

    # The caller must declare an unaligned BAM. An aligned BAM would store
    # reverse-strand reads mis-oriented (see module docstring) — not supported yet.
    if not inputs.expect_unaligned:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=YAML_STEP_NAME,
            reason=(
                "expect_unaligned=False: loading an aligned BAM is not supported "
                "(reverse-strand reads would be stored in reference orientation); "
                "this workflow expects an unaligned basecaller uBAM"
            ),
        )

    # Track the real cgroup, so a `--mem-gb` override and the runner's OOM
    # escalation both actually reach DuckDB's memory_limit (a literal here would
    # cap the escalated retry at the same limit that OOM'd the first attempt).
    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS)

    workspace.mkdir(parents=True, exist_ok=True)
    intermediate = workspace / "_intermediate_reads.parquet"
    # Output basename is the DuckLake table name: a downstream register-files step
    # maps `read.parquet` -> the `read` table.
    out_path = workspace / "read.parquet"
    out = validate_parquet_path(out_path)

    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=memory_gb,
                threads=_DUCKDB_THREADS,
            )

            # BAM -> intermediate Parquet. read_sequences_sam yields the same
            # read_fastx-compatible columns (incl. the per-record `sequence_index`
            # ordinal used for the sequence_idx rewrite below), so this SELECT
            # mirrors fastq_to_parquet's read_fastx SELECT. `comment` is dropped.
            conn.execute(
                "COPY ( SELECT sequence_index, read_id, sequence1, qual1, sequence2, qual2 "
                "FROM read_sequences_sam(?) ) "
                f"TO '{intermediate}' ({PARQUET_OPTS_INTERMEDIATE})",
                [str(inputs.bam_path)],
            )

        # Count + distinct read_id in one scan: the total sizes the mint; the
        # distinct count drives the one-record-per-read guard below.
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=memory_gb,
                threads=_DUCKDB_THREADS,
            )
            count, distinct_read_ids = conn.execute(
                "SELECT count(*), count(DISTINCT read_id) FROM read_parquet(?)",
                [str(intermediate)],
            ).fetchone()

        if count == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason=f"BAM file contains no reads: {inputs.bam_path}",
            )

        # One-record-per-read guard. read_sequences_sam emits every SAM record and
        # exposes no FLAG column, so a paired mate or a secondary/supplementary
        # alignment repeats its QNAME — each would otherwise get a distinct
        # sequence_idx (two identifiers for one molecular event, corrupting the
        # `read` table). Reject before the mint rather than load bad rows. Runs
        # after the empty check (an empty file trivially has distinct == count).
        if distinct_read_ids != count:
            raise BackendFailure(
                kind=FailureKind.BAD_INPUT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=YAML_STEP_NAME,
                reason=(
                    f"BAM has {count} records but only {distinct_read_ids} distinct "
                    f"read_id(s): every read must appear exactly once. Duplicate "
                    f"QNAMEs mean paired-end mates or secondary/supplementary "
                    f"alignments, which this single-end read loader does not support "
                    f"(it targets an unaligned basecaller uBAM)"
                ),
            )

        # Mint a sequence_idx range from the CP — unless the work_ticket carries a
        # `pre_minted_range` (E-operator recovery). A 409 is NOT a failure here:
        # `mint_or_reuse_sequence_range` reads back the range a prior crashed
        # attempt left and reuses it, so an OOM-escalated retry completes instead of
        # dying on the one-shot mint contract (and instead of masking the OOM that
        # actually killed the first attempt behind a mint conflict).
        if inputs.pre_minted_range is not None:
            recovery = inputs.pre_minted_range
            recovered_count = recovery.sequence_idx_stop - recovery.sequence_idx_start + 1
            if recovered_count != count:
                raise BackendFailure(
                    kind=FailureKind.BAD_INPUT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=YAML_STEP_NAME,
                    reason=(
                        f"pre_minted_range covers {recovered_count} indices "
                        f"({recovery.sequence_idx_start}..{recovery.sequence_idx_stop}) "
                        f"but the BAM has {count} reads — the recovery range must "
                        f"match the prior attempt's mint count exactly"
                    ),
                )
            sequence_idx_start = recovery.sequence_idx_start
        else:
            async with make_cp_client() as http:
                sequence_idx_start = await mint_or_reuse_sequence_range(
                    http, inputs.prep_sample_idx, count, step_name=YAML_STEP_NAME
                )

        # Rewrite intermediate -> final with sequence_idx assigned and physically
        # sorted on disk. sequence_idx = read_sequences_sam's per-file 1-based
        # sequence_index + start - 1 (deterministic by construction — file order IS
        # the assignment order, exactly like fastq_to_parquet). No miint here — a
        # plain read_parquet carries the columns through. prep_sample_idx is a
        # per-run constant (the `read` table's scope/prune column).
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=memory_gb,
                threads=_DUCKDB_THREADS,
            )
            conn.execute(
                "COPY ( SELECT "
                "  ?::BIGINT AS prep_sample_idx,"
                "  sequence_index + ? - 1 AS sequence_idx,"
                "  read_id, sequence1, qual1, sequence2, qual2 "
                "FROM read_parquet(?) "
                "ORDER BY sequence_idx ) "
                f"TO '{out}' ({PARQUET_OPTS})",
                [inputs.prep_sample_idx, sequence_idx_start, str(intermediate)],
            )
    finally:
        # Clean up the intermediate BEFORE returning so the SLURM launcher's
        # manifest walker (which runs after execute()) sees only read.parquet.
        intermediate.unlink(missing_ok=True)

    # The workspace holds only read.parquet (intermediate unlinked above), exposed
    # as read_staging_dir so a register-files step loads it into the DuckLake
    # `read` table.
    return {"read_staging_dir": workspace}
