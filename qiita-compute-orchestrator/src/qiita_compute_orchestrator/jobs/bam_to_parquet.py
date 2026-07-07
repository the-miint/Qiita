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

**Unaligned uBAM, enforced.** The target input is a basecaller uBAM (PacBio HiFi
/ ONT dorado), where every record is unaligned — `read_sequences_sam` returns
`SEQ` verbatim, which for an unaligned read IS the sequenced orientation. An
ALIGNED BAM (e.g. pbmm2/minimap2 output) carries reverse-strand records whose
`SEQ` is reference-forward — reverse-complemented relative to the original read —
which this loader would store mis-oriented, silently. `read_sequences_sam`
exposes no FLAG column, so the caller DECLARES the expectation via
`expect_unaligned` (the sequence-load step sets it True) and this job CONFIRMS it:
a flags-only `read_alignments` pass rejects (BAD_INPUT) if any record is mapped,
before the parse. `expect_unaligned=False` (a caller asserting an aligned BAM) is
not supported yet and is rejected outright. This turns "unaligned only" from a
silent assumption into a fail-loud contract.

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

from pathlib import Path

import httpx
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
)
from ..sequence_range import (
    PreMintedRange,
    PrepSampleNotEligibleForSequenceRange,
    SequenceRangeAlreadyExists,
    mint_sequence_range,
)
from ..sequence_range_retry import cp_call_failure, cp_call_with_retry

# YAML step name this module implements. Hard-coded because execute() raises
# BackendFailures itself (which need a step_name); the integration smoke asserts
# work_ticket.failure_step_name, so a rename here that diverges from the
# `- step: bam` YAML entry fails loudly.
YAML_STEP_NAME = "bam"

# DuckDB resource caps, mirroring fastq_to_parquet's rationale: the YAML
# allocation (workflows/bam-to-parquet/1.0.0.yaml: mem_gb=8, cpu=2) sizes the
# SLURM cgroup; DuckDB's caps sit just below (`mem_gb - 1` leaves ~1 GB for
# Python/miint/OS overhead). 7 GB matches fastq_to_parquet's cap, but note a BAM
# record is heavier than a FASTQ read: a PacBio HiFi / ONT uBAM record carries
# per-base modification (MM/ML) and kinetics (ipd/pw) aux tags that htslib
# materializes with the whole record BEFORE read_sequences_sam projects them away
# — so peak parse memory reflects the tagged record, not the ~20 KB seq+qual this
# job keeps. The pipeline is the flags-only read_alignments verify pass, the
# read_sequences_sam parse, a footer count + `count(DISTINCT read_id)` aggregate,
# and a sorted COPY — all spill to duckdb_tmp under the cap. Revisit against a
# real kinetics-laden uBAM MaxRSS if the parse dominates.
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2


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

    workspace.mkdir(parents=True, exist_ok=True)
    intermediate = workspace / "_intermediate_reads.parquet"
    # Output basename is the DuckLake table name: a downstream register-files step
    # maps `read.parquet` -> the `read` table.
    out_path = workspace / "read.parquet"
    out = validate_parquet_path(out_path)

    try:
        # BAM -> intermediate Parquet. read_sequences_sam yields the same
        # read_fastx-compatible columns (incl. the per-record `sequence_index`
        # ordinal used for the sequence_idx rewrite below), so this SELECT mirrors
        # fastq_to_parquet's read_fastx SELECT. `comment` is dropped (unused).
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=_DUCKDB_MEMORY_GB,
                threads=_DUCKDB_THREADS,
            )

            # Confirm the declared `expect_unaligned`: read_sequences_sam exposes
            # no FLAG column, so verify via a flags-only read_alignments pass and
            # reject BAD_INPUT if ANY record is mapped. `LIMIT 1` + DuckDB's
            # predicate pushdown into read_alignments (HTSlib-layer flag filter)
            # short-circuit on the first aligned record, so an aligned BAM fails
            # fast — before the seq/qual parse below. A uBAM (every record
            # unmapped) scans flags only, then proceeds.
            aligned = conn.execute(
                "SELECT 1 FROM read_alignments(?) WHERE NOT alignment_is_unmapped(flags) LIMIT 1",
                [str(inputs.bam_path)],
            ).fetchone()
            if aligned is not None:
                raise BackendFailure(
                    kind=FailureKind.BAD_INPUT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=YAML_STEP_NAME,
                    reason=(
                        f"expect_unaligned is set but the BAM contains aligned "
                        f"records: {inputs.bam_path}. This loader stores SEQ verbatim "
                        f"and would mis-orient reverse-strand reads — supply an "
                        f"unaligned basecaller uBAM"
                    ),
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
                memory_gb=_DUCKDB_MEMORY_GB,
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
        # `pre_minted_range` (E-operator recovery). Same typed-exception → explicit
        # BackendFailure mapping as fastq_to_parquet (the framework dispatcher only
        # wraps bare NotImplementedError/FileNotFoundError/ValueError).
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
            try:
                async with make_cp_client() as http:
                    rng = await cp_call_with_retry(
                        lambda: mint_sequence_range(
                            http=http, prep_sample_idx=inputs.prep_sample_idx, count=count
                        )
                    )
            except SequenceRangeAlreadyExists as exc:
                raise BackendFailure(
                    kind=FailureKind.UNKNOWN_PERMANENT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=YAML_STEP_NAME,
                    reason=str(exc),
                ) from exc
            except PrepSampleNotEligibleForSequenceRange as exc:
                raise BackendFailure(
                    kind=FailureKind.BAD_INPUT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=YAML_STEP_NAME,
                    reason=str(exc),
                ) from exc
            except (httpx.HTTPStatusError, httpx.TransportError) as exc:
                raise cp_call_failure(
                    inputs.prep_sample_idx, exc, step_name=YAML_STEP_NAME
                ) from exc
            sequence_idx_start = rng.sequence_idx_start

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
                memory_gb=_DUCKDB_MEMORY_GB,
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
