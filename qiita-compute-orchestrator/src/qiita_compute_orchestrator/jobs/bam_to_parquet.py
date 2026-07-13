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
modified between work_ticket submission and step execution — the retry path
reuses the range a crashed attempt minted, which is only valid while the read
count is stable across attempts.
"""

from __future__ import annotations

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
from ..sequence_range_retry import mint_or_reuse_sequence_range

# YAML step name this module implements. Hard-coded because execute() raises
# BackendFailures itself (which need a step_name); the integration smoke asserts
# work_ticket.failure_step_name, so a rename here that diverges from the
# `- step: bam` YAML entry fails loudly.
YAML_STEP_NAME = "bam"

# DuckDB resource caps. The limit is sized from the REAL SLURM cgroup via
# `resolve_duckdb_memory_gb`, not a literal: a hardcoded cap silently ignores both
# the per-run `--mem-gb` override and the runner's OOM memory escalation, so an
# escalated retry would re-OOM at the same in-process limit no matter how much
# SLURM granted it. `_DUCKDB_MEMORY_GB` is the OFF-SLURM fallback only (local
# backend / tests), where there is no cgroup to read; it is NOT the YAML baseline
# minus headroom, and deliberately stays small so a dev box isn't asked for 29 GB.
#
# What actually needs the memory is the final sorted COPY — a BLOCKING operator over
# the full seq+qual payload. A PacBio HiFi read is ~15-25 kB of seq+qual against
# Illumina's ~150 bp, so a routine 2M-read sample is tens of GB to sort. That is
# where the first real PacBio run died (`Out of buffer` inside the COPY), and it is
# why the YAML baseline is long-read-sized rather than inherited from
# fastq_to_parquet.
#
# The PARSE is not the problem: read_sequences_sam streams, and miint projects the
# aux tags (MM/ML, ipd/pw) away rather than materialising them into the pipeline.
# An earlier version of this comment blamed htslib's per-record tag handling for
# peak memory; nothing observed supports that, and the failure was in the sort.
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2

# Target UNCOMPRESSED payload per output part (see _write_read_parts). This is what
# bounds peak memory: the only blocking operator left is the per-part ORDER BY, which
# sorts at most this much. 1 GiB keeps a part comfortably inside the DuckDB limit
# even at the off-SLURM fallback, while staying far above the 64 MB row-group target
# so each part still holds many row groups.
#
# There is deliberately NO plan(). Memory no longer scales with the input — it is
# flat in the batch size — so there is nothing for a down-only sizing hint to
# usefully lower, and a walltime hint risks burning one of the ticket's three shared
# retries on a TIMEOUT. The job is now a streaming job, in the sense
# docs/writing-a-job.md means it.
_ROWS_PER_PART_TARGET_BYTES = 1024**3


class Inputs(BaseModel):
    """Typed input contract for bam_to_parquet.

    `bam_path` is the workflow-declared input (the action_context's bam_path
    flows through here). `expect_unaligned` is the caller's alignment-state
    declaration (threaded from action_context via the step's `params:`; the
    sequence-load step sets it True). Defaults True — a hand-submitted ticket that
    omits it still gets the unaligned verification. `prep_sample_idx` and
    `work_ticket_idx` are framework-injected scope scalars; `prep_sample_idx` is
    also the key the CP's sequence-range allocator uses, and `work_ticket_idx` is
    what proves an orphaned range belongs to THIS step before it is reused.
    """

    bam_path: Path
    expect_unaligned: bool = True
    prep_sample_idx: int
    work_ticket_idx: int


def _write_read_parts(
    conn,
    *,
    intermediate: Path,
    read_dir: Path,
    prep_sample_idx: int,
    sequence_idx_start: int,
    count: int,
) -> int:
    """Write the final reads as `read/part_*.parquet`, in bounded monotone batches.

    Returns the number of parts written.

    WHY NOT ONE SORTED COPY. The obvious form —

        COPY (SELECT ... FROM read_parquet(intermediate) ORDER BY sequence_idx)
        TO 'read.parquet'

    — is a BLOCKING sort over the full seq+qual payload. For a PacBio HiFi sample
    (~15-25 kB per read, millions of reads) that is tens of GB, and it is exactly
    where the first real PacBio run died. Paying for it with a bigger allocation
    would be paying for work we do not need to do.

    What the ORDER BY actually buys is NOT a globally sorted file — `PARQUET_OPTS`
    says so explicitly: with `preserve_insertion_order=false` (which
    `ROW_GROUP_SIZE_BYTES` requires) "row groups land in thread-finish order". What
    it buys is that each row group stays CLUSTERED on the sort key, giving tight
    per-group min/max, which is what DuckLake pruning and Parquet predicate pushdown
    actually read.

    Clustering does not need a global sort, because the data is already monotone:
    `sequence_idx = sequence_index + start - 1`, and `sequence_index` is
    read_sequences_sam's per-file 1-based ordinal. So we slice on `sequence_index`
    and write one part per slice. Every row in part N has a sequence_idx strictly
    below every row in part N+1, so each part's row groups carry a tight, disjoint
    min/max — the same pruning the global sort produced. The ORDER BY inside a part
    then sorts at most `_ROWS_PER_PART_TARGET_BYTES` worth of payload, so peak memory
    is bounded by the batch, not by the sample.

    The multi-file table shape is not new: `register-files` maps a top-level subdir
    of `part_*.parquet` to the table named after the directory, which is what
    reference_load and hash_sequences already do — and for this same reason (a
    single-file sort+write of a large payload OOMs DuckDB).
    """
    # Size the batch from the intermediate's UNCOMPRESSED payload, read from the
    # Parquet footer (parquet_metadata is metadata-only — no data scan). Row count
    # alone would be a poor proxy: HiFi reads are ~100x an Illumina read.
    uncompressed_bytes = (
        conn.execute(
            "SELECT coalesce(sum(total_uncompressed_size), 0) FROM parquet_metadata(?)",
            [str(intermediate)],
        ).fetchone()[0]
        or 0
    )
    bytes_per_row = max(1, uncompressed_bytes // max(1, count))
    rows_per_part = max(1, _ROWS_PER_PART_TARGET_BYTES // bytes_per_row)

    # Created here, not up front: a run that fails before this point (bad input, a
    # duplicate QNAME, a refused mint) must leave NO output directory behind for the
    # launcher's manifest walker to find.
    read_dir.mkdir(parents=True, exist_ok=True)

    parts = 0
    for lo in range(1, count + 1, rows_per_part):
        hi = min(lo + rows_per_part - 1, count)
        part = validate_parquet_path(read_dir / f"part_{parts:05d}.parquet")
        conn.execute(
            "COPY ( SELECT "
            "  ?::BIGINT AS prep_sample_idx,"
            "  sequence_index + ? - 1 AS sequence_idx,"
            "  read_id, sequence1, qual1, sequence2, qual2 "
            "FROM read_parquet(?) "
            "WHERE sequence_index BETWEEN ? AND ? "
            # Bounded: at most one batch's payload, not the whole sample. Still
            # explicit — preserve_insertion_order=false means DuckDB is free to emit
            # a batch's rows out of order, and the clustering is the point.
            "ORDER BY sequence_idx ) "
            f"TO '{part}' ({PARQUET_OPTS})",
            [prep_sample_idx, sequence_idx_start, str(intermediate), lo, hi],
        )
        parts += 1
    return parts


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
    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS)

    workspace.mkdir(parents=True, exist_ok=True)
    intermediate = workspace / "_intermediate_reads.parquet"
    # `read` is a DIRECTORY of part_*.parquet rather than a single read.parquet: the
    # final write is batched (see _write_read_parts). register-files already maps a
    # top-level subdir of parts to the table named after the directory — the same
    # multi-file form reference_load/hash_sequences use, and for the same reason.
    read_dir = workspace / "read"

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

        # Mint the sequence_idx range from the CP. A 409 is NOT automatically a
        # failure: `mint_or_reuse_sequence_range` reads back the existing range and
        # reuses it IF this ticket minted it (a prior crashed attempt of this step),
        # so an OOM-escalated retry completes instead of dying on the one-shot mint
        # contract. A range minted by a DIFFERENT ticket means the reads are already
        # loaded, and it refuses — reuse would register them twice.
        async with make_cp_client() as http:
            sequence_idx_start = await mint_or_reuse_sequence_range(
                http,
                inputs.prep_sample_idx,
                count,
                work_ticket_idx=inputs.work_ticket_idx,
                step_name=YAML_STEP_NAME,
            )

        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=memory_gb,
                threads=_DUCKDB_THREADS,
            )
            _write_read_parts(
                conn,
                intermediate=intermediate,
                read_dir=read_dir,
                prep_sample_idx=inputs.prep_sample_idx,
                sequence_idx_start=sequence_idx_start,
                count=count,
            )
    finally:
        # Clean up the intermediate BEFORE returning so the SLURM launcher's
        # manifest walker (which runs after execute()) sees only the read/ parts.
        intermediate.unlink(missing_ok=True)

    # The workspace holds only read/part_*.parquet (intermediate unlinked above),
    # exposed as read_staging_dir so a register-files step loads the parts into the
    # DuckLake `read` table.
    return {"read_staging_dir": workspace}
