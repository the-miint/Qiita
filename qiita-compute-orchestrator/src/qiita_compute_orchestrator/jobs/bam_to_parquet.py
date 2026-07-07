"""Native job: convert a BAM/SAM/CRAM into a Parquet of reads, keyed by a
CP-minted `sequence_idx` BIGINT.

The BAM analogue of `fastq_to_parquet` — same B-staged-Parquet pipeline (parse →
intermediate → count → mint range → rewrite with sequence_idx), with miint's
`read_alignments` reader in place of `read_fastx`. It is a plain **read loader**:
it takes only `read_id`, `sequence`, and `qual` from each record and DISCARDS
everything else — alignment fields (reference/position/CIGAR) and every aux tag,
including base-modification (MM/ML methylation) and kinetics (ipd/pw) tags. If
per-read methylation ever needs preserving, that is a separate reader
(`read_sequences_sam`) and a separate table, not this job.

Scope assumptions, enforced or documented:

  - **Primary records only.** `read_alignments` emits one row per alignment
    record, so a read with secondary (FLAG 0x100) or supplementary (0x800)
    alignments would appear multiple times. We filter them out
    (`alignment_is_secondary` / `alignment_is_supplementary`) so each read maps
    to exactly one row — and one `sequence_idx`. For an unaligned uBAM (the
    long-read basecaller case) every record is primary, so the filter is a
    no-op.
  - **Single-end, enforced.** Long-read BAMs are single-end; `sequence2`/`qual2`
    are always NULL. A paired BAM (repeated QNAME across mates) is REJECTED
    (BAD_INPUT) by a duplicate-QNAME guard after the count — the per-read
    `sequence_idx` assignment below assigns one id per primary record, so two
    primary mates sharing a QNAME would get two distinct sequence_idx for one
    molecular event (the invariant `fastq_to_parquet` preserves by emitting one
    row per pair). A comment isn't a guard; the check makes the assumption
    load-bearing.
  - **Orientation.** `sequence` is the BAM SEQ field verbatim. For an unaligned
    read that IS the sequenced orientation; for a reverse-strand *aligned* read
    it is reference-forward (reverse-complemented relative to the original read).
    We store it as-is — acceptable because this job ignores strand/modification
    semantics; a strand-aware loader would reverse-complement here.

sequence_idx assignment. `read_alignments` (unlike `read_fastx`) exposes no
per-record ordinal, so the intermediate mints one via
`row_number() OVER (ORDER BY read_id)` — a deterministic total order once the
unique-QNAME guard has passed. The final `sequence_idx` is `rec_index + start -
1`; contiguous within the sample, a unique sorted join key (never a dense row
counter — see the fastq job's gap note; the same applies).

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

# DuckDB resource caps, mirroring fastq_to_parquet: the YAML allocation
# (workflows/bam-to-parquet/1.0.0.yaml: mem_gb=8, cpu=2) sizes the SLURM cgroup;
# DuckDB's cap sits just below (`mem_gb - 1` leaves ~1 GB for Python/miint/OS
# overhead). 7 GB is sized against the same long-read budget fastq_to_parquet
# documents (~2.4 GB resident/thread: 2048 STANDARD_VECTOR_SIZE × 60
# row_group_size × ~20 KB avg long-read record incl. quality). This job ADDS a
# full sort over the VARCHAR read_id (the `row_number()` ordinal below, and the
# `count(DISTINCT read_id)` guard), which spills to duckdb_tmp under the cap
# rather than growing it — so 7 GB stays the envelope; revisit against a real
# long-read uBAM MaxRSS if the sort dominates. Literal (off-SLURM fallback and
# on-SLURM cap alike).
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2


class Inputs(BaseModel):
    """Typed input contract for bam_to_parquet.

    `bam_path` is the workflow-declared input (the action_context's bam_path
    flows through here). `prep_sample_idx` and `work_ticket_idx` are
    framework-injected scope scalars; `prep_sample_idx` is also the key the CP's
    sequence-range allocator uses. `pre_minted_range` is the optional E-operator
    recovery hook — set only on a retry where a prior attempt minted then failed
    after the mint (see `PreMintedRange`).
    """

    bam_path: Path
    prep_sample_idx: int
    work_ticket_idx: int
    pre_minted_range: PreMintedRange | None = None


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """B-staged-Parquet pipeline. See module docstring for the full description."""
    if not inputs.bam_path.exists():
        raise FileNotFoundError(f"BAM file not found: {inputs.bam_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    intermediate = workspace / "_intermediate_reads.parquet"
    # Output basename is the DuckLake table name: a downstream register-files step
    # maps `read.parquet` -> the `read` table.
    out_path = workspace / "read.parquet"
    out = validate_parquet_path(out_path)

    try:
        # BAM -> intermediate Parquet. `read_alignments(?, include_seq_qual:=true)`
        # yields read_id/sequence/qual; we keep only primary records and mint a
        # deterministic per-read ordinal (rec_index) so the sequence_idx rewrite
        # below is `rec_index + start - 1`. `sequence IS NOT NULL` drops the rare
        # SEQ='*' record (can't populate the NOT NULL sequence1 column).
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=_DUCKDB_MEMORY_GB,
                threads=_DUCKDB_THREADS,
            )
            conn.execute(
                "COPY ( SELECT "
                "  row_number() OVER (ORDER BY read_id) AS rec_index,"
                "  read_id,"
                "  sequence AS sequence1,"
                "  qual AS qual1 "
                "FROM read_alignments(?, include_seq_qual := true) "
                "WHERE NOT alignment_is_secondary(flags) "
                "  AND NOT alignment_is_supplementary(flags) "
                "  AND sequence IS NOT NULL ) "
                f"TO '{intermediate}' ({PARQUET_OPTS_INTERMEDIATE})",
                [str(inputs.bam_path)],
            )

        # Count via Parquet footer (no scan). Zero usable reads → terminal NO_DATA
        # before any mint, mirroring fastq_to_parquet's empty-input handling.
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=_DUCKDB_MEMORY_GB,
                threads=_DUCKDB_THREADS,
            )
            # count(DISTINCT read_id) alongside the total: the guard below needs
            # both. This scans the intermediate's read_id column (the total alone
            # would be a footer read), a bounded cost the correctness check earns.
            count, distinct_read_ids = conn.execute(
                "SELECT count(*), count(DISTINCT read_id) FROM read_parquet(?)",
                [str(intermediate)],
            ).fetchone()

        if count == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME,
                reason=f"BAM file contains no primary reads: {inputs.bam_path}",
            )

        # Enforce the one-primary-record-per-read invariant the sequence_idx
        # assignment relies on. Duplicate QNAMEs (a paired-end BAM, whose mates
        # share a QNAME and are both primary; or any duplicate) would each get a
        # distinct sequence_idx — two identifiers for one molecular event,
        # silently corrupting the `read` table. Fail fast before the mint rather
        # than load bad rows. Runs after the empty check (an empty file trivially
        # has distinct == count == 0).
        if distinct_read_ids != count:
            raise BackendFailure(
                kind=FailureKind.BAD_INPUT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=YAML_STEP_NAME,
                reason=(
                    f"BAM has {count} primary records but only {distinct_read_ids} "
                    f"distinct read_id(s): duplicate QNAMEs are not supported — this "
                    f"loader treats input as single-end and assigns one sequence_idx "
                    f"per read (a paired-end BAM, whose mates share a QNAME, must be "
                    f"loaded via a paired-aware workflow)"
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
        # sorted on disk. No miint here — a plain read_parquet carries the columns
        # through. sequence2/qual2 are NULL (single-end); prep_sample_idx is a
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
                "  rec_index + ? - 1 AS sequence_idx,"
                "  read_id, sequence1, qual1,"
                "  NULL::VARCHAR AS sequence2, NULL::UTINYINT[] AS qual2 "
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
