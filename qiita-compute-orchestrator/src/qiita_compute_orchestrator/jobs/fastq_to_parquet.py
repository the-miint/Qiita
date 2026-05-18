"""Native job: convert a FASTQ (or FASTA) file to a Parquet of reads,
keyed by a CP-minted `sequence_idx` BIGINT.

Reads via DuckDB + miint's `read_fastx` table function, mints a
contiguous bigint range from the control plane (`POST /sequence-range`
via the sibling client in ../sequence_range.py), and writes a Parquet
with one row per input read (or one row per read PAIR when an R2 file
is supplied). No deduplication — every read (pair) becomes one row,
including duplicate sequences.

Paired-end: pass `reverse_fastq_path` to read both files in lockstep
(`read_fastx(?, sequence2:=?)`). miint emits a single row per pair, so
the pair shares one `sequence_idx` (paired reads correspond to one
molecular event and must not be assigned independent identifiers).
Mate-id parity is asserted by miint's `SequenceReader::read_pe` on
parse; a mismatch surfaces as a duckdb.Error and propagates up as
BAD_INPUT via the framework dispatcher.

Schema (sorted by sequence_idx, the lake-friendly join key) — uses
miint's native column names so the file round-trips through
read_fastx-shaped consumers without aliasing:

    sequence_idx      BIGINT      NOT NULL  -- CP-minted, contiguous within this sample
    read_id           VARCHAR     NOT NULL  -- FASTQ/A record id (label, no longer the join key)
    sequence1         VARCHAR     NOT NULL  -- R1 sequence
    qual1             UTINYINT[]            -- R1 phred-decoded; NULL for FASTA
    sequence2         VARCHAR               -- R2 sequence (paired-end); NULL when unpaired
    qual2             UTINYINT[]            -- R2 phred-decoded; NULL for FASTA or unpaired

No `sequence_length` column: Parquet row-group metadata stores the
uncompressed size of every column in bytes per row group, so a
file-level length estimate is metadata-only; per-row length is
`length(sequence1)`, which DuckDB computes from the in-memory string
without a buffer copy. Carrying a precomputed BIGINT per row is a
storage cost without a query-speed win.

Pipeline (B-staged-Parquet):

  Phase 0: Reject empty input. A decompressed-stream peek (handles
           plain and .gz) catches zero-record FASTQs before any DuckDB
           work; empty input raises ValueError → BAD_INPUT, and no
           empty Parquet is emitted. This also sidesteps miint's
           "Empty file: ..." throw, so we don't depend on the upstream
           wording (cf. #39).

  Phase 1: FASTQ -> intermediate Parquet (no sequence_idx yet).
           One streaming pass through miint's read_fastx. The
           intermediate is snappy-compressed (PARQUET_OPTS_INTERMEDIATE)
           — read once by phase 4 and deleted, so decode speed beats
           on-disk size here; the final phase-4 output stays on zstd.

  Phase 2: Count via Parquet footer (sub-second; no data scan).

  Phase 3: POST /api/v1/sequence-range with the exact count. The CP
           function holds an advisory lock for the nextval/setval/INSERT
           critical section and returns the minted (start, stop) range.
           count > 0 guaranteed by phase 0.

  Phase 4: Read intermediate + assign sequence_idx via
           `sequence_index + start - 1` (miint's per-file 1-based
           index is carried through the intermediate), write the
           final Parquet sorted by sequence_idx. Assignment is
           deterministic by construction — no window function or
           extra sort needed.

  Phase 5 (try/finally): cleanup intermediate + DuckDB temp_directory
           before returning. The SLURM launcher's manifest walker runs
           AFTER execute() returns, so the transient files are
           invisible to it — the manifest sees only reads.parquet.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import httpx
from pydantic import BaseModel, Field
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_INTERMEDIATE,
    ensure_miint_installed,
    is_empty_sequence_file,
    open_conn,
)
from ..sequence_range import (
    PrepSampleNotEligibleForSequenceRange,
    SequenceRangeAlreadyExists,
    make_cp_client,
    mint_sequence_range,
)

# YAML step name this module implements. Hard-coded here because
# execute() raises BackendFailures itself (the dispatcher only wraps
# bare NotImplementedError/FileNotFoundError/ValueError) and a
# BackendFailure needs a step_name. If the workflow YAML's
# `- step: fastq` entry is ever renamed, this constant must follow —
# the integration smoke test asserts work_ticket.failure_step_name,
# so a mismatch fails loudly.
_YAML_STEP_NAME = "fastq"

# Conservative DuckDB resource caps. TODO(#38): plumb from
# JobParams.baseline_resources so each step's SLURM allocation drives
# DuckDB's own limits. For now these mirror the YAML's declared
# baseline_resources (cpu=2, mem_gb=8): DuckDB gets `mem_gb - 1` so
# Python + miint runtime have ~1 GB of headroom — DuckDB's
# memory_limit only caps DuckDB itself, not the rest of the process.
# Long-read inputs need this safely above ~2.4 GB resident per thread
# (2048 STANDARD_VECTOR_SIZE × 60 default Parquet row_group_size ×
# ~20 KB avg long-read record incl. quality). Threads stay at the
# cgroup cpu ask; oversubscribing threads doesn't kill a job, but
# exceeding memory_limit does.
_DUCKDB_MAX_MEMORY_GB = 7
_DUCKDB_MAX_THREADS = 2


def _apply_duckdb_settings(conn: duckdb.DuckDBPyConnection, duckdb_tmp: Path) -> None:
    """Apply the four DuckDB settings every pipeline connection needs.

    - `memory_limit='{N}GB'` — cap RAM so SLURM cgroups don't OOM-kill.
    - `threads={N}` — match the cgroup cpu allocation; the default
      would try to use all host cores.
    - `preserve_insertion_order=false` — let DuckDB parallelize freely.
      Determinism is guaranteed by carrying miint's per-file 1-based
      `sequence_index` column through the intermediate Parquet:
      sequence_idx = sequence_index + start - 1 is deterministic by
      construction, independent of physical row order.
    - `temp_directory='{workspace}/.duckdb_tmp'` — spill on the same
      fast scratch as the workspace, not the system /tmp (which is
      often small tmpfs).

    The canonical setting names are `memory_limit` and `threads` —
    `max_memory` / `max_threads` are aliases in newer DuckDB versions
    but not the ones miint targets, so use the canonical forms."""
    conn.execute(f"SET memory_limit='{_DUCKDB_MAX_MEMORY_GB}GB'")
    conn.execute(f"SET threads={_DUCKDB_MAX_THREADS}")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute(f"SET temp_directory='{duckdb_tmp}'")


class PreMintedRange(BaseModel):
    """Operator-supplied recovery range for a retried fastq_to_parquet
    work_ticket.

    Set only when phase 4 failed transiently on a prior attempt AFTER
    phase 3 had already minted a sequence-range — the prep_sample's
    `qiita.sequence_range` row exists and a fresh mint would 409. The
    operator (or the runner-side automation tracked as #40 section (a))
    reads the existing range, resubmits the work_ticket with this field
    populated, and the orchestrator skips the mint call.

    The two indices are inclusive on both ends and must match the
    FASTQ's read count exactly: `sequence_idx_stop - sequence_idx_start
    + 1 == count_of_reads`. Mismatch → BAD_INPUT.

    See docs/runbooks/fastq-to-parquet-retry-recovery.md for the full
    operator workflow.
    """

    sequence_idx_start: int = Field(gt=0)
    sequence_idx_stop: int = Field(gt=0)


class Inputs(BaseModel):
    """Typed input contract for fastq_to_parquet.

    `fastq_path` is the workflow-declared input (the action_context's
    fastq_path flows through here). `reverse_fastq_path` is the
    optional R2 file for paired-end input — when set, miint reads both
    files in lockstep (`read_fastx(?, sequence2:=?)`) and emits one row
    per pair, so the pair shares one `sequence_idx`. `prep_sample_idx`
    and `work_ticket_idx` are framework-injected scope scalars merged
    by `flatten_native_inputs`; `prep_sample_idx` is also the key the
    CP's sequence-range allocator uses, so it's load-bearing here
    (not just provenance as the comment used to imply).

    `pre_minted_range` is the optional E-operator recovery hook: set
    only on a retry where the prior attempt successfully minted in
    phase 3 then failed in phase 4. See `PreMintedRange` and the
    retry-recovery runbook for the full flow.
    """

    fastq_path: Path
    reverse_fastq_path: Path | None = None
    prep_sample_idx: int
    work_ticket_idx: int
    pre_minted_range: PreMintedRange | None = None


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """B-staged-Parquet pipeline. See module docstring for the
    full pipeline description."""
    if not inputs.fastq_path.exists():
        raise FileNotFoundError(f"FASTQ file not found: {inputs.fastq_path}")
    if inputs.reverse_fastq_path is not None and not inputs.reverse_fastq_path.exists():
        raise FileNotFoundError(f"reverse FASTQ file not found: {inputs.reverse_fastq_path}")

    # Empty-input check via a Python decompressed-stream peek (handles
    # plain and .gz). Surfaces empty FASTQs as BAD_INPUT before any
    # DuckDB work — no empty Parquet is written, no sequence-range is
    # minted, and we don't depend on miint's exception wording for the
    # detection (cf. #39).
    if is_empty_sequence_file(inputs.fastq_path):
        raise ValueError(f"FASTQ file contains no records: {inputs.fastq_path}")
    if inputs.reverse_fastq_path is not None and is_empty_sequence_file(inputs.reverse_fastq_path):
        raise ValueError(f"reverse FASTQ file contains no records: {inputs.reverse_fastq_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    intermediate = workspace / "_intermediate_reads.parquet"
    out_path = workspace / "reads.parquet"
    out = validate_parquet_path(out_path)
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    # Paired-end input: pass R2 via miint's `sequence2:=` named arg.
    # miint reads both streams in lockstep and emits one row per pair
    # (shared read_id, shared eventual sequence_idx) — `SequenceReader::
    # read_pe` calls `check_ids` per pair, so mate-id parity is asserted
    # at parse time. Unpaired: sequence2/quality2 come back NULL from
    # miint, so the same SELECT works for both shapes.
    if inputs.reverse_fastq_path is not None:
        read_fastx_clause = "read_fastx(?, sequence2:=?)"
        read_fastx_args: list[str] = [
            str(inputs.fastq_path),
            str(inputs.reverse_fastq_path),
        ]
    else:
        read_fastx_clause = "read_fastx(?)"
        read_fastx_args = [str(inputs.fastq_path)]

    await ensure_miint_installed()

    try:
        # Phase 1: FASTQ -> intermediate Parquet. Empty inputs were
        # rejected as BAD_INPUT above, so read_fastx is guaranteed to
        # have at least one record here.
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            conn.execute("LOAD miint;")
            # `sequence_index` (miint's 1-based per-file row index) is
            # carried through the intermediate so phase 4 can assign
            # sequence_idx via `sequence_index + start - 1` without a
            # window function. Reset-per-file isn't a concern: we
            # always pass exactly one file (or one R1/R2 pair) per
            # execute() call.
            conn.execute(
                "COPY ( SELECT sequence_index,"
                "         read_id,"
                "         sequence1,"
                "         qual1,"
                "         sequence2,"
                "         qual2 "
                f"FROM {read_fastx_clause}) "
                f"TO '{intermediate}' ({PARQUET_OPTS_INTERMEDIATE})",
                read_fastx_args,
            )

        # Phase 2: count via Parquet footer (no scan).
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            count = conn.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(intermediate)]
            ).fetchone()[0]

        # Phase 3: mint a sequence_idx range from the CP — unless the
        # work_ticket carries a `pre_minted_range` (E-operator recovery
        # path: the prior attempt already minted in phase 3 and failed
        # transiently in phase 4; the operator resubmits with the
        # existing range so the CP's one-shot mint contract isn't
        # violated). count > 0 by phase-1 pre-check, so no zero-count
        # special case here.
        #
        # The mint helper raises typed Python exceptions; the framework
        # dispatcher in jobs/__init__.py only wraps bare
        # NotImplementedError/FileNotFoundError/ValueError, so we map
        # each mint-side exception to an explicit BackendFailure here.
        # Doing it at the call site (rather than upstream in
        # sequence_range.py) keeps the helper transport-agnostic — the
        # mapping from "how the CP responded" to "what BackendFailure
        # kind the runner should see" is workflow-policy, not protocol.
        if inputs.pre_minted_range is not None:
            # E-operator recovery: skip the HTTP mint entirely. Validate
            # the supplied range covers exactly the FASTQ's read count
            # so a stale recovery (different mint count) fails loudly
            # rather than producing a Parquet whose sequence_idx values
            # would mismatch qiita.sequence_range at registration.
            recovery = inputs.pre_minted_range
            recovered_count = recovery.sequence_idx_stop - recovery.sequence_idx_start + 1
            if recovered_count != count:
                raise BackendFailure(
                    kind=FailureKind.BAD_INPUT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=_YAML_STEP_NAME,
                    reason=(
                        f"pre_minted_range covers {recovered_count} indices "
                        f"({recovery.sequence_idx_start}..{recovery.sequence_idx_stop}) "
                        f"but the FASTQ has {count} reads — the recovery range "
                        f"must match the prior attempt's mint count exactly"
                    ),
                )
            sequence_idx_start = recovery.sequence_idx_start
        else:
            try:
                async with make_cp_client() as http:
                    rng = await mint_sequence_range(
                        http=http, prep_sample_idx=inputs.prep_sample_idx, count=count
                    )
            except SequenceRangeAlreadyExists as exc:
                # Mid-step failure left a range on a previous attempt;
                # this attempt's POST 409s. Operator can either DELETE
                # the prep_sample (CASCADE removes the range and starts
                # over) or — preferred — resubmit with `pre_minted_range`
                # set to the existing row's (start, stop). See
                # docs/runbooks/fastq-to-parquet-retry-recovery.md.
                raise BackendFailure(
                    kind=FailureKind.UNKNOWN_PERMANENT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=_YAML_STEP_NAME,
                    reason=str(exc),
                ) from exc
            except PrepSampleNotEligibleForSequenceRange as exc:
                # The prep_sample doesn't exist or isn't sequenced. The
                # submit route checks processing_kind already, so this
                # only surfaces if the prep_sample was deleted between
                # submission and step execution. BAD_INPUT because the
                # work_ticket's scope_target points at something that
                # no longer exists.
                raise BackendFailure(
                    kind=FailureKind.BAD_INPUT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=_YAML_STEP_NAME,
                    reason=str(exc),
                ) from exc
            except httpx.HTTPStatusError as exc:
                # 401/403: the compute service-account PAT is missing or
                # wrong, or its scope ceiling was lowered. The deploy is
                # misconfigured; retry won't help.
                if exc.response.status_code in (401, 403):
                    raise BackendFailure(
                        kind=FailureKind.CONTRACT_VIOLATION,
                        stage=WorkTicketFailureStage.STEP_RUN,
                        step_name=_YAML_STEP_NAME,
                        reason=(
                            f"CP rejected sequence-range mint with HTTP "
                            f"{exc.response.status_code} — compute SA PAT misconfigured "
                            "(see docs/runbooks/compute-service-account-provisioning.md)"
                        ),
                    ) from exc
                # 5xx and anything else unexpected. Conservatively
                # permanent today; a follow-up could add a retriable
                # CP_UNREACHABLE FailureKind alongside SLURMRESTD_UNREACHABLE
                # so genuine 5xx blips bounce back to QUEUED.
                raise BackendFailure(
                    kind=FailureKind.UNKNOWN_PERMANENT,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=_YAML_STEP_NAME,
                    reason=(
                        f"CP sequence-range mint failed with HTTP "
                        f"{exc.response.status_code}: {exc.response.text!r}"
                    ),
                ) from exc
            sequence_idx_start = rng.sequence_idx_start

        # Phase 4: rewrite intermediate -> final with sequence_idx
        # assigned and physically sorted on disk.
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            # sequence_idx = miint's sequence_index (1-based per file)
            # + start - 1. Deterministic by construction — file order
            # IS the assignment order. The outer ORDER BY controls the
            # physical row order in the final Parquet
            # (preserve_insertion_order=false means the COPY respects
            # only explicit ORDER BY clauses).
            conn.execute(
                "COPY ( SELECT "
                "  sequence_index + ? - 1 AS sequence_idx,"
                "  read_id, sequence1, qual1, sequence2, qual2 "
                "FROM read_parquet(?) "
                "ORDER BY sequence_idx ) "
                f"TO '{out}' ({PARQUET_OPTS})",
                [sequence_idx_start, str(intermediate)],
            )
    finally:
        # Clean up transient artifacts BEFORE returning so the SLURM
        # launcher's manifest walker (which runs after execute()) sees
        # only reads.parquet. Best-effort: a hard-killed process leaves
        # these behind in the failed-attempt workspace, but the runner
        # creates a fresh attempt-N+1 dir on retry so it doesn't cascade.
        intermediate.unlink(missing_ok=True)
        shutil.rmtree(duckdb_tmp, ignore_errors=True)

    return {"reads": out_path}
