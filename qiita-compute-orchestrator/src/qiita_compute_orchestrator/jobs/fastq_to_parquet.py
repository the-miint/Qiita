"""Native job: convert a FASTQ (or FASTA) file to a Parquet of reads,
keyed by a CP-minted `sequence_idx` BIGINT.

Reads via DuckDB + miint's `read_fastx` table function, mints a
contiguous bigint range from the control plane (`POST /sequence-range`
via the sibling client in ../sequence_range.py), and writes a Parquet
with one row per input read. No deduplication — every read becomes
one row, including duplicate sequences.

Schema (sorted by sequence_idx, the lake-friendly join key):

    sequence_idx      BIGINT      NOT NULL  -- CP-minted, contiguous within this sample
    read_id           VARCHAR     NOT NULL  -- FASTQ/A record id (label, no longer the join key)
    sequence          VARCHAR     NOT NULL  -- aliased from miint's sequence1
    quality           UTINYINT[]            -- from qual1; phred-decoded; NULL for FASTA
    sequence_length   BIGINT      NOT NULL  -- length(sequence)

Pipeline (B-staged-Parquet):

  Phase 1: FASTQ -> intermediate Parquet (no sequence_idx yet).
           One streaming pass through miint's read_fastx; the
           intermediate Parquet is zstd-compressed so disk peak stays
           small. Empty-file branch substitutes a header-only schema
           so consumers see the same schema regardless of input size.

  Phase 2: Count via Parquet footer (sub-second; no data scan).

  Phase 3: POST /api/v1/sequence-range with the exact count. The CP
           function holds an advisory lock for the nextval/setval/INSERT
           critical section and returns the minted (start, stop) range.
           Skipped when count = 0 (the CP rejects count <= 0; empty
           samples just write an empty final Parquet with the full
           schema and no minted range).

  Phase 4: Read intermediate + assign sequence_idx via
           `start + row_number() OVER (ORDER BY read_id) - 1`, write
           the final Parquet sorted by sequence_idx (which is
           monotonic in read_id by construction).

  Phase 5 (try/finally): cleanup intermediate + DuckDB temp_directory
           before returning. The SLURM launcher's manifest walker runs
           AFTER execute() returns, so the transient files are
           invisible to it — the manifest sees only reads.parquet.

DuckDB settings applied on every connection:

  - `memory_limit='{N}GB'`   : cap RAM so SLURM cgroups don't OOM-kill.
  - `threads={N}`            : match the cgroup cpu allocation; defaults
                               try to use all host cores.
  - `preserve_insertion_order=false` : let DuckDB parallelize freely.
                               Determinism is guaranteed by the explicit
                               ORDER BY read_id in phase 4 (both as the
                               window-function ordering and the COPY's
                               output ordering).
  - `temp_directory='{workspace}/.duckdb_tmp'` : spill on the same fast
                               scratch as the workspace, not the system
                               /tmp (which is often small tmpfs).

The `memory_limit` and `threads` values are conservative hardcodes in
this commit; #38 tracks plumbing them from JobParams.baseline_resources
so each step's SLURM allocation drives DuckDB's own limits.

Mint-side failure mapping. The mint helper raises typed Python
exceptions; the native-step dispatcher (jobs/__init__.py) only wraps
bare NotImplementedError/FileNotFoundError/ValueError, so this module
maps each mint exception to an explicit BackendFailure at the call
site in phase 3:

  - SequenceRangeAlreadyExists (409) → UNKNOWN_PERMANENT.
    The prep_sample's range was minted on a previous attempt that
    failed before phase 4. Operator recovery: DELETE the prep_sample
    (CASCADE removes the range) and resubmit the work_ticket. See
    sequence_range.py module docstring for the GET-on-mint-scope
    follow-up that would make retries transparent.
  - PrepSampleNotEligibleForSequenceRange (404) → BAD_INPUT.
    The prep_sample was deleted between submission and step execution.
  - httpx.HTTPStatusError 401/403 → CONTRACT_VIOLATION.
    The compute service-account PAT is missing/wrong; deploy issue,
    retry won't help. (see compute-service-account-provisioning.md)
  - httpx.HTTPStatusError 5xx and other → UNKNOWN_PERMANENT.
    Conservative today; a follow-up could add a retriable
    CP_UNREACHABLE FailureKind alongside SLURMRESTD_UNREACHABLE so
    genuine 5xx blips bounce back to QUEUED.

Sibling: `LocalBackend._run_hash` in `backends/local.py` is
structurally similar — same DuckDB+miint plumbing, same `PARQUET_OPTS`,
same use of `read_fastx` — but is a *reference-side dedup* job, not a
per-sample ingest. _run_hash rejects duplicate read_ids and writes a
manifest sorted by content hash; this job keeps every read and writes
raw reads sorted by the CP-minted sequence_idx. They share mechanics,
not semantics.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import httpx
from pydantic import BaseModel
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage
from qiita_common.parquet import validate_parquet_path

from ..miint import PARQUET_OPTS, ensure_miint_installed, open_conn
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
# DuckDB's own limits. For now these match the YAML's declared
# baseline_resources (cpu=2, mem_gb=4) with headroom for Python+miint.
_DUCKDB_MAX_MEMORY_GB = 2
_DUCKDB_MAX_THREADS = 2


def _apply_duckdb_settings(conn: duckdb.DuckDBPyConnection, duckdb_tmp: Path) -> None:
    """Apply the four standard DuckDB knobs the other dev recommended.
    Called at the top of every connection in the pipeline. The
    canonical setting names are `memory_limit` and `threads` —
    `max_memory` / `max_threads` are aliases in newer DuckDB versions
    but not the ones miint targets, so use the canonical forms."""
    conn.execute(f"SET memory_limit='{_DUCKDB_MAX_MEMORY_GB}GB'")
    conn.execute(f"SET threads={_DUCKDB_MAX_THREADS}")
    conn.execute("SET preserve_insertion_order=false")
    conn.execute(f"SET temp_directory='{duckdb_tmp}'")


class Inputs(BaseModel):
    """Typed input contract for fastq_to_parquet.

    `fastq_path` is the workflow-declared input (the action_context's
    fastq_path flows through here). `prep_sample_idx` and
    `work_ticket_idx` are framework-injected scope scalars merged by
    `flatten_native_inputs`; `prep_sample_idx` is also the key the
    CP's sequence-range allocator uses, so it's load-bearing here
    (not just provenance as the comment used to imply).
    """

    fastq_path: Path
    prep_sample_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """B-staged-Parquet pipeline. See module docstring for the
    full pipeline description."""
    if not inputs.fastq_path.exists():
        raise FileNotFoundError(f"FASTQ file not found: {inputs.fastq_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    intermediate = workspace / "_intermediate_reads.parquet"
    out_path = workspace / "reads.parquet"
    out = validate_parquet_path(out_path)
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    await ensure_miint_installed()

    try:
        # Phase 1: FASTQ -> intermediate Parquet.
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            conn.execute("LOAD miint;")
            try:
                conn.execute(
                    "COPY ( SELECT read_id,"
                    "         sequence1 AS sequence,"
                    "         qual1 AS quality,"
                    "         CAST(length(sequence1) AS BIGINT) AS sequence_length "
                    "FROM read_fastx(?)) "
                    f"TO '{intermediate}' ({PARQUET_OPTS})",
                    [str(inputs.fastq_path)],
                )
            except duckdb.Error as exc:
                if "Empty file" not in str(exc):
                    raise
                # miint refuses zero-byte input. Synthesize an empty
                # intermediate Parquet with the right schema so phases
                # 2+4 stay schema-uniform.
                conn.execute(
                    "CREATE TEMP TABLE _empty ("
                    "  read_id VARCHAR, sequence VARCHAR,"
                    "  quality UTINYINT[], sequence_length BIGINT"
                    ")"
                )
                conn.execute(f"COPY (SELECT * FROM _empty) TO '{intermediate}' ({PARQUET_OPTS})")

        # Phase 2: count via Parquet footer (no scan).
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            count = conn.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(intermediate)]
            ).fetchone()[0]

        # Phase 3: mint a sequence_idx range from the CP (skipped for
        # empty samples — the CP rejects count <= 0, and an empty file
        # has no reads to key).
        #
        # The mint helper raises typed Python exceptions; the framework
        # dispatcher in jobs/__init__.py only wraps bare
        # NotImplementedError/FileNotFoundError/ValueError, so we map
        # each mint-side exception to an explicit BackendFailure here.
        # Doing it at the call site (rather than upstream in
        # sequence_range.py) keeps the helper transport-agnostic — the
        # mapping from "how the CP responded" to "what BackendFailure
        # kind the runner should see" is workflow-policy, not protocol.
        if count > 0:
            try:
                async with make_cp_client() as http:
                    rng = await mint_sequence_range(
                        http=http, prep_sample_idx=inputs.prep_sample_idx, count=count
                    )
            except SequenceRangeAlreadyExists as exc:
                # Mid-step failure left a range on a previous attempt;
                # this attempt's POST 409s. Operator must DELETE the
                # prep_sample (CASCADE removes the range) and resubmit.
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
        else:
            # Sentinel — phase 4 won't reference it for an empty input
            # because the SELECT produces zero rows.
            sequence_idx_start = 0

        # Phase 4: rewrite intermediate -> final with sequence_idx
        # assigned and physically sorted on disk.
        with open_conn() as conn:
            _apply_duckdb_settings(conn, duckdb_tmp)
            # ORDER BY read_id inside the window AND on the outer SELECT:
            # the window-function ordering controls sequence_idx assignment
            # (deterministic across runs of the same input); the outer
            # ORDER BY controls the physical row order in the final
            # Parquet (preserve_insertion_order=false means the COPY
            # respects only explicit ORDER BY clauses).
            conn.execute(
                "COPY ( SELECT "
                "  ? + row_number() OVER (ORDER BY read_id) - 1 AS sequence_idx,"
                "  read_id, sequence, quality, sequence_length "
                "FROM read_parquet(?) "
                "ORDER BY read_id ) "
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
