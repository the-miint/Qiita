"""Native job: ingest a bcl-convert pool's per-sample FASTQs into the
DuckLake `read` table, once.

Runs as the read-storage tail of the bcl-convert workflow, after the
`bcl_convert` demux step. For every sample in the pool it parses that
sample's FASTQ(s), mints a contiguous `sequence_idx` range from the
control plane, and writes the FULL reads as `read.parquet` keyed by the
minted `sequence_idx`. The reads are stored ONCE here, independent of
any mask; the repeatable read-mask workflow consumes them and never
re-runs this step. This is the read-storage half of what used to be the
single `fastq-to-parquet` workflow — split out so a new host reference
is a new mask over the same reads, never a re-parse of FASTQ.

**Pool-level, not per-sample.** The bcl-convert work-ticket is
sequenced_pool-scoped, so this one step fans over every sample. The
`{prep_sample_idx, pool_item_id}` roster arrives as a runner-staged
`sample_map.parquet`: the orchestrator has no DB access, so the CP
embeds the roster in the bcl-convert ticket's action_context and the
runner materializes it (`_resolve_sample_map`), exactly as it
materializes the QC adapter set.

**Two write targets per sample** (one inode, hardlinked):
  1. `<read_staging_dir>/read/<prep_sample_idx>.parquet` — a part file a
     downstream `register-files` step loads into the DuckLake `read`
     table (its subdir-of-parts -> one-table convention).
  2. `compute_reads_staging_path(reads_staging_root, prep_sample_idx)` —
     `<root>/reads/<prep_sample_idx>/read.parquet`, the durable,
     prep_sample-addressable copy the read-mask workflow binds as
     `reads`. Written first; (1) is `os.link`ed to it, so registration
     and the durable copy share one inode and a register-time unlink of
     the part can't drop the durable read.

**Idempotent / re-runnable.** A sample whose durable `read.parquet`
already exists is skipped (its range was minted and reads written on a
prior attempt), so a bcl-convert ticket retry never re-mints (which
would 409) or re-parses. The hardlink into the register staging dir is
re-created from the existing durable copy so the retry still registers
every sample.

**Transparent retry of a mint-then-crash partial.** If a prior attempt
minted a sample's range but crashed before publishing its durable
`read.parquet` (the classic case: OOM-killed mid-write on an oversized
sample), the durable copy is absent, so the retry does NOT skip — it
re-mints, which 409s. Rather than fail and demand operator recovery,
this step reads the existing range back (`get_sequence_range`), validates
it still covers exactly the FASTQ's read count, and reuses its start.
That makes the runner's OOM memory-escalation effective: the escalated
retry reuses the range and completes, instead of dying on the one-shot
mint contract. The only durable mutation a crashed attempt leaves is the
`sequence_range` row, and reuse consumes it — no orphan, no manual
DELETE.

**Empty wells are first-class.** A sample whose FASTQ has zero records
is skipped — no range minted, no reads written. An empty / no-template /
failed-yield well is expected and numerous on a real plate. A *missing*
required R1 (no file at all) is a broken pool, not an empty well: all
such samples are collected and the step fails BAD_INPUT so nothing is
silently dropped. A pool with no non-empty wells at all raises
StepNoData (the whole ticket is no-data).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb
import httpx
from pydantic import BaseModel
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.duckdb_miint import is_empty_sequence_file
from qiita_common.models import WorkTicketFailureStage
from qiita_common.parquet import validate_parquet_path

from ..cp_client import make_cp_client
from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_INTERMEDIATE,
    apply_duckdb_settings,
    open_conn,
    open_miint_conn,
)
from ..sequence_range import (
    PrepSampleNotEligibleForSequenceRange,
    SequenceRangeAlreadyExists,
    get_sequence_range,
    mint_sequence_range,
)

# YAML step name this module implements. Hard-coded because execute()
# raises BackendFailures itself (which need a step_name); the integration
# smoke asserts work_ticket.failure_step_name, so a rename here that
# diverges from the `- step: ingest_reads` YAML entry fails loudly.
YAML_STEP_NAME = "ingest_reads"

# DuckDB caps mirror fastq_to_parquet's: the per-sample parse is the same
# read_fastx -> intermediate -> sorted COPY pipeline. The pool loop is
# sequential, so one sample's working set at a time.
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2


class Inputs(BaseModel):
    """Typed input contract for ingest_reads.

    `convert_dir` is the `bcl_convert` step's output directory (per-sample
    FASTQs nested under Sample_Project subdirs). `sample_map` is the
    runner-staged Parquet roster `(prep_sample_idx BIGINT, pool_item_id
    VARCHAR)`. `reads_staging_root` is the scratch staging root the durable
    per-sample `read.parquet` copies hang under (via
    `compute_reads_staging_path`). `sequenced_pool_idx` / `sequencing_run_idx`
    / `work_ticket_idx` are the framework-injected scope scalars for the
    sequenced_pool-scoped bcl-convert ticket.
    """

    convert_dir: Path
    sample_map: Path
    reads_staging_root: Path
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def _read_sample_map(path: Path) -> list[tuple[int, str]]:
    """Read the `(prep_sample_idx, pool_item_id)` roster from the staged
    Parquet. Ordered by prep_sample_idx for deterministic processing /
    error reporting. Raises ValueError (BAD_INPUT via the dispatcher) on an
    empty or unreadable roster — a bcl-convert pool always has samples."""
    try:
        with open_conn() as conn:
            rows = conn.execute(
                "SELECT prep_sample_idx, pool_item_id FROM read_parquet(?) "
                "ORDER BY prep_sample_idx",
                [str(path)],
            ).fetchall()
    except duckdb.Error as exc:
        raise ValueError(f"sample_map could not be read: {path}: {exc}") from exc
    if not rows:
        raise ValueError(f"sample_map is empty: {path}")
    return [(int(r[0]), str(r[1])) for r in rows]


def _match_fastq(convert_dir: Path, pool_item_id: str, read_tag: str) -> Path | None:
    """Resolve the single `<pool_item_id>_*_<read_tag>_*.fastq.gz` under
    convert_dir (recursive — bcl-convert nests per-sample FASTQs under a
    Sample_Project subdir). The trailing `_` anchors the prefix so `12`
    never matches `120_...`. Returns the Path on a unique match, None when
    none match. >1 match (lane-split) raises ValueError — the per-sample
    pipeline takes a single fastq_path.

    INVARIANT: `pool_item_id` == the bcl-convert FASTQ basename prefix. It
    is now produced two files away — submit-bcl-convert sets
    `sequenced_pool_item_id = str(illumina_sample_idx)` and embeds it in the
    `sample_map`, and bcl-convert emits Sample_ID as that same
    illumina_sample_idx — so a future preflight that changes Sample_ID would
    silently break this match."""
    matches = sorted(convert_dir.rglob(f"{pool_item_id}_*_{read_tag}_*.fastq.gz"))
    if len(matches) == 1:
        return matches[0]
    if not matches:
        return None
    raise ValueError(
        f"{len(matches)} {read_tag} FASTQs matched {pool_item_id} (lane-split runs "
        f"are not supported): {', '.join(m.name for m in matches)}"
    )


def _http_status_failure(prep_sample_idx: int, exc: httpx.HTTPStatusError) -> BackendFailure:
    """Map an httpx status error from a CP sequence-range call (mint or
    read-back) to a BackendFailure: 401/403 → CONTRACT_VIOLATION (bad
    token / missing scope — a deploy misconfig), anything else →
    UNKNOWN_PERMANENT. Shared by the mint and the reuse-GET arms so both
    classify identically."""
    kind = (
        FailureKind.CONTRACT_VIOLATION
        if exc.response.status_code in (401, 403)
        else FailureKind.UNKNOWN_PERMANENT
    )
    return BackendFailure(
        kind=kind,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name=YAML_STEP_NAME,
        reason=(
            f"CP sequence-range call for prep_sample {prep_sample_idx} failed "
            f"with HTTP {exc.response.status_code}"
        ),
    )


async def _mint_or_reuse_range(http: httpx.AsyncClient, prep_sample_idx: int, count: int) -> int:
    """Mint a sequence range for one sample, or reuse the existing one.

    The caller invokes this only when the sample's durable read.parquet is
    ABSENT, so a 409 means a prior attempt minted the range then crashed
    before the durable write (typically OOM-killed mid-write). Instead of
    failing and demanding operator recovery, read the existing range back
    and reuse its start — so an OOM-escalated retry completes transparently
    rather than dying on the one-shot mint contract. Returns the inclusive
    range start. Maps the typed mint exceptions to BackendFailures (the
    dispatcher only wraps bare NotImplementedError/FileNotFoundError/
    ValueError)."""
    try:
        rng = await mint_sequence_range(http=http, prep_sample_idx=prep_sample_idx, count=count)
        return rng.sequence_idx_start
    except SequenceRangeAlreadyExists as exc:
        # Reuse the range a prior crashed attempt left. The GET is gated on
        # the same `sequence_range:mint` scope the SA already holds.
        try:
            existing = await get_sequence_range(http=http, prep_sample_idx=prep_sample_idx)
        except httpx.HTTPStatusError as get_exc:
            raise _http_status_failure(prep_sample_idx, get_exc) from get_exc
        if existing is None:
            # 409 on mint but 404 on read-back: the range vanished between the
            # two calls (an operator deleted the prep_sample / range mid-retry).
            # A fresh resubmit will re-mint cleanly, but THIS attempt can't run
            # against a moving target.
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=YAML_STEP_NAME,
                reason=(
                    f"prep_sample {prep_sample_idx} sequence_range 409'd on mint but "
                    "404'd on read-back — concurrent deletion during retry; resubmit"
                ),
            ) from exc
        recovered_count = existing.sequence_idx_stop - existing.sequence_idx_start + 1
        if recovered_count != count:
            # The existing range was minted against a different read count than
            # this attempt's FASTQ — reusing it would write sequence_idx values
            # that mismatch qiita.sequence_range at registration. Deterministic
            # demux makes this unreachable in practice; fail loudly if it isn't.
            raise BackendFailure(
                kind=FailureKind.BAD_INPUT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=YAML_STEP_NAME,
                reason=(
                    f"prep_sample {prep_sample_idx} has an existing sequence_range covering "
                    f"{recovered_count} indices "
                    f"({existing.sequence_idx_start}..{existing.sequence_idx_stop}) but its "
                    f"FASTQ now has {count} reads — the range must match the prior mint "
                    "count exactly; delete the prep_sample to re-mint"
                ),
            ) from exc
        return existing.sequence_idx_start
    except PrepSampleNotEligibleForSequenceRange as exc:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=YAML_STEP_NAME,
            reason=str(exc),
        ) from exc
    except httpx.HTTPStatusError as exc:
        raise _http_status_failure(prep_sample_idx, exc) from exc


def _write_sample_reads(
    fastq_path: Path,
    reverse_fastq_path: Path | None,
    prep_sample_idx: int,
    sequence_idx_start: int,
    out_path: Path,
    duckdb_tmp: Path,
) -> None:
    """Parse one sample's FASTQ(s) into the durable `read.parquet` at
    `out_path`, sorted by `(prep_sample_idx, sequence_idx)`. Same B-staged
    pipeline as the retired fastq_to_parquet: read_fastx -> intermediate ->
    sequence_idx-assigned sorted COPY. `sequence_idx_start` is the inclusive
    mint start; `sequence_index` is miint's 1-based per-file row index.

    **Atomic publish.** The final sorted COPY lands in a `.partial` sibling,
    then `os.replace`s into `out_path` (atomic on the same filesystem). This is
    load-bearing for idempotency: `out_path` is ALSO the retry sentinel
    (execute() skips a sample whose durable copy exists), so it must only ever
    appear complete — DuckDB `COPY ... TO` is not atomic, and an OOM-kill /
    walltime cut mid-COPY would otherwise leave a truncated `read.parquet` that
    the next attempt skips and registers as the full read set."""
    intermediate = out_path.parent / "_intermediate_reads.parquet"
    partial_path = out_path.parent / f"{out_path.name}.partial"
    partial = validate_parquet_path(partial_path)
    if reverse_fastq_path is not None:
        read_fastx_clause = "read_fastx(?, sequence2:=?)"
        read_fastx_args: list[str] = [str(fastq_path), str(reverse_fastq_path)]
    else:
        read_fastx_clause = "read_fastx(?)"
        read_fastx_args = [str(fastq_path)]
    try:
        with open_miint_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )
            conn.execute(
                "COPY ( SELECT sequence_index, read_id, sequence1, qual1, sequence2, qual2 "
                f"FROM {read_fastx_clause}) TO '{intermediate}' ({PARQUET_OPTS_INTERMEDIATE})",
                read_fastx_args,
            )
        with open_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )
            conn.execute(
                "COPY ( SELECT "
                "  ?::BIGINT AS prep_sample_idx,"
                "  sequence_index + ? - 1 AS sequence_idx,"
                "  read_id, sequence1, qual1, sequence2, qual2 "
                "FROM read_parquet(?) "
                "ORDER BY prep_sample_idx, sequence_idx ) "
                f"TO '{partial}' ({PARQUET_OPTS})",
                [prep_sample_idx, sequence_idx_start, str(intermediate)],
            )
        # Publish atomically: the durable path only ever appears complete.
        os.replace(partial_path, out_path)
    finally:
        intermediate.unlink(missing_ok=True)
        # If the COPY died before the replace, drop the half-written partial so
        # a retry re-derives instead of finding stale bytes.
        partial_path.unlink(missing_ok=True)


def _count_reads(fastq_path: Path, reverse_fastq_path: Path | None, duckdb_tmp: Path) -> int:
    """Count records via a streaming read_fastx pass (footer count needs the
    intermediate; this is one extra pass but keeps `_write_sample_reads`
    free of the count so the mint happens between them). Paired input is
    counted in lockstep, one row per pair."""
    if reverse_fastq_path is not None:
        clause, params = "read_fastx(?, sequence2:=?)", [str(fastq_path), str(reverse_fastq_path)]
    else:
        clause, params = "read_fastx(?)", [str(fastq_path)]
    with open_miint_conn() as conn:
        apply_duckdb_settings(
            conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
        )
        return conn.execute(f"SELECT count(*) FROM {clause}", params).fetchone()[0]


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Ingest every pool sample's reads. See module docstring for the
    pipeline and idempotency model."""
    if not inputs.convert_dir.is_dir():
        raise FileNotFoundError(f"convert_dir not found: {inputs.convert_dir}")
    roster = _read_sample_map(inputs.sample_map)

    workspace.mkdir(parents=True, exist_ok=True)
    # register-files maps the `read/` subdir's part files -> the `read` table.
    register_dir = workspace / "read"
    register_dir.mkdir(parents=True, exist_ok=True)
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    missing_r1: list[str] = []
    registered = 0
    try:
        async with make_cp_client() as http:
            for prep_sample_idx, pool_item_id in roster:
                durable = compute_reads_staging_path(inputs.reads_staging_root, prep_sample_idx)
                part = register_dir / f"{prep_sample_idx}.parquet"

                # Idempotent fast path: reads already stored on a prior attempt.
                # Re-create the register hardlink (the prior workspace is gone)
                # so the retry still registers this sample.
                if durable.exists():
                    _hardlink(durable, part)
                    registered += 1
                    continue

                r1 = _match_fastq(inputs.convert_dir, pool_item_id, "R1")
                if r1 is None:
                    missing_r1.append(
                        f"  - pool_item_id {pool_item_id} (prep_sample {prep_sample_idx}): "
                        "no R1 FASTQ matched"
                    )
                    continue
                r2 = _match_fastq(inputs.convert_dir, pool_item_id, "R2")

                # Empty well — expected, not an error. No range minted, no reads.
                if is_empty_sequence_file(r1) or (r2 is not None and is_empty_sequence_file(r2)):
                    continue

                count = _count_reads(r1, r2, duckdb_tmp)
                sequence_idx_start = await _mint_or_reuse_range(http, prep_sample_idx, count)
                durable.parent.mkdir(parents=True, exist_ok=True)
                _write_sample_reads(
                    r1, r2, prep_sample_idx, sequence_idx_start, durable, duckdb_tmp
                )
                _hardlink(durable, part)
                registered += 1
    finally:
        shutil.rmtree(duckdb_tmp, ignore_errors=True)

    if missing_r1:
        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=YAML_STEP_NAME,
            reason=(
                "could not resolve a required R1 FASTQ for every pool sample; "
                "no reads registered:\n" + "\n".join(missing_r1)
            ),
        )
    if registered == 0:
        # Every well was empty — the whole pool is no-data. Terminal NO_DATA,
        # not a failure; register-files would otherwise abort on an empty dir.
        raise StepNoData(
            step_name=YAML_STEP_NAME,
            reason=f"sequenced_pool {inputs.sequenced_pool_idx} has no non-empty wells",
        )

    # `read_staging_dir` is the workspace: register-files finds the `read/`
    # subdir of per-sample parts and loads them all into the `read` table.
    return {"read_staging_dir": workspace}


def _hardlink(src: Path, dst: Path) -> None:
    """Hardlink `src` -> `dst` (same scratch filesystem), replacing an
    existing dst. Falls back to a copy across filesystems (defensive — the
    durable copy and the workspace are both under PATH_SCRATCH)."""
    dst.unlink(missing_ok=True)
    try:
        os.link(src, dst)
    except OSError:
        shutil.copyfile(src, dst)
