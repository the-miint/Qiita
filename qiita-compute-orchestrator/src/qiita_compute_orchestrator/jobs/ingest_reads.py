"""Native job: ingest a bcl-convert pool's per-sample FASTQs into the
DuckLake `read` table, once.

Runs as the read-storage tail of the bcl-convert workflow, after the
`bcl_convert` demux step. For every sample in the pool it parses that
sample's FASTQ(s) ONCE, mints a contiguous `sequence_idx` range from the
control plane, and writes the FULL reads as `read.parquet` keyed by the
minted `sequence_idx`. The read count needed to size the mint comes from
the staged intermediate's row count, not a second FASTQ parse. The reads
are stored ONCE here, independent of any mask; the repeatable read-mask
workflow consumes them and never re-runs this step. This is the
read-storage half of what used to be the single `fastq-to-parquet`
workflow — split out so a new host reference is a new mask over the same
reads, never a re-parse of FASTQ.

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

import asyncio
import os
import shutil
from pathlib import Path

import duckdb
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
    duckdb_headroom_gb,
    duckdb_tmp_dir,
    open_conn,
    open_miint_conn,
    slurm_alloc_gb,
)
from ..sequence_range_retry import mint_or_reuse_sequence_range

# YAML step name this module implements. Hard-coded because execute()
# raises BackendFailures itself (which need a step_name); the integration
# smoke asserts work_ticket.failure_step_name, so a rename here that
# diverges from the `- step: ingest_reads` YAML entry fails loudly.
YAML_STEP_NAME = "ingest_reads"

# Bounded-concurrent pool loop: up to _CONCURRENCY samples are staged / minted /
# written at once (asyncio.gather + a Semaphore; the DuckDB work runs in worker
# threads, which is real parallelism because DuckDB releases the GIL in its C++
# COPY). Each slot gets _DUCKDB_THREADS DuckDB threads. The read_fastx parse is
# serial (~1 core; measured parse CPU-time ≈ wall-time) but the sort phase
# parallelizes ~2x, so 2 threads/slot keeps each sample's turnaround fast — the
# priority is clearing wells quickly — at the cost of underusing the 2nd core
# during the parse. Total cores wanted = _CONCURRENCY * _DUCKDB_THREADS. Per-slot
# memory is divided from the real SLURM cgroup by `_per_slot_caps` (so a
# `--mem-gb` override reaches each slot); `_DUCKDB_MEMORY_GB` is only the
# off-SLURM (test / local) per-slot fallback.
_CONCURRENCY = 4
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2


def _per_slot_caps(concurrency: int) -> tuple[int, int]:
    """Per-slot DuckDB ``(memory_gb, threads)`` for `concurrency` samples in
    flight at once. threads is `_DUCKDB_THREADS` (2 — keep the sort's ~2x
    parallelism so each sample clears fast). Under SLURM the memory is the cgroup
    allocation minus headroom for all ``concurrency * threads`` threads, split
    evenly across slots — so a per-run ``--mem-gb`` override reaches each slot.
    Off SLURM (`slurm_alloc_gb()` is None — tests / local backend) it falls back
    to the single-slot literal."""
    threads = _DUCKDB_THREADS
    alloc = slurm_alloc_gb()
    if alloc is None:
        return _DUCKDB_MEMORY_GB, threads
    usable = alloc - duckdb_headroom_gb(concurrency * threads)
    return max(1, usable // concurrency), threads


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


def _stage_intermediate_reads(
    fastq_path: Path,
    reverse_fastq_path: Path | None,
    intermediate_path: Path,
    duckdb_tmp: Path,
    memory_gb: int,
    threads: int,
) -> int:
    """Parse one sample's FASTQ(s) ONCE into a transient intermediate Parquet at
    `intermediate_path`, keyed by miint's 1-based per-file `sequence_index`, and
    return the read count.

    This is the *single* FASTQ parse of the per-sample pipeline. The count is the
    COPY's row-count return value, NOT a second streaming `read_fastx` pass —
    FASTQ parsing is slow and inherently serial, so the mint that sits between
    this and `_write_sorted_reads` rides on the parse we have to do anyway. Paired
    input is staged in lockstep, one row per pair, so the count is the pair
    count."""
    intermediate = validate_parquet_path(intermediate_path)
    if reverse_fastq_path is not None:
        read_fastx_clause = "read_fastx(?, sequence2:=?)"
        read_fastx_args: list[str] = [str(fastq_path), str(reverse_fastq_path)]
    else:
        read_fastx_clause = "read_fastx(?)"
        read_fastx_args = [str(fastq_path)]
    with open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)
        (count,) = conn.execute(
            "COPY ( SELECT sequence_index, read_id, sequence1, qual1, sequence2, qual2 "
            f"FROM {read_fastx_clause}) TO '{intermediate}' ({PARQUET_OPTS_INTERMEDIATE})",
            read_fastx_args,
        ).fetchone()
    return int(count)


def _write_sorted_reads(
    intermediate_path: Path,
    prep_sample_idx: int,
    sequence_idx_start: int,
    out_path: Path,
    duckdb_tmp: Path,
    memory_gb: int,
    threads: int,
) -> None:
    """Second pass: read the staged intermediate, assign the minted
    `sequence_idx`, and write the durable `read.parquet` at `out_path` sorted by
    `sequence_idx`. No FASTQ re-parse — the heavy parse already happened in
    `_stage_intermediate_reads`. `sequence_idx_start` is the inclusive mint start;
    `sequence_index` is miint's 1-based per-file row index.

    Sorts by `sequence_idx` alone: `prep_sample_idx` is a constant literal for the
    whole sample (cardinality 1), so adding it to the sort key orders nothing —
    the output is identical to sorting by `(prep_sample_idx, sequence_idx)`. The
    explicit ORDER BY is load-bearing: the read happens with
    `preserve_insertion_order=false`, which lets DuckDB write rows out of order,
    so only the sort guarantees `sequence_idx` is ordered at rest (for DuckLake
    pruning / row-group pushdown).

    **Atomic publish.** The sorted COPY lands in a `.partial` sibling, then
    `os.replace`s into `out_path` (atomic on the same filesystem). This is
    load-bearing for idempotency: `out_path` is ALSO the retry sentinel
    (execute() skips a sample whose durable copy exists), so it must only ever
    appear complete — DuckDB `COPY ... TO` is not atomic, and an OOM-kill /
    walltime cut mid-COPY would otherwise leave a truncated `read.parquet` that
    the next attempt skips and registers as the full read set."""
    partial_path = out_path.parent / f"{out_path.name}.partial"
    partial = validate_parquet_path(partial_path)
    try:
        with open_conn() as conn:
            apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)
            conn.execute(
                "COPY ( SELECT "
                "  ?::BIGINT AS prep_sample_idx,"
                "  sequence_index + ? - 1 AS sequence_idx,"
                "  read_id, sequence1, qual1, sequence2, qual2 "
                "FROM read_parquet(?) "
                "ORDER BY sequence_idx ) "
                f"TO '{partial}' ({PARQUET_OPTS})",
                [prep_sample_idx, sequence_idx_start, str(intermediate_path)],
            )
        # Publish atomically: the durable path only ever appears complete.
        os.replace(partial_path, out_path)
    finally:
        # If the COPY died before the replace, drop the half-written partial so
        # a retry re-derives instead of finding stale bytes.
        partial_path.unlink(missing_ok=True)


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Ingest every pool sample's reads, up to `_CONCURRENCY` at once. See the
    module docstring for the per-sample pipeline and idempotency model."""
    if not inputs.convert_dir.is_dir():
        raise FileNotFoundError(f"convert_dir not found: {inputs.convert_dir}")
    roster = _read_sample_map(inputs.sample_map)

    workspace.mkdir(parents=True, exist_ok=True)
    # register-files maps the `read/` subdir's part files -> the `read` table.
    register_dir = workspace / "read"
    register_dir.mkdir(parents=True, exist_ok=True)

    memory_gb, threads = _per_slot_caps(_CONCURRENCY)
    sem = asyncio.Semaphore(_CONCURRENCY)

    async def _process_sample(http: object, prep_sample_idx: int, pool_item_id: str) -> object:
        """Store one sample's reads. Returns ``"registered"``, ``"empty"``, or a
        ``("missing_r1", message)`` tuple; raises (→ dispatcher) on a hard error.
        Each sample is independent of every other (its own FASTQ, atomic mint,
        and output file), so samples run concurrently under the semaphore. The
        DuckDB stage / write run in worker threads (the GIL is released in C++);
        the mint stays on the event loop, where many overlap on the one shared CP
        client."""
        async with sem:
            durable = compute_reads_staging_path(inputs.reads_staging_root, prep_sample_idx)
            part = register_dir / f"{prep_sample_idx}.parquet"

            # Idempotent fast path: reads already stored on a prior attempt.
            # Re-create the register hardlink (the prior workspace is gone) so the
            # retry still registers this sample.
            if durable.exists():
                _hardlink(durable, part)
                return "registered"

            r1 = _match_fastq(inputs.convert_dir, pool_item_id, "R1")
            if r1 is None:
                return (
                    "missing_r1",
                    f"  - pool_item_id {pool_item_id} (prep_sample {prep_sample_idx}): "
                    "no R1 FASTQ matched",
                )
            r2 = _match_fastq(inputs.convert_dir, pool_item_id, "R2")

            # Empty well — expected, not an error. No range minted, no reads.
            if is_empty_sequence_file(r1) or (r2 is not None and is_empty_sequence_file(r2)):
                return "empty"

            durable.parent.mkdir(parents=True, exist_ok=True)
            intermediate = durable.parent / "_intermediate_reads.parquet"
            # Per-sample DuckDB temp dir so concurrent slots never collide on spill.
            sample_tmp = duckdb_tmp / str(prep_sample_idx)
            sample_tmp.mkdir(parents=True, exist_ok=True)
            try:
                # One FASTQ parse: stage the intermediate and take the read count
                # from its COPY row-count return (no second parse), then mint the
                # range and write the sorted durable from the staged intermediate.
                # The intermediate is mint-independent (keyed by the per-file
                # sequence_index), so staging it before the mint is safe and lets
                # the count come for free. DuckDB work runs off the event loop so
                # sibling samples progress while this one parses/sorts.
                count = await asyncio.to_thread(
                    _stage_intermediate_reads, r1, r2, intermediate, sample_tmp, memory_gb, threads
                )
                sequence_idx_start = await mint_or_reuse_sequence_range(
                    http,
                    prep_sample_idx,
                    count,
                    work_ticket_idx=inputs.work_ticket_idx,
                    step_name=YAML_STEP_NAME,
                )
                await asyncio.to_thread(
                    _write_sorted_reads,
                    intermediate,
                    prep_sample_idx,
                    sequence_idx_start,
                    durable,
                    sample_tmp,
                    memory_gb,
                    threads,
                )
            finally:
                intermediate.unlink(missing_ok=True)
            _hardlink(durable, part)
            return "registered"

    with duckdb_tmp_dir(workspace) as duckdb_tmp:
        async with make_cp_client() as http:
            outcomes = await asyncio.gather(
                *(_process_sample(http, psi, pid) for psi, pid in roster),
                return_exceptions=True,
            )

    # Surface the first hard error in roster order — matches the old sequential
    # abort-on-first behavior and preserves the dispatcher's wrapping of bare
    # ValueError / duckdb.Error. Independent samples that already completed have
    # only written their own durable copy, which is harmless.
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            raise outcome

    missing_r1 = [o[1] for o in outcomes if isinstance(o, tuple) and o[0] == "missing_r1"]
    registered = sum(1 for o in outcomes if o == "registered")

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
