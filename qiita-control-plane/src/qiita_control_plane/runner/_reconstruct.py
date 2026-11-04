"""Runner restart-recovery output reconstruction and action-primitive dispatch."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import asyncpg
import pyarrow
import pyarrow.flight
from qiita_common.actions import (
    ALIGNMENT_IDX_BINDING,
    PROCESSING_IDX_BINDING,
    WorkflowAction,
    WorkflowStep,
)
from qiita_common.api_paths import (
    LibraryPrimitive,
)
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.data_plane_errors import (
    is_data_plane_transient,
)
from qiita_common.models import (
    TERMINAL_STEP_PROGRESS_STATES,
    ComputeTarget,
    ScopeTargetKind,
    StepHandleWire,
    StepProgressState,
    StepStatus,
    StepStatusWire,
    WorkTicketFailureStage,
)

from .. import step_progress
from ..actions.library import (
    LIBRARY,
    MINT_ANNOTATION_MAP_OUTPUT_BASENAME,
    MINT_FEATURES_OUTPUT_BASENAME,
    QcProducer,
)
from ..repositories.assembly import AssemblySampleInvalidated
from ..repositories.reference_membership import count_reference_shards
from ..shard_orchestration import (
    BUILD_SHARD_INDEX_ACTION_ID,
    BUILD_SHARD_INDEX_ACTION_VERSION,
    SHARD_BUILD_CONTEXT_KEYS,
    expected_shard_index_types,
    plan_and_submit_shards,
)
from ..workspace import (
    action_payload_record,
    step_attempt_dir,
    step_logs_dir,
    step_output_dir,
)
from ._alignment import _workflow_writes_alignment_gate
from ._context import RunContext
from ._dispatch import _best_effort_record_failed, _result_with_infra_retry
from ._mask import MASK_IDX_BINDING, _workflow_writes_mask_gate
from ._processing import _workflow_writes_assembly_gate
from ._read_ingest import (
    ROUTER_PENDING_BINDING,
    SHARD_MAPPING_BINDING,
    _stage_shard_mapping,
)

# =============================================================================
# Restart-recovery output reconstruction
# =============================================================================
#
# On resume, an entry already marked COMPLETED in a prior run must NOT be
# re-run (an in-process action: is not idempotent) — its outputs are rebuilt
# from the shared workspace instead, then bound forward exactly as a fresh run
# would. The per-attempt workspace layout (`<workspace>/<name>/attempt-<N>/`)
# is deterministic, so the producer's attempt number — read from the progress
# row — is enough to find every output on disk.


def _completed_progress_row(
    progress: list[step_progress.StepProgressRow], step_index: int
) -> step_progress.StepProgressRow | None:
    """The COMPLETED row for `step_index` across any attempt, or None. A step
    that failed attempt 0 but completed attempt 1 counts as completed."""
    for row in progress:
        if row.step_index == step_index and row.state is StepProgressState.COMPLETED:
            return row
    return None


def _progress_row_for(
    prior_progress: list[step_progress.StepProgressRow], *, step_index: int, attempt: int
) -> step_progress.StepProgressRow | None:
    """The start-of-run progress row for this exact `(step_index, attempt)`, or
    None. Single home for the key lookup the attempt predicates below share."""
    for row in prior_progress:
        if row.step_index == step_index and row.attempt == attempt:
            return row
    return None


def _attempt_is_unowned(
    prior_progress: list[step_progress.StepProgressRow], *, step_index: int, attempt: int
) -> bool:
    """Whether this entry's `(step_index, attempt)` is unowned by a start-of-run
    progress row — i.e. the caller may treat any attempt dir on disk as orphaned.

    Keyed on the START-OF-RUN progress (the snapshot loaded once before the
    loop). A pre-existing row for this exact `(step_index, attempt)` means a
    prior process owns the dir, so it is NOT unowned (return False; leave the dir
    alone). No such row means the attempt is unowned: either a first dispatch
    (dir absent — the caller just mkdirs it) or a re-run whose row was
    deliberately dropped (a `/run` redrive clearing dead rows, a download-ticket
    redrive clearing completed ones, or `update-lane`
    invalidating a completed prep row). In the re-run case the prior attempt left
    stale, read-only (0o440) output + manifest on disk that must not be reused;
    the caller advances to a fresh attempt dir rather than deleting it (the
    output is owned by the SLURM job user — the control plane can't unlink or
    chmod it).

    Ownership here is only about the DIR. It says nothing about whether the
    owning row is still resumable: a terminal row owns its dir but must not be
    adopted. `_attempt_is_terminal` is the predicate for that, and the caller
    checks it first."""
    return _progress_row_for(prior_progress, step_index=step_index, attempt=attempt) is None


def _attempt_is_terminal(
    prior_progress: list[step_progress.StepProgressRow], *, step_index: int, attempt: int
) -> bool:
    """Whether this entry's `(step_index, attempt)` already reached a terminal
    progress state in a prior run — i.e. is history rather than something to
    resume into.

    The invariant: **only a LIVE attempt is adoptable.** A terminal attempt's job
    has already ended, so re-attaching to its persisted `slurm_job_id` reaches a
    job slurmrestd will eventually purge, and the poll loop's filesystem
    tiebreaker then decides the step from a workspace whose manifest is missing
    precisely because that attempt failed. The caller must advance to the next
    attempt number instead.

    `COMPLETED` is included even though the step-level fast-forward normally
    consumes a completed entry before the attempt loop is reached: the set is the
    shared `TERMINAL_STEP_PROGRESS_STATES`, and narrowing it here would encode
    "unreachable today" as a correctness assumption of this predicate.

    Distinct from `_attempt_is_unowned` (does a row exist at all — a dir
    question); this asks whether the row that exists is still live."""
    row = _progress_row_for(prior_progress, step_index=step_index, attempt=attempt)
    return row is not None and row.state in TERMINAL_STEP_PROGRESS_STATES


async def _reconstruct_completed_outputs(
    ctx: RunContext,
    entry: WorkflowStep | WorkflowAction,
    completed: step_progress.StepProgressRow,
) -> dict[str, Any]:
    """Rebuild the bound outputs of an already-COMPLETED entry from disk,
    without re-running it.

    A `step:` entry re-reads its output manifest through `result_step`
    (reconstructing a handle from the progress row's job id + the deterministic
    per-attempt `output`/`logs` dirs), marked `recovered` (see
    `verify_container_output`). This doubles as the filesystem tiebreaker for a now-purged job:
    a valid manifest yields the outputs; a missing / broken one, or a declared
    output that is gone, raises CONTRACT_VIOLATION → the resumed workflow fails,
    as it should when a completed step's output has vanished from scratch.

    An `action:` entry rebuilds its deterministic output paths in-process (see
    `_reconstruct_action_outputs`) — the in-process primitive must not re-run.
    `plan-shards` is the one action whose control-flow bindings (`router_pending`
    + the staged `shard_mapping`) are re-derived from the DURABLE shard
    assignment rather than a scratch path, so the router build stays gated and
    fed correctly across a CP restart (see `_reconstruct_plan_shards_outputs`).

    A non-SLURM (local) completed step has no on-disk manifest to re-read;
    recovery is a SLURM-backend concern (local steps are synchronous and don't
    survive a restart mid-flight), so this returns its outputs empty — a
    downstream consumer that needs a missing binding fails loudly via KeyError."""
    attempt_workspace = step_attempt_dir(ctx.workspace, entry.name, completed.attempt)
    if isinstance(entry, WorkflowAction):
        if entry.name == LibraryPrimitive.PLAN_SHARDS:
            return await _reconstruct_plan_shards_outputs(
                ctx.pool, ctx.scope_target, attempt_workspace
            )
        return _reconstruct_action_outputs(entry, attempt_workspace)
    if completed.compute_target is not ComputeTarget.SLURM:
        return {}
    handle = StepHandleWire(
        compute_target=ComputeTarget.SLURM,
        step_name=entry.name,
        slurm_job_id=completed.slurm_job_id,
        job_name=completed.job_name,
        output_path=str(step_output_dir(attempt_workspace)),
        logs_path=str(step_logs_dir(attempt_workspace)),
    )
    status = StepStatusWire(status=StepStatus.COMPLETED, recovered=True)
    raw_outputs = await _result_with_infra_retry(
        ctx.backend_client,
        handle,
        status,
        pool=ctx.pool,
        work_ticket_idx=ctx.work_ticket_idx,
        poll_interval_seconds=ctx.poll_interval_seconds,
    )
    return {name: Path(raw_outputs[name]) for name in entry.outputs}


def _reconstruct_action_outputs(entry: WorkflowAction, attempt_workspace: Path) -> dict[str, Any]:
    """Deterministic output paths an `action:` primitive wrote, for resume.
    Only the two minting primitives contribute a binding (the feature-map Parquet
    each wrote into its workspace); the other primitives produce no bound output.
    (`plan-shards` is handled separately in `_reconstruct_completed_outputs` —
    its bindings come from the DB, not a scratch path.)
    Each basename is single-sourced from the primitive itself
    (`MINT_FEATURES_OUTPUT_BASENAME` / `MINT_ANNOTATION_MAP_OUTPUT_BASENAME`) so this
    resume path can't drift from where the primitive actually writes the file. The two
    basenames differ precisely because both primitives can run in one workflow, and a
    shared name would resume the second onto the first's map.

    Note `mint-annotation-features` writes TWO Parquets and binds only one: the map
    keyed by the annotation's natural key is the OUTPUT, while the sequence_hash →
    feature_idx map it mints through (`MINT_ANNOTATION_FEATURES_OUTPUT_BASENAME`) is an
    intermediate that nothing downstream reads. Resuming onto the intermediate would
    hand `reference_load` a file with an entirely different schema, so the two must not
    be confused here."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        return {entry.outputs[0]: attempt_workspace / MINT_FEATURES_OUTPUT_BASENAME}
    if entry.name == LibraryPrimitive.MINT_ANNOTATION_FEATURES:
        return {entry.outputs[0]: attempt_workspace / MINT_ANNOTATION_MAP_OUTPUT_BASENAME}
    return {}


async def _reconstruct_plan_shards_outputs(
    pool: asyncpg.Pool,
    scope_target: dict[str, Any],
    attempt_workspace: Path,
) -> dict[str, Any]:
    """Re-derive the plan-shards arm's control-flow bindings on resume.

    plan-shards produces the router gate `router_pending` and — when it fanned
    out (N > 0) — the staged `shard_mapping` Parquet the router build consumes.
    Neither is a declared step output, so rebuild them from the DURABLE shard
    assignment (`reference_membership.shard_id`) rather than trusting a scratch
    file to survive: `router_pending = (N > 0)`, and when N > 0 re-stage
    `shard_mapping` to the same deterministic path the live arm used. This keeps
    the router build resume-robust — a CP restart between plan-shards and
    build_routing_index re-provides the mapping and re-opens the gate. Mirrors
    the live plan-shards arm's return; keep the two in step."""
    reference_idx = scope_target["reference_idx"]
    n = await count_reference_shards(pool, reference_idx)
    if not n:
        return {ROUTER_PENDING_BINDING: False}
    mapping_path = await _stage_shard_mapping(
        pool, reference_idx, attempt_workspace / "shard_mapping.parquet"
    )
    return {ROUTER_PENDING_BINDING: True, SHARD_MAPPING_BINDING: mapping_path}


def _action_failure_is_retriable(exc: BaseException) -> bool:
    """True if an `action:` entry's failure is a data-plane one the next attempt can
    clear, so the retry loop re-runs the entry.

    Gated on the pyarrow types, as the orchestrator's dispatcher is, so a primitive's
    own Postgres error cannot match by text. `register-files` is re-run on the same
    failures as every other entry: the re-run resends the payload the first call
    recorded (`_register_files_payload`), so a connection that dropped mid-move
    neither shrinks the payload nor registers a file twice."""
    if not isinstance(exc, (pyarrow.ArrowException, pyarrow.flight.FlightError)):
        return False
    return is_data_plane_transient(exc)


def _staged_register_files(staging_dir: Path) -> dict[str, str]:
    """Filename → DuckLake table mapping derived from the staging dir.

    Convention:
      - Top-level `<table>.parquet` files register as the table named after the
        file's stem (single-file table).
      - Top-level subdirs containing `*.parquet` files register each part as the
        table named after the directory (multi-file table). The filename carries
        the subdir prefix relative to staging_dir; the data plane normalises to
        basename when placing each part in the permanent per-table directory.

    The multi-file form exists for `reference_sequence_chunks` — at GG2 scale a
    single-file sort+write of ~30 GB of chunk_data OOMs DuckDB, so reference_load
    writes it as batched part files instead."""
    files: dict[str, str] = {}
    for entry_path in sorted(staging_dir.iterdir()):
        if entry_path.is_file() and entry_path.suffix == ".parquet":
            files[entry_path.name] = entry_path.stem
        elif entry_path.is_dir():
            for part in sorted(entry_path.glob("*.parquet")):
                rel = part.relative_to(staging_dir).as_posix()
                files[rel] = entry_path.name
    return files


def _register_files_payload(attempt_dir: Path, staging_dir: Path) -> dict[str, str]:
    """The `files` payload for a register-files call on `staging_dir`.

    The data plane moves each file out of staging before its catalog commit, so
    a replay — the control plane lost the reply, or died before recording the
    step complete — finds the directory partly or wholly empty. The payload is
    therefore recorded before the first call, where every attempt of the entry
    finds it, and resent on a replay whatever the directory still holds; the data plane answers a
    resent payload from where it moved the files (`register_files` in the data
    plane's flight_service/registration.rs). A staged file the record does not name was not
    part of the earlier call, so the replay refuses it rather than registering
    a payload neither call sent."""
    staged = _staged_register_files(staging_dir)
    record = action_payload_record(attempt_dir, str(staging_dir))
    if record.exists():
        files: dict[str, str] = json.loads(record.read_text())["files"]
        unsent = {name: table for name, table in staged.items() if files.get(name) != table}
        if unsent:
            raise RuntimeError(
                f"register-files: staging_dir {staging_dir} holds {sorted(unsent)}, which the "
                f"earlier call recorded in {record} did not send"
            )
        return files
    if not staged:
        # A call made before the control plane recorded its payload leaves no
        # record, and may still have moved the files and committed.
        raise RuntimeError(
            f"register-files: staging_dir {staging_dir} contains no Parquet files, and no "
            "earlier call of this entry recorded a payload for it; if an earlier call "
            "reached the data plane, it moved them into the lake and its registration "
            "may have committed — check whether the files are registered "
            "(`make lake-shell`) before re-running"
        )
    record.parent.mkdir(parents=True, exist_ok=True)
    partial = record.with_suffix(".partial")
    # `staging_dir` is not read back (the file's name is its hash); it names the
    # directory for whoever opens the record by hand.
    partial.write_text(json.dumps({"staging_dir": str(staging_dir), "files": staged}))
    partial.replace(record)
    return staged


async def _dispatch_action(
    ctx: RunContext,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    *,
    step_index: int,
    attempt: int,
) -> dict[str, Any]:
    """Run one in-process `action:` entry and record its progress.

    Action entries run on the control plane (no backend hop, no SLURM job),
    so they are recorded with `compute_target='control_plane'`. They go in
    the progress table alongside compute `step:` entries because correct
    multi-step restart recovery needs to know which entries already completed
    — an `action:` that succeeded must be skipped (and its outputs rebound)
    on resume, not re-run.

    A primitive raising (plain Python or BackendFailure) marks this attempt's
    progress row failed before the exception propagates to the retry / outer
    handler — which owns the work_ticket-level FAILED transition. A data-plane
    failure `is_data_plane_transient` recognises is raised as a transient
    `DATA_PLANE_TRANSIENT` BackendFailure, so the retry loop re-runs the entry;
    any other exception is re-raised unchanged."""
    await step_progress.record_submitting(
        ctx.pool,
        work_ticket_idx=ctx.work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        step_name=entry.name,
        compute_target=ComputeTarget.CONTROL_PLANE,
    )
    try:
        outputs = await _run_action_primitive(ctx, entry, bound, workspace)
    except BackendFailure as exc:
        await _best_effort_record_failed(
            ctx.pool,
            work_ticket_idx=ctx.work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=exc.kind.value,
            failure_reason=exc.reason[:2000],
        )
        raise
    except Exception as exc:
        if _action_failure_is_retriable(exc):
            failure = BackendFailure(
                kind=FailureKind.DATA_PLANE_TRANSIENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=entry.name,
                reason=f"{type(exc).__name__}: {exc!s}"[:2000],
            )
            await _best_effort_record_failed(
                ctx.pool,
                work_ticket_idx=ctx.work_ticket_idx,
                step_index=step_index,
                attempt=attempt,
                failure_kind=failure.kind.value,
                failure_reason=failure.reason,
            )
            raise failure from exc
        # Plain Python from a LIBRARY primitive (untyped failure / bug), or a
        # data-plane failure this entry is not re-run on. The outer run_workflow
        # handler classifies it UNKNOWN_PERMANENT; record the same on the progress
        # row, then re-raise unchanged.
        await _best_effort_record_failed(
            ctx.pool,
            work_ticket_idx=ctx.work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=FailureKind.UNKNOWN_PERMANENT.value,
            failure_reason=f"{type(exc).__name__}: {exc!s}"[:2000],
        )
        raise
    await step_progress.record_completed(
        ctx.pool, work_ticket_idx=ctx.work_ticket_idx, step_index=step_index, attempt=attempt
    )
    return outputs


QC_PRODUCER_BINDING = "qc_producer"


def _workflow_qc_producer(steps: list[Any]) -> QcProducer | None:
    """The gate the workflow's QC rows are counted under: alignment, else mask, else
    `QcProducer.NONE`. Python `None` (refused by the persist arm) when it finalizes a gate
    with no QC producer (assembly), so `qc_verdict` never counts such rows ungated. Mirrored
    at load by `ActionDefinition._qc_checks_persist_after_their_gate`."""
    if _workflow_writes_alignment_gate(steps):
        return QcProducer.ALIGNMENT
    if _workflow_writes_assembly_gate(steps):
        return None
    if _workflow_writes_mask_gate(steps):
        return QcProducer.MASK
    return QcProducer.NONE


def _require_scope(entry_name: str, scope_target: dict[str, Any], kind: ScopeTargetKind) -> None:
    """Refuse a ticket whose scope target is not `kind`."""
    if scope_target["kind"] != kind.value:
        raise RuntimeError(
            f"{entry_name} requires a {kind.value}-scoped ticket; got {scope_target['kind']!r}"
        )


async def _ticket_alignment_idx(
    pool: asyncpg.Pool, work_ticket_idx: int, *, entry: str, scope_target: dict[str, Any]
) -> int:
    """The alignment identity this ticket is scoped to, refusing a non-prep_sample
    scope and refusing NULL.

    The scope check lives here rather than at each arm because both arms scope on the
    same thing and must refuse identically; the block-scoped path never reaches here.

    Both per-sample alignment arms read the COLUMN rather than `action_context` (which
    is whatever the submitter sent) and both must refuse the same way, because they
    scope on the same thing: the delete clears an identity's rows, the register writes
    under the identity a step's `params:` stamped, and the gate records that the two
    agree. NULL means neither writer ran — the runner's de novo resolver
    (`_alignment._persist_alignment_idx`) or `align_planner` — or a
    `DELETE /alignment-definition/{idx}` detached it mid-flight (ON DELETE SET NULL).
    """
    _require_scope(entry, scope_target, ScopeTargetKind.PREP_SAMPLE)
    alignment_idx = await pool.fetchval(
        "SELECT alignment_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if alignment_idx is None:
        raise RuntimeError(
            f"{entry} requires work_ticket {work_ticket_idx} to carry an alignment_idx; "
            "the column is NULL (nothing set it, or the alignment definition was "
            "deleted mid-flight)"
        )
    return alignment_idx


async def _run_action_primitive(
    ctx: RunContext,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
) -> dict[str, Any]:
    """Translate a workflow `action:` entry into the matching LIBRARY call.
    Per-primitive logic lives here because each primitive has its own
    input/output shape — a generic dispatcher would just push the same
    `if name == ...` ladder somewhere else."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        # Resolved by fixed binding NAME, not positionally, and the declared list
        # is pinned — a YAML naming some other binding would otherwise mint
        # features out of whatever path sat first. Same shape as the other
        # input-pinning dispatches in this ladder.
        if set(entry.inputs) != {"manifest"}:
            raise RuntimeError(f"{entry.name} expects inputs [manifest]; got {entry.inputs!r}")
        manifest_path = Path(bound["manifest"])
        # `genome_map_path` is a workflow-context optional, not an entry input —
        # the YAML's mint-features `inputs:` stays single-valued. Pulled directly
        # from `bound` so a ticket whose action_context carries it picks up
        # genome-association writes for free.
        genome_map = bound.get("genome_map_path")
        feature_map_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
            ctx.pool,
            manifest_path,
            workspace,
            genome_map_path=Path(genome_map) if genome_map else None,
            scope=f"work_ticket {ctx.work_ticket_idx}",
        )
        # YAML declares one output (typically "feature_map"); bind it.
        return {entry.outputs[0]: feature_map_path}

    if entry.name == LibraryPrimitive.MINT_ANNOTATION_FEATURES:
        # Resolved by fixed binding NAME, not positionally, so a YAML reorder cannot
        # silently swap the annotation manifest for the sequence one — they have
        # overlapping column names and the swap would mint the wrong features.
        if set(entry.inputs) != {"annotation_manifest", "feature_map"}:
            raise RuntimeError(
                f"{entry.name} expects inputs "
                f"[annotation_manifest, feature_map]; got {entry.inputs!r}"
            )
        annotation_map_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_ANNOTATION_FEATURES](
            ctx.pool,
            ctx.scope_target["reference_idx"],
            Path(bound["annotation_manifest"]),
            Path(bound["feature_map"]),
            workspace,
        )
        # YAML declares one output ("annotation_map"); bind it.
        return {entry.outputs[0]: annotation_map_path}

    if entry.name == LibraryPrimitive.WRITE_MEMBERSHIP:
        # Resolved by fixed binding NAME, not positionally (manifest + feature_map
        # both carry a sequence_hash column; a YAML reorder must not swap them).
        # manifest supplies the representative read_id (accession) per feature_idx;
        # feature_map supplies feature_idx. Mirrors write-assembly-membership below.
        if set(entry.inputs) != {"manifest", "feature_map"}:
            raise RuntimeError(
                f"{entry.name} expects inputs [manifest, feature_map]; got {entry.inputs!r}"
            )
        await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
            ctx.pool,
            ctx.scope_target["reference_idx"],
            Path(bound["manifest"]),
            Path(bound["feature_map"]),
        )
        return {}

    if entry.name == LibraryPrimitive.WRITE_ASSEMBLY_MEMBERSHIP:
        # Assembly analogue of write-membership: link this prep_sample's
        # assembly-run contigs to qiita.assembly_membership, tagged by
        # (kind, bin_id). Inputs are resolved by their fixed binding names — not
        # positionally — so a YAML reorder can't silently swap them. bin_map +
        # manifest come from assembly_hash; feature_map from mint-features;
        # genomes_dir from the assemble step, for its per-contig attribute sidecar.
        # prep_sample_idx from the scope target; processing_idx from `bound` (the
        # runner minted it before the step loop because assembly_load threads it
        # via params — mirrors how the reference dispatch reads reference_idx).
        if set(entry.inputs) != {"bin_map", "manifest", "feature_map", "genomes_dir"}:
            raise RuntimeError(
                f"{entry.name} expects inputs "
                f"[bin_map, manifest, feature_map, genomes_dir]; got {entry.inputs!r}"
            )
        await LIBRARY[LibraryPrimitive.WRITE_ASSEMBLY_MEMBERSHIP](
            ctx.pool,
            ctx.scope_target["prep_sample_idx"],
            bound[PROCESSING_IDX_BINDING],
            Path(bound["bin_map"]),
            Path(bound["manifest"]),
            Path(bound["feature_map"]),
            Path(bound["genomes_dir"]),
        )
        return {}

    if entry.name == LibraryPrimitive.REGISTER_FILES:
        staging_dir = Path(bound[entry.inputs[0]])
        files = _register_files_payload(workspace, staging_dir)
        await LIBRARY[LibraryPrimitive.REGISTER_FILES](
            staging_dir=str(staging_dir),
            files=files,
            work_ticket_idx=ctx.work_ticket_idx,
            signing_key=ctx.signing_key,
            data_plane_url=ctx.data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.REGISTER_INDEX:
        # Native step outputs are paths (StepResultResponse.outputs is
        # dict[str, str]), so an index builder can't hand back the build params
        # as a dict binding — it writes a small meta JSON and exposes its path
        # (e.g. `rype_index_meta`, `minimap2_index_meta`). The binding name is
        # the step's single declared input, NOT hardcoded: a host reference runs
        # two register-index steps (rype + minimap2), each pointing at its own
        # meta. Read it for index_type / fs_path / params (index_type comes from
        # the builder, not hardcoded here). `shard_id` is optional: a host meta
        # JSON omits it (`.get` -> None -> unsharded row); a sharded analysis
        # index builder emits one meta per shard carrying its shard_id.
        if len(entry.inputs) != 1:
            raise RuntimeError(
                f"{entry.name} expects exactly one input (the index meta); got {entry.inputs!r}"
            )
        meta_path = Path(bound[entry.inputs[0]])
        meta = json.loads(meta_path.read_text())
        await LIBRARY[LibraryPrimitive.REGISTER_INDEX](
            ctx.pool,
            reference_idx=ctx.scope_target["reference_idx"],
            index_type=meta["index_type"],
            fs_path=meta["fs_path"],
            params=meta["params"],
            shard_id=meta.get("shard_id"),
        )
        return {}

    if entry.name == LibraryPrimitive.PLAN_SHARDS:
        # Assign this reference's genome-bearing features to N lineage-sorted
        # shards (reference_membership.shard_id) and fan out one build-shard-index
        # ticket per shard. No file inputs: reference_idx from the scope target;
        # the taxonomy DoGet + PG export are internal. The build gates/knobs the
        # shard tickets carry are copied from THIS ticket's action_context
        # (present in `bound`); the originator is inherited from this ticket.
        #
        # Sharding is OPT-IN. The step self-defends on `shard_index` and no-ops
        # when it is absent/falsy — mirroring the finalize check in
        # `_is_sharded_fanout_in_progress` (which reads the same `bound` key), so a
        # plain reference-add never fans out (and, absent this guard, would even
        # shard a genome-bearing reference nobody asked to shard). The YAML's
        # `when: shard_index` gate ALSO skips an explicit `shard_index: false`
        # opt-out before we get here; this guard covers the absent-key default,
        # which the gate treats as ON (its absent⇒ON default is correct for the
        # build_* gates but not for this opt-in flag). A no-op → router_pending
        # False so the router build entries stay OFF (see ROUTER_PENDING_BINDING).
        if not bound.get("shard_index"):
            return {ROUTER_PENDING_BINDING: False}
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.REFERENCE)
        if ctx.dispatch_cb is None:
            # Fanning out without a dispatch mechanism would silently strand the
            # shard tickets in PENDING until the next startup reconcile — fail
            # loud instead (dispatch always threads a callback in production).
            raise RuntimeError(f"{entry.name} requires a dispatch_cb to fan out shard tickets")
        originator_principal_idx = await ctx.pool.fetchval(
            "SELECT originator_principal_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            ctx.work_ticket_idx,
        )
        shard_context = {k: bound[k] for k in SHARD_BUILD_CONTEXT_KEYS if k in bound}
        summary = await plan_and_submit_shards(
            ctx.pool,
            ctx.scope_target["reference_idx"],
            signing_key=ctx.signing_key,
            data_plane_url=ctx.data_plane_url,
            workspace=workspace,
            originator_principal_idx=originator_principal_idx,
            build_action_id=BUILD_SHARD_INDEX_ACTION_ID,
            build_action_version=BUILD_SHARD_INDEX_ACTION_VERSION,
            action_context=shard_context,
            dispatch_cb=ctx.dispatch_cb,
            max_inflight=ctx.max_inflight,
        )
        # N == 0 here means an EXPLICIT shard_index=true request (the :416 guard
        # already returned for the absent/false case) produced zero shard-bearing
        # features — a reference with no genomes / no genome map. Finalizing that to
        # a terminal `active` reference with no router is the wrong outcome: the
        # first align-plan 409s ("no rype_router built"), and ACTIVE is terminal so
        # remediation would be delete + full re-ingest. Fail loud instead — the
        # workflow's `failure_status: failed` applies and `failed → pending` is a
        # legal redrive edge, so the operator supplies genomes / a genome map and
        # re-runs. Server-side because a direct POST /work-ticket bypasses the CLI.
        # N > 0 → stage the shard→bucket mapping the whole-reference rype_router
        # build consumes (from the assignment plan_and_submit_shards just wrote) and
        # flip router_pending True so the router build entries run.
        if summary.get("shards", 0) <= 0:
            raise RuntimeError(
                f"{entry.name}: reference {ctx.scope_target['reference_idx']} was requested "
                "with shard_index=true but has no genome-bearing features to shard "
                "(N=0); refusing to finalize an unroutable `active` reference. Supply "
                "genomes / a genome map and redrive, or omit shard_index."
            )
        mapping_path = await _stage_shard_mapping(
            ctx.pool,
            ctx.scope_target["reference_idx"],
            workspace / "shard_mapping.parquet",
        )
        return {ROUTER_PENDING_BINDING: True, SHARD_MAPPING_BINDING: mapping_path}

    if entry.name == LibraryPrimitive.FINALIZE_SHARD:
        # Terminal step of a build-shard-index ticket: count-based, fail-closed
        # completion. The expected index_types are derived from THIS ticket's
        # build gates (in `bound`), so finalize counts exactly what was built.
        # No file inputs; reference_idx from the scope target.
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.REFERENCE)
        await LIBRARY[LibraryPrimitive.FINALIZE_SHARD](
            ctx.pool,
            ctx.scope_target["reference_idx"],
            expected_shard_index_types(bound),
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_READ_METRICS:
        # Persist the three per-stage read counts onto this prep_sample's
        # 1:1 sequenced_sample, derived from the `read_mask` Parquet (one row per
        # read, carrying the per-read mask `reason`). The single declared input is
        # the read_mask path host_filter emitted; the primitive computes the
        # both-mates `_r1r2` totals from the mask (raw/biological/quality_filtered
        # by reason). Resolved by its fixed binding name, not positionally.
        if entry.inputs != ["read_mask"]:
            raise RuntimeError(f"{entry.name} expects inputs [read_mask]; got {entry.inputs!r}")
        await LIBRARY[LibraryPrimitive.PERSIST_READ_METRICS](
            ctx.pool,
            ctx.scope_target["prep_sample_idx"],
            Path(bound["read_mask"]),
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_ENA_RUN_OUTCOMES:
        if entry.inputs != ["run_outcomes"]:
            raise RuntimeError(f"{entry.name} expects inputs [run_outcomes]; got {entry.inputs!r}")
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.SEQUENCED_POOL)
        await LIBRARY[LibraryPrimitive.PERSIST_ENA_RUN_OUTCOMES](
            ctx.pool,
            ctx.scope_target["sequenced_pool_idx"],
            Path(bound["run_outcomes"]),
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_SYNDNA_READ_COUNT:
        # Per-insert SynDNA read counts from the `syndna` step's alignment output.
        # mask_idx from the ticket (runner-bound for the prep_sample branch), the
        # sample from the scope target.
        if entry.inputs != ["alignment"]:
            raise RuntimeError(f"{entry.name} expects inputs [alignment]; got {entry.inputs!r}")
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.PREP_SAMPLE)
        await LIBRARY[LibraryPrimitive.PERSIST_SYNDNA_READ_COUNT](
            ctx.pool,
            mask_idx=bound[MASK_IDX_BINDING],
            prep_sample_idx=ctx.scope_target["prep_sample_idx"],
            alignment_path=Path(bound["alignment"]),
        )
        return {}

    if entry.name == LibraryPrimitive.FINALIZE_MASK_SAMPLE:
        # Terminal step of the per-sample read-mask workflow: record this sample's
        # masking as completed in the mask_sample gate (the per-sample twin of
        # reconcile-block's gate flip). Runs AFTER register-files so the gate never
        # reads 'completed' before the masked reads are in DuckLake. No file inputs:
        # prep_sample_idx from the scope target, mask_idx from the ticket (runner-bound
        # above for the prep_sample branch).
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.PREP_SAMPLE)
        await LIBRARY[LibraryPrimitive.FINALIZE_MASK_SAMPLE](
            ctx.pool,
            mask_idx=bound[MASK_IDX_BINDING],
            prep_sample_idx=ctx.scope_target["prep_sample_idx"],
        )
        return {}

    if entry.name == LibraryPrimitive.FINALIZE_ASSEMBLY_SAMPLE:
        # Terminal step of the long-read-assembly workflow: write 'completed' into
        # the assembly_sample gate. No file inputs: prep_sample_idx from the scope
        # target, processing_idx from the run identity the runner minted before the
        # loop. Where this entry sits in the step list, and why, is on the workflow
        # YAML entry that declares it.
        #
        # The scope check below is this adapter's own precondition, not a second
        # copy of the runner's. Through `run_workflow` it cannot fire: the pre-loop
        # refusal in `_workflow.py` keys on the same declared entry that selects
        # this arm and rejects a non-prep_sample ticket before the loop. A direct
        # `_run_action_primitive` call reaches it. The mask sibling above is not in
        # that position — its pre-loop guard admits block-scoped tickets too, so
        # its arm is the only prep_sample check on that path.
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.PREP_SAMPLE)
        # `bound` rather than a threaded local, for the same reason
        # write-assembly-membership above reads it: the pre-loop mint has already
        # overwritten any submitter-supplied `processing_idx` by the time any entry
        # runs, and the gate keys on the value that stamped the rows it gates.
        try:
            await LIBRARY[LibraryPrimitive.FINALIZE_ASSEMBLY_SAMPLE](
                ctx.pool,
                processing_idx=bound[PROCESSING_IDX_BINDING],
                prep_sample_idx=ctx.scope_target["prep_sample_idx"],
            )
        except AssemblySampleInvalidated as exc:
            # A person withdrew this (run, sample) while the run was in flight, and
            # the gate write refuses rather than re-completing it. Typed here
            # because the alternative is `run_workflow`'s catch-all, which records
            # UNKNOWN_PERMANENT — a classification that tells the operator to look
            # for a bug when what happened is a decision someone made. Permanent:
            # a redrive re-resolves the same identity and refuses identically until
            # the pair is restored.
            raise BackendFailure(
                kind=FailureKind.BAD_INPUT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=entry.name,
                reason=str(exc),
            ) from exc
        return {}

    if entry.name == LibraryPrimitive.FINALIZE_ALIGNMENT_SAMPLE:
        # Terminal step of a prep_sample-scoped alignment workflow (align-denovo):
        # flip this sample's alignment_sample gate to 'completed'. No file inputs:
        # prep_sample_idx from the scope target, alignment_idx from the identity the
        # runner minted before the loop.
        #
        # `alignment_idx` comes from the TICKET column, not from `bound`, for the
        # reason delete-alignment-sample below reads it there: the delete and the
        # register scope on the same identity, and the gate must record the one they
        # used. The pre-loop resolver writes the column and binds the same value, so
        # through `run_workflow` the two agree by construction.
        alignment_idx = await _ticket_alignment_idx(
            ctx.pool,
            ctx.work_ticket_idx,
            entry=entry.name,
            scope_target=ctx.scope_target,
        )
        await LIBRARY[LibraryPrimitive.FINALIZE_ALIGNMENT_SAMPLE](
            ctx.pool,
            alignment_idx=alignment_idx,
            prep_sample_idx=ctx.scope_target["prep_sample_idx"],
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_QC_CHECKS:
        if entry.inputs != ["qc_checks"]:
            raise RuntimeError(f"{entry.name} expects inputs [qc_checks]; got {entry.inputs!r}")
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.PREP_SAMPLE)
        if QC_PRODUCER_BINDING not in bound:
            raise RuntimeError(f"{entry.name}: the runner did not bind {QC_PRODUCER_BINDING}")
        if bound[QC_PRODUCER_BINDING] is None:
            raise RuntimeError(
                f"{entry.name}: this workflow finalizes a gate QC checks cannot be "
                "counted under (assembly)"
            )
        await LIBRARY[LibraryPrimitive.PERSIST_QC_CHECKS](
            ctx.pool,
            work_ticket_idx=ctx.work_ticket_idx,
            prep_sample_idx=ctx.scope_target["prep_sample_idx"],
            qc_checks_path=Path(bound["qc_checks"]),
            expected_producer=QcProducer(bound[QC_PRODUCER_BINDING]),
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_QC_REPORT:
        # Persist the two fastqc-equivalent QC reports onto this prep_sample's
        # 1:1 sequenced_sample. Each declared input is a Path to a qc_report.json
        # sidecar (the qc_report_raw / qc_report_filtered step outputs); we read
        # each verbatim and hand the parsed dicts to the primitive. Inputs are
        # resolved by their fixed binding names — not positionally — so a YAML
        # reorder can't silently swap raw/filtered.
        if set(entry.inputs) != {"raw_qc_report", "filtered_qc_report"}:
            raise RuntimeError(
                f"{entry.name} expects inputs "
                "[raw_qc_report, filtered_qc_report]; "
                f"got {entry.inputs!r}"
            )

        def _report(name: str) -> dict[str, Any]:
            return json.loads(Path(bound[name]).read_text())

        await LIBRARY[LibraryPrimitive.PERSIST_QC_REPORT](
            ctx.pool,
            ctx.scope_target["prep_sample_idx"],
            _report("raw_qc_report"),
            _report("filtered_qc_report"),
        )
        return {}

    if entry.name == LibraryPrimitive.DELETE_READ_MASK_BLOCK:
        # Idempotent block replace: delete this block's exact read_mask footprint
        # BEFORE register-files re-writes it, so a re-run (retry, or a resubmitted
        # block covering the same footprint) never double-counts. Exact by
        # construction (per-member OR), so a split sample's sibling-block rows
        # survive. No file inputs: block_idx from the scope target, mask_idx from
        # the ticket (runner-bound above for the block branch).
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.BLOCK)
        await LIBRARY[LibraryPrimitive.DELETE_READ_MASK_BLOCK](
            ctx.pool,
            block_idx=ctx.scope_target["block_idx"],
            mask_idx=bound[MASK_IDX_BINDING],
            signing_key=ctx.signing_key,
            data_plane_url=ctx.data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.RECONCILE_BLOCK:
        # Terminal step of the bulk-block read-mask workflow: mark this block
        # completed, then finalize each covered sample whose last covering block
        # just completed (per-sample rollup + mask_sample gate flip). Reads the
        # mask counts from DuckLake (across all the sample's blocks), so it runs
        # AFTER register-files. No file inputs: block_idx from the scope target,
        # mask_idx from the ticket (runner-bound above for the block branch).
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.BLOCK)
        await LIBRARY[LibraryPrimitive.RECONCILE_BLOCK](
            ctx.pool,
            block_idx=ctx.scope_target["block_idx"],
            mask_idx=bound[MASK_IDX_BINDING],
            signing_key=ctx.signing_key,
            data_plane_url=ctx.data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.DELETE_ALIGNMENT_BLOCK:
        # Idempotent block replace (align): delete this block's exact alignment
        # footprint BEFORE register-files re-writes it, so a re-run (retry, or a
        # resubmitted block covering the same footprint) never double-counts. Exact
        # by construction (per-member OR) and feature_idx-agnostic (all of a read's
        # alignment rows go), so a split sample's sibling-block rows survive. No
        # file inputs: block_idx from the scope target, alignment_idx from the
        # ticket (runner-bound above for the align block branch).
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.BLOCK)
        await LIBRARY[LibraryPrimitive.DELETE_ALIGNMENT_BLOCK](
            ctx.pool,
            block_idx=ctx.scope_target["block_idx"],
            alignment_idx=bound[ALIGNMENT_IDX_BINDING],
            signing_key=ctx.signing_key,
            data_plane_url=ctx.data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.DELETE_ALIGNMENT_SAMPLE:
        # Idempotent sample replace (align): runs BEFORE register-files. What the
        # delete selects is on the data plane's `delete_alignment_sample`.
        #
        # The context value is cross-checked against the column rather than ignored,
        # because the delete and the WRITE scope on different things: a step's
        # `params:` binds `alignment_idx` from `action_context` (align's
        # `align_sharded` stamps that value onto every row it emits). Let the two
        # disagree and this clears one alignment's rows for the sample while the
        # register that follows writes under the other, so a re-run appends — the
        # double-count this primitive exists to prevent.
        #
        # The prep_sample writer is the runner's de novo resolver
        # (`_alignment._persist_alignment_idx`), which writes the column before the
        # step loop; `align_planner.plan_and_submit_alignments` is the block-scoped
        # one. So a NULL here means neither ran for this ticket.
        alignment_idx = await _ticket_alignment_idx(
            ctx.pool,
            ctx.work_ticket_idx,
            entry=entry.name,
            scope_target=ctx.scope_target,
        )
        context_alignment_idx = bound.get(ALIGNMENT_IDX_BINDING)
        if context_alignment_idx is not None and context_alignment_idx != alignment_idx:
            raise RuntimeError(
                f"{entry.name}: work_ticket {ctx.work_ticket_idx} carries "
                f"alignment_idx {alignment_idx} but its action_context declares "
                f"{context_alignment_idx}; a step binding alignment_idx from "
                f"action_context would write under {context_alignment_idx} while this "
                f"delete cleared {alignment_idx}"
            )
        await LIBRARY[LibraryPrimitive.DELETE_ALIGNMENT_SAMPLE](
            alignment_idx=alignment_idx,
            prep_sample_idx=ctx.scope_target["prep_sample_idx"],
            signing_key=ctx.signing_key,
            data_plane_url=ctx.data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.RECONCILE_ALIGNMENT_BLOCK:
        # Terminal step of the `align` workflow: mark this block completed, then
        # finalize each covered sample whose last covering block just completed
        # (flip its alignment_sample gate). No count-assertion / metrics rollup
        # (alignment rows are not 1:1 with reads), so no data-plane hop. Runs AFTER
        # register-files. No file inputs: block_idx from the scope target,
        # alignment_idx from the ticket (runner-bound above for the align branch).
        _require_scope(entry.name, ctx.scope_target, ScopeTargetKind.BLOCK)
        await LIBRARY[LibraryPrimitive.RECONCILE_ALIGNMENT_BLOCK](
            ctx.pool,
            block_idx=ctx.scope_target["block_idx"],
            alignment_idx=bound[ALIGNMENT_IDX_BINDING],
        )
        return {}

    if entry.name == LibraryPrimitive.SYNC_REFERENCE_EXCLUSION:
        # Post-load exclusion sync: re-resolve the GLOBAL blocklist and REPLACE the
        # data plane's `reference_exclusion` mirror wholesale, so a fresh assembly
        # of an already-blocked genome (new feature_idx the standing mirror can't
        # know about) is caught. No file inputs; the dest Parquet lands in this
        # attempt's workspace, which is under the shared PATH_SCRATCH tree the data
        # plane re-validates under its scratch_root (same containment the DoGet
        # exporters rely on). Idempotent full-replace ⇒ safe to re-run on a redrive.
        await LIBRARY[LibraryPrimitive.SYNC_REFERENCE_EXCLUSION](
            ctx.pool,
            dest=workspace / "reference_exclusion.parquet",
            signing_key=ctx.signing_key,
            data_plane_url=ctx.data_plane_url,
        )
        return {}

    raise RuntimeError(f"runner has no adapter for action {entry.name!r}")
