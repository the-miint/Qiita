"""Runner top-level orchestration loop (run_workflow and per-entry retry)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.actions import (
    ActionCeiling,
    ActionDefinition,
    WorkflowAction,
    WorkflowStep,
)
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import (
    FailureType,
    ScopeTargetKind,
    StepPlanResponse,
    WorkTicketFailureStage,
    WorkTicketState,
)

from .. import step_progress
from ..repositories.block import fetch_block_members
from ._base import (
    _STEP_POLL_INTERVAL_SECONDS,
    WorkflowAborted,
    _clear_transient_retry,
    _is_transient_db_error,
    _log,
)
from ._db import (
    _atomic_transition,
    _bump_retry_and_requeue,
    _fetch_action,
    _fetch_work_ticket,
    _retry_count,
    _safe_entry_name,
    _transition_to_failed,
    _transition_to_no_data,
    _transition_to_processing_for_resume,
)
from ._dispatch import (
    _advance_completed_step_status,
    _build_scope_target,
    _ceiling_exhausted_failure,
    _current_resource_status,
    _dispatch_step,
    _escalated_mem_floor_after_oom,
    _escalated_walltime_after_timeout,
    _fetch_plan_hint,
    _patch_resource_status,
    _shard_fanout_owns_finalize,
)
from ._feature_table import (
    GENOME_MAP_PATH_BINDING,
    _resolve_feature_table_bindings,
)
from ._mask import (
    ALIGNMENT_IDX_BINDING,
    LIMA_ARGS_BINDING,
    MASK_IDX_BINDING,
    _mint_read_mask,
    _persist_mask_idx,
    _resolved_lima,
    _resolved_syndna,
    _workflow_needs_mask,
)
from ._processing import (
    ASSEMBLER_BINDING,
    _mint_processing_idx,
    _workflow_needs_processing,
)
from ._read_ingest import (
    READS_STAGING_ROOT_BINDING,
    ROUTER_PENDING_BINDING,
    SAMPLE_MAP_BINDING,
    _resolve_sample_map,
    _resolve_staged_masked_reads,
    _resolve_staged_masked_reads_block,
    _resolve_staged_reads,
    _resolve_staged_reads_block,
    _stage_shard_roster,
    _workflow_declares_input,
    _workflow_needs_staged_masked_reads,
    _workflow_needs_staged_reads,
)
from ._reconstruct import (
    _attempt_is_unowned,
    _completed_progress_row,
    _dispatch_action,
    _reconstruct_completed_outputs,
)
from ._reference import (
    QC_ADAPTER_BINDING,
    _resolve_host_filter_indexes,
    _resolve_qc_adapters,
    _resolve_sharded_align_index_bindings,
    _resolve_syndna_index,
    _workflow_needs_adapters,
    _workflow_needs_sharded_align_indexes,
)
from ._upload import (
    _consume_upload_handles,
    _resolve_upload_handles,
    _submission_bad_input,
)


async def run_workflow(
    work_ticket_idx: int,
    pool: asyncpg.Pool,
    backend_client: ComputeBackendClient,
    *,
    signing_key: bytes,
    data_plane_url: str,
    work_ticket_workspace_root: Path,
    upload_staging_root: Path,
    default_adapter_reference_idx: int | None = None,
    poll_interval_seconds: float = _STEP_POLL_INTERVAL_SECONDS,
    resume: bool = False,
    dispatch_cb: Callable[[int], Any] | None = None,
) -> None:
    """Execute (or resume) the workflow attached to one work ticket.

    Reads the ticket and its action from the DB, transitions to PROCESSING,
    walks each entry in ``action.steps``, and finishes by transitioning
    PROCESSING → COMPLETED. Any unhandled exception transitions the ticket to
    FAILED, best-effort PATCHes the resource to ``action.failure_status``,
    and re-raises.

    **Resume (`resume=True`).** Startup recovery re-drives an in-flight ticket
    here instead of failing it (deploys stop/start the CP without draining).
    The loop re-walks from entry 0, but any entry already marked COMPLETED in
    `qiita.work_ticket_step` is *fast-forwarded* — its outputs are rebuilt from
    the shared workspace (a SLURM step re-reads its verified manifest via
    `result_step`; an in-process `action:` rebuilds its deterministic output
    paths) and its `target_status` PATCH is skipped (the resource is already
    past it) — never re-run. The first incomplete entry resumes: an in-flight
    SLURM step re-attaches to its persisted job id (see `_adopt_or_submit`).
    This same fast-forward also makes a `/run` redrive of a FAILED ticket skip
    its already-completed entries.

    Pre-conditions:
        * Without `resume`, the ticket must be 'pending' (a leftover PROCESSING
          means a crashed run — the runner refuses to silently re-run). With
          `resume`, any non-terminal state is accepted and moved to PROCESSING.
        * Action ``(action_id, version)`` must exist in qiita.action with
          ``enabled=true``.
    """
    work_ticket = await _fetch_work_ticket(pool, work_ticket_idx)
    # Optional per-run resource bump (gated to wet_lab_admin+ and validated
    # <= the action ceiling at submission). Read once here as the starting
    # memory floor; `_run_entry_with_retry` raises it (up to the ceiling) on an
    # OOM-killed retry. A CP restart re-attaches with this static floor and
    # re-escalates from there.
    _override = work_ticket.get("resource_override")
    mem_gb_override = _override.get("mem_gb") if isinstance(_override, dict) else None
    if not resume and work_ticket["state"] != WorkTicketState.PENDING.value:
        raise RuntimeError(
            f"work_ticket {work_ticket_idx} is in state {work_ticket['state']!r}, "
            f"must be {WorkTicketState.PENDING.value!r}; manual recovery required"
        )

    # Bound BEFORE the try because the except handlers below dereference these:
    # scope_target unconditionally, `action`/`index` guarded. They are reads of
    # the work_ticket, not step I/O — scope_target is the one that CAN raise (an
    # unknown scope_target_kind), so keep `_build_scope_target` exhaustive with
    # the qiita.scope_target_kind enum or a new kind strands its tickets here.
    # `action` and `index` are pre-bound so a fetch/transition that fails before
    # the loop still leaves the handlers a defined value (they guard
    # `action is not None` and attribute a None index to the SUBMISSION stage).
    bound: dict[str, Any] = dict(work_ticket["action_context"] or {})
    scope_target = _build_scope_target(work_ticket)
    max_retries: int = work_ticket["max_retries"]
    workspace = work_ticket_workspace_root / str(work_ticket_idx)
    action: ActionDefinition | None = None
    index: int | None = None
    uploads_to_consume: list[int] = []

    try:
        # Everything from the action fetch through the step loop is INSIDE the
        # try so ANY pre-loop failure — an action disabled between submit and
        # dispatch, a DB blip on the PROCESSING transition, a filesystem error
        # on mkdir, a bad upload handle — lands in the outer FAILED-transition
        # handler instead of stranding the ticket in PENDING/PROCESSING with no
        # failure recorded (and a misleading "marked FAILED" dispatch log). A
        # pre-loop failure is attributed to the SUBMISSION stage (no step ran
        # yet), which the failure-step-name CHECK requires to carry a NULL name.
        action = await _fetch_action(pool, work_ticket["action_id"], work_ticket["action_version"])
        if action is None:
            raise RuntimeError(
                f"action ({work_ticket['action_id']!r}, "
                f"{work_ticket['action_version']!r}) not found or disabled"
            )

        if resume:
            # Re-drive from any non-terminal state (PENDING/QUEUED/PROCESSING) →
            # PROCESSING. Idempotent if already PROCESSING; raises on a terminal
            # ticket (shouldn't be in the recovery set).
            await _transition_to_processing_for_resume(pool, work_ticket_idx)
        else:
            await _atomic_transition(
                pool,
                work_ticket_idx,
                expected=WorkTicketState.PENDING,
                new=WorkTicketState.PROCESSING,
            )

        workspace.mkdir(parents=True, exist_ok=True)

        # Per-entry progress from any prior run. Empty on a first dispatch; on a
        # resume (or a /run redrive) it carries the COMPLETED rows the loop
        # fast-forwards. Loaded once — this run's own writes don't feed back in.
        progress = await step_progress.load_step_progress(pool, work_ticket_idx)

        _log.info(
            "running workflow %s/%s for work_ticket %d (max_retries=%d)",
            action.action_id,
            action.version,
            work_ticket_idx,
            max_retries,
        )

        # Resolve `*_upload_idx` keys to filesystem paths BEFORE the step
        # loop runs. A failure here (unknown / unready / wrong-owner /
        # missing-staged-file) raises a typed BackendFailure that the
        # outer `except BackendFailure` block translates into a FAILED
        # work_ticket — same path a step-level bad input would take.
        # The consume-list is held until workflow completion so a
        # mid-step failure leaves its uploads in `ready` for the
        # operator to redrive against the same handles.
        resolved_paths, uploads_to_consume = await _resolve_upload_handles(
            pool,
            action_context=bound,
            originator_principal_idx=work_ticket["originator_principal_idx"],
            upload_staging_root=upload_staging_root,
        )
        bound.update(resolved_paths)

        # Host-filter index resolution, gated by `host_filter_enabled` in
        # action_context (two-reference for 1.2.0 via host_rype_reference_idx /
        # host_minimap2_reference_idx; legacy single host_reference_idx for 1.1.0).
        # Like upload-handle resolution it runs inside this try, so a raise
        # (unknown / non-active host reference, missing index) lands in the outer
        # FAILED handler instead of leaving the ticket stuck in PROCESSING. None
        # of the host_*_reference_idx keys are `*_upload_idx`, so the walker above
        # left them untouched.
        bound.update(await _resolve_host_filter_indexes(pool, action_context=bound))
        # Syndna's minimap2 index, resolved (and validated ACTIVE) before the mask
        # mint below reads `syndna_reference_idx` into the identity hash.
        bound.update(await _resolve_syndna_index(pool, action_context=bound))
        # lima's argument string is CP-resolved from the client's `lima_preset`
        # (never client-supplied) and bound so the lima_export step's `params:`
        # can thread it into `lima_config.json` for the container. Resolving here
        # also fails a bad preset at SUBMISSION rather than mid-loop.
        _lima = _resolved_lima(bound)
        if _lima is not None:
            bound[LIMA_ARGS_BINDING] = _lima["args"]

        # Sharded-aligner index resolution (the `align` workflow): when a step
        # lists router_index_path/shard_directory as inputs, resolve them from
        # action_context (align_reference_idx + aligner) before the loop — the
        # consumer wiring into the resolver. Same inside-try placement as the
        # host-filter resolver so a failure (unknown / non-active reference,
        # unbuilt router / per-shard index) lands in the outer FAILED handler
        # instead of leaving the ticket stuck in PROCESSING. None of these keys are
        # `*_upload_idx`, so the upload walker left them untouched.
        if _workflow_needs_sharded_align_indexes(action.steps):
            bound.update(await _resolve_sharded_align_index_bindings(pool, action_context=bound))

        # QC adapter materialization: when any step needs `adapter_parquet` (the
        # qc step) AND the mask enables QC adapter trimming, DoGet the configured
        # artifact_sequence_set reference's sequences and stage them as a local
        # Parquet in the ticket workspace. Same pre-loop, inside-try placement as
        # host-filter resolution so a failure (unconfigured / non-active / empty
        # adapter set) lands in the outer FAILED handler rather than leaving the
        # ticket stuck in PROCESSING.
        #
        # `qc_adapter_enabled` defaults True (absent ⇒ short-read, unchanged). A
        # long-read / PacBio mask sets it False: no adapter set is fetched or bound,
        # so the qc step runs the length/quality filter with no adapter trim (its
        # `adapter_parquet` is an optional input — see the read-mask workflows).
        if _workflow_needs_adapters(action.steps) and bound.get("qc_adapter_enabled", True):
            bound.update(
                await _resolve_qc_adapters(
                    pool,
                    default_adapter_reference_idx=default_adapter_reference_idx,
                    data_plane_url=data_plane_url,
                    signing_key=signing_key,
                    workspace=workspace,
                )
            )

        # Read-ingest bindings (bcl-convert workflow's `ingest_reads` step):
        # materialize the pool roster as a Parquet and hand the step the scratch
        # root it writes durable per-sample reads under. Same inside-try
        # placement as the resolvers above. `convert_dir` is NOT resolved here —
        # it is the upstream `bcl_convert` step's output, bound during the loop.
        if _workflow_declares_input(action.steps, SAMPLE_MAP_BINDING):
            bound.update(await _resolve_sample_map(bound, workspace))
        if _workflow_declares_input(action.steps, READS_STAGING_ROOT_BINDING):
            bound[READS_STAGING_ROOT_BINDING] = str(upload_staging_root)

        # Staged-read binding (read-mask workflows): `reads` is consumed by qc /
        # host_filter but produced by no step, so bind it from stored reads.
        # Inside-try so an un-ingested sample / empty block FAILs cleanly.
        #   - PREP_SAMPLE: one sample's durable stored reads (per-sample path).
        #   - BLOCK: the union of the block's members' `read` sub-ranges, sourced
        #     from the persistent DuckLake `read` table (the block-compute path).
        if _workflow_needs_staged_reads(action.steps):
            if scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
                bound.update(
                    await _resolve_staged_reads(
                        scope_target,
                        upload_staging_root,
                        data_plane_url=data_plane_url,
                        signing_key=signing_key,
                        workspace=workspace,
                    )
                )
            elif scope_target["kind"] == ScopeTargetKind.BLOCK.value:
                members = [
                    {
                        "prep_sample_idx": ps,
                        "sequence_idx_start": lo,
                        "sequence_idx_stop": hi,
                    }
                    for (ps, lo, hi) in await fetch_block_members(pool, scope_target["block_idx"])
                ]
                # An ALIGN block ticket carries a non-NULL alignment_idx and aligns
                # the block's HOST-DEPLETED reads: stage the MASKED reads (the
                # read_masked view scoped to the ticket's completed mask_idx). A
                # read-mask-block ticket (alignment_idx NULL) masks the RAW reads,
                # so it stages the raw `read` table. mask_idx is pre-resolved at
                # plan time on both paths (the align planner sets it to the samples'
                # completed mask; the block-mask planner to the partition mask).
                #
                # Discriminate on the action_context alignment_idx (the value the
                # rest of the run already trusts — _reconstruct reads
                # bound[ALIGNMENT_IDX_BINDING]), NOT the work_ticket.alignment_idx
                # COLUMN: that column is ON DELETE SET NULL, so a mid-flight
                # DELETE /alignment-definition NULLs it while action_context still
                # carries the idx. Trusting the column would silently fall to the
                # raw-reads branch and realign non-host-depleted, un-QC'd reads. Fail
                # loud on any disagreement (the delete case) instead.
                context_alignment_idx = bound.get(ALIGNMENT_IDX_BINDING)
                if context_alignment_idx is not None:
                    if work_ticket.get("alignment_idx") != context_alignment_idx:
                        raise _submission_bad_input(
                            "align block ticket action_context alignment_idx "
                            f"{context_alignment_idx} disagrees with "
                            "work_ticket.alignment_idx "
                            f"{work_ticket.get('alignment_idx')!r} — the alignment "
                            "definition was likely deleted mid-flight (the column is "
                            "ON DELETE SET NULL); refusing to silently realign raw reads"
                        )
                    align_mask_idx = work_ticket["mask_idx"]
                    if align_mask_idx is None:
                        raise _submission_bad_input(
                            "an align block ticket must carry the completed mask_idx its "
                            "reads were masked under (set at plan time); found NULL"
                        )
                    bound.update(
                        await _resolve_staged_masked_reads_block(
                            members,
                            mask_idx=align_mask_idx,
                            data_plane_url=data_plane_url,
                            signing_key=signing_key,
                            workspace=workspace,
                        )
                    )
                else:
                    bound.update(
                        await _resolve_staged_reads_block(
                            members,
                            data_plane_url=data_plane_url,
                            signing_key=signing_key,
                            workspace=workspace,
                        )
                    )
            else:
                raise _submission_bad_input(
                    "a workflow that masks stored reads must be prep_sample- or "
                    f"block-scoped; got {scope_target['kind']!r}"
                )

        # Staged MASKED-read binding (assembly workflows): `masked_reads_fastq` is
        # a sample's `read_masked` pass-set for the action_context `mask_idx`,
        # STREAMED from the data plane over a `read_masked` DoGet straight to gzip
        # FASTQ (miint's native COPY FORMAT FASTQ) — no bespoke DoAction, no
        # intermediate Parquet. Distinct from `reads` (raw) above — read-mask
        # workflows consume raw reads to CREATE a mask; long-read-assembly consumes
        # an EXISTING mask's pass-set to assemble.
        if _workflow_needs_staged_masked_reads(action.steps):
            if scope_target["kind"] != ScopeTargetKind.PREP_SAMPLE.value:
                raise _submission_bad_input(
                    "a workflow that assembles masked reads must be prep_sample-"
                    f"scoped; got {scope_target['kind']!r}"
                )
            mask_idx = bound.get(MASK_IDX_BINDING)
            if mask_idx is None:
                raise _submission_bad_input(
                    "a masked-reads workflow requires `mask_idx` in action_context"
                )
            bound.update(
                await _resolve_staged_masked_reads(
                    pool,
                    scope_target,
                    int(mask_idx),
                    data_plane_url=data_plane_url,
                    signing_key=signing_key,
                    workspace=workspace,
                )
            )

        # Sharded-index build roster (build-shard-index workflow): a
        # reference-scoped ticket carrying a non-NULL shard_id builds ONE shard.
        # Stage its feature roster (`shard_features`) + `shard_id` before the loop
        # so the build steps' Inputs resolve. Inside-try, so a Flight failure /
        # empty shard FAILs the ticket cleanly. Keyed off the ticket's shard_id
        # (not a step-input scan) — the whole ticket is a single-shard build.
        if (
            scope_target["kind"] == ScopeTargetKind.REFERENCE.value
            and work_ticket.get("shard_id") is not None
        ):
            bound.update(
                await _stage_shard_roster(
                    pool,
                    scope_target["reference_idx"],
                    work_ticket["shard_id"],
                    data_plane_url=data_plane_url,
                    signing_key=signing_key,
                    workspace=workspace,
                )
            )

        # Feature-table (OGU) genome-map staging (estimate-feature-table workflow):
        # when a step consumes `genome_map_path`, stage the reference's
        # feature->genome map and validate the cohort's alignment_sample
        # completeness + reference/scope consistency at SUBMIT. reference_idx is the
        # scope scalar (reference-scoped ticket). Same inside-try placement as the
        # resolvers above so a bad cohort / mismatched reference FAILs the ticket
        # cleanly instead of leaving it stuck in PROCESSING.
        if scope_target["kind"] == ScopeTargetKind.REFERENCE.value and _workflow_declares_input(
            action.steps, GENOME_MAP_PATH_BINDING
        ):
            bound.update(
                await _resolve_feature_table_bindings(
                    pool,
                    action_context=bound,
                    reference_idx=scope_target["reference_idx"],
                    workspace=workspace,
                )
            )

        # Read-mask identity: when a step threads `mask_idx` through its params
        # (the host_filter step), bind the mask_idx before the loop. Same
        # inside-try placement as the resolvers above so a failure lands in the
        # outer FAILED handler.
        #   - PREP_SAMPLE: mint the mask for this filtering config (deduped on the
        #     config hash) and persist it onto the ticket.
        #   - BLOCK: the mask was resolved AT PLAN TIME (the partition key) and
        #     stored on `work_ticket.mask_idx`; bind that value directly — never
        #     re-mint (a block spans many samples, has no single prep_sample the
        #     mint keys on, and the partition already fixed the identity).
        if _workflow_needs_mask(action.steps):
            if scope_target["kind"] == ScopeTargetKind.PREP_SAMPLE.value:
                adapter_path = bound.get(QC_ADAPTER_BINDING)
                # Host refs come from the SAME action_context values
                # `_resolve_host_filter_indexes` consumes for the applied filter,
                # so the minted mask_idx's params describe the filter that ran.
                # Absent → None (faithful "no host filtering").
                bound.update(
                    await _mint_read_mask(
                        pool,
                        action_id=action.action_id,
                        action_version=action.version,
                        prep_sample_idx=scope_target["prep_sample_idx"],
                        originator_principal_idx=work_ticket["originator_principal_idx"],
                        instrument_model=bound.get("instrument_model"),
                        adapter_parquet=Path(adapter_path) if adapter_path is not None else None,
                        host_rype_reference_idx=bound.get("host_rype_reference_idx"),
                        host_minimap2_reference_idx=bound.get("host_minimap2_reference_idx"),
                        # What actually distinguishes the five PacBio protocols:
                        # prep_protocol_idx is uniform across them. Both are gated
                        # on their `*_enabled` flag, so a stale key cannot shift
                        # the hash of a run that did not use the feature.
                        resolved_lima=_resolved_lima(bound),
                        resolved_syndna=_resolved_syndna(bound),
                    )
                )
                # Persist the minted mask_idx onto the ticket for durable
                # traceability (and a cheap shared-mask guard). Idempotent: a
                # re-mint on resume re-resolves to the same mask_idx via the
                # config-hash upsert and re-writes the same value here.
                await _persist_mask_idx(pool, work_ticket_idx, bound[MASK_IDX_BINDING])
            elif scope_target["kind"] == ScopeTargetKind.BLOCK.value:
                block_mask_idx = work_ticket["mask_idx"]
                if block_mask_idx is None:
                    raise _submission_bad_input(
                        "a block-scoped read-mask ticket must carry a pre-resolved "
                        "mask_idx (set at plan time); found NULL on the work_ticket"
                    )
                bound[MASK_IDX_BINDING] = block_mask_idx
            else:
                raise _submission_bad_input(
                    "a workflow that masks reads (threads mask_idx) must be "
                    f"prep_sample- or block-scoped; got {scope_target['kind']!r}"
                )

        # Processing identity: when a step threads `processing_idx` (the assembly
        # membership + load steps), mint the run's processing_idx before the loop —
        # the canonical-params hash of {workflow, version, result-affecting knobs
        # like the assembler}. Same inside-try placement so a mint failure lands in
        # the FAILED handler. Idempotent: a re-mint on resume re-resolves the same
        # id via the params-hash upsert.
        if _workflow_needs_processing(action.steps):
            # Single-source the assembler default from the action's context_schema
            # (the one result-affecting knob today) — the same default the hash and
            # the container use, so neither can drift from a re-declared literal.
            assembler_default = (
                action.context_schema.get("properties", {})
                .get(ASSEMBLER_BINDING, {})
                .get("default")
            )
            bound.update(
                await _mint_processing_idx(
                    pool,
                    action_id=action.action_id,
                    action_version=action.version,
                    bound=bound,
                    assembler_default=assembler_default,
                )
            )

        # Default-OFF anchor for the whole-reference rype_router build gate. The
        # `when: router_pending` router entries (sharded reference-add) must NOT
        # run unless the plan-shards arm sets router_pending True (N > 0). Because
        # an absent `when:` key defaults ON, seed it False here so the gate is OFF
        # in every no-router case — including when plan-shards is skipped
        # (shard_index explicitly false, so it never runs and never sets the key)
        # or a no-op. plan-shards overrides this to True on a real fan-out; on a
        # resume the completed plan-shards re-derives it from the durable shard
        # assignment (see `_reconstruct_completed_outputs`). Harmless for
        # workflows with no router entries (nothing reads the key).
        bound.setdefault(ROUTER_PENDING_BINDING, False)

        for index, entry in enumerate(action.steps):
            # Conditional gate (WorkflowStep/WorkflowAction.when): skip this
            # entry when its named action_context key is present and falsy
            # (default-ON — an absent key RUNS the step; read-mask's
            # context_schema `required:` is what stops that being a footgun).
            # Evaluated FIRST, before the fast-forward / target_status PATCH /
            # dispatch, so a gated-off entry neither advances status nor binds
            # outputs.
            #
            # `bound` is NOT just the persisted action_context: it is seeded from
            # it, then accumulates resolved paths, mask bindings, the minted
            # processing_idx, and `bound.update(outputs)` after EVERY completed
            # step. So a later entry's gate key can in principle be shadowed by an
            # earlier step's OUTPUT binding. Nothing does that today (every shipped
            # `when:` key comes from action_context), but do not assume otherwise
            # when naming an output. Resume is unaffected: action_context is
            # persisted, and a skipped entry `continue`s rather than being filtered
            # out, so step_index stays stable.
            if entry.when is not None and not bool(bound.get(entry.when, True)):
                _log.info(
                    "workflow %d: skipping entry %d (%s) — when=%r is falsy",
                    work_ticket_idx,
                    index,
                    entry.name,
                    entry.when,
                )
                continue

            completed = _completed_progress_row(progress, index)

            if completed is not None:
                # Fast-forward an entry a prior run already finished: rebuild
                # its outputs from disk without re-running it (an in-process
                # action: is not idempotent; a SLURM step's result is
                # re-verified from its manifest).
                #
                # Its status advance must be RE-APPLIED here, not skipped: a
                # `/run` redrive of a FAILED ticket resets a `failed` reference
                # to `pending` (the FSM's only legal exit from `failed`) while
                # KEEPING the completed step rows, rewinding the resource behind
                # the transitions those steps already made. Without re-walking
                # those edges the reference sits at `pending` while the first
                # not-yet-completed step tries to advance from where it left off
                # (e.g. `minting → loading`), which is illegal and dead-ends the
                # redrive. `_advance_completed_step_status` only ever moves the
                # resource FORWARD along a legal edge; on a normal
                # startup-recovery resume (resource not rewound) it is a no-op or
                # a rejected backward edge, both benign.
                if entry.target_status:
                    await _advance_completed_step_status(pool, scope_target, entry.target_status)
                bound.update(
                    await _reconstruct_completed_outputs(
                        entry,
                        completed,
                        workspace,
                        backend_client,
                        pool=pool,
                        work_ticket_idx=work_ticket_idx,
                        poll_interval_seconds=poll_interval_seconds,
                        scope_target=scope_target,
                    )
                )
                continue

            if entry.target_status:
                # Idempotent status advance, keyed off the resource's ACTUAL
                # status (single-CP-process contract makes that authoritative).
                # On a resume the PATCH may already have fired before the crash
                # — re-issuing the same transition raises IllegalStatusTransition
                # — so only PATCH when the resource isn't already there.
                if await _current_resource_status(pool, scope_target) != entry.target_status:
                    await _patch_resource_status(pool, scope_target, entry.target_status)

            outputs = await _run_entry_with_retry(
                pool=pool,
                work_ticket_idx=work_ticket_idx,
                index=index,
                entry=entry,
                action_ceiling=action.action_ceiling,
                mem_gb_override=mem_gb_override,
                bound=bound,
                workspace=workspace,
                scope_target=scope_target,
                backend_client=backend_client,
                signing_key=signing_key,
                data_plane_url=data_plane_url,
                max_retries=max_retries,
                poll_interval_seconds=poll_interval_seconds,
                prior_progress=progress,
                resume=resume,
                dispatch_cb=dispatch_cb,
            )
            bound.update(outputs)

        # Anything below this line is "finalize" stage — failures here
        # must classify as FINALIZE (with NULL step_name) to honour the
        # `work_ticket_failure_step_name_consistent` DB CHECK. The inner
        # try wraps the success path so a BackendFailure raised by
        # `_atomic_transition` (e.g. PROCESSING → COMPLETED couldn't fire
        # because state changed under us) carries the right stage.
        #
        # Three UPDATEs fire here as ONE Postgres transaction:
        #
        #   (1) qiita.upload  : ready  → consumed (every resolved upload)
        #   (2) qiita.reference: <prev> → action.success_status (e.g. active)
        #   (3) qiita.work_ticket: processing → completed
        #
        # The transaction binds all three so a mid-finalize failure can't
        # leave the system in a partial state — uploads consumed with a
        # PROCESSING ticket, or a COMPLETED ticket whose uploads are still
        # `ready`. Either everything advances or nothing does; the inner
        # except below reclassifies any raise as a FINALIZE failure and
        # the outer handler then transitions the ticket to FAILED with
        # the rollback already applied.
        try:
            async with pool.acquire() as conn, conn.transaction():
                await _consume_upload_handles(conn, upload_idxs=uploads_to_consume)
                # Skip the success_status patch when a sharded fan-out is in
                # progress — finalize-shard owns `indexing → active` once every
                # shard registers (see `_shard_fanout_owns_finalize`). Every
                # other case (unsharded ref-add, sharded-but-N=0, host-ref-add)
                # patches inline as before.
                if action.success_status and not await _shard_fanout_owns_finalize(
                    conn, scope_target, bound
                ):
                    await _patch_resource_status(conn, scope_target, action.success_status)
                await _atomic_transition(
                    conn,
                    work_ticket_idx,
                    expected=WorkTicketState.PROCESSING,
                    new=WorkTicketState.COMPLETED,
                )
        except Exception as exc:
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.FINALIZE,
                step_name=None,
                reason=f"{type(exc).__name__}: {exc!s}"[:2000],
            ) from exc

        _log.info("workflow %d completed", work_ticket_idx)
    except WorkflowAborted as exc:
        # The ticket went terminal in the DB out from under us — an operator
        # force-fail/cancel. The terminal state + failure surface were set
        # externally; do NOT re-transition or PATCH (that would clobber the
        # operator's failure surface). Just stop. Not re-raised: this is a
        # clean, expected unwind, not a task-level error for _run_and_log.
        _log.warning(
            "workflow %d aborted: ticket went %s out from under the runner; stopping",
            work_ticket_idx,
            exc.state,
        )
        # Clear our own in-place-retry marker so the now-terminal ticket doesn't
        # carry a stale "stuck since T" reason (which a monitoring query would
        # misread). Safe: transient_* is orthogonal to state/failure_*, and the
        # write is guarded to a no-op when nothing is set.
        await _clear_transient_retry(pool, work_ticket_idx)
        return
    except StepNoData as exc:
        # Terminal no-data outcome (an empty FASTQ well) — NOT a failure. The
        # step minted no identifiers and wrote no output; transition the ticket
        # PROCESSING → NO_DATA with NULL failure_* columns. Deliberately does
        # NOT PATCH action.failure_status (this isn't a failure) and does NOT
        # advance action.success_status (the resource didn't reach the success
        # state — no data was produced). Clear any in-place-retry marker so the
        # now-terminal ticket shows no stale "stuck retrying" reason.
        _log.info("workflow %d ended with no data: %s", work_ticket_idx, exc)
        await _transition_to_no_data(pool, work_ticket_idx)
        return
    except BackendFailure as exc:
        # Retry-loop already exhausted retries (transient) or this was a
        # permanent failure. The retry loop has not yet transitioned the
        # ticket — we own that transition here so failure_status PATCH
        # and the FAILED row insert happen together.
        _log.warning("workflow %d failed: %s", work_ticket_idx, exc)
        if action is not None and action.failure_status:
            try:
                await _patch_resource_status(pool, scope_target, action.failure_status)
            except Exception:
                _log.exception(
                    "best-effort failure_status PATCH for work_ticket %d failed",
                    work_ticket_idx,
                )
        await _transition_to_failed(
            pool,
            work_ticket_idx,
            failure_type=(FailureType.RETRIABLE if exc.transient else FailureType.PERMANENT),
            failure_stage=exc.stage,
            failure_step_name=exc.step_name,
            failure_reason=exc.reason,
        )
        raise
    except Exception as exc:
        # Plain Python from inside the step loop — LIBRARY primitive
        # raising untyped, or a programming bug. Treat as
        # UNKNOWN_PERMANENT (re-running won't change a deterministic
        # Python failure) and tag with the most recent step's name so
        # ops dashboards can join back to action metadata. Re-raise the
        # original exception unchanged so callers that asserted on its
        # type keep working.
        #
        # EXCEPT a transient CP-DB error (a `command_timeout` / brief connection
        # blip on one of the runner's OWN DB calls): that is NOT a deterministic
        # failure of the step's work — the ticket's state is fully recoverable
        # once PG is reachable — so record it RETRIABLE (not PERMANENT) so a
        # `/run` redrive re-attempts instead of the ticket being abandoned (the
        # healthy, often already-submitted SLURM job orphaned). The poll loop
        # already retries the common case (the force-fail check) in place; this
        # is the safety net for any other runner DB call.
        transient_db = _is_transient_db_error(exc)
        _log.exception("workflow %d failed (unwrapped exception)", work_ticket_idx)
        if action is not None and action.failure_status:
            try:
                await _patch_resource_status(pool, scope_target, action.failure_status)
            except Exception:
                _log.exception(
                    "best-effort failure_status PATCH for work_ticket %d failed",
                    work_ticket_idx,
                )
        # A failure before the step loop ran (index unbound) has no step to
        # attribute to — record it as SUBMISSION with a NULL step name, which
        # the failure-step-name CHECK requires. Only a failure from inside the
        # loop is a STEP_RUN (index is the entry that raised; it stays None until
        # the loop's first iteration binds it).
        _failed_index = index
        await _transition_to_failed(
            pool,
            work_ticket_idx,
            failure_type=FailureType.RETRIABLE if transient_db else FailureType.PERMANENT,
            failure_stage=(
                WorkTicketFailureStage.STEP_RUN
                if _failed_index is not None
                else WorkTicketFailureStage.SUBMISSION
            ),
            failure_step_name=_safe_entry_name(action, _failed_index),
            failure_reason=(
                ("transient control-plane DB error: " if transient_db else "")
                + f"{type(exc).__name__}: {exc!s}"
            )[:2000],
        )
        raise


async def _run_entry_with_retry(
    *,
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    index: int,
    entry: WorkflowStep | WorkflowAction,
    action_ceiling: ActionCeiling,
    mem_gb_override: int | None = None,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    backend_client: ComputeBackendClient,
    signing_key: bytes,
    data_plane_url: str,
    max_retries: int,
    poll_interval_seconds: float,
    prior_progress: list[step_progress.StepProgressRow],
    resume: bool = False,
    dispatch_cb: Callable[[int], Any] | None = None,
) -> dict[str, Any]:
    """Dispatch one workflow entry, with auto-retry on transient
    `BackendFailure`. Returns the entry's output map on success; raises
    `BackendFailure` on permanent failure or once retry budget is
    exhausted.

    `resume` flows down to `_dispatch_step` → `_adopt_or_submit`: on a resumed
    run a write-ahead 'submitting' row with no persisted job id may be an
    orphan from a crashed prior process, so the adopt path does a find-by-name
    lookup before re-submitting. On a fresh run the row was just written by
    this process, so that lookup is skipped.

    Retry semantics:
      * On `BackendFailure(transient=True)` and retry_count < max_retries:
        increment retry_count, transition PROCESSING → QUEUED → PROCESSING
        atomically, retry the same step. Earlier successful entries are
        not re-run — `bound` carries their outputs forward.
      * On an `OOM_KILLED` retry specifically, grow the step's memory floor
        (×`_OOM_MEMORY_GROWTH`, clamped to `action_ceiling.mem_gb`) before the
        next attempt — a step the scheduler OOM-killed will OOM again at the
        same size. Symmetrically, on a `TIMEOUT_BEFORE_START` retry (SLURM
        marks a job TIMEOUT when it exceeds walltime) grow the step's walltime
        floor (×`_TIMEOUT_WALLTIME_GROWTH`, clamped to
        `action_ceiling.walltime`) — a step that hit the wall needs more time,
        not a re-run at the same limit. Other transient kinds retry at the same
        allocation. Both escalated floors are process-local (not persisted): a
        CP restart re-attaches to the in-flight job and re-escalates (memory
        from the ticket's static override, walltime from the YAML baseline).
        Once a floor is already pinned at the ceiling, escalation can't grow it
        — a re-run would fail identically — so the OOM/TIMEOUT is reclassified
        as a permanent `RESOURCE_CEILING_EXHAUSTED` and fails the ticket
        immediately instead of consuming the remaining retry budget.
      * On permanent failure or retry_count >= max_retries: re-raise so
        the outer handler in `run_workflow` writes the failure_* columns
        and transitions to FAILED.

    The state churn (PROCESSING → QUEUED → PROCESSING) is observable to
    monitoring queries: a ticket bouncing through QUEUED indicates a
    retry attempt.
    """
    # Per-attempt workspace isolates retry artifacts from each other so a
    # failed attempt's stale outputs don't leak into the verifier (gate 5:
    # "every file under $QIITA_OUTPUT_PATH must be in manifest") on the
    # retry, and prior-attempt artifacts stay on disk for postmortem. The
    # entry-name segment also isolates concurrent steps in the same
    # workflow from each other. `attempt` is local to this invocation
    # rather than the work-ticket-wide retry_count: that counter skips
    # numbers between entries that retry, which would produce confusing
    # gaps like attempt-0 → attempt-3 for an entry that itself only
    # retried once.
    attempt = 0
    # Escalating memory floor: starts at the ticket's static override and is
    # raised on each OOM-killed retry (see the except arm below). Threaded into
    # every step dispatch in place of the static `mem_gb_override`.
    effective_mem_override = mem_gb_override
    # Escalating walltime floor: starts unset (use the YAML baseline) and is
    # raised on each TIMEOUT retry, clamped to the action ceiling. Threaded into
    # every step dispatch alongside the memory floor.
    effective_walltime_override: timedelta | None = None
    # Optional plan() sizing hint, fetched ONCE (native steps only) before the
    # loop — it depends only on inputs, not the attempt, and only ever
    # down-sizes below the baseline. Advisory: None (container step, or any
    # failure) means "use the YAML baseline". Escalation still grows from the
    # baseline, so a retry overrides the hint (see _resolve_baseline_for_step).
    plan_hint: StepPlanResponse | None = None
    if isinstance(entry, WorkflowStep):
        plan_hint = await _fetch_plan_hint(
            backend_client, entry, bound, scope_target, work_ticket_idx=work_ticket_idx
        )
    while True:
        attempt_workspace = workspace / entry.name / f"attempt-{attempt}"
        # Skip past a stale attempt dir to a fresh one. This fires only when an
        # attempt dir already exists on disk but NO start-of-run progress row
        # owns this (step_index, attempt) — i.e. a re-run after the row was
        # deliberately dropped (a /run redrive, or update-lane invalidating a
        # completed prep row). The orphaned dir holds the prior run's known-bad
        # output (read-only 0o440 files under 0550 dirs), which we can neither
        # reuse (it would trip the verifier or block the overwrite) nor delete —
        # a container step's output is owned by the SLURM job user, so the
        # control-plane process here can't unlink or chmod it. So advance to the
        # next attempt dir, which this process creates fresh. A row PRESENT means
        # resume-adoption owns the dir (see `_attempt_is_unowned`):
        # `_adopt_or_submit` must re-attach to its live job and reuse the
        # workspace, so we leave it and proceed.
        if attempt_workspace.exists() and _attempt_is_unowned(
            prior_progress, step_index=index, attempt=attempt
        ):
            _log.info(
                "work_ticket %d entry %r attempt %d: orphaned attempt dir from a "
                "dropped progress row; advancing to a fresh attempt dir",
                work_ticket_idx,
                entry.name,
                attempt,
            )
            attempt += 1
            continue
        attempt_workspace.mkdir(parents=True, exist_ok=True)
        try:
            if isinstance(entry, WorkflowStep):
                return await _dispatch_step(
                    backend_client,
                    entry,
                    bound,
                    attempt_workspace,
                    scope_target,
                    pool=pool,
                    work_ticket_idx=work_ticket_idx,
                    step_index=index,
                    attempt=attempt,
                    action_ceiling=action_ceiling,
                    mem_gb_override=effective_mem_override,
                    walltime_override=effective_walltime_override,
                    plan_hint=plan_hint,
                    poll_interval_seconds=poll_interval_seconds,
                    resume=resume,
                )
            if isinstance(entry, WorkflowAction):
                return await _dispatch_action(
                    pool,
                    entry,
                    bound,
                    attempt_workspace,
                    scope_target,
                    work_ticket_idx=work_ticket_idx,
                    step_index=index,
                    attempt=attempt,
                    signing_key=signing_key,
                    data_plane_url=data_plane_url,
                    dispatch_cb=dispatch_cb,
                )
            # WorkflowEntry is a closed union; the discriminator on
            # ActionDefinition guarantees one of the two arms above.
            raise TypeError(f"unexpected entry type at index {index}: {type(entry)!r}")
        except BackendFailure as exc:
            if not exc.transient:
                raise
            current_retry = await _retry_count(pool, work_ticket_idx)
            if current_retry >= max_retries:
                _log.warning(
                    "work_ticket %d step %r exhausted retries (%d/%d); failing",
                    work_ticket_idx,
                    entry.name,
                    current_retry,
                    max_retries,
                )
                raise
            # An OOM-killed step would OOM again at the same size, so grow its
            # memory floor (clamped to the action ceiling) before re-queuing.
            # Steps only — `action:` entries carry no baseline_resources and
            # never OOM-kill. Other transient kinds retry at the same size.
            if exc.kind is FailureKind.OOM_KILLED and isinstance(entry, WorkflowStep):
                grown = _escalated_mem_floor_after_oom(
                    entry=entry,
                    bound=bound,
                    action_ceiling=action_ceiling,
                    current_override=effective_mem_override,
                )
                if grown == effective_mem_override:
                    # The just-failed attempt already ran at the memory ceiling
                    # (escalation returns the floor unchanged once it is pinned
                    # there), so there is no larger size left to try — a re-run
                    # would OOM identically. Fail-fast with a permanent kind
                    # (see `_ceiling_exhausted_failure`) rather than burn the
                    # remaining retry budget on a guaranteed repeat.
                    _log.warning(
                        "work_ticket %d step %r OOM-killed at the action memory "
                        "ceiling (%d GB); escalation exhausted, failing instead "
                        "of retrying at the same size",
                        work_ticket_idx,
                        entry.name,
                        action_ceiling.mem_gb,
                    )
                    raise _ceiling_exhausted_failure(
                        exc,
                        event="OOM-killed",
                        axis="memory",
                        ceiling=f"{action_ceiling.mem_gb} GB",
                    ) from exc
                effective_mem_override = grown
            # A timed-out step needs more wall to finish, not a re-run at the same
            # limit; grow its walltime floor (clamped to the action ceiling) before
            # re-queuing. Steps only — `action:` entries carry no baseline_resources.
            if exc.kind is FailureKind.TIMEOUT_BEFORE_START and isinstance(entry, WorkflowStep):
                grown_walltime = _escalated_walltime_after_timeout(
                    entry=entry,
                    bound=bound,
                    action_ceiling=action_ceiling,
                    current_override=effective_walltime_override,
                )
                if grown_walltime == effective_walltime_override:
                    # Already pinned at the walltime ceiling — a re-run would time
                    # out identically. Same fail-fast reclassification as the OOM
                    # arm above (see its comment for the rationale).
                    _log.warning(
                        "work_ticket %d step %r timed out at the action walltime "
                        "ceiling (%s); escalation exhausted, failing instead of "
                        "retrying at the same limit",
                        work_ticket_idx,
                        entry.name,
                        action_ceiling.walltime,
                    )
                    raise _ceiling_exhausted_failure(
                        exc,
                        event="timed out",
                        axis="walltime",
                        ceiling=str(action_ceiling.walltime),
                    ) from exc
                effective_walltime_override = grown_walltime
            _log.warning(
                "work_ticket %d step %r transient failure (%s); retrying %d/%d "
                "(mem_gb floor=%s, walltime floor=%s)",
                work_ticket_idx,
                entry.name,
                exc.kind.value,
                current_retry + 1,
                max_retries,
                effective_mem_override,
                effective_walltime_override,
            )
            attempt += 1
            await _bump_retry_and_requeue(pool, work_ticket_idx)
            await _atomic_transition(
                pool,
                work_ticket_idx,
                expected=WorkTicketState.QUEUED,
                new=WorkTicketState.PROCESSING,
            )
