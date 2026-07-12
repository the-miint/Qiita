"""Runner compute-step dispatch helpers (submit / poll / result, resource sizing)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.actions import (
    ActionCeiling,
    FlatBaselineResources,
    WorkflowStep,
)
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import (
    ComputeTarget,
    FoundJobWire,
    ReferenceStatus,
    ScopeTargetKind,
    StepBaselineResources,
    StepHandleWire,
    StepPlanResponse,
    StepStatus,
    StepStatusWire,
    WorkTicketFailureStage,
)

import qiita_control_plane.runner as _runner_pkg

from .. import step_progress
from ..actions.reference import (
    IllegalStatusTransition,
)
from ._base import (
    _INFRA_UNREACHABLE_KINDS,
    _clear_transient_retry,
    _infra_retry_wait,
    _log,
    _raise_if_ticket_terminal,
)

# =============================================================================
# Dispatch helpers
# =============================================================================


def _build_scope_target(work_ticket: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct a {kind, ...idx fields} dict matching qiita_common's
    ScopeTarget tagged-union shape from the work_ticket row."""
    kind = work_ticket["scope_target_kind"]
    if kind == ScopeTargetKind.REFERENCE.value:
        return {
            "kind": ScopeTargetKind.REFERENCE.value,
            "reference_idx": work_ticket["reference_idx"],
        }
    if kind == ScopeTargetKind.STUDY_PREP.value:
        return {
            "kind": ScopeTargetKind.STUDY_PREP.value,
            "study_idx": work_ticket["study_idx"],
            "prep_idx": work_ticket["prep_idx"],
        }
    if kind == ScopeTargetKind.PREP_SAMPLE.value:
        return {
            "kind": ScopeTargetKind.PREP_SAMPLE.value,
            "prep_sample_idx": work_ticket["prep_sample_idx"],
        }
    if kind == ScopeTargetKind.SEQUENCED_POOL.value:
        return {
            "kind": ScopeTargetKind.SEQUENCED_POOL.value,
            "sequenced_pool_idx": work_ticket["sequenced_pool_idx"],
            "sequencing_run_idx": work_ticket["sequencing_run_idx"],
        }
    if kind == ScopeTargetKind.BLOCK.value:
        return {
            "kind": ScopeTargetKind.BLOCK.value,
            "block_idx": work_ticket["block_idx"],
        }
    raise RuntimeError(f"unknown scope_target_kind: {kind!r}")


async def _patch_resource_status(
    pool: asyncpg.Pool | asyncpg.Connection,
    scope_target: dict[str, Any],
    target_status: str,
) -> None:
    """Drive the appropriate resource-status transition for the scope_target.
    Today only `reference` is wired."""
    if scope_target["kind"] == ScopeTargetKind.REFERENCE.value:
        await _runner_pkg.transition_reference_status(
            pool, scope_target["reference_idx"], ReferenceStatus(target_status)
        )
        return
    raise NotImplementedError(
        f"status transition for scope_target.kind={scope_target['kind']!r} not yet wired"
    )


async def _advance_completed_step_status(
    pool: asyncpg.Pool | asyncpg.Connection,
    scope_target: dict[str, Any],
    target_status: str,
) -> None:
    """Re-apply a fast-forwarded (already-completed) step's ``target_status``,
    advancing the scope_target resource along its FSM only when it is currently
    *behind* this step.

    Needed because a ``/run`` redrive of a FAILED ticket resets a ``failed``
    reference to ``pending`` (its only legal exit from ``failed``) while keeping
    the completed step rows. The runner then fast-forwards those completed
    steps; if it skipped their status advances, a multi-transition reference
    would stay at ``pending`` and the first re-run step's transition (e.g.
    ``minting → loading``) would raise IllegalStatusTransition and dead-end the
    redrive. Re-walking each completed step's edge restores the resource to the
    status the next live step expects.

    Two benign no-advance cases, both on a normal startup-recovery resume where
    the resource was never rewound:

    * already AT this status — nothing to do (the ``==`` short-circuit);
    * already PAST this status — the backward edge is illegal and
      ``transition_reference_status`` raises IllegalStatusTransition, which we
      swallow (the resource is correctly ahead).

    ReferenceNotFound is deliberately NOT swallowed — a missing scope row under a
    live ticket is a referential-integrity fault, not a benign skip.
    """
    if await _current_resource_status(pool, scope_target) == target_status:
        return
    try:
        await _patch_resource_status(pool, scope_target, target_status)
    except IllegalStatusTransition:
        # Resource is already past this step (not rewound) — leave it ahead.
        pass


async def _current_resource_status(pool: asyncpg.Pool, scope_target: dict[str, Any]) -> str | None:
    """The scope_target resource's current status, used to make the per-entry
    `target_status` PATCH idempotent on a resume / redrive (only PATCH when the
    resource isn't already there). Returns None for scope kinds that carry no
    status (only `reference` is wired today) — those entries never declare a
    `target_status`, so the caller's `actual != target` check still does the
    right thing."""
    if scope_target["kind"] == ScopeTargetKind.REFERENCE.value:
        return await pool.fetchval(
            "SELECT status FROM qiita.reference WHERE reference_idx = $1",
            scope_target["reference_idx"],
        )
    return None


def _resolve_baseline_for_step(
    *,
    entry: WorkflowStep,
    bound: dict[str, Any],
    action_ceiling: ActionCeiling,
    mem_gb_override: int | None = None,
    walltime_override: timedelta | None = None,
    plan_hint: StepPlanResponse | None = None,
) -> FlatBaselineResources:
    """Resolve a step's ``baseline_resources`` to a concrete
    ``FlatBaselineResources`` and clamp against ``action_ceiling``.

    ``plan_hint`` (a native step's optional ``plan()`` sizing hint, fetched once
    before the retry loop) is applied FIRST, as a raise-NEVER *down-size*: for
    each axis the hint sets AND that has escalation headroom (``baseline <
    ceiling``), ``resolved.X = min(resolved.X, hint.X)``. It only ever LOWERS a
    step below its YAML baseline (a small input needs less); a hint above the
    baseline is a no-op, and an axis with ``baseline == ceiling`` is left alone
    (no headroom to recover — see the inline comment). Applied BEFORE the
    raise-only override floors below so escalation always wins on a retry: the
    escalated floor is seeded from the YAML baseline (>= any down-sized value),
    so a retry after an OOM/TIMEOUT restores at least the baseline regardless of
    the hint.

    ``mem_gb_override`` (the ticket's optional per-run resource bump) raises the
    resolved memory *floor*: ``mem_gb = max(resolved.mem_gb, mem_gb_override)``.
    It only ever increases memory — a smaller override leaves a step the YAML
    sized higher untouched. The bump is applied before the ceiling assertion
    below, so an override above ``action_ceiling.mem_gb`` is rejected here too
    (defense in depth; the submission route already 422s it).

    ``walltime_override`` is the symmetric raise-only *walltime* floor — the
    escalating override raised on each TIMEOUT retry by
    ``_escalated_walltime_after_timeout`` — applied the same way and bounded by
    the same ceiling assertion.

    Two paths, picked by which population the YAML declared:

    * Flat: cpu/mem_gb/walltime/gpu are taken verbatim from the YAML.
    * Lookup: ``from_step_output`` names an upstream step's output file
      already bound under that name; the file's stripped UTF-8 contents
      are the key; ``profiles[key]`` gives the resolved resources.

    Both populations end in a ``FlatBaselineResources`` that gets
    validated against the action's ceiling. Any non-conformance —
    missing lookup file, key not in profiles, resolved value exceeds
    ceiling — raises ``BackendFailure(CONTRACT_VIOLATION, STEP_RUN)``
    naming the step.
    """
    br = entry.baseline_resources
    if br.from_step_output is not None:
        # Lookup population. `from_step_output` is the name of an upstream
        # step's output. The runner records every step's outputs into
        # `bound` under their YAML-declared names, so the path is just a
        # bound-key lookup. `profiles` is guaranteed non-empty by
        # BaselineResources's model_validator.
        lookup_path = bound.get(br.from_step_output)
        if lookup_path is None:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=entry.name,
                reason=(
                    f"baseline_resources.from_step_output={br.from_step_output!r}"
                    " is not bound — no upstream step produced an output by that name"
                ),
            )
        try:
            key = Path(lookup_path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=entry.name,
                reason=(
                    f"baseline_resources lookup: failed to read {lookup_path}:"
                    f" {type(exc).__name__}: {exc}"
                ),
            )
        # profiles is guaranteed non-None and non-empty by the
        # BaselineResources model_validator.
        assert br.profiles is not None
        if key not in br.profiles:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=entry.name,
                reason=(
                    f"baseline_resources lookup: instrument {key!r} has no"
                    f" resource profile; known profiles: {sorted(br.profiles)}"
                ),
            )
        resolved = br.profiles[key]
    else:
        # Flat population. model_validator guarantees all three required
        # fields are populated; the asserts narrow the Optional types
        # without runtime cost on the happy path.
        assert br.cpu is not None
        assert br.mem_gb is not None
        assert br.walltime is not None
        resolved = FlatBaselineResources(
            cpu=br.cpu, mem_gb=br.mem_gb, walltime=br.walltime, gpu=br.gpu
        )

    # plan() down-size (raise-NEVER), applied BEFORE the raise-only floors so an
    # OOM/TIMEOUT retry (whose floor is seeded from the YAML baseline) always
    # restores at least the baseline. Each axis the hint sets lowers the
    # resolved value; a hint >= baseline is a no-op. gpu is deliberately not a
    # plan() axis (see JobResourcePlan).
    #
    # Down-size ONLY an axis with escalation HEADROOM (baseline < ceiling). If
    # baseline == ceiling there is no room for escalation to grow, so a
    # down-sized attempt that OOMs/TIMEOUTs would be misread as
    # RESOURCE_CEILING_EXHAUSTED (the escalation helper, re-resolving from the
    # baseline, returns the floor unchanged) and fail the ticket without ever
    # running at the baseline. Leaving a no-headroom axis at its baseline keeps
    # the "escalation can always recover to >= baseline" invariant the
    # saturation check depends on. The chained `hint < baseline < ceiling`
    # expresses both "hint lowers it" and "there is headroom to recover".
    if plan_hint is not None:
        updates: dict[str, Any] = {}
        if plan_hint.cpu is not None and plan_hint.cpu < resolved.cpu < action_ceiling.cpu:
            updates["cpu"] = plan_hint.cpu
        if (
            plan_hint.mem_gb is not None
            and plan_hint.mem_gb < resolved.mem_gb < action_ceiling.mem_gb
        ):
            updates["mem_gb"] = plan_hint.mem_gb
        if plan_hint.walltime_seconds is not None:
            hint_walltime = timedelta(seconds=plan_hint.walltime_seconds)
            if hint_walltime < resolved.walltime < action_ceiling.walltime:
                updates["walltime"] = hint_walltime
        if updates:
            resolved = resolved.model_copy(update=updates)

    # Per-run memory floor (raise-only): never lowers a step the YAML sized
    # higher than the override.
    if mem_gb_override is not None and mem_gb_override > resolved.mem_gb:
        resolved = resolved.model_copy(update={"mem_gb": mem_gb_override})

    # Per-run walltime floor (raise-only): the escalating override raised on each
    # TIMEOUT retry. Like the memory floor it only ever increases walltime; its
    # producer already clamps to the ceiling, so the assertion below is defense in
    # depth.
    if walltime_override is not None and walltime_override > resolved.walltime:
        resolved = resolved.model_copy(update={"walltime": walltime_override})

    _assert_within_ceiling(entry=entry, resolved=resolved, action_ceiling=action_ceiling)
    return resolved


def _ceiling_exhausted_failure(
    cause: BackendFailure, *, event: str, axis: str, ceiling: str
) -> BackendFailure:
    """Build the permanent ``RESOURCE_CEILING_EXHAUSTED`` failure the retry loop
    raises when a step's OOM/timeout escalation is already pinned at the action
    ceiling — a re-run would fail identically, so fail-fast instead of burning
    the retry budget. ``event`` is the human verb (``"OOM-killed"`` /
    ``"timed out"``), ``axis`` the resource word (``"memory"`` / ``"walltime"``),
    ``ceiling`` its rendered value (e.g. ``"32 GB"`` / ``"4:00:00"``).

    Single home for both escalation arms so a future third resource axis can't
    copy-paste a drifting third reason string. Reuses the cause's stage /
    step_name (rather than reconstructing from ``entry.name``) so the new
    failure satisfies the same STEP_RUN ⇔ step_name DB CHECK the original
    already did, with no risk of a stage/step_name desync.
    """
    return BackendFailure(
        kind=FailureKind.RESOURCE_CEILING_EXHAUSTED,
        stage=cause.stage,
        step_name=cause.step_name,
        reason=(
            f"step {event} at the action {axis} ceiling ({ceiling}); {axis} "
            f"escalation exhausted, not retrying. Raise the action {axis} ceiling "
            f"or shrink the input. Original: {cause.reason}"
        ),
    )


# Growth factor applied to a step's resolved memory on each OOM_KILLED retry.
# A step the scheduler OOM-killed will OOM again at the same size, so doubling
# — clamped to the action's mem ceiling — is the only retry that can fit.
_OOM_MEMORY_GROWTH = 2


def _escalated_mem_floor_after_oom(
    *,
    entry: WorkflowStep,
    bound: dict[str, Any],
    action_ceiling: ActionCeiling,
    current_override: int | None,
) -> int | None:
    """Memory floor (``mem_gb``) for the next attempt after an OOM, or
    ``current_override`` unchanged once the resolved allocation has reached the
    action ceiling.

    Escalation always grows from the YAML baseline: this re-resolves WITHOUT the
    ``plan()`` hint (so the floor climbs from ``max(baseline.mem_gb,
    current_override)``, grown by ``_OOM_MEMORY_GROWTH`` and clamped to
    ``action_ceiling.mem_gb``), and the result is threaded back into
    ``_dispatch_step`` as ``mem_gb_override``. When a ``plan()`` hint down-sized
    the just-failed attempt below the baseline, that attempt actually ran at the
    (smaller) hint, not the baseline — the first escalation deliberately jumps
    to the grown-from-baseline value (skipping the optimistic down-size), which
    the headroom guard in ``_resolve_baseline_for_step`` guarantees exceeds the
    baseline. Growing from the baseline, not from the down-sized size, is what
    lets escalation recover a step whose ``plan()`` estimate was too low.
    """
    resolved = _resolve_baseline_for_step(
        entry=entry,
        bound=bound,
        action_ceiling=action_ceiling,
        mem_gb_override=current_override,
    )
    grown = min(resolved.mem_gb * _OOM_MEMORY_GROWTH, action_ceiling.mem_gb)
    # No headroom left (already at the ceiling): return `current_override`
    # unchanged. The caller treats an unchanged floor as the saturation signal
    # — there is no larger size to retry at, so it fails the ticket permanently
    # rather than re-running at the same (guaranteed-to-OOM) size.
    return grown if grown > resolved.mem_gb else current_override


# Growth factor applied to a step's resolved walltime on each TIMEOUT retry.
# A step that hit the wall needs more time to finish, not a re-run at the same
# limit, so doubling — clamped to the action's walltime ceiling — gives the next
# attempt a real chance. Mirrors `_OOM_MEMORY_GROWTH` for memory.
_TIMEOUT_WALLTIME_GROWTH = 2


def _escalated_walltime_after_timeout(
    *,
    entry: WorkflowStep,
    bound: dict[str, Any],
    action_ceiling: ActionCeiling,
    current_override: timedelta | None,
) -> timedelta | None:
    """Walltime floor for the next attempt after a TIMEOUT, or
    ``current_override`` unchanged once the resolved allocation has reached the
    action ceiling.

    Escalation always grows from the YAML baseline: this re-resolves WITHOUT the
    ``plan()`` hint (so the floor climbs from ``max(baseline.walltime,
    current_override)``, grown by ``_TIMEOUT_WALLTIME_GROWTH`` and clamped to
    ``action_ceiling.walltime``), and the result is threaded back into
    ``_dispatch_step`` as ``walltime_override``. When a ``plan()`` hint
    down-sized the just-failed attempt below the baseline (e.g. qc's small-input
    walltime), that attempt ran at the smaller hint, not the baseline — the
    first escalation deliberately jumps to the grown-from-baseline value, which
    the headroom guard in ``_resolve_baseline_for_step`` guarantees exceeds the
    baseline. The exact mirror of ``_escalated_mem_floor_after_oom`` for
    walltime, minus the static per-run seed (there is no
    ``resource_override.walltime``): escalation always starts from the YAML
    baseline.
    """
    resolved = _resolve_baseline_for_step(
        entry=entry,
        bound=bound,
        action_ceiling=action_ceiling,
        walltime_override=current_override,
    )
    grown = min(resolved.walltime * _TIMEOUT_WALLTIME_GROWTH, action_ceiling.walltime)
    # No headroom left (already at the ceiling): return `current_override`
    # unchanged. The caller treats an unchanged floor as the saturation signal
    # — there is no longer limit to retry at, so it fails the ticket permanently
    # rather than re-running at the same (guaranteed-to-time-out) limit.
    return grown if grown > resolved.walltime else current_override


def _assert_within_ceiling(
    *,
    entry: WorkflowStep,
    resolved: FlatBaselineResources,
    action_ceiling: ActionCeiling,
) -> None:
    """Reject a resolved baseline that exceeds any ceiling axis.

    Ceiling is always flat (a single upper bound), so the comparison is
    field-by-field. gpu is treated symmetrically: a step that resolves
    to gpu>0 against a ceiling of gpu=0 is rejected. Reasons name the
    offending axis so a YAML author can fix it without reading code.
    """
    if resolved.cpu > action_ceiling.cpu:
        raise BackendFailure(
            kind=FailureKind.CONTRACT_VIOLATION,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=entry.name,
            reason=(
                f"resolved baseline cpu={resolved.cpu} exceeds"
                f" action_ceiling.cpu={action_ceiling.cpu}"
            ),
        )
    if resolved.mem_gb > action_ceiling.mem_gb:
        raise BackendFailure(
            kind=FailureKind.CONTRACT_VIOLATION,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=entry.name,
            reason=(
                f"resolved baseline mem_gb={resolved.mem_gb} exceeds"
                f" action_ceiling.mem_gb={action_ceiling.mem_gb}"
            ),
        )
    if resolved.walltime > action_ceiling.walltime:
        raise BackendFailure(
            kind=FailureKind.CONTRACT_VIOLATION,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=entry.name,
            reason=(
                f"resolved baseline walltime={resolved.walltime} exceeds"
                f" action_ceiling.walltime={action_ceiling.walltime}"
            ),
        )
    if resolved.gpu > action_ceiling.gpu:
        raise BackendFailure(
            kind=FailureKind.CONTRACT_VIOLATION,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=entry.name,
            reason=(
                f"resolved baseline gpu={resolved.gpu} exceeds"
                f" action_ceiling.gpu={action_ceiling.gpu}"
            ),
        )


def _bind_step_inputs(entry: WorkflowStep, bound: dict[str, Any]) -> dict[str, Any]:
    """Build a step's name -> value input map from the binding map `bound`.

    `inputs` / `optional_inputs` are host paths (Path-coerced); scalar build
    params (`WorkflowStep.params`, keyed action_context_key -> Inputs field) are
    NOT host paths, so they are merged un-Path-coerced as strings — the wire
    carries `inputs: dict[str, str]` and the native job's Pydantic `Inputs`
    model re-coerces each string to its declared type (e.g. "35" -> int).
    Native steps only: `_resolve_input_binds` (which would treat a value as a
    bind-mount path) is container-only, so a scalar here is never mistaken for
    one. Shared by `_dispatch_step` (submit) and `_fetch_plan_hint` (plan) so
    the two send identical inputs."""
    inputs: dict[str, Any] = {name: Path(bound[name]) for name in entry.inputs}
    inputs.update({name: Path(bound[name]) for name in entry.optional_inputs if name in bound})
    inputs.update(
        {field: str(bound[ctx_key]) for ctx_key, field in entry.params.items() if ctx_key in bound}
    )
    return inputs


async def _fetch_plan_hint(
    backend_client: ComputeBackendClient,
    entry: WorkflowStep,
    bound: dict[str, Any],
    scope_target: dict[str, Any],
    *,
    work_ticket_idx: int,
) -> StepPlanResponse | None:
    """Fetch a native step's optional `plan()` resource hint, ONCE, before its
    retry loop. Returns None for a container step (no `plan()`) or on ANY
    failure.

    ADVISORY by contract: the hint only ever LOWERS a step below its YAML
    baseline (`_resolve_baseline_for_step`), and a missing hint means "use the
    baseline", so a failure here must never fail the ticket. We therefore
    swallow every exception — an unreachable orchestrator, a classified
    BackendFailure from a broken module, a malformed response — and log it, so
    dispatch proceeds on the baseline exactly as it did before `plan()`
    existed."""
    if entry.module is None:
        return None
    try:
        return await backend_client.plan_step(
            step_name=entry.name,
            inputs=_bind_step_inputs(entry, bound),
            scope_target=scope_target,
            work_ticket_idx=work_ticket_idx,
            module=entry.module,
        )
    except Exception as exc:  # noqa: BLE001 - advisory: any failure -> baseline
        _log.warning(
            "work_ticket %d step %r plan() fetch failed (%s: %s); using YAML baseline",
            work_ticket_idx,
            entry.name,
            type(exc).__name__,
            exc,
        )
        return None


async def _dispatch_step(
    backend_client: ComputeBackendClient,
    entry: WorkflowStep,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    *,
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    action_ceiling: ActionCeiling,
    mem_gb_override: int | None = None,
    walltime_override: timedelta | None = None,
    plan_hint: StepPlanResponse | None = None,
    poll_interval_seconds: float,
    resume: bool = False,
) -> dict[str, Any]:
    """Dispatch one `step:` entry: write-ahead intent, submit to the
    orchestrator, then poll status until terminal and fetch the verified
    result — never holding the CP→CO connection open for the job's full
    duration (the fix for the 600s-timeout bug). Records per-attempt
    progress in `qiita.work_ticket_step` throughout so a CP restart can
    re-attach.

    Failure handling:
      * An infra-unreachable BackendFailure (CO / slurmrestd down) inside the
        submit / poll / result helpers is retried in place — it never
        advances the attempt or fails the ticket.
      * Any other BackendFailure is a genuine step failure: this attempt's
        progress row is marked failed and the exception propagates to
        `_run_entry_with_retry`, which decides retry-as-new-attempt
        (transient kinds) vs. fail (permanent / exhausted).

    `optional_inputs` flow through if present in the binding map; missing
    ones are simply omitted. `action_ceiling` clamps the resolved baseline;
    the lookup population reads an upstream step's named output file and
    selects the matching profile, the flat population uses the YAML values."""
    inputs = _bind_step_inputs(entry, bound)
    resolved = _resolve_baseline_for_step(
        entry=entry,
        bound=bound,
        action_ceiling=action_ceiling,
        mem_gb_override=mem_gb_override,
        walltime_override=walltime_override,
        plan_hint=plan_hint,
    )
    baseline = StepBaselineResources(
        cpu=resolved.cpu,
        mem_gb=resolved.mem_gb,
        walltime_seconds=int(resolved.walltime.total_seconds()),
        gpu=resolved.gpu,
    )

    # Write-ahead intent BEFORE submit. compute_target is the production
    # assumption (slurm) carrying the deterministic job name; if the backend
    # turns out to be the in-process LocalBackend, record_synchronous_completion
    # below corrects it. record_submitting is idempotent on re-entry, so a
    # recovery resuming this exact attempt doesn't reset the row.
    job_name = f"qiita-wt{work_ticket_idx}-{entry.name}-a{attempt}"
    await step_progress.record_submitting(
        pool,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        step_name=entry.name,
        compute_target=ComputeTarget.SLURM,
        job_name=job_name,
    )

    handle = await _adopt_or_submit(
        backend_client,
        pool,
        entry=entry,
        inputs=inputs,
        workspace=workspace,
        scope_target=scope_target,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        baseline=baseline,
        poll_interval_seconds=poll_interval_seconds,
        resume=resume,
    )

    # Synchronous backend (LocalBackend ran the module in-process and handed
    # back terminal outputs): skip polling, correct the row's compute_target,
    # and use the outputs directly. Invariant (StepHandleWire): terminal_outputs
    # non-None ⇒ non-empty.
    if handle.terminal_outputs is not None:
        await step_progress.record_synchronous_completion(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            compute_target=handle.compute_target,
        )
        raw_outputs = {k: Path(v) for k, v in handle.terminal_outputs.items()}
        return {name: raw_outputs[name] for name in entry.outputs}

    # Asynchronous (SLURM) path: the job id is already persisted (by
    # _adopt_or_submit, on a fresh submit). Poll to terminal, fetch the
    # verified result.
    try:
        status = await _poll_until_terminal(
            backend_client,
            handle,
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            poll_interval_seconds=poll_interval_seconds,
        )
        raw_outputs = await _result_with_infra_retry(
            backend_client,
            handle,
            status,
            pool=pool,
            work_ticket_idx=work_ticket_idx,
            poll_interval_seconds=poll_interval_seconds,
        )
    except BackendFailure as exc:
        # Genuine step failure (infra-unreachable kinds loop forever inside
        # the helpers and never reach here). Mark this attempt failed; the
        # retry loop decides retry-as-new-attempt vs. fail.
        await _best_effort_record_failed(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=exc.kind.value,
            failure_reason=exc.reason[:2000],
        )
        raise
    await step_progress.record_completed(
        pool, work_ticket_idx=work_ticket_idx, step_index=step_index, attempt=attempt
    )
    # Convention: the orchestrator's output dict keys match the YAML's
    # `outputs:` names exactly. A mismatch is a workflow authoring error and
    # surfaces here as a KeyError.
    return {name: Path(raw_outputs[name]) for name in entry.outputs}


async def _adopt_or_submit(
    backend_client: ComputeBackendClient,
    pool: asyncpg.Pool,
    *,
    entry: WorkflowStep,
    inputs: dict[str, Path],
    workspace: Path,
    scope_target: dict[str, Any],
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    baseline: StepBaselineResources,
    poll_interval_seconds: float,
    resume: bool = False,
) -> StepHandleWire:
    """Submit the step, or adopt a job already recorded for this exact
    `(work_ticket_idx, step_index, attempt)`.

    Idempotency: if a prior dispatch of this same attempt already persisted a
    `slurm_job_id` (a re-entry, or restart recovery resuming this attempt),
    do NOT submit again — reconstruct the handle from the row and resume
    polling. `output_path` / `logs_path` are deterministic from the
    per-attempt workspace (the SLURM backend uses `<workspace>/output` and
    `<workspace>/logs`), so the progress row need not store them. This is the
    guard against duplicate concurrent jobs.

    On a fresh SLURM submit the returned job id is persisted here
    (`record_submitted`) before the handle is returned, so the caller's poll
    loop and any later re-entry both see it. A synchronous (local) handle
    carries no job id and is returned as-is for the caller to finalize. A
    fresh submit retries in place on an infra-unreachable failure (CO down),
    honouring the never-fail-on-CO-outage rule.

    The write-ahead 'submitting' window (find-by-name closer): if a prior
    process crashed between a successful `submit_step` and its
    `record_submitted`, its progress row is left in `submitting` with no job
    id but WITH the deterministic `job_name`. On a resume (`resume=True`) we
    look that job up by name before re-submitting — if slurmrestd still has it
    we adopt the orphan (persist its id, reconstruct the handle) instead of
    launching a duplicate at the same `attempt-N/output` dir. This lookup runs
    only on resume: a fresh dispatch just wrote this `submitting` row itself,
    so there is no orphan to find and the (cluster-wide `GET /slurm/jobs`)
    lookup would be wasted. If the lookup can't reach slurmrestd it retries in
    place (recovery never fails on a CO/slurmrestd blip); if slurmrestd has
    purged the job (no match), we fall through to a fresh submit."""
    rows = await step_progress.load_step_progress(pool, work_ticket_idx)
    existing = next((r for r in rows if r.step_index == step_index and r.attempt == attempt), None)
    if existing is not None and existing.slurm_job_id is not None:
        _log.info(
            "work_ticket %d step %r attempt %d already submitted as job %s; adopting",
            work_ticket_idx,
            entry.name,
            attempt,
            existing.slurm_job_id,
        )
        return StepHandleWire(
            compute_target=ComputeTarget.SLURM,
            step_name=entry.name,
            slurm_job_id=existing.slurm_job_id,
            job_name=existing.job_name,
            output_path=str(workspace / "output"),
            logs_path=str(workspace / "logs"),
        )

    # Resume-only orphan adoption: a 'submitting' row with no job id but a
    # recorded job_name may be a job a crashed prior process launched but
    # never persisted. Find it by name before re-submitting.
    if (
        resume
        and existing is not None
        and existing.slurm_job_id is None
        and existing.job_name is not None
    ):
        found = await _find_existing_job(
            backend_client,
            existing.job_name,
            pool=pool,
            work_ticket_idx=work_ticket_idx,
            poll_interval_seconds=poll_interval_seconds,
        )
        if found is not None:
            _log.warning(
                "work_ticket %d step %r attempt %d: adopting orphaned SLURM job %s found by"
                " name %r (its id was never persisted); not re-submitting",
                work_ticket_idx,
                entry.name,
                attempt,
                found.slurm_job_id,
                existing.job_name,
            )
            await step_progress.record_submitted(
                pool,
                work_ticket_idx=work_ticket_idx,
                step_index=step_index,
                attempt=attempt,
                slurm_job_id=found.slurm_job_id,
            )
            return StepHandleWire(
                compute_target=ComputeTarget.SLURM,
                step_name=entry.name,
                slurm_job_id=found.slurm_job_id,
                job_name=existing.job_name,
                output_path=str(workspace / "output"),
                logs_path=str(workspace / "logs"),
            )
    n = 0
    while True:
        try:
            handle = await backend_client.submit_step(
                step_name=entry.name,
                inputs=inputs,
                workspace=workspace,
                scope_target=scope_target,
                work_ticket_idx=work_ticket_idx,
                attempt=attempt,
                container=entry.container,
                module=entry.module,
                entrypoint=entry.entrypoint,
                baseline_resources=baseline,
                derived_inputs=entry.derived_inputs,
            )
            break
        except BackendFailure as exc:
            if exc.kind not in _INFRA_UNREACHABLE_KINDS:
                raise
            _log.warning(
                "work_ticket %d step %r submit unreachable (%s); retry %d",
                work_ticket_idx,
                entry.name,
                exc.kind.value,
                n + 1,
            )
            n = await _infra_retry_wait(
                pool,
                work_ticket_idx,
                what="submit",
                kind=exc.kind,
                n=n,
                base=poll_interval_seconds,
            )
    if n:
        # Submit got through after an outage — clear the stuck marker.
        await _clear_transient_retry(pool, work_ticket_idx)
    # SLURM async submit — persist the job id before returning so the poll
    # loop and any restart re-entry resolve to the same job. A synchronous
    # (local) handle has no job id; the caller's terminal_outputs branch
    # corrects the row's compute_target instead.
    if handle.terminal_outputs is None:
        await step_progress.record_submitted(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            slurm_job_id=handle.slurm_job_id,
        )
    return handle


async def _find_existing_job(
    backend_client: ComputeBackendClient,
    job_name: str,
    *,
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    poll_interval_seconds: float,
) -> FoundJobWire | None:
    """Look up a live SLURM job by its deterministic name for orphan
    adoption, returning the single match or None.

    Infra-unreachable failures (CO / slurmrestd down) retry in place — a
    recovery sweep must not fail a ticket because the orchestrator is briefly
    unreachable (the never-fail-on-outage rule) — with capped backoff, and
    bailing if the ticket is force-failed mid-outage. A non-infra
    BackendFailure (slurmrestd 4xx => 'job list unreadable') is swallowed to
    None: if we genuinely can't read the job list, fall back to a fresh submit
    (the gap's pre-closer behavior) rather than failing recovery. More than one
    match for a deterministic name shouldn't happen; if it does, adopt the
    first and log — the extras keep running but the duplicate-prevention goal
    is already met for this attempt."""
    n = 0
    while True:
        try:
            jobs = await backend_client.find_jobs_by_name(job_name)
            break
        except BackendFailure as exc:
            if exc.kind in _INFRA_UNREACHABLE_KINDS:
                n = await _infra_retry_wait(
                    pool,
                    work_ticket_idx,
                    what="find-by-name",
                    kind=exc.kind,
                    n=n,
                    base=poll_interval_seconds,
                )
                continue
            _log.warning(
                "find_jobs_by_name(%r) failed (%s); falling back to a fresh submit",
                job_name,
                exc.kind.value,
            )
            return None
    if n:
        await _clear_transient_retry(pool, work_ticket_idx)
    if not jobs:
        return None
    if len(jobs) > 1:
        # Should be impossible: the name encodes work_ticket_idx (a DB PK) +
        # step + attempt, and a single CP process submits at most once per
        # attempt — so a duplicate means a cluster that reused the name or a
        # double-submit from a prior bug. We adopt+poll the first and DO NOT
        # cancel the rest (no CP→CO cancel route exists): the un-adopted jobs
        # keep running and write to the SAME `attempt-N/output` dir, so they
        # can race/clobber this attempt's output. Loud ERROR so it's caught —
        # cancel the strays by hand (scancel) if this ever fires.
        _log.error(
            "find_jobs_by_name(%r) matched %d jobs (expected 1); adopting job %s and"
            " polling it, but the other %d are LEFT RUNNING and will race on %s's"
            " shared output dir — scancel them by hand",
            job_name,
            len(jobs),
            jobs[0].slurm_job_id,
            len(jobs) - 1,
            job_name,
        )
    return jobs[0]


async def _poll_until_terminal(
    backend_client: ComputeBackendClient,
    handle: StepHandleWire,
    pool: asyncpg.Pool,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    poll_interval_seconds: float,
) -> StepStatusWire:
    """Poll `status_step` until the step is terminal (COMPLETED / FAILED),
    returning the terminal status. Sleeps `poll_interval_seconds` between
    reads — the CP, not the orchestrator, owns this loop now, so there is no
    600s client-timeout ceiling.

    An infra-unreachable BackendFailure is retried in place: the loop keeps
    going straight through a CO / slurmrestd outage (the never-fail-on-outage
    rule).

    A non-infra BackendFailure from `status_step` means the job is no longer
    readable from slurmrestd — i.e. it was **purged** (aged out of the
    controller's memory after a long outage; `status_step` only raises
    "couldn't read status", never "the job failed"). The job's true outcome
    then lives only on the shared filesystem, so we hand back a synthesized
    COMPLETED status: the caller's `result_step` runs verify + parse against
    the output manifest, which decides it — a valid manifest yields the
    outputs (completed), a missing / broken one raises CONTRACT_VIOLATION
    (failed). This is the filesystem tiebreaker. Records the running
    transition once, the first time the job is observed on a node."""
    recorded_running = False
    n = 0
    while True:
        try:
            status = await backend_client.status_step(handle)
        except BackendFailure as exc:
            if exc.kind in _INFRA_UNREACHABLE_KINDS:
                n = await _infra_retry_wait(
                    pool,
                    work_ticket_idx,
                    what="status",
                    kind=exc.kind,
                    n=n,
                    base=poll_interval_seconds,
                )
                continue
            # Purged job → defer to the on-disk manifest via result_step.
            _log.warning(
                "work_ticket %d step %d job unreadable (%s); deciding outcome"
                " from the output manifest on shared scratch",
                work_ticket_idx,
                step_index,
                exc.kind.value,
            )
            return StepStatusWire(
                status=StepStatus.COMPLETED,
                raw_state="PURGED",
                reason=f"slurmrestd no longer has the job ({exc.kind.value}); "
                "deciding from filesystem",
            )
        if n:
            # status_step got through after an outage — clear the marker.
            await _clear_transient_retry(pool, work_ticket_idx)
            n = 0
        if status.status in (StepStatus.COMPLETED, StepStatus.FAILED):
            return status
        if status.status is StepStatus.RUNNING and not recorded_running:
            await step_progress.record_running(
                pool,
                work_ticket_idx=work_ticket_idx,
                step_index=step_index,
                attempt=attempt,
            )
            recorded_running = True
        # Normal poll cadence (a healthy in-flight job): flat, not backed off.
        # Still re-check for an operator force-fail so a long-running job's
        # poll loop is escapable, not just the outage retry.
        await _raise_if_ticket_terminal(pool, work_ticket_idx)
        await asyncio.sleep(poll_interval_seconds)


async def _result_with_infra_retry(
    backend_client: ComputeBackendClient,
    handle: StepHandleWire,
    status: StepStatusWire,
    *,
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    poll_interval_seconds: float,
) -> dict[str, Path]:
    """Fetch the terminal step's verified result, retrying in place on an
    infra-unreachable failure (CO down) with capped backoff + a force-fail bail.
    A genuine step failure — the job ended FAILED, so `result_step`
    raises the classified BackendFailure — propagates to the caller, which
    records it and lets the retry loop decide."""
    n = 0
    while True:
        try:
            result = await backend_client.result_step(handle, status)
        except BackendFailure as exc:
            if exc.kind not in _INFRA_UNREACHABLE_KINDS:
                raise
            n = await _infra_retry_wait(
                pool,
                work_ticket_idx,
                what="result",
                kind=exc.kind,
                n=n,
                base=poll_interval_seconds,
            )
            continue
        if n:
            await _clear_transient_retry(pool, work_ticket_idx)
        return result


async def _best_effort_record_failed(
    pool: asyncpg.Pool,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    failure_kind: str,
    failure_reason: str,
) -> None:
    """Mark this attempt's progress row failed, but never let a DB blip on
    that write mask the real failure. The caller re-raises the original
    exception (preserving its FailureKind for the retry loop's
    transient-vs-permanent decision); a lost progress row is logged, not
    fatal — same best-effort discipline `run_workflow` uses for the
    failure_status PATCH."""
    try:
        await step_progress.record_failed(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=failure_kind,
            failure_reason=failure_reason,
        )
    except Exception:
        _log.exception(
            "best-effort record_failed for work_ticket %d step %d attempt %d failed",
            work_ticket_idx,
            step_index,
            attempt,
        )
