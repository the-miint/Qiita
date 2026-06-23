"""Workflow runner — walks an action's `steps` list for one work ticket.

`step:` entries dispatch to the orchestrator via ComputeBackendClient
(HTTP). `action:` entries dispatch to LIBRARY in-process — no HTTP hop.
Status transitions declared in YAML are PATCHed before each entry that
declares one. Workflow-level success/failure transitions wrap the run.

Lives in the control plane: direct DB access for work_ticket / action /
reference rows is legitimate here. The orchestrator is reduced to its
SLURM-driver role behind `POST /step/*`.

Workspace contract: each entry runs against a per-attempt subdir
`<work_ticket_workspace_root>/<work_ticket_idx>/<entry-name>/attempt-<N>/`
minted by `_run_entry_with_retry`. The nesting gives two properties at
once — retries land in fresh dirs (the verifier's "every file in
$output_path must be in manifest" gate stays clean), and prior attempts
persist on disk for postmortem. Entries see each other's outputs via
the runner's binding map, which carries absolute paths forward so
consumers don't need to know the producer's attempt number.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.actions import (
    ActionCeiling,
    ActionDefinition,
    FlatBaselineResources,
    WorkflowAction,
    WorkflowStep,
)
from qiita_common.api_paths import LibraryPrimitive, compute_upload_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import (
    HOST_FILTER_INDEX_TYPE_MINIMAP2,
    HOST_FILTER_INDEX_TYPE_RYPE,
    ComputeTarget,
    FailureType,
    FoundJobWire,
    ReferenceStatus,
    ScopeTargetKind,
    StepBaselineResources,
    StepHandleWire,
    StepProgressState,
    StepStatus,
    StepStatusWire,
    UploadStatus,
    WorkTicketFailureStage,
    WorkTicketState,
)

from . import step_progress
from .actions.library import LIBRARY
from .actions.reference import (
    IllegalStatusTransition,
    ReferenceNotFound,
    transition_reference_status,
)
from .auth.tickets import sign_ticket

_log = logging.getLogger(__name__)

# Suffix that marks an action_context key as a DoPut upload handle. The
# runner resolves every `{prefix}_upload_idx` entry to the canonical
# staging path under `{prefix}_path` before invoking workflow steps.
_UPLOAD_IDX_SUFFIX = "_upload_idx"
_PATH_SUFFIX = "_path"

# How long the runner sleeps between status polls of a submitted step. The
# control plane owns the poll loop now (the orchestrator is a stateless
# pass-through), so this is the cadence at which a long-running SLURM job is
# observed. Mirrors the orchestrator's prior internal poll interval. A
# constant, not an env var: deploys don't need to tune it, and Phase-7's
# deploy note explicitly expects no new required env var.
_STEP_POLL_INTERVAL_SECONDS = 10.0

# FailureKinds that mean "couldn't reach the orchestrator / slurmrestd to
# read status" — an infra-reachability hiccup, NEVER a statement that the
# step itself failed. When status_step / result_step / submit_step raise one
# of these, the runner sleeps and retries the SAME call (same attempt, same
# deterministic job name) instead of failing the ticket or resubmitting. This
# is what makes a CP→CO outage during a deploy safe: the poll loop keeps
# looping until the orchestrator comes back, capped only by the SLURM job's
# own walltime (the job going terminal ends the loop). Every other
# BackendFailure from the trio is a real step failure that flows to the
# retry/fail path.
_INFRA_UNREACHABLE_KINDS = frozenset(
    {FailureKind.SLURMRESTD_UNREACHABLE, FailureKind.ORCHESTRATOR_UNREACHABLE}
)

# Cap for the in-place infra-unreachable retry backoff. The base is the
# caller's poll interval; each successive retry doubles it up to this cap, so a
# long CO/slurmrestd outage backs off instead of hammering a flat cadence —
# while still re-checking often enough (≤ cap) to notice an operator
# force-fail. base=0 (the test cadence) stays 0, so suites never sleep.
_INFRA_RETRY_BACKOFF_CAP_SECONDS = 60.0

# Work-ticket states the runner does NOT own. Once a ticket reaches one of
# these out from under a running workflow — an operator
# `qiita-admin ticket force-fail` flips it to FAILED — the runner must stop:
# the in-place infra-retry/poll loops re-check this each iteration and bail via
# WorkflowAborted instead of retrying forever against a ticket that is no
# longer theirs.
_TERMINAL_WORK_TICKET_STATES = frozenset(
    {WorkTicketState.COMPLETED.value, WorkTicketState.FAILED.value}
)


class WorkflowAborted(Exception):
    """Unwind a running workflow whose ticket went terminal in the DB out from
    under the runner (operator force-fail/cancel). NOT a failure: the terminal
    state + failure surface were set externally, so run_workflow catches this,
    logs, and returns WITHOUT re-transitioning the ticket or PATCHing the
    resource (which would clobber the operator's failure surface)."""

    def __init__(self, work_ticket_idx: int, state: str) -> None:
        super().__init__(f"work_ticket {work_ticket_idx} went terminal ({state}); aborting run")
        self.work_ticket_idx = work_ticket_idx
        self.state = state


def _infra_backoff_delay(
    n: int, *, base: float, cap: float = _INFRA_RETRY_BACKOFF_CAP_SECONDS
) -> float:
    """Delay before the (n+1)-th in-place infra-retry: ``base * 2**n`` capped
    at ``cap`` (n starts at 0). Pure — no clock, no I/O. base=0 → always 0.

    The exponent is clamped (n is bounded well past where the result saturates
    at ``cap``) so a very long outage — n in the hundreds — can't push
    ``2.0**n`` to float ``inf`` and turn ``0.0 * inf`` into ``nan`` (which would
    crash ``asyncio.sleep``). 2**32 already dwarfs any sane base/cap."""
    return min(cap, base * (2.0 ** min(n, 32)))


async def _raise_if_ticket_terminal(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Bail (WorkflowAborted) out of an in-place retry/poll loop if the ticket
    has gone terminal in the DB — an operator force-fail/cancel — so the runner
    stops working a ticket it no longer owns. A cheap one-column read run
    once per loop iteration.

    Like every runner DB call this assumes Postgres is reachable: a PG outage
    here raises a (non-BackendFailure) asyncpg error that unwinds the run via
    run_workflow's catch-all, same as any other runner DB write. The
    never-fail-on-outage invariant covers the *compute* backend (CO/slurmrestd),
    not the control plane's own database."""
    state = await pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if state in _TERMINAL_WORK_TICKET_STATES:
        raise WorkflowAborted(work_ticket_idx, state)


async def _note_transient_retry(pool: asyncpg.Pool, work_ticket_idx: int, reason: str) -> None:
    """Surface *why* the runner is retrying in place, for the status routes:
    refresh `transient_reason` and stamp `transient_since` on the first
    retry of this episode (COALESCE preserves the original start time)."""
    await pool.execute(
        "UPDATE qiita.work_ticket"
        " SET transient_reason = $2, transient_since = COALESCE(transient_since, now())"
        " WHERE work_ticket_idx = $1",
        work_ticket_idx,
        reason,
    )


async def _clear_transient_retry(
    executor: asyncpg.Pool | asyncpg.Connection, work_ticket_idx: int
) -> None:
    """Clear the in-place-retry marker once the runner makes progress (a
    backend call succeeds) or the ticket fails. Guarded so it's a no-op write
    when nothing is set."""
    await executor.execute(
        "UPDATE qiita.work_ticket"
        " SET transient_reason = NULL, transient_since = NULL"
        " WHERE work_ticket_idx = $1 AND transient_reason IS NOT NULL",
        work_ticket_idx,
    )


async def _infra_retry_wait(
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    *,
    what: str,
    kind: FailureKind,
    n: int,
    base: float,
) -> int:
    """One iteration of the in-place infra-unreachable retry: bail if the
    ticket went terminal, surface the reason, then sleep with capped
    backoff. Returns the next backoff counter."""
    await _raise_if_ticket_terminal(pool, work_ticket_idx)
    await _note_transient_retry(pool, work_ticket_idx, f"{what}: {kind.value}")
    await asyncio.sleep(_infra_backoff_delay(n, base=base))
    return n + 1


async def run_workflow(
    work_ticket_idx: int,
    pool: asyncpg.Pool,
    backend_client: ComputeBackendClient,
    *,
    hmac_secret: bytes,
    data_plane_url: str,
    work_ticket_workspace_root: Path,
    upload_staging_root: Path,
    default_adapter_reference_idx: int | None = None,
    poll_interval_seconds: float = _STEP_POLL_INTERVAL_SECONDS,
    resume: bool = False,
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
    # <= the action ceiling at submission). Read once here and threaded to
    # every step's dispatch so a CP restart re-attaches with the same override.
    _override = work_ticket.get("resource_override")
    mem_gb_override = _override.get("mem_gb") if isinstance(_override, dict) else None
    if not resume and work_ticket["state"] != WorkTicketState.PENDING.value:
        raise RuntimeError(
            f"work_ticket {work_ticket_idx} is in state {work_ticket['state']!r}, "
            f"must be {WorkTicketState.PENDING.value!r}; manual recovery required"
        )

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

    workspace = work_ticket_workspace_root / str(work_ticket_idx)
    workspace.mkdir(parents=True, exist_ok=True)

    # Per-entry progress from any prior run. Empty on a first dispatch; on a
    # resume (or a /run redrive) it carries the COMPLETED rows the loop
    # fast-forwards. Loaded once — this run's own writes don't feed back in.
    progress = await step_progress.load_step_progress(pool, work_ticket_idx)

    bound: dict[str, Any] = dict(work_ticket["action_context"] or {})
    scope_target = _build_scope_target(work_ticket)
    max_retries: int = work_ticket["max_retries"]

    _log.info(
        "running workflow %s/%s for work_ticket %d (max_retries=%d)",
        action.action_id,
        action.version,
        work_ticket_idx,
        max_retries,
    )

    uploads_to_consume: list[int] = []

    try:
        # Resolve `*_upload_idx` keys to filesystem paths BEFORE the step
        # loop runs. A failure here (unknown / unready / wrong-owner /
        # missing-staged-file) raises a typed BackendFailure that the
        # outer `except BackendFailure` block translates into a FAILED
        # work_ticket — same path a step-level bad input would take.
        # The consume-list is held until workflow completion so a
        # mid-step failure leaves its uploads in `ready` for the
        # operator to redrive against the same handles.
        #
        # Inside the try block (not above the PROCESSING transition)
        # because a raise here MUST land in the outer FAILED-transition
        # handler — without that, the ticket sticks in PROCESSING.
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

        # QC adapter materialization: when any step needs `adapter_parquet` (the
        # qc step), DoGet the configured artifact_sequence_set reference's
        # sequences and stage them as a local Parquet in the ticket workspace.
        # Same pre-loop, inside-try placement as host-filter resolution so a
        # failure (unconfigured / non-active / empty adapter set) lands in the
        # outer FAILED handler rather than leaving the ticket stuck in PROCESSING.
        if _workflow_needs_adapters(action.steps):
            bound.update(
                await _resolve_qc_adapters(
                    pool,
                    default_adapter_reference_idx=default_adapter_reference_idx,
                    data_plane_url=data_plane_url,
                    hmac_secret=hmac_secret,
                    workspace=workspace,
                )
            )

        for index, entry in enumerate(action.steps):
            # Conditional gate (WorkflowStep/WorkflowAction.when): skip this
            # entry when its named action_context key is present and falsy
            # (default-ON — an absent key runs). Evaluated FIRST, before the
            # fast-forward / target_status PATCH / dispatch, so a gated-off
            # entry neither advances status nor binds outputs. `bound` is
            # seeded from the persisted action_context, so the decision is
            # deterministic and resume-safe; skipping via `continue` (never by
            # filtering action.steps) keeps the integer step_index that
            # `_completed_progress_row` matches on stable across a resume.
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
                hmac_secret=hmac_secret,
                data_plane_url=data_plane_url,
                max_retries=max_retries,
                poll_interval_seconds=poll_interval_seconds,
                resume=resume,
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
                if action.success_status:
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
    except BackendFailure as exc:
        # Retry-loop already exhausted retries (transient) or this was a
        # permanent failure. The retry loop has not yet transitioned the
        # ticket — we own that transition here so failure_status PATCH
        # and the FAILED row insert happen together.
        _log.warning("workflow %d failed: %s", work_ticket_idx, exc)
        if action.failure_status:
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
        _log.exception("workflow %d failed (unwrapped exception)", work_ticket_idx)
        if action.failure_status:
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
            failure_type=FailureType.PERMANENT,
            failure_stage=WorkTicketFailureStage.STEP_RUN,
            failure_step_name=_safe_entry_name(action, locals().get("index")),
            failure_reason=f"{type(exc).__name__}: {exc!s}"[:2000],
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
    hmac_secret: bytes,
    data_plane_url: str,
    max_retries: int,
    poll_interval_seconds: float,
    resume: bool = False,
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
    while True:
        attempt_workspace = workspace / entry.name / f"attempt-{attempt}"
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
                    mem_gb_override=mem_gb_override,
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
                    hmac_secret=hmac_secret,
                    data_plane_url=data_plane_url,
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
            _log.warning(
                "work_ticket %d step %r transient failure (%s); retrying %d/%d",
                work_ticket_idx,
                entry.name,
                exc.kind.value,
                current_retry + 1,
                max_retries,
            )
            attempt += 1
            await _bump_retry_and_requeue(pool, work_ticket_idx)
            await _atomic_transition(
                pool,
                work_ticket_idx,
                expected=WorkTicketState.QUEUED,
                new=WorkTicketState.PROCESSING,
            )


# =============================================================================
# DB access helpers
# =============================================================================


# LEFT JOIN qiita.sequenced_pool so the SEQUENCED_POOL scope_target arm
# can carry the parent sequencing_run_idx — _build_scope_target reads it
# alongside sequenced_pool_idx to produce the {kind: sequenced_pool, ...}
# dict the orchestrator's SCOPE_SCALARS_BY_KIND injection consumes.
_WORK_TICKET_COLS = (
    "wt.work_ticket_idx, wt.action_id, wt.action_version, wt.originator_principal_idx, "
    "wt.scope_target_kind, wt.study_idx, wt.prep_idx, wt.reference_idx, "
    "wt.prep_sample_idx, wt.sequenced_pool_idx, sp.sequencing_run_idx, "
    "wt.action_context, wt.state, wt.retry_count, wt.max_retries, "
    "wt.resource_override"
)
_WORK_TICKET_FROM = (
    " FROM qiita.work_ticket wt LEFT JOIN qiita.sequenced_pool sp ON sp.idx = wt.sequenced_pool_idx"
)

_ACTION_COLS = (
    "action_id, version, target_kind, description, "
    "scopes, audience, context_schema, steps, "
    "cpu_ceiling, mem_ceiling_gb, walltime_ceiling, gpu_ceiling, "
    "success_status, failure_status"
)


async def _fetch_work_ticket(pool: asyncpg.Pool, work_ticket_idx: int) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"SELECT {_WORK_TICKET_COLS}{_WORK_TICKET_FROM} WHERE wt.work_ticket_idx = $1",
        work_ticket_idx,
    )
    if row is None:
        raise RuntimeError(f"work_ticket {work_ticket_idx} not found")
    out = dict(row)
    # action_context is JSONB — asyncpg returns it as a JSON string by
    # default; parse it eagerly so the runner can index into it.
    if out.get("action_context") is not None and isinstance(out["action_context"], str):
        out["action_context"] = json.loads(out["action_context"])
    # resource_override is JSONB (nullable) — decode the same way so the runner
    # can read its mem_gb. NULL stays None (no override).
    if out.get("resource_override") is not None and isinstance(out["resource_override"], str):
        out["resource_override"] = json.loads(out["resource_override"])
    return out


async def _fetch_action(
    pool: asyncpg.Pool, action_id: str, version: str
) -> ActionDefinition | None:
    """Reconstruct an ActionDefinition from qiita.action — filtered by
    enabled=true so a manually disabled action is unreachable to the
    runner without an explicit operator un-disable."""
    row = await pool.fetchrow(
        f"SELECT {_ACTION_COLS} FROM qiita.action "
        "WHERE action_id = $1 AND version = $2 AND enabled = true",
        action_id,
        version,
    )
    if row is None:
        return None
    return ActionDefinition.model_validate(
        {
            "action_id": row["action_id"],
            "version": row["version"],
            "target_kind": row["target_kind"],
            "description": row["description"],
            "scopes": list(row["scopes"]),
            "audience": json.loads(row["audience"]),
            "context_schema": json.loads(row["context_schema"]),
            "steps": json.loads(row["steps"]),
            "action_ceiling": {
                "cpu": row["cpu_ceiling"],
                "mem_gb": row["mem_ceiling_gb"],
                "walltime": row["walltime_ceiling"],
                "gpu": row["gpu_ceiling"],
            },
            "success_status": row["success_status"],
            "failure_status": row["failure_status"],
        }
    )


async def _atomic_transition(
    pool: asyncpg.Pool | asyncpg.Connection,
    work_ticket_idx: int,
    *,
    expected: WorkTicketState,
    new: WorkTicketState,
) -> None:
    """UPDATE state with a TOCTOU-safe WHERE clause. Raises if the row
    isn't in the expected state — surfacing a stuck PROCESSING ticket
    instead of silently overwriting it.

    Accepts either a pool (auto-acquires a transient connection) or a
    live Connection (so the finalize block can fire this UPDATE inside
    the same transaction as `_consume_upload_handles` and the status
    PATCH)."""
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket SET state = $1::qiita.work_ticket_state "
        "WHERE work_ticket_idx = $2 AND state = $3::qiita.work_ticket_state "
        "RETURNING work_ticket_idx",
        new.value,
        work_ticket_idx,
        expected.value,
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not transition work_ticket {work_ticket_idx} "
            f"from {expected.value!r} to {new.value!r}; actual state {actual!r}"
        )


async def _transition_to_processing_for_resume(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Move a ticket to PROCESSING from any non-terminal state, for startup
    recovery re-driving an in-flight ticket. Unlike `_atomic_transition`
    (single expected state), this accepts PENDING / QUEUED / PROCESSING so
    recovery doesn't need to know exactly where the crash left it; a
    PROCESSING → PROCESSING is a harmless no-op. Raises on a terminal ticket
    — recovery should never be handed one."""
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket SET state = $1::qiita.work_ticket_state"
        " WHERE work_ticket_idx = $2 AND state = ANY($3::qiita.work_ticket_state[])"
        " RETURNING work_ticket_idx",
        WorkTicketState.PROCESSING.value,
        work_ticket_idx,
        [
            WorkTicketState.PENDING.value,
            WorkTicketState.QUEUED.value,
            WorkTicketState.PROCESSING.value,
        ],
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not resume work_ticket {work_ticket_idx} to processing: "
            f"expected non-terminal, got {actual!r}"
        )


async def _retry_count(pool: asyncpg.Pool, work_ticket_idx: int) -> int:
    """Read the current retry_count. Used by the retry loop to compare
    against max_retries before requeuing."""
    return await pool.fetchval(
        "SELECT retry_count FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )


async def _bump_retry_and_requeue(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Atomic PROCESSING → QUEUED transition with retry_count + 1. Single
    UPDATE so monitoring queries always see a coherent (state, count)
    pair; an observer that reads after this commit sees QUEUED with the
    bumped count, never PROCESSING with the bumped count or QUEUED with
    the old count."""
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket"
        " SET state = $1::qiita.work_ticket_state,"
        "     retry_count = retry_count + 1"
        " WHERE work_ticket_idx = $2"
        "   AND state = $3::qiita.work_ticket_state"
        " RETURNING work_ticket_idx",
        WorkTicketState.QUEUED.value,
        work_ticket_idx,
        WorkTicketState.PROCESSING.value,
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not bump retry on work_ticket {work_ticket_idx}: "
            f"expected processing, got {actual!r}"
        )


async def _transition_to_failed(
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    *,
    failure_type: FailureType,
    failure_stage: WorkTicketFailureStage,
    failure_step_name: str | None,
    failure_reason: str,
) -> None:
    """Atomic transition into FAILED with all four failure_* columns
    populated in one UPDATE. The DB's `work_ticket_failure_consistent`
    CHECK enforces all-or-nothing; doing it in one statement keeps that
    invariant honoured.

    Accepts transition from any non-terminal state — the runner may be
    in PROCESSING (most common) or QUEUED (if a retry's QUEUED → PROCESSING
    transition raced with shutdown). Refuses already-terminal tickets so
    a buggy second call doesn't overwrite a COMPLETED state."""
    updated = await pool.fetchval(
        "UPDATE qiita.work_ticket"
        " SET state = $1::qiita.work_ticket_state,"
        "     failure_type = $2::qiita.failure_type,"
        "     failure_stage = $3::qiita.work_ticket_failure_stage,"
        "     failure_step_name = $4,"
        "     failure_reason = $5,"
        # A genuine failure ends any in-place-retry episode: clear the
        # transient marker so the FAILED ticket shows only its real failure
        # surface, not a stale "stuck retrying" reason.
        "     transient_reason = NULL,"
        "     transient_since = NULL"
        " WHERE work_ticket_idx = $6"
        "   AND state = ANY($7::qiita.work_ticket_state[])"
        " RETURNING work_ticket_idx",
        WorkTicketState.FAILED.value,
        failure_type.value,
        failure_stage.value,
        failure_step_name,
        failure_reason,
        work_ticket_idx,
        [
            WorkTicketState.PENDING.value,
            WorkTicketState.QUEUED.value,
            WorkTicketState.PROCESSING.value,
        ],
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not mark work_ticket {work_ticket_idx} FAILED: "
            f"expected non-terminal, got {actual!r}"
        )


def _safe_entry_name(action: ActionDefinition, index: int | None) -> str | None:
    """Best-effort lookup of the entry name at `index`. Returns None if
    the index is out of range (e.g. action.steps is empty so the loop
    never iterated). When the loop body has executed at least once,
    `index` is the most recent entry — the natural name to record on
    failure."""
    if index is None:
        return None
    if 0 <= index < len(action.steps):
        return action.steps[index].name
    return None


# =============================================================================
# Upload-handle resolution
# =============================================================================
#
# Source-of-truth for the upload domain — what a `qiita.upload` row means
# and the consume contract — lives in db/migrations/20260521000000_upload.sql.
# These helpers tie that domain to the workflow runner: pre-step resolution
# (find the file the step will read) and post-success consumption (mark the
# slot terminal).


def _submission_bad_input(reason: str) -> BackendFailure:
    """A BAD_INPUT failure attributed to workflow SUBMISSION (not any one step).

    The shared shape every pre-step resolution pass raises — `_resolve_upload_handles`
    and `_resolve_host_filter_indexes` — so the outer `except BackendFailure`
    block in `run_workflow` translates each into a FAILED work_ticket
    identically (step_name=None ⇒ attributed to the workflow's submission)."""
    return BackendFailure(
        kind=FailureKind.BAD_INPUT,
        stage=WorkTicketFailureStage.SUBMISSION,
        step_name=None,
        reason=reason,
    )


async def _resolve_upload_handles(
    pool: asyncpg.Pool,
    *,
    action_context: dict[str, Any],
    originator_principal_idx: int,
    upload_staging_root: Path,
) -> tuple[dict[str, Path], list[int]]:
    """For every `{prefix}_upload_idx` key in `action_context`, resolve
    to a `{prefix}_path` Path binding pointing at the canonical staging
    file (`{staging_root}/uploads/{idx}/upload.parquet`).

    Validates four invariants per upload, in this order:
      1. The upload row exists.
      2. status='ready' — the DoPut completed and /done was called.
      3. created_by_idx == originator_principal_idx — uploaders can only
         feed their own work tickets. Matches the same per-row ownership
         gate `POST /upload/{idx}/done` and `GET /upload/{idx}` enforce
         (see routes/upload.py); the runner double-checks here because
         the originator on a work_ticket isn't necessarily the same as
         the principal that created each referenced upload.
      4. The on-disk file exists at the canonical staging path. Catches
         a CP↔DP layout drift (or a deleted scratch) before the workflow
         hands the path to a step that would 404 on it.

    Any violation raises a typed `BackendFailure(BAD_INPUT)` at
    stage=SUBMISSION — the work_ticket goes to FAILED with the failure
    attributed to the workflow's submission, not any one step.
    Non-`_upload_idx` keys (e.g. legacy `fasta_path` literals) flow
    through untouched in the caller's binding map.

    Returns `(resolved_paths, upload_idxs_to_consume)`. The consume list
    is held until workflow success; mid-flight failures leave the
    referenced uploads in `ready` so the operator can decide whether to
    redrive against the same handles.
    """

    # Shared SUBMISSION-attributed BAD_INPUT shape (see _submission_bad_input).
    _bad = _submission_bad_input

    # First pass: validate keys + value shape, collect (key, prefix, upload_idx).
    pending: list[tuple[str, str, int]] = []
    for key, value in sorted(action_context.items()):
        if not key.endswith(_UPLOAD_IDX_SUFFIX):
            continue
        # Bare suffix as the full key — `"_upload_idx": N` — would
        # resolve to `_path`, clobbering any unrelated binding under the
        # same name. Reject the empty-prefix case so the convention's
        # `{prefix}_path` injection is always meaningful.
        prefix = key.removesuffix(_UPLOAD_IDX_SUFFIX)
        if not prefix:
            raise _bad(
                f"action_context key {key!r} has no name prefix before "
                f"{_UPLOAD_IDX_SUFFIX!r}; use e.g. fasta_upload_idx, not _upload_idx"
            )
        if not isinstance(value, int) or value <= 0:
            raise _bad(f"action_context.{key} must be a positive integer, got {value!r}")
        pending.append((key, prefix, value))

    if not pending:
        return {}, []

    # Second pass: single batched fetch keyed by upload_idx → row.
    upload_idxs = [p[2] for p in pending]
    rows = await pool.fetch(
        "SELECT upload_idx, status, created_by_idx FROM qiita.upload"
        " WHERE upload_idx = ANY($1::bigint[])",
        upload_idxs,
    )
    by_idx = {r["upload_idx"]: r for r in rows}

    resolved: dict[str, Path] = {}
    to_consume: list[int] = []
    for key, prefix, upload_idx in pending:
        row = by_idx.get(upload_idx)
        if row is None:
            raise _bad(f"action_context.{key}={upload_idx} references unknown upload")
        if row["status"] != UploadStatus.READY.value:
            raise _bad(
                f"action_context.{key}={upload_idx} expected status "
                f"{UploadStatus.READY.value!r}, got {row['status']!r}"
            )
        if row["created_by_idx"] != originator_principal_idx:
            raise _bad(
                f"action_context.{key}={upload_idx} was created by principal "
                f"{row['created_by_idx']}, work_ticket originator is "
                f"{originator_principal_idx}"
            )
        staging_path = compute_upload_staging_path(upload_staging_root, upload_idx)
        if not staging_path.exists():
            raise _bad(
                f"action_context.{key}={upload_idx} resolves to {staging_path} "
                "but the staged file is missing — CP and DP "
                "upload_staging_root disagree, or scratch was wiped"
            )
        resolved[prefix + _PATH_SUFFIX] = staging_path
        to_consume.append(upload_idx)
    return resolved, to_consume


async def _consume_upload_handles(
    pool: asyncpg.Pool | asyncpg.Connection, *, upload_idxs: list[int]
) -> None:
    """Bulk-transition `ready → consumed` for the listed upload rows.
    Mismatches (count of rows updated != len(upload_idxs)) raise a
    FINALIZE-stage BackendFailure so a stolen handle surfaces loudly
    instead of silently completing the workflow.

    Accepts either a pool or a live Connection so the success-path
    finalize block can run this inside the same transaction as the
    work_ticket COMPLETED transition."""
    if not upload_idxs:
        return
    # completed_at is pinned at the first terminal transition (the
    # pending→ready UPDATE in POST /upload/{idx}/done) per the migration
    # comment on `upload_terminal_has_completed_at`. Any other path that
    # mutates `status` off `pending` must populate `completed_at`; paths
    # that move between non-pending states (ready→consumed here, a future
    # consumed→archived, etc.) must NOT overwrite it.
    rows = await pool.fetch(
        "UPDATE qiita.upload"
        " SET status = $1"
        " WHERE upload_idx = ANY($2::bigint[])"
        "   AND status = $3"
        " RETURNING upload_idx",
        UploadStatus.CONSUMED.value,
        upload_idxs,
        UploadStatus.READY.value,
    )
    if len(rows) != len(upload_idxs):
        consumed = {r["upload_idx"] for r in rows}
        missing = sorted(set(upload_idxs) - consumed)
        raise BackendFailure(
            kind=FailureKind.UNKNOWN_PERMANENT,
            stage=WorkTicketFailureStage.FINALIZE,
            step_name=None,
            reason=(
                f"could not transition uploads {missing} from "
                f"{UploadStatus.READY.value!r} to {UploadStatus.CONSUMED.value!r}: "
                "concurrent state change"
            ),
        )


# =============================================================================
# Reference-index resolution
# =============================================================================


class ReferenceIndexNotBuilt(ValueError):
    """The reference is ACTIVE but carries no index of the requested type.

    A `ValueError` subclass so existing callers / tests that catch `ValueError`
    still match, while `_resolve_host_filter_indexes` can catch THIS narrowly to
    treat a missing index type as "skip that host-filter stage" — distinct from a
    non-active reference (a plain `ValueError`), which stays a hard error."""


async def _resolve_reference_index_path(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    index_type: str,
) -> str:
    """Resolve the on-disk path of the newest `index_type` index for an
    ACTIVE reference — a host-filter index path the `host_filter` step is
    injected with (the rype `.ryxdi` or minimap2 `.mmi` for a host reference;
    see `_resolve_host_filter_indexes`).

    `qiita.reference_index` has no UNIQUE(reference_idx, index_type) by design
    (growing a reference appends a newer generation), so "newest wins":
    ordered by created_at then reference_index_idx, both descending, so a
    same-timestamp tie still resolves deterministically to the latest row.

    Raises:
      * ReferenceNotFound — the reference row doesn't exist.
      * ValueError — the reference exists but isn't `active` (an index built
        against a still-`indexing`/failed reference must not be served; the
        build may be mid-flight).
      * ReferenceIndexNotBuilt (a ValueError subclass) — the reference is active
        but no `index_type` index exists yet. Narrower than the not-active case
        so `_resolve_host_filter_indexes` can treat a single missing index type
        as "skip that stage" while still hard-failing a non-active reference."""
    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if status is None:
        raise ReferenceNotFound(reference_idx)
    if status != ReferenceStatus.ACTIVE.value:
        raise ValueError(
            f"reference {reference_idx} status is {status!r}, must be "
            f"{ReferenceStatus.ACTIVE.value!r} to resolve its {index_type!r} index"
        )
    fs_path = await pool.fetchval(
        "SELECT fs_path FROM qiita.reference_index"
        " WHERE reference_idx = $1 AND index_type = $2"
        " ORDER BY created_at DESC, reference_index_idx DESC"
        " LIMIT 1",
        reference_idx,
        index_type,
    )
    if fs_path is None:
        raise ReferenceIndexNotBuilt(
            f"reference {reference_idx} has no {index_type!r} index built yet"
        )
    return fs_path


def _coerce_reference_idx(value: Any, field: str) -> int:
    """Validate a host-filter reference idx pulled from `action_context`. `type(...)
    is int` (not isinstance) rejects a JSON bool — an int subclass — rather than
    silently treating it as 0/1. Raises a SUBMISSION BAD_INPUT on a missing /
    non-positive / wrong-typed value."""
    if type(value) is not int or value <= 0:
        raise _submission_bad_input(f"{field} must be a positive integer, got {value!r}")
    return value


async def _resolve_required_host_index(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    index_type: str,
    field: str,
) -> Path:
    """Resolve `index_type` from `reference_idx`, mapping EVERY failure mode
    (unknown reference, non-active, index not built) to a SUBMISSION BAD_INPUT —
    the two-reference layout designates a reference explicitly for this index
    type, so a missing index is a hard error (not a skipped stage as in the legacy
    single-reference layout)."""
    try:
        return Path(await _resolve_reference_index_path(pool, reference_idx, index_type))
    except ReferenceNotFound as exc:
        raise _submission_bad_input(
            f"{field}={reference_idx} references an unknown reference"
        ) from exc
    except ValueError as exc:
        # Non-active reference OR ReferenceIndexNotBuilt (a ValueError subclass) —
        # both hard errors here (the reference was designated for this index).
        raise _submission_bad_input(str(exc)) from exc


async def _resolve_host_filter_indexes(
    pool: asyncpg.Pool | asyncpg.Connection,
    *,
    action_context: dict[str, Any],
) -> dict[str, Path]:
    """Resolve the host-filter index paths when host filtering is enabled, else {}.

    Gated by `host_filter_enabled` (bool) in `action_context`. Two layouts are
    accepted, never mixed:

    * **Two-reference** (fastq-to-parquet/1.2.0): an independent reference per
      tool — `host_rype_reference_idx` (REQUIRED) supplies the rype `.ryxdi`, and
      the optional `host_minimap2_reference_idx` supplies the minimap2 `.mmi`.
      Each is bound from its OWN reference, which MUST be ACTIVE and MUST carry the
      named index type (a designated reference missing its index is a hard error).
      minimap2 omitted → only `host_rype_path` is bound.
    * **Legacy single-reference** (fastq-to-parquet/1.1.0): `host_reference_idx`
      names ONE active reference; whichever of its rype/minimap2 indexes exist are
      bound (>=1 required; a missing one just skips that stage). Kept for
      back-compat.

    Both bind `host_rype_path` / `host_minimap2_path` — the `host_filter` step's
    optional inputs; the step skips the stage whose path is None, so
    `host_filter.py` is unchanged across both layouts. When disabled (flag
    false/absent) nothing is resolved and the step runs as a pass-through.

    Mirrors `_resolve_upload_handles`: every failure (a required idx absent /
    non-positive, a reference unknown / non-active / missing its designated index,
    NEITHER index in the legacy case, or mixing the two layouts) raises a typed
    `BackendFailure(BAD_INPUT)` at stage=SUBMISSION that `run_workflow` turns into
    a FAILED work_ticket. None of these keys end in `_upload_idx`, so
    `_resolve_upload_handles` leaves them untouched."""
    if not action_context.get("host_filter_enabled"):
        return {}

    legacy_idx = action_context.get("host_reference_idx")
    rype_idx = action_context.get("host_rype_reference_idx")
    minimap2_idx = action_context.get("host_minimap2_reference_idx")

    # The two layouts are mutually exclusive — mixing them is a contract error,
    # not a silent precedence pick.
    if legacy_idx is not None and (rype_idx is not None or minimap2_idx is not None):
        raise _submission_bad_input(
            "host filtering accepts EITHER host_reference_idx (legacy single "
            "reference) OR host_rype_reference_idx (+ optional "
            "host_minimap2_reference_idx), not both"
        )

    # Enabled but no reference key at all: name BOTH layouts so a caller who
    # dropped (or typo'd) their key isn't pointed at a key they never set — the
    # bare two-reference fallthrough below would otherwise blame
    # host_rype_reference_idx even for a legacy 1.1.0 submission.
    if legacy_idx is None and rype_idx is None and minimap2_idx is None:
        raise _submission_bad_input(
            "host_filter_enabled requires host_reference_idx (legacy single "
            "reference) or host_rype_reference_idx (two-reference layout)"
        )

    if legacy_idx is not None:
        return await _resolve_host_filter_legacy(pool, legacy_idx)
    return await _resolve_host_filter_two_reference(pool, rype_idx, minimap2_idx)


async def _resolve_host_filter_two_reference(
    pool: asyncpg.Pool | asyncpg.Connection,
    rype_idx: Any,
    minimap2_idx: Any,
) -> dict[str, Path]:
    """Two-reference host filter (fastq-to-parquet/1.2.0): bind the rype index from
    the REQUIRED `host_rype_reference_idx` and, when set, the minimap2 index from
    `host_minimap2_reference_idx` — each from its own reference. See
    `_resolve_host_filter_indexes`."""
    bound: dict[str, Path] = {
        "host_rype_path": await _resolve_required_host_index(
            pool,
            _coerce_reference_idx(rype_idx, "host_rype_reference_idx"),
            HOST_FILTER_INDEX_TYPE_RYPE,
            "host_rype_reference_idx",
        )
    }
    if minimap2_idx is not None:
        bound["host_minimap2_path"] = await _resolve_required_host_index(
            pool,
            _coerce_reference_idx(minimap2_idx, "host_minimap2_reference_idx"),
            HOST_FILTER_INDEX_TYPE_MINIMAP2,
            "host_minimap2_reference_idx",
        )
    return bound


async def _resolve_host_filter_legacy(
    pool: asyncpg.Pool | asyncpg.Connection,
    host_reference_idx: Any,
) -> dict[str, Path]:
    """Legacy single-reference host filter (fastq-to-parquet/1.1.0):
    `host_reference_idx` names ONE active reference; bind whichever of its
    rype/minimap2 indexes exist (>=1 required; a missing one skips that stage).
    Preserved for 1.1.0 back-compat. See `_resolve_host_filter_indexes`."""
    host_reference_idx = _coerce_reference_idx(host_reference_idx, "host_reference_idx")

    # Resolve each index type independently: a host reference may carry only one
    # (rype-only / minimap2-only). A missing index type (ReferenceIndexNotBuilt)
    # is non-fatal — that stage is simply skipped — but an unknown or non-active
    # reference is a hard BAD_INPUT, and a reference with NEITHER index can't
    # filter anything, so it's rejected too.
    bound: dict[str, Path] = {}
    for index_type, binding in (
        (HOST_FILTER_INDEX_TYPE_RYPE, "host_rype_path"),
        (HOST_FILTER_INDEX_TYPE_MINIMAP2, "host_minimap2_path"),
    ):
        try:
            bound[binding] = Path(
                await _resolve_reference_index_path(pool, host_reference_idx, index_type)
            )
        except ReferenceNotFound as exc:
            raise _submission_bad_input(
                f"host_reference_idx={host_reference_idx} references an unknown reference"
            ) from exc
        except ReferenceIndexNotBuilt:
            # This index type wasn't built for the reference — skip its stage.
            continue
        except ValueError as exc:
            # Reference not active (build may be mid-flight) — hard error.
            raise _submission_bad_input(str(exc)) from exc
    if not bound:
        raise _submission_bad_input(
            f"host_reference_idx={host_reference_idx} has neither a "
            f"{HOST_FILTER_INDEX_TYPE_RYPE!r} nor a {HOST_FILTER_INDEX_TYPE_MINIMAP2!r} index; "
            "a host reference must carry at least one host-filter index"
        )
    return bound


# Binding name the runner stages the canonical adapter set (a Parquet) under. A
# step that lists this in its `inputs` (the qc step) signals the runner to
# materialize the adapter set before the step loop (see `_resolve_qc_adapters`).
QC_ADAPTER_BINDING = "adapter_parquet"

# The DuckLake table holding actual sequence bytes (reference_sequences is
# metadata only). Must match the data plane's ALLOWED_TABLES whitelist and the
# route's _DOGET_ALLOWED_TABLES.
_REFERENCE_CHUNKS_TABLE = "reference_sequence_chunks"


def _do_get_reference_sequence_chunks(
    data_plane_url: str, ticket_bytes: bytes
) -> list[tuple[int, int, str]]:
    """Synchronous Flight DoGet of a reference's sequence chunks — runs in a
    thread executor (pyarrow.flight is sync). Returns (feature_idx, chunk_index,
    chunk_data) rows. Mirrors `actions.library._do_action_register`'s client
    use; pyarrow imported lazily to keep it off the module hot path. Isolated as
    a module function so unit tests stub the real DoGet."""
    import pyarrow.flight as flight  # noqa: PLC0415

    with flight.FlightClient(data_plane_url) as client:
        table = client.do_get(flight.Ticket(ticket_bytes)).read_all()
    cols = {
        name: table.column(name).to_pylist()
        for name in ("feature_idx", "chunk_index", "chunk_data")
    }
    return list(zip(cols["feature_idx"], cols["chunk_index"], cols["chunk_data"], strict=True))


def _write_adapter_parquet(rows: list[tuple[int, int, str]], out_path: Path) -> int:
    """Reassemble chunked sequences (group by feature_idx, order by chunk_index,
    concat chunk_data — the same string_agg the data plane documents) into a
    Parquet at `out_path`, one row per feature with columns `feature_idx` (BIGINT,
    provenance) and `sequence` (VARCHAR, the adapter). Rows are sorted by
    feature_idx for determinism; the qc job reads only `sequence` via
    `read_parquet`. Returns the sequence count. Raises ValueError on an empty set
    — an adapter reference with no sequences is a misconfiguration, not a valid QC
    input.

    Parquet (not FASTA) keeps the adapter set in the same columnar format as the
    reads it trims, so the qc job reads it with `read_parquet` and no FASTA
    parsing. pyarrow (already this module's Flight dependency) writes it directly
    from the reassembled rows — no DuckDB connection needed on the control plane's
    pre-loop path.

    Input contract (the reference-load flow, jobs/reference_load.py): chunk_data
    is a substring of a parsed FASTA record, so it is newline-free, and a feature
    is loaded exactly once with monotonic chunk_index (a reference is loaded once,
    pending→loading→active), so (feature_idx, chunk_index) is unique. Hence no
    newline sanitation or chunk dedup here — both would mask a real corruption we
    want to surface."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    by_feature: dict[int, list[tuple[int, str]]] = {}
    for feature_idx, chunk_index, chunk_data in rows:
        by_feature.setdefault(feature_idx, []).append((chunk_index, chunk_data))
    if not by_feature:
        raise ValueError("adapter reference returned no sequences")
    feature_ids = sorted(by_feature)
    sequences = [
        "".join(chunk for _, chunk in sorted(by_feature[feature_idx]))
        for feature_idx in feature_ids
    ]
    table = pa.table(
        {
            "feature_idx": pa.array(feature_ids, type=pa.int64()),
            "sequence": pa.array(sequences, type=pa.string()),
        }
    )
    pq.write_table(table, str(out_path))
    return len(by_feature)


def _workflow_needs_adapters(steps: list[Any]) -> bool:
    """True iff some entry declares `adapter_parquet` as an (optional) input — the
    signal the runner must materialize the adapter set before the step loop."""
    for entry in steps:
        names = list(getattr(entry, "inputs", []) or []) + list(
            getattr(entry, "optional_inputs", []) or []
        )
        if QC_ADAPTER_BINDING in names:
            return True
    return False


async def _resolve_qc_adapters(
    pool: asyncpg.Pool | asyncpg.Connection,
    *,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    hmac_secret: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Materialize the canonical adapter set as a local one-`sequence`-column
    Parquet for the QC step.

    Run before the step loop when `_workflow_needs_adapters`. Resolves the
    configured `artifact_sequence_set` reference, signs + DoGets its sequence
    chunks from the data plane, reassembles them, and writes
    `<workspace>/adapters.parquet` (the shared-FS ticket root every compute node
    sees) — bound to the qc step as `adapter_parquet`. Re-run safe: a resume
    re-materializes the same file (DoGet is read-only).

    Like `_resolve_host_filter_indexes`, every failure raises a
    SUBMISSION-attributed BAD_INPUT the outer handler turns into a FAILED ticket:
    no configured default, an unknown / wrong-kind / non-active reference, or an
    empty adapter set."""
    if default_adapter_reference_idx is None:
        raise _submission_bad_input(
            "this workflow needs an adapter set but no default adapter reference is "
            "configured — set QIITA_DEFAULT_ADAPTER_REFERENCE_IDX to the loaded "
            "artifact_sequence_set reference_idx"
        )
    # NOTE: single-gate (kind/status checked here, then DoGet) — same TOCTOU
    # shape as _resolve_reference_index_path. Safe for a canonical, static
    # adapter set that nothing transitions out of `active` mid-run; revisit if
    # the adapter reference ever gains a rotation lifecycle.
    row = await pool.fetchrow(
        "SELECT kind, status FROM qiita.reference WHERE reference_idx = $1",
        default_adapter_reference_idx,
    )
    if row is None:
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx} does not exist"
        )
    if row["kind"] != "artifact_sequence_set":
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx} has kind "
            f"{row['kind']!r}, expected 'artifact_sequence_set'"
        )
    if row["status"] != ReferenceStatus.ACTIVE.value:
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx} status is "
            f"{row['status']!r}, must be {ReferenceStatus.ACTIVE.value!r}"
        )

    ticket = sign_ticket(
        table=_REFERENCE_CHUNKS_TABLE,
        filter={"reference_idx": [default_adapter_reference_idx]},
        secret=hmac_secret,
    )
    # A Flight failure (data plane unreachable / errored) raises
    # pyarrow.flight.FlightError, which is NOT a BackendFailure — letting it
    # escape this pre-loop pass would hit run_workflow's bare `except Exception`,
    # which records stage=STEP_RUN with step_name=None and so VIOLATES the
    # work_ticket_failure_step_name_consistent CHECK (step_run ⇒ step_name NOT
    # NULL) — the failure transition itself would throw and strand the ticket in
    # PROCESSING. Wrap it as a SUBMISSION failure like every other pre-loop
    # resolver. (Not retried in place: the operator resubmits if the data plane
    # was down.)
    try:
        rows = await asyncio.get_event_loop().run_in_executor(
            None, _do_get_reference_sequence_chunks, data_plane_url, ticket
        )
    except Exception as exc:
        raise _submission_bad_input(
            f"could not fetch adapter sequences for reference "
            f"{default_adapter_reference_idx} from the data plane: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    workspace.mkdir(parents=True, exist_ok=True)
    adapter_parquet = workspace / "adapters.parquet"
    try:
        _write_adapter_parquet(rows, adapter_parquet)
    except ValueError as exc:
        adapter_parquet.unlink(missing_ok=True)
        raise _submission_bad_input(
            f"default adapter reference {default_adapter_reference_idx}: {exc}"
        ) from exc
    return {QC_ADAPTER_BINDING: adapter_parquet}


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
    raise RuntimeError(f"unknown scope_target_kind: {kind!r}")


async def _patch_resource_status(
    pool: asyncpg.Pool | asyncpg.Connection,
    scope_target: dict[str, Any],
    target_status: str,
) -> None:
    """Drive the appropriate resource-status transition for the scope_target.
    Today only `reference` is wired."""
    if scope_target["kind"] == ScopeTargetKind.REFERENCE.value:
        await transition_reference_status(
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
) -> FlatBaselineResources:
    """Resolve a step's ``baseline_resources`` to a concrete
    ``FlatBaselineResources`` and clamp against ``action_ceiling``.

    ``mem_gb_override`` (the ticket's optional per-run resource bump) raises the
    resolved memory *floor*: ``mem_gb = max(resolved.mem_gb, mem_gb_override)``.
    It only ever increases memory — a smaller override leaves a step the YAML
    sized higher untouched. The bump is applied before the ceiling assertion
    below, so an override above ``action_ceiling.mem_gb`` is rejected here too
    (defense in depth; the submission route already 422s it).

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

    # Per-run memory floor (raise-only): never lowers a step the YAML sized
    # higher than the override.
    if mem_gb_override is not None and mem_gb_override > resolved.mem_gb:
        resolved = resolved.model_copy(update={"mem_gb": mem_gb_override})

    _assert_within_ceiling(entry=entry, resolved=resolved, action_ceiling=action_ceiling)
    return resolved


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
    inputs = {name: Path(bound[name]) for name in entry.inputs}
    inputs.update({name: Path(bound[name]) for name in entry.optional_inputs if name in bound})
    # Scalar build params (WorkflowStep.params, keyed action_context_key ->
    # Inputs field). Unlike inputs/optional_inputs these are NOT host paths, so
    # they are merged un-Path-coerced and as strings: the wire carries
    # `inputs: dict[str, str]` and the native job's Pydantic `Inputs` model
    # re-coerces each string to its declared type (e.g. "35" -> int w). Native
    # steps only — `_resolve_input_binds` (which would treat a value as a path)
    # is container-only, so a scalar here is never mistaken for a bind mount.
    inputs.update(
        {field: str(bound[ctx_key]) for ctx_key, field in entry.params.items() if ctx_key in bound}
    )
    resolved = _resolve_baseline_for_step(
        entry=entry,
        bound=bound,
        action_ceiling=action_ceiling,
        mem_gb_override=mem_gb_override,
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


async def _reconstruct_completed_outputs(
    entry: WorkflowStep | WorkflowAction,
    completed: step_progress.StepProgressRow,
    workspace: Path,
    backend_client: ComputeBackendClient,
    *,
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    """Rebuild the bound outputs of an already-COMPLETED entry from disk,
    without re-running it.

    A `step:` entry re-reads its verified output manifest through `result_step`
    (reconstructing a handle from the progress row's job id + the deterministic
    per-attempt `output`/`logs` dirs). This doubles as the filesystem
    tiebreaker for a now-purged job: a valid manifest yields the outputs; a
    missing / broken one raises CONTRACT_VIOLATION → the resumed workflow
    fails, as it should when a completed step's output has vanished from
    scratch.

    An `action:` entry rebuilds its deterministic output paths in-process (see
    `_reconstruct_action_outputs`) — the in-process primitive must not re-run.

    A non-SLURM (local) completed step has no on-disk manifest to re-read;
    recovery is a SLURM-backend concern (local steps are synchronous and don't
    survive a restart mid-flight), so this returns its outputs empty — a
    downstream consumer that needs a missing binding fails loudly via KeyError."""
    attempt_workspace = workspace / entry.name / f"attempt-{completed.attempt}"
    if isinstance(entry, WorkflowAction):
        return _reconstruct_action_outputs(entry, attempt_workspace)
    if completed.compute_target is not ComputeTarget.SLURM:
        return {}
    handle = StepHandleWire(
        compute_target=ComputeTarget.SLURM,
        step_name=entry.name,
        slurm_job_id=completed.slurm_job_id,
        job_name=completed.job_name,
        output_path=str(attempt_workspace / "output"),
        logs_path=str(attempt_workspace / "logs"),
    )
    status = StepStatusWire(status=StepStatus.COMPLETED, raw_state="RECOVERED")
    raw_outputs = await _result_with_infra_retry(
        backend_client,
        handle,
        status,
        pool=pool,
        work_ticket_idx=work_ticket_idx,
        poll_interval_seconds=poll_interval_seconds,
    )
    return {name: Path(raw_outputs[name]) for name in entry.outputs}


def _reconstruct_action_outputs(entry: WorkflowAction, attempt_workspace: Path) -> dict[str, Any]:
    """Deterministic output paths an `action:` primitive wrote, for resume.
    Only `mint-features` contributes a binding (the feature-map Parquet it
    wrote into its workspace); the other primitives produce no bound output.
    Mirrors the output shapes in `_run_action_primitive` — keep the two in
    step when a primitive's outputs change."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        return {entry.outputs[0]: attempt_workspace / "feature_map.parquet"}
    return {}


async def _dispatch_action(
    pool: asyncpg.Pool,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    hmac_secret: bytes,
    data_plane_url: str,
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
    handler — which owns the work_ticket-level FAILED transition. The
    exception is re-raised unchanged so the outer handler classifies it
    exactly as before."""
    await step_progress.record_submitting(
        pool,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        step_name=entry.name,
        compute_target=ComputeTarget.CONTROL_PLANE,
    )
    try:
        outputs = await _run_action_primitive(
            pool,
            entry,
            bound,
            workspace,
            scope_target,
            work_ticket_idx=work_ticket_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
    except BackendFailure as exc:
        await _best_effort_record_failed(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=exc.kind.value,
            failure_reason=exc.reason[:2000],
        )
        raise
    except Exception as exc:
        # Plain Python from a LIBRARY primitive (untyped failure / bug). The
        # outer run_workflow handler classifies it UNKNOWN_PERMANENT; record
        # the same on the progress row, then re-raise unchanged.
        await _best_effort_record_failed(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=FailureKind.UNKNOWN_PERMANENT.value,
            failure_reason=f"{type(exc).__name__}: {exc!s}"[:2000],
        )
        raise
    await step_progress.record_completed(
        pool, work_ticket_idx=work_ticket_idx, step_index=step_index, attempt=attempt
    )
    return outputs


async def _run_action_primitive(
    pool: asyncpg.Pool,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    *,
    work_ticket_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict[str, Any]:
    """Translate a workflow `action:` entry into the matching LIBRARY call.
    Per-primitive logic lives here because each primitive has its own
    input/output shape — a generic dispatcher would just push the same
    `if name == ...` ladder somewhere else."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        manifest_path = Path(bound[entry.inputs[0]])
        # `genome_map_path` is a workflow-context optional, not an entry
        # input — the YAML's mint-features `inputs:` stays single-valued.
        # Pulled directly from `bound` so a ticket whose action_context
        # carries it picks up genome-association writes for free.
        genome_map = bound.get("genome_map_path")
        feature_map_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
            pool,
            manifest_path,
            workspace,
            genome_map_path=Path(genome_map) if genome_map else None,
        )
        # YAML declares one output (typically "feature_map"); bind it.
        return {entry.outputs[0]: feature_map_path}

    if entry.name == LibraryPrimitive.WRITE_MEMBERSHIP:
        feature_map_path = Path(bound[entry.inputs[0]])
        await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
            pool, scope_target["reference_idx"], feature_map_path
        )
        return {}

    if entry.name == LibraryPrimitive.REGISTER_FILES:
        staging_dir = Path(bound[entry.inputs[0]])
        # Filename → DuckLake table mapping derived from the staging dir.
        # Convention:
        #   - Top-level `<table>.parquet` files register as the table
        #     named after the file's stem (single-file table).
        #   - Top-level subdirs containing `*.parquet` files register
        #     each part as the table named after the directory
        #     (multi-file table). The filename in `files` carries the
        #     subdir prefix relative to staging_dir; the data plane
        #     normalises to basename when placing each part in the
        #     permanent per-table directory.
        # The multi-file form exists for `reference_sequence_chunks` —
        # at GG2 scale a single-file sort+write of ~30 GB of chunk_data
        # OOMs DuckDB; reference_load batches it into part files
        # instead (jobs/reference_load.py:_write_reference_sequence_chunks).
        files: dict[str, str] = {}
        for entry_path in sorted(staging_dir.iterdir()):
            if entry_path.is_file() and entry_path.suffix == ".parquet":
                files[entry_path.name] = entry_path.stem
            elif entry_path.is_dir():
                for part in sorted(entry_path.glob("*.parquet")):
                    rel = part.relative_to(staging_dir).as_posix()
                    files[rel] = entry_path.name
        if not files:
            raise RuntimeError(
                f"register-files: staging_dir {staging_dir} contains no Parquet files"
            )
        await LIBRARY[LibraryPrimitive.REGISTER_FILES](
            staging_dir=str(staging_dir),
            files=files,
            work_ticket_idx=work_ticket_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
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
        # the builder, not hardcoded here).
        if len(entry.inputs) != 1:
            raise RuntimeError(
                f"register-index expects exactly one input (the index meta); got {entry.inputs!r}"
            )
        meta_path = Path(bound[entry.inputs[0]])
        meta = json.loads(meta_path.read_text())
        await LIBRARY[LibraryPrimitive.REGISTER_INDEX](
            pool,
            reference_idx=scope_target["reference_idx"],
            index_type=meta["index_type"],
            fs_path=meta["fs_path"],
            params=meta["params"],
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_READ_METRICS:
        # Persist the three per-stage read counts onto this prep_sample's
        # 1:1 sequenced_sample. Each declared input is a Path to a
        # read_count.json sidecar; we read the both-mates `read_count_r1r2` from
        # each and hand structured ints to the primitive (same pattern as
        # register-index reading its meta JSON). Inputs are resolved by their
        # fixed binding names — not positionally — so a YAML reorder can't
        # silently swap raw/biological/quality_filtered.
        if set(entry.inputs) != {
            "raw_read_count",
            "biological_read_count",
            "quality_filtered_read_count",
        }:
            raise RuntimeError(
                "persist-read-metrics expects inputs "
                "[raw_read_count, biological_read_count, quality_filtered_read_count]; "
                f"got {entry.inputs!r}"
            )

        def _count(name: str) -> int:
            return json.loads(Path(bound[name]).read_text())["read_count_r1r2"]

        await LIBRARY[LibraryPrimitive.PERSIST_READ_METRICS](
            pool,
            scope_target["prep_sample_idx"],
            _count("raw_read_count"),
            _count("biological_read_count"),
            _count("quality_filtered_read_count"),
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
                "persist-qc-report expects inputs "
                "[raw_qc_report, filtered_qc_report]; "
                f"got {entry.inputs!r}"
            )

        def _report(name: str) -> dict[str, Any]:
            return json.loads(Path(bound[name]).read_text())

        await LIBRARY[LibraryPrimitive.PERSIST_QC_REPORT](
            pool,
            scope_target["prep_sample_idx"],
            _report("raw_qc_report"),
            _report("filtered_qc_report"),
        )
        return {}

    raise RuntimeError(f"runner has no adapter for action {entry.name!r}")
