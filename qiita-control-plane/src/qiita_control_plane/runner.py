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
once — retries (and re-runs) land in fresh dirs (the verifier's "every file
in $output_path must be in manifest" gate stays clean), and prior attempts
persist on disk for postmortem. An entry RE-RUN whose progress row was
deliberately dropped (a `/run` redrive, or `update-lane` invalidating a
completed prep row) skips past its now-orphaned attempt dir to a fresh one
rather than reusing it: the prior output is known-bad and its read-only files
would block the re-run, and the runner cannot delete that dir — a container
step's output is owned by the SLURM job user with read-only (0550) dirs the
control-plane process can neither unlink nor chmod (see `_attempt_is_unowned`).
Entries see each other's outputs via the runner's binding map, which carries
absolute paths forward so consumers don't need to know the producer's attempt
number.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Callable
from datetime import timedelta
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
from qiita_common.api_paths import (
    LibraryPrimitive,
    compute_reads_staging_path,
    compute_upload_staging_path,
)
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
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
    StepPlanResponse,
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
from .auth.tickets import sign_action, sign_ticket
from .repositories.block import fetch_block_members
from .repositories.mask_definition import lookup_mask_idx_by_params, mint_mask_definition
from .shard_orchestration import (
    BUILD_SHARD_INDEX_ACTION_ID,
    BUILD_SHARD_INDEX_ACTION_VERSION,
    SHARD_BUILD_CONTEXT_KEYS,
    expected_shard_index_types,
    plan_and_submit_shards,
)

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

# Transient errors on the runner's OWN control-plane DB calls (poll / retry
# loops). A per-statement `command_timeout` surfaces as a bare
# `asyncio.TimeoutError` (which *is* the builtin `TimeoutError` in Python 3.11+,
# with empty args); a brief CP-DB blip (failover, restart, connection reset,
# pool drain race) surfaces as a `PostgresConnectionError` / `InterfaceError`.
# None of these mean the step's WORK failed — the ticket's true state is fully
# recoverable from the DB once it is reachable again — so the runner extends its
# never-fail-on-outage rule to them: retry the cheap poll-loop read in place
# (see `_raise_if_ticket_terminal`), and if a transient DB error still escapes
# any other runner DB call it is recorded RETRIABLE (not PERMANENT) in
# run_workflow's catch-all so a `/run` redrive re-attempts instead of the ticket
# being abandoned as a deterministic failure. A real SQL error (constraint
# violation, query bug) is a `PostgresError` that is NOT one of these, so it
# still propagates as permanent.
#
# `TimeoutError` membership rests on an invariant worth stating: the only bare
# builtin `TimeoutError` reachable on a runner code path is asyncpg's
# `command_timeout`. Compute-backend timeouts are converted to a `BackendFailure`
# at the ComputeBackendClient boundary (never a bare `TimeoutError`), so they are
# classified by `.kind`, not here. If a future caller wraps work in
# `asyncio.wait_for(...)`, its `TimeoutError` would be mislabeled "DB error" —
# the failure *direction* stays safe (RETRIABLE is the forgiving choice), only
# the label would be wrong. `InterfaceError` covers the CP-drain / restart race
# ("pool is closing" / "connection is closed"); its other variant ("another
# operation is in progress") is a shared-connection bug that can't arise here —
# the runner acquires a fresh connection per call via `pool.fetchval`/`execute`.
_TRANSIENT_DB_ERRORS: tuple[type[BaseException], ...] = (
    TimeoutError,
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
)


def _is_transient_db_error(exc: BaseException) -> bool:
    """True for a transient control-plane DB error a retry can self-heal (see
    `_TRANSIENT_DB_ERRORS`). A `BackendFailure` is never one of these — it is the
    compute backend's typed failure, classified by its own `.kind`."""
    return isinstance(exc, _TRANSIENT_DB_ERRORS)


# The poll loop's per-iteration force-fail escape check (`_raise_if_ticket_terminal`)
# is a single cheap one-column read; the pool's default 10s `command_timeout` is
# an arbitrary — and under a lock wait / checkpoint / load spike, too tight —
# ceiling for it, and a single timeout there used to abandon a healthy in-flight
# job. Give this specific read a generous per-call timeout and retry it in place
# a bounded number of times on a transient DB error before giving up. The total
# in-place wait (≈ attempts × timeout) stays well under any real SLURM walltime.
_POLL_DB_READ_TIMEOUT_SECONDS = 30.0
_POLL_DB_READ_MAX_ATTEMPTS = 3
_POLL_DB_READ_BACKOFF_SECONDS = 1.0

# Work-ticket states the runner does NOT own. Once a ticket reaches one of
# these out from under a running workflow — an operator
# `qiita-admin ticket force-fail` flips it to FAILED — the runner must stop:
# the in-place infra-retry/poll loops re-check this each iteration and bail via
# WorkflowAborted instead of retrying forever against a ticket that is no
# longer theirs.
_TERMINAL_WORK_TICKET_STATES = frozenset(
    {
        WorkTicketState.COMPLETED.value,
        WorkTicketState.NO_DATA.value,
        WorkTicketState.FAILED.value,
    }
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

    Resilient to a transient CP-DB hiccup: the read uses a generous per-call
    timeout (`_POLL_DB_READ_TIMEOUT_SECONDS`, overriding the pool's tighter
    default for this one cheap read) and is retried in place up to
    `_POLL_DB_READ_MAX_ATTEMPTS` on a transient DB error (`_is_transient_db_error`)
    with a short backoff. A brief blip — a `command_timeout` under a lock wait /
    checkpoint / load spike, or a momentary connection drop — therefore does NOT
    abandon a healthy in-flight job. Only a sustained outage exhausts the retries
    and re-raises; run_workflow's catch-all then records it RETRIABLE (not
    PERMANENT), so a redrive recovers rather than the ticket dying as a
    deterministic failure. A non-transient DB error (a real SQL bug) propagates
    on the first raise.

    Deliberately NOT the `_infra_backoff_delay` (capped-exponential) +
    `_note_transient_retry` machinery the compute-side in-place retries use: this
    is a short, bounded read-retry (a few attempts, not an open-ended outage
    wait), and it cannot surface a `transient_reason` row because the very thing
    that's failing IS the DB it would write that row to."""
    last_exc: BaseException | None = None
    for attempt in range(_POLL_DB_READ_MAX_ATTEMPTS):
        try:
            state = await pool.fetchval(
                "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
                timeout=_POLL_DB_READ_TIMEOUT_SECONDS,
            )
        except _TRANSIENT_DB_ERRORS as exc:
            last_exc = exc
            _log.warning(
                "work_ticket %d: transient DB error on the force-fail check (%s); attempt %d/%d",
                work_ticket_idx,
                type(exc).__name__,
                attempt + 1,
                _POLL_DB_READ_MAX_ATTEMPTS,
            )
            if attempt + 1 < _POLL_DB_READ_MAX_ATTEMPTS:
                await asyncio.sleep(_POLL_DB_READ_BACKOFF_SECONDS * (attempt + 1))
            continue
        if state in _TERMINAL_WORK_TICKET_STATES:
            raise WorkflowAborted(work_ticket_idx, state)
        return
    # Every attempt hit a transient DB error — let it propagate so the catch-all
    # records the ticket RETRIABLE; a resume/redrive re-attaches once PG recovers.
    assert last_exc is not None  # only reached after a recorded transient failure
    raise last_exc


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
                        hmac_secret=hmac_secret,
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
                bound.update(
                    await _resolve_staged_reads_block(
                        members,
                        data_plane_url=data_plane_url,
                        hmac_secret=hmac_secret,
                        workspace=workspace,
                    )
                )
            else:
                raise _submission_bad_input(
                    "a workflow that masks stored reads must be prep_sample- or "
                    f"block-scoped; got {scope_target['kind']!r}"
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
                    hmac_secret=hmac_secret,
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
    hmac_secret: bytes,
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
                    hmac_secret=hmac_secret,
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
    "wt.block_idx, wt.mask_idx, wt.shard_id, "
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


# The non-terminal states a work_ticket may legitimately transition FROM.
# Shared by every guarded transition so the allowed-source set is defined once.
_NON_TERMINAL_STATES = [
    WorkTicketState.PENDING.value,
    WorkTicketState.QUEUED.value,
    WorkTicketState.PROCESSING.value,
]


async def _guarded_state_update(
    pool: asyncpg.Pool | asyncpg.Connection,
    work_ticket_idx: int,
    *,
    set_clause: str,
    set_params: list[Any],
    allowed_states: list[str],
    action: str,
) -> None:
    """Run a TOCTOU-safe work_ticket state UPDATE.

    Applies `set_clause` only when the row's current state is one of
    `allowed_states`. Coupling the caller MUST honour: `set_clause` references
    exactly $1..$len(set_params); the helper appends the WHERE's $n+1
    (work_ticket_idx) and $n+2 (allowed_states) after them. If nothing matched, reads the
    actual state and raises — surfacing a stuck/racing ticket loudly instead of
    silently overwriting it. `action` names the attempted transition in that
    error. Accepts a pool (transient connection) or a live Connection, so the
    finalize block can run its transition inside the same transaction as
    `_consume_upload_handles` and the status PATCH."""
    n = len(set_params)
    updated = await pool.fetchval(
        f"UPDATE qiita.work_ticket SET {set_clause}"
        f" WHERE work_ticket_idx = ${n + 1}"
        f"   AND state = ANY(${n + 2}::qiita.work_ticket_state[])"
        " RETURNING work_ticket_idx",
        *set_params,
        work_ticket_idx,
        allowed_states,
    )
    if updated is None:
        actual = await pool.fetchval(
            "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        raise RuntimeError(
            f"could not {action} work_ticket {work_ticket_idx}: "
            f"expected state in {allowed_states}, got {actual!r}"
        )


async def _atomic_transition(
    pool: asyncpg.Pool | asyncpg.Connection,
    work_ticket_idx: int,
    *,
    expected: WorkTicketState,
    new: WorkTicketState,
) -> None:
    """Guarded single-state transition (expected → new). Raises if the row isn't
    in `expected` — surfacing a stuck ticket instead of overwriting it. Accepts a
    pool or a live Connection (the finalize block fires this in its transaction)."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause="state = $1::qiita.work_ticket_state",
        set_params=[new.value],
        allowed_states=[expected.value],
        action=f"transition to {new.value!r}",
    )


async def _transition_to_processing_for_resume(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Move a ticket to PROCESSING from any non-terminal state, for startup
    recovery re-driving an in-flight ticket. Unlike `_atomic_transition`
    (single expected state), this accepts PENDING / QUEUED / PROCESSING so
    recovery doesn't need to know exactly where the crash left it; a
    PROCESSING → PROCESSING is a harmless no-op. Raises on a terminal ticket
    — recovery should never be handed one."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause="state = $1::qiita.work_ticket_state",
        set_params=[WorkTicketState.PROCESSING.value],
        allowed_states=_NON_TERMINAL_STATES,
        action="resume to processing",
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
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause="state = $1::qiita.work_ticket_state, retry_count = retry_count + 1",
        set_params=[WorkTicketState.QUEUED.value],
        allowed_states=[WorkTicketState.PROCESSING.value],
        action="bump retry on",
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
    a buggy second call doesn't overwrite a COMPLETED state.

    A genuine failure ends any in-place-retry episode: the transient marker is
    cleared so the FAILED ticket shows only its real failure surface, not a
    stale "stuck retrying" reason."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause=(
            "state = $1::qiita.work_ticket_state,"
            " failure_type = $2::qiita.failure_type,"
            " failure_stage = $3::qiita.work_ticket_failure_stage,"
            " failure_step_name = $4,"
            " failure_reason = $5,"
            " transient_reason = NULL,"
            " transient_since = NULL"
        ),
        set_params=[
            WorkTicketState.FAILED.value,
            failure_type.value,
            failure_stage.value,
            failure_step_name,
            failure_reason,
        ],
        allowed_states=_NON_TERMINAL_STATES,
        action="mark FAILED",
    )


async def _transition_to_no_data(pool: asyncpg.Pool, work_ticket_idx: int) -> None:
    """Atomic transition into NO_DATA — the terminal outcome for a step that
    legitimately produced no data (an empty FASTQ well).

    Distinct from `_transition_to_failed`: NO_DATA is not a failure, so all four
    failure_* columns are explicitly written NULL (honouring the DB's
    `work_ticket_failure_consistent` all-or-nothing CHECK from the
    none-populated side) and the transient-retry marker is cleared. Accepts a
    transition from any non-terminal state (PROCESSING most commonly, or QUEUED
    if a retry's requeue raced shutdown); refuses an already-terminal ticket so
    a buggy second call can't overwrite a COMPLETED/FAILED state."""
    await _guarded_state_update(
        pool,
        work_ticket_idx,
        set_clause=(
            "state = $1::qiita.work_ticket_state,"
            " failure_type = NULL,"
            " failure_stage = NULL,"
            " failure_step_name = NULL,"
            " failure_reason = NULL,"
            " transient_reason = NULL,"
            " transient_since = NULL"
        ),
        set_params=[WorkTicketState.NO_DATA.value],
        allowed_states=_NON_TERMINAL_STATES,
        action="mark NO_DATA",
    )


def _safe_entry_name(action: ActionDefinition | None, index: int | None) -> str | None:
    """Best-effort lookup of the entry name at `index`. Returns None if
    `action` is unresolved (a pre-loop failure never fetched it) or the index
    is out of range (e.g. action.steps is empty so the loop never iterated).
    When the loop body has executed at least once, `index` is the most recent
    entry — the natural name to record on failure."""
    if action is None or index is None:
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

    This is the *whole-reference* (unsharded) lookup: it filters to
    `shard_id IS NULL` so a per-shard analysis-index row can never be served
    here. All rows are NULL today, so this is a no-op now and forward-safe once
    shard rows exist. Shard-aware resolution (routing a read to its shard) is a
    later milestone and is deliberately NOT built here.

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
        " WHERE reference_idx = $1 AND index_type = $2 AND shard_id IS NULL"
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
# Read ingest + staged-read bindings
# =============================================================================
#
# The bcl-convert workflow's `ingest_reads` step stores the pool's reads once;
# the repeatable read-mask workflow consumes them from a durable, prep_sample-
# addressable copy. Two runner-side bindings bridge the orchestrator's lack of
# DB access:
#   * `sample_map`  — the `{prep_sample_idx, pool_item_id}` roster the CP knows
#     and the ingest step needs, materialized to a Parquet (like the adapter
#     set) because `params:` only carry scalars.
#   * `reads`       — bound from `compute_reads_staging_path` for a mask
#     workflow, which has no step that produces reads.
# `reads_staging_root` hands the ingest step the scratch root it writes the
# durable copies under.

SAMPLE_MAP_BINDING = "sample_map"
STAGED_READS_BINDING = "reads"
READS_STAGING_ROOT_BINDING = "reads_staging_root"

# Bindings a sharded build ticket's build steps consume: the per-shard feature
# roster Parquet (`shard_features`) and the shard ordinal (`shard_id`). The
# runner stages both BEFORE the step loop (see `_stage_shard_roster`), from the
# ticket's `shard_id` + `reference_membership.shard_id`; the shard build jobs
# (build_rype/minimap2/bowtie2_index) resolve them as their `Inputs`.
SHARD_FEATURES_BINDING = "shard_features"
SHARD_ID_BINDING = "shard_id"
_REFERENCE_SEQUENCES_TABLE = "reference_sequences"


def _do_get_reference_sequences_roster(
    data_plane_url: str, ticket_bytes: bytes, out_path: Path
) -> int:
    """Synchronous Flight DoGet of a feature-scoped `reference_sequences` slice,
    written to a `(feature_idx BIGINT, sequence_length_bp BIGINT)` roster Parquet
    at `out_path`. Runs in a thread executor (pyarrow.flight is sync); isolated
    so `_stage_shard_roster`'s unit test stubs the whole seam. Returns the row
    count (the shard's feature count that has a sequence)."""
    import pyarrow.flight as flight  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    with flight.FlightClient(data_plane_url) as client:
        table = client.do_get(flight.Ticket(ticket_bytes)).read_all()
    # Project to exactly the roster columns the build jobs expect (drop
    # sequence_hash) — the shard build reads `feature_idx` to scope its own chunk
    # stream and `sequence_length_bp` for plan() sizing.
    roster = table.select(["feature_idx", "sequence_length_bp"])
    pq.write_table(roster, str(out_path), compression="snappy")
    return roster.num_rows


async def _stage_shard_roster(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    shard_id: int,
    *,
    data_plane_url: str,
    hmac_secret: bytes,
    workspace: Path,
) -> dict[str, Any]:
    """Stage this shard's feature roster before the build step loop and bind it.

    The shard's features are the cover-map (`reference_membership.shard_id`);
    their `sequence_length_bp` lives in DuckLake `reference_sequences`, reachable
    only over Flight. So we read the shard's feature_idx set from Postgres, sign
    a `feature_idx`-scoped `reference_sequences` DoGet (the B6 subset ticket — so
    each shard transfers only its own slice, not the whole reference N times),
    and write `<workspace>/shard_roster.parquet`. Binds `shard_features` (the
    roster path) and `shard_id` so the build steps' `Inputs` resolve.

    Like the other pre-loop resolvers, a Flight failure is wrapped as a
    SUBMISSION-attributed BAD_INPUT so it lands in the outer FAILED handler
    instead of escaping as an untyped exception (which would violate the
    step-name CHECK). An empty membership shard is a misconfiguration — fail
    loud rather than build an empty index."""
    rows = await pool.fetch(
        "SELECT feature_idx FROM qiita.reference_membership"
        " WHERE reference_idx = $1 AND shard_id = $2",
        reference_idx,
        shard_id,
    )
    feature_idxs = [r["feature_idx"] for r in rows]
    if not feature_idxs:
        raise _submission_bad_input(
            f"shard {shard_id} of reference {reference_idx} has no member features "
            "(reference_membership.shard_id) — nothing to build"
        )
    ticket = sign_ticket(
        table=_REFERENCE_SEQUENCES_TABLE,
        filter={"reference_idx": [reference_idx], "feature_idx": feature_idxs},
        secret=hmac_secret,
    )
    workspace.mkdir(parents=True, exist_ok=True)
    roster_path = workspace / "shard_roster.parquet"
    try:
        await asyncio.get_event_loop().run_in_executor(
            None, _do_get_reference_sequences_roster, data_plane_url, ticket, roster_path
        )
    except Exception as exc:
        raise _submission_bad_input(
            f"could not fetch reference_sequences for reference {reference_idx} "
            f"shard {shard_id} from the data plane: {type(exc).__name__}: {exc}"
        ) from exc
    return {SHARD_FEATURES_BINDING: roster_path, SHARD_ID_BINDING: shard_id}


def _workflow_declares_input(steps: list[Any], name: str) -> bool:
    """True iff some entry declares `name` among its `inputs`/`optional_inputs`."""
    for entry in steps:
        names = list(getattr(entry, "inputs", []) or []) + list(
            getattr(entry, "optional_inputs", []) or []
        )
        if name in names:
            return True
    return False


def _workflow_needs_staged_reads(steps: list[Any]) -> bool:
    """True iff `reads` is consumed by some step but produced by none — so it must
    be bound externally from the prep_sample's stored reads (the read-mask
    workflow). The bcl-convert workflow produces reads internally (`ingest_reads`
    emits `read_staging_dir`, not `reads`), so it does not match."""
    if not _workflow_declares_input(steps, STAGED_READS_BINDING):
        return False
    for entry in steps:
        if STAGED_READS_BINDING in (getattr(entry, "outputs", []) or []):
            return False
    return True


def _write_sample_map_parquet(roster: list[dict[str, Any]], out_path: Path) -> None:
    """Write the `{prep_sample_idx, pool_item_id}` roster to a Parquet
    `(prep_sample_idx BIGINT, pool_item_id VARCHAR)` for the ingest step.
    pyarrow (already a Flight dependency) writes it directly — no DuckDB needed
    on the pre-loop path, mirroring `_write_adapter_parquet`."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    prep = [int(r["prep_sample_idx"]) for r in roster]
    items = [str(r["pool_item_id"]) for r in roster]
    table = pa.table(
        {
            "prep_sample_idx": pa.array(prep, type=pa.int64()),
            "pool_item_id": pa.array(items, type=pa.string()),
        }
    )
    pq.write_table(table, str(out_path))


async def _resolve_sample_map(action_context: dict[str, Any], workspace: Path) -> dict[str, Path]:
    """Materialize the bcl-convert pool roster from action_context into a local
    Parquet for the `ingest_reads` step. Same pre-loop, inside-try placement as
    the other resolvers so a failure lands in the outer FAILED handler. Raises a
    SUBMISSION-attributed BAD_INPUT on a missing/empty roster."""
    roster = action_context.get(SAMPLE_MAP_BINDING)
    if not roster:
        raise _submission_bad_input(
            "an ingest workflow requires a non-empty `sample_map` roster in "
            "action_context (the CP embeds it at submit-bcl-convert time)"
        )
    workspace.mkdir(parents=True, exist_ok=True)
    out = workspace / "sample_map.parquet"
    _write_sample_map_parquet(roster, out)
    return {SAMPLE_MAP_BINDING: out}


def _do_action_export(action_type: str, data_plane_url: str, token: bytes) -> dict[str, Any]:
    """Shared body for the read-export DoActions (`export_read`,
    `export_read_block`): run a synchronous Flight DoAction of `action_type` in a
    thread executor (pyarrow.flight is sync). The data plane writes the file; the
    bulk read bytes never transit the control plane. Returns `{"count": int,
    "dest": str}` with `count` already coerced to int, raising ValueError on a
    missing/garbled body or a non-integer `count` so the caller (inside its
    `except`) turns it into a clean SUBMISSION failure rather than a cryptic
    backtrace. Mirrors `actions.library._do_action_register`."""
    import pyarrow.flight as flight  # noqa: PLC0415

    with flight.FlightClient(data_plane_url) as client:
        results = list(client.do_action(flight.Action(action_type, token)))
    if not results:
        return {"count": 0, "dest": ""}
    body = results[0].body.to_pybytes()
    if not body:
        return {"count": 0, "dest": ""}
    # Parse + coerce here (inside the executor, so the caller's `except` wraps any
    # failure) — never hand the caller a `count` it must coerce outside its try.
    try:
        parsed = json.loads(body)
        count = int(parsed["count"])
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise ValueError(f"{action_type} returned an unparseable result body: {exc!r}") from exc
    return {"count": count, "dest": str(parsed.get("dest", ""))}


def _do_action_export_read(data_plane_url: str, token: bytes) -> dict[str, Any]:
    """`export_read` DoAction: the data plane re-materializes ONE prep_sample's
    reads from its DuckLake `read` table into a per-ticket Parquet on shared
    scratch. Isolated (thin wrapper over `_do_action_export`) so unit tests stub
    the real call by name."""
    return _do_action_export("export_read", data_plane_url, token)


def _do_action_export_read_block(data_plane_url: str, token: bytes) -> dict[str, Any]:
    """`export_read_block` DoAction: the data plane materializes the UNION of a
    block's `(prep_sample_idx, sequence_idx sub-range)` members from its DuckLake
    `read` table into one per-ticket Parquet. Isolated (thin wrapper over
    `_do_action_export`) so unit tests stub the real call by name."""
    return _do_action_export("export_read_block", data_plane_url, token)


async def _resolve_staged_reads(
    scope_target: dict[str, Any],
    staging_root: Path,
    *,
    data_plane_url: str,
    hmac_secret: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Bind `reads` to the prep_sample's stored reads for a read-mask workflow.

    Fast path: the durable staging copy `ingest_reads` wrote
    (`compute_reads_staging_path`). That copy is ephemeral, so when it is gone
    (reprocessing a run stored earlier) fall back to the PERSISTENT store: ask the
    data plane to re-materialize the sample's reads from its DuckLake `read` table
    into a per-ticket `reads.parquet` via the `export_read` DoAction (the data
    plane writes the file; the bulk read bytes never transit the control plane).
    Either source binds the same `reads` path; they are byte-equivalent modulo row
    order, and qc / host_filter are order-independent.

    Fails SUBMISSION/BAD_INPUT (so the ticket FAILs cleanly) if the sample has no
    stored reads in either place — it must be ingested before a mask can be
    created over it — or if the data plane is unreachable."""
    prep_sample_idx = scope_target["prep_sample_idx"]

    durable = compute_reads_staging_path(staging_root, prep_sample_idx)
    if durable.exists():
        return {STAGED_READS_BINDING: durable}

    # Ephemeral durable copy gone — source from the persistent DuckLake `read`
    # table. We name the per-ticket destination (under the shared scratch tree the
    # data plane validates); the data plane writes it.
    workspace.mkdir(parents=True, exist_ok=True)
    dest = workspace / "reads.parquet"
    token = sign_action(
        action="export_read",
        payload={"prep_sample_idx": prep_sample_idx, "dest": str(dest)},
        secret=hmac_secret,
    )
    # A Flight failure (data plane unreachable / errored) is NOT a BackendFailure;
    # wrap it as a SUBMISSION BAD_INPUT like the other pre-loop resolvers so the
    # outer handler FAILs the ticket cleanly (step_name=None) rather than letting
    # an untyped exception strand it in PROCESSING. (Not retried in place: the
    # operator resubmits if the data plane was down.)
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, _do_action_export_read, data_plane_url, token
        )
    except Exception as exc:
        raise _submission_bad_input(
            f"could not materialize reads for prep_sample {prep_sample_idx} from "
            f"the data plane: {type(exc).__name__}: {exc}"
        ) from exc

    # `count` is already an int (coerced in `_do_action_export_read`).
    if result.get("count", 0) == 0:
        # The persistent store has no reads for this sample either (the data plane
        # writes no file for an empty result) — same "must be ingested" semantics.
        raise _submission_bad_input(
            f"no stored reads for prep_sample {prep_sample_idx}; the sample must be "
            "ingested (submit-bcl-convert stores reads) before a read mask can be "
            "created over it"
        )
    if not dest.exists():
        # The data plane reported reads but no file landed at dest (a data-plane
        # bug, a full disk, or a mid-write failure). Fail at submission rather than
        # handing a downstream step a path that isn't there.
        raise _submission_bad_input(
            f"the data plane reported reads for prep_sample {prep_sample_idx} but "
            f"wrote no file at {dest}"
        )
    return {STAGED_READS_BINDING: dest}


async def _resolve_staged_reads_block(
    members: list[dict[str, int]],
    *,
    data_plane_url: str,
    hmac_secret: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Bind `reads` to a BLOCK's reads for a read-mask-block workflow — the
    multi-sample analog of `_resolve_staged_reads`.

    A block spans a set of `(prep_sample_idx, sequence_idx sub-range)` `members`
    that all resolve to one `mask_idx`. Because a block may hold only a sub-range
    of a large sample, the per-sample durable staging copy cannot serve it, so we
    always source from the PERSISTENT DuckLake `read` table: ask the data plane to
    materialize the union of the members' sub-ranges into a per-ticket
    `reads.parquet` via the `export_read_block` DoAction (the data plane writes
    the file; the bulk read bytes never transit the control plane). `qc` /
    `host_filter` read `prep_sample_idx` per-row, so a multi-sample file is fine.

    Fails SUBMISSION/BAD_INPUT (so the ticket FAILs cleanly, step_name=None) if:
    `members` is empty (a planning bug); the data plane is unreachable; the block
    selects zero reads (its members' ranges match nothing — a planning bug, since
    blocks are tiled from `sequence_range` bounds that must exist); or the data
    plane reported reads but no file landed."""
    if not members:
        raise _submission_bad_input("a read-mask block requires a non-empty members list")
    workspace.mkdir(parents=True, exist_ok=True)
    dest = workspace / "reads.parquet"
    # Coerce the member shape up front (a malformed member is a planner bug):
    # a missing key or non-int value must FAIL the ticket cleanly as BAD_INPUT,
    # not escape as an untyped KeyError/TypeError that strands it in PROCESSING.
    try:
        member_payload = [
            {
                "prep_sample_idx": int(m["prep_sample_idx"]),
                "sequence_idx_start": int(m["sequence_idx_start"]),
                "sequence_idx_stop": int(m["sequence_idx_stop"]),
            }
            for m in members
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise _submission_bad_input(
            f"malformed read-mask block member (a planning bug): {type(exc).__name__}: {exc}"
        ) from exc
    token = sign_action(
        action="export_read_block",
        payload={"dest": str(dest), "members": member_payload},
        secret=hmac_secret,
    )
    # A Flight failure (data plane unreachable / errored) is NOT a BackendFailure;
    # wrap it as a SUBMISSION BAD_INPUT like the per-sample resolver so the outer
    # handler FAILs the ticket cleanly rather than stranding it in PROCESSING.
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, _do_action_export_read_block, data_plane_url, token
        )
    except Exception as exc:
        raise _submission_bad_input(
            f"could not materialize reads for the block from the data plane: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    # `count` is already an int (coerced in `_do_action_export`).
    if result.get("count", 0) == 0:
        raise _submission_bad_input(
            "the block selected zero reads from the data plane; its members' "
            "sequence_idx ranges match no stored reads (a planning bug — blocks "
            "are tiled from qiita.sequence_range bounds that must exist)"
        )
    if not dest.exists():
        raise _submission_bad_input(
            f"the data plane reported reads for the block but wrote no file at {dest}"
        )
    return {STAGED_READS_BINDING: dest}


# =============================================================================
# Read-mask identity (mask_idx) minting
# =============================================================================
#
# A read mask's identity is its filtering CONFIG: the filter workflow + version,
# the host reference(s) it depletes against, and the resolved QC config. The
# control plane mints a `mask_idx` deduplicated on the SHA-256 of that config so
# the same config resolves to the same mask_idx fleet-wide; the host_filter step
# stamps it onto every read_mask row. The host references are read from the
# sequenced_sample row (where they are pinned at pool fan-out); the resolved QC
# values mirror the qc job's fastp-equivalent constants so a metadata edit to a
# protocol row that doesn't change the effective filter yields the same mask.

# Binding name the runner threads the minted mask_idx under. The host_filter step
# lists it in its `params:` (mask_idx -> host_filter.Inputs.mask_idx), which both
# signals the runner to mint the mask before the step loop and carries the value
# into the step.
MASK_IDX_BINDING = "mask_idx"

# Resolved QC config the mask hash covers — the effective fastp-equivalent
# filter the qc job applies. Mirrors the constants in
# qiita_compute_orchestrator.jobs.qc (the fastp `-l 100` defaults); kept here
# (not imported) because the control plane does not depend on the orchestrator
# package. A change to the qc filter must update both so the mask identity stays
# faithful to the filter actually applied.
_QC_RESOLVED_MIN_LENGTH = 100
_QC_RESOLVED_FILTER_TAIL = "0, 15, 40, 5, 0"


def _workflow_needs_mask(steps: list[Any]) -> bool:
    """True iff some entry threads `mask_idx` through its `params:` — the signal
    the runner must mint a read mask before the step loop. Mirrors
    `_workflow_needs_adapters` (which keys off an input binding); the mask is a
    scalar param, so it keys off `params` values instead."""
    for entry in steps:
        params = getattr(entry, "params", None) or {}
        if MASK_IDX_BINDING in params.values():
            return True
    return False


def _adapter_set_hash(adapter_parquet: Path) -> str:
    """SHA-256 hex of the materialized adapter-set Parquet's bytes — the resolved
    adapter identity for the mask config hash. Hashing the staged file (not the
    reference idx) keeps the mask identity tied to the adapter bytes actually
    applied, so a re-pointed-but-identical adapter set collapses to one mask.

    Note the hash is over the SERIALIZED Parquet bytes, not the logical sequence
    set: mint and backfill agree only because both materialize the adapter Parquet
    through the same `_write_adapter_parquet` / pyarrow writer. A writer change
    that alters the byte layout shifts this hash and would force a re-mint rather
    than collapsing to the existing mask — it is an assumption, not something the
    code enforces."""
    return hashlib.sha256(adapter_parquet.read_bytes()).hexdigest()


def _build_mask_params(
    *,
    action_id: str,
    action_version: str,
    prep_protocol_idx: int | None,
    instrument_model: str | None,
    adapter_set_hash: str | None,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
) -> dict[str, Any]:
    """Assemble the resolved-filter-config dict that `mint_mask_definition`
    hashes (canonical JSON → SHA-256 → `params_hash`) to mint/dedup a mask.

    This is the SINGLE source of truth for the mask's identity shape — both the
    mint path (`_mint_read_mask`) and the legacy backfill
    (`backfill_work_ticket_mask_idx`) call it so the two derive the SAME hash for
    the SAME effective config. Every value is the EFFECTIVE filter (the host refs
    the filter applies + adapter bytes hash + thresholds), so two callers with the
    same effective config collapse to one mask even if descriptive metadata
    differs. `adapter_set_hash` is passed in already computed (the SHA-256 hex of
    the materialized adapter Parquet, via `_adapter_set_hash`) rather than a file
    path, so the backfill can supply it from a re-materialized adapter set without
    this helper touching the filesystem.

    Any change to the keys, nesting, or resolved-QC constants here changes every
    mask's identity fleet-wide — keep it deterministic and keyed only on the
    effective filter.
    """
    return {
        "filter_workflow": action_id,
        "filter_version": action_version,
        "host_rype_reference_idx": host_rype_reference_idx,
        "host_minimap2_reference_idx": host_minimap2_reference_idx,
        "prep_protocol_idx": prep_protocol_idx,
        "resolved_qc": {
            "instrument_model": instrument_model,
            "min_length": _QC_RESOLVED_MIN_LENGTH,
            "filter_read_tail": _QC_RESOLVED_FILTER_TAIL,
            "adapter_set_hash": adapter_set_hash,
        },
    }


async def _mint_read_mask(
    pool: asyncpg.Pool,
    *,
    action_id: str,
    action_version: str,
    prep_sample_idx: int,
    originator_principal_idx: int,
    instrument_model: str | None,
    adapter_parquet: Path | None,
    host_rype_reference_idx: int | None,
    host_minimap2_reference_idx: int | None,
) -> dict[str, int]:
    """Mint (or resolve) the `mask_idx` for this filtering config and bind it.

    Run before the step loop when `_workflow_needs_mask`. The config is:
      * the filter workflow + version (this action),
      * the host reference(s) the `host_filter` step actually APPLIES, passed in
        from the same action_context values `_resolve_host_filter_indexes`
        consumes (`host_rype_reference_idx` / `host_minimap2_reference_idx`) — so
        the minted mask_idx's params describe the filter that ran. Absent host
        refs mean no host filtering, a faithful part of the config (None), and
      * the resolved QC config (instrument model gating polyG, the fastp-`-l 100`
        thresholds, and a hash of the materialized adapter set).
    `mint_mask_definition` hashes `params` (canonical JSON) and upserts on it, so
    the same effective config resolves to the same mask_idx fleet-wide.

    Like the other pre-loop resolvers, any failure raises a SUBMISSION-attributed
    BAD_INPUT the outer handler turns into a FAILED ticket: no sequenced_sample
    row (the sample must be pooled first), or an unknown originator principal.
    """
    prep_protocol_idx = await pool.fetchval(
        "SELECT ps.prep_protocol_idx"
        "  FROM qiita.sequenced_sample ss"
        "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE ss.prep_sample_idx = $1",
        prep_sample_idx,
    )
    if prep_protocol_idx is None:
        # fetchval returns None both when no row matched and when the column is
        # NULL; distinguish by re-checking row existence so a real "not pooled"
        # error keeps its specific message and a legitimately-NULL prep protocol
        # still mints.
        row_exists = await pool.fetchval(
            "SELECT 1 FROM qiita.sequenced_sample WHERE prep_sample_idx = $1",
            prep_sample_idx,
        )
        if row_exists is None:
            raise _submission_bad_input(
                f"no sequenced_sample row for prep_sample_idx={prep_sample_idx}; the "
                "sample must be pooled (its 1:1 sequenced_sample created) before a "
                "read mask can be minted"
            )

    # Resolved config — assembled by the shared `_build_mask_params` so the mint
    # path and the legacy backfill derive the SAME hash for the same effective
    # config. The adapter identity is the SHA-256 of the materialized adapter
    # bytes (None when this workflow uses no adapter set).
    params = _build_mask_params(
        action_id=action_id,
        action_version=action_version,
        prep_protocol_idx=prep_protocol_idx,
        instrument_model=instrument_model,
        adapter_set_hash=(
            _adapter_set_hash(adapter_parquet) if adapter_parquet is not None else None
        ),
        host_rype_reference_idx=host_rype_reference_idx,
        host_minimap2_reference_idx=host_minimap2_reference_idx,
    )

    try:
        async with pool.acquire() as conn:
            mask_row = await mint_mask_definition(
                conn,
                filter_workflow=action_id,
                filter_version=action_version,
                params=params,
                principal_idx=originator_principal_idx,
            )
    except asyncpg.ForeignKeyViolationError as exc:
        raise _submission_bad_input(
            f"could not mint read mask: originator principal "
            f"{originator_principal_idx} does not exist"
        ) from exc
    return {MASK_IDX_BINDING: mask_row["mask_idx"]}


async def _persist_mask_idx(pool: asyncpg.Pool, work_ticket_idx: int, mask_idx: int) -> None:
    """Write the minted `mask_idx` onto the ticket row (durable ticket→mask
    traceability + a cheap shared-mask guard). Idempotent: a re-mint on resume
    re-resolves to the same mask_idx via the config-hash upsert, so re-running
    this writes the same value. Like every runner DB write it fails loud — a PG
    outage raises and unwinds the run via run_workflow's catch-all."""
    await pool.execute(
        "UPDATE qiita.work_ticket SET mask_idx = $1 WHERE work_ticket_idx = $2",
        mask_idx,
        work_ticket_idx,
    )


# Actions whose tickets thread a `mask_idx`; the backfill scopes to these so it
# never touches a ticket that never minted a mask. Keep in sync with the
# workflows that declare `_workflow_needs_mask` (read-mask + fastq-to-parquet).
_MASK_BEARING_ACTION_IDS = ("read-mask", "fastq-to-parquet")


async def _materialize_backfill_adapter_set_hash(
    pool: asyncpg.Pool,
    *,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    hmac_secret: bytes,
    workspace: Path,
) -> str | None:
    """Re-derive the canonical adapter-set hash for the backfill, once.

    Every read-mask / fastq-to-parquet ticket masks against the SAME canonical
    adapter set (`default_adapter_reference_idx`), so the `adapter_set_hash` that
    feeds `_build_mask_params` is identical across all of them. We re-materialize
    the adapter Parquet once via the same DoGet path the mint uses
    (`_resolve_qc_adapters`) and hash its bytes (`_adapter_set_hash`). The hash is
    over the SERIALIZED Parquet bytes, so this reproduces the mint's hash only as
    long as the backfill runs under the same pyarrow/Parquet writer the mint did:
    a writer change that alters the on-disk byte layout would shift the hash and
    force a re-mint rather than a backfill match. Returns None when no default
    adapter reference is configured (a deploy that mints maskless / for a test
    seam) — the caller then builds params with `adapter_set_hash=None`.
    """
    if default_adapter_reference_idx is None:
        return None
    bound = await _resolve_qc_adapters(
        pool,
        default_adapter_reference_idx=default_adapter_reference_idx,
        data_plane_url=data_plane_url,
        hmac_secret=hmac_secret,
        workspace=workspace,
    )
    return _adapter_set_hash(bound[QC_ADAPTER_BINDING])


async def backfill_work_ticket_mask_idx(
    pool: asyncpg.Pool,
    *,
    workspace: Path,
    default_adapter_reference_idx: int | None,
    data_plane_url: str,
    hmac_secret: bytes,
    apply: bool,
) -> dict[str, Any]:
    """One-time, idempotent backfill of `work_ticket.mask_idx` for existing
    read-mask / fastq-to-parquet tickets created before the column existed.

    For each such ticket with `mask_idx IS NULL`, reconstruct the filtering
    config the runner hashed at mint (the SAME `_build_mask_params` shape, fed by
    the ticket's stored `action_context` + the prep_protocol_idx join + the
    re-materialized canonical adapter-set hash), then LOOK UP the matching
    `mask_definition` row (`lookup_mask_idx_by_params`). On a hit, set the
    ticket's `mask_idx`; on a miss (the ticket failed before minting, or its
    config drifted off the current hash logic) SKIP it and record it — the
    backfill NEVER mints a new mask.

    Scoped to `_MASK_BEARING_ACTION_IDS` and to `mask_idx IS NULL`, so it is
    idempotent: a second run finds nothing left to populate. Processes tickets in
    ANY state (not just failed) so a COMPLETED ticket that SHARES a mask is
    populated too — the shared-mask guard reads this column.

    `apply=False` is a dry run: it computes the same hit/miss classification and
    reports what it WOULD do without writing. `apply=True` writes inside a single
    transaction. Returns a report dict: counted / populated / skipped_no_mask /
    skipped_not_prep_sample, plus the skipped ticket idxs.
    """
    adapter_set_hash = await _materialize_backfill_adapter_set_hash(
        pool,
        default_adapter_reference_idx=default_adapter_reference_idx,
        data_plane_url=data_plane_url,
        hmac_secret=hmac_secret,
        workspace=workspace,
    )

    rows = await pool.fetch(
        "SELECT work_ticket_idx, action_id, action_version, prep_sample_idx, action_context"
        "  FROM qiita.work_ticket"
        " WHERE mask_idx IS NULL"
        "   AND action_id = ANY($1::text[])"
        " ORDER BY work_ticket_idx",
        list(_MASK_BEARING_ACTION_IDS),
    )

    populated: list[dict[str, int]] = []
    skipped_no_mask: list[int] = []
    skipped_not_prep_sample: list[int] = []

    for row in rows:
        ticket_idx = row["work_ticket_idx"]
        prep_sample_idx = row["prep_sample_idx"]
        if prep_sample_idx is None:
            # A mask keys on a prep_sample's reads; a ticket of these actions with
            # no prep_sample never minted a mask. Record and skip rather than
            # crash on the prep_protocol join below.
            skipped_not_prep_sample.append(ticket_idx)
            continue

        action_context = row["action_context"]
        if isinstance(action_context, str):
            # action_context is JSONB; asyncpg returns it as a string unless a
            # JSON codec is registered. Decode the same way _fetch_work_ticket does.
            action_context = json.loads(action_context)
        action_context = action_context or {}

        prep_protocol_idx = await pool.fetchval(
            "SELECT ps.prep_protocol_idx"
            "  FROM qiita.sequenced_sample ss"
            "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
            " WHERE ss.prep_sample_idx = $1",
            prep_sample_idx,
        )

        # Read host refs + instrument_model straight off action_context — the same
        # keys `_mint_read_mask` reads from `bound` (the resolvers add host_*_path
        # bindings but never overwrite these reference-idx keys), so the
        # reconstructed config reproduces the minted one. The adapter_set_hash
        # component is over the serialized adapter Parquet bytes, so this match
        # holds only while backfill and mint run under the same Parquet writer (see
        # `_adapter_set_hash`); a writer change would force a re-mint.
        params = _build_mask_params(
            action_id=row["action_id"],
            action_version=row["action_version"],
            prep_protocol_idx=prep_protocol_idx,
            instrument_model=action_context.get("instrument_model"),
            adapter_set_hash=adapter_set_hash,
            host_rype_reference_idx=action_context.get("host_rype_reference_idx"),
            host_minimap2_reference_idx=action_context.get("host_minimap2_reference_idx"),
        )

        mask_idx = await lookup_mask_idx_by_params(pool, params)
        if mask_idx is None:
            skipped_no_mask.append(ticket_idx)
            continue
        populated.append({"work_ticket_idx": ticket_idx, "mask_idx": mask_idx})

    if apply and populated:
        async with pool.acquire() as conn, conn.transaction():
            for item in populated:
                # Re-guard on mask_idx IS NULL in the WHERE so a concurrent mint
                # (or a prior partial run) is never clobbered; idempotent.
                await conn.execute(
                    "UPDATE qiita.work_ticket SET mask_idx = $1"
                    " WHERE work_ticket_idx = $2 AND mask_idx IS NULL",
                    item["mask_idx"],
                    item["work_ticket_idx"],
                )

    return {
        "applied": apply,
        "counted": len(rows),
        "populated": len(populated),
        "populated_detail": populated,
        "skipped_no_mask": skipped_no_mask,
        "skipped_not_prep_sample": skipped_not_prep_sample,
    }


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


async def _shard_fanout_owns_finalize(
    conn: asyncpg.Connection,
    scope_target: dict[str, Any],
    bound: dict[str, Any],
) -> bool:
    """True when this ticket kicked off a sharded fan-out now in progress, so the
    parent action's finalize must NOT apply its success_status — the terminal
    `finalize-shard` owns `indexing → active` once every shard registers.

    Fires only for a reference-scoped ticket whose action_context set
    `shard_index` AND whose reference is currently `indexing` (plan-shards
    transitioned it because N > 0). A sharded reference with no genomes stays
    `loading` (N = 0, no fan-out) → this returns False → the parent patches
    `active` inline (nothing to shard). An unsharded reference-add (no
    `shard_index`) also returns False → patches `active` unchanged. `bound`
    carries the action_context (resume-safe: reseeded from the persisted ticket
    context), so this is correct across a CP restart."""
    if not bound.get("shard_index"):
        return False
    if scope_target["kind"] != ScopeTargetKind.REFERENCE.value:
        return False
    status = await conn.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        scope_target["reference_idx"],
    )
    return status == ReferenceStatus.INDEXING.value


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


def _attempt_is_unowned(
    prior_progress: list[step_progress.StepProgressRow], *, step_index: int, attempt: int
) -> bool:
    """Whether this entry's `(step_index, attempt)` is unowned by a start-of-run
    progress row — i.e. the caller may treat any attempt dir on disk as orphaned.

    Keyed on the START-OF-RUN progress (the snapshot loaded once before the
    loop). A pre-existing row for this exact `(step_index, attempt)` means a
    prior process owns the dir and we're resuming/adopting it — `_adopt_or_submit`
    re-attaches to that row's job and must reuse its workspace, so it is NOT
    unowned (return False; leave the dir alone). No such row means the attempt is
    unowned: either a first dispatch (dir absent — the caller just mkdirs it) or a
    re-run whose row was deliberately dropped (a `/run` redrive clearing failed
    rows, or `update-lane` invalidating a completed prep row). In the re-run case
    the prior attempt left stale, read-only (0o440) output + manifest on disk that
    must not be reused; the caller advances to a fresh attempt dir rather than
    deleting it (the output is owned by the SLURM job user — the control plane
    can't unlink or chmod it)."""
    return not any(
        row.step_index == step_index and row.attempt == attempt for row in prior_progress
    )


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
    dispatch_cb: Callable[[int], Any] | None = None,
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
            dispatch_cb=dispatch_cb,
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
    dispatch_cb: Callable[[int], Any] | None = None,
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
        # the builder, not hardcoded here). `shard_id` is optional: a host meta
        # JSON omits it (`.get` -> None -> unsharded row); a sharded analysis
        # index builder emits one meta per shard carrying its shard_id.
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
        if scope_target["kind"] != ScopeTargetKind.REFERENCE.value:
            raise RuntimeError(
                f"plan-shards requires a reference-scoped ticket; got {scope_target['kind']!r}"
            )
        if dispatch_cb is None:
            # Fanning out without a dispatch mechanism would silently strand the
            # shard tickets in PENDING until the next startup reconcile — fail
            # loud instead (dispatch always threads a callback in production).
            raise RuntimeError("plan-shards requires a dispatch_cb to fan out shard tickets")
        originator_principal_idx = await pool.fetchval(
            "SELECT originator_principal_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        shard_context = {k: bound[k] for k in SHARD_BUILD_CONTEXT_KEYS if k in bound}
        await plan_and_submit_shards(
            pool,
            scope_target["reference_idx"],
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
            workspace=workspace,
            originator_principal_idx=originator_principal_idx,
            build_action_id=BUILD_SHARD_INDEX_ACTION_ID,
            build_action_version=BUILD_SHARD_INDEX_ACTION_VERSION,
            action_context=shard_context,
            dispatch_cb=dispatch_cb,
        )
        return {}

    if entry.name == LibraryPrimitive.FINALIZE_SHARD:
        # Terminal step of a build-shard-index ticket: count-based, fail-closed
        # completion. The expected index_types are derived from THIS ticket's
        # build gates (in `bound`), so finalize counts exactly what was built.
        # No file inputs; reference_idx from the scope target.
        if scope_target["kind"] != ScopeTargetKind.REFERENCE.value:
            raise RuntimeError(
                f"finalize-shard requires a reference-scoped ticket; got {scope_target['kind']!r}"
            )
        await LIBRARY[LibraryPrimitive.FINALIZE_SHARD](
            pool,
            scope_target["reference_idx"],
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
            raise RuntimeError(
                f"persist-read-metrics expects inputs [read_mask]; got {entry.inputs!r}"
            )
        await LIBRARY[LibraryPrimitive.PERSIST_READ_METRICS](
            pool,
            scope_target["prep_sample_idx"],
            Path(bound["read_mask"]),
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

    if entry.name == LibraryPrimitive.DELETE_READ_MASK_BLOCK:
        # Idempotent block replace: delete this block's exact read_mask footprint
        # BEFORE register-files re-writes it, so a re-run (retry, or a resubmitted
        # block covering the same footprint) never double-counts. Exact by
        # construction (per-member OR), so a split sample's sibling-block rows
        # survive. No file inputs: block_idx from the scope target, mask_idx from
        # the ticket (runner-bound above for the block branch).
        if scope_target["kind"] != ScopeTargetKind.BLOCK.value:
            raise RuntimeError(
                f"delete-block-mask requires a block-scoped ticket; got {scope_target['kind']!r}"
            )
        await LIBRARY[LibraryPrimitive.DELETE_READ_MASK_BLOCK](
            pool,
            block_idx=scope_target["block_idx"],
            mask_idx=bound[MASK_IDX_BINDING],
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.RECONCILE_BLOCK:
        # Terminal step of the bulk-block read-mask workflow: mark this block
        # completed, then finalize each covered sample whose last covering block
        # just completed (per-sample rollup + mask_sample gate flip). Reads the
        # mask counts from DuckLake (across all the sample's blocks), so it runs
        # AFTER register-files. No file inputs: block_idx from the scope target,
        # mask_idx from the ticket (runner-bound above for the block branch).
        if scope_target["kind"] != ScopeTargetKind.BLOCK.value:
            raise RuntimeError(
                f"reconcile-block requires a block-scoped ticket; got {scope_target['kind']!r}"
            )
        await LIBRARY[LibraryPrimitive.RECONCILE_BLOCK](
            pool,
            block_idx=scope_target["block_idx"],
            mask_idx=bound[MASK_IDX_BINDING],
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {}

    raise RuntimeError(f"runner has no adapter for action {entry.name!r}")
