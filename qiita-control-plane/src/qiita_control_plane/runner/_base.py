"""Runner shared base — constants, the WorkflowAborted signal, and transient-retry helpers."""

from __future__ import annotations

import asyncio
import logging

import asyncpg
from qiita_common.backend_failure import FailureKind
from qiita_common.models import (
    TERMINAL_WORK_TICKET_STATES,
)

import qiita_control_plane.runner as _runner_pkg

_log = logging.getLogger("qiita_control_plane.runner")

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

    `TERMINAL_WORK_TICKET_STATES` here means "states the runner does not own": if
    a ticket lands in one out from under a running workflow, the in-place
    infra-retry / poll loops must stop rather than retry forever against work
    that is no longer theirs.

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
                await asyncio.sleep(_runner_pkg._POLL_DB_READ_BACKOFF_SECONDS * (attempt + 1))
            continue
        if state in TERMINAL_WORK_TICKET_STATES:
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
