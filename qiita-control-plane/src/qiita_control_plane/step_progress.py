"""Per-step-entry progress rows for work tickets (`qiita.work_ticket_step`).

The write-ahead spine for compute decoupling + restart recovery. Every
entry in an action's `steps:` list — compute `step:` and in-process
`action:` alike — gets one row per attempt, recording where it ran
(`compute_target`), the SLURM job id when applicable, and its lifecycle
state. The runner writes `submitting` *before* it calls the backend
submit, so a CP restart (a routine, undrained event on every deploy) can
re-attach in-flight work from these rows instead of failing it.

Writers are atomic and WHERE-guarded, mirroring `runner._atomic_transition`:
a guard miss raises rather than silently overwriting, surfacing a corrupted
progress sequence loudly (fail-fast). They accept either a pool or a live
connection so a caller can fold a progress write into a larger transaction.

A row's identity is `(step_name, step_occurrence)`, not `step_index`: the
runner reads the rows through `align_step_progress`, which first points each
row's `step_index` at the entry its identity names in the action's current
`steps:` list. Recovery reads these rows to decide,
for a non-terminal ticket, which entries already completed (skip + rebuild
`bound` from disk) and which to resume.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

import asyncpg
from qiita_common.actions import WorkflowAction, WorkflowStep
from qiita_common.models import ComputeTarget, StepProgressState

_log = logging.getLogger(__name__)

# asyncpg accepts either a pool (auto-acquires a transient connection per
# call) or a live connection. Writers take both so the runner can fold a
# progress write into the same transaction as a ticket transition.
_Executor = asyncpg.Pool | asyncpg.Connection

# Keep three things in lockstep: this column list, the StepProgressRow fields,
# and StepProgressRow.from_record's body. A drift surfaces as a runtime KeyError
# in from_record, not an import-time error.
_COLUMNS = (
    "work_ticket_idx, step_index, attempt, step_name, step_occurrence, compute_target, state, "
    "slurm_job_id, job_name, failure_kind, failure_reason, created_at, updated_at"
)


@dataclass(frozen=True)
class StepProgressRow:
    """One persisted `qiita.work_ticket_step` row, decoded into typed enums."""

    work_ticket_idx: int
    step_index: int
    attempt: int
    step_name: str
    step_occurrence: int
    compute_target: ComputeTarget
    state: StepProgressState
    slurm_job_id: int | None
    job_name: str | None
    failure_kind: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, row: asyncpg.Record) -> StepProgressRow:
        return cls(
            work_ticket_idx=row["work_ticket_idx"],
            step_index=row["step_index"],
            attempt=row["attempt"],
            step_name=row["step_name"],
            step_occurrence=row["step_occurrence"],
            compute_target=ComputeTarget(row["compute_target"]),
            state=StepProgressState(row["state"]),
            slurm_job_id=row["slurm_job_id"],
            job_name=row["job_name"],
            failure_kind=row["failure_kind"],
            failure_reason=row["failure_reason"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


# ---------------------------------------------------------------------------
# Writers — atomic, WHERE-guarded, total
# ---------------------------------------------------------------------------


async def record_submitting(
    executor: _Executor,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    step_name: str,
    step_occurrence: int,
    compute_target: ComputeTarget,
    job_name: str | None = None,
) -> None:
    """Write-ahead intent: insert the progress row in `submitting` before the
    backend submit fires, carrying `compute_target` and (for SLURM) the
    deterministic `job_name`. Idempotent — `ON CONFLICT DO NOTHING` — so a
    re-entry for the same `(work_ticket_idx, step_index, attempt)` never
    resets a row that already advanced."""
    await executor.execute(
        "INSERT INTO qiita.work_ticket_step"
        " (work_ticket_idx, step_index, attempt, step_name, step_occurrence,"
        "  compute_target, job_name, state)"
        " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        " ON CONFLICT (work_ticket_idx, step_index, attempt) DO NOTHING",
        work_ticket_idx,
        step_index,
        attempt,
        step_name,
        step_occurrence,
        compute_target.value,
        job_name,
        StepProgressState.SUBMITTING.value,
    )


async def record_submitted(
    executor: _Executor,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    slurm_job_id: int,
) -> None:
    """Record the SLURM job id the backend returned: `submitting → submitted`."""
    await _guarded_update(
        executor,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        set_clause="state = $4, slurm_job_id = $5",
        extra_args=(StepProgressState.SUBMITTED.value, slurm_job_id),
        allowed=(StepProgressState.SUBMITTING,),
    )


async def record_running(
    executor: _Executor,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
) -> None:
    """Mirror a status poll reporting the job on a node: `submitted → running`.
    Idempotent across repeated `running` polls (allowed from `running` too)."""
    await _guarded_update(
        executor,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        set_clause="state = $4",
        extra_args=(StepProgressState.RUNNING.value,),
        allowed=(StepProgressState.SUBMITTED, StepProgressState.RUNNING),
    )


async def record_completed(
    executor: _Executor,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
) -> None:
    """Terminal success. Reachable from any non-`failed` state — `control_plane`
    / `local` entries jump straight from `submitting`, SLURM entries from
    `running`. Idempotent (allowed from `completed`) — safe because the only
    payload is `state`, so a re-entry rewrites the same value (re-run-after-
    crash finalize relies on this); blocked from `failed`."""
    await _guarded_update(
        executor,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        set_clause="state = $4",
        extra_args=(StepProgressState.COMPLETED.value,),
        allowed=(
            StepProgressState.SUBMITTING,
            StepProgressState.SUBMITTED,
            StepProgressState.RUNNING,
            StepProgressState.COMPLETED,
        ),
    )


async def record_synchronous_completion(
    executor: _Executor,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    compute_target: ComputeTarget,
) -> None:
    """Terminal success for a backend that finished *at submit time* (the
    LocalBackend runs the module in-process and returns terminal outputs).

    The runner write-aheads a step entry as `compute_target='slurm'` with the
    deterministic job name — the production assumption, made before the submit
    reveals which backend actually ran it. When the handle comes back
    synchronous (`compute_target='local'`), this writer corrects the row in
    one UPDATE: sets the real target and clears the now-bogus SLURM job fields
    while moving to `completed`. Doing it atomically keeps the
    `work_ticket_step_slurm_fields_consistent` CHECK satisfied (a non-slurm
    row must carry neither job id nor job name). Blocked from `failed`."""
    await _guarded_update(
        executor,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        set_clause="state = $4, compute_target = $5, slurm_job_id = NULL, job_name = NULL",
        extra_args=(StepProgressState.COMPLETED.value, compute_target.value),
        # Only the write-ahead `submitting` state precedes a synchronous
        # completion (record_submitted never fired — the handle came back
        # terminal). `completed` is allowed solely for idempotency on re-run.
        # `submitted` / `running` belong to a real SLURM job and must never
        # be NULL-ed out here — narrowing the guard fails loudly if this is
        # ever called on one.
        allowed=(StepProgressState.SUBMITTING, StepProgressState.COMPLETED),
    )


async def record_failed(
    executor: _Executor,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    failure_kind: str,
    failure_reason: str,
) -> None:
    """Terminal failure with the captured `failure_kind` / `failure_reason`.
    Reachable from any non-terminal state; blocked from both `completed`
    (a finished step can't be re-stamped failed by a late stray signal) and
    `failed` itself — unlike `record_completed`, this writer carries mutable
    payload, so re-entry on an already-failed row raises rather than silently
    overwriting the original diagnostic. A retry is a *new* attempt row, not
    a re-fail of the same one."""
    await _guarded_update(
        executor,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        set_clause="state = $4, failure_kind = $5, failure_reason = $6",
        extra_args=(StepProgressState.FAILED.value, failure_kind, failure_reason),
        allowed=(
            StepProgressState.SUBMITTING,
            StepProgressState.SUBMITTED,
            StepProgressState.RUNNING,
        ),
    )


async def _guarded_update(
    executor: _Executor,
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    set_clause: str,
    extra_args: tuple,
    allowed: Sequence[StepProgressState],
) -> None:
    """Run a TOCTOU-safe UPDATE guarded on the current state being one of
    `allowed`. Raises if no row matched — surfacing a missing row or an
    illegal transition loudly instead of silently overwriting (mirrors
    `runner._atomic_transition`). $1..$3 are the primary key; $4.. are
    `extra_args` referenced by `set_clause`; the allowed-state list is bound
    last as an array.

    `set_clause` is interpolated into the SQL text, NOT bound — it MUST be a
    module-internal compile-time constant (every caller above passes a string
    literal). Never route caller-controlled data through it; only column
    values reach the query as bound `$n` parameters."""
    allowed_values = [s.value for s in allowed]
    args = (work_ticket_idx, step_index, attempt, *extra_args, allowed_values)
    guard_param = len(args)
    updated = await executor.fetchval(
        f"UPDATE qiita.work_ticket_step SET {set_clause}"
        " WHERE work_ticket_idx = $1 AND step_index = $2 AND attempt = $3"
        f"   AND state = ANY(${guard_param}::text[])"
        " RETURNING work_ticket_idx",
        *args,
    )
    if updated is None:
        actual = await executor.fetchval(
            "SELECT state FROM qiita.work_ticket_step"
            " WHERE work_ticket_idx = $1 AND step_index = $2 AND attempt = $3",
            work_ticket_idx,
            step_index,
            attempt,
        )
        # actual is None ⇒ the row is gone (never inserted, or CASCADE-deleted
        # by a concurrent work_ticket delete); distinguish it from a real
        # illegal-transition so a 2am post-mortem isn't misled by "state None".
        if actual is None:
            raise RuntimeError(
                f"work_ticket_step ({work_ticket_idx}, {step_index}, {attempt}) "
                f"not found (never inserted, or deleted out from under the write)"
            )
        raise RuntimeError(
            f"could not transition work_ticket_step "
            f"({work_ticket_idx}, {step_index}, {attempt}) via {set_clause!r}; "
            f"actual state {actual!r}, allowed {allowed_values}"
        )


# ---------------------------------------------------------------------------
# Recovery query helpers
# ---------------------------------------------------------------------------


async def load_step_progress(executor: _Executor, work_ticket_idx: int) -> list[StepProgressRow]:
    """Return every progress row for a ticket, ordered by (step_index,
    attempt) — the per-entry compute_target / job id / state view that
    recovery and the summary read consume."""
    rows = await executor.fetch(
        f"SELECT {_COLUMNS} FROM qiita.work_ticket_step"
        " WHERE work_ticket_idx = $1"
        " ORDER BY step_index, attempt",
        work_ticket_idx,
    )
    return [StepProgressRow.from_record(r) for r in rows]


class StepProgressMismatch(Exception):
    """A ticket's progress rows name entries its action's current `steps:` list
    does not have, so which entries already ran cannot be decided from them."""


def step_occurrences(names: Sequence[str]) -> list[int]:
    """Each entry's occurrence: how many earlier entries in `names` carry the
    same name. `(name, occurrence)` is an entry's identity in
    `qiita.work_ticket_step` (see the `step_occurrence` column comment)."""
    seen: dict[str, int] = {}
    occurrences = []
    for name in names:
        occurrence = seen.get(name, 0)
        occurrences.append(occurrence)
        seen[name] = occurrence + 1
    return occurrences


def _runs_where_recorded(entry: WorkflowStep | WorkflowAction, row: StepProgressRow) -> bool:
    """Whether `entry` is the kind of entry that writes `row`: an `action:` runs
    on the control plane, a `step:` on a compute backend. A row matching an
    entry by identity but not by kind was written by a different entry."""
    if isinstance(entry, WorkflowAction):
        return row.compute_target is ComputeTarget.CONTROL_PLANE
    return row.compute_target is not ComputeTarget.CONTROL_PLANE


def _entry_positions(
    entries: Sequence[WorkflowStep | WorkflowAction],
) -> dict[tuple[str, int], tuple[int, WorkflowStep | WorkflowAction]]:
    """Each entry's identity, mapped to its position in `entries` and the entry."""
    return {
        (entry.name, occurrence): (index, entry)
        for index, (entry, occurrence) in enumerate(
            zip(entries, step_occurrences([e.name for e in entries]), strict=True)
        )
    }


def check_step_progress(
    work_ticket_idx: int,
    rows: Sequence[StepProgressRow],
    entries: Sequence[WorkflowStep | WorkflowAction],
) -> None:
    """Raise `StepProgressMismatch` when any of `rows` names no entry of
    `entries`, or an entry of the other kind, naming those rows. The runner
    checks before it resumes (`align_step_progress`), and `/run` before it
    redrives, so a ticket that cannot resume is refused rather than failed again."""
    positions = _entry_positions(entries)
    unmatched = []
    for row in rows:
        hit = positions.get((row.step_name, row.step_occurrence))
        if hit is None or not _runs_where_recorded(hit[1], row):
            unmatched.append(row)
    if not unmatched:
        return
    described = "; ".join(
        f"{r.step_name!r} occurrence {r.step_occurrence} (recorded at step"
        f" {r.step_index}, attempt {r.attempt}, {r.compute_target.value},"
        f" {r.state.value})"
        for r in unmatched
    )
    unmatchable = (
        " A negative occurrence marks a row the step_occurrence migration could"
        " not match to an entry of the list it was written under."
        if any(r.step_occurrence < 0 for r in unmatched)
        else ""
    )
    raise StepProgressMismatch(
        f"work_ticket {work_ticket_idx} has progress rows for entries the"
        f" action's current steps list does not have: {described}. The"
        " workflow was edited after these rows were written, so the ticket"
        f" cannot be resumed; submit a new ticket instead.{unmatchable}"
    )


async def align_step_progress(
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    entries: Sequence[WorkflowStep | WorkflowAction],
) -> list[StepProgressRow]:
    """Point every progress row of the ticket at the entry of `entries` that its
    `(step_name, step_occurrence)` names, then return the rows as
    `load_step_progress` does.

    A row's `step_index` is the position its entry had when the row was
    written, and the action's `steps:` list may have been edited and re-synced
    since. After this call `step_index` is the entry's position in `entries`,
    so every reader keyed on it reads the entry the row was written for.

    Raises `StepProgressMismatch`, changing nothing, when any row's identity
    names no entry of `entries` or an entry of the other kind."""
    async with pool.acquire() as conn, conn.transaction():
        rows = [
            StepProgressRow.from_record(r)
            for r in await conn.fetch(
                f"SELECT {_COLUMNS} FROM qiita.work_ticket_step"
                " WHERE work_ticket_idx = $1"
                " ORDER BY step_index, attempt"
                " FOR UPDATE",
                work_ticket_idx,
            )
        ]
        positions = _entry_positions(entries)
        check_step_progress(work_ticket_idx, rows, entries)
        moved = any(positions[(r.step_name, r.step_occurrence)][0] != r.step_index for r in rows)
        if moved:
            # The primary key includes step_index and is checked row by row, so
            # an UPDATE that swaps two rows' positions collides part-way. Every
            # row is first moved past both the old and the new positions, then
            # set to its entry's position.
            offset = 1 + max(len(entries), *(r.step_index for r in rows))
            await conn.execute(
                "UPDATE qiita.work_ticket_step SET step_index = step_index + $2"
                " WHERE work_ticket_idx = $1",
                work_ticket_idx,
                offset,
            )
            identities = sorted({(r.step_name, r.step_occurrence) for r in rows})
            await conn.execute(
                "UPDATE qiita.work_ticket_step s SET step_index = m.step_index"
                " FROM unnest($2::text[], $3::int[], $4::int[])"
                "   AS m(step_name, step_occurrence, step_index)"
                " WHERE s.work_ticket_idx = $1"
                "   AND s.step_name = m.step_name"
                "   AND s.step_occurrence = m.step_occurrence",
                work_ticket_idx,
                [name for name, _ in identities],
                [occurrence for _, occurrence in identities],
                [positions[identity][0] for identity in identities],
            )
            _log.warning(
                "work_ticket %d: progress rows re-pointed at the current steps list"
                " (the workflow was edited since they were written)",
                work_ticket_idx,
            )
        return await load_step_progress(conn, work_ticket_idx)


def first_incomplete_step_index(rows: Sequence[StepProgressRow], total_steps: int) -> int:
    """The "current entry": the lowest step index in `[0, total_steps)` with
    no `completed` row across any attempt. Returns `total_steps` when every
    step has completed (i.e. the runner should proceed to finalize). Pure —
    no business logic in SQL."""
    completed = {r.step_index for r in rows if r.state is StepProgressState.COMPLETED}
    for i in range(total_steps):
        if i not in completed:
            return i
    return total_steps
