"""Re-arm a finished align block ticket so the align workflow replaces its rows, and
delete the rows of a prep_sample no gate tracks.

`rerun_align_block` is one Postgres transaction. It leaves the ticket PENDING and the
caller dispatches it after commit. The workflow then does the replacement itself:
`delete-alignment-block` removes the block's exact footprint, `register-files` adds
the new file, and `reconcile-alignment-block` completes the block and the gates
(`workflows/align/1.0.0.yaml`).

Two starting points are accepted:

* **COMPLETED** ticket, block `completed`. Every step row is deleted, so the runner
  runs every entry again instead of fast-forwarding it, and the block and its
  `completed` gates go back to `pending`. Consumers read only `completed` gates, so
  the block's prep_samples are not served while its rows are replaced.
* **FAILED** ticket, block `pending`, under a deprecated mask, with
  `allow_deprecated_mask`. This is a failed re-run being retried: `/run` refuses a
  deprecated mask. The step rows are treated as `/run` treats them
  (`ticket_redrive.drop_dead_step_rows`), so finished entries are kept.

The new lake file must not take the old file's name. The name is derived from the
staging directory (`registration.rs` `lake_dest_filename`), and the runner picks the
first attempt directory that does not exist yet (`runner/_workflow.py`), so the
re-run gets a new name only while the directory of the attempt that produced the
registered file is still on disk. If it is gone, registration would see the old
name: `already_committed` with no rows written when the old file still holds rows
outside the footprint, else `failed_precondition` after the footprint was already
deleted. The check is here, before anything changes, because the delete step runs
first.

A request either commits within `RERUN_REQUEST_BOUND_S` of the route starting or
does not commit, so a caller that lost the response can check the ticket after that
bound and know no commit is still coming. What bounds each part is beside the
constants.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from pathlib import Path

import asyncpg
from qiita_common.actions import ALIGN_ACTION_ID, ESTIMATE_FEATURE_TABLE_ACTION_ID
from qiita_common.api_paths import URL_WORK_TICKET_RUN, LibraryPrimitive
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    MaskDefinitionStatus,
    StepProgressState,
    WorkTicketState,
)

from ..repositories.alignment_definition import (
    ALIGNMENT_SAMPLE_COMPLETED,
    ALIGNMENT_SAMPLE_INVALIDATED,
    ALIGNMENT_SAMPLE_PENDING,
    AlignmentDefinitionDeprecated,
    AlignmentDefinitionNotFound,
    alignment_definition_exists,
    lock_active_alignment,
)
from ..repositories.block import (
    BLOCK_COMPLETED,
    BLOCK_PENDING,
    fetch_block_members,
    list_samples_with_incomplete_covering_alignment_block,
    set_block_state,
)
from ..repositories.mask_definition import MaskDefinitionNotFound, lock_mask
from ..step_progress import StepProgressMismatch, check_step_progress, load_step_progress
from ..ticket_redrive import drop_dead_step_rows, reset_ticket_to_pending
from ..workspace import step_attempt_dir, ticket_workspace
from .library import delete_alignment_sample_data

_log = logging.getLogger(__name__)

# The route's lookups and the pool acquire share this budget: past it the call is
# refused before the transaction starts (`deadline_passed`).
_PRE_TXN_BUDGET_S = 10.0
# Each statement of the transaction runs under `statement_timeout`, which counts lock
# waits. The transaction runs at most _MAX_STATEMENTS of them, a fixed number on each
# path (the gate rows are locked in one), and a test counts them on both paths.
#   both:      SET LOCAL, ticket, mask lock, alignment lock, block lock, feature-table
#              tickets, block members, gate locks, sibling blocks, producing step
#              rows, ticket reset, gate UPDATE
#   completed: step-row DELETE, block state
#   failed:    dead step-row DELETE, step-progress read
# The one filesystem call, the registered attempt directory's stat, runs in a daemon
# thread of its own under _STAT_TIMEOUT_S (`_stat_off_loop`), so a stalled scratch
# mount neither blocks the event loop nor holds the transaction open past the bound.
# COMMIT is not under `statement_timeout`; a deferred trigger's work at COMMIT ran
# past it (Postgres 17.11). The tables written here have no deferred constraint or
# trigger, so their COMMIT is the WAL flush alone, which nothing here bounds.
_STATEMENT_TIMEOUT_MS = 2000
_MAX_STATEMENTS = 14
_STAT_TIMEOUT_S = 2.0
RERUN_REQUEST_BOUND_S = (
    _PRE_TXN_BUDGET_S + _MAX_STATEMENTS * _STATEMENT_TIMEOUT_MS / 1000 + _STAT_TIMEOUT_S
)

# The 503 reasons, each raised only before anything committed.
NOT_COMMITTED_REASONS = frozenset({"deadline_passed", "statement_timeout", "scratch_timeout"})


class AlignRerunRefused(Exception):
    """The ticket cannot be re-armed. `reason` is a stable code the CLI reports;
    `status` the HTTP status the route answers with. Raised before anything
    changed: the transaction rolls back."""

    def __init__(self, reason: str, detail: str, *, status: int = 409, **extra: object):
        self.reason = reason
        self.detail = detail
        self.status = status
        self.extra = extra
        super().__init__(detail)


def _refuse(reason: str, detail: str, **extra: object) -> AlignRerunRefused:
    return AlignRerunRefused(reason, detail, **extra)


def _deadline_passed(work_ticket_idx: int) -> AlignRerunRefused:
    return AlignRerunRefused(
        "deadline_passed",
        f"the lookups and the connection for work_ticket {work_ticket_idx} took more than"
        f" {_PRE_TXN_BUDGET_S} s from the route's start; nothing was changed",
        status=503,
    )


def _stat_off_loop(path: Path) -> asyncio.Future:
    """`path.stat()` in a daemon thread, as a future of this loop. Not the default
    executor: a stat stuck on a hung mount would hold one of its shared workers, and
    the executor's workers are joined at interpreter exit, so a restart would wait
    on it. A stuck daemon thread is left behind, one per timed-out call."""
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def settle(result=None, exc=None):
        if not fut.done():
            fut.set_exception(exc) if exc is not None else fut.set_result(result)

    def run():
        try:
            result, exc = path.stat(), None
        except BaseException as e:  # handed to the awaiting coroutine
            result, exc = None, e
        try:
            loop.call_soon_threadsafe(settle, result, exc)
        except RuntimeError:  # the loop closed while the stat was stuck
            pass

    threading.Thread(target=run, name="rerun-attempt-stat", daemon=True).start()
    return fut


def pre_txn_deadline() -> float:
    """The `deadline` a route passes to `rerun_align_block`, taken when it starts."""
    return time.monotonic() + _PRE_TXN_BUDGET_S


def registered_output_step(action_steps: list) -> str:
    """The step whose output `register-files` loads into the lake: the one step that
    outputs `register-files`' input."""
    registers = [e for e in action_steps if e.name == LibraryPrimitive.REGISTER_FILES]
    if len(registers) != 1:
        raise RuntimeError(f"align action has {len(registers)} register-files entries, not 1")
    wanted = set(registers[0].inputs)
    producers = [e.name for e in action_steps if e is not registers[0] and wanted & set(e.outputs)]
    if len(producers) != 1:
        raise RuntimeError(
            f"align action has {len(producers)} steps producing register-files' input"
            f" {sorted(wanted)}, not 1: {producers}"
        )
    return producers[0]


async def rerun_align_block(
    pool: asyncpg.Pool,
    *,
    deadline: float,
    alignment_idx: int,
    work_ticket_idx: int,
    allow_deprecated_mask: bool,
    reason: str,
    principal_idx: int,
    ticket_root: Path,
    action_steps: list,
) -> dict:
    """See the module docstring. Returns the fields of `AlignmentRerunBlockResponse`.

    `deadline` is `pre_txn_deadline()` taken when the route started. `action_steps`
    is the align action's step list. Raises AlignRerunRefused.
    """
    produce_step = registered_output_step(action_steps)
    left = deadline - time.monotonic()
    if left <= 0:
        raise _deadline_passed(work_ticket_idx)
    try:
        conn = await pool.acquire(timeout=left)
    except TimeoutError as exc:
        raise _deadline_passed(work_ticket_idx) from exc
    try:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = {_STATEMENT_TIMEOUT_MS}")
            outcome = await _rearm(
                conn,
                alignment_idx=alignment_idx,
                work_ticket_idx=work_ticket_idx,
                allow_deprecated_mask=allow_deprecated_mask,
                ticket_root=ticket_root,
                action_steps=action_steps,
                produce_step=produce_step,
            )
    except asyncpg.QueryCanceledError as exc:
        # Only a statement inside the transaction runs under the timeout, so the
        # transaction rolled back.
        raise AlignRerunRefused(
            "statement_timeout",
            f"a statement re-arming work_ticket {work_ticket_idx} ran past"
            f" {_STATEMENT_TIMEOUT_MS} ms; nothing was changed: {exc}",
            status=503,
        ) from exc
    except asyncpg.UniqueViolationError as exc:
        # work_ticket_one_in_flight_per_block: another unfinished ticket runs the block.
        raise _refuse("block_in_flight", f"block of work_ticket {work_ticket_idx}: {exc}") from exc
    finally:
        await pool.release(conn)
    _log.info(
        "rerun-block: principal %d re-armed work_ticket %d (block %d, alignment %d) from %s;"
        " mask override %s; %d gate(s) to pending; %d step row(s) deleted; reason: %s",
        principal_idx,
        work_ticket_idx,
        outcome["block_idx"],
        alignment_idx,
        outcome["rerun_of"],
        outcome["mask_override"],
        len(outcome["pending_prep_sample_idx"]),
        outcome["step_rows_deleted"],
        reason,
    )
    return outcome


async def _rearm(
    conn: asyncpg.Connection,
    *,
    alignment_idx: int,
    work_ticket_idx: int,
    allow_deprecated_mask: bool,
    ticket_root: Path,
    action_steps: list,
    produce_step: str,
) -> dict:
    wt = await conn.fetchrow(
        "SELECT action_id, state::text AS state, block_idx, mask_idx, alignment_idx,"
        "       action_context->>'alignment_idx' AS context_alignment_idx"
        "  FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    if wt is None:
        raise AlignRerunRefused("not_found", f"work_ticket {work_ticket_idx} not found", status=404)
    if wt["action_id"] != ALIGN_ACTION_ID:
        raise _refuse(
            "wrong_action",
            f"work_ticket {work_ticket_idx} is {wt['action_id']!r}, not {ALIGN_ACTION_ID!r}",
        )
    # Both: the gate predicates join on the column, the workflow's delete and
    # reconcile take the bound context value.
    if wt["alignment_idx"] != alignment_idx or wt["context_alignment_idx"] != str(alignment_idx):
        raise _refuse(
            "wrong_alignment",
            f"work_ticket {work_ticket_idx} records alignment {wt['alignment_idx']}"
            f" (action_context {wt['context_alignment_idx']}), not {alignment_idx}",
        )
    state = wt["state"]
    if state in (WorkTicketState.COMPLETED, WorkTicketState.FAILED):
        rerun_of = WorkTicketState(state)
    else:
        raise _refuse(
            "wrong_state",
            f"work_ticket {work_ticket_idx} is {state!r}; only completed or failed tickets"
            " are re-armed",
        )

    # Locks in /run's order: mask, definition, block.
    mask_deprecated = False
    if wt["mask_idx"] is not None:
        try:
            mask = await lock_mask(conn, wt["mask_idx"])
        except MaskDefinitionNotFound:
            raise AlignRerunRefused(
                "not_found", f"mask {wt['mask_idx']} not found", status=404
            ) from None
        mask_deprecated = mask["status"] == MaskDefinitionStatus.DEPRECATED.value
    if mask_deprecated and not allow_deprecated_mask:
        raise _refuse(
            "mask_deprecated",
            f"work_ticket {work_ticket_idx} runs under mask {wt['mask_idx']}, which is"
            " deprecated; pass allow_deprecated_mask to run it under that mask again",
        )
    if allow_deprecated_mask and not mask_deprecated:
        raise _refuse(
            "mask_not_deprecated",
            f"allow_deprecated_mask was passed, but work_ticket {work_ticket_idx}'s mask"
            f" {wt['mask_idx']} is not deprecated",
        )
    if rerun_of == WorkTicketState.FAILED and not allow_deprecated_mask:
        raise _refuse(
            "wrong_state",
            f"work_ticket {work_ticket_idx} is failed under an active mask; retry it with"
            f" POST {URL_WORK_TICKET_RUN.format(work_ticket_idx=work_ticket_idx)}",
        )
    try:
        await lock_active_alignment(conn, alignment_idx)
    except AlignmentDefinitionNotFound:
        raise AlignRerunRefused(
            "not_found", f"alignment {alignment_idx} not found", status=404
        ) from None
    except AlignmentDefinitionDeprecated as exc:
        raise _refuse("alignment_deprecated", exc.detail) from exc

    block_idx = wt["block_idx"]
    block_state = await conn.fetchval(
        "SELECT state FROM qiita.block WHERE block_idx = $1 FOR UPDATE", block_idx
    )
    wanted_block = BLOCK_COMPLETED if rerun_of == WorkTicketState.COMPLETED else BLOCK_PENDING
    if block_state != wanted_block:
        raise _refuse(
            "wrong_state",
            f"block {block_idx} is {block_state!r}; a {rerun_of.value} ticket is re-armed only"
            f" from a {wanted_block!r} block",
        )

    feature_tables = await conn.fetch(
        "SELECT work_ticket_idx FROM qiita.work_ticket"
        " WHERE action_id = $1 AND state = ANY($2::qiita.work_ticket_state[])"
        "   AND ($3 IN (action_context->>'alignment_idx',"
        "               action_context->>'denovo_alignment_idx'))"
        " ORDER BY work_ticket_idx",
        ESTIMATE_FEATURE_TABLE_ACTION_ID,
        sorted(NON_TERMINAL_WORK_TICKET_STATES),
        str(alignment_idx),
    )
    if feature_tables:
        idxs = [r["work_ticket_idx"] for r in feature_tables]
        raise _refuse(
            "feature_table_in_flight",
            f"feature-table ticket(s) {idxs} read alignment {alignment_idx}; their"
            " completeness check ran at dispatch, so wait for them to finish",
            work_ticket_idx=idxs,
        )

    members = [ps for ps, _lo, _hi in await fetch_block_members(conn, block_idx)]
    # One statement, in prep_sample_idx order: the order reconcile locks them in.
    gates = dict(
        await conn.fetch(
            "SELECT prep_sample_idx, state FROM qiita.alignment_sample"
            " WHERE alignment_idx = $1 AND prep_sample_idx = ANY($2::bigint[])"
            " ORDER BY prep_sample_idx FOR UPDATE",
            alignment_idx,
            members,
        )
    )
    missing = sorted(set(members) - set(gates))
    if missing:
        raise _refuse(
            "gate_missing",
            f"prep_sample(s) {missing} of block {block_idx} have no gate under alignment"
            f" {alignment_idx}; reconcile would fail on them",
            prep_sample_idx=missing,
        )
    invalidated = sorted(p for p, s in gates.items() if s == ALIGNMENT_SAMPLE_INVALIDATED)
    if invalidated:
        raise _refuse(
            "gate_invalidated",
            f"prep_sample(s) {invalidated} are invalidated under alignment {alignment_idx};"
            " restore them first, or reconcile would refuse them",
            prep_sample_idx=invalidated,
        )
    # After the gate locks: a sibling re-armed meanwhile has committed its block.
    siblings = await list_samples_with_incomplete_covering_alignment_block(
        conn,
        alignment_idx=alignment_idx,
        prep_sample_idxs=sorted(gates),
        exclude_block_idx=block_idx,
    )
    if siblings:
        raise _refuse(
            "sibling_incomplete",
            f"prep_sample(s) {siblings} of block {block_idx} are covered by another block"
            " that is not completed; re-arm this one after it completes",
            prep_sample_idx=siblings,
        )

    produced = await conn.fetch(
        "SELECT attempt FROM qiita.work_ticket_step"
        " WHERE work_ticket_idx = $1 AND step_name = $2 AND state = $3",
        work_ticket_idx,
        produce_step,
        StepProgressState.COMPLETED.value,
    )
    if rerun_of == WorkTicketState.COMPLETED and len(produced) != 1:
        raise _refuse(
            "completed_step_rows",
            f"work_ticket {work_ticket_idx} has {len(produced)} completed {produce_step!r}"
            " rows; exactly 1 names the registered attempt",
        )
    if len(produced) > 1:
        raise _refuse(
            "completed_step_rows",
            f"work_ticket {work_ticket_idx} has {len(produced)} completed {produce_step!r} rows",
        )
    if produced:
        attempt_dir = step_attempt_dir(
            ticket_workspace(ticket_root, work_ticket_idx), produce_step, produced[0]["attempt"]
        )
        try:
            await asyncio.wait_for(_stat_off_loop(attempt_dir), _STAT_TIMEOUT_S)
        except FileNotFoundError:
            raise _refuse(
                "registered_attempt_missing",
                f"{attempt_dir} is gone, so the re-run would reuse it and the registered"
                " file's lake name; nothing was changed",
            ) from None
        except TimeoutError:
            raise AlignRerunRefused(
                "scratch_timeout",
                f"stat of {attempt_dir} took more than {_STAT_TIMEOUT_S} s; nothing was changed",
                status=503,
            ) from None
        except OSError as exc:
            raise _refuse(
                "registered_attempt_unreadable",
                f"cannot tell whether {attempt_dir} exists: {exc}; nothing was changed",
            ) from exc

    if not await reset_ticket_to_pending(conn, work_ticket_idx=work_ticket_idx, from_state=state):
        raise _refuse("state_changed", f"work_ticket {work_ticket_idx} changed state; retry")
    if rerun_of == WorkTicketState.COMPLETED:
        status = await conn.execute(
            "DELETE FROM qiita.work_ticket_step WHERE work_ticket_idx = $1", work_ticket_idx
        )
        step_rows_deleted = int(status.split()[-1])
        await set_block_state(
            conn, block_idx=block_idx, new_state=BLOCK_PENDING, expected_states=[BLOCK_COMPLETED]
        )
    else:
        step_rows_deleted = await drop_dead_step_rows(
            conn, work_ticket_idx=work_ticket_idx, after_cancel=False
        )
        try:
            check_step_progress(
                work_ticket_idx,
                ALIGN_ACTION_ID,
                await load_step_progress(conn, work_ticket_idx),
                action_steps,
            )
        except StepProgressMismatch as exc:
            raise _refuse("step_progress_mismatch", str(exc)) from exc
    pending = [
        r["prep_sample_idx"]
        for r in await conn.fetch(
            "UPDATE qiita.alignment_sample SET state = $3"
            " WHERE alignment_idx = $1 AND prep_sample_idx = ANY($2::bigint[]) AND state = $4"
            " RETURNING prep_sample_idx",
            alignment_idx,
            sorted(gates),
            ALIGNMENT_SAMPLE_PENDING,
            ALIGNMENT_SAMPLE_COMPLETED,
        )
    ]
    return {
        "alignment_idx": alignment_idx,
        "work_ticket_idx": work_ticket_idx,
        "block_idx": block_idx,
        "rerun_of": rerun_of,
        "mask_override": allow_deprecated_mask,
        "pending_prep_sample_idx": sorted(pending),
        "step_rows_deleted": step_rows_deleted,
    }


class SampleGated(Exception):
    """The prep_sample has a gate under the alignment, so its rows are not purged."""


async def purge_alignment_sample_rows(
    pool: asyncpg.Pool,
    *,
    alignment_idx: int,
    prep_sample_idx: int,
    reason: str,
    principal_idx: int,
    signing_key: bytes,
    data_plane_url: str,
) -> int:
    """Delete every `alignment` row of `(alignment_idx, prep_sample_idx)` and return
    the count. Refused when the pair has a gate row: consumers trust the gate, so a
    gated sample with its rows deleted would be served as complete with nothing in it.
    For rows a block wrote under a prep_sample_idx nothing tracks, e.g. a zeroed one.

    Raises AlignmentDefinitionNotFound, SampleGated, or pyarrow.flight.FlightError."""
    if not await alignment_definition_exists(pool, alignment_idx):
        raise AlignmentDefinitionNotFound(alignment_idx)
    gated = await pool.fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        prep_sample_idx,
    )
    if gated is not None:
        raise SampleGated(
            f"prep_sample {prep_sample_idx} has a {gated!r} gate under alignment"
            f" {alignment_idx}; its rows are replaced by re-running its blocks, not purged"
        )
    rows_deleted = await delete_alignment_sample_data(
        alignment_idx=alignment_idx,
        prep_sample_idx=prep_sample_idx,
        signing_key=signing_key,
        data_plane_url=data_plane_url,
    )
    _log.info(
        "purge-sample-rows: principal %d deleted %d row(s) of prep_sample %d under alignment"
        " %d; reason: %s",
        principal_idx,
        rows_deleted,
        prep_sample_idx,
        alignment_idx,
        reason,
    )
    return rows_deleted
