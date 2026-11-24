"""State changes of a krepp build (`qiita.reference_index` rows with index_type krepp).

A build is registered by `actions.library.register_index` (`registered`), activated
once it has passed the activation checks (`activate_krepp_build`), and
removed once no ticket may still read it (`remove_krepp_build`). Each change locks the
reference's krepp rows `FOR UPDATE` first, so two changes to one reference's builds
serialize. `/run` refuses a ticket whose build is removed (`removed_build_of_ticket`).

`work_ticket.reference_index_idx` is the build a ticket is fixed to. Nothing writes it
yet. The code that will (the placement workflow's step that fixes a ticket to the
reference's active build) has to hold the build row `FOR SHARE`, re-read under that
lock that the build is `active`, and keep the lock until it commits the ticket's
column: `remove_krepp_build` counts the tickets that may read a build under `FOR UPDATE`
of the same row, and a write made outside that lock could fix a ticket to a build
being removed.
"""

from dataclasses import dataclass

import asyncpg
from qiita_common.krepp import REFERENCE_INDEX_IDX_BINDING
from qiita_common.models import (
    INDEX_TYPE_KREPP,
    NON_TERMINAL_WORK_TICKET_STATES,
    ReferenceIndexState,
)

from . import require_transaction


class KreppBuildNotFound(LookupError):
    """No krepp row has this reference_index_idx."""

    def __init__(self, reference_index_idx: int):
        self.reference_index_idx = reference_index_idx
        super().__init__(f"no krepp build with reference_index_idx {reference_index_idx}")


class KreppBuildTransitionRefused(Exception):
    """The requested state change is not allowed from the build's current state, or a
    running ticket still uses the build. `reason` is the sentence an operator reads."""

    def __init__(self, reference_index_idx: int, reason: str):
        self.reference_index_idx = reference_index_idx
        self.reason = reason
        super().__init__(f"krepp build {reference_index_idx}: {reason}")


@dataclass(frozen=True)
class KreppBuildChange:
    """What a state change did: the build changed, and the build it retired, if any."""

    reference_index_idx: int
    reference_idx: int
    state: ReferenceIndexState
    retired_reference_index_idx: int | None = None


async def _lock_reference_builds(
    conn: asyncpg.Connection, reference_index_idx: int
) -> asyncpg.Record:
    """Lock every krepp row of the build's reference and return the build's row."""
    reference_idx = await conn.fetchval(
        "SELECT reference_idx FROM qiita.reference_index"
        " WHERE reference_index_idx = $1 AND index_type = $2",
        reference_index_idx,
        INDEX_TYPE_KREPP,
    )
    if reference_idx is None:
        raise KreppBuildNotFound(reference_index_idx)
    # Locked in reference_index_idx order, so two changes to one reference take the
    # rows in the same order.
    await conn.execute(
        "SELECT 1 FROM qiita.reference_index"
        " WHERE reference_idx = $1 AND index_type = $2"
        " ORDER BY reference_index_idx FOR UPDATE",
        reference_idx,
        INDEX_TYPE_KREPP,
    )
    return await conn.fetchrow(
        "SELECT reference_index_idx, reference_idx, state FROM qiita.reference_index"
        " WHERE reference_index_idx = $1",
        reference_index_idx,
    )


async def activate_krepp_build(
    conn: asyncpg.Connection, reference_index_idx: int
) -> KreppBuildChange:
    """Make a `registered` build the reference's active one, retiring the build that
    was active. The previous build is retired before this one is activated, because
    `reference_index_one_active_krepp` is a unique index checked row by row. Activating
    the build that is already active returns without a change, so a retried
    activation step succeeds."""
    require_transaction(conn)
    row = await _lock_reference_builds(conn, reference_index_idx)
    state = ReferenceIndexState(row["state"])
    if state is ReferenceIndexState.ACTIVE:
        return KreppBuildChange(reference_index_idx, row["reference_idx"], state)
    if state is not ReferenceIndexState.REGISTERED:
        raise KreppBuildTransitionRefused(
            reference_index_idx, f"is {state}; only a registered build can be activated"
        )
    retired = await conn.fetchval(
        "UPDATE qiita.reference_index SET state = $3"
        " WHERE reference_idx = $1 AND index_type = $2 AND state = $4"
        " RETURNING reference_index_idx",
        row["reference_idx"],
        INDEX_TYPE_KREPP,
        ReferenceIndexState.RETIRED.value,
        ReferenceIndexState.ACTIVE.value,
    )
    await conn.execute(
        "UPDATE qiita.reference_index SET state = $2 WHERE reference_index_idx = $1",
        reference_index_idx,
        ReferenceIndexState.ACTIVE.value,
    )
    return KreppBuildChange(
        reference_index_idx,
        row["reference_idx"],
        ReferenceIndexState.ACTIVE,
        retired_reference_index_idx=retired,
    )


async def tickets_running_on_build(conn: asyncpg.Connection, reference_index_idx: int) -> list[int]:
    """The tickets that may still have a job reading the build, oldest first: fixed to
    it or naming it in their context (an activation ticket), and non-terminal or with
    a cancel's reap of their SLURM jobs still pending. Removal waits for every such
    ticket because a job on a node deletes that node's copy of a removed build
    (`node_cache.evict`) whether or not another job there is still reading it. The
    context value is compared as jsonb, so `42` and `42.0` name the same build."""
    rows = await conn.fetch(
        "SELECT work_ticket_idx FROM qiita.work_ticket"
        " WHERE (reference_index_idx = $1 OR action_context->$3 = to_jsonb($1::bigint))"
        "   AND (state = ANY($2::qiita.work_ticket_state[]) OR reap_pending_since IS NOT NULL)"
        " ORDER BY work_ticket_idx",
        reference_index_idx,
        list(NON_TERMINAL_WORK_TICKET_STATES),
        REFERENCE_INDEX_IDX_BINDING,
    )
    return [r["work_ticket_idx"] for r in rows]


async def remove_krepp_build(
    conn: asyncpg.Connection, reference_index_idx: int
) -> KreppBuildChange:
    """Mark a `registered` or `retired` build `removed`. Refused for the active build
    (activate another first) and while a ticket may still read it
    (`tickets_running_on_build`).
    Finished tickets keep their `reference_index_idx`; `/run` refuses to restart one
    whose build is removed. Removing a removed build returns without a change. The
    master is deleted by the operator afterwards (docs/runbooks/krepp-index.md)."""
    require_transaction(conn)
    row = await _lock_reference_builds(conn, reference_index_idx)
    state = ReferenceIndexState(row["state"])
    if state is ReferenceIndexState.REMOVED:
        return KreppBuildChange(reference_index_idx, row["reference_idx"], state)
    if state is ReferenceIndexState.ACTIVE:
        raise KreppBuildTransitionRefused(
            reference_index_idx,
            "is the reference's active build; activate another build first",
        )
    running = await tickets_running_on_build(conn, reference_index_idx)
    if running:
        raise KreppBuildTransitionRefused(
            reference_index_idx,
            f"work tickets {running} may still read it (running, or cancelled with their"
            " SLURM jobs still being reaped); wait for them to finish",
        )
    await conn.execute(
        "UPDATE qiita.reference_index SET state = $2 WHERE reference_index_idx = $1",
        reference_index_idx,
        ReferenceIndexState.REMOVED.value,
    )
    return KreppBuildChange(reference_index_idx, row["reference_idx"], ReferenceIndexState.REMOVED)


async def removed_build_of_ticket(conn: asyncpg.Connection, work_ticket_idx: int) -> int | None:
    """For a redrive: the build the ticket is fixed to, or names in its context (an
    activation ticket), when that build is removed, else None. The build row is held
    `FOR SHARE` whatever its state, so a `remove_krepp_build` (which locks it
    `FOR UPDATE` and counts the tickets that may read it, by the same two links)
    either commits first and is refused here, or waits for the redrive to commit and
    then sees the revived ticket."""
    row = await conn.fetchrow(
        "SELECT ri.reference_index_idx, ri.state FROM qiita.work_ticket wt"
        " JOIN qiita.reference_index ri ON ri.reference_index_idx = wt.reference_index_idx"
        "   OR wt.action_context->$2 = to_jsonb(ri.reference_index_idx)"
        " WHERE wt.work_ticket_idx = $1"
        " ORDER BY ri.reference_index_idx"
        " FOR SHARE OF ri",
        work_ticket_idx,
        REFERENCE_INDEX_IDX_BINDING,
    )
    if row is None or row["state"] != ReferenceIndexState.REMOVED:
        return None
    return row["reference_index_idx"]
