"""Reference-row mutations callable from both routes and the runner.

The runner (in-process) and the PATCH /reference/{idx}/status route share
this transition logic so the validation matrix and TOCTOU-safe UPDATE
have one home.
"""

from __future__ import annotations

import asyncpg
from qiita_common.models import (
    VALID_STATUS_TRANSITIONS,
    ReferenceResponse,
    ReferenceStatus,
)

# Column projection backing every ReferenceResponse. Imported by
# routes/reference.py too, so the two callers can't drift (they previously
# kept hand-synced copies).
REFERENCE_RETURNING = (
    "reference_idx, name, version, kind, status, is_host, created_by_idx, created_at"
)


class ReferenceNotFound(Exception):
    """Raised when the reference_idx doesn't exist."""


# Work-ticket states that block a reference delete. In-flight states block
# unconditionally (a running job is reading/writing the reference's data);
# terminal states block only without `force` (a completed test run is exactly
# what an admin purging a test reference wants gone).
_WORK_TICKET_IN_FLIGHT_STATES = ("pending", "queued", "processing")
_WORK_TICKET_TERMINAL_STATES = ("completed", "failed")


class ReferenceDeleteBlocked(Exception):
    """Raised when a reference cannot be deleted because work tickets
    reference it. `in_flight` always blocks; `terminal` blocks only when the
    caller did not pass force=True."""

    def __init__(self, *, reference_idx: int, in_flight: int, terminal: int) -> None:
        self.reference_idx = reference_idx
        self.in_flight = in_flight
        self.terminal = terminal
        if in_flight:
            reason = (
                f"{in_flight} in-flight work ticket(s) "
                f"({'/'.join(_WORK_TICKET_IN_FLIGHT_STATES)}) reference it; "
                "wait for them to finish or cancel them"
            )
        else:
            reason = (
                f"{terminal} completed/failed work ticket(s) reference it; "
                "re-issue with force=true to delete them too"
            )
        super().__init__(f"Reference {reference_idx} cannot be deleted: {reason}")


def _rowcount(status: str) -> int:
    """Parse the affected-row count out of an asyncpg command tag
    (e.g. 'DELETE 5' → 5). Returns 0 for an unparseable tag rather than
    raising — the count is informational, not a control signal."""
    try:
        return int(status.rsplit(" ", 1)[1])
    except IndexError, ValueError:
        return 0


async def assert_reference_deletable(
    conn: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    *,
    force: bool,
) -> str:
    """Existence + work-ticket gating precheck for a reference delete.

    Returns the reference's current status on success. Raises
    ReferenceNotFound if it doesn't exist, or ReferenceDeleteBlocked if work
    tickets reference it (in-flight always; terminal unless force). Run this
    *before* any destructive step so a blocked delete touches nothing."""
    status = await conn.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if status is None:
        raise ReferenceNotFound(reference_idx)
    rows = await conn.fetch(
        "SELECT state, count(*) AS n FROM qiita.work_ticket"
        " WHERE reference_idx = $1 GROUP BY state",
        reference_idx,
    )
    counts = {r["state"]: r["n"] for r in rows}
    in_flight = sum(counts.get(s, 0) for s in _WORK_TICKET_IN_FLIGHT_STATES)
    terminal = sum(counts.get(s, 0) for s in _WORK_TICKET_TERMINAL_STATES)
    if in_flight or (terminal and not force):
        raise ReferenceDeleteBlocked(
            reference_idx=reference_idx,
            in_flight=in_flight,
            terminal=terminal,
        )
    return status


async def delete_reference_cascade(
    conn: asyncpg.Connection,
    reference_idx: int,
) -> dict[str, int]:
    """Tear down every Postgres row owned by a reference, in FK-dependency
    order, ending with the `qiita.reference` row itself. Must run inside the
    caller's transaction; the caller must have already gated via
    `assert_reference_deletable`.

    The schema uses ON DELETE RESTRICT throughout (no cascades), so order is
    explicit: work_ticket (→ work_ticket_step CASCADEs) → reference_index →
    reference_membership → orphan feature_genome/feature → orphan genome →
    reference. Features and genomes are deleted only when *orphaned* — claimed
    by no other reference — so a shared feature survives.

    Returns the per-table delete counts for the caller's response."""
    # Orphan features: this reference's features that no other reference claims.
    # Computed before the membership DELETE below (the EXCEPT needs this
    # reference's rows present). This set MUST match the data-plane orphan
    # computation in qiita-data-plane's flight_service.rs::delete_reference —
    # the two stores GC the same features independently, so a change to either
    # query must change the other or sequences/features desync across stores.
    orphan_features = [
        r["feature_idx"]
        for r in await conn.fetch(
            "SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1"
            " EXCEPT"
            " SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx <> $1",
            reference_idx,
        )
    ]

    work_ticket_deleted = _rowcount(
        await conn.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", reference_idx)
    )
    index_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", reference_idx
        )
    )
    membership_deleted = _rowcount(
        await conn.execute(
            "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
        )
    )

    if orphan_features:
        # Capture the genomes these features mapped to before deleting the
        # junction rows, so we can GC any genome left with no features.
        candidate_genomes = [
            r["genome_idx"]
            for r in await conn.fetch(
                "SELECT DISTINCT genome_idx FROM qiita.feature_genome"
                " WHERE feature_idx = ANY($1::bigint[])",
                orphan_features,
            )
        ]
        await conn.execute(
            "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
            orphan_features,
        )
        await conn.execute(
            "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
            orphan_features,
        )
        if candidate_genomes:
            await conn.execute(
                "DELETE FROM qiita.genome g WHERE g.genome_idx = ANY($1::bigint[])"
                " AND NOT EXISTS ("
                "   SELECT 1 FROM qiita.feature_genome fg WHERE fg.genome_idx = g.genome_idx"
                ")",
                candidate_genomes,
            )

    await conn.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)

    return {
        "membership_deleted": membership_deleted,
        "index_deleted": index_deleted,
        "work_ticket_deleted": work_ticket_deleted,
        "orphan_feature_count": len(orphan_features),
    }


class IllegalStatusTransition(Exception):
    """Raised when the current status can't transition to the target."""

    def __init__(self, *, current: str | None, target: ReferenceStatus) -> None:
        super().__init__(f"Cannot transition from {current!r} to {target!r}")
        self.current = current
        self.target = target


async def transition_reference_status(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    target: ReferenceStatus,
) -> ReferenceResponse:
    """Atomically transition a reference's status, validated against
    qiita_common.models.VALID_STATUS_TRANSITIONS.

    Raises ReferenceNotFound if the row doesn't exist; IllegalStatusTransition
    if no source status maps to `target`, or if the row is in a state that
    cannot reach `target`.
    """
    valid_sources = [
        str(src) for src, targets in VALID_STATUS_TRANSITIONS.items() if target in targets
    ]
    if not valid_sources:
        raise IllegalStatusTransition(current=None, target=target)

    row = await pool.fetchrow(
        "UPDATE qiita.reference SET status = $1"
        " WHERE reference_idx = $2 AND status = ANY($3::text[])"
        f" RETURNING {REFERENCE_RETURNING}",
        str(target),
        reference_idx,
        valid_sources,
    )
    if row is not None:
        return ReferenceResponse(**dict(row))

    current = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if current is None:
        raise ReferenceNotFound(reference_idx)
    raise IllegalStatusTransition(current=current, target=target)
