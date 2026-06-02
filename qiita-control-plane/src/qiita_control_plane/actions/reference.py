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
