"""Repository functions for study-access resolution.

Functions take an asyncpg.Connection or asyncpg.Pool as their first
positional argument; they never acquire their own connection or
transaction. They return data, not policy — the access predicate lives
in qiita_control_plane.auth.study_access and consumes the shapes
returned here.
"""

from typing import NamedTuple

import asyncpg
from qiita_common.models import Tier


class CallerStudyAccessRow(NamedTuple):
    """Owner, caller-tier, and study default_tier for one (caller, study) pair.

    `owner_idx` is the study's owner principal_idx. `access_tier` is the
    caller's tier on that study, or None when the caller has no
    qiita.study_access row (effective tier 'public' by absence; the
    interpretation is policy-layer, not data-layer). `default_tier` is
    the study's own default access tier, used by guards that resolve
    their `min_tier` per-study rather than per-route.
    """

    owner_idx: int
    access_tier: Tier | None
    default_tier: Tier


async def fetch_caller_study_access(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    principal_idx: int,
    study_idx: int,
) -> CallerStudyAccessRow | None:
    """Return the caller's access row for one study, or None if no study.

    Single LEFT JOIN: qiita.study → qiita.study_access on
    (study_idx, principal_idx). Resolves access_tier to a Tier enum
    member, leaving NULL as None for the caller layer to interpret as
    'public-by-absence'. Also returns the study's own `default_tier`
    so guards that compare against the study-default can do so without
    a second round trip.
    """
    # One round trip; LEFT JOIN preserves the study row even when the
    # caller has no study_access row.
    row = await conn.fetchrow(
        "SELECT s.owner_idx, s.default_tier, sa.access_tier"
        " FROM qiita.study s"
        " LEFT JOIN qiita.study_access sa"
        "   ON sa.study_idx = s.idx AND sa.principal_idx = $2"
        " WHERE s.idx = $1",
        study_idx,
        principal_idx,
    )
    if row is None:
        return None
    return _to_access_row(row)


async def fetch_caller_study_access_batch(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    principal_idx: int,
    study_idxs: list[int],
) -> dict[int, CallerStudyAccessRow]:
    """Batched `fetch_caller_study_access`: one query for many studies.

    A study_idx with no `qiita.study` row is absent from the mapping, exactly as
    the single-study form returns None for it.

    Exists because the narrowing filter it feeds
    (`auth.guards.filter_studies_caller_can_read`) is reachable from a route
    where the CALLER chooses the identifier list, so the number of distinct
    studies is attacker-controlled rather than bounded by a real pool's shape.
    One round trip per study would make that a request-amplification lever.
    """
    if not study_idxs:
        return {}
    rows = await conn.fetch(
        "SELECT s.idx, s.owner_idx, s.default_tier, sa.access_tier"
        " FROM qiita.study s"
        " LEFT JOIN qiita.study_access sa"
        "   ON sa.study_idx = s.idx AND sa.principal_idx = $2"
        " WHERE s.idx = ANY($1::bigint[])",
        study_idxs,
        principal_idx,
    )
    return {row["idx"]: _to_access_row(row) for row in rows}


def _to_access_row(row: asyncpg.Record) -> CallerStudyAccessRow:
    """Shared row → CallerStudyAccessRow mapping for the two fetches above, so
    the NULL-access_tier convention has one definition."""
    return CallerStudyAccessRow(
        owner_idx=row["owner_idx"],
        access_tier=Tier(row["access_tier"]) if row["access_tier"] is not None else None,
        default_tier=Tier(row["default_tier"]),
    )


# ---------------------------------------------------------------------------
# qiita.study_access rows (the grant surface)
# ---------------------------------------------------------------------------

# Every read below returns this column set, so the route maps rows one way.
# `email` is NULL for a grantee with no qiita.user row (a service account
# granted by hand); the FK is to qiita.principal, not qiita.user.
_ACCESS_ROW_COLUMNS = (
    "sa.study_idx, sa.principal_idx, u.email, sa.access_tier, sa.granted_by_idx, sa.granted_at"
)


class GranteeCandidate(NamedTuple):
    """The principal a grant names by email, with the state a grant checks."""

    principal_idx: int
    disabled: bool
    retired: bool


async def fetch_grantee_by_email(
    conn: asyncpg.Connection | asyncpg.Pool, *, email: str
) -> GranteeCandidate | None:
    """Resolve an email to the qiita.user principal carrying it, or None.

    `qiita.user.email` is CITEXT, so the match is case-insensitive.
    """
    row = await conn.fetchrow(
        "SELECT p.idx, p.disabled, p.retired"
        " FROM qiita.user u JOIN qiita.principal p ON p.idx = u.principal_idx"
        " WHERE u.email = $1",
        email,
    )
    if row is None:
        return None
    return GranteeCandidate(
        principal_idx=row["idx"], disabled=row["disabled"], retired=row["retired"]
    )


async def list_study_access(
    conn: asyncpg.Connection | asyncpg.Pool, *, study_idx: int
) -> list[asyncpg.Record]:
    """Every study_access row on one study, highest tier first, then by email."""
    return await conn.fetch(
        f"SELECT {_ACCESS_ROW_COLUMNS}"
        " FROM qiita.study_access sa"
        " LEFT JOIN qiita.user u ON u.principal_idx = sa.principal_idx"
        " WHERE sa.study_idx = $1"
        " ORDER BY sa.access_tier DESC, u.email",
        study_idx,
    )


async def fetch_study_access_row(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    principal_idx: int,
    for_update: bool = False,
) -> asyncpg.Record | None:
    """One grantee's row on one study, or None. `for_update` locks it for the
    rest of the caller's transaction, so a concurrent change or revoke of the
    same row serializes behind this one and re-checks the tier it finds."""
    lock = " FOR UPDATE OF sa" if for_update else ""
    return await conn.fetchrow(
        f"SELECT {_ACCESS_ROW_COLUMNS}"
        " FROM qiita.study_access sa"
        " LEFT JOIN qiita.user u ON u.principal_idx = sa.principal_idx"
        f" WHERE sa.study_idx = $1 AND sa.principal_idx = $2{lock}",
        study_idx,
        principal_idx,
    )


async def lock_caller_study_access_row(
    conn: asyncpg.Connection, *, study_idx: int, principal_idx: int
) -> None:
    """Share-lock the caller's own row on the study, if they have one, for the
    rest of the transaction. A concurrent change or revoke of that row waits
    until this transaction ends; one that already committed is what the next
    read sees."""
    await conn.execute(
        "SELECT 1 FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2 FOR SHARE",
        study_idx,
        principal_idx,
    )


async def insert_study_access(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    principal_idx: int,
    access_tier: Tier,
    granted_by_idx: int,
) -> asyncpg.Record:
    """Insert a grant and return it. Raises asyncpg.UniqueViolationError
    (`study_access_unique_per_principal`) when the grantee already has a row."""
    await conn.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_idx,
        principal_idx,
        access_tier,
        granted_by_idx,
    )
    row = await fetch_study_access_row(conn, study_idx=study_idx, principal_idx=principal_idx)
    if row is None:
        raise RuntimeError(
            f"study_access row ({study_idx}, {principal_idx}) missing right after its INSERT"
        )
    return row


async def update_study_access_tier(
    conn: asyncpg.Connection, *, study_idx: int, principal_idx: int, access_tier: Tier
) -> None:
    """Set an existing row's tier. The caller has already locked the row."""
    await conn.execute(
        "UPDATE qiita.study_access SET access_tier = $3::qiita.tier"
        " WHERE study_idx = $1 AND principal_idx = $2",
        study_idx,
        principal_idx,
        access_tier,
    )


async def delete_study_access(
    conn: asyncpg.Connection, *, study_idx: int, principal_idx: int
) -> None:
    """Hard-delete a row (study_access keeps no revoked state)."""
    await conn.execute(
        "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
        study_idx,
        principal_idx,
    )
