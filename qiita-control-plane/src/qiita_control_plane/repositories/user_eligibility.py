"""Repository function for user-eligibility data fetches.

Returns the principal+user state flags a route needs to apply
use-case-specific eligibility checks (must-be-user, not-disabled,
not-retired, profile-complete). The repository's contract is to surface
the data; which combinations of flags are eligible is policy and lives
in the calling route.

Distinct from the auth-time machinery in qiita_control_plane.auth.principal:
that path resolves the calling principal and raises 401 on
disabled/retired. This path examines a candidate principal_idx (e.g.,
a body-supplied biosample owner_idx) and returns flags so the route can
emit the appropriate 422 message.
"""

from typing import NamedTuple

import asyncpg


class UserEligibility(NamedTuple):
    """Flags needed to evaluate user-eligibility policy.

    `is_user` is True iff the principal has a qiita.user row.
    `disabled` and `retired` reflect qiita.principal state. `profile_complete`
    reflects qiita.user.profile_complete and defaults to False when no
    user row exists; only meaningful when is_user is True.
    """

    is_user: bool
    disabled: bool
    retired: bool
    profile_complete: bool


async def fetch_user_eligibility(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    principal_idx: int,
) -> UserEligibility | None:
    """Return user-eligibility flags for a principal, or None if no principal.

    Single LEFT JOIN: qiita.principal → qiita.user on principal_idx.
    Returns None if qiita.principal has no row for this idx; otherwise
    returns the four flags. Callers apply their own combination policy.
    """
    # One round trip; LEFT JOIN preserves principal even when there's no
    # user subtype, so non-user-kind principals are surfaced with
    # is_user=False rather than None.
    row = await conn.fetchrow(
        "SELECT p.disabled, p.retired,"
        " u.principal_idx IS NOT NULL AS is_user,"
        " COALESCE(u.profile_complete, false) AS profile_complete"
        " FROM qiita.principal p"
        " LEFT JOIN qiita.user u ON u.principal_idx = p.idx"
        " WHERE p.idx = $1",
        principal_idx,
    )
    if row is None:
        return None
    return UserEligibility(
        is_user=row["is_user"],
        disabled=row["disabled"],
        retired=row["retired"],
        profile_complete=row["profile_complete"],
    )
