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
    access_tier = Tier(row["access_tier"]) if row["access_tier"] is not None else None
    return CallerStudyAccessRow(
        owner_idx=row["owner_idx"],
        access_tier=access_tier,
        default_tier=Tier(row["default_tier"]),
    )
