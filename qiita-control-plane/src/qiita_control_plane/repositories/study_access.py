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
