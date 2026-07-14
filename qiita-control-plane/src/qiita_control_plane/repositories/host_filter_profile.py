"""Repository functions for the qiita.host_filter_profile table.

The table maps a host taxon + sequencing platform to the reference build(s) used
for host-read depletion — the config layer that keeps "which build" a
submission-time choice while "which organism" stays biosample metadata.

Read functions accept either a pool or a connection so they compose inside an
open transaction or stand alone. The write function takes a connection, never
acquires its own, and never opens its own top-level transaction; the caller owns
transaction scope.

Every function returns the typed qiita_common.models.HostFilterProfile rather
than an asyncpg.Record: the row is small, closed, and read by a resolver that
branches on its fields, so the model earns its keep here in a way it would not
for the wide, mostly-passthrough rows elsewhere in this package.
"""

import asyncpg
from qiita_common.models import HostFilterProfile, Platform

# The column list every read selects. Kept as one constant so the SELECT shape
# and the model's field set cannot drift apart across the three functions.
_COLUMNS = "idx, host_term_idx, platform, rype_reference_idx, minimap2_reference_idx"


def _to_model(row: asyncpg.Record) -> HostFilterProfile:
    return HostFilterProfile(**dict(row))


async def get_host_filter_profile(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    host_term_idx: int,
    platform: Platform,
) -> HostFilterProfile | None:
    """Return the profile for (host_term_idx, platform), or None if none exists.

    The single-row lookup the host-filter resolver calls. `None` is the ordinary
    "this host has no profile on this platform" answer, not an error — the
    resolver turns it into an UNRESOLVED outcome with a reason, because only the
    caller knows whether that is fatal.

    A UNIQUE constraint on (host_term_idx, platform) makes the single row
    unambiguous by construction, so this never has to choose among candidates.
    """
    row = await pool_or_conn.fetchrow(
        f"SELECT {_COLUMNS}"
        "  FROM qiita.host_filter_profile"
        " WHERE host_term_idx = $1 AND platform = $2",
        host_term_idx,
        platform,
    )
    return _to_model(row) if row is not None else None


async def insert_host_filter_profile(
    conn: asyncpg.Connection,
    *,
    host_term_idx: int,
    platform: Platform,
    rype_reference_idx: int,
    minimap2_reference_idx: int | None = None,
    principal_idx: int,
) -> HostFilterProfile:
    """Insert one profile row and return it.

    Raises asyncpg.UniqueViolationError when a profile already exists for
    (host_term_idx, platform) — a rebuild of the host DB is an UPDATE of the
    existing row's reference idx, not a second row, so a duplicate insert is
    caller error rather than something to swallow. Raises
    asyncpg.ForeignKeyViolationError for an unknown term, reference, or
    principal.
    """
    row = await conn.fetchrow(
        "INSERT INTO qiita.host_filter_profile"
        " (host_term_idx, platform, rype_reference_idx, minimap2_reference_idx,"
        "  created_by_idx)"
        " VALUES ($1, $2, $3, $4, $5)"
        f" RETURNING {_COLUMNS}",
        host_term_idx,
        platform,
        rype_reference_idx,
        minimap2_reference_idx,
        principal_idx,
    )
    return _to_model(row)


async def list_host_filter_profiles(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    platform: Platform | None = None,
) -> list[HostFilterProfile]:
    """Return every profile, optionally narrowed to one platform.

    Ordered by (host_term_idx, platform) so the listing is stable across calls
    — it feeds an operator-facing "what could I filter against" surface, where a
    shifting order would read as churn.
    """
    rows = await pool_or_conn.fetch(
        f"SELECT {_COLUMNS}"
        "  FROM qiita.host_filter_profile"
        " WHERE $1::qiita.platform IS NULL OR platform = $1"
        " ORDER BY host_term_idx, platform",
        platform,
    )
    return [_to_model(row) for row in rows]
