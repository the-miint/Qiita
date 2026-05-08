"""Repository functions and composers for the qiita.study tables.

Functions take an asyncpg.Connection as their first positional argument,
never acquire their own connection, and never open their own top-level
transaction; the caller controls transaction scope so multiple writes
compose atomically on one connection. The `create_study` composer guards
on `conn.is_in_transaction()` at entry and raises if the caller forgot
to wrap the call in a transaction.
"""

import json

import asyncpg
from qiita_common.models import Tier

from . import require_transaction

# Columns returned by every create_study INSERT ... RETURNING. Covers every
# caller-visible column on the row; the route consumes the result via
# named-key access so column order in this string is not load-bearing.
_STUDY_RETURNING_COLS = (
    "idx, owner_idx, principal_investigator_idx, title, alias,"
    " description, abstract, funding, ebi_study_accession, vamps_id,"
    " notes, extra_metadata, default_tier, created_by_idx,"
    " created_at, updated_at"
)


async def fetch_study(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    study_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.study row for the given idx, or None if no match.

    Selects the same caller-visible column set produced by
    `create_study` (see `_STUDY_RETURNING_COLS`) so a route handler can
    reuse the same row → response mapping for both POST and GET. The
    returned record carries `extra_metadata` as a JSONB-text string
    when present; the caller is responsible for `json.loads` if a dict
    shape is needed (mirrors the create-route pattern). Accepts either
    a pool or a connection so the helper composes inside an open
    transaction or stands alone.
    """
    # Single-row fetch by idx; same column list as the INSERT RETURNING
    # so route handlers share one row → response shaping path.
    return await pool_or_conn.fetchrow(
        f"SELECT {_STUDY_RETURNING_COLS} FROM qiita.study WHERE idx = $1",
        study_idx,
    )


async def fetch_study_exists(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    study_idx: int,
) -> bool:
    """Return True iff qiita.study has a row with the given idx.

    Sibling of fetch_caller_study_access for the case where a route only
    needs to know whether the study exists (e.g., to raise 404 on a
    nonexistent path resource) and does not need the caller's access tier.
    Accepts either a pool or a connection so the helper composes inside an
    open transaction or stands alone.
    """
    # Existence-only fetch; the SELECT 1 form keeps the row payload minimal.
    return (
        await pool_or_conn.fetchval(
            "SELECT 1 FROM qiita.study WHERE idx = $1",
            study_idx,
        )
        is not None
    )


async def insert_study(
    conn: asyncpg.Connection,
    *,
    owner_idx: int,
    created_by_idx: int,
    title: str,
    principal_investigator_idx: int | None = None,
    alias: str | None = None,
    description: str | None = None,
    abstract: str | None = None,
    funding: str | None = None,
    ebi_study_accession: str | None = None,
    vamps_id: str | None = None,
    notes: str | None = None,
    extra_metadata: dict | None = None,
    default_tier: Tier | None = None,
) -> asyncpg.Record:
    """Insert one row into qiita.study and return all caller-visible columns.

    Exposes every column the caller may legitimately set on a fresh row.
    `default_tier=None` lets the schema default ('member') apply. The
    generated `search_vector` and the trigger-managed `updated_at` are
    populated by Postgres and are part of the RETURNING list so the
    caller has the complete row.

    Raises asyncpg.PostgresError on FK violation (e.g., a non-existent
    `principal_investigator_idx`) or trigger rejection (e.g., owner is
    not a user-kind principal).
    """
    # Serialize the JSONB column once so asyncpg ships it as text + ::jsonb.
    extra_metadata_json = json.dumps(extra_metadata) if extra_metadata is not None else None

    # Single INSERT carrying every settable column; defaulted columns
    # receive NULL and the schema-default kicks in for default_tier.
    return await conn.fetchrow(
        "INSERT INTO qiita.study ("
        "    owner_idx, principal_investigator_idx, title, alias,"
        "    description, abstract, funding, ebi_study_accession,"
        "    vamps_id, notes, extra_metadata, default_tier, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb,"
        "          COALESCE($12::qiita.tier, 'member'::qiita.tier), $13)"
        f" RETURNING {_STUDY_RETURNING_COLS}",
        owner_idx,
        principal_investigator_idx,
        title,
        alias,
        description,
        abstract,
        funding,
        ebi_study_accession,
        vamps_id,
        notes,
        extra_metadata_json,
        default_tier,
        created_by_idx,
    )


async def insert_owner_study_access_admin(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    owner_idx: int,
    granted_by_idx: int,
) -> None:
    """Grant the study's owner ADMIN access in qiita.study_access.

    The auto-grant ensures the owner appears explicitly in study_access
    queries even though the owner-bypass path in
    `auth.guards.require_study_access` would let the owner through
    without one. `granted_by_idx` is the caller (which equals owner_idx
    on self-create and the on-behalf actor on admin-creates-for-other).

    Raises asyncpg.UniqueViolationError if a row already exists for this
    (study_idx, owner_idx) pair (impossible during create_study but
    surfaced for callers reusing the helper later).
    """
    # Single INSERT against the (study_idx, principal_idx) unique constraint;
    # access_tier is hardcoded to ADMIN because that is the only tier that
    # makes sense for the owner-auto-grant path.
    await conn.execute(
        "INSERT INTO qiita.study_access ("
        "    study_idx, principal_idx, access_tier, granted_by_idx"
        ") VALUES ($1, $2, 'admin'::qiita.tier, $3)",
        study_idx,
        owner_idx,
        granted_by_idx,
    )


async def create_study(
    conn: asyncpg.Connection,
    *,
    owner_idx: int,
    created_by_idx: int,
    title: str,
    principal_investigator_idx: int | None = None,
    alias: str | None = None,
    description: str | None = None,
    abstract: str | None = None,
    funding: str | None = None,
    ebi_study_accession: str | None = None,
    vamps_id: str | None = None,
    notes: str | None = None,
    extra_metadata: dict | None = None,
    default_tier: Tier | None = None,
) -> asyncpg.Record:
    """Create a study and the owner's auto-granted ADMIN study_access row.

    The caller must wrap the call in `async with conn.transaction():`;
    the guard at entry raises RuntimeError otherwise so partial failure
    cannot leave a study row without its access grant.
    """
    # Fail-fast guard against caller forgetting to wrap in a transaction.
    require_transaction(conn)

    # Step a: insert the study row, returning every caller-visible column.
    study_row = await insert_study(
        conn,
        owner_idx=owner_idx,
        created_by_idx=created_by_idx,
        title=title,
        principal_investigator_idx=principal_investigator_idx,
        alias=alias,
        description=description,
        abstract=abstract,
        funding=funding,
        ebi_study_accession=ebi_study_accession,
        vamps_id=vamps_id,
        notes=notes,
        extra_metadata=extra_metadata,
        default_tier=default_tier,
    )

    # Step b: auto-grant the owner ADMIN access on the freshly created study.
    await insert_owner_study_access_admin(
        conn,
        study_idx=study_row["idx"],
        owner_idx=owner_idx,
        granted_by_idx=created_by_idx,
    )

    return study_row
