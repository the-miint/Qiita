"""Repository functions and composers for the qiita.study tables.

Functions take an asyncpg.Connection as their first positional argument,
never acquire their own connection, and never open their own top-level
transaction; the caller controls transaction scope so multiple writes
compose atomically on one connection. The `create_study` composer guards
on `conn.is_in_transaction()` at entry and raises if the caller forgot
to wrap the call in a transaction.
"""

import json
from typing import get_args

import asyncpg
from qiita_common.models import StudyAccessionField, Tier

from . import require_transaction, update_row

# Columns returned by every create_study INSERT ... RETURNING. Covers every
# caller-visible column on the row; the route consumes the result via
# named-key access so column order in this string is not load-bearing.
_STUDY_RETURNING_COLS = (
    "idx, owner_idx, principal_investigator_idx, title, alias,"
    " description, abstract, funding, ena_study_accession,"
    " bioproject_accession, notes, last_submission_at, submission_error,"
    " extra_metadata, default_tier, created_by_idx,"
    " created_at, updated_at"
)


async def fetch_study(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    study_idx: int,
    *,
    for_update: bool = False,
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

    `for_update=True` appends `FOR UPDATE`; concurrent callers
    serialize on the row lock until the holder commits or rolls back.
    Pass only inside an open transaction — with a pool the implicit
    single-statement transaction releases the lock immediately and
    the flag is a no-op.
    """
    # Single-row fetch by idx; same column list as the INSERT RETURNING
    # so route handlers share one row → response shaping path.
    sql = f"SELECT {_STUDY_RETURNING_COLS} FROM qiita.study WHERE idx = $1"
    if for_update:
        sql += " FOR UPDATE"
    return await pool_or_conn.fetchrow(sql, study_idx)


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


async def fetch_study_idxs_by_accession(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    values: list[str],
    accession_field: StudyAccessionField = "bioproject_accession",
) -> dict[str, int]:
    """Return `{accession: study_idx}` for every value in `values` that
    resolves to a qiita.study row on the column named by `accession_field`
    (default bioproject_accession). Values absent from the table are omitted
    from the returned map.

    The study table has no soft-delete column, so every key in the result
    is a live row.
    """
    # Pin the interpolated column to the closed StudyAccessionField set; an
    # out-of-set value cannot reach the SQL identifier.
    if accession_field not in get_args(StudyAccessionField):
        raise ValueError(f"invalid study accession field: {accession_field!r}")
    if not values:
        return {}
    rows = await pool_or_conn.fetch(
        f"SELECT idx, {accession_field} FROM qiita.study WHERE {accession_field} = ANY($1::text[])",
        values,
    )
    return {r[accession_field]: r["idx"] for r in rows}


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
    ena_study_accession: str | None = None,
    bioproject_accession: str | None = None,
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
        "    description, abstract, funding, ena_study_accession,"
        "    bioproject_accession, notes, extra_metadata, default_tier,"
        "    created_by_idx"
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
        ena_study_accession,
        bioproject_accession,
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


# Columns this repo's PATCH composer is allowed to write. Held as a
# frozenset so unknown column names are rejected at the repo boundary
# rather than reaching the SQL builder. owner_idx is intentionally
# excluded (ownership transfer is a separate surface); default_tier is
# intentionally excluded (its policy-shape needs its own design); the
# submission-tracking columns are intentionally excluded (subsystem-owned,
# and this route is owner-accessible).
STUDY_PATCHABLE_COLUMNS: frozenset[str] = frozenset(
    {
        "title",
        "alias",
        "description",
        "abstract",
        "funding",
        "ena_study_accession",
        "bioproject_accession",
        "notes",
        "extra_metadata",
        "principal_investigator_idx",
    }
)

# Columns serialized + cast as JSONB by the UPDATE composer. The
# update_row helper json.dumps the value and emits `$N::jsonb`.
_STUDY_JSONB_COLUMNS: frozenset[str] = frozenset({"extra_metadata"})


async def update_study(
    conn: asyncpg.Connection,
    study_idx: int,
    *,
    fields: dict[str, object],
) -> asyncpg.Record | None:
    """Update the named columns on the study row, return the post-UPDATE row.

    Thin wrapper around the shared update_row composer that pins the
    table, allowlist, RETURNING shape, and JSONB-cast column set for
    qiita.study. See update_row for the field-validation and
    explicit-null semantics; see fetch_study for the returned column
    list.
    """
    return await update_row(
        conn,
        table="study",
        row_idx=study_idx,
        fields=fields,
        allowlist=STUDY_PATCHABLE_COLUMNS,
        returning_cols=_STUDY_RETURNING_COLS,
        jsonb_cols=_STUDY_JSONB_COLUMNS,
        repo_name="update_study",
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
    ena_study_accession: str | None = None,
    bioproject_accession: str | None = None,
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
        ena_study_accession=ena_study_accession,
        bioproject_accession=bioproject_accession,
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


# Constraint names create_study's underlying INSERT can trip when a
# concurrent caller wins the race for the same accession; used by
# get_or_create_study_by_ena_accessions to distinguish "lost the create
# race" from any other UniqueViolationError.
_STUDY_ACCESSION_UNIQUE_CONSTRAINTS = frozenset(
    {"study_bioproject_accession_unique", "study_ena_study_accession_unique"}
)


async def get_or_create_study_by_ena_accessions(
    conn: asyncpg.Connection,
    *,
    bioproject_accession: str,
    ena_study_accession: str | None,
    owner_idx: int,
    created_by_idx: int,
    title: str,
) -> tuple[asyncpg.Record, bool]:
    """Race-safe find-or-create for an ENA-study import (T02-1).

    Positional accession mapping is the caller's responsibility (see
    `ena_import.registration.register_ena_study`): this function only
    keys the lookup/insert on the two accession values it is handed --
    it does not itself decide which resolved ENA field maps to which
    column.

    Looks up an existing study by bioproject_accession first (the cheap,
    common re-import path, avoiding an unnecessary create attempt). On a
    miss, attempts create_study inside `async with conn.transaction():`
    -- a real `BEGIN` if `conn` is not already inside a transaction, a
    `SAVEPOINT` otherwise (asyncpg's `Connection.transaction()` detects
    which; see `_sample_helpers.write_global_metadata_or_diagnose` for
    the same nested-transaction pattern) -- and falls back to the same
    accession lookup on `asyncpg.UniqueViolationError` from either
    study_bioproject_accession_unique or study_ena_study_accession_unique.
    create_study has no ON CONFLICT variant because it is a two-step
    composer (INSERT + the owner's auto-granted study_access row), so a
    plain `INSERT ... ON CONFLICT DO NOTHING RETURNING` cannot express
    "reuse the existing row's access grant too" -- catch-and-refetch is
    the race-safe shape available to a composer, not a single INSERT.

    Returns (row, created): row is the same RETURNING/fetch_study column
    shape either way; created is True only on the insert branch.
    """
    existing_row = await _fetch_study_by_bioproject_accession(conn, bioproject_accession)
    if existing_row is not None:
        return existing_row, False

    try:
        async with conn.transaction():
            row = await create_study(
                conn,
                owner_idx=owner_idx,
                created_by_idx=created_by_idx,
                title=title,
                ena_study_accession=ena_study_accession,
                bioproject_accession=bioproject_accession,
            )
        return row, True
    except asyncpg.UniqueViolationError as exc:
        if exc.constraint_name not in _STUDY_ACCESSION_UNIQUE_CONSTRAINTS:
            raise
        # Lost the create race; the winner's row satisfies the same
        # accession lookup this function started with.
        existing_row = await _fetch_study_by_bioproject_accession(conn, bioproject_accession)
        if existing_row is None:
            # qiita.study has no delete surface, so this branch is not
            # reachable in practice -- kept as a fail-loud backstop rather
            # than a silent None return, mirroring insert_sequencing_run's
            # equivalent guard.
            raise asyncpg.PostgresError(
                "find-or-create on study(bioproject_accession="
                f"{bioproject_accession!r}) collided on insert but the"
                " existing row is not visible"
            ) from exc
        return existing_row, False


async def _fetch_study_by_bioproject_accession(
    conn: asyncpg.Connection, bioproject_accession: str
) -> asyncpg.Record | None:
    """Resolve one study row by bioproject_accession, or None on miss."""
    idxs = await fetch_study_idxs_by_accession(
        conn, values=[bioproject_accession], accession_field="bioproject_accession"
    )
    existing_idx = idxs.get(bioproject_accession)
    if existing_idx is None:
        return None
    return await fetch_study(conn, existing_idx)
