"""Repository functions for biosample metadata tables.

Mirrors the qiita.biosample_metadata / biosample_study_field /
biosample_global_field DB layout. Functions take an asyncpg.Connection as
their first positional argument, never acquire their own connection, and
never open their own top-level transaction; the caller controls
transaction scope so multiple writes compose atomically on one connection.

The biosample-import composer in repositories.biosample re-uses the
helpers here; routes that surface metadata-shaped errors import the
exception classes from this module.
"""

from collections.abc import Iterable
from datetime import date
from decimal import Decimal

import asyncpg
from qiita_common.models import FieldDataType, Tier

from . import require_transaction
from ._sample_helpers import (
    GLOBAL_METADATA_VALUE_COLUMN,
    EntityMetadataSpec,
    GlobalFieldRow,
    GlobalMetadataRow,
    SampleEntityKind,
    StudyFieldConflictError,
)

# ---------------------------------------------------------------------------
# Structured exceptions raised by the import path
# ---------------------------------------------------------------------------
# BiosampleOwnerIdFieldCollisionError has no prep_sample analog and stays
# in this entity-specific module rather than the cross-entity helpers.


class BiosampleOwnerIdFieldCollisionError(Exception):
    """Raised when import metadata carries an entry whose key equals the
    request's owner_biosample_id_field_name. The owner-biosample-id row is
    purely-local and flagged; allowing the same display_name as a globally
    linked metadata entry would conflict with that contract.
    """

    def __init__(self, display_name: str) -> None:
        self.display_name = display_name
        super().__init__(
            f"metadata key {display_name!r} collides with owner_biosample_id_field_name"
        )


async def fetch_biosample_global_fields_by_display_names(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    display_names: Iterable[str],
) -> dict[str, GlobalFieldRow]:
    """Return a dict of display_name -> GlobalFieldRow for the matching
    rows in qiita.biosample_global_field.

    Display names that have no matching row are absent from the returned
    dict; callers detect "unknown field" by checking dict membership for
    each requested name. Empty input short-circuits with no DB call.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    names = list(display_names)
    if not names:
        return {}

    # Single SELECT keyed on display_name = ANY($1::text[]).
    rows = await pool_or_conn.fetch(
        "SELECT idx, display_name, data_type"
        " FROM qiita.biosample_global_field"
        " WHERE display_name = ANY($1::text[])",
        names,
    )

    # Wrap each row in the typed tuple, keyed on display_name.
    return {
        r["display_name"]: GlobalFieldRow(
            idx=r["idx"],
            display_name=r["display_name"],
            data_type=FieldDataType(r["data_type"]),
        )
        for r in rows
    }


async def fetch_global_metadata_for_biosample(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    biosample_idx: int,
) -> dict[str, GlobalMetadataRow]:
    """Return a dict of internal_name -> GlobalMetadataRow for every
    globally-linked metadata value the biosample carries.

    Filters on biosample_metadata.global_field_idx IS NOT NULL: purely-local
    rows (including the owner-biosample-id row) are excluded. The read is
    not study-scoped — the canonical global value persists across
    biosample_to_study link retirement, and per-study read access is
    governed by the study_access predicate at the caller's auth boundary,
    not here. Missing-reason rows (value_missing_reason_idx populated)
    are also excluded -- they have no typed value to surface and the
    import path does not currently write them.

    Currently supports data_type in {TEXT, NUMERIC, DATE} (matching the
    import path's closed set); rows of other data_types raise
    NotImplementedError so a future addition is a coordinated extension
    of read + write paths.
    """
    # Pull every globally-linked, non-missing-reason row for the biosample
    # in one round trip; carry every typed value column the closed set covers.
    rows = await pool_or_conn.fetch(
        "SELECT bgf.internal_name, bgf.display_name, bgf.description, bgf.data_type,"
        " bm.value_text, bm.value_numeric, bm.value_date"
        " FROM qiita.biosample_metadata bm"
        " JOIN qiita.biosample_global_field bgf ON bgf.idx = bm.global_field_idx"
        " WHERE bm.biosample_idx = $1"
        "   AND bm.global_field_idx IS NOT NULL"
        "   AND bm.value_missing_reason_idx IS NULL",
        biosample_idx,
    )

    # Walk rows, dispatch each to the value column the data_type names.
    # The unsupported branch raises so an out-of-set data_type cannot
    # silently surface a NULL value.
    result: dict[str, GlobalMetadataRow] = {}
    for r in rows:
        data_type = FieldDataType(r["data_type"])
        column = GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
        if column is None:
            raise NotImplementedError(
                f"global metadata read for data_type={data_type} is not yet implemented"
            )
        result[r["internal_name"]] = GlobalMetadataRow(
            internal_name=r["internal_name"],
            display_name=r["display_name"],
            description=r["description"],
            data_type=data_type,
            value=r[column],
        )
    return result


async def get_or_create_globally_linked_biosample_study_field(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    global_field_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
) -> tuple[int, bool]:
    """Find a biosample_study_field linked to global_field_idx; create on miss.

    `global_field_idx` is the biosample_global_field row this study_field
    binds to; the SQL column on biosample_study_field is named
    biosample_global_field_idx, but callers pass the entity-suffix-stripped
    kwarg so the function's signature matches the parallel prep_sample
    helper and the cross-entity write function in repositories.__init__.

    Returns (idx, created): created is True when this call inserted the
    row; False when the fallback SELECT branch resolved against a row a
    concurrent caller had already committed (or that pre-existed entirely).

    The created row populates biosample_global_field_idx and leaves
    data_type / required / terminology_idx / tier_override NULL per the
    biosample_study_field_inheritance_consistent CHECK; the global field
    owns those fields.

    Raises StudyFieldConflictError(entity_kind=SampleEntityKind.BIOSAMPLE, ...) when a
    row at (study_idx, display_name) already exists but is purely-local
    (biosample_global_field_idx IS NULL) or is bound to a different global
    field. Both cases mean the caller is trying to write metadata against
    a field that is not the global field they think it is; silently
    returning the existing idx would attach the value to the wrong field.

    Concurrency: same INSERT ... ON CONFLICT DO NOTHING RETURNING idx +
    fallback SELECT pattern as the local sibling, race-free under
    READ COMMITTED.
    """
    # Both branches must observe the same snapshot; require a wrapping
    # transaction so the INSERT and the fallback SELECT cannot straddle
    # an implicit-commit boundary.
    require_transaction(conn)

    # Create branch — globally-linked row leaves the inherited columns NULL.
    idx = await conn.fetchval(
        "INSERT INTO qiita.biosample_study_field ("
        "    study_idx, biosample_global_field_idx,"
        "    display_name, description, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5)"
        " ON CONFLICT (study_idx, display_name) DO NOTHING"
        " RETURNING idx",
        study_idx,
        global_field_idx,
        display_name,
        description,
        created_by_idx,
    )
    if idx is not None:
        return idx, True

    # Fallback branch — existing row at (study_idx, display_name). Verify
    # its global link matches what the caller asked for; otherwise the
    # row is bound to a different global field (or none) and reusing it would
    # attach the value to the wrong field.
    row = await conn.fetchrow(
        "SELECT idx, biosample_global_field_idx"
        " FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    if row["biosample_global_field_idx"] != global_field_idx:
        raise StudyFieldConflictError(
            entity_kind=SampleEntityKind.BIOSAMPLE,
            study_idx=study_idx,
            display_name=display_name,
            expected_global_field_idx=global_field_idx,
            found_global_field_idx=row["biosample_global_field_idx"],
        )
    return row["idx"], False


async def get_or_create_local_biosample_study_field(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
    data_type: FieldDataType = FieldDataType.TEXT,
    required: bool = False,
    terminology_idx: int | None = None,
    tier_override: Tier | None = None,
) -> tuple[int, bool, int | None]:
    """Find a biosample_study_field by (study_idx, display_name); create local on miss.

    Returns (idx, created, biosample_global_field_idx): created is True
    when this call inserted the row on the create branch, False when the
    fallback SELECT branch resolved against a row a concurrent caller had
    already committed (or that pre-existed entirely). The third element
    is the resolved row's biosample_global_field_idx — None for a
    purely-local row, non-None when the row turned out to be globally
    linked (the lookup branch can resolve to either kind; the create
    branch always produces a purely-local row and reports None).

    The lookup branch returns the existing row's idx whether the row is
    currently linked to a biosample_global_field or purely local — a
    downstream metadata write against either kind is well-defined because
    the biosample_metadata_apply_field_contract trigger reads from
    biosample_study_field.biosample_global_field_idx either way.
    Surfacing the link status in the return tuple lets callers that need
    strict local-only semantics reject a globally-linked resolution
    rather than silently writing through it.

    The create branch produces a purely-local row (biosample_global_field_idx
    NULL); creating a globally-linked row is a separate operation because
    its non-null inputs are the inverse set per the
    biosample_study_field_inheritance_consistent CHECK.

    Concurrency: the natural-key UNIQUE constraint
    biosample_study_field_display_name_unique can race two concurrent callers
    for the same (study_idx, display_name). Implemented as
    INSERT ... ON CONFLICT DO NOTHING RETURNING idx with a fallback SELECT on
    miss so concurrent callers converge on the same idx without surfacing
    UniqueViolationError. Race-free under READ COMMITTED (the project default,
    set in qiita_control_plane.db); under REPEATABLE READ / SERIALIZABLE the
    fallback SELECT could miss a row committed after the transaction snapshot
    and a different pattern would be required.
    """
    # Create branch — purely-local row, biosample_global_field_idx left NULL.
    # ON CONFLICT DO NOTHING absorbs the unique-constraint hit so the
    # concurrent loser of the race does not raise.
    idx = await conn.fetchval(
        "INSERT INTO qiita.biosample_study_field ("
        "    study_idx, display_name, description,"
        "    data_type, required, terminology_idx, tier_override,"
        "    created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        " ON CONFLICT (study_idx, display_name) DO NOTHING"
        " RETURNING idx",
        study_idx,
        display_name,
        description,
        data_type,
        required,
        terminology_idx,
        tier_override,
        created_by_idx,
    )
    if idx is not None:
        # Create branch — the row is purely-local by construction.
        return idx, True, None

    # Lookup branch — fallback fires only on conflict; takes a fresh snapshot
    # under READ COMMITTED so it sees the row the concurrent winner committed.
    # Surface biosample_global_field_idx so callers can detect a
    # globally-linked resolution.
    row = await conn.fetchrow(
        "SELECT idx, biosample_global_field_idx"
        " FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    return row["idx"], False, row["biosample_global_field_idx"]


async def insert_biosample_metadata_text(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    biosample_study_field_idx: int,
    value_text: str,
    created_by_idx: int,
    is_owner_biosample_id: bool = False,
) -> int:
    """Insert a text-valued biosample_metadata row and return its idx.

    The biosample_metadata_unique_owner_biosample_id partial unique index
    rejects a second is_owner_biosample_id=true row for the same biosample.
    The biosample_metadata_reject_if_link_retired trigger rejects writes
    against retired biosample_to_study links. Both surface as
    asyncpg.PostgresError subclasses.

    global_field_idx is populated by trigger from the source field row;
    the other five value columns belong to sibling functions for those
    value types and are left NULL here so that
    biosample_metadata_exactly_one_value is satisfied.
    """
    # Single INSERT; value_text is the only value column populated.
    return await conn.fetchval(
        "INSERT INTO qiita.biosample_metadata ("
        "    biosample_idx, biosample_study_field_idx,"
        "    value_text, is_owner_biosample_id, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5)"
        " RETURNING idx",
        biosample_idx,
        biosample_study_field_idx,
        value_text,
        is_owner_biosample_id,
        created_by_idx,
    )


async def insert_biosample_metadata_numeric(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    biosample_study_field_idx: int,
    value_numeric: Decimal,
    created_by_idx: int,
) -> int:
    """Insert a numeric-valued biosample_metadata row and return its idx.

    The biosample_metadata_apply_field_contract trigger rejects writes that
    do not match the source field's resolved data_type; this helper expects
    a NUMERIC-typed field. is_owner_biosample_id is text-only by contract
    and is not a parameter here.
    """
    # Single INSERT; value_numeric is the only value column populated.
    return await conn.fetchval(
        "INSERT INTO qiita.biosample_metadata ("
        "    biosample_idx, biosample_study_field_idx,"
        "    value_numeric, created_by_idx"
        ") VALUES ($1, $2, $3, $4)"
        " RETURNING idx",
        biosample_idx,
        biosample_study_field_idx,
        value_numeric,
        created_by_idx,
    )


async def insert_biosample_metadata_date(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    biosample_study_field_idx: int,
    value_date: date,
    created_by_idx: int,
) -> int:
    """Insert a date-valued biosample_metadata row and return its idx.

    The biosample_metadata_apply_field_contract trigger rejects writes that
    do not match the source field's resolved data_type; this helper expects
    a DATE-typed field. is_owner_biosample_id is text-only by contract and
    is not a parameter here.
    """
    # Single INSERT; value_date is the only value column populated.
    return await conn.fetchval(
        "INSERT INTO qiita.biosample_metadata ("
        "    biosample_idx, biosample_study_field_idx,"
        "    value_date, created_by_idx"
        ") VALUES ($1, $2, $3, $4)"
        " RETURNING idx",
        biosample_idx,
        biosample_study_field_idx,
        value_date,
        created_by_idx,
    )


async def insert_typed_metadata_for_biosample(
    conn: asyncpg.Connection,
    *,
    entity_idx: int,
    study_field_idx: int,
    data_type: FieldDataType,
    value: str | Decimal | date,
    created_by_idx: int,
) -> int:
    """Dispatch a typed metadata INSERT on data_type and return the new idx.

    Bound into BIOSAMPLE_METADATA_SPEC.insert_typed_metadata so the cross-
    entity write function in repositories.__init__ can issue the INSERT
    without naming a biosample-specific function. The else branch covers
    FieldDataType members the if/elif chain does not name (BOOLEAN,
    TERMINOLOGY today); it is unreachable in practice because composer
    pre-flights and write_global_metadata_or_diagnose callers screen
    unsupported types before reaching here.
    """
    # Dispatch on the field's data_type; the typed inserter validates value
    # column placement on its own.
    if data_type is FieldDataType.TEXT:
        return await insert_biosample_metadata_text(
            conn,
            biosample_idx=entity_idx,
            biosample_study_field_idx=study_field_idx,
            value_text=value,
            created_by_idx=created_by_idx,
        )
    if data_type is FieldDataType.NUMERIC:
        return await insert_biosample_metadata_numeric(
            conn,
            biosample_idx=entity_idx,
            biosample_study_field_idx=study_field_idx,
            value_numeric=value,
            created_by_idx=created_by_idx,
        )
    if data_type is FieldDataType.DATE:
        return await insert_biosample_metadata_date(
            conn,
            biosample_idx=entity_idx,
            biosample_study_field_idx=study_field_idx,
            value_date=value,
            created_by_idx=created_by_idx,
        )
    raise NotImplementedError(
        f"biosample metadata insert for data_type={data_type} is not yet implemented"
    )


# ---------------------------------------------------------------------------
# EntityMetadataSpec for biosample (consumed by write_global_metadata_or_diagnose)
# ---------------------------------------------------------------------------

BIOSAMPLE_METADATA_SPEC = EntityMetadataSpec(
    entity_kind=SampleEntityKind.BIOSAMPLE,
    metadata_table="qiita.biosample_metadata",
    entity_key_column="biosample_idx",
    study_field_table="qiita.biosample_study_field",
    study_field_idx_column="biosample_study_field_idx",
    global_field_unique_index_name="biosample_metadata_one_value_per_global_field",
    local_unique_per_field_index_name="biosample_metadata_unique_per_field",
    get_or_create_globally_linked_field=get_or_create_globally_linked_biosample_study_field,
    get_or_create_local_field=get_or_create_local_biosample_study_field,
    insert_typed_metadata=insert_typed_metadata_for_biosample,
)
