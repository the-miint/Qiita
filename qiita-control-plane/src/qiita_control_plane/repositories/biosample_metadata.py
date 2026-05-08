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
from decimal import Decimal, InvalidOperation
from typing import NamedTuple

import asyncpg
from qiita_common.models import FieldDataType, Tier

# Closed set of data_types fetch_global_metadata_for_biosample currently
# decodes from biosample_metadata. Boolean and terminology are intentionally
# absent so a future addition is a coordinated extension (read + write paths
# need to learn the new value_* column at the same time).
_GLOBAL_METADATA_VALUE_COLUMN: dict[FieldDataType, str] = {
    FieldDataType.TEXT: "value_text",
    FieldDataType.NUMERIC: "value_numeric",
    FieldDataType.DATE: "value_date",
}

# ---------------------------------------------------------------------------
# Structured exceptions raised by the import path
# ---------------------------------------------------------------------------
# Each carries its own attributes so the route can build a 422 detail body
# without parsing the message string. Subclassing Exception directly (no
# shared base) lets the route catch them individually with distinct mappings.


class BiosampleMetadataUnknownFieldsError(Exception):
    """Raised when import metadata names display_names that have no matching
    biosample_global_field row. Carries every unknown name in one list so
    the caller can surface them all in a single 422.
    """

    def __init__(self, unknown_display_names: list[str]) -> None:
        self.unknown_display_names = unknown_display_names
        super().__init__(f"unknown biosample global field display_names: {unknown_display_names!r}")


class BiosampleMetadataParseError(Exception):
    """Raised when a metadata text value cannot be coerced into the Python
    type that matches its global field's data_type. Carries the failing
    display_name plus the raw inputs so the route can build a field-scoped
    422 detail.
    """

    def __init__(
        self,
        display_name: str,
        data_type: FieldDataType,
        text_value: str,
        reason: str,
    ) -> None:
        self.display_name = display_name
        self.data_type = data_type
        self.text_value = text_value
        self.reason = reason
        super().__init__(
            f"could not parse {display_name!r} value {text_value!r} as {data_type}: {reason}"
        )


class BiosampleStudyFieldConflictError(Exception):
    """Raised by get_or_create_globally_linked_biosample_study_field when a
    biosample_study_field row already exists at (study_idx, display_name)
    that is purely-local (found_global_field_idx is None) or globally
    linked to a different concept than the one the caller requested.
    """

    def __init__(
        self,
        study_idx: int,
        display_name: str,
        expected_global_field_idx: int,
        found_global_field_idx: int | None,
    ) -> None:
        self.study_idx = study_idx
        self.display_name = display_name
        self.expected_global_field_idx = expected_global_field_idx
        self.found_global_field_idx = found_global_field_idx
        super().__init__(
            f"biosample_study_field at study_idx={study_idx},"
            f" display_name={display_name!r} is bound to global"
            f" {found_global_field_idx!r}, expected {expected_global_field_idx!r}"
        )


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


# ---------------------------------------------------------------------------
# Lookup row shape for fetch_biosample_global_fields_by_display_names
# ---------------------------------------------------------------------------


class BiosampleGlobalFieldRow(NamedTuple):
    """Subset of biosample_global_field columns the import path needs."""

    idx: int
    display_name: str
    data_type: FieldDataType


class BiosampleGlobalMetadataRow(NamedTuple):
    """One row from fetch_global_metadata_for_biosample.

    Carries the global field's stable internal_name (which doubles as the
    dict key in the function's return mapping), the cosmetic display_name
    and description (taken from biosample_global_field, not from any
    per-study biosample_study_field override, because biosample reads
    are not study-scoped), the field's data_type, and the typed Python
    value extracted from the matching biosample_metadata.value_* column.
    """

    internal_name: str
    display_name: str
    description: str | None
    data_type: FieldDataType
    value: str | Decimal | date


async def fetch_biosample_global_fields_by_display_names(
    conn: asyncpg.Connection,
    display_names: Iterable[str],
) -> dict[str, BiosampleGlobalFieldRow]:
    """Return a dict of display_name -> BiosampleGlobalFieldRow for the
    matching rows in qiita.biosample_global_field.

    Display names that have no matching row are absent from the returned
    dict; callers detect "unknown field" by checking dict membership for
    each requested name. Empty input short-circuits with no DB call.
    """
    # Materialize so emptiness is detectable and the param can be passed as ANY.
    names = list(display_names)
    if not names:
        return {}

    # Single SELECT keyed on display_name = ANY($1::text[]).
    rows = await conn.fetch(
        "SELECT idx, display_name, data_type"
        " FROM qiita.biosample_global_field"
        " WHERE display_name = ANY($1::text[])",
        names,
    )

    # Wrap each row in the typed tuple, keyed on display_name.
    return {
        r["display_name"]: BiosampleGlobalFieldRow(
            idx=r["idx"],
            display_name=r["display_name"],
            data_type=FieldDataType(r["data_type"]),
        )
        for r in rows
    }


async def fetch_global_metadata_for_biosample(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    biosample_idx: int,
) -> dict[str, BiosampleGlobalMetadataRow]:
    """Return a dict of internal_name -> BiosampleGlobalMetadataRow for every
    globally-linked metadata value the biosample carries.

    Filters on biosample_metadata.global_field_idx IS NOT NULL: purely-local
    rows (including the owner-biosample-id row) and rows whose
    biosample_to_study link has been retired (the
    biosample_to_study_retirement_demote_globals trigger nulls out
    global_field_idx) are both excluded by the same predicate.
    Missing-reason rows (value_missing_reason_idx populated) are also
    excluded -- they have no typed value to surface and the import path
    does not currently write them.

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
    result: dict[str, BiosampleGlobalMetadataRow] = {}
    for r in rows:
        data_type = FieldDataType(r["data_type"])
        column = _GLOBAL_METADATA_VALUE_COLUMN.get(data_type)
        if column is None:
            raise NotImplementedError(
                f"global metadata read for data_type={data_type} is not yet implemented"
            )
        result[r["internal_name"]] = BiosampleGlobalMetadataRow(
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
    biosample_global_field_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
) -> tuple[int, bool]:
    """Find a biosample_study_field linked to biosample_global_field_idx; create on miss.

    Returns (idx, created): created is True when this call inserted the
    row; False when the fallback SELECT branch resolved against a row a
    concurrent caller had already committed (or that pre-existed entirely).

    The created row populates biosample_global_field_idx and leaves
    data_type / required / terminology_idx / tier_override NULL per the
    biosample_study_field_inheritance_consistent CHECK; the global concept
    owns those fields.

    Raises BiosampleStudyFieldConflictError when a row at
    (study_idx, display_name) already exists but is purely-local
    (biosample_global_field_idx IS NULL) or is bound to a different global
    concept. Both cases mean the caller is trying to write metadata against
    a field that is not the global concept they think it is; silently
    returning the existing idx would attach the value to the wrong concept.

    Concurrency: same INSERT ... ON CONFLICT DO NOTHING RETURNING idx +
    fallback SELECT pattern as the local sibling, race-free under
    READ COMMITTED.
    """
    # Create branch — globally-linked row leaves the inherited columns NULL.
    idx = await conn.fetchval(
        "INSERT INTO qiita.biosample_study_field ("
        "    study_idx, biosample_global_field_idx,"
        "    display_name, description, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5)"
        " ON CONFLICT (study_idx, display_name) DO NOTHING"
        " RETURNING idx",
        study_idx,
        biosample_global_field_idx,
        display_name,
        description,
        created_by_idx,
    )
    if idx is not None:
        return idx, True

    # Fallback branch — existing row at (study_idx, display_name). Verify
    # its global link matches what the caller asked for; otherwise the
    # row is bound to a different concept (or none) and reusing it would
    # attach the value to the wrong field.
    row = await conn.fetchrow(
        "SELECT idx, biosample_global_field_idx"
        " FROM qiita.biosample_study_field"
        " WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    if row["biosample_global_field_idx"] != biosample_global_field_idx:
        raise BiosampleStudyFieldConflictError(
            study_idx=study_idx,
            display_name=display_name,
            expected_global_field_idx=biosample_global_field_idx,
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
) -> tuple[int, bool]:
    """Find a biosample_study_field by (study_idx, display_name); create local on miss.

    Returns (idx, created): created is True when this call inserted the row
    on the create branch, False when the fallback SELECT branch resolved
    against a row a concurrent caller had already committed (or that
    pre-existed entirely).

    The lookup branch returns the existing row's idx whether the row is
    currently linked to a biosample_global_field or purely local — a
    downstream metadata write against either kind is well-defined because
    the biosample_metadata_apply_field_contract trigger reads from
    biosample_study_field.biosample_global_field_idx either way.

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
        return idx, True

    # Lookup branch — fallback fires only on conflict; takes a fresh snapshot
    # under READ COMMITTED so it sees the row the concurrent winner committed.
    existing_idx = await conn.fetchval(
        "SELECT idx FROM qiita.biosample_study_field WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    return existing_idx, False


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


def _parse_text_for_data_type(
    display_name: str,
    data_type: FieldDataType,
    text_value: str,
) -> str | Decimal | date:
    """Coerce a text input into the Python type matching data_type.

    Outer whitespace is stripped before parsing. TEXT returns the stripped
    string; NUMERIC returns Decimal; DATE returns datetime.date. BOOLEAN
    and TERMINOLOGY are not yet supported and raise NotImplementedError.

    Conversion failures raise BiosampleMetadataParseError carrying the
    display_name, data_type, raw text, and a friendly reason so the route
    can build a field-scoped 422 message.
    """
    # Normalize once; all parse arms see the stripped value.
    stripped = text_value.strip()
    if data_type is FieldDataType.TEXT:
        return stripped
    if data_type is FieldDataType.NUMERIC:
        try:
            return Decimal(stripped)
        except InvalidOperation as exc:
            raise BiosampleMetadataParseError(
                display_name=display_name,
                data_type=data_type,
                text_value=text_value,
                reason="not a valid decimal number",
            ) from exc
    if data_type is FieldDataType.DATE:
        try:
            return date.fromisoformat(stripped)
        except ValueError as exc:
            raise BiosampleMetadataParseError(
                display_name=display_name,
                data_type=data_type,
                text_value=text_value,
                reason="not a valid ISO date (YYYY-MM-DD)",
            ) from exc
    # Closed-set fallback: BOOLEAN and TERMINOLOGY land here and raise.
    raise NotImplementedError(
        f"text-to-typed parsing for data_type={data_type} is not yet implemented"
    )
