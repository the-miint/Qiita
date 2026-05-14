"""Repository functions for prep-sample metadata tables.

Mirrors the qiita.prep_sample_metadata / prep_sample_study_field /
prep_sample_global_field DB layout. Functions take an asyncpg.Connection
as their first positional argument, never acquire their own connection,
and never open their own top-level transaction; the caller controls
transaction scope so multiple writes compose atomically on one connection.

The sequenced-prep-sample composer in repositories.prep_sample re-uses the
helpers here; routes that surface metadata-shaped errors import the
exception classes from this module. Parallel to the biosample_metadata
module — owner-biosample-id-shaped features (collision check, local-field
upsert, is_owner_biosample_id flag) have no analogue on prep_sample and
are intentionally absent. GlobalFieldRow and MetadataParseError are
shared with biosample_metadata and live in repositories.__init__.
"""

from collections.abc import Iterable
from datetime import date
from decimal import Decimal

import asyncpg
from qiita_common.models import FieldDataType

from . import (
    GLOBAL_METADATA_VALUE_COLUMN,
    GlobalFieldRow,
    GlobalMetadataRow,
    SampleEntityKind,
    StudyFieldConflictError,
    require_transaction,
)

# Structured exceptions raised by the composer path live in
# repositories.__init__ (MetadataParseError, MetadataUnknownFieldsError,
# StudyFieldConflictError) and take entity_kind at the raise site.
# prep_sample has no owner-id-collision analog.


async def fetch_global_metadata_for_prep_sample(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    prep_sample_idx: int,
) -> dict[str, GlobalMetadataRow]:
    """Return a dict of internal_name -> GlobalMetadataRow for every
    globally-linked metadata value the prep_sample carries.

    Filters on prep_sample_metadata.global_field_idx IS NOT NULL: purely-local
    rows and rows whose prep_sample_to_study link has been retired (the
    prep_sample_to_study_retirement_demote_globals trigger nulls out
    global_field_idx) are both excluded by the same predicate.
    Missing-reason rows are also excluded.

    Currently supports data_type in {TEXT, NUMERIC, DATE} (matching the
    write path's closed set); rows of other data_types raise
    NotImplementedError so a future addition is a coordinated extension
    of read + write paths.
    """
    # Pull every globally-linked, non-missing-reason row for the prep_sample
    # in one round trip; carry every typed value column the closed set covers.
    rows = await pool_or_conn.fetch(
        "SELECT pgf.internal_name, pgf.display_name, pgf.description, pgf.data_type,"
        " psm.value_text, psm.value_numeric, psm.value_date"
        " FROM qiita.prep_sample_metadata psm"
        " JOIN qiita.prep_sample_global_field pgf ON pgf.idx = psm.global_field_idx"
        " WHERE psm.prep_sample_idx = $1"
        "   AND psm.global_field_idx IS NOT NULL"
        "   AND psm.value_missing_reason_idx IS NULL",
        prep_sample_idx,
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


async def fetch_prep_sample_global_fields_by_display_names(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    display_names: Iterable[str],
) -> dict[str, GlobalFieldRow]:
    """Return a dict of display_name -> GlobalFieldRow for the matching
    rows in qiita.prep_sample_global_field.

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
        " FROM qiita.prep_sample_global_field"
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


async def get_or_create_globally_linked_prep_sample_study_field(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    prep_sample_global_field_idx: int,
    display_name: str,
    created_by_idx: int,
    description: str | None = None,
) -> tuple[int, bool]:
    """Find a prep_sample_study_field linked to prep_sample_global_field_idx;
    create on miss.

    Returns (idx, created): created is True when this call inserted the
    row; False when the fallback SELECT branch resolved against a row a
    concurrent caller had already committed (or that pre-existed entirely).

    The created row populates prep_sample_global_field_idx and leaves
    data_type / required / terminology_idx / tier_override NULL per the
    prep_sample_study_field_inheritance_consistent CHECK; the global concept
    owns those fields.

    Raises StudyFieldConflictError(entity_kind=SampleEntityKind.PREP_SAMPLE, ...) when a
    row at (study_idx, display_name) already exists but is purely-local
    (prep_sample_global_field_idx IS NULL) or is bound to a different
    global concept. Both cases mean the caller is trying to write metadata
    against a field that is not the global concept they think it is;
    silently returning the existing idx would attach the value to the
    wrong concept.

    Concurrency: INSERT ... ON CONFLICT DO NOTHING RETURNING idx + a
    fallback SELECT on miss. Race-free under READ COMMITTED (the project
    default, set in qiita_control_plane.db).
    """
    # Both branches must observe the same snapshot; require a wrapping
    # transaction so the INSERT and the fallback SELECT cannot straddle
    # an implicit-commit boundary.
    require_transaction(conn)

    # Create branch — globally-linked row leaves the inherited columns NULL.
    idx = await conn.fetchval(
        "INSERT INTO qiita.prep_sample_study_field ("
        "    study_idx, prep_sample_global_field_idx,"
        "    display_name, description, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5)"
        " ON CONFLICT (study_idx, display_name) DO NOTHING"
        " RETURNING idx",
        study_idx,
        prep_sample_global_field_idx,
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
        "SELECT idx, prep_sample_global_field_idx"
        " FROM qiita.prep_sample_study_field"
        " WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )
    if row["prep_sample_global_field_idx"] != prep_sample_global_field_idx:
        raise StudyFieldConflictError(
            entity_kind=SampleEntityKind.PREP_SAMPLE,
            study_idx=study_idx,
            display_name=display_name,
            expected_global_field_idx=prep_sample_global_field_idx,
            found_global_field_idx=row["prep_sample_global_field_idx"],
        )
    return row["idx"], False


async def insert_prep_sample_metadata_text(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    prep_sample_study_field_idx: int,
    value_text: str,
    created_by_idx: int,
) -> int:
    """Insert a text-valued prep_sample_metadata row and return its idx.

    The prep_sample_metadata_apply_field_contract trigger rejects writes
    whose value column does not match the source field's resolved
    data_type; this helper expects a TEXT-typed field. global_field_idx
    is populated by the same trigger from the source field row, so it is
    not a parameter here.

    The prep_sample_metadata_reject_if_link_retired trigger rejects writes
    against retired prep_sample_to_study links; surfaces as
    asyncpg.RaiseError. The unique-per-field and one-value-per-global-
    concept indexes surface as asyncpg.UniqueViolationError.
    """
    # Single INSERT; value_text is the only value column populated. The
    # other five value columns belong to sibling functions for those
    # value types and stay NULL here so prep_sample_metadata_exactly_one_value
    # is satisfied.
    return await conn.fetchval(
        "INSERT INTO qiita.prep_sample_metadata ("
        "    prep_sample_idx, prep_sample_study_field_idx,"
        "    value_text, created_by_idx"
        ") VALUES ($1, $2, $3, $4)"
        " RETURNING idx",
        prep_sample_idx,
        prep_sample_study_field_idx,
        value_text,
        created_by_idx,
    )


async def insert_prep_sample_metadata_numeric(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    prep_sample_study_field_idx: int,
    value_numeric: Decimal,
    created_by_idx: int,
) -> int:
    """Insert a numeric-valued prep_sample_metadata row and return its idx.

    The prep_sample_metadata_apply_field_contract trigger rejects writes
    whose value column does not match the source field's resolved
    data_type; this helper expects a NUMERIC-typed field.
    """
    # Single INSERT; value_numeric is the only value column populated.
    return await conn.fetchval(
        "INSERT INTO qiita.prep_sample_metadata ("
        "    prep_sample_idx, prep_sample_study_field_idx,"
        "    value_numeric, created_by_idx"
        ") VALUES ($1, $2, $3, $4)"
        " RETURNING idx",
        prep_sample_idx,
        prep_sample_study_field_idx,
        value_numeric,
        created_by_idx,
    )


async def insert_prep_sample_metadata_date(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    prep_sample_study_field_idx: int,
    value_date: date,
    created_by_idx: int,
) -> int:
    """Insert a date-valued prep_sample_metadata row and return its idx.

    The prep_sample_metadata_apply_field_contract trigger rejects writes
    whose value column does not match the source field's resolved
    data_type; this helper expects a DATE-typed field.
    """
    # Single INSERT; value_date is the only value column populated.
    return await conn.fetchval(
        "INSERT INTO qiita.prep_sample_metadata ("
        "    prep_sample_idx, prep_sample_study_field_idx,"
        "    value_date, created_by_idx"
        ") VALUES ($1, $2, $3, $4)"
        " RETURNING idx",
        prep_sample_idx,
        prep_sample_study_field_idx,
        value_date,
        created_by_idx,
    )
