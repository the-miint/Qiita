"""Repository functions and composers for biosample-related tables.

Functions take an asyncpg.Connection as their first positional argument,
never acquire their own connection, and never open their own top-level
transaction; the caller controls transaction scope so multiple calls compose
atomically on one connection. Composers that perform more than one write
guard on conn.is_in_transaction() at entry and raise if the caller did not
wrap the call in a transaction.
"""

import asyncpg
from qiita_common.models import FieldDataType

from . import require_transaction


async def insert_biosample(
    conn: asyncpg.Connection,
    *,
    owner_idx: int,
    created_by_idx: int,
    metadata_checklist_idx: int | None = None,
    biosample_accession: str | None = None,
    ena_sample_accession: str | None = None,
) -> int:
    """Insert a row into qiita.biosample and return the generated idx.

    Exposes every column the caller may legitimately set on a fresh row:
    the two principal references, the optional checklist link, and the two
    external accessions. Submission-tracking, metadata-touch, retirement,
    and audit-timestamp columns are populated by triggers, defaults, or
    schema CHECKs and are not parameters of this function.

    Raises asyncpg.PostgresError on FK violation or constraint failure.
    """
    # Single INSERT carrying all caller-settable columns.
    return await conn.fetchval(
        "INSERT INTO qiita.biosample ("
        "    owner_idx, created_by_idx, metadata_checklist_idx,"
        "    biosample_accession, ena_sample_accession"
        ") VALUES ($1, $2, $3, $4, $5)"
        " RETURNING idx",
        owner_idx,
        created_by_idx,
        metadata_checklist_idx,
        biosample_accession,
        ena_sample_accession,
    )


async def insert_biosample_to_study(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    study_idx: int,
    created_by_idx: int,
) -> None:
    """Insert a (biosample, study) link row in qiita.biosample_to_study.

    The four retirement columns are CHECK-pinned to NULL/false on a fresh
    row so they have no place in a create call; created_at defaults to
    now(). Those are the only other settable columns on the table.

    Raises asyncpg.UniqueViolationError if the (biosample_idx, study_idx)
    pair already exists, asyncpg.ForeignKeyViolationError on bad refs.
    """
    # Single INSERT against the (biosample_idx, study_idx) PK.
    await conn.execute(
        "INSERT INTO qiita.biosample_to_study ("
        "    biosample_idx, study_idx, created_by_idx"
        ") VALUES ($1, $2, $3)",
        biosample_idx,
        study_idx,
        created_by_idx,
    )


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
    tier_override: str | None = None,
) -> int:
    """Find a biosample_study_field by (study_idx, display_name); create local on miss.

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
        return idx

    # Lookup branch — fallback fires only on conflict; takes a fresh snapshot
    # under READ COMMITTED so it sees the row the concurrent winner committed.
    return await conn.fetchval(
        "SELECT idx FROM qiita.biosample_study_field WHERE study_idx = $1 AND display_name = $2",
        study_idx,
        display_name,
    )


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

    The biosample_metadata_one_owner_id_per_biosample partial unique index
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


async def import_biosample_from_owner_id(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    owner_idx: int,
    owner_id_field_name: str,
    owner_id_value: str,
    caller_idx: int,
    metadata_checklist_idx: int | None = None,
    biosample_accession: str | None = None,
    ena_sample_accession: str | None = None,
) -> int:
    """Import one biosample from its owner's identifier-for-it; return its idx.

    Creates the biosample, links it to the study, finds or creates a local
    biosample_study_field with the supplied display_name (data_type='text',
    required=True on auto-create), and writes the owner-id metadata value
    flagged with is_owner_biosample_id=True. The
    biosample_metadata_one_owner_id_per_biosample partial unique index
    enforces at most one such row per biosample.

    The caller must wrap the call in `async with conn.transaction():`; the
    guard at entry raises RuntimeError otherwise so partial failure cannot
    leave orphan rows.
    """
    # Fail-fast guard against caller forgetting to wrap in a transaction.
    require_transaction(conn)

    # Step a: create the biosample.
    bs_idx = await insert_biosample(
        conn,
        owner_idx=owner_idx,
        created_by_idx=caller_idx,
        metadata_checklist_idx=metadata_checklist_idx,
        biosample_accession=biosample_accession,
        ena_sample_accession=ena_sample_accession,
    )

    # Step b: link the biosample to the study.
    await insert_biosample_to_study(
        conn,
        biosample_idx=bs_idx,
        study_idx=study_idx,
        created_by_idx=caller_idx,
    )

    # Step c: find or create the local owner-id field on this study.
    field_idx = await get_or_create_local_biosample_study_field(
        conn,
        study_idx=study_idx,
        display_name=owner_id_field_name,
        created_by_idx=caller_idx,
        required=True,
    )

    # Step d: write the owner-id metadata row, flagged.
    await insert_biosample_metadata_text(
        conn,
        biosample_idx=bs_idx,
        biosample_study_field_idx=field_idx,
        value_text=owner_id_value,
        created_by_idx=caller_idx,
        is_owner_biosample_id=True,
    )

    return bs_idx
