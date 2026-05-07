"""Repository functions and the import composer for the qiita.biosample tables.

Direct functions cover the core biosample row, its study link
(qiita.biosample, qiita.biosample_to_study), and the bulk-id read over
the link table. Metadata-shaped tables (biosample_global_field,
biosample_study_field, biosample_metadata) live in the sibling
biosample_metadata module; the composer here imports the helpers it
needs from there.

Write functions take an asyncpg.Connection as their first positional
argument, never acquire their own connection, and never open their own
top-level transaction; the caller controls transaction scope so multiple
calls compose atomically on one connection. Composers that perform more
than one write guard on conn.is_in_transaction() at entry and raise if
the caller did not wrap the call in a transaction. Read functions accept
either a pool or a connection so they compose inside an open transaction
or stand alone.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import asyncpg
from qiita_common.models import FieldDataType, Tier

from . import require_transaction
from .biosample_metadata import (
    BiosampleGlobalFieldRow,
    BiosampleMetadataUnknownFieldsError,
    BiosampleOwnerIdFieldCollisionError,
    _parse_text_for_data_type,
    fetch_biosample_global_fields_by_display_names,
    get_or_create_globally_linked_biosample_study_field,
    get_or_create_local_biosample_study_field,
    insert_biosample_metadata_date,
    insert_biosample_metadata_numeric,
    insert_biosample_metadata_text,
)

# Owner display values often contain real names (PII), so the owner-biosample-id
# field is pinned above the study's default tier: even on a public study, only
# study members may read the owner-id metadata. Held as a constant so a future
# policy change (e.g., to Tier.VIEWER) is a one-line edit.
OWNER_BIOSAMPLE_ID_TIER_OVERRIDE: Tier = Tier.MEMBER


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


async def fetch_biosample(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    biosample_idx: int,
) -> asyncpg.Record | None:
    """Return the qiita.biosample row for the given idx, or None on miss.

    Selects every caller-visible column on the row so the route's
    row -> response shaping has a single source of truth (mirrors
    fetch_study). Accepts either a pool or a connection so the helper
    composes inside an open transaction or stands alone.
    """
    # Single-row fetch by idx; column list mirrors BiosampleResponse one-for-one
    # (with idx -> biosample_idx renamed at the route boundary).
    return await pool_or_conn.fetchrow(
        "SELECT idx, owner_idx, metadata_checklist_idx,"
        " biosample_accession, ena_sample_accession,"
        " last_submission_at, submission_error, last_metadata_change_at,"
        " created_by_idx, created_at, updated_at,"
        " retired, retired_by_idx, retired_at, retire_reason"
        " FROM qiita.biosample WHERE idx = $1",
        biosample_idx,
    )


async def fetch_caller_has_biosample_access(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    principal_idx: int,
    biosample_idx: int,
) -> bool:
    """Return True iff the caller has a non-admin read path to the biosample.

    A read path exists when the caller is the biosample's owner OR has
    any qiita.study_access row on a non-retired biosample_to_study link.
    The "any qiita.study_access row" check captures viewer-or-higher
    tier because public-by-absence callers have no row at all (the
    study_access_no_public_tier CHECK rejects 'public' as an
    access_tier value). admin / wet_lab_admin role-bypass is handled
    at the route layer; this helper does not consider system_role.
    """
    # One round trip: short-circuit OR of the owner check and the
    # link-plus-study_access EXISTS subquery.
    return await pool_or_conn.fetchval(
        "SELECT EXISTS ("
        "    SELECT 1 FROM qiita.biosample b"
        "     WHERE b.idx = $2 AND b.owner_idx = $1"
        ") OR EXISTS ("
        "    SELECT 1 FROM qiita.biosample_to_study bts"
        "      JOIN qiita.study_access sa"
        "        ON sa.study_idx = bts.study_idx AND sa.principal_idx = $1"
        "     WHERE bts.biosample_idx = $2 AND bts.retired = false"
        ")",
        principal_idx,
        biosample_idx,
    )


async def fetch_biosample_idxs_for_study(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    study_idx: int,
    limit: int,
) -> list[int]:
    """Return up to `limit` biosample idxs linked to study_idx, newest-linked first.

    Excludes retired links (biosample_to_study.retired = true) and
    retired biosamples (biosample.retired = true). Sort:
    (biosample_to_study.created_at DESC, biosample_idx DESC). Callers
    that need to detect truncation pass `limit = cap + 1`; if the
    returned list has length > cap, the underlying set exceeded the
    cap. Accepts either a pool or a connection so the helper composes
    inside an open transaction or stands alone (mirrors fetch_study).
    """
    # Single round trip; the partial index biosample_to_study_active_idx
    # covers the bts.retired = false predicate and the join to biosample
    # filters out separately-retired biosamples.
    rows = await pool_or_conn.fetch(
        "SELECT bts.biosample_idx"
        " FROM qiita.biosample_to_study bts"
        " JOIN qiita.biosample b ON b.idx = bts.biosample_idx"
        " WHERE bts.study_idx = $1"
        "   AND bts.retired = false"
        "   AND b.retired = false"
        " ORDER BY bts.created_at DESC, bts.biosample_idx DESC"
        " LIMIT $2",
        study_idx,
        limit,
    )
    return [r["biosample_idx"] for r in rows]


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


@dataclass(frozen=True)
class BiosampleImportResult:
    """Composite return shape for import_biosample_from_owner_biosample_id."""

    biosample_idx: int
    biosample_study_field_idx: int
    biosample_study_field_created: bool


async def import_biosample_from_owner_biosample_id(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    owner_idx: int,
    owner_biosample_id_field_name: str,
    owner_biosample_id_value: str,
    caller_idx: int,
    metadata: dict[str, str],
    metadata_checklist_idx: int | None = None,
    biosample_accession: str | None = None,
    ena_sample_accession: str | None = None,
) -> BiosampleImportResult:
    """Import one biosample with its owner-id and any globally-linked metadata.

    Creates the biosample, links it to the study, writes any supplied metadata
    against globally-linked biosample_study_field rows (auto-creating each
    linked field on first use), and writes the owner-biosample-id metadata
    value against a purely-local biosample_study_field flagged
    is_owner_biosample_id=True. Returns a BiosampleImportResult naming the
    new biosample plus the owner-biosample-id field row.

    The metadata dict maps biosample_global_field.display_name to a text
    value; values are parsed into the Python type matching the global
    field's data_type before insert. Pre-flight validation runs before any
    writes:

        - BiosampleOwnerIdFieldCollisionError when metadata carries an
          entry whose key equals owner_biosample_id_field_name (the
          owner-biosample-id row must remain purely-local; the same
          display_name cannot also be a globally-linked metadata entry).
        - BiosampleMetadataUnknownFieldsError when any metadata key has
          no matching biosample_global_field row; all unknown names are
          collected in one error.
        - BiosampleMetadataParseError on first failure to coerce a text
          value into the type its global field declares.

    The caller must wrap the call in `async with conn.transaction():`; the
    guard at entry raises RuntimeError otherwise so partial failure cannot
    leave orphan rows.
    """
    # Fail-fast guard against caller forgetting to wrap in a transaction.
    require_transaction(conn)

    # Pre-flight: pure-logic collision check between the owner-id field
    # name and the metadata dict's keys. The owner-id row is purely-local;
    # a globally-linked entry at the same display_name would violate that.
    if owner_biosample_id_field_name in metadata:
        raise BiosampleOwnerIdFieldCollisionError(owner_biosample_id_field_name)

    # Pre-flight: resolve every metadata key against biosample_global_field
    # in one query. Unknown names are collected (not first-only) so the
    # caller surfaces every bad name in a single 422.
    global_field_rows = await fetch_biosample_global_fields_by_display_names(conn, metadata.keys())
    unknown = [name for name in metadata if name not in global_field_rows]
    if unknown:
        raise BiosampleMetadataUnknownFieldsError(unknown)

    # Pre-flight: parse every text value into its typed Python value.
    # Failing here keeps the writes below from running for partial inputs;
    # the surrounding transaction would still roll back, but pre-flight
    # avoids the wasted writes.
    parsed_metadata: list[tuple[BiosampleGlobalFieldRow, str | Decimal | date]] = []
    for display_name, text_value in metadata.items():
        global_row = global_field_rows[display_name]
        parsed_value = _parse_text_for_data_type(display_name, global_row.data_type, text_value)
        parsed_metadata.append((global_row, parsed_value))

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

    # Step c: write each globally-linked metadata entry. The study field row
    # is upserted on first use; subsequent entries on the same study reuse it.
    for global_row, parsed_value in parsed_metadata:
        linked_field_idx, _ = await get_or_create_globally_linked_biosample_study_field(
            conn,
            study_idx=study_idx,
            biosample_global_field_idx=global_row.idx,
            display_name=global_row.display_name,
            created_by_idx=caller_idx,
        )
        # Dispatch on the global field's data_type. The else branch covers
        # FieldDataType members the if/elif chain does not name (BOOLEAN,
        # TERMINOLOGY today); it is unreachable in practice because
        # _parse_text_for_data_type raises NotImplementedError for those
        # types in the pre-flight parse pass. A future maintainer adding
        # BOOLEAN/TERMINOLOGY support must extend both the parser and this
        # dispatch.
        if global_row.data_type is FieldDataType.TEXT:
            await insert_biosample_metadata_text(
                conn,
                biosample_idx=bs_idx,
                biosample_study_field_idx=linked_field_idx,
                value_text=parsed_value,
                created_by_idx=caller_idx,
            )
        elif global_row.data_type is FieldDataType.NUMERIC:
            await insert_biosample_metadata_numeric(
                conn,
                biosample_idx=bs_idx,
                biosample_study_field_idx=linked_field_idx,
                value_numeric=parsed_value,
                created_by_idx=caller_idx,
            )
        elif global_row.data_type is FieldDataType.DATE:
            await insert_biosample_metadata_date(
                conn,
                biosample_idx=bs_idx,
                biosample_study_field_idx=linked_field_idx,
                value_date=parsed_value,
                created_by_idx=caller_idx,
            )
        else:
            raise NotImplementedError(
                f"metadata insert for data_type={global_row.data_type} is not yet implemented"
            )

    # Step d: find or create the local owner-biosample-id field on this study.
    # The tier_override pins the field above any study-level default so the
    # owner display value never surfaces to non-members (PII concern).
    field_idx, field_created = await get_or_create_local_biosample_study_field(
        conn,
        study_idx=study_idx,
        display_name=owner_biosample_id_field_name,
        created_by_idx=caller_idx,
        required=True,
        tier_override=OWNER_BIOSAMPLE_ID_TIER_OVERRIDE,
    )

    # Step e: write the owner-biosample-id metadata row, flagged.
    await insert_biosample_metadata_text(
        conn,
        biosample_idx=bs_idx,
        biosample_study_field_idx=field_idx,
        value_text=owner_biosample_id_value,
        created_by_idx=caller_idx,
        is_owner_biosample_id=True,
    )

    return BiosampleImportResult(
        biosample_idx=bs_idx,
        biosample_study_field_idx=field_idx,
        biosample_study_field_created=field_created,
    )
