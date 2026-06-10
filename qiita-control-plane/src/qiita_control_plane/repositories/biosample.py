"""Repository functions and the import composer for qiita.biosample.

Write functions take an asyncpg.Connection as their first positional
argument, never acquire their own connection, and never open their
own top-level transaction; the caller controls transaction scope.
Composers that perform more than one write guard on
conn.is_in_transaction() at entry. Read functions accept either a
pool or a connection so they compose inside an open transaction or
stand alone.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, get_args

import asyncpg
from qiita_common.models import Tier

from . import require_transaction, update_row
from ._sample_helpers import (
    LocalWriteOnGloballyLinkedFieldError,
    SampleEntityKind,
    _get_or_create_local_study_field,
    fetch_missing_value_reason_idxs_by_names,
    link_entity_to_studies,
    preflight_global_metadata,
    validate_primary_secondary_studies,
    write_global_metadata_entries,
)
from .biosample_metadata import (
    BIOSAMPLE_METADATA_SPEC,
    BiosampleOwnerIdFieldCollisionError,
    BiosampleOwnerIdMissingValueError,
    insert_owner_biosample_id_metadata,
)

# Owner display values often contain real names (PII), so the
# owner-biosample-id field is pinned above the study's default tier:
# even on a public study, only study members may read the owner-id
# metadata.
OWNER_BIOSAMPLE_ID_TIER_OVERRIDE: Tier = Tier.MEMBER


async def insert_biosample(
    conn: asyncpg.Connection,
    *,
    owner_idx: int,
    created_by_idx: int,
    metadata_checklist_idx: int | None = None,
    biosample_accession: str | None = None,
    ena_sample_accession: str | None = None,
    matrix_tube_id: str | None = None,
) -> int:
    """Insert a row into qiita.biosample and return the generated idx.

    Exposes every column the caller may legitimately set on a fresh
    row: the two principal references (required) plus the optional
    checklist link, two external accessions, and matrix_tube_id.

    Raises asyncpg.PostgresError on FK violation or constraint failure.
    """
    return await conn.fetchval(
        "INSERT INTO qiita.biosample ("
        "    owner_idx, created_by_idx, metadata_checklist_idx,"
        "    biosample_accession, ena_sample_accession, matrix_tube_id"
        ") VALUES ($1, $2, $3, $4, $5, $6)"
        " RETURNING idx",
        owner_idx,
        created_by_idx,
        metadata_checklist_idx,
        biosample_accession,
        ena_sample_accession,
        matrix_tube_id,
    )


# Columns returned by both fetch_biosample and update_biosample so the
# row -> response shaping has a single source of truth.
_BIOSAMPLE_RETURNING_COLS = (
    "idx, owner_idx, metadata_checklist_idx,"
    " (SELECT name FROM qiita.metadata_checklist mc"
    "  WHERE mc.idx = biosample.metadata_checklist_idx) AS metadata_checklist_name,"
    " biosample_accession, ena_sample_accession, matrix_tube_id,"
    " last_submission_at, submission_error, last_metadata_change_at,"
    " created_by_idx, created_at, updated_at,"
    " retired, retired_by_idx, retired_at, retire_reason"
)


async def fetch_biosample(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    biosample_idx: int,
    *,
    for_update: bool = False,
) -> asyncpg.Record | None:
    """Return the qiita.biosample row for the given idx, or None on miss.

    Selects every caller-visible column on the row so the
    row -> response shaping has a single source of truth. Accepts
    either a pool or a connection so the helper composes inside an
    open transaction or stands alone.

    `for_update=True` appends `FOR UPDATE`; concurrent callers
    serialize on the row lock until the holder commits or rolls back.
    Pass only inside an open transaction — with a pool the implicit
    single-statement transaction releases the lock immediately and
    the flag is a no-op.
    """
    sql = f"SELECT {_BIOSAMPLE_RETURNING_COLS} FROM qiita.biosample WHERE idx = $1"
    if for_update:
        sql += " FOR UPDATE"
    return await pool_or_conn.fetchrow(sql, biosample_idx)


# Columns this repo's PATCH composer is allowed to write. Held as a
# frozenset so unknown column names are rejected at the repo boundary
# rather than reaching the SQL builder.
BIOSAMPLE_PATCHABLE_COLUMNS: frozenset[str] = frozenset(
    {
        "metadata_checklist_idx",
        "owner_idx",
        "biosample_accession",
        "ena_sample_accession",
        "matrix_tube_id",
        "last_submission_at",
        "submission_error",
    }
)


async def update_biosample(
    conn: asyncpg.Connection,
    biosample_idx: int,
    *,
    fields: dict[str, object],
) -> asyncpg.Record | None:
    """Update the named columns on the biosample row, return the post-UPDATE row.

    Thin wrapper around the shared update_row composer that pins the
    table, allowlist, and RETURNING shape for qiita.biosample. See
    update_row for the field-validation and explicit-null semantics;
    see fetch_biosample for the returned column list.
    """
    return await update_row(
        conn,
        table="biosample",
        row_idx=biosample_idx,
        fields=fields,
        allowlist=BIOSAMPLE_PATCHABLE_COLUMNS,
        returning_cols=_BIOSAMPLE_RETURNING_COLS,
        repo_name="update_biosample",
    )


async def fetch_caller_has_biosample_access(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    principal_idx: int,
    biosample_idx: int,
) -> bool:
    """Return True iff the caller has a non-admin read path to the biosample.

    A read path exists when the caller is the biosample's owner OR
    has any qiita.study_access row on a non-retired
    biosample_to_study link. The "any qiita.study_access row" check
    captures viewer-or-higher tier because public-by-absence callers
    have no row at all. Role-based bypass is out of scope here.
    """
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

    Excludes retired links and retired biosamples. Callers that need
    to detect truncation pass `limit = cap + 1`; if the returned list
    has length > cap, the underlying set exceeded the cap. Accepts
    either a pool or a connection so the helper composes inside an
    open transaction or stands alone.
    """
    # The retired-link predicate is index-covered; the join filters
    # out separately-retired biosamples.
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


BiosampleLookupKey = Literal["biosample_accession", "matrix_tube_id"]


async def fetch_biosample_idxs_by_natural_key(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    key: BiosampleLookupKey,
    values: list[str],
) -> dict[str, int]:
    """Return `{value: biosample_idx}` for every value in `values` that
    resolves to a non-retired qiita.biosample row via the named natural-key
    column.

    Values absent from the table or carried only by retired rows are
    omitted from the returned map, so a caller can detect misses by
    set-difference against the input `values`.
    """
    if key not in get_args(BiosampleLookupKey):
        raise ValueError(f"invalid biosample lookup key: {key!r}")
    if not values:
        return {}
    # Column name is interpolated because Postgres can't parameter-bind
    # identifiers; the guard above pins it to the BiosampleLookupKey set.
    rows = await pool_or_conn.fetch(
        f"SELECT idx, {key} FROM qiita.biosample"
        f" WHERE {key} = ANY($1::text[])"
        "   AND retired = false",
        values,
    )
    return {r[key]: r["idx"] for r in rows}


@dataclass(frozen=True)
class BiosampleImportResult:
    """Result of importing one biosample with its owner-id field.

    owner_id_biosample_study_field_* name the biosample_study_field
    row that holds the owner-biosample-id for this study — the
    purely-local, PII-tier-pinned field flagged
    is_owner_biosample_id=True on the associated biosample_metadata
    row.
    """

    biosample_idx: int
    owner_id_biosample_study_field_idx: int
    owner_id_biosample_study_field_created: bool


async def import_biosample_from_owner_biosample_id(
    conn: asyncpg.Connection,
    *,
    primary_study_idx: int,
    secondary_study_idxs: Sequence[int] = (),
    owner_idx: int,
    owner_biosample_id_field_name: str,
    owner_biosample_id_value: str,
    caller_idx: int,
    metadata: dict[str, str],
    metadata_checklist_idx: int | None = None,
    biosample_accession: str | None = None,
    ena_sample_accession: str | None = None,
    matrix_tube_id: str | None = None,
) -> BiosampleImportResult:
    """Import one biosample with its owner-id and any globally-linked metadata.

    Creates the biosample, links it to primary_study_idx plus every
    entry in secondary_study_idxs, writes any supplied metadata
    against globally-linked biosample_study_field rows on
    primary_study_idx (auto-creating each linked field on first use),
    and writes the owner-biosample-id value against a purely-local
    biosample_study_field on primary_study_idx flagged
    is_owner_biosample_id=True. Returns a BiosampleImportResult
    naming the new biosample plus the owner-biosample-id field row.

    primary_study_idx owns the globally-linked field rows and the
    owner-biosample-id local field row; secondary studies share the
    value through the global field slot but do not own the field
    row. The asymmetry mirrors import_sequenced_prep_sample so the
    composers stay parallel. The current POST
    /api/v1/study/{study_idx}/biosample route only ever passes the
    path study as primary with no secondaries: biosamples are created
    *for a study* so they naturally start with one link, whereas
    prep_samples are created *for a sequencing run* which is not
    scoped to a single study. The multi-study path is kept available
    on the composer so a future route or admin tool can exercise it
    without reshaping this function.

    primary_study_idx must not also appear in secondary_study_idxs;
    ValueError otherwise.

    The metadata dict maps biosample_global_field.display_name to a
    text value; values are parsed into the Python type matching the
    global field's data_type before insert. A text value matching a
    qiita.missing_value_reason name is recorded as
    value_missing_reason_idx rather than typed-parsed; a text value on a
    TERMINOLOGY-typed field that matches a qiita.terminology_term row
    scoped to the field's terminology_idx is recorded as
    value_terminology_term_idx. Pre-flight validation runs before any
    writes:

        - BiosampleOwnerIdFieldCollisionError when metadata carries an
          entry whose key equals owner_biosample_id_field_name.
        - BiosampleOwnerIdMissingValueError when
          owner_biosample_id_value matches a missing_value_reason name.
        - MetadataUnknownFieldsError when any metadata key has no
          matching biosample_global_field row; all unknown names are
          collected in one error.
        - MetadataParseError on first failure to coerce a non-marker
          text value into the type its global field declares, or on
          first TERMINOLOGY-typed text that does not resolve to a term
          in the field's terminology.
        - LocalWriteOnGloballyLinkedFieldError when
          owner_biosample_id_field_name resolves to a field on
          primary_study_idx that is already globally linked.

    Caller must wrap the call in `async with conn.transaction():`;
    RuntimeError otherwise so partial failure cannot leave orphan
    rows.
    """
    # Fail-fast guard against caller forgetting to wrap in a transaction.
    require_transaction(conn)

    # Reject primary appearing in the secondary list at the composer boundary.
    validate_primary_secondary_studies(primary_study_idx, secondary_study_idxs)

    # Pre-flight: pure-logic collision check between the owner-id field
    # name and the metadata dict's keys. The owner-id row is purely-local;
    # a globally-linked entry at the same display_name would violate that.
    if owner_biosample_id_field_name in metadata:
        raise BiosampleOwnerIdFieldCollisionError(owner_biosample_id_field_name)

    # Pre-flight: resolve every text value that could plausibly be a
    # missing-reason marker in one DB round trip, including the owner-id
    # text. Values are stripped so a padded marker (e.g. " not collected ")
    # still resolves; the set covers every value the composer will inspect.
    stripped_owner_id = owner_biosample_id_value.strip()
    candidate_texts = {v.strip() for v in metadata.values()} | {stripped_owner_id}
    known_missing_reasons = await fetch_missing_value_reason_idxs_by_names(conn, candidate_texts)

    # Reject owner-id marker before any DB write: the owner-id row carries
    # an identifier (PII); a missing-value marker is incompatible with that
    # contract.
    if stripped_owner_id in known_missing_reasons:
        raise BiosampleOwnerIdMissingValueError(
            owner_biosample_id_value, known_missing_reasons[stripped_owner_id]
        )

    # Pre-flight: type-resolve every metadata entry against
    # biosample_global_field; unknown-name, parse-failure, and
    # unresolved-terminology cases raise before any DB write.
    parsed_metadata = await preflight_global_metadata(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        metadata=metadata,
        known_missing_reasons=known_missing_reasons,
    )

    bs_idx = await insert_biosample(
        conn,
        owner_idx=owner_idx,
        created_by_idx=caller_idx,
        metadata_checklist_idx=metadata_checklist_idx,
        biosample_accession=biosample_accession,
        ena_sample_accession=ena_sample_accession,
        matrix_tube_id=matrix_tube_id,
    )

    await link_entity_to_studies(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        entity_idx=bs_idx,
        primary_study_idx=primary_study_idx,
        secondary_study_idxs=secondary_study_idxs,
        caller_idx=caller_idx,
    )

    await write_global_metadata_entries(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        entity_idx=bs_idx,
        study_idx=primary_study_idx,
        caller_idx=caller_idx,
        parsed_metadata=parsed_metadata,
    )

    # tier_override pins the field above any study-level default so
    # the owner display value never surfaces to non-members (PII).
    (
        field_idx,
        field_created,
        resolved_global_field_idx,
    ) = await _get_or_create_local_study_field(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        study_idx=primary_study_idx,
        display_name=owner_biosample_id_field_name,
        created_by_idx=caller_idx,
        required=True,
        tier_override=OWNER_BIOSAMPLE_ID_TIER_OVERRIDE,
    )
    # The owner-biosample-id row is purely-local PII. If get-or-create
    # resolved an already globally-linked field at this
    # (study, display_name), refuse rather than write the value through
    # a cross-study global slot.
    if resolved_global_field_idx is not None:
        raise LocalWriteOnGloballyLinkedFieldError(
            entity_kind=SampleEntityKind.BIOSAMPLE,
            study_idx=primary_study_idx,
            display_name=owner_biosample_id_field_name,
            study_field_idx=field_idx,
            found_global_field_idx=resolved_global_field_idx,
        )

    await insert_owner_biosample_id_metadata(
        conn,
        biosample_idx=bs_idx,
        biosample_study_field_idx=field_idx,
        value_text=owner_biosample_id_value,
        created_by_idx=caller_idx,
    )

    return BiosampleImportResult(
        biosample_idx=bs_idx,
        owner_id_biosample_study_field_idx=field_idx,
        owner_id_biosample_study_field_created=field_created,
    )
