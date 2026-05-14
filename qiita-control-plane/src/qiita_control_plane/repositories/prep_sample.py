"""Repository functions and the sequenced-prep-sample composer for the
qiita.prep_sample hierarchy.

Direct functions cover the supertype row (qiita.prep_sample), its
sequencing-pathway subtype (qiita.sequenced_sample), the per-study link
(qiita.prep_sample_to_study), and the per-sample composer that ties them
together with metadata writes against globally-linked
prep_sample_study_field rows. Metadata-shaped tables live in the sibling
prep_sample_metadata module; the composer here imports the helpers it
needs from there.

Write functions take an asyncpg.Connection as their first positional
argument, never acquire their own connection, and never open their own
top-level transaction; the caller controls transaction scope so multiple
calls compose atomically on one connection. Composers that perform more
than one write guard on conn.is_in_transaction() at entry and raise if
the caller did not wrap the call in a transaction.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import asyncpg
from qiita_common.models import FieldDataType

from . import (
    GlobalFieldRow,
    MetadataUnknownFieldsError,
    SampleEntityKind,
    parse_text_for_data_type,
    require_transaction,
    validate_patch_fields,
)
from .prep_sample_metadata import (
    fetch_prep_sample_global_fields_by_display_names,
    get_or_create_globally_linked_prep_sample_study_field,
    insert_prep_sample_metadata_date,
    insert_prep_sample_metadata_numeric,
    insert_prep_sample_metadata_text,
)

# The single supported processing_kind today. The supertype's
# processing_kind column is plain NOT NULL (not GENERATED ALWAYS), so the
# composer must supply this value explicitly when inserting the
# supertype row. The subtype table pins its own processing_kind column
# via GENERATED ALWAYS AS so no caller passes it there.
_PROCESSING_KIND_SEQUENCED = "sequenced"


async def fetch_sequenced_sample_with_prep_sample(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    sequenced_sample_idx: int,
    *,
    for_update: bool = False,
) -> asyncpg.Record | None:
    """Return the joined sequenced_sample + supertype prep_sample row, or
    None on miss.

    Selects every caller-visible column from both tables in one round
    trip and computes `effective_updated_at` = GREATEST(prep_sample.updated_at,
    sequenced_sample.updated_at) so a single timestamp captures the
    latest write to either side; the route layer feeds it directly to
    etag_for_updated_at. Column list mirrors SequencedSampleResponse
    one-for-one (with idx -> sequenced_sample_idx and ps.idx ->
    prep_sample_idx renamed at the route boundary).

    `for_update=True` appends `FOR UPDATE` to the SELECT so rows from
    both joined tables are locked for the duration of the surrounding
    transaction; concurrent callers serialize on those locks until the
    holder commits or rolls back. Used by PATCH to close the
    lost-update window between the preflight ETag check and the
    UPDATE — a second caller's preflight blocks until the first
    commits, then sees the post-commit `effective_updated_at` and 412s
    on its stale `If-Match`. Pass only inside an open transaction —
    with a pool the implicit single-stmt transaction releases the
    lock immediately and the flag is a no-op.
    """
    # Single-row fetch by sequenced_sample.idx; supertype prep_sample is
    # joined 1:1 via the subtype FK and contributes the owner / retirement
    # / metadata-touch / created-* columns.
    sql = (
        "SELECT ss.idx, ss.prep_sample_idx, ss.sequenced_pool_idx,"
        " ss.sequenced_pool_item_id, ss.ena_experiment_accession,"
        " ss.ena_run_accession, ss.last_submission_at, ss.submission_error,"
        " ps.biosample_idx, ps.owner_idx, ps.prep_protocol_idx,"
        " ps.metadata_checklist_idx, ps.last_metadata_change_at,"
        " ps.created_by_idx, ps.created_at,"
        " GREATEST(ps.updated_at, ss.updated_at) AS effective_updated_at,"
        " ps.retired, ps.retired_by_idx, ps.retired_at, ps.retire_reason"
        " FROM qiita.sequenced_sample ss"
        " JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE ss.idx = $1"
    )
    if for_update:
        sql += " FOR UPDATE"
    return await pool_or_conn.fetchrow(sql, sequenced_sample_idx)


# Columns the PATCH composer is allowed to write on the sequenced_sample
# subtype row. Subtype-only per the v1 design decision: supertype
# prep_sample fields will land via a future PATCH /prep-sample/{idx}
# endpoint, and identity-level columns (sequenced_pool_idx,
# sequenced_pool_item_id) are not editable.
SEQUENCED_SAMPLE_PATCHABLE_COLUMNS: frozenset[str] = frozenset(
    {
        "ena_experiment_accession",
        "ena_run_accession",
        "last_submission_at",
        "submission_error",
    }
)


async def update_sequenced_sample(
    conn: asyncpg.Connection,
    sequenced_sample_idx: int,
    *,
    fields: dict[str, object],
) -> None:
    """Update the named columns on the sequenced_sample subtype row.

    `fields` maps column name -> new value; only the listed keys are
    written, and explicit None sets the column to NULL. Unknown keys
    and an empty dict raise ValueError so misuse fails at the repo
    boundary rather than reaching SQL.

    Returns None on success and raises RuntimeError when the UPDATE
    matches zero rows. The route's FOR UPDATE preflight on the same
    transaction is expected to confirm existence and hold a row lock
    through commit, so the raise is a fail-fast backstop for any
    future caller that bypasses that preflight rather than a silent
    200 with stale joined data. The caller still refetches the
    joined row via fetch_sequenced_sample_with_prep_sample to pick
    up the post-update column set including effective_updated_at;
    a single-table RETURNING cannot produce the GREATEST(supertype,
    subtype) timestamp, so the follow-up SELECT is necessary either
    way.

    Raises asyncpg.UniqueViolationError on ENA-accession collisions
    against either of the two unique constraints on this table.
    The schema trigger sequenced_sample_set_updated_at refreshes
    sequenced_sample.updated_at; the
    sequenced_sample_clear_submission_error_on_new_attempt trigger
    nulls submission_error when last_submission_at changes unless
    the same UPDATE explicitly sets submission_error.
    """
    # Reject misuse at the repo boundary so the SQL builder never sees
    # an empty SET clause or an unknown column name.
    validate_patch_fields(
        fields,
        allowlist=SEQUENCED_SAMPLE_PATCHABLE_COLUMNS,
        repo_name="update_sequenced_sample",
    )

    # Build the parameterized SET clause. Column names come from the
    # allowlist above so f-string interpolation is safe; per-column
    # values are passed as positional asyncpg parameters. Sort keys so
    # the generated SQL is deterministic across calls (helps logs and
    # plan-cache stability).
    columns = sorted(fields.keys())
    set_clause = ", ".join(f"{col} = ${i + 1}" for i, col in enumerate(columns))
    values = [fields[col] for col in columns]
    idx_param = f"${len(columns) + 1}"

    # Single UPDATE with RETURNING idx so a zero-row result fails
    # loudly here instead of silently 200-ing with stale joined data;
    # the route's FOR UPDATE preflight makes the None branch
    # unreachable from the current caller.
    returned_idx = await conn.fetchval(
        f"UPDATE qiita.sequenced_sample SET {set_clause} WHERE idx = {idx_param} RETURNING idx",
        *values,
        sequenced_sample_idx,
    )
    if returned_idx is None:
        raise RuntimeError(
            f"update_sequenced_sample: no sequenced_sample row matched idx={sequenced_sample_idx}"
        )


async def insert_prep_sample(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    owner_idx: int,
    prep_protocol_idx: int,
    processing_kind: str,
    created_by_idx: int,
    metadata_checklist_idx: int | None = None,
) -> int:
    """Insert a row into qiita.prep_sample and return the generated idx.

    Exposes every column the caller may legitimately set on a fresh row:
    the three NOT-NULL FKs (biosample, owner, prep_protocol), the
    processing_kind enum, the optional metadata_checklist link, and the
    creator principal. Subtype-only columns (sequencing-pool linkage, ENA
    accessions, submission tracking) live on the subtype tables and have
    no place in a supertype create call. Retirement and metadata-touch
    timestamps are populated by triggers, defaults, or schema CHECKs.

    Raises asyncpg.PostgresError on FK violation or constraint failure.
    """
    # Single INSERT carrying every supertype caller-settable column.
    return await conn.fetchval(
        "INSERT INTO qiita.prep_sample ("
        "    biosample_idx, owner_idx, prep_protocol_idx, metadata_checklist_idx,"
        "    processing_kind, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5::qiita.processing_kind, $6)"
        " RETURNING idx",
        biosample_idx,
        owner_idx,
        prep_protocol_idx,
        metadata_checklist_idx,
        processing_kind,
        created_by_idx,
    )


async def insert_sequenced_sample(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    sequenced_pool_idx: int,
    sequenced_pool_item_id: str,
    created_by_idx: int,
    ena_experiment_accession: str | None = None,
    ena_run_accession: str | None = None,
) -> int:
    """Insert a row into qiita.sequenced_sample and return the generated idx.

    Does not pass processing_kind: that column is GENERATED ALWAYS AS the
    'sequenced' enum literal on the subtype table; the composite FK to
    qiita.prep_sample (idx, processing_kind) enforces that the supertype
    row was created with processing_kind='sequenced'.

    The sequenced_sample_pool_pair_consistent CHECK requires both
    sequenced_pool_idx and sequenced_pool_item_id to be set together; this
    helper takes both as required arguments so a half-populated pair
    cannot be constructed. submission tracking columns are NULL on a fresh
    row and are not parameters here.

    Raises asyncpg.UniqueViolationError on a collision against the unique
    indexes (per-pool item id, ENA experiment, ENA run); raises
    asyncpg.ForeignKeyViolationError on a bad sequenced_pool_idx.
    """
    # Single INSERT; processing_kind is pinned by the subtype GENERATED
    # column and is intentionally omitted from the column list here.
    return await conn.fetchval(
        "INSERT INTO qiita.sequenced_sample ("
        "    prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id,"
        "    ena_experiment_accession, ena_run_accession, created_by_idx"
        ") VALUES ($1, $2, $3, $4, $5, $6)"
        " RETURNING idx",
        prep_sample_idx,
        sequenced_pool_idx,
        sequenced_pool_item_id,
        ena_experiment_accession,
        ena_run_accession,
        created_by_idx,
    )


async def insert_prep_sample_to_study(
    conn: asyncpg.Connection,
    *,
    prep_sample_idx: int,
    study_idx: int,
    created_by_idx: int,
) -> None:
    """Insert a (prep_sample, study) link row in qiita.prep_sample_to_study.

    The four retirement columns are CHECK-pinned to NULL/false on a fresh
    row, so they have no place in a create call; created_at defaults to
    now().

    The schema trigger prep_sample_to_study_reject_without_biosample_link
    fires before INSERT and raises asyncpg.RaiseError if the underlying
    biosample is not linked (non-retired) to the same study. The composer
    relies on this trigger rather than pre-checking (which would race);
    callers translate the marker substring to a 422 at the route layer.

    Raises asyncpg.UniqueViolationError if the (prep_sample_idx,
    study_idx) pair already exists, asyncpg.ForeignKeyViolationError on
    bad refs.
    """
    # Single INSERT against the (prep_sample_idx, study_idx) PK.
    await conn.execute(
        "INSERT INTO qiita.prep_sample_to_study ("
        "    prep_sample_idx, study_idx, created_by_idx"
        ") VALUES ($1, $2, $3)",
        prep_sample_idx,
        study_idx,
        created_by_idx,
    )


@dataclass(frozen=True)
class SequencedPrepSampleCreateResult:
    """Composite return shape for create_sequenced_prep_sample."""

    prep_sample_idx: int
    sequenced_sample_idx: int
    # display_name -> (study_field_idx, created)
    prep_sample_study_fields: dict[str, tuple[int, bool]]


async def create_sequenced_prep_sample(
    conn: asyncpg.Connection,
    *,
    sequenced_pool_idx: int,
    biosample_idx: int,
    prep_protocol_idx: int,
    owner_idx: int,
    sequenced_pool_item_id: str,
    metadata: dict[str, str],
    study_idxs: list[int],
    caller_idx: int,
    metadata_checklist_idx: int | None = None,
    ena_experiment_accession: str | None = None,
    ena_run_accession: str | None = None,
) -> SequencedPrepSampleCreateResult:
    """Create one sequenced prep sample with its study links and metadata.

    Composer order of operations (the order matters because of trigger
    interactions):

      1. Require an open transaction (fail-fast at the function boundary).
      2. Pre-flight metadata validation: resolve every metadata key
         against prep_sample_global_field in one SELECT, then parse every
         text value into its typed Python value. No DB writes yet; both
         unknown-name and parse-failure cases raise typed exceptions
         before any row is touched.
      3. INSERT qiita.prep_sample with processing_kind='sequenced'
         supplied explicitly (the supertype column is plain NOT NULL,
         not GENERATED ALWAYS).
      4. INSERT qiita.sequenced_sample referencing the new prep_sample
         and the caller-supplied sequenced_pool (the subtype's composite
         FK + the GENERATED processing_kind enforce supertype agreement).
      5. INSERT one qiita.prep_sample_to_study row per study_idx (sorted
         ascending for deterministic error reporting). The
         reject_without_biosample_link trigger fires on every INSERT and
         raises asyncpg.RaiseError if any requested study lacks a
         non-retired biosample_to_study link.
      6. For each metadata entry: get_or_create_globally_linked_prep_sample_study_field
         against the field-owning study, then INSERT one
         prep_sample_metadata row. The reject_if_link_retired trigger
         fires here, which is why step 5 must precede step 6.

    The caller must wrap the call in `async with conn.transaction():`; the
    guard at entry raises RuntimeError otherwise so partial failure cannot
    leave orphan rows.

    Currently study_idxs has length 1. The Pydantic max_length=1 at the
    route boundary enforces this; the composer adds a defensive ValueError
    so a future bypass route also fails loudly. Multi-study assignment
    requires a future schema decision about which study owns the per-
    display_name prep_sample_study_field row (cross-study reads see the
    value through global_field_idx, which is scoped to a single
    field-owning study).
    """
    # Fail-fast guard against caller forgetting to wrap in a transaction.
    require_transaction(conn)

    # Multi-study guard. Pydantic blocks this at the route boundary;
    # the defensive ValueError here covers any caller that bypasses the
    # route.
    if len(study_idxs) != 1:
        raise ValueError(
            f"create_sequenced_prep_sample currently accepts exactly one study_idx;"
            f" got {len(study_idxs)}"
        )

    # Pre-flight: resolve every metadata key against prep_sample_global_field
    # in one query. Unknown names are collected (not first-only) so the
    # caller surfaces every bad name in a single 422.
    global_field_rows = await fetch_prep_sample_global_fields_by_display_names(
        conn, metadata.keys()
    )
    unknown = [name for name in metadata if name not in global_field_rows]
    if unknown:
        raise MetadataUnknownFieldsError(SampleEntityKind.PREP_SAMPLE, unknown)

    # Pre-flight: parse every text value into its typed Python value.
    # Failing here keeps the writes below from running for partial inputs;
    # the surrounding transaction would still roll back, but pre-flight
    # avoids the wasted writes.
    parsed_metadata: list[tuple[GlobalFieldRow, str | Decimal | date]] = []
    for display_name, text_value in metadata.items():
        global_row = global_field_rows[display_name]
        parsed_value = parse_text_for_data_type(display_name, global_row.data_type, text_value)
        parsed_metadata.append((global_row, parsed_value))

    # Step a: create the supertype prep_sample with processing_kind pinned.
    ps_idx = await insert_prep_sample(
        conn,
        biosample_idx=biosample_idx,
        owner_idx=owner_idx,
        prep_protocol_idx=prep_protocol_idx,
        processing_kind=_PROCESSING_KIND_SEQUENCED,
        created_by_idx=caller_idx,
        metadata_checklist_idx=metadata_checklist_idx,
    )

    # Step b: create the sequenced_sample subtype row attached to the
    # caller's pool. The subtype's composite FK to (idx, processing_kind)
    # ensures the supertype row above is reachable only from this subtype.
    ss_idx = await insert_sequenced_sample(
        conn,
        prep_sample_idx=ps_idx,
        sequenced_pool_idx=sequenced_pool_idx,
        sequenced_pool_item_id=sequenced_pool_item_id,
        created_by_idx=caller_idx,
        ena_experiment_accession=ena_experiment_accession,
        ena_run_accession=ena_run_accession,
    )

    # Step c: link the prep_sample to every requested study. Sorted-
    # ascending iteration makes the failing study idx reproducible if the
    # biosample-link trigger fires.
    for study_idx in sorted(study_idxs):
        await insert_prep_sample_to_study(
            conn,
            prep_sample_idx=ps_idx,
            study_idx=study_idx,
            created_by_idx=caller_idx,
        )

    # Step d: write each globally-linked metadata entry. The study field
    # row is upserted on first use; subsequent entries on the same study
    # reuse it. Currently pins the field-owning study to the sole study_idx (see
    # the docstring's multi-study note).
    field_owning_study_idx = study_idxs[0]
    prep_sample_study_fields: dict[str, tuple[int, bool]] = {}
    for global_row, parsed_value in parsed_metadata:
        linked_field_idx, created = await get_or_create_globally_linked_prep_sample_study_field(
            conn,
            study_idx=field_owning_study_idx,
            prep_sample_global_field_idx=global_row.idx,
            display_name=global_row.display_name,
            created_by_idx=caller_idx,
        )
        prep_sample_study_fields[global_row.display_name] = (linked_field_idx, created)

        # Dispatch on the global field's data_type. The else branch covers
        # FieldDataType members the if/elif chain does not name (BOOLEAN,
        # TERMINOLOGY today); it is unreachable in practice because
        # parse_text_for_data_type raises NotImplementedError for those
        # types in the pre-flight parse pass. A future maintainer adding
        # BOOLEAN/TERMINOLOGY support must extend both the parser and this
        # dispatch.
        if global_row.data_type is FieldDataType.TEXT:
            await insert_prep_sample_metadata_text(
                conn,
                prep_sample_idx=ps_idx,
                prep_sample_study_field_idx=linked_field_idx,
                value_text=parsed_value,
                created_by_idx=caller_idx,
            )
        elif global_row.data_type is FieldDataType.NUMERIC:
            await insert_prep_sample_metadata_numeric(
                conn,
                prep_sample_idx=ps_idx,
                prep_sample_study_field_idx=linked_field_idx,
                value_numeric=parsed_value,
                created_by_idx=caller_idx,
            )
        elif global_row.data_type is FieldDataType.DATE:
            await insert_prep_sample_metadata_date(
                conn,
                prep_sample_idx=ps_idx,
                prep_sample_study_field_idx=linked_field_idx,
                value_date=parsed_value,
                created_by_idx=caller_idx,
            )
        else:
            raise NotImplementedError(
                f"prep_sample metadata insert for data_type={global_row.data_type}"
                " is not yet implemented"
            )

    return SequencedPrepSampleCreateResult(
        prep_sample_idx=ps_idx,
        sequenced_sample_idx=ss_idx,
        prep_sample_study_fields=prep_sample_study_fields,
    )
