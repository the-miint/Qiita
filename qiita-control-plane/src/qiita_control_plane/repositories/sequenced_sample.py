"""Repository functions and the sequenced-sample import composer for the
qiita.sequenced_sample subtype.

Direct functions cover the sequenced_sample subtype row (the read that
joins the supertype prep_sample for projection, the subtype-only PATCH,
and the subtype insert) plus the run-scoped and study-scoped bulk-id
reads. The composer ties the supertype prep_sample, this subtype, the
per-study links, and the globally-linked metadata writes together for
one sequenced sample; it imports the supertype and junction inserts
from the sibling prep_sample module. Metadata-shaped tables live in the
sibling prep_sample_metadata module.

Write functions take an asyncpg.Connection as their first positional
argument, never acquire their own connection, and never open their own
top-level transaction; the caller controls transaction scope so multiple
calls compose atomically on one connection. Composers that perform more
than one write guard on conn.is_in_transaction() at entry and raise if
the caller did not wrap the call in a transaction. Read functions accept
either a pool or a connection so they compose inside an open transaction
or stand alone.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import asyncpg

from . import require_transaction, validate_patch_fields
from ._sample_helpers import (
    fetch_missing_value_reason_idxs_by_names,
    link_entity_to_studies,
    preflight_global_metadata,
    validate_primary_secondary_studies,
    write_global_metadata_entries,
)
from .prep_sample import insert_prep_sample
from .prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC

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
        " ps.metadata_checklist_idx,"
        " (SELECT name FROM qiita.metadata_checklist mc"
        "  WHERE mc.idx = ps.metadata_checklist_idx) AS metadata_checklist_name,"
        " ps.last_metadata_change_at,"
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


@dataclass(frozen=True)
class SequencedPrepSampleImportResult:
    """Composite return shape for import_sequenced_prep_sample."""

    prep_sample_idx: int
    sequenced_sample_idx: int


async def import_sequenced_prep_sample(
    conn: asyncpg.Connection,
    *,
    sequenced_pool_idx: int,
    biosample_idx: int,
    prep_protocol_idx: int,
    owner_idx: int,
    sequenced_pool_item_id: str,
    metadata: dict[str, str],
    primary_study_idx: int,
    secondary_study_idxs: Sequence[int] = (),
    caller_idx: int,
    metadata_checklist_idx: int | None = None,
    ena_experiment_accession: str | None = None,
    ena_run_accession: str | None = None,
) -> SequencedPrepSampleImportResult:
    """Create one sequenced prep sample with its study links and metadata.

    Composer order of operations (the order matters because of trigger
    interactions):

      1. Require an open transaction (fail-fast at the function boundary).
      2. Pre-flight metadata validation: resolve every metadata key
         against prep_sample_global_field in one SELECT, then parse every
         text value into its typed Python value (or, when the text matches
         a missing_value_reason name, into a MissingReasonRef that lands
         as value_missing_reason_idx; or, on a TERMINOLOGY-typed field,
         into a TerminologyTermRef that lands as
         value_terminology_term_idx). No DB writes yet; unknown-name,
         parse-failure, and unresolved-terminology cases raise typed
         exceptions before any row is touched.
      3. INSERT qiita.prep_sample with processing_kind='sequenced'
         supplied explicitly (the supertype column is plain NOT NULL,
         not GENERATED ALWAYS).
      4. INSERT qiita.sequenced_sample referencing the new prep_sample
         and the caller-supplied sequenced_pool (the subtype's composite
         FK + the GENERATED processing_kind enforce supertype agreement).
      5. INSERT qiita.prep_sample_to_study for primary_study_idx, then
         for each entry in sorted(secondary_study_idxs). Primary first
         so its link row carries the smallest created_at ordering; the
         reject_without_biosample_link trigger fires on every INSERT
         and raises asyncpg.RaiseError if any requested study lacks a
         non-retired biosample_to_study link.
      6. For each metadata entry: call write_global_metadata_or_diagnose
         with PREP_SAMPLE_METADATA_SPEC against primary_study_idx (the
         field-owning study). That upserts a globally-linked
         prep_sample_study_field bound to the global field, then INSERTs
         one prep_sample_metadata row. The reject_if_link_retired trigger
         fires here, which is why step 5 must precede step 6.

    The caller must wrap the call in `async with conn.transaction():`; the
    guard at entry raises RuntimeError otherwise so partial failure cannot
    leave orphan rows.

    primary_study_idx owns the per-display_name prep_sample_study_field
    rows written for metadata; secondary studies share the value through
    the global field slot but do not own the field row. The asymmetry
    is forced by the schema's one-slot-per-(prep_sample, global_field_idx)
    invariant, so exactly one linked study must be designated.
    primary_study_idx must not also appear in secondary_study_idxs; the
    Pydantic validator on SequencedSampleCreateRequest blocks this at
    the route, and the composer raises ValueError as defense-in-depth.
    """
    # Fail-fast guard against caller forgetting to wrap in a transaction.
    require_transaction(conn)

    # Reject primary appearing in the secondary list at the composer boundary;
    # defense-in-depth against callers bypassing the wire-level guard.
    validate_primary_secondary_studies(primary_study_idx, secondary_study_idxs)

    # Pre-flight: resolve every text value that could plausibly be a
    # missing-reason marker in one DB round trip; the prep-sample composer
    # has no owner-id field so the candidate set is just the metadata values.
    # Values are stripped so a padded marker (e.g. " not collected ") resolves.
    candidate_texts = {v.strip() for v in metadata.values()}
    known_missing_reasons = await fetch_missing_value_reason_idxs_by_names(conn, candidate_texts)

    # Pre-flight: type-resolve every metadata entry against
    # prep_sample_global_field; unknown-name, parse-failure, and
    # unresolved-terminology cases raise before any DB write.
    parsed_metadata = await preflight_global_metadata(
        conn,
        spec=PREP_SAMPLE_METADATA_SPEC,
        metadata=metadata,
        known_missing_reasons=known_missing_reasons,
    )

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

    # Step c: link the prep_sample to every requested study (dedup, sort,
    # primary first). The reject_without_biosample_link trigger fires per
    # row inside the shared helper.
    await link_entity_to_studies(
        conn,
        spec=PREP_SAMPLE_METADATA_SPEC,
        entity_idx=ps_idx,
        primary_study_idx=primary_study_idx,
        secondary_study_idxs=secondary_study_idxs,
        caller_idx=caller_idx,
    )

    # Step d: write each globally-linked metadata entry against
    # primary_study_idx, the field-owning study.
    await write_global_metadata_entries(
        conn,
        spec=PREP_SAMPLE_METADATA_SPEC,
        entity_idx=ps_idx,
        study_idx=primary_study_idx,
        caller_idx=caller_idx,
        parsed_metadata=parsed_metadata,
    )

    return SequencedPrepSampleImportResult(
        prep_sample_idx=ps_idx,
        sequenced_sample_idx=ss_idx,
    )


async def fetch_sequenced_sample_idxs_for_run(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    sequencing_run_idx: int,
    limit: int,
) -> list[int]:
    """Return up to `limit` sequenced_sample idxs reachable from the run.

    Walks the run -> sequenced_pool -> sequenced_sample -> prep_sample
    chain and excludes sequenced_samples whose supertype prep_sample row
    is retired. Sort: (sequenced_sample.created_at DESC, idx DESC) so
    newer rows surface first. Callers that need to detect truncation pass
    `limit = cap + 1`; if the returned list has length > cap, the
    underlying set exceeded the cap.
    """
    # Single round trip; the partial index prep_sample_active_idx covers
    # the retired = false predicate and the join filters down to one run.
    rows = await pool_or_conn.fetch(
        "SELECT ss.idx"
        " FROM qiita.sequenced_sample ss"
        " JOIN qiita.sequenced_pool sp ON sp.idx = ss.sequenced_pool_idx"
        " JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE sp.sequencing_run_idx = $1"
        "   AND ps.retired = false"
        " ORDER BY ss.created_at DESC, ss.idx DESC"
        " LIMIT $2",
        sequencing_run_idx,
        limit,
    )
    return [r["idx"] for r in rows]


async def fetch_sequenced_pool_samples(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    sequenced_pool_idx: int,
    limit: int,
) -> list[asyncpg.Record]:
    """Return up to `limit` active sequenced_samples in one pool, each with
    its supertype prep_sample_idx and sequenced_pool_item_id.

    Pool-scoped sibling of fetch_sequenced_sample_idxs_for_run: that one
    spans every pool in a run and returns bare idxs; this one is scoped to a
    single sequenced_pool and returns the
    (idx, prep_sample_idx, sequenced_pool_item_id) triple a per-sample
    fan-out needs (e.g. submit-host-filter-pool). Excludes sequenced_samples
    whose supertype prep_sample row is retired. Sort by
    sequenced_pool_item_id so the fan-out order is stable across calls.
    Callers that need to detect truncation pass `limit = cap + 1`; if the
    returned list has length > cap, the underlying set exceeded the cap.
    """
    # Single round trip; the partial index prep_sample_active_idx covers the
    # retired = false predicate and the join filters down to one pool.
    rows = await pool_or_conn.fetch(
        # Alias ss.idx so a row maps straight onto SequencedSampleListItem via
        # model_validate(dict(row)) — the route does no field renaming.
        "SELECT ss.idx AS sequenced_sample_idx, ss.prep_sample_idx,"
        " ss.sequenced_pool_item_id"
        " FROM qiita.sequenced_sample ss"
        " JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
        " WHERE ss.sequenced_pool_idx = $1"
        "   AND ps.retired = false"
        " ORDER BY ss.sequenced_pool_item_id"
        " LIMIT $2",
        sequenced_pool_idx,
        limit,
    )
    return list(rows)


async def fetch_sequenced_sample_idxs_for_study(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    study_idx: int,
    limit: int,
) -> list[int]:
    """Return up to `limit` sequenced_sample idxs linked to study_idx, newest-linked first.

    Walks the prep_sample_to_study link to its supertype prep_sample and
    down to the sequenced_sample subtype, so only prep_samples that carry
    a sequenced_sample row surface. Excludes retired links
    (prep_sample_to_study.retired = true) and retired prep_samples
    (prep_sample.retired = true); the sequenced_sample subtype has no
    own retirement surface. Sort: (prep_sample_to_study.created_at DESC,
    sequenced_sample.idx DESC) so newest-linked rows surface first with a
    deterministic tiebreak. Callers that need to detect truncation pass
    `limit = cap + 1`; if the returned list has length > cap, the
    underlying set exceeded the cap. Accepts either a pool or a
    connection so the helper composes inside an open transaction or
    stands alone (mirrors fetch_biosample_idxs_for_study).
    """
    # Single round trip; the partial index prep_sample_to_study_active_idx
    # covers the pts.retired = false predicate and the join to prep_sample
    # filters out separately-retired prep_samples. Joining sequenced_sample
    # restricts the roster to prep_samples that carry a sequenced subtype.
    rows = await pool_or_conn.fetch(
        "SELECT ss.idx"
        " FROM qiita.prep_sample_to_study pts"
        " JOIN qiita.sequenced_sample ss ON ss.prep_sample_idx = pts.prep_sample_idx"
        " JOIN qiita.prep_sample ps ON ps.idx = pts.prep_sample_idx"
        " WHERE pts.study_idx = $1"
        "   AND pts.retired = false"
        "   AND ps.retired = false"
        " ORDER BY pts.created_at DESC, ss.idx DESC"
        " LIMIT $2",
        study_idx,
        limit,
    )
    return [r["idx"] for r in rows]
