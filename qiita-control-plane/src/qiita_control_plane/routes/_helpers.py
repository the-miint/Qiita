"""Cross-route helpers shared by sibling route modules.

Centralizing them keeps response wording consistent across parallel
endpoints — same input shape, same on-the-wire output.
"""

from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import datetime
from typing import NoReturn

import asyncpg
from fastapi import HTTPException
from qiita_common.models import (
    GLOBAL_FIELD_IDX_ATTR,
    STUDY_FIELD_IDX_ATTR,
    IdxsListResponse,
    MetadataEntry,
    MetadataFieldWriteResult,
    MissingReasonRef,
    SampleGlobalFieldResponse,
    SampleMetadataWriteResponse,
    SampleStudyFieldCreateRequest,
    SampleStudyFieldResponse,
    TerminologyTermRef,
    Tier,
    field_wire_name,
)

from ..auth.guards import (
    COHORT_MIN_TIER,
    PrepSampleReadAccess,
    filter_prep_samples_caller_can_read,
)
from ..auth.principal import Principal
from ..repositories._sample_helpers import (
    ConflictingValueDifferentStudyError,
    ConflictingValueSameStudyError,
    DuplicateGlobalFieldTargetError,
    DuplicateValueDifferentStudyError,
    DuplicateValueSameStudyError,
    EntityMetadataSpec,
    MetadataChecklistUnknownError,
    MetadataParseError,
    MetadataRow,
    MetadataUnknownFieldsError,
    OwnerSampleIdMetadataWriteError,
    SlotOccupiedByMissingReasonError,
    SlotOccupiedByTypedValueError,
    SlotOccupiedError,
    StudyFieldAlreadyExistsError,
    StudyFieldConflictError,
    TransientWriteRaceError,
    create_study_field_and_read_back,
    fetch_entity_is_linked_to_study,
    fetch_global_metadata,
    fetch_local_metadata,
    fetch_metadata_checklist_idx_by_name,
    write_sample_metadata,
)
from ..repositories.alignment_definition import alignment_definition_exists
from ..repositories.block import list_incomplete_alignment_samples

REFERENCE_NOT_FOUND_DETAIL = "Reference not found"


async def require_reference_exists(pool: asyncpg.Pool, reference_idx: int) -> None:
    """404 unless the reference exists. Every reference-scoped route needs this so a
    typo'd idx is distinguishable from a genuinely empty answer — or, on a route that
    resolves the reference's contents, from contents that are genuinely absent."""
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=REFERENCE_NOT_FOUND_DETAIL)


# The one wording for "no such alignment", shared by every route that keys on an
# alignment_idx — the three cohort routes below and the alignment delete. Two spellings
# of one condition is a difference a client can accidentally depend on, and the delete
# route had its own until they were converged.
ALIGNMENT_NOT_FOUND_DETAIL = "alignment not found"


async def authorize_completed_alignment_cohort(
    pool: asyncpg.Pool,
    *,
    caller: Principal,
    alignment_idx: int,
    prep_sample_idx: list[int],
    nothing_to: str,
) -> list[int]:
    """The gate every route that names a cohort of one alignment runs, and the single
    copy of the order it runs in. Returns the authorized cohort.

    **The order is a disclosure decision, not a style.** Three checks:

    1. **The alignment exists** → 404, before anything discloses cohort state.
    2. **Access** → 403, all-or-nothing. A partially-served cohort would answer for some
       of a caller's samples and silently omit the rest.
    3. **Completeness** → 422, only once access has passed. Reversed, the 422's sample
       list would tell a caller which samples are in an alignment they have no right to
       read at all. It also subsumes an unknown identifier: a prep_sample that is not
       part of this alignment has no `qiita.alignment_sample` row and is reported here
       rather than vanishing from an answer claiming to cover the whole cohort.
       `alignment_sample.state` is first-class because alignment rows are NOT 1:1 with
       reads, so the presence of rows never means done.

    `nothing_to` completes the 422 — "an identifier names processed data, so there is
    nothing yet to name" — and is the one thing the callers legitimately differ in. It is
    a parameter rather than three copies of the whole ladder precisely because the order
    above is what must not drift between them: three routes hand-writing a
    security-relevant sequence is three chances for one to be reordered alone.
    """
    if not await alignment_definition_exists(pool, alignment_idx):
        raise HTTPException(status_code=404, detail=ALIGNMENT_NOT_FOUND_DETAIL)

    cohort = await authorize_prep_sample_cohort(
        pool, caller=caller, prep_sample_idx=prep_sample_idx, min_tier=COHORT_MIN_TIER
    )

    incomplete = await list_incomplete_alignment_samples(pool, alignment_idx, cohort)
    if incomplete:
        detail = (
            f"{len(incomplete)} prep_sample(s) not completed for alignment"
            f" {alignment_idx} (e.g. {first_few(incomplete)})"
        )
        raise HTTPException(status_code=422, detail=f"{detail}{nothing_to}")
    return cohort


def first_few(idxs: list[int], limit: int = 5) -> str:
    """Render at most `limit` identifiers, eliding the rest with an ellipsis.

    Any message built from identifiers the CALLER supplied must truncate: a
    refusal that echoes the whole cohort back, annotated, answers "which of these
    exist?" for the entire request body in one round trip — and the cohort caps
    run to 10 000.

    Used by every refusal on the prep_sample cohort routes. The host-filter
    refusals in routes/sequencing_run.py truncate for the same reason but predate
    this and render a bare `[:5]` list repr; converting them would change two
    live messages' wording, so they are deliberately left alone rather than
    quietly reworded here.
    """
    head = ", ".join(str(idx) for idx in idxs[:limit])
    return f"{head}, …" if len(idxs) > limit else head


async def authorize_prep_sample_cohort(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    *,
    caller: Principal,
    prep_sample_idx: Iterable[int],
    min_tier: Tier,
) -> list[int]:
    """Resolve a caller-named prep_sample cohort to the sorted, deduped list the
    route may act on, or 403 naming what would have to change.

    **All-or-nothing, never narrowed.** Every route that takes a cohort in a
    request body produces something whose meaning depends on the whole cohort — a
    signed ticket, a label map shipped beside a feature table — so quietly
    trimming it answers a different question under the name of the one that was
    asked. The paired *discovery* reads narrow instead, because a listing carries
    no result.

    Sorted and deduped because a cohort is a set: two spellings of one request
    should sign the same bytes and return the same rows.

    One function rather than eight copied lines per route, because what is being
    shared is a security decision — which samples a caller may act on, and how
    much a refusal is allowed to say — and the second copy is where those start to
    disagree.
    """
    cohort = sorted(set(prep_sample_idx))
    access = await filter_prep_samples_caller_can_read(
        pool_or_conn, caller=caller, prep_sample_idxs=cohort, min_tier=min_tier
    )
    if access.unlinked or access.blocked_by:
        raise HTTPException(
            status_code=403, detail=prep_sample_access_denied_detail(access, min_tier=min_tier)
        )
    return cohort


def prep_sample_access_denied_detail(access: PrepSampleReadAccess, *, min_tier: Tier) -> str:
    """The 403 body for a cohort read the caller may not fully perform: what the
    caller would have to change to be allowed.

    Both denial modes are reported, and separately — an unreadable study is
    something to go ask for, an unlinked sample is a data anomaly to report.

    **Deliberately truncated, and deliberately NOT correlated.** The caller
    chooses the cohort, so a message that named every blocked sample alongside
    the study that blocked it would answer, in one request, "which of these
    identifiers exist and which studies are they in?" for the whole body — an
    enumeration oracle over `prep_sample_to_study` handed to the lowest role we
    have. Naming a few of each is enough to act on and does not scale into a
    dump. Same reason and same shape as the host-filter refusal's `[:5]` in
    routes/sequencing_run.py.

    Shared by every all-or-nothing prep_sample cohort route, so the wording of a
    refusal — and its disclosure ceiling — has one definition.
    """
    parts = []
    if access.blocked_by:
        studies = sorted({s for denied in access.blocked_by.values() for s in denied})
        parts.append(
            f"requires study access at tier {str(min_tier)!r} or higher on"
            f" {len(studies)} study/studies (e.g. {first_few(studies)})"
        )
    if access.unlinked:
        parts.append(
            f"{len(access.unlinked)} prep_sample(s) have no active study link and"
            f" cannot be authorized (e.g. {first_few(access.unlinked)})"
        )
    return "; ".join(parts)


def _attempted_label(value: object) -> str:
    """Render the 'what was attempted' noun for a slot-collision message.

    Returns "missing-reason marker" for MissingReasonRef, "terminology
    term" for TerminologyTermRef, and "value" for bare typed scalars.
    """
    if isinstance(value, MissingReasonRef):
        return "missing-reason marker"
    if isinstance(value, TerminologyTermRef):
        return "terminology term"
    return "value"


# Shared 422-detail string for a foreign-key violation whose constraint
# is not in a route's specific message map. Lifted here so the wording
# stays identical across every route that falls back to it.
GENERIC_FK_VIOLATION = "references a row that does not exist"

# The optimistic-concurrency header pair: every route that emits a version
# stamp writes ETAG_HEADER, and every PATCH that gates on one reads
# IF_MATCH_HEADER. A caller round-trips the first into the second, so the two
# spellings are a contract rather than incidental strings.
ETAG_HEADER = "ETag"
IF_MATCH_HEADER = "If-Match"


def metadata_entries_from_rows(rows: Mapping[str, MetadataRow]) -> dict[str, MetadataEntry]:
    """Map a metadata-row dict to MetadataEntry, preserving the input keys.

    Each input key is reused unchanged as the output key, so whatever the rows
    were keyed on carries through. Only the four MetadataEntry fields are read
    from each row; a row's internal_name, when it has one, rides along as the
    key rather than as an entry field.
    """
    return {
        key: MetadataEntry(
            display_name=row.display_name,
            description=row.description,
            data_type=row.data_type,
            value=row.value,
        )
        for key, row in rows.items()
    }


async def read_global_and_local_entries(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
) -> tuple[dict[str, MetadataEntry], dict[str, MetadataEntry]]:
    """Read an entity's globally-linked and study-local metadata and shape both
    into MetadataEntry dicts.

    Spec-driven. The caller owns the connection/snapshot and the
    link/existence/retired gating. Returns (global_metadata keyed by
    internal_name, local_metadata keyed by display_name).
    """
    global_rows = await fetch_global_metadata(conn, spec=spec, entity_idx=entity_idx)
    local_rows = await fetch_local_metadata(
        conn, spec=spec, entity_idx=entity_idx, study_idx=study_idx
    )
    global_metadata = metadata_entries_from_rows(global_rows)
    local_metadata = metadata_entries_from_rows(local_rows)
    return global_metadata, local_metadata


def detail_for_unlinked_entity(*, noun: str, entity_idx: int, study_idx: int) -> str:
    """Build the HTTP-404 detail for an entity with no writable study link.

    One wording for every route and every layer that reports it, so a link
    rejected by the pre-write gate and one rejected mid-write by the database
    are indistinguishable on the wire. Returns the bare string; the caller
    wraps it in HTTPException with status 404.
    """
    return f"{noun} {entity_idx} is not linked to study {study_idx}"


async def resolve_linked_study_entity(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    fetch_row: Callable[[asyncpg.Connection, int], Awaitable[asyncpg.Record | None]],
    entity_idx: int,
    metadata_idx_column: str,
    study_idx: int,
    noun: str,
    retired_status: int,
    retired_detail: str,
) -> tuple[asyncpg.Record, int]:
    """Fetch a study-scoped entity and gate it on its study link + retirement.

    metadata_idx_column names the row column that keys metadata and the study
    link -- the entity's own idx for a direct entity, a supertype idx for a
    subtype (prep_sample_idx on a sequenced_sample). A nonexistent row and an
    unlinked one share the "not linked" 404 so existence never leaks across the
    study boundary; retirement is checked only after the link passes and raises
    retired_status/retired_detail (a read passes 404, a write passes 409).
    Returns the (non-None) row plus its metadata/link idx.
    """
    row = await fetch_row(conn, entity_idx)
    metadata_entity_idx = None if row is None else row[metadata_idx_column]
    linked = metadata_entity_idx is not None and await fetch_entity_is_linked_to_study(
        conn, spec=spec, entity_idx=metadata_entity_idx, study_idx=study_idx
    )
    if not linked:
        raise HTTPException(
            status_code=404,
            detail=detail_for_unlinked_entity(
                noun=noun, entity_idx=entity_idx, study_idx=study_idx
            ),
        )
    if row["retired"]:
        raise HTTPException(status_code=retired_status, detail=retired_detail)
    return row, metadata_entity_idx


async def read_study_scoped_entity(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    fetch_row: Callable[[asyncpg.Connection, int], Awaitable[asyncpg.Record | None]],
    entity_idx: int,
    metadata_idx_column: str,
    study_idx: int,
    noun: str,
) -> tuple[asyncpg.Record, dict[str, MetadataEntry], dict[str, MetadataEntry]]:
    """Fetch a study-scoped entity for reading and return its metadata.

    Gates via resolve_linked_study_entity with a read's 404-on-retired, then
    reads the entity's global and study-local metadata. Returns the (non-None)
    row plus the two MetadataEntry dicts.
    """
    row, metadata_entity_idx = await resolve_linked_study_entity(
        conn,
        spec=spec,
        fetch_row=fetch_row,
        entity_idx=entity_idx,
        metadata_idx_column=metadata_idx_column,
        study_idx=study_idx,
        noun=noun,
        retired_status=404,
        retired_detail=f"{noun} {entity_idx} not found",
    )
    global_metadata, local_metadata = await read_global_and_local_entries(
        conn, spec=spec, entity_idx=metadata_entity_idx, study_idx=study_idx
    )
    return row, global_metadata, local_metadata


# Sample-family metadata-write exceptions carrying one shared HTTP mapping.
# Entity-specific errors (owner-id-field-collision, required-field, asyncpg) are
# excluded — they are mapped per entity, not here.
SAMPLE_METADATA_WRITE_ERRORS = (
    MetadataUnknownFieldsError,
    MetadataParseError,
    StudyFieldConflictError,
    DuplicateGlobalFieldTargetError,
    OwnerSampleIdMetadataWriteError,
    SlotOccupiedError,
    TransientWriteRaceError,
)


async def raise_http_for_sample_metadata_write_error(
    conn: asyncpg.Connection, exc: Exception
) -> NoReturn:
    """Map a sample-family metadata-write exception to its HTTPException.

    One exception maps to exactly one response, so the mapping cannot drift.
    Parse, unknown-field, study-field-conflict, duplicate-global-target, and
    owner-sample-id errors map to 422; a slot collision to 409 (diagnosed
    against conn); a transient write race to 503. Always raises.
    """
    if isinstance(exc, MetadataUnknownFieldsError):
        raise HTTPException(
            status_code=422,
            detail=f"unknown metadata fields: {', '.join(exc.field_keys)}",
        )
    if isinstance(exc, MetadataParseError):
        raise HTTPException(
            status_code=422,
            detail=(
                f"could not parse metadata field {exc.field_key!r}"
                f" value {exc.text_value!r} as {exc.data_type}: {exc.reason}"
            ),
        )
    if isinstance(exc, StudyFieldConflictError):
        # found_global_field_idx None means the shadowing study field is
        # purely-local; otherwise it is bound to a different global field.
        if exc.found_global_field_idx is None:
            conflict = "a purely-local field of that name"
        else:
            conflict = "a field of that name bound to a different global field"
        raise HTTPException(
            status_code=422,
            detail=(
                f"metadata field {exc.display_name!r} conflicts with"
                f" {conflict} already on this study"
            ),
        )
    if isinstance(exc, DuplicateGlobalFieldTargetError):
        raise HTTPException(
            status_code=422,
            detail=f"metadata fields {exc.field_keys!r} all resolve to the same global field",
        )
    if isinstance(exc, OwnerSampleIdMetadataWriteError):
        raise HTTPException(
            status_code=422,
            detail=(
                f"metadata field {exc.display_name!r} is an owner-sample-id field"
                " and cannot be written as ordinary metadata"
            ),
        )
    if isinstance(exc, SlotOccupiedError):
        detail = await detail_for_slot_collision(conn, exc)
        raise HTTPException(status_code=409, detail=detail)
    if isinstance(exc, TransientWriteRaceError):
        raise_for_transient_write_race(exc)
    # Reached only if SAMPLE_METADATA_WRITE_ERRORS above gained a member with no
    # branch here; a parity test pins the two together. Fail loud rather than
    # swallow the exception or answer with a status picked for a different error.
    raise exc


async def write_and_map_sample_metadata(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    entity_idx: int,
    study_idx: int,
    metadata: Mapping[str, str],
    caller_idx: int,
    unlinked_detail: str,
    global_internal_names: bool = False,
) -> SampleMetadataWriteResponse:
    """Upsert a metadata dict for a sample-family entity and shape the result.

    Writes each field (allow_local=True), maps the metadata-write exceptions to
    their HTTP responses, and returns the per-field results keyed by the key the
    caller sent, in its input order. global_internal_names keys global fields on
    internal_name rather than display_name; study-local fields are display-name-
    keyed either way. Each result carries the resolved field's internal_name,
    which is the key a globally-linked value reads back under; it equals the key
    the caller sent only when that key was the global's own internal_name, so a
    value resolved through a study-local alias reads back under a different key
    whatever the flag says. A cross-study slot collision still 409s; a
    same-study, same-field, different-value rewrite is a last-writer-wins
    overwrite -- there is no If-Match on this path, so the caller accepts
    lost-update semantics.

    unlinked_detail is the 404 body for a study link the database refuses at
    write time; the caller supplies it because only the caller knows which idx
    it named the entity by (an entity keying its metadata on a supertype idx
    still answers under the idx the request carried).
    """
    try:
        # on_conflict="upsert" overwrites the caller's own study's value in
        # place. No If-Match guards this, so a concurrent same-study rewrite of
        # the same field is last-writer-wins (lost update); a foreign study's
        # value still raises (409) rather than being overwritten.
        results = await write_sample_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=study_idx,
            metadata=metadata,
            caller_idx=caller_idx,
            allow_local=True,
            on_conflict="upsert",
            global_internal_names=global_internal_names,
        )
    except SAMPLE_METADATA_WRITE_ERRORS as exc:
        await raise_http_for_sample_metadata_write_error(conn, exc)
    except asyncpg.RaiseError as exc:
        # The retired-link trigger tags its error DETAIL with a `trigger` key
        # naming the raising DB function. Dispatch on that key (never on message
        # prose) and re-raise every other RaiseError: several other guards on
        # these tables share this SQLSTATE, and answering 404 for one of those
        # would name the wrong cause. The link was writable when the caller was
        # gated and is not now, so the answer is the gate's own 404 -- what a
        # retry returns, and one status for one condition.
        detail_fields = parse_kv_detail(exc.detail)
        if detail_fields.get("trigger") == spec.metadata_retired_link_trigger:
            raise HTTPException(status_code=404, detail=unlinked_detail)
        raise
    return SampleMetadataWriteResponse(
        results={
            # scope is derived from internal_name on the wire model, so it is
            # not passed here (extra="forbid" would reject it).
            r.field_key: MetadataFieldWriteResult(
                internal_name=r.internal_name, outcome=r.outcome, value=r.value
            )
            for r in results
        }
    )


async def create_and_map_study_field(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    study_idx: int,
    body: SampleStudyFieldCreateRequest,
    caller_idx: int,
    response_model: type[SampleStudyFieldResponse],
) -> SampleStudyFieldResponse:
    """Create one study-local field for a sample-family entity and shape the
    stored row into response_model.

    Create-side conflicts map to 409 and DB-level violations to 422 (the
    Pydantic body should preempt the CHECK, but it is the last defense). The
    caller owns the transaction.
    """
    noun = spec.entity_kind
    try:
        row = await create_study_field_and_read_back(
            conn,
            spec=spec,
            study_idx=study_idx,
            display_name=body.display_name,
            created_by_idx=caller_idx,
            description=body.description,
            global_field_idx=body.global_field_idx,
            data_type=body.data_type,
            required=body.required,
            terminology_idx=body.terminology_idx,
            tier_override=body.tier_override,
        )
    except StudyFieldAlreadyExistsError:
        raise HTTPException(
            status_code=409,
            detail=f"a {noun} field named {body.display_name!r} already exists on this study",
        )
    except StudyFieldConflictError:
        raise HTTPException(
            status_code=409,
            detail=(
                f"a {noun} field named {body.display_name!r} already exists"
                " on this study bound to a different global field"
            ),
        )
    except TransientWriteRaceError as exc:
        raise_for_transient_write_race(exc)
    except asyncpg.ForeignKeyViolationError:
        raise HTTPException(status_code=422, detail=GENERIC_FK_VIOLATION)
    except asyncpg.CheckViolationError:
        raise HTTPException(status_code=422, detail=f"violates a database constraint on {noun}")

    return map_study_field_row(row, spec=spec, response_model=response_model)


def map_study_field_row[T: SampleStudyFieldResponse](
    row: asyncpg.Record,
    *,
    spec: EntityMetadataSpec,
    response_model: type[T],
) -> T:
    """Shape one {entity}_study_field row into response_model.

    Every column but the two idx fields is named identically
    on the wire, so only those are renamed — the row's own idx, which arrives
    as `idx`, and the global link, which arrives under its entity-specific SQL
    column — each to whichever entity-qualified spelling response_model
    declares for it.
    """
    payload = dict(row)
    payload[field_wire_name(response_model, STUDY_FIELD_IDX_ATTR)] = payload.pop("idx")
    payload[field_wire_name(response_model, GLOBAL_FIELD_IDX_ATTR)] = payload.pop(
        spec.study_field_global_fk_column
    )
    validated = response_model.model_validate(payload)
    return validated


# same-pattern-ok: registry sibling of map_study_field_row; kept separate so each
# response model is bound to the idx field it actually declares, which a shared
# idx-attr parameter would stop enforcing
def map_global_field_row[T: SampleGlobalFieldResponse](
    row: asyncpg.Record,
    *,
    response_model: type[T],
) -> T:
    """Shape one {entity}_global_field row into response_model.

    Every column but the row's own idx is named identically on the wire, so
    only that one key is renamed — to whichever entity-qualified spelling
    response_model declares for it.
    """
    payload = dict(row)
    payload[field_wire_name(response_model, GLOBAL_FIELD_IDX_ATTR)] = payload.pop("idx")
    validated = response_model.model_validate(payload)
    return validated


def raise_for_unique_violation(
    exc: asyncpg.UniqueViolationError,
    *,
    constraint_messages: dict[str, str],
    generic: str,
) -> None:
    """Translate a UNIQUE-constraint violation into a 409 response.

    Looks up `exc.constraint_name` against `constraint_messages`; an
    unknown name yields `generic` as the detail. Never returns; always
    raises HTTPException.
    """
    detail = constraint_messages.get(exc.constraint_name, generic)
    raise HTTPException(status_code=409, detail=detail)


async def resolve_metadata_checklist_idx(
    conn: asyncpg.Connection,
    name: str | None,
) -> int | None:
    """Resolve a caller-supplied checklist name to its idx for a write.

    None passes through as None. An unknown name is mapped to a 422 so
    every create/patch surface reports it identically rather than letting
    it surface as a downstream FK violation.
    """
    try:
        return await fetch_metadata_checklist_idx_by_name(conn, name)
    except MetadataChecklistUnknownError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"metadata_checklist_name {exc.name!r} does not reference an existing checklist",  # noqa: E501
        )


def etag_for_updated_at(updated_at: datetime) -> str:
    """Build the quoted ETag header value from a row's updated_at timestamp.

    The surrounding double-quotes are required by RFC 7232's entity-tag
    grammar — the on-the-wire value is `"<iso8601>"`, not `<iso8601>`.
    The inner ISO 8601 timestamp is opaque to clients; only its
    byte-for-byte equality with a subsequent If-Match header matters.
    """
    return f'"{updated_at.isoformat()}"'


def require_if_match(if_match: str | None) -> str:
    """Raise 428 when the caller did not send an If-Match header.

    All patching requires optimistic-concurrency control; routing
    the 428 through this helper keeps the wording identical."""
    if if_match is None:
        raise HTTPException(status_code=428, detail="If-Match header required")
    return if_match


def require_etag_match(
    row: asyncpg.Record | None,
    *,
    if_match: str,
    label: str,
    row_idx: int,
) -> None:
    """Run the post-FOR-UPDATE-preflight 404 / 412 checks for a PATCH route.

    Called after `fetch_<entity>(conn, idx, for_update=True)` to fold
    "row absent (404)" and "ETag stale (412)" into one site so every
    PATCH endpoint emits the same wording. `label` is the entity noun
    embedded in the 404 detail (e.g. "study", "biosample").
    """
    if row is None:
        raise HTTPException(status_code=404, detail=f"{label} {row_idx} not found")
    if if_match != etag_for_updated_at(row["updated_at"]):
        raise HTTPException(status_code=412, detail="If-Match did not match")


async def detail_for_slot_collision(
    conn: asyncpg.Connection,
    exc: SlotOccupiedError,
) -> str:
    """Build the HTTP-409 detail string for a metadata slot collision.

    Dispatches on the SlotOccupiedError subclass to produce a sub-case-
    specific message; both metadata-writing routes (biosample import,
    sequenced-sample create) call this so wording stays consistent
    across endpoints. Both the global-write and local-write paths route
    through this dispatcher: exc.global_field_idx is non-None for the
    global path and None for the local path, selecting the slot
    identifier embedded in the message. For the missing-reason sub-case
    the helper resolves the missing_value_reason row's name with one
    extra SELECT against the spec-known table — actionability over
    terseness, so the caller learns what reason occupies the slot.

    Returns the bare string; the caller wraps it in HTTPException with
    status 409. Per the project decision, all six sub-cases return 409
    (Conflict). Contributing study idx is surfaced so callers can
    correlate; study name is intentionally not joined (caller may not
    have read access to that study).
    """
    # The same/different subclasses cover typed-vs-typed,
    # missing-vs-missing, and terminology-vs-terminology equality; the
    # attempted_value's kind selects the wording noun per branch via
    # _attempted_label.
    what = _attempted_label(exc.attempted_value)
    # Slot identifier: global path is keyed by global_field_idx, local
    # path by the entity-scoped study_field_idx. The non-None-ness of
    # exc.global_field_idx discriminates without a separate flag.
    slot_id = (
        f"global_field_idx={exc.global_field_idx}"
        if exc.global_field_idx is not None
        else f"{exc.entity_kind}_study_field_idx={exc.study_field_idx}"
    )
    # Where the occupied slot sits — field, entity, and slot identifier. Every
    # branch below names it, so it is rendered once here.
    slot_location = (
        f"field {exc.display_name!r} on {exc.entity_kind}_idx={exc.entity_idx} ({slot_id})"
    )
    # Match on the concrete subclass to pick the right wording. The
    # generic SlotOccupiedError fallback covers any future subclass
    # added without a wording branch here; reading the catch-all
    # message in production points the maintainer at this dispatch.
    if isinstance(exc, DuplicateValueSameStudyError):
        return (
            f"your study already wrote this same {what} for {slot_location}; no new row was created"
        )
    if isinstance(exc, ConflictingValueSameStudyError):
        return (
            f"your study previously wrote a different {what} for {slot_location};"
            f" correct it via PATCH or DELETE+INSERT, not INSERT"
        )
    if isinstance(exc, DuplicateValueDifferentStudyError):
        return (
            f"the {what} you attempted is already present for {slot_location},"
            f" contributed by study_idx={exc.contributing_study_idx}; your study does"
            f" not own the row"
        )
    if isinstance(exc, ConflictingValueDifferentStudyError):
        return (
            f"another study (study_idx={exc.contributing_study_idx}) has"
            f" written a different {what} for {slot_location};"
            f" the global field's canonical value is in dispute"
        )
    if isinstance(exc, SlotOccupiedByMissingReasonError):
        # One extra SELECT to resolve the human-readable reason name so the
        # caller knows what reason occupies the slot; the missing_value_reason
        # table is shared across all entity kinds (no spec dispatch needed).
        # existing_missing_reason_idx is non-None whenever this subclass fires
        # (the diagnose path only constructs it when the missing-reason FK is
        # populated); the assert documents the invariant for asyncpg's binder.
        assert exc.existing_missing_reason_idx is not None
        reason_name = await conn.fetchval(
            "SELECT name FROM qiita.missing_value_reason WHERE idx = $1",
            exc.existing_missing_reason_idx,
        )
        return (
            f"the value for {slot_location} is"
            f" recorded as intentionally missing (reason: {reason_name});"
            f" the missing-reason row must be deleted before a typed"
            f" value can be written"
        )
    if isinstance(exc, SlotOccupiedByTypedValueError):
        # Existing typed value travels on the exception payload — no DB
        # roundtrip needed for str/Decimal/date. A terminology-term slot
        # carries an int FK (qiita.terminology_term.idx); resolve it to
        # the human-readable term_id + label with one extra SELECT so the
        # caller sees what term occupies the slot rather than a bare idx.
        # str values render via repr() (quoting distinguishes "123" from 123);
        # Decimal / date render via str() so the body shows "1.5" / "2024-01-02"
        # instead of "Decimal('1.5')" / "datetime.date(2024, 1, 2)".
        if isinstance(exc.existing_value, int) and not isinstance(exc.existing_value, bool):
            term_row = await conn.fetchrow(
                "SELECT term_id, label FROM qiita.terminology_term WHERE idx = $1",
                exc.existing_value,
            )
            rendered_existing = (
                f"terminology term {term_row['term_id']!r} ({term_row['label']!r})"
                if term_row is not None
                else f"terminology_term_idx={exc.existing_value}"
            )
        elif isinstance(exc.existing_value, str):
            rendered_existing = repr(exc.existing_value)
        else:
            rendered_existing = str(exc.existing_value)
        return (
            f"the value for {slot_location} is"
            f" already recorded as a typed value ({rendered_existing});"
            f" the typed row must be deleted before a missing-reason"
            f" marker can be written"
        )
    # Fallback: an unrecognised subclass means the exception hierarchy
    # grew without the dispatch above being extended. Surface a generic
    # message rather than crashing the route; the maintainer can find
    # the missing branch via this string.
    return (
        f"{exc.entity_kind}_metadata slot for {exc.display_name!r} is"
        f" already occupied (entity_idx={exc.entity_idx}, {slot_id})"
    )


def parse_kv_detail(detail: str | None) -> dict[str, str]:
    """Parse a Postgres error ``DETAIL`` of comma-separated ``key=value`` pairs.

    A trigger that needs to hand structured data to a route puts it in
    the error's DETAIL field as ``k1=v1, k2=v2`` rather than embedding it
    in the human-readable MESSAGE: regex-parsing MESSAGE for data couples
    route code to migration string literals and breaks silently when the
    wording is edited. Chunks without an ``=`` are skipped; a None or
    empty detail yields an empty dict.

    Splitting is on a bare ``,`` — values must be comma-free (the current
    callers emit integers and schema identifiers, both safe). A value
    that can contain a comma needs a different encoding.
    """
    fields: dict[str, str] = {}
    if not detail:
        return fields
    for chunk in detail.split(","):
        key, sep, value = chunk.partition("=")
        if sep:
            fields[key.strip()] = value.strip()
    return fields


def detail_for_biosample_link_rejection(detail_fields: dict[str, str]) -> str:
    """Build the HTTP-422 detail for a rejected prep_sample_to_study link.

    Takes the already-parsed DETAIL fields (see parse_kv_detail) emitted
    by the prep_sample_to_study_reject_without_biosample_link trigger in
    db/migrations/20260501000011_prep_sample.sql. That trigger fires once
    per link row the sequenced-sample composer inserts — a primary study
    plus zero or more secondaries — so "the requested study" is ambiguous
    when a body lists several; this helper names the exact study that
    lacks a biosample link. Missing keys degrade to ``?`` so a trigger
    that ever stops emitting DETAIL produces a vague message instead of
    crashing the route.
    """
    study_idx = detail_fields.get("study_idx", "?")
    biosample_idx = detail_fields.get("biosample_idx", "?")
    return (
        f"prep_sample cannot be linked to study_idx={study_idx}:"
        f" biosample_idx={biosample_idx} is not linked to that study"
        " (or the link is retired)"
    )


# Retry-After is advisory; the race self-resolves the instant the
# concurrent delete commits, so a 1-second hint is generous. Sent as a
# string because that is the on-the-wire header value.
_TRANSIENT_WRITE_RACE_RETRY_AFTER = "1"


def raise_for_transient_write_race(exc: TransientWriteRaceError) -> None:
    """Translate a lost write race into a 503 retry response.

    Both metadata-writing routes call this so the status, wording, and
    Retry-After hint stay identical across endpoints. The occupant that
    triggered the unique violation was concurrently deleted before it
    could be diagnosed, so the slot is free again and the same request
    will succeed on resubmission — 503 (transient) with Retry-After, not
    409 (the state is not actually in conflict) and not 500.

    Never returns; always raises HTTPException.
    """
    raise HTTPException(
        status_code=503,
        detail=(
            f"a concurrent delete raced your {exc.row_label} write"
            f" ({exc.slot_summary}); the slot is now free —"
            f" resubmit the identical request"
        ),
        headers={"Retry-After": _TRANSIENT_WRITE_RACE_RETRY_AFTER},
    )


async def resolve_idxs_by_natural_key(
    *,
    values: list[str],
    fetcher: Callable[[list[str]], Awaitable[dict[str, int]]],
) -> tuple[dict[str, int], list[str]]:
    """Dedup `values` in input order, resolve survivors via `fetcher`, and
    return `(resolved, missing)`.

    The caller supplies `fetcher` already bound to its pool and any per-key
    SQL details so this helper stays table-agnostic. `missing` is the
    input-order deduped list of values that did not resolve.
    """
    # Dedup while preserving input order so `missing` is deterministic.
    dedup_ordered: list[str] = []
    seen: set[str] = set()
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        dedup_ordered.append(v)

    resolved = await fetcher(dedup_ordered)
    missing = [v for v in dedup_ordered if v not in resolved]
    return resolved, missing


def cap_rows[T](rows: list[T], cap: int) -> tuple[list[T], bool]:
    """Split a `cap + 1` fetch into `(rows, truncated)`.

    A capped route over-fetches by one row, so a length strictly greater than
    `cap` means the underlying set is larger than the page. Returns the rows
    sliced back to `cap` and whether the slice dropped anything. Read one row
    short of `cap + 1` and `truncated` is False on a set that is in fact larger.

    **This is the posture for a LISTING**, where a short answer is still a usable
    answer and `truncated` tells the caller to narrow. A read whose result is
    consumed as a JOIN key — a lookup table — must not use it: a silently short
    lookup makes the caller's derived artifact *wrong* rather than partial, and
    nobody checks `truncated` on a map. Those refuse instead (413 naming the real
    size); `routes/reference.py`'s genome map is the worked example.
    """
    if len(rows) > cap:
        return rows[:cap], True
    return rows, False


def build_idxs_list_response(
    idxs: list[int], *, cap: int, caller_system_role: str
) -> IdxsListResponse:
    """Build the capped IdxsListResponse envelope.

    `idxs` is the already-fetched list; callers fetch `cap + 1` rows so a
    length strictly greater than `cap` signals the underlying set overflowed.
    Slices back to `cap` and sets `truncated` accordingly, centralizing the
    fetch-cap-plus-one / slice / envelope shaping in one place.
    """
    kept, truncated = cap_rows(idxs, cap)
    return IdxsListResponse(
        idxs=kept,
        count=len(kept),
        truncated=truncated,
        caller_system_role=caller_system_role,
    )
