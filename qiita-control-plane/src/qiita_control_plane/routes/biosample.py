"""Biosample routes.

Writes apply their mutation inside one connection-scoped transaction,
delegating multi-table work to the biosample composer.
"""

import contextlib
from collections.abc import Awaitable, Callable, Sequence
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Response
from pydantic import Field
from qiita_common.api_paths import (
    PATH_BIOSAMPLE_BULK_BY_STUDY,
    PATH_BIOSAMPLE_BY_IDX,
    PATH_BIOSAMPLE_BY_STUDY,
    PATH_BIOSAMPLE_BY_STUDY_AND_IDX,
    PATH_BIOSAMPLE_BY_STUDY_UNIQUE_FIELD,
    PATH_BIOSAMPLE_GLOBAL_FIELD_PREFIX,
    PATH_BIOSAMPLE_GLOBAL_FIELD_ROOT,
    PATH_BIOSAMPLE_LIST_BY_STUDY,
    PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION,
    PATH_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID,
    PATH_BIOSAMPLE_METADATA_BY_STUDY,
    PATH_BIOSAMPLE_METADATA_BY_STUDY_UNIQUE_FIELD,
    PATH_BIOSAMPLE_PREFIX,
    PATH_BIOSAMPLE_RESOLVE_ROSTER,
    PATH_BIOSAMPLE_STUDY_FIELD_BY_IDX,
    PATH_BIOSAMPLE_STUDY_FIELD_BY_STUDY,
    PATH_STUDY_PREFIX,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    BiosampleBulkImportRequest,
    BiosampleBulkImportResponse,
    BiosampleGlobalFieldResponse,
    BiosampleImportRequest,
    BiosampleImportResponse,
    BiosampleLookupByAccessionRequest,
    BiosampleLookupByAccessionResponse,
    BiosampleLookupByMatrixTubeIdRequest,
    BiosampleLookupByMatrixTubeIdResponse,
    BiosampleMetadataWriteByUniqueFieldResponse,
    BiosamplePatchRequest,
    BiosampleResponse,
    BiosampleStudyFieldCreateRequest,
    BiosampleStudyFieldResponse,
    IdxsListResponse,
    MetadataChecklistRef,
    MetadataEntry,
    RosterResolveFailure,
    RosterResolveRequest,
    RosterResolveResponse,
    SampleMetadataWriteByUniqueFieldRequest,
    SampleMetadataWriteRequest,
    SampleMetadataWriteResponse,
    SampleStudyFieldPatchRequest,
    SampleUniqueFieldRef,
    StudyScopedBiosampleResponse,
    Tier,
)

from ..auth.guards import (
    require_complete_profile,
    require_eligible_owner,
    require_human,
    require_role_at_least,
    require_scope,
    require_study_access,
    require_study_exists,
)
from ..auth.principal import HumanUser, Principal
from ..deps import TxConnFactory, get_db_pool, get_snapshot_conn_factory, get_tx_conn_factory
from ..repositories._sample_helpers import (
    LocalWriteOnGloballyLinkedFieldError,
    MetadataMissingRequiredFieldsError,
    ResolvedFieldValue,
    StudyFieldDataTypeNotTextError,
    StudyFieldNotUniqueInStudyError,
    UniqueInStudyViolation,
    classify_unique_in_study_violation,
    fetch_global_fields,
    fetch_global_metadata,
    fetch_study_fields_for_study,
    insert_new_entities_metadata_batch,
    write_resolved_metadata_entries,
)
from ..repositories.biosample import (
    BiosampleLookupKey,
    fetch_biosample,
    fetch_biosample_idxs_by_natural_key,
    fetch_biosample_idxs_for_study,
    fetch_caller_has_biosample_access,
    import_biosample_from_owner_biosample_id,
    update_biosample,
)
from ..repositories.biosample_metadata import (
    BIOSAMPLE_METADATA_SPEC,
    BiosampleOwnerIdFieldCollisionError,
    BiosampleOwnerIdMissingValueError,
)
from ..roster_resolution import classify_roster, fetch_roster_facts
from ._helpers import (
    ETAG_HEADER,
    GENERIC_CHECK_VIOLATION,
    GENERIC_FK_VIOLATION,
    IF_MATCH_HEADER,
    SAMPLE_METADATA_WRITE_ERRORS,
    build_idxs_list_response,
    create_and_map_study_field,
    detail_for_retired_unique_field_entity,
    detail_for_unique_field_miss,
    etag_for_updated_at,
    map_global_field_row,
    map_study_field_row,
    metadata_entries_from_rows,
    patch_and_map_study_field,
    raise_for_unique_violation,
    raise_http_for_sample_metadata_write_error,
    raise_transient_retry,
    read_and_map_study_field,
    read_study_scoped_entity,
    require_etag_match,
    require_if_match,
    resolve_and_write_study_scoped_metadata,
    resolve_idxs_by_natural_key,
    resolve_metadata_checklist_idx,
    resolve_study_entity_by_unique_field,
)

router = APIRouter(prefix=PATH_STUDY_PREFIX, tags=["biosample"])
biosample_router = APIRouter(prefix=PATH_BIOSAMPLE_PREFIX, tags=["biosample"])
global_field_router = APIRouter(prefix=PATH_BIOSAMPLE_GLOBAL_FIELD_PREFIX, tags=["biosample"])


_MSG_OWNER_NOT_ELIGIBLE = "owner is not eligible to own biosamples"

# Map of biosample-level constraint names import_biosample_from_owner_biosample_id
# can trip. The owner-id field's own uniqueness index is the same exception class
# and is answered by an earlier arm; everything else is pre-flight-checked,
# swallowed by ON CONFLICT, or surfaces as a different exception class. Unknown
# names fall back to the generic strings on the matching exception path.
_UNIQUE_VIOLATION_MESSAGES: dict[str, str] = {
    "biosample_accession_unique": "biosample_accession already in use",
    "biosample_ena_sample_accession_unique": "ena_sample_accession already in use",
    "biosample_matrix_tube_id_unique": "matrix_tube_id already in use",
}
_FK_VIOLATION_MESSAGES: dict[str, str] = {
    "biosample_metadata_checklist_idx_fkey": (
        "metadata_checklist_idx does not reference an existing checklist"
    ),
}
# CHECK-constraint-name → caller-facing detail for 422 responses. The
# Pydantic models for matrix_tube_id should preempt this in practice, but
# the DB CHECK is the last line of defense and a violation here surfaces
# the same field-specific message a bypassed validator would have.
_CHECK_VIOLATION_MESSAGES: dict[str, str] = {
    "biosample_matrix_tube_id_format": ("matrix_tube_id must be exactly 10 digits"),
}
_GENERIC_UNIQUE_VIOLATION = "conflicts with an existing biosample"


@router.post(PATH_BIOSAMPLE_BY_STUDY, status_code=201)
async def import_biosample(
    study_idx: Annotated[int, Field(gt=0)],
    body: BiosampleImportRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> BiosampleImportResponse:
    """Create a biosample on a study, atomically with its owner-provided id and metadata.

    The caller must be a HumanUser with profile_complete=True, must hold
    the biosample:write scope, and must have `Tier.ADMIN` access (or
    higher) to the path's study — equivalently, must own the study OR
    carry a `study_access` row at the ADMIN tier OR be wet_lab_admin /
    system_admin (role bypass). `require_study_exists` composes alongside
    `require_study_access` so role-bypass callers still get 404 on a
    non-existent study_idx rather than slipping through to an FK violation.
    """
    async with tx() as conn:
        await _require_eligible_import_owner(conn, body.owner_idx)
        response, _ = await _import_one_biosample(
            conn,
            study_idx=study_idx,
            body=body,
            caller_idx=user.principal_idx,
            metadata_checklist_idx=await resolve_metadata_checklist_idx(
                conn, body.metadata_checklist_name
            ),
        )
        return response


async def _require_eligible_import_owner(conn: asyncpg.Connection, owner_idx: int) -> None:
    """Owner eligibility pre-flight for an import; collapses every
    ineligibility case to one 422. Once per request: a bulk batch has one
    owner (BiosampleBulkImportRequest enforces it)."""
    await require_eligible_owner(conn, candidate_idx=owner_idx, detail=_MSG_OWNER_NOT_ELIGIBLE)


async def _import_one_biosample(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    body: BiosampleImportRequest,
    caller_idx: int,
    metadata_checklist_idx: int | None,
    defer_metadata_write: bool = False,
) -> tuple[BiosampleImportResponse, Sequence[ResolvedFieldValue] | None]:
    """Import ONE biosample within an EXISTING transaction, mapping composer and DB
    errors to HTTPException.

    Shared by the single POST (`import_biosample`) and the bulk POST
    (`import_biosamples_bulk`) so both surface identical errors. The CALLER owns the
    transaction: a raised HTTPException propagates out of the caller's `async with
    tx()` and rolls its whole unit back — which is what makes the bulk route
    all-or-nothing. The bulk route catches and re-raises with the failing row's index.

    Returns the response and, with `defer_metadata_write`, the row's validated
    metadata for the caller to write in a batch (`import_biosamples_bulk`).

    The caller has already checked the owner's eligibility
    (`_require_eligible_import_owner`) and resolved the checklist name, so a bulk
    batch does each once rather than once per row.
    """
    result = await _with_import_error_mapping(
        conn,
        body,
        lambda: import_biosample_from_owner_biosample_id(
            conn,
            primary_study_idx=study_idx,
            owner_idx=body.owner_idx,
            owner_biosample_id_field_name=body.owner_biosample_id_field_name,
            owner_biosample_id_value=body.owner_biosample_id_value,
            caller_idx=caller_idx,
            metadata=body.metadata,
            metadata_checklist_idx=metadata_checklist_idx,
            biosample_accession=body.biosample_accession,
            ena_sample_accession=body.ena_sample_accession,
            matrix_tube_id=body.matrix_tube_id,
            global_internal_names=body.global_internal_names,
            defer_metadata_write=defer_metadata_write,
        ),
    )
    response = BiosampleImportResponse(
        biosample_idx=result.biosample_idx,
        owner_id_biosample_study_field_idx=result.owner_id_biosample_study_field_idx,
        owner_id_biosample_study_field_created=result.owner_id_biosample_study_field_created,
    )
    return response, result.deferred_metadata


async def _with_import_error_mapping[T](
    conn: asyncpg.Connection,
    body: BiosampleImportRequest,
    operation: Callable[[], Awaitable[T]],
) -> T:
    """Run one import step, mapping composer and DB errors to HTTPException.

    The single place a biosample import's failures are translated, so the per-row
    import and the bulk route's metadata fallback answer identically. Known
    composer-side validation errors and DB-level violations become 422 / 409
    responses; composer-specific exceptions are caught first so their detail wins
    over the generic asyncpg fallbacks.
    """
    try:
        return await operation()
    except BiosampleOwnerIdFieldCollisionError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                f"metadata key {exc.display_name!r} collides with owner_biosample_id_field_name"
            ),
        )
    except BiosampleOwnerIdMissingValueError as exc:
        # owner_biosample_id_value matches a missing_value_reason name.
        # The owner-id row carries an identifier; a missing-value
        # marker is incompatible with that contract.
        raise HTTPException(
            status_code=422,
            detail=(
                f"owner_biosample_id_value {exc.owner_biosample_id_value!r}"
                " cannot be a missing-value marker"
            ),
        )
    except MetadataMissingRequiredFieldsError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                "missing required metadata field(s):"
                f" {', '.join(exc.missing_display_names)}."
                " A field may be given a missing-value marker (e.g."
                " 'not applicable') — declining to answer is allowed, silence"
                " is not."
            ),
        )
    except SAMPLE_METADATA_WRITE_ERRORS as exc:
        # Shared metadata-write arms (parse / unknown / conflict /
        # duplicate-global-target / slot-collision / transient-race) map
        # through one place. The owner-id and asyncpg arms below stay
        # route-specific. SlotOccupiedError is unreachable through this POST
        # (a fresh biosample per call cannot pre-occupy a slot) but is
        # handled for the shared PATCH path that reuses this composer.
        await raise_http_for_sample_metadata_write_error(conn, exc)
    except LocalWriteOnGloballyLinkedFieldError as exc:
        # The requested owner-biosample-id field name resolves to a
        # field already globally linked on this study. The owner-id
        # row is purely-local identifier and must not be written through a
        # cross-study global slot; the caller must pick a different
        # owner_biosample_id_field_name. Its own exception family,
        # independent of the asyncpg.UniqueViolationError catch below.
        raise HTTPException(
            status_code=409,
            detail=(
                f"owner_biosample_id_field_name {exc.display_name!r} is"
                " already bound to a global field on this study"
            ),
        )
    except StudyFieldNotUniqueInStudyError as exc:
        # The named field exists on this study but does not declare that
        # its values are unique within it, so it cannot serve as the
        # owner's identifier. Resolving it is the study's call — make that
        # field unique, or name a different one — so the refusal says which
        # field rather than choosing for them.
        raise HTTPException(
            status_code=409,
            detail=(
                f"owner_biosample_id_field_name {exc.display_name!r} does not"
                " declare its values unique within this study; make it unique or"
                " name a different field"
            ),
        )
    except StudyFieldDataTypeNotTextError as exc:
        # The named field exists on this study but stores something other
        # than text, so it cannot hold the identifier as submitted. Naming
        # a different field is the study's call, so the refusal says which
        # field and what it stores rather than choosing for them.
        raise HTTPException(
            status_code=409,
            detail=(
                f"owner_biosample_id_field_name {exc.display_name!r} does not store"
                f" text (data_type is {exc.data_type!r}); name a different field"
            ),
        )
    except asyncpg.UniqueViolationError as exc:
        # A repeated owner id through one field trips that field's own
        # uniqueness index rather than any of the biosample-level
        # constraints, and the generic message would not say so. The scope
        # is the field, not the study: a study may record owner ids through
        # more than one local field, and the same value through a different
        # one is not a repeat.
        if (
            classify_unique_in_study_violation(exc, spec=BIOSAMPLE_METADATA_SPEC)
            is UniqueInStudyViolation.DUPLICATE_VALUE
        ):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"owner_biosample_id_value {body.owner_biosample_id_value!r} is"
                    " already used by another biosample through"
                    f" {body.owner_biosample_id_field_name!r}"
                ),
            )
        raise_for_unique_violation(
            exc,
            constraint_messages=_UNIQUE_VIOLATION_MESSAGES,
            generic=_GENERIC_UNIQUE_VIOLATION,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        detail = _FK_VIOLATION_MESSAGES.get(exc.constraint_name, GENERIC_FK_VIOLATION)
        raise HTTPException(status_code=422, detail=detail)
    except asyncpg.CheckViolationError as exc:
        detail = _CHECK_VIOLATION_MESSAGES.get(
            exc.constraint_name, f"{GENERIC_CHECK_VIOLATION} biosample"
        )
        raise HTTPException(status_code=422, detail=detail)
    except asyncpg.DeadlockDetectedError:
        raise_transient_retry(
            "a concurrent edit of one of this study's fields interrupted the"
            " import; nothing was stored — resubmit the identical request"
        )
    except TimeoutError, asyncpg.QueryCanceledError:
        # Each biosample INSERT takes a per-owner lock held until commit, so an
        # import waits behind any other open import for the same owner -- a bulk
        # batch holds it for its whole run. Waiting past the pool's command
        # timeout lands here: nothing was written, and it clears once the other
        # import commits.
        raise_transient_retry(
            "another import for this biosample owner held a lock this one needed"
            " for too long; nothing was stored — resubmit the identical request"
        )


@contextlib.asynccontextmanager
async def _named_row_errors(i: int, row: BiosampleImportRequest, study_idx: int):
    """Name the bulk row a failure came from. A transient 503 (a deadlock, or a
    lock wait that timed out) is the batch's, not the row's: it keeps its
    Retry-After and gets the batch's own wording. Any other HTTPException gets
    the row prefix; an unmapped exception stays a 500 with its traceback and a
    note naming the row."""
    try:
        yield
    except HTTPException as exc:
        if exc.status_code == 503:
            raise HTTPException(
                status_code=503,
                detail=(
                    "the batch waited on another import or edit on this study or"
                    " for this owner and was interrupted; nothing was stored —"
                    " resubmit the identical request"
                ),
                headers=exc.headers,
            ) from exc
        raise HTTPException(
            status_code=exc.status_code,
            detail=(
                f"row {i} (counting from 0; owner_biosample_id_value="
                f"{row.owner_biosample_id_value!r}): {exc.detail}"
            ),
            headers=exc.headers,
        ) from exc
    except Exception as exc:
        exc.add_note(
            f"bulk biosample import, study {study_idx}: failed at row {i}"
            f" (counting from 0; owner_biosample_id_value={row.owner_biosample_id_value!r})"
        )
        raise


@router.post(PATH_BIOSAMPLE_BULK_BY_STUDY, status_code=201)
async def import_biosamples_bulk(
    study_idx: Annotated[int, Field(gt=0)],
    body: BiosampleBulkImportRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> BiosampleBulkImportResponse:
    """Create many biosamples on a study in ONE transaction — the wetlab front door.

    A sheet's biosamples in a single call instead of N. Same per-row contract,
    guards (Tier.ADMIN on the study, or wet_lab_admin+), and error mapping as the
    single import — each row runs through the shared `_import_one_biosample`. The
    batch is ALL-OR-NOTHING: a failing row rolls back every row, and the error
    names the failing row (counting from 0) and its owner_biosample_id_value so
    the caller can fix the sheet and resubmit. The request model enforces one
    owner and one id field per batch, and the size caps.
    """
    # Two rows naming the same owner id would collide on the id field's unique
    # index mid-write, and the error would blame "another biosample" that is in
    # fact this request. Refuse before writing, naming both rows.
    first_row_for: dict[str, int] = {}
    for i, row in enumerate(body.rows):
        j = first_row_for.setdefault(row.owner_biosample_id_value, i)
        if j != i:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"rows {j} and {i} (counting from 0) both give owner_biosample_id_value"
                    f" {row.owner_biosample_id_value!r}; each biosample needs its own"
                ),
            )

    async with tx() as conn:
        await _require_eligible_import_owner(conn, body.rows[0].owner_idx)
        checklist_idx_for: dict[str | None, int | None] = {}
        results: list[BiosampleImportResponse] = []
        deferred: list[tuple[int, Sequence[ResolvedFieldValue]]] = []
        # Phase 1, per row: create, link, write the owner id, and validate the
        # metadata -- but defer writing it, so the batch can write all of it at once.
        for i, row in enumerate(body.rows):
            async with _named_row_errors(i, row, study_idx):
                name = row.metadata_checklist_name
                if name not in checklist_idx_for:
                    checklist_idx_for[name] = await resolve_metadata_checklist_idx(conn, name)
                response, resolved = await _import_one_biosample(
                    conn,
                    study_idx=study_idx,
                    body=row,
                    caller_idx=user.principal_idx,
                    metadata_checklist_idx=checklist_idx_for[name],
                    defer_metadata_write=True,
                )
                results.append(response)
                deferred.append((response.biosample_idx, resolved or ()))
        # Phase 2: every row's metadata in a few statements. A rejected batch
        # rolls back to before it and is written again row by row through the
        # per-value path, which raises the same diagnosed error, for the same
        # row, as an import that never batched.
        # The batch write re-raises a transient DB error (deadlock, serialization,
        # lock-wait timeout) rather than returning False; map it to the same
        # retryable 503 the single import and phase 1 send, not a 500. A
        # non-transient failure still returns False and falls through to the
        # row-by-row replay below. The batch is not one row, so it maps against the
        # first -- only the transient arms, which read no row fields, can fire here
        # (every mappable non-transient returns False instead of raising).
        written = await _with_import_error_mapping(
            conn,
            body.rows[0],
            lambda: insert_new_entities_metadata_batch(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                study_idx=study_idx,
                caller_idx=user.principal_idx,
                entries=deferred,
            ),
        )
        if not written:
            for i, (row, (biosample_idx, resolved)) in enumerate(
                zip(body.rows, deferred, strict=True)
            ):
                async with _named_row_errors(i, row, study_idx):
                    await _with_import_error_mapping(
                        conn,
                        row,
                        lambda biosample_idx=biosample_idx, resolved=resolved: (
                            write_resolved_metadata_entries(
                                conn,
                                spec=BIOSAMPLE_METADATA_SPEC,
                                entity_idx=biosample_idx,
                                study_idx=study_idx,
                                caller_idx=user.principal_idx,
                                resolved_metadata=resolved,
                            )
                        ),
                    )
    return BiosampleBulkImportResponse(results=results)


@router.post(PATH_BIOSAMPLE_STUDY_FIELD_BY_STUDY, status_code=201)
async def create_biosample_field(
    study_idx: Annotated[int, Field(gt=0)],
    body: BiosampleStudyFieldCreateRequest,
    response: Response,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> BiosampleStudyFieldResponse:
    """Create a study-local biosample field definition (no metadata value).

    The caller must be a HumanUser with profile_complete=True, hold the
    biosample:write scope, and have `Tier.ADMIN` access (or higher) to the
    path's study — study owner, an ADMIN study_access row, or wet_lab_admin /
    system_admin (role bypass). `require_study_exists` composes alongside
    `require_study_access` so role-bypass callers still get 404 on a
    non-existent study_idx. A field of that name already on the study is a 409;
    the response body is the created field.

    Access policy is interim: the ADMIN gate is a coarse stand-in matching the
    study-scoped biosample metadata routes, held until per-field
    visibility-tier enforcement lands. When that clamp comes off, this route
    goes back to `Tier.MEMBER` — minting a study-local field is work a study
    member is meant to do.

    biosample_global_field_idx discriminates two mutually-exclusive modes.
    Purely-local (omitted): data_type is required, plus optional required /
    terminology_idx / tier_override / unique_in_study. Globally-linked (set):
    only display_name (+ optional description), every other attribute omitted;
    data_type / required / terminology_idx come back on the response resolved
    to the global field's values.

    unique_in_study makes the study's values through this field distinct and
    forbids a missing-value marker among them; the shapes that may carry it,
    and why, are stated by unique_in_study_rejection_reason. Anything it
    refuses is a 422.

    The response carries an `ETag` header derived from the new row's
    `updated_at`, so a caller that mints a field holds the value an edit's
    `If-Match` needs without a second round trip.
    """
    async with tx() as conn:
        created = await create_and_map_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            body=body,
            caller_idx=user.principal_idx,
            response_model=BiosampleStudyFieldResponse,
        )

    response.headers[ETAG_HEADER] = etag_for_updated_at(created.updated_at)
    return created


# same-pattern-ok: cross-entity twin of list_prep_sample_fields_in_study.
# The decorator, path constant, scope, tier, spec, and response model are
# the whole per-entity declaration, over a mapper that's already generic.
@router.get(PATH_BIOSAMPLE_STUDY_FIELD_BY_STUDY)
async def list_biosample_fields_in_study(
    study_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.VIEWER, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> list[BiosampleStudyFieldResponse]:
    """List the biosample field definitions on the path's study, by display_name.

    Caller must be a HumanUser holding Scope.BIOSAMPLE_READ with viewer tier or
    higher on the study (wet_lab_admin and system_admin bypass tier).
    require_study_exists composes alongside require_study_access so an
    admin-bypass caller still gets 404 on a non-existent study rather than an
    empty list. Viewer tier suffices because this returns field definitions
    and no metadata values. Returns both globally-linked and purely-local fields;
    a linked field's data_type, required, and terminology_idx arrive resolved
    from its global field.
    """
    rows = await fetch_study_fields_for_study(
        pool, spec=BIOSAMPLE_METADATA_SPEC, study_idx=study_idx
    )
    fields = [
        map_study_field_row(
            row, spec=BIOSAMPLE_METADATA_SPEC, response_model=BiosampleStudyFieldResponse
        )
        for row in rows
    ]
    return fields


# same-pattern-ok: cross-entity twin of get_prep_sample_field. The decorator,
# path constant, scope, tier, spec, and response model are the whole per-entity
# declaration, over a shared reader that carries the contract.
@router.get(PATH_BIOSAMPLE_STUDY_FIELD_BY_IDX)
async def get_biosample_field(
    study_idx: Annotated[int, Field(gt=0)],
    study_field_idx: Annotated[int, Field(gt=0)],
    response: Response,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.VIEWER, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> BiosampleStudyFieldResponse:
    """Return one study-local biosample field definition.

    Same access bar as the list route on this study: viewer tier suffices
    because this returns a field definition and no metadata value. A field
    absent, or belonging to another study, is a 404 either way.

    The response carries an `ETag` header derived from the row's `updated_at`,
    which is the value an edit's `If-Match` must carry; it is a quoted ISO 8601
    timestamp and is opaque by contract.
    """
    async with pool.acquire() as conn:
        field, updated_at = await read_and_map_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            study_field_idx=study_field_idx,
            response_model=BiosampleStudyFieldResponse,
        )

    response.headers[ETAG_HEADER] = etag_for_updated_at(updated_at)
    return field


# same-pattern-ok: cross-entity twin of list_prep_sample_global_fields, and the
# study-free sibling of list_biosample_fields_in_study; same reason as that one —
# the gate and the spec/model pair are the declaration, over an already-generic read.
@global_field_router.get(PATH_BIOSAMPLE_GLOBAL_FIELD_ROOT)
async def list_biosample_global_fields(
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
) -> list[BiosampleGlobalFieldResponse]:
    """List the global biosample field registry, by internal_name.

    Caller must be a HumanUser holding Scope.BIOSAMPLE_READ. The registry is
    global: a caller with no study grants at all still gets the full list, and
    only read scope is needed because a global field is a definition,
    carrying no metadata value and no study's data.
    """
    rows = await fetch_global_fields(pool, spec=BIOSAMPLE_METADATA_SPEC)
    fields = [
        map_global_field_row(row, response_model=BiosampleGlobalFieldResponse) for row in rows
    ]
    return fields


# Hard cap on the bulk-id read. Sized to comfortably cover any single
# study's biosample roster while bounding per-response payload size.
# The sequencing-run roster cap happens to share this numeric value, but
# the two bound conceptually distinct rosters and are sized independently;
# they are intentionally not factored into a shared constant.
_BIOSAMPLE_IDXS_HARD_CAP = 500_000


@router.get(PATH_BIOSAMPLE_LIST_BY_STUDY)
async def list_biosample_idxs_in_study(
    study_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.STUDY_READ)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.VIEWER, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> IdxsListResponse:
    """List biosample idxs linked to the path's study, newest-linked first.

    Caller must be a HumanUser with Scope.STUDY_READ; access to the
    path's study_idx requires viewer tier or higher (wet_lab_admin and
    system_admin bypass tier). require_study_exists composes alongside
    require_study_access so admin-bypass callers still get 404 on a
    non-existent study_idx rather than a silent empty list. Excludes
    retired biosample_to_study links and retired biosamples
    unconditionally. The `truncated` flag indicates the underlying set
    exceeded the hard cap; callers hitting it should narrow their
    scope.
    """
    # Fetch cap+1 rows so a count strictly greater than the cap signals
    # truncation; build_idxs_list_response slices back to the cap.
    rows = await fetch_biosample_idxs_for_study(
        pool, study_idx=study_idx, limit=_BIOSAMPLE_IDXS_HARD_CAP + 1
    )
    return build_idxs_list_response(
        rows, cap=_BIOSAMPLE_IDXS_HARD_CAP, caller_system_role=user.system_role
    )


async def _read_study_scoped_biosample(
    conn: asyncpg.Connection,
    *,
    study_idx: int,
    biosample_idx: int,
    response: Response,
    caller_system_role: SystemRole,
    not_found_detail: str | None = None,
) -> StudyScopedBiosampleResponse:
    """Read one study's view of a biosample and stamp the response's ETag.

    Gates on the study link and retirement, reads both metadata scopes, and
    shapes the result. The caller supplies an open connection so the whole read
    lands in one snapshot, and must already have resolved biosample_idx by
    whatever means its own surface offers. A surface that resolved it from
    something other than an idx passes not_found_detail to word the gate's 404
    in those same terms.
    """
    row, global_metadata, local_metadata = await read_study_scoped_entity(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        fetch_row=fetch_biosample,
        entity_idx=biosample_idx,
        metadata_idx_column="idx",
        study_idx=study_idx,
        noun="biosample",
        not_found_detail=not_found_detail,
    )

    # Set the ETag header so callers can use it as the If-Match value on a
    # subsequent PATCH; the value is opaque-by-contract.
    response.headers[ETAG_HEADER] = etag_for_updated_at(row["updated_at"])

    return _study_scoped_response_from_row(
        row,
        global_metadata=global_metadata,
        local_metadata=local_metadata,
        caller_system_role=caller_system_role,
    )


@router.post(PATH_BIOSAMPLE_BY_STUDY_UNIQUE_FIELD)
async def lookup_biosample_in_study_by_unique_field(
    study_idx: Annotated[int, Field(gt=0)],
    body: SampleUniqueFieldRef,
    response: Response,
    snapshot: TxConnFactory = Depends(get_snapshot_conn_factory),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> StudyScopedBiosampleResponse:
    """Return one study's view of the biosample the study's own name identifies.

    The same response as the by-idx read, for a caller holding a
    unique-in-study field's display_name and the value it carries rather than a
    biosample_idx. POST rather than GET because that value can be the owner's
    own name for the sample, which is restricted and sometimes carries PII; the
    body keeps it out of URLs and access logs.

    The named field must exist on the study and must declare unique_in_study
    (otherwise 422, since its values could name several samples). A well-formed
    pair naming no sample is 404.

    Access and the remaining refusals are those of the by-idx read: Tier.ADMIN
    study access with a wet_lab_admin+ role bypass as an interim stand-in until
    per-field visibility-tier enforcement lands, and a retired biosample or
    retired link answering 404.
    """
    # One REPEATABLE READ snapshot spanning resolution and the read, so the
    # value that resolved the idx and the row read back cannot straddle a
    # concurrent writer's commit.
    async with snapshot() as conn:
        biosample_idx = await resolve_study_entity_by_unique_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            display_name=body.unique_field_display_name,
            value=body.unique_field_value,
            noun="biosample",
        )
        return await _read_study_scoped_biosample(
            conn,
            study_idx=study_idx,
            biosample_idx=biosample_idx,
            response=response,
            caller_system_role=user.system_role,
            not_found_detail=detail_for_unique_field_miss(
                noun="biosample",
                study_idx=study_idx,
                display_name=body.unique_field_display_name,
                value=body.unique_field_value,
            ),
        )


@router.get(PATH_BIOSAMPLE_BY_STUDY_AND_IDX)
async def get_biosample_in_study(
    study_idx: Annotated[int, Field(gt=0)],
    biosample_idx: Annotated[int, Field(gt=0)],
    response: Response,
    snapshot: TxConnFactory = Depends(get_snapshot_conn_factory),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> StudyScopedBiosampleResponse:
    """Return one study's view of a biosample: the core row, its globally-linked
    metadata, and this study's purely-local metadata.

    Access policy is interim: gated at Tier.ADMIN study access with a
    wet_lab_admin+ role bypass -- a coarse stand-in until per-field
    visibility-tier enforcement lands. 401 on Anonymous, 403 on missing scope or
    sub-ADMIN tier, 404 on a study that does not exist. require_study_exists
    composes alongside require_study_access so an admin-bypass caller still gets
    404 on a non-existent study.

    The biosample must be linked to this study: a nonexistent biosample_idx and
    one with no non-retired biosample_to_study link to study_idx share the same
    404 so a caller never learns the state of a biosample outside their study;
    retirement is evaluated only after the link passes, and a retired biosample
    is likewise 404 (mirroring the biosample-level read's retired carve-out).

    local_metadata includes the owner-biosample-id row, and every caller the
    gate above admits sees it: an ADMIN-tier study_access row, the study's
    owner, or a wet_lab_admin+ role, which returns before the tier check runs.
    The field is pinned to member tier in the schema, but nothing enforces that
    pin yet, so the gate is what restricts the value. This route does not write it.

    The response carries an `ETag` header derived from the row's `updated_at`
    column; the value is a quoted ISO 8601 timestamp and is opaque by contract.
    """
    # One REPEATABLE READ snapshot so the row read, the link check, and both
    # metadata reads cannot disagree about a concurrent writer's commit.
    async with snapshot() as conn:
        return await _read_study_scoped_biosample(
            conn,
            study_idx=study_idx,
            biosample_idx=biosample_idx,
            response=response,
            caller_system_role=user.system_role,
        )


# Keep this ahead of the {biosample_idx} form below: both are PATCH on the same
# path shape, and the literal segment is unreachable once the parameterized one
# is registered first.
@router.patch(PATH_BIOSAMPLE_METADATA_BY_STUDY_UNIQUE_FIELD)
async def patch_biosample_metadata_by_unique_field(
    study_idx: Annotated[int, Field(gt=0)],
    body: SampleMetadataWriteByUniqueFieldRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> BiosampleMetadataWriteByUniqueFieldResponse:
    """Upsert this study's metadata on the biosample the study's own id names.

    The by-idx metadata write, for a caller holding a unique-in-study field's
    display_name and the value it carries instead of a biosample_idx, so a
    read-modify-write never has to handle one. The response adds the
    biosample_idx the pair resolved to.

    The identifying field must exist on the study and must declare
    unique_in_study -- without it the value could name several samples, so the
    write is refused (422) rather than applied to an arbitrary one. A
    well-formed pair naming no sample is 404.

    The identifying field may appear in the metadata body, but only carrying
    the value it already holds, which changes nothing. Offering a different one
    is a 422: a value stored through a unique_in_study field is what names the
    sample, and the write that would hand that name to another sample is
    refused. The owner-biosample-id field is a 422 either way, being written
    only through its own surface.

    Access and the remaining refusals are the by-idx write's: Tier.ADMIN study
    access with a wet_lab_admin+ role bypass as an interim stand-in until
    per-field visibility-tier enforcement lands, a retired biosample answering
    409, and a retired study link answering 404.

    There is NO If-Match on this route, exactly as on the by-idx write: a
    concurrent same-study, same-field write is last-writer-wins. Within one
    request the pair cannot go stale: resolution and the write share a
    transaction, and no API path moves a stored value from one biosample to
    another. Across requests it can: a field edit may rename a field, so a
    display_name a caller still holds could resolve to a different field, or to
    none.
    """
    async with tx() as conn:
        biosample_idx = await resolve_study_entity_by_unique_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            display_name=body.unique_field_display_name,
            value=body.unique_field_value,
            noun="biosample",
        )
        written = await resolve_and_write_study_scoped_metadata(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            fetch_row=fetch_biosample,
            entity_idx=biosample_idx,
            metadata_idx_column="idx",
            study_idx=study_idx,
            noun="biosample",
            metadata=body.metadata,
            caller_idx=user.principal_idx,
            global_internal_names=body.global_internal_names,
            unlinked_detail=detail_for_unique_field_miss(
                noun="biosample",
                study_idx=study_idx,
                display_name=body.unique_field_display_name,
                value=body.unique_field_value,
            ),
            retired_detail=detail_for_retired_unique_field_entity(
                noun="biosample",
                study_idx=study_idx,
                display_name=body.unique_field_display_name,
                value=body.unique_field_value,
            ),
        )
    return BiosampleMetadataWriteByUniqueFieldResponse(
        results=written.results, biosample_idx=biosample_idx
    )


@router.patch(PATH_BIOSAMPLE_METADATA_BY_STUDY)
async def patch_biosample_metadata(
    study_idx: Annotated[int, Field(gt=0)],
    biosample_idx: Annotated[int, Field(gt=0)],
    body: SampleMetadataWriteRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> SampleMetadataWriteResponse:
    """Upsert this study's metadata values on a biosample.

    Access policy is interim: gated at Tier.ADMIN study access with a
    wet_lab_admin+ role bypass -- a coarse stand-in until per-field
    visibility-tier enforcement lands. 401 on Anonymous, 403 on missing scope,
    an incomplete profile, or sub-ADMIN tier, 404 on a study that does not
    exist. require_study_exists composes alongside require_study_access so an
    admin-bypass caller still gets 404 on a non-existent study.

    The biosample must be linked to this study: a nonexistent biosample_idx and
    one with no non-retired biosample_to_study link to study_idx share the same
    404 so a caller never learns the state of a biosample outside their study; a
    retired biosample is a 409 (its metadata cannot be written), checked only
    after the link passes. A link retired between that check and the write is
    refused by the database and answers the same 404, so the status does not
    depend on which of the two noticed.

    The body maps field display_name -> text value -- or global internal_name ->
    text value when it sets global_internal_names, which leaves study-local
    fields display-name-keyed. Each key resolves against the study's existing
    global or study-local fields and is upserted (unknown fields -> 422, an empty
    body -> 422 at the wire boundary, the owner-biosample-id field -> 422 since
    it is changed only through its own surface, a cross-study global slot
    collision -> 409). The response reports per field, in input order, whether it
    resolved global or local, the write outcome (inserted / updated /
    unchanged), the value now in the slot, and the internal_name a globally
    linked value reads back under.

    There is NO If-Match on this route: a concurrent same-study, same-field
    write is last-writer-wins (a silent lost update). The per-slot upsert is
    race-safe against cross-study collisions (409) but does not serialize
    same-study rewrites; callers needing lost-update protection coordinate out
    of band.
    """
    async with tx() as conn:
        written = await resolve_and_write_study_scoped_metadata(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            fetch_row=fetch_biosample,
            entity_idx=biosample_idx,
            metadata_idx_column="idx",
            study_idx=study_idx,
            noun="biosample",
            metadata=body.metadata,
            caller_idx=user.principal_idx,
            global_internal_names=body.global_internal_names,
        )
    return written


# Roles that may bypass the per-biosample owner / linked-study-access check.
# A bypass-role caller still gets the standard 404 on a missing or retired
# biosample (see the docstring on get_biosample for the retired-row
# carve-out planned for a future change).
_BIOSAMPLE_GET_BYPASS_ROLE: SystemRole = SystemRole.WET_LAB_ADMIN


def _biosample_core_row_dict(row: asyncpg.Record) -> dict[str, object]:
    """Map a qiita.biosample row's columns to BiosampleResponse field names.

    Centralises the column -> field mapping (the idx -> biosample_idx rename
    aside, every key matches its column). Excludes the metadata dicts and
    caller_system_role. Runs no DB queries.
    """
    return {
        "biosample_idx": row["idx"],
        "owner_idx": row["owner_idx"],
        "metadata_checklist": MetadataChecklistRef.from_row(
            row["metadata_checklist_idx"], row["metadata_checklist_name"]
        ),
        "biosample_accession": row["biosample_accession"],
        "ena_sample_accession": row["ena_sample_accession"],
        "matrix_tube_id": row["matrix_tube_id"],
        "last_submission_at": row["last_submission_at"],
        "submission_error": row["submission_error"],
        "last_metadata_change_at": row["last_metadata_change_at"],
        "created_by_idx": row["created_by_idx"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "retired": row["retired"],
        "retired_by_idx": row["retired_by_idx"],
        "retired_at": row["retired_at"],
        "retire_reason": row["retire_reason"],
    }


def _biosample_response_from_row(
    row: asyncpg.Record,
    *,
    global_metadata: dict[str, MetadataEntry],
    caller_system_role: SystemRole,
) -> BiosampleResponse:
    """Shape a qiita.biosample row + decoded global metadata into BiosampleResponse.

    Reuses the shared core-column mapping; the global_metadata dict is supplied
    by the caller -- this helper runs no DB queries.
    """
    payload = _biosample_core_row_dict(row) | {
        "global_metadata": global_metadata,
        "caller_system_role": caller_system_role,
    }
    return BiosampleResponse.model_validate(payload)


def _study_scoped_response_from_row(
    row: asyncpg.Record,
    *,
    global_metadata: dict[str, MetadataEntry],
    local_metadata: dict[str, MetadataEntry],
    caller_system_role: SystemRole,
) -> StudyScopedBiosampleResponse:
    """Shape a biosample row + its global and study-local metadata into
    StudyScopedBiosampleResponse.

    Reuses the shared core-column mapping and adds this study's purely-local
    metadata (keyed by display_name) alongside the global map. Both metadata
    dicts are supplied by the caller -- this helper runs no DB queries.
    """
    payload = _biosample_core_row_dict(row) | {
        "global_metadata": global_metadata,
        "local_metadata": local_metadata,
        "caller_system_role": caller_system_role,
    }
    return StudyScopedBiosampleResponse.model_validate(payload)


@biosample_router.get(PATH_BIOSAMPLE_BY_IDX)
async def get_biosample(
    biosample_idx: Annotated[int, Field(gt=0)],
    response: Response,
    snapshot: TxConnFactory = Depends(get_snapshot_conn_factory),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
) -> BiosampleResponse:
    """Return the qiita.biosample row plus its globally-linked metadata.

    Access policy: any wet_lab_admin or higher passes; otherwise the
    caller must be the biosample's owner OR have a qiita.study_access
    row on a non-retired biosample_to_study link (the
    fetch_caller_has_biosample_access predicate). 401 on Anonymous, 403
    on missing scope or no read path, 404 on a missing biosample.

    Retired biosamples currently 404 unconditionally. A future change
    will let wet_lab_admin and system_admin retrieve retired rows so
    the audit-trail surface is reachable from the API; until then the
    404 keeps callers (including admins) from seeing partially-revoked
    rows by accident.

    The response carries an `ETag` header derived from the row's
    `updated_at` column. The format is a quoted ISO 8601 timestamp;
    clients must treat it as opaque.
    """
    # All reads share one REPEATABLE READ snapshot so the supertype row,
    # the access predicate, and the metadata read cannot disagree about a
    # concurrent writer's commit.
    async with snapshot() as conn:
        # Fetch the row first so 404 fires before the access predicate runs;
        # the predicate is defined for any biosample_idx but emitting 404 here
        # avoids a confusing "no access" 403 on a row that does not exist.
        row = await fetch_biosample(conn, biosample_idx)
        if row is None:
            raise HTTPException(status_code=404, detail=f"biosample {biosample_idx} not found")

        # Retired-row carve-out (see docstring): treat as not found until the
        # planned wet-lab+ retired-retrieval surface lands. Applied uniformly
        # across roles so the 404 contract is unconditional in the meantime.
        if row["retired"]:
            raise HTTPException(status_code=404, detail=f"biosample {biosample_idx} not found")

        # Role bypass for wet_lab_admin and higher; everyone else must satisfy
        # the owner-or-linked-study-access predicate.
        authorized = user.has_role_at_least(
            _BIOSAMPLE_GET_BYPASS_ROLE
        ) or await fetch_caller_has_biosample_access(
            conn,
            principal_idx=user.principal_idx,
            biosample_idx=biosample_idx,
        )
        if not authorized:
            raise HTTPException(
                status_code=403,
                detail=f"caller has no read path to biosample {biosample_idx}",
            )

        # Pull the globally-linked metadata once access has been resolved; the
        # repo function handles the global_field_idx IS NOT NULL filter and the
        # data_type-driven value column dispatch.
        metadata_rows = await fetch_global_metadata(
            conn, spec=BIOSAMPLE_METADATA_SPEC, entity_idx=biosample_idx
        )

    global_metadata = metadata_entries_from_rows(metadata_rows)

    # Set the ETag header so callers can use it as the If-Match value on a
    # subsequent PATCH; the value is opaque-by-contract.
    response.headers[ETAG_HEADER] = etag_for_updated_at(row["updated_at"])

    return _biosample_response_from_row(
        row,
        global_metadata=global_metadata,
        caller_system_role=user.system_role,
    )


def _biosample_natural_key_fetcher(
    pool: asyncpg.Pool, key: BiosampleLookupKey
) -> Callable[[list[str]], Awaitable[dict[str, int]]]:
    """Return a single-argument awaitable that resolves a list of
    natural-key values to a `{value: biosample_idx}` map."""
    return lambda values: fetch_biosample_idxs_by_natural_key(pool, key=key, values=values)


@biosample_router.post(PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION)
async def lookup_biosample_by_accession(
    body: BiosampleLookupByAccessionRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
) -> BiosampleLookupByAccessionResponse:
    """Resolve a list of biosample accession values to biosample_idx, keyed
    on the column named by `body.accession_field` (default
    biosample_accession).

    POST (not GET) because a typical bcl-convert pool carries up to 384
    accessions; threaded through query-params that would exceed nginx's
    default 8 KB request-line cap. Body has no such cap.

    Auth: HumanUser with Scope.BIOSAMPLE_READ. No per-row access predicate
    runs — the response carries only the (accession, idx) mapping with no
    biosample columns, so a caller who sees the idx still cannot read the
    row without satisfying GET /biosample/{idx}'s tier+access checks.
    This keeps the bcl-convert flow from needing wet_lab_admin+ for a
    pool whose samples span studies the caller isn't a member of.

    Retired biosamples are excluded from `resolved` (and therefore listed
    in `missing`) because the find-or-create chain the CLI uses afterwards
    would refuse to FK a fresh prep_sample to a retired biosample.

    Input deduplication: accessions appearing twice in the request are
    deduped before the SQL fetch; `missing` echoes back the input-order
    deduped list of accessions that did not resolve.
    """
    # _user is read only to keep the dependency chain explicit — no
    # per-caller filter runs here (see auth docstring).
    _ = user
    resolved, missing = await resolve_idxs_by_natural_key(
        values=body.accessions,
        fetcher=_biosample_natural_key_fetcher(pool, body.accession_field),
    )
    return BiosampleLookupByAccessionResponse(resolved=resolved, missing=missing)


@biosample_router.post(PATH_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID)
async def lookup_biosample_by_matrix_tube_id(
    body: BiosampleLookupByMatrixTubeIdRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
) -> BiosampleLookupByMatrixTubeIdResponse:
    """Resolve a list of matrix_tube_id values to biosample_idx.

    Mirrors the accession variant in every way except the keyed column.
    Auth, access-predicate-skip rationale, retired-row exclusion, and
    input-dedup behavior are identical; see lookup_biosample_by_accession
    for the full rationale.
    """
    _ = user
    resolved, missing = await resolve_idxs_by_natural_key(
        values=body.matrix_tube_ids,
        fetcher=_biosample_natural_key_fetcher(pool, "matrix_tube_id"),
    )
    return BiosampleLookupByMatrixTubeIdResponse(resolved=resolved, missing=missing)


@biosample_router.post(
    PATH_BIOSAMPLE_RESOLVE_ROSTER,
    responses={422: {"description": "Rows that did not resolve; detail is a RosterResolveFailure"}},
)
async def resolve_biosample_roster(
    body: RosterResolveRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_READ)),
    _study_scope: Principal = Depends(require_scope(Scope.STUDY_READ)),
) -> RosterResolveResponse:
    """Resolve a pool roster to biosample_idx + study links, by matrix tube or
    accession, in one round trip.

    The rules live in `roster_resolution`; this route gathers the facts and
    maps the outcome. Any unresolved row refuses the whole roster with a 422
    whose `detail` is a `RosterResolveFailure` naming every problem, so a
    submitter fixes the sheet once rather than one miss per attempt.

    Auth: `biosample:read` and `study:read`, since the answer names both. No
    per-row access predicate runs, as in lookup_biosample_by_accession: the
    response carries only idxs, and creating a prep_sample on a resolved study
    still passes that route's own study-access check. The facts are read in one
    read-only snapshot, so a concurrent retire cannot mix two states.
    """
    _ = user
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        facts = await fetch_roster_facts(conn, body.rows)
    resolved, problems = classify_roster(body.rows, facts)
    if problems:
        unresolved = len({p.item_id for p in problems})
        failure = RosterResolveFailure(
            message=f"{unresolved} of {len(body.rows)} roster rows did not resolve",
            problems=problems,
        )
        raise HTTPException(status_code=422, detail=failure.model_dump(mode="json"))
    return RosterResolveResponse(rows=resolved)


# Substring of the asyncpg.RaiseError message thrown by the role-typed FK
# trigger on biosample.owner_idx. The trigger fires before the underlying
# FK constraint, so a non-user owner_idx surfaces as RaiseError; the route
# maps it to the same eligibility-422 the preflight emits. The marker text
# is pinned to the RAISE EXCEPTION format string in
# db/migrations/20260501000013_role_typed_fk_triggers.sql -- if either
# side changes, update both in lockstep.
_OWNER_TRIGGER_RAISE_MARKER = "user-kind principal"


@biosample_router.patch(PATH_BIOSAMPLE_BY_IDX)
async def patch_biosample(
    biosample_idx: Annotated[int, Field(gt=0)],
    body: BiosamplePatchRequest,
    response: Response,
    if_match: Annotated[str | None, Header(alias=IF_MATCH_HEADER)] = None,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
) -> BiosampleResponse:
    """Edit a biosample's core record.

    Auth bar: caller holds Scope.BIOSAMPLE_WRITE and is a Principal at
    system_role >= wet_lab_admin. The route's intended audience includes
    the NCBI / ENA submission subsystem (a service account writing back
    accessions), but require_role_at_least currently rejects every
    ServiceAccount because the auth model treats service-account authz
    as scope-only and ServiceAccount carries no system_role field. A
    wider auth-model change (so ServiceAccount carries a role) is
    required before that path opens; until then the runtime caller set
    is humans-only despite the Principal type.

    If-Match is required: missing -> 428, mismatch -> 412. The body's
    editable fields are validated by BiosamplePatchRequest (extra=forbid
    rejects immutable / retirement columns with 422; an empty body is
    also 422). Inside one connection-scoped transaction the route runs
    a `SELECT ... FOR UPDATE` preflight on the row (existence -> 404,
    retirement -> 409, ETag -> 412); validates the candidate owner via
    require_eligible_owner when owner_idx is in the body (422 on
    ineligibility); applies the UPDATE; re-reads global metadata for
    the response. The FOR UPDATE lock is held from preflight through
    commit, so concurrent PATCHes on the same row serialize at the
    preflight: the second caller blocks until the first commits, then
    sees the post-commit `updated_at` and 412s on its now-stale
    If-Match header. This closes the lost-update window between the
    ETag check and the UPDATE that an unlocked preflight would leave
    open. Uniqueness violations on biosample_accession /
    ena_sample_accession map to 409, FK violations on
    metadata_checklist_idx to 422, and the role-typed FK trigger on
    owner_idx (a backstop the preflight should preempt in practice) to
    the same eligibility-422.

    The response carries an `ETag` header derived from the new row's
    `updated_at` column; format mirrors the GET endpoint's contract
    and is opaque to clients.
    """
    assert isinstance(caller, HumanUser), (
        "caller must be HumanUser pre-commit; shaper reads .system_role and "
        "would 500 after SELECT FOR UPDATE + UPDATE commits (see docstring)"
    )

    if_match = require_if_match(if_match)

    # Build the column-keyed write set from the model's set fields so the
    # repository sees only what the caller explicitly included; explicit
    # null vs. absent is distinguished by model_fields_set.
    fields = {name: getattr(body, name) for name in body.model_fields_set}

    async with tx() as conn:
        try:
            # Preflight: existence -> 404, retirement -> 409, ETag -> 412.
            # for_update=True acquires a row-level lock for the rest of
            # the transaction so a concurrent PATCH on the same row
            # serializes here instead of racing through the ETag check
            # and silently overwriting the first writer's update.
            row = await fetch_biosample(conn, biosample_idx, for_update=True)
            # Retirement is biosample-specific; absent / stale-ETag is
            # the shared post-FOR-UPDATE preflight every PATCH runs.
            if row is not None and row["retired"]:
                raise HTTPException(status_code=409, detail=f"biosample {biosample_idx} is retired")
            require_etag_match(row, if_match=if_match, label="biosample", row_idx=biosample_idx)

            # Eligibility preflight runs only when ownership is being
            # transferred; collapses every ineligibility case to 422.
            if "owner_idx" in fields:
                await require_eligible_owner(
                    conn,
                    candidate_idx=fields["owner_idx"],
                    detail=_MSG_OWNER_NOT_ELIGIBLE,
                )

            # Translate the caller-facing checklist name into the idx
            # column update_biosample writes; an explicit null clears the
            # checklist, an unknown name -> 422 below.
            if "metadata_checklist_name" in fields:
                fields["metadata_checklist_idx"] = await resolve_metadata_checklist_idx(
                    conn, fields.pop("metadata_checklist_name")
                )

            # Apply the UPDATE; the repo function returns the post-UPDATE
            # row in the same shape fetch_biosample selects, so no follow-up
            # SELECT is needed. The FOR UPDATE preflight holds a row lock
            # for the rest of this transaction, so update_biosample cannot
            # return None here — the row is guaranteed to exist under our
            # lock. The defensive None check is kept as a backstop and
            # surfaces as the same 404 the preflight emits, so an invariant
            # violation (someone removing the lock without rethinking) fails
            # loudly rather than indexing into None.
            updated_row = await update_biosample(conn, biosample_idx, fields=fields)
            if updated_row is None:
                raise HTTPException(status_code=404, detail=f"biosample {biosample_idx} not found")

            # Re-read global metadata in the same transaction so the response
            # and the UPDATE see one consistent snapshot.
            metadata_rows = await fetch_global_metadata(
                conn, spec=BIOSAMPLE_METADATA_SPEC, entity_idx=biosample_idx
            )
        except asyncpg.UniqueViolationError as exc:
            raise_for_unique_violation(
                exc,
                constraint_messages=_UNIQUE_VIOLATION_MESSAGES,
                generic=_GENERIC_UNIQUE_VIOLATION,
            )
        except asyncpg.ForeignKeyViolationError as exc:
            detail = _FK_VIOLATION_MESSAGES.get(exc.constraint_name, GENERIC_FK_VIOLATION)
            raise HTTPException(status_code=422, detail=detail)
        except asyncpg.CheckViolationError as exc:
            detail = _CHECK_VIOLATION_MESSAGES.get(
                exc.constraint_name, f"{GENERIC_CHECK_VIOLATION} biosample"
            )
            raise HTTPException(status_code=422, detail=detail)
        except asyncpg.RaiseError as exc:
            # Role-typed FK trigger on biosample.owner_idx: candidate is
            # non-user. The preflight should have caught this; the trigger
            # is the schema-level backstop and the caller-facing surface is
            # the same 422 the preflight emits.
            if _OWNER_TRIGGER_RAISE_MARKER in str(exc):
                raise HTTPException(status_code=422, detail=_MSG_OWNER_NOT_ELIGIBLE)
            raise

    # Set the new ETag from the updated row's bumped updated_at.
    response.headers[ETAG_HEADER] = etag_for_updated_at(updated_row["updated_at"])

    # Reuse the GET route's row -> response shaper so the PATCH and GET
    # surfaces share one source of truth for the response shape.
    global_metadata = metadata_entries_from_rows(metadata_rows)
    return _biosample_response_from_row(
        updated_row,
        global_metadata=global_metadata,
        caller_system_role=caller.system_role,
    )


# same-pattern-ok: cross-entity twin of patch_prep_sample_field. The decorator, path
# constant, scope, spec, and response model are the whole per-entity
# declaration, over a shared helper that carries the contract.
@router.patch(PATH_BIOSAMPLE_STUDY_FIELD_BY_IDX)
async def patch_biosample_field(
    study_idx: Annotated[int, Field(gt=0)],
    study_field_idx: Annotated[int, Field(gt=0)],
    body: SampleStudyFieldPatchRequest,
    response: Response,
    if_match: Annotated[str | None, Header(alias=IF_MATCH_HEADER)] = None,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> BiosampleStudyFieldResponse:
    """Edit a study-local biosample field definition.

    Same access bar as the create route on this study. If-Match is required;
    patch_and_map_study_field carries the rest of the contract, including which
    attributes a globally-linked field refuses and what happens when a field's
    existing values cannot satisfy a uniqueness policy being switched on.

    The response carries an `ETag` header derived from the new row's
    `updated_at`, matching the create and read endpoints' contract.
    """
    async with tx() as conn:
        updated = await patch_and_map_study_field(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            study_idx=study_idx,
            study_field_idx=study_field_idx,
            body=body,
            if_match=if_match,
            response_model=BiosampleStudyFieldResponse,
        )

    response.headers[ETAG_HEADER] = etag_for_updated_at(updated.updated_at)
    return updated
