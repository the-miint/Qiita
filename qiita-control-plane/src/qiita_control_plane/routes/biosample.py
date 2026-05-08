"""Biosample routes.

Two routers live here. The study-scoped router (prefix=/study) carries the
single-biosample import (POST) and the study-scoped bulk-id read
(GET .../list-idxs). The biosample-scoped router (prefix=/biosample)
carries the single-resource read (GET /{biosample_idx}) and the
single-resource PATCH (PATCH /{biosample_idx}). Bulk-import,
retirement, search, and admin metadata-schema endpoints are deferred.
The write handler gates on caller scope, role, and study existence and
delegates the multi-table write to the repositories.biosample composer
inside one connection-scoped transaction; the study-scoped read gates
on caller scope and study access (with admin role bypass); the
single-biosample read gates on caller scope, then 404s on missing or
retired biosamples and gates non-admin callers on
owner-or-linked-study-access via the repository predicate; the PATCH
gates on caller scope and wet_lab_admin (or higher) role and applies
its mutation inside one connection-scoped transaction with required
If-Match optimistic-concurrency control.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Response
from pydantic import Field
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    BiosampleGlobalMetadataEntry,
    BiosampleIdxsListResponse,
    BiosampleImportRequest,
    BiosampleImportResponse,
    BiosamplePatchRequest,
    BiosampleResponse,
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
from ..deps import get_db_pool
from ..repositories.biosample import (
    fetch_biosample,
    fetch_biosample_idxs_for_study,
    fetch_caller_has_biosample_access,
    import_biosample_from_owner_biosample_id,
    update_biosample,
)
from ..repositories.biosample_metadata import (
    BiosampleMetadataParseError,
    BiosampleMetadataUnknownFieldsError,
    BiosampleOwnerIdFieldCollisionError,
    BiosampleStudyFieldConflictError,
    fetch_global_metadata_for_biosample,
)

router = APIRouter(prefix="/study", tags=["biosample"])
biosample_router = APIRouter(prefix="/biosample", tags=["biosample"])


_MSG_OWNER_NOT_ELIGIBLE = "owner is not eligible to own biosamples"

# Map of constraint names import_biosample_from_owner_biosample_id can trip
# (everything else is pre-flight-checked, swallowed by ON CONFLICT, or surfaces
# as a different exception class). Unknown names fall back to the generic
# strings on the matching exception path.
_UNIQUE_VIOLATION_MESSAGES: dict[str, str] = {
    "biosample_accession_unique": "biosample_accession already in use",
    "biosample_ena_sample_accession_unique": "ena_sample_accession already in use",
}
_FK_VIOLATION_MESSAGES: dict[str, str] = {
    "biosample_metadata_checklist_idx_fkey": (
        "metadata_checklist_idx does not reference an existing checklist"
    ),
}
_GENERIC_UNIQUE_VIOLATION = "conflicts with an existing biosample"
_GENERIC_FK_VIOLATION = "references a row that does not exist"


@router.post("/{study_idx}/biosample", status_code=201)
async def import_biosample(
    study_idx: Annotated[int, Field(gt=0)],
    body: BiosampleImportRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _exists: None = Depends(require_study_exists),
) -> BiosampleImportResponse:
    """Create a biosample on a study, atomically with its owner-provided id and metadata.

    Wraps the repositories.biosample composer in a single transaction. The
    caller must be a HumanUser with profile_complete=True, must hold the
    biosample:write scope, must be wet_lab_admin or higher, and the path's
    study_idx must exist.
    """
    # Owner eligibility pre-flight; collapses every ineligibility case to
    # one 422.
    await require_eligible_owner(
        pool,
        candidate_idx=body.owner_idx,
        detail=_MSG_OWNER_NOT_ELIGIBLE,
    )

    # Open the connection-scoped transaction the composer requires; map known
    # composer-side validation errors and DB-level violations to user-friendly
    # 422 / 409 responses. Composer-specific exceptions are caught first so
    # their detail wins over the generic asyncpg fallbacks.
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                result = await import_biosample_from_owner_biosample_id(
                    conn,
                    study_idx=study_idx,
                    owner_idx=body.owner_idx,
                    owner_biosample_id_field_name=body.owner_biosample_id_field_name,
                    owner_biosample_id_value=body.owner_biosample_id_value,
                    caller_idx=user.principal_idx,
                    metadata=body.metadata,
                    metadata_checklist_idx=body.metadata_checklist_idx,
                    biosample_accession=body.biosample_accession,
                    ena_sample_accession=body.ena_sample_accession,
                )
    except BiosampleOwnerIdFieldCollisionError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                f"metadata key {exc.display_name!r} collides with owner_biosample_id_field_name"
            ),
        )
    except BiosampleMetadataUnknownFieldsError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"unknown metadata fields: {', '.join(exc.unknown_display_names)}",
        )
    except BiosampleMetadataParseError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                f"could not parse metadata field {exc.display_name!r}"
                f" value {exc.text_value!r} as {exc.data_type}: {exc.reason}"
            ),
        )
    except BiosampleStudyFieldConflictError as exc:
        raise HTTPException(
            status_code=422,
            detail=(
                f"study has an existing field at display_name {exc.display_name!r}"
                " bound to a different global concept"
            ),
        )
    except asyncpg.UniqueViolationError as exc:
        detail = _UNIQUE_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_UNIQUE_VIOLATION)
        raise HTTPException(status_code=409, detail=detail)
    except asyncpg.ForeignKeyViolationError as exc:
        detail = _FK_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_FK_VIOLATION)
        raise HTTPException(status_code=422, detail=detail)

    return BiosampleImportResponse(
        biosample_idx=result.biosample_idx,
        biosample_study_field_idx=result.biosample_study_field_idx,
        biosample_study_field_created=result.biosample_study_field_created,
    )


# Hard cap on the bulk-id read. Sized to comfortably cover any single
# study's biosample roster while bounding per-response payload size.
_BIOSAMPLE_IDXS_HARD_CAP = 100_000


@router.get("/{study_idx}/biosample/list-idxs")
async def list_biosample_idxs_in_study(
    study_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.STUDY_READ)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.VIEWER, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> BiosampleIdxsListResponse:
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
    # truncation; the route slices back to the cap before returning.
    rows = await fetch_biosample_idxs_for_study(
        pool, study_idx=study_idx, limit=_BIOSAMPLE_IDXS_HARD_CAP + 1
    )
    truncated = len(rows) > _BIOSAMPLE_IDXS_HARD_CAP
    if truncated:
        rows = rows[:_BIOSAMPLE_IDXS_HARD_CAP]
    return BiosampleIdxsListResponse(
        biosample_idxs=rows,
        count=len(rows),
        truncated=truncated,
        caller_system_role=user.system_role,
    )


# Roles that may bypass the per-biosample owner / linked-study-access check.
# A bypass-role caller still gets the standard 404 on a missing or retired
# biosample (see the docstring on get_biosample_route for the retired-row
# carve-out planned for a future change).
_BIOSAMPLE_GET_BYPASS_ROLE: SystemRole = SystemRole.WET_LAB_ADMIN


def _biosample_response_from_row(
    row: asyncpg.Record,
    *,
    global_metadata: dict[str, BiosampleGlobalMetadataEntry],
    caller_system_role: SystemRole,
) -> BiosampleResponse:
    """Shape a qiita.biosample row + decoded global metadata into BiosampleResponse.

    Centralises the column -> field mapping (idx -> biosample_idx) so a future
    GET / PATCH route can reuse it. The global_metadata dict is supplied by
    the caller -- this helper does not run any DB queries.
    """
    return BiosampleResponse.model_validate(
        {
            "biosample_idx": row["idx"],
            "owner_idx": row["owner_idx"],
            "metadata_checklist_idx": row["metadata_checklist_idx"],
            "biosample_accession": row["biosample_accession"],
            "ena_sample_accession": row["ena_sample_accession"],
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
            "global_metadata": global_metadata,
            "caller_system_role": caller_system_role,
        }
    )


def _etag_for_updated_at(updated_at) -> str:
    """Build the quoted ETag header value from biosample.updated_at.

    Per project conventions the ETag is a quoted ISO 8601 representation of
    the row's updated_at column; clients treat the value as opaque and never
    parse it, so the timestamp's exact spelling is not part of the contract.
    """
    return f'"{updated_at.isoformat()}"'


@biosample_router.get("/{biosample_idx}")
async def get_biosample_route(
    biosample_idx: Annotated[int, Field(gt=0)],
    response: Response,
    pool: asyncpg.Pool = Depends(get_db_pool),
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
    # Fetch the row first so 404 fires before the access predicate runs;
    # the predicate is defined for any biosample_idx but emitting 404 here
    # avoids a confusing "no access" 403 on a row that does not exist.
    row = await fetch_biosample(pool, biosample_idx)
    if row is None:
        raise HTTPException(status_code=404, detail=f"biosample {biosample_idx} not found")

    # Retired-row carve-out (see docstring): treat as not found until the
    # planned wet-lab+ retired-retrieval surface lands. Applied uniformly
    # across roles so the 404 contract is unconditional in the meantime.
    if row["retired"]:
        raise HTTPException(status_code=404, detail=f"biosample {biosample_idx} not found")

    # Role bypass for wet_lab_admin and higher; everyone else must satisfy
    # the owner-or-linked-study-access predicate.
    if not user.has_role_at_least(_BIOSAMPLE_GET_BYPASS_ROLE):
        has_access = await fetch_caller_has_biosample_access(
            pool,
            principal_idx=user.principal_idx,
            biosample_idx=biosample_idx,
        )
        if not has_access:
            raise HTTPException(
                status_code=403,
                detail=f"caller has no read path to biosample {biosample_idx}",
            )

    # Pull the globally-linked metadata once access has been resolved; the
    # repo function handles the global_field_idx IS NOT NULL filter and the
    # data_type-driven value column dispatch.
    metadata_rows = await fetch_global_metadata_for_biosample(pool, biosample_idx)
    global_metadata = {
        internal_name: BiosampleGlobalMetadataEntry(
            display_name=entry.display_name,
            description=entry.description,
            data_type=entry.data_type,
            value=entry.value,
        )
        for internal_name, entry in metadata_rows.items()
    }

    # Set the ETag header so callers can use it as the If-Match value on a
    # subsequent PATCH; the value is opaque-by-contract.
    response.headers["ETag"] = _etag_for_updated_at(row["updated_at"])

    return _biosample_response_from_row(
        row,
        global_metadata=global_metadata,
        caller_system_role=user.system_role,
    )


# Substring of the asyncpg.RaiseError message thrown by the role-typed FK
# trigger on biosample.owner_idx. The trigger fires before the underlying
# FK constraint, so a non-user owner_idx surfaces as RaiseError; the route
# maps it to the same eligibility-422 the preflight emits. The marker text
# is pinned to the RAISE EXCEPTION format string in
# db/migrations/20260501000013_role_typed_fk_triggers.sql -- if either
# side changes, update both in lockstep.
_OWNER_TRIGGER_RAISE_MARKER = "user-kind principal"


@biosample_router.patch("/{biosample_idx}")
async def patch_biosample_route(
    biosample_idx: Annotated[int, Field(gt=0)],
    body: BiosamplePatchRequest,
    response: Response,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.BIOSAMPLE_WRITE)),
) -> BiosampleResponse:
    """Edit a biosample's core record.

    Auth bar: caller holds Scope.BIOSAMPLE_WRITE and is a HumanUser at
    system_role >= wet_lab_admin. require_role_at_least always rejects
    ServiceAccount callers (the auth model treats service-account
    authz as scope-only and ServiceAccount carries no system_role
    field), so the NCBI / ENA submission subsystem cannot reach this
    PATCH on its current credentials. A separate scope-gated surface
    (or a wider auth-model change so ServiceAccount carries a role)
    will be needed when the submission subsystem starts writing back
    accessions through the API.

    If-Match is required: missing -> 428, mismatch -> 412. The body's
    editable fields are validated by BiosamplePatchRequest (extra=forbid
    rejects immutable / retirement columns with 422; an empty body is
    also 422). Inside one connection-scoped transaction the route
    preflights the row for existence (404), retirement (409), and ETag
    match (412); validates the candidate owner via
    require_eligible_owner when owner_idx is in the body (422 on
    ineligibility); applies the UPDATE; re-reads global metadata for
    the response. The UPDATE is also checked for a None return — the
    row can vanish between the preflight and the UPDATE under READ
    COMMITTED isolation if a concurrent transaction commits a delete
    in that window — and surfaces as the same 404 the preflight emits.
    Uniqueness violations on biosample_accession / ena_sample_accession
    map to 409, FK violations on metadata_checklist_idx to 422, and the
    role-typed FK trigger on owner_idx (a backstop the preflight should
    preempt in practice) to the same eligibility-422.

    The response carries an `ETag` header derived from the new row's
    `updated_at` column; format mirrors the GET endpoint's contract
    and is opaque to clients.
    """
    # Missing If-Match is 428 before any DB work runs.
    if if_match is None:
        raise HTTPException(status_code=428, detail="If-Match header required")

    # Build the column-keyed write set from the model's set fields so the
    # repository sees only what the caller explicitly included; explicit
    # null vs. absent is distinguished by model_fields_set.
    fields = {name: getattr(body, name) for name in body.model_fields_set}

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Preflight: existence -> 404, retirement -> 409, ETag -> 412.
                row = await fetch_biosample(conn, biosample_idx)
                if row is None:
                    raise HTTPException(
                        status_code=404, detail=f"biosample {biosample_idx} not found"
                    )
                if row["retired"]:
                    raise HTTPException(
                        status_code=409, detail=f"biosample {biosample_idx} is retired"
                    )
                if if_match != _etag_for_updated_at(row["updated_at"]):
                    raise HTTPException(status_code=412, detail="If-Match did not match")

                # Eligibility preflight runs only when ownership is being
                # transferred; collapses every ineligibility case to 422.
                if "owner_idx" in fields:
                    await require_eligible_owner(
                        conn,
                        candidate_idx=fields["owner_idx"],
                        detail=_MSG_OWNER_NOT_ELIGIBLE,
                    )

                # Apply the UPDATE; the repo function returns the post-UPDATE
                # row in the same shape fetch_biosample selects, so no
                # follow-up SELECT is needed.
                updated_row = await update_biosample(conn, biosample_idx, fields=fields)

                # The repo returns None when the row vanished between the
                # preflight SELECT and this UPDATE; surface that as the
                # same 404 the preflight emits for the static missing-row
                # case.
                if updated_row is None:
                    raise HTTPException(
                        status_code=404, detail=f"biosample {biosample_idx} not found"
                    )

                # Re-read global metadata in the same transaction so the
                # response and the UPDATE see one consistent snapshot.
                metadata_rows = await fetch_global_metadata_for_biosample(
                    conn, biosample_idx
                )
    except asyncpg.UniqueViolationError as exc:
        detail = _UNIQUE_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_UNIQUE_VIOLATION)
        raise HTTPException(status_code=409, detail=detail)
    except asyncpg.ForeignKeyViolationError as exc:
        detail = _FK_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_FK_VIOLATION)
        raise HTTPException(status_code=422, detail=detail)
    except asyncpg.RaiseError as exc:
        # Role-typed FK trigger on biosample.owner_idx: candidate is
        # non-user. The preflight should have caught this; the trigger is
        # the schema-level backstop and the caller-facing surface is the
        # same 422 the preflight emits.
        if _OWNER_TRIGGER_RAISE_MARKER in str(exc):
            raise HTTPException(status_code=422, detail=_MSG_OWNER_NOT_ELIGIBLE)
        raise

    # Set the new ETag from the updated row's bumped updated_at.
    response.headers["ETag"] = _etag_for_updated_at(updated_row["updated_at"])

    # Reuse the GET route's row -> response shaper so the PATCH and GET
    # surfaces share one source of truth for the response shape.
    global_metadata = {
        internal_name: BiosampleGlobalMetadataEntry(
            display_name=entry.display_name,
            description=entry.description,
            data_type=entry.data_type,
            value=entry.value,
        )
        for internal_name, entry in metadata_rows.items()
    }
    return _biosample_response_from_row(
        updated_row,
        global_metadata=global_metadata,
        caller_system_role=caller.system_role,
    )
