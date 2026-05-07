"""Biosample routes.

Single-biosample import (POST) plus the study-scoped bulk-id read
(GET .../list-idxs). Bulk-import, single-resource GET / PATCH,
retirement, search, and admin metadata-schema endpoints are deferred.
The write handler gates on caller scope, role, and study existence and
delegates the multi-table write to the repositories.biosample composer
inside one connection-scoped transaction; the read handler gates on
caller scope and study access (with admin role bypass) and delegates
to a single repository read.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    BiosampleIdxsListResponse,
    BiosampleImportRequest,
    BiosampleImportResponse,
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
    fetch_biosample_idxs_for_study,
    import_biosample_from_owner_biosample_id,
)
from ..repositories.biosample_metadata import (
    BiosampleMetadataParseError,
    BiosampleMetadataUnknownFieldsError,
    BiosampleOwnerIdFieldCollisionError,
    BiosampleStudyFieldConflictError,
)

router = APIRouter(prefix="/study", tags=["biosample"])


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
    # Owner eligibility pre-flight. The helper skips the lookup when
    # candidate == caller (already validated by require_complete_profile)
    # and collapses every ineligibility case to one 422.
    await require_eligible_owner(
        pool,
        candidate_idx=body.owner_idx,
        caller_idx=user.principal_idx,
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
