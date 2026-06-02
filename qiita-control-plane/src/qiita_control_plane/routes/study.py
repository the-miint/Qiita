"""Study create / read routes.

POST creates a study and gates on caller scope plus the lab-tech-on-behalf
rule; GET reads a study by idx and gates on the per-study default_tier
policy with admin / wet_lab_admin bypass. Both delegate the row →
StudyResponse mapping to a shared helper. PATCH / DELETE / search
endpoints are deferred.
"""

import json
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import PATH_STUDY_BY_IDX, PATH_STUDY_PREFIX, PATH_STUDY_ROOT
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import StudyCreate, StudyResponse

from ..auth.guards import (
    require_complete_profile,
    require_eligible_owner,
    require_scope,
    require_study_access,
    require_study_exists,
)
from ..auth.principal import HumanUser, Principal
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.study import create_study, fetch_study
from ._helpers import GENERIC_FK_VIOLATION, raise_for_unique_violation

router = APIRouter(prefix=PATH_STUDY_PREFIX, tags=["study"])


_MSG_ON_BEHALF_REQUIRES_WET_LAB_ADMIN = (
    "creating studies on behalf of another user requires wet_lab_admin or higher"
)
_MSG_OWNER_NOT_ELIGIBLE = "owner is not eligible to own studies"
_MSG_BAD_PI_NOT_USER = "principal_investigator_idx must reference a user-kind principal"
_MSG_BAD_OWNER_NOT_USER = "owner_idx must reference a user-kind principal"

# Keys must mirror constraint names from db/migrations/; drift falls
# through to _GENERIC_UNIQUE_VIOLATION.
_UNIQUE_VIOLATION_MESSAGES: dict[str, str] = {
    "study_ebi_study_accession_unique": "ebi_study_accession already in use",
}
_GENERIC_UNIQUE_VIOLATION = "conflicts with an existing study"


def _study_response_from_row(row: asyncpg.Record) -> StudyResponse:
    """Shape a qiita.study row (from create_study or fetch_study) into the
    StudyResponse the POST and GET routes both return.

    asyncpg returns JSONB columns as text, so `extra_metadata` is decoded
    back to a dict / None before handing the row to Pydantic.
    """
    # Decode the JSONB column once so the response shape is dict-or-None.
    extra_metadata = row["extra_metadata"]
    if isinstance(extra_metadata, str):
        extra_metadata = json.loads(extra_metadata)

    return StudyResponse.model_validate(
        {
            "study_idx": row["idx"],
            "owner_idx": row["owner_idx"],
            "principal_investigator_idx": row["principal_investigator_idx"],
            "title": row["title"],
            "alias": row["alias"],
            "description": row["description"],
            "abstract": row["abstract"],
            "funding": row["funding"],
            "ebi_study_accession": row["ebi_study_accession"],
            "notes": row["notes"],
            "extra_metadata": extra_metadata,
            "default_tier": row["default_tier"],
            "created_by_idx": row["created_by_idx"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    )


@router.post(PATH_STUDY_ROOT, status_code=201)
async def create_study_route(
    body: StudyCreate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.STUDY_WRITE)),
) -> StudyResponse:
    """Create a study atomically with the owner's ADMIN study_access row.

    The caller must be a HumanUser with profile_complete=True and must hold
    the study:write scope. `body.owner_idx=None` defaults the owner to the
    caller (caller-creates-own-study); supplying a different owner requires
    wet_lab_admin or higher (lab-tech-on-behalf rule). The caller is always
    recorded as `created_by_idx`; only the owner is transferred when
    on-behalf creation is in play.
    """
    # Resolve the effective owner: caller's principal_idx when the body
    # leaves it null, otherwise the body value.
    effective_owner_idx = body.owner_idx if body.owner_idx is not None else user.principal_idx

    # Lab-tech-on-behalf rule. Only wet_lab_admin or higher may name a
    # different principal as the study owner.
    if effective_owner_idx != user.principal_idx and not user.has_role_at_least(
        SystemRole.WET_LAB_ADMIN
    ):
        raise HTTPException(status_code=403, detail=_MSG_ON_BEHALF_REQUIRES_WET_LAB_ADMIN)

    async with tx() as conn:
        # Owner eligibility pre-flight; collapses every ineligibility case to
        # one 422.
        await require_eligible_owner(
            conn,
            candidate_idx=effective_owner_idx,
            detail=_MSG_OWNER_NOT_ELIGIBLE,
        )

        # Map known FK / trigger / unique violations to user-friendly responses.
        try:
            row = await create_study(
                conn,
                owner_idx=effective_owner_idx,
                created_by_idx=user.principal_idx,
                title=body.title,
                principal_investigator_idx=body.principal_investigator_idx,
                alias=body.alias,
                description=body.description,
                abstract=body.abstract,
                funding=body.funding,
                ebi_study_accession=body.ebi_study_accession,
                notes=body.notes,
                extra_metadata=body.extra_metadata,
                default_tier=body.default_tier,
            )
        except asyncpg.UniqueViolationError as exc:
            raise_for_unique_violation(
                exc,
                constraint_messages=_UNIQUE_VIOLATION_MESSAGES,
                generic=_GENERIC_UNIQUE_VIOLATION,
            )
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(status_code=422, detail=GENERIC_FK_VIOLATION)
        except asyncpg.RaiseError as exc:
            # tg_principal_must_be_user fires for owner_idx or PI idx pointing
            # at a non-user-kind principal (service account or bare principal).
            # Disambiguate by message text since both columns share one trigger.
            msg = str(exc)
            if "must reference a user-kind principal" in msg:
                if "principal_investigator_idx" in msg:
                    raise HTTPException(status_code=422, detail=_MSG_BAD_PI_NOT_USER)
                raise HTTPException(status_code=422, detail=_MSG_BAD_OWNER_NOT_USER)
            raise

    # Shape the inserted row into the response model (shared with the GET
    # handler so column → field mapping has a single source of truth).
    return _study_response_from_row(row)


@router.get(PATH_STUDY_BY_IDX)
async def get_study(
    study_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.STUDY_READ)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(require_study_access(bypass_role=SystemRole.WET_LAB_ADMIN)),
) -> StudyResponse:
    """Return the qiita.study row for the path's idx as a StudyResponse.

    Access policy: any wet_lab_admin or higher passes; otherwise the
    caller's effective tier on this study (public-by-absence when no
    qiita.study_access row) must be at or above the study's
    `default_tier`. require_study_exists composes alongside
    require_study_access so admin-bypass callers still get 404 on a
    non-existent study_idx (the access guard's bypass path returns
    without any DB lookup, so it cannot surface that 404 on its own);
    the access guard then emits the 401 / 403 responses for callers
    that fail the tier policy.
    """
    # The guard chain has already validated existence + access; the
    # fetch is therefore expected to find the row, but a None defends
    # against the (theoretically possible) race where the study is
    # deleted between the guard and the handler.
    row = await fetch_study(pool, study_idx)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"study {study_idx} not found",
        )
    return _study_response_from_row(row)
