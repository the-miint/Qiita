"""Study create / read / patch routes.

POST creates a study and gates on caller scope plus the lab-tech-on-behalf
rule; GET reads a study by idx and gates on the per-study default_tier
policy with admin / wet_lab_admin bypass; PATCH edits the editable
post-create columns and gates on caller scope plus per-study Tier.ADMIN
(wet_lab_admin bypass). The three handlers share the row → StudyResponse
mapping via a helper in this module. DELETE / search endpoints are
deferred.
"""

import json
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Response
from pydantic import Field
from qiita_common.api_paths import (
    PATH_STUDY_BY_IDX,
    PATH_STUDY_LOOKUP_BY_ACCESSION,
    PATH_STUDY_PREFIX,
    PATH_STUDY_ROOT,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    StudyCreate,
    StudyLookupByAccessionRequest,
    StudyLookupByAccessionResponse,
    StudyPatchRequest,
    StudyResponse,
    Tier,
)

from ..auth.guards import (
    require_complete_profile,
    require_eligible_owner,
    require_human,
    require_scope,
    require_study_access,
    require_study_exists,
)
from ..auth.principal import HumanUser, Principal
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.study import (
    create_study,
    fetch_study,
    fetch_study_idxs_by_accession,
    update_study,
)
from ._helpers import (
    GENERIC_FK_VIOLATION,
    etag_for_updated_at,
    raise_for_unique_violation,
    require_etag_match,
    require_if_match,
    resolve_idxs_by_natural_key,
)

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


@router.patch(PATH_STUDY_BY_IDX)
async def patch_study(
    study_idx: Annotated[int, Field(gt=0)],
    body: StudyPatchRequest,
    response: Response,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    _scope: Principal = Depends(require_scope(Scope.STUDY_WRITE)),
    _exists: None = Depends(require_study_exists),
    _access: None = Depends(
        require_study_access(min_tier=Tier.ADMIN, bypass_role=SystemRole.WET_LAB_ADMIN)
    ),
) -> StudyResponse:
    """Edit a study's core record.

    Auth bar: caller holds Scope.STUDY_WRITE AND either is wet_lab_admin
    or higher, or holds Tier.ADMIN on this study (the owner-bypass path
    inside require_study_access covers the study owner). require_study_exists
    composes alongside require_study_access so role-bypass callers still get
    404 on a non-existent study_idx (the access guard's bypass path returns
    without any DB lookup, so it cannot surface that 404 on its own).

    Once the scope, existence, and access gates above pass, If-Match
    is required: missing -> 428, mismatch -> 412. The body's
    editable fields are validated by StudyPatchRequest (extra=forbid
    rejects immutable / system-managed columns with 422; an empty body
    is also 422; explicit-null title is 422). Inside one connection-
    scoped transaction the route runs a `SELECT ... FOR UPDATE`
    preflight on the row (existence -> 404, ETag -> 412); applies the
    UPDATE; returns the post-UPDATE row. The FOR UPDATE lock is held
    from preflight through commit, so concurrent PATCHes on the same
    row serialize at the preflight: the second caller blocks until
    the first commits, then sees the post-commit `updated_at` and
    412s on its now-stale If-Match header. Uniqueness violations on
    ebi_study_accession map to 409 via the shared helper; FK violations
    to the generic 422; the role-typed FK trigger on
    principal_investigator_idx (a candidate non-user principal) to a
    disambiguated 422.

    The response carries an `ETag` header derived from the new row's
    `updated_at` column; format mirrors the GET endpoint's contract
    and is opaque to clients.
    """
    if_match = require_if_match(if_match)

    # Build the column-keyed write set from the model's set fields so the
    # repository sees only what the caller explicitly included; explicit
    # null vs. absent is distinguished by model_fields_set.
    fields = {name: getattr(body, name) for name in body.model_fields_set}

    async with tx() as conn:
        try:
            # Preflight: existence -> 404, ETag -> 412. for_update=True
            # acquires a row-level lock for the rest of the transaction
            # so a concurrent PATCH on the same row serializes here
            # instead of racing through the ETag check and silently
            # overwriting the first writer's update.
            row = await fetch_study(conn, study_idx, for_update=True)
            require_etag_match(row, if_match=if_match, label="study", row_idx=study_idx)

            # Apply the UPDATE; the repo function returns the post-UPDATE
            # row in the same shape fetch_study selects, so no follow-up
            # SELECT is needed. The FOR UPDATE preflight holds a row lock
            # for the rest of this transaction, so update_study cannot
            # return None here — the row is guaranteed to exist under
            # our lock. The defensive None check is kept as a backstop
            # and surfaces as the same 404 the preflight emits, so an
            # invariant violation (someone removing the lock without
            # rethinking) fails loudly rather than indexing into None.
            updated_row = await update_study(conn, study_idx, fields=fields)
            if updated_row is None:
                raise HTTPException(status_code=404, detail=f"study {study_idx} not found")
        except asyncpg.UniqueViolationError as exc:
            raise_for_unique_violation(
                exc,
                constraint_messages=_UNIQUE_VIOLATION_MESSAGES,
                generic=_GENERIC_UNIQUE_VIOLATION,
            )
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(status_code=422, detail=GENERIC_FK_VIOLATION)
        except asyncpg.RaiseError as exc:
            # tg_principal_must_be_user fires for PI idx pointing at a
            # non-user-kind principal (service account or bare principal).
            # owner_idx is not patchable so that arm of the trigger is
            # unreachable here.
            msg = str(exc)
            if "must reference a user-kind principal" in msg:
                raise HTTPException(status_code=422, detail=_MSG_BAD_PI_NOT_USER)
            raise

    # Set the new ETag from the updated row's bumped updated_at.
    response.headers["ETag"] = etag_for_updated_at(updated_row["updated_at"])

    return _study_response_from_row(updated_row)


@router.post(PATH_STUDY_LOOKUP_BY_ACCESSION)
async def lookup_study_by_accession(
    body: StudyLookupByAccessionRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.STUDY_READ)),
) -> StudyLookupByAccessionResponse:
    """Resolve a list of ebi_study_accession values to study_idx.

    POST (not GET) so a long accession list rides in the body rather
    than tripping nginx's default URL-line cap.

    Auth: HumanUser with Scope.STUDY_READ. The response is only the
    (accession, idx) mapping — no study columns — so resolution does
    not itself disclose row contents; reading a row still requires the
    per-row access policy on GET /study/{idx}.

    `missing` lists input-order-deduped accessions that did not resolve.
    """
    # _user is read only to keep the dependency chain explicit — no
    # per-caller filter runs here (see auth docstring).
    _ = user
    resolved, missing = await resolve_idxs_by_natural_key(
        values=body.accessions,
        fetcher=lambda values: fetch_study_idxs_by_accession(pool, values=values),
    )
    return StudyLookupByAccessionResponse(resolved=resolved, missing=missing)
