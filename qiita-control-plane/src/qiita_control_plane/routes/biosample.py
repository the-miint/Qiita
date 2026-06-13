"""Biosample routes.

Two routers live here. The study-scoped router (prefix=/study) carries the
single-biosample import (POST) and the study-scoped bulk-id read
(GET .../list-idxs). The biosample-scoped router (prefix=/biosample)
carries the single-resource read (GET /{biosample_idx}) and the
single-resource PATCH (PATCH /{biosample_idx}). Bulk-import,
retirement, search, and admin metadata-schema endpoints are deferred.
The write handler gates on caller scope, study existence, and
per-study ADMIN access (wet_lab_admin+ bypass) and delegates the
multi-table write to the repositories.biosample composer inside one
connection-scoped transaction; the study-scoped read gates
on caller scope and study access (with admin role bypass); the
single-biosample read gates on caller scope, then 404s on missing or
retired biosamples and gates non-admin callers on
owner-or-linked-study-access via the repository predicate; the PATCH
gates on caller scope and wet_lab_admin (or higher) role and applies
its mutation inside one connection-scoped transaction with required
If-Match optimistic-concurrency control.
"""

from collections.abc import Awaitable, Callable
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Response
from pydantic import Field
from qiita_common.api_paths import (
    PATH_BIOSAMPLE_BY_IDX,
    PATH_BIOSAMPLE_BY_STUDY,
    PATH_BIOSAMPLE_LIST_BY_STUDY,
    PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION,
    PATH_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID,
    PATH_BIOSAMPLE_PREFIX,
    PATH_STUDY_PREFIX,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    BiosampleImportRequest,
    BiosampleImportResponse,
    BiosampleLookupByAccessionRequest,
    BiosampleLookupByAccessionResponse,
    BiosampleLookupByMatrixTubeIdRequest,
    BiosampleLookupByMatrixTubeIdResponse,
    BiosamplePatchRequest,
    BiosampleResponse,
    GlobalMetadataEntry,
    IdxsListResponse,
    MetadataChecklistRef,
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
    MetadataParseError,
    MetadataUnknownFieldsError,
    SlotOccupiedError,
    StudyFieldConflictError,
    TransientWriteRaceError,
    fetch_global_metadata,
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
from ._helpers import (
    GENERIC_FK_VIOLATION,
    detail_for_slot_collision,
    etag_for_updated_at,
    raise_for_transient_write_race,
    raise_for_unique_violation,
    require_etag_match,
    require_if_match,
    resolve_idxs_by_natural_key,
    resolve_metadata_checklist_idx,
)

router = APIRouter(prefix=PATH_STUDY_PREFIX, tags=["biosample"])
biosample_router = APIRouter(prefix=PATH_BIOSAMPLE_PREFIX, tags=["biosample"])


_MSG_OWNER_NOT_ELIGIBLE = "owner is not eligible to own biosamples"

# Map of constraint names import_biosample_from_owner_biosample_id can trip
# (everything else is pre-flight-checked, swallowed by ON CONFLICT, or surfaces
# as a different exception class). Unknown names fall back to the generic
# strings on the matching exception path.
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
_GENERIC_CHECK_VIOLATION = "violates a database constraint on biosample"


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
        # Owner eligibility pre-flight; collapses every ineligibility case to
        # one 422.
        await require_eligible_owner(
            conn,
            candidate_idx=body.owner_idx,
            detail=_MSG_OWNER_NOT_ELIGIBLE,
        )

        # Map known composer-side validation errors and DB-level violations to
        # user-friendly 422 / 409 responses. Composer-specific exceptions are
        # caught first so their detail wins over the generic asyncpg fallbacks.
        # Resolve the checklist name to its idx before the write; an
        # unknown name surfaces as a clean 422 rather than an FK violation.
        metadata_checklist_idx = await resolve_metadata_checklist_idx(
            conn, body.metadata_checklist_name
        )

        try:
            result = await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=study_idx,
                owner_idx=body.owner_idx,
                owner_biosample_id_field_name=body.owner_biosample_id_field_name,
                owner_biosample_id_value=body.owner_biosample_id_value,
                caller_idx=user.principal_idx,
                metadata=body.metadata,
                metadata_checklist_idx=metadata_checklist_idx,
                biosample_accession=body.biosample_accession,
                ena_sample_accession=body.ena_sample_accession,
                matrix_tube_id=body.matrix_tube_id,
            )
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
        except MetadataUnknownFieldsError as exc:
            raise HTTPException(
                status_code=422,
                detail=f"unknown metadata fields: {', '.join(exc.unknown_display_names)}",
            )
        except MetadataParseError as exc:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"could not parse metadata field {exc.display_name!r}"
                    f" value {exc.text_value!r} as {exc.data_type}: {exc.reason}"
                ),
            )
        except StudyFieldConflictError as exc:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"study has an existing field at display_name {exc.display_name!r}"
                    " bound to a different global field"
                ),
            )
        except SlotOccupiedError as exc:
            # SlotOccupiedError is its own exception family (not an
            # asyncpg.UniqueViolationError subclass), so this catch and
            # the generic UniqueViolationError catch below are independent;
            # both return 409, but this one's detail discriminates the
            # six sub-cases rather than collapsing to the generic message.
            #
            # NOT DEAD CODE — do not prune. Currently unreachable
            # through this POST because the route creates a fresh
            # biosample per call, so neither the global-field slot nor
            # the per-field local slot for that biosample can be pre-
            # occupied and the unique constraint cannot fire. Kept for
            # the planned PATCH-style write-metadata-on-existing-
            # biosample endpoint, which will share this composer path;
            # that endpoint can hit either constraint whenever a caller
            # writes a value into a biosample whose slot was already
            # claimed (by another study on the global path, or by an
            # earlier write on the same study on the local path).
            detail = await detail_for_slot_collision(conn, exc)
            raise HTTPException(status_code=409, detail=detail)
        except TransientWriteRaceError as exc:
            # The diagnostic read found the colliding occupant already
            # gone — a concurrent delete won the race and the slot is
            # free again. Independent of the asyncpg catches; maps to a
            # 503 + Retry-After so the client resubmits the same request.
            raise_for_transient_write_race(exc)
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
            detail = _CHECK_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_CHECK_VIOLATION)
            raise HTTPException(status_code=422, detail=detail)

    return BiosampleImportResponse(
        biosample_idx=result.biosample_idx,
        owner_id_biosample_study_field_idx=result.owner_id_biosample_study_field_idx,
        owner_id_biosample_study_field_created=result.owner_id_biosample_study_field_created,
    )


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
    # truncation; the route slices back to the cap before returning.
    rows = await fetch_biosample_idxs_for_study(
        pool, study_idx=study_idx, limit=_BIOSAMPLE_IDXS_HARD_CAP + 1
    )
    truncated = len(rows) > _BIOSAMPLE_IDXS_HARD_CAP
    if truncated:
        rows = rows[:_BIOSAMPLE_IDXS_HARD_CAP]
    return IdxsListResponse(
        idxs=rows,
        count=len(rows),
        truncated=truncated,
        caller_system_role=user.system_role,
    )


# Roles that may bypass the per-biosample owner / linked-study-access check.
# A bypass-role caller still gets the standard 404 on a missing or retired
# biosample (see the docstring on get_biosample for the retired-row
# carve-out planned for a future change).
_BIOSAMPLE_GET_BYPASS_ROLE: SystemRole = SystemRole.WET_LAB_ADMIN


def _biosample_response_from_row(
    row: asyncpg.Record,
    *,
    global_metadata: dict[str, GlobalMetadataEntry],
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
            "global_metadata": global_metadata,
            "caller_system_role": caller_system_role,
        }
    )


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

    global_metadata = {
        internal_name: GlobalMetadataEntry(
            display_name=entry.display_name,
            description=entry.description,
            data_type=entry.data_type,
            value=entry.value,
        )
        for internal_name, entry in metadata_rows.items()
    }

    # Set the ETag header so callers can use it as the If-Match value on a
    # subsequent PATCH; the value is opaque-by-contract.
    response.headers["ETag"] = etag_for_updated_at(row["updated_at"])

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
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
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
            detail = _CHECK_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_CHECK_VIOLATION)
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
    response.headers["ETag"] = etag_for_updated_at(updated_row["updated_at"])

    # Reuse the GET route's row -> response shaper so the PATCH and GET
    # surfaces share one source of truth for the response shape.
    global_metadata = {
        internal_name: GlobalMetadataEntry(
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
