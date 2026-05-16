"""Sequencing-ingestion routes.

Holds the three POST handlers that load-code uses to land a sequencing
run plus its lanes and per-item prep samples (POST /sequencing-run, POST
/sequencing-run/{idx}/sequenced-pool, and the atomic
POST /sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample
composer), the run-scoped bulk-id read of sequenced_sample idxs, the
single-sequenced-sample GET that surfaces the combined sequenced_sample +
supertype prep_sample row plus its globally-linked metadata, and the
single-sequenced-sample PATCH that edits the subtype-table columns
(ENA accessions and submission tracking).

Every write handler gates on caller scope (Scope.PREP_SAMPLE_WRITE) and
role (wet_lab_admin or higher) plus require_complete_profile
(humans-only); the read handlers gate on Scope.PREP_SAMPLE_READ +
wet_lab_admin role unconditionally. All handlers delegate their DB work
to the sibling repository modules. Service accounts are rejected by the
role gate today; a wider auth-model change is required before
submission-subsystem service accounts can satisfy require_role_at_least.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, Header, HTTPException, Response
from pydantic import Field
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import (
    GlobalMetadataEntry,
    IdxsListResponse,
    SequencedPoolCreateRequest,
    SequencedPoolCreateResponse,
    SequencedSampleCreateRequest,
    SequencedSampleCreateResponse,
    SequencedSamplePatchRequest,
    SequencedSampleResponse,
    SequencingRunCreateRequest,
    SequencingRunCreateResponse,
)

from ..auth.guards import (
    require_complete_profile,
    require_eligible_owner,
    require_human,
    require_role_at_least,
    require_scope,
    require_sequenced_pool_in_run,
    require_sequencing_run_exists,
)
from ..auth.principal import HumanUser, Principal
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories._sample_helpers import (
    GlobalFieldSlotOccupiedError,
    MetadataParseError,
    MetadataUnknownFieldsError,
    StudyFieldConflictError,
    TransientMetadataWriteRaceError,
)
from ..repositories.prep_sample import (
    create_sequenced_prep_sample,
    fetch_sequenced_sample_with_prep_sample,
    update_sequenced_sample,
)
from ..repositories.prep_sample_metadata import fetch_global_metadata_for_prep_sample
from ..repositories.sequencing_run import (
    fetch_sequenced_sample_idxs_for_run,
    insert_sequenced_pool,
    insert_sequencing_run,
)
from ._helpers import (
    detail_for_global_field_collision,
    etag_for_updated_at,
    raise_for_transient_write_race,
)

router = APIRouter(prefix="/sequencing-run", tags=["sequencing-run"])
sequenced_sample_router = APIRouter(prefix="/sequenced-sample", tags=["sequenced-sample"])


# Map of constraint names insert_sequencing_run can trip. Unknown names fall
# back to the generic string on the matching exception path.
_UNIQUE_VIOLATION_MESSAGES: dict[str, str] = {
    "sequencing_run_instrument_run_id_unique": "instrument_run_id already in use",
}
_GENERIC_UNIQUE_VIOLATION = "conflicts with an existing sequencing_run"


@router.post("", status_code=201)
async def create_sequencing_run(
    body: SequencingRunCreateRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
) -> SequencingRunCreateResponse:
    """Create a sequencing_run row.

    The caller must be a HumanUser with profile_complete=True, must hold
    the prep_sample:write scope, and must be wet_lab_admin or higher.
    """
    async with tx() as conn:
        # Single INSERT inside the transaction so the audit / retention
        # trigger surface remains the same as every other write route.
        try:
            sequencing_run_idx = await insert_sequencing_run(
                conn,
                instrument_run_id=body.instrument_run_id,
                platform=body.platform,
                created_by_idx=user.principal_idx,
                instrument_model=body.instrument_model,
                instrument_serial=body.instrument_serial,
                run_performed_at=body.run_performed_at,
                extra_metadata=body.extra_metadata,
            )
        except asyncpg.UniqueViolationError as exc:
            detail = _UNIQUE_VIOLATION_MESSAGES.get(exc.constraint_name, _GENERIC_UNIQUE_VIOLATION)
            raise HTTPException(status_code=409, detail=detail)

    return SequencingRunCreateResponse(sequencing_run_idx=sequencing_run_idx)


_GENERIC_FK_VIOLATION = "references a row that does not exist"


@router.post("/{sequencing_run_idx}/sequenced-pool", status_code=201)
async def create_sequenced_pool(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    body: SequencedPoolCreateRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _run_exists: None = Depends(require_sequencing_run_exists),
) -> SequencedPoolCreateResponse:
    """Create a sequenced_pool attached to the path's sequencing_run.

    The require_sequencing_run_exists guard fires the 404 before the
    transaction opens. The body's run_preflight_blob is base64-decoded by
    Pydantic; the route stores the raw bytes in the BYTEA column.
    """
    async with tx() as conn:
        try:
            sequenced_pool_idx = await insert_sequenced_pool(
                conn,
                sequencing_run_idx=sequencing_run_idx,
                run_preflight_blob=body.run_preflight_blob,
                run_preflight_filename=body.run_preflight_filename,
                created_by_idx=user.principal_idx,
                extra_metadata=body.extra_metadata,
            )
        except asyncpg.ForeignKeyViolationError:
            # The require_sequencing_run_exists guard above rules this out
            # for the sequencing_run_idx column; a TOCTOU race (run deleted
            # between the guard and the INSERT) lands here and surfaces as 422.
            raise HTTPException(status_code=422, detail=_GENERIC_FK_VIOLATION)

    return SequencedPoolCreateResponse(sequenced_pool_idx=sequenced_pool_idx)


_MSG_OWNER_NOT_ELIGIBLE = "owner is not eligible to own prep samples"

# Markers and message maps for the sequenced-sample composer's exception
# ladder. Constraint names pin to the migration; if either side changes the
# other must follow in lockstep.
_BIOSAMPLE_LINK_TRIGGER_MARKER = "requires a non-retired biosample_to_study"

_SEQUENCED_SAMPLE_UNIQUE_MESSAGES: dict[str, str] = {
    "sequenced_sample_pool_item_id_unique": ("sequenced_pool_item_id already in use for this pool"),
    "sequenced_sample_ena_experiment_accession_unique": ("ena_experiment_accession already in use"),
    "sequenced_sample_ena_run_accession_unique": "ena_run_accession already in use",
    "prep_sample_metadata_unique_per_field": (
        "duplicate metadata entry for the same prep_sample_study_field"
    ),
    "prep_sample_metadata_one_value_per_global_field": (
        "duplicate metadata entry for the same global field on this prep_sample"
    ),
}
_SEQUENCED_SAMPLE_FK_MESSAGES: dict[str, str] = {
    "prep_sample_biosample_idx_fkey": ("biosample_idx does not reference an existing biosample"),
    "prep_sample_prep_protocol_idx_fkey": (
        "prep_protocol_idx does not reference an existing prep protocol"
    ),
    "prep_sample_metadata_checklist_idx_fkey": (
        "metadata_checklist_idx does not reference an existing checklist"
    ),
    "sequenced_sample_sequenced_pool_idx_fkey": (
        "sequenced_pool_idx does not reference an existing sequenced_pool"
    ),
    "prep_sample_to_study_study_idx_fkey": ("study_idx does not reference an existing study"),
}
_GENERIC_SEQ_UNIQUE_VIOLATION = "conflicts with an existing prep_sample / sequenced_sample"


@router.post(
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/sequenced-sample",
    status_code=201,
)
async def create_sequenced_sample(
    sequenced_pool_idx: Annotated[int, Field(gt=0)],
    body: SequencedSampleCreateRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _pool_in_run: None = Depends(require_sequenced_pool_in_run),
) -> SequencedSampleCreateResponse:
    """Create a sequenced prep_sample with study links and metadata, atomically.

    Path consistency: the pool named in the URL must belong to the run
    named in the URL. The require_sequenced_pool_in_run guard fires the
    404 (pool missing) or 422 (pool exists but does not belong to the
    run) before the transaction opens. Inside the transaction, the route
    checks owner eligibility and delegates the multi-table write to the
    create_sequenced_prep_sample composer; composer-side validation errors
    and DB-level constraint / trigger violations are mapped to 422 / 409
    with user-friendly detail strings.
    """
    async with tx() as conn:
        # Owner eligibility pre-flight inside the transaction; collapses
        # every ineligibility case to one 422 and shares the lookup with
        # any future composers on the same connection.
        await require_eligible_owner(
            conn,
            candidate_idx=body.owner_idx,
            detail=_MSG_OWNER_NOT_ELIGIBLE,
        )

        # Map known composer-side errors and DB-level violations to user-
        # friendly responses. Typed catches first so their detail wins.
        try:
            result = await create_sequenced_prep_sample(
                conn,
                sequenced_pool_idx=sequenced_pool_idx,
                biosample_idx=body.biosample_idx,
                prep_protocol_idx=body.prep_protocol_idx,
                owner_idx=body.owner_idx,
                sequenced_pool_item_id=body.sequenced_pool_item_id,
                metadata=body.metadata,
                primary_study_idx=body.primary_study_idx,
                secondary_study_idxs=body.secondary_study_idxs,
                caller_idx=user.principal_idx,
                metadata_checklist_idx=body.metadata_checklist_idx,
                ena_experiment_accession=body.ena_experiment_accession,
                ena_run_accession=body.ena_run_accession,
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
                    f"study {exc.study_idx} has an existing field at"
                    f" display_name {exc.display_name!r} bound to a different"
                    " global field"
                ),
            )
        except GlobalFieldSlotOccupiedError as exc:
            # GlobalFieldSlotOccupiedError is its own exception family (not
            # an asyncpg.UniqueViolationError subclass), so this catch and
            # the generic UniqueViolationError catch below are independent;
            # both return 409, but this one's detail discriminates the five
            # cross-study sub-cases rather than collapsing to the generic
            # message.
            #
            # NOT DEAD CODE — do not prune. Currently unreachable
            # through this POST because the route creates a fresh
            # prep_sample per call, so (prep_sample_idx, global_field_idx)
            # is always empty pre-INSERT and the partial unique index
            # cannot fire. Kept for the planned PATCH-style
            # write-metadata-on-existing-prep_sample endpoint, which
            # will share this composer path; that endpoint can hit the
            # partial index whenever a caller writes a value for a
            # prep_sample whose global field slot was already claimed
            # by another study. Helper unit tests in
            # tests/routes/test__helpers.py cover the wording for every
            # subclass even though no current route flow triggers them.
            detail = await detail_for_global_field_collision(conn, exc)
            raise HTTPException(status_code=409, detail=detail)
        except TransientMetadataWriteRaceError as exc:
            # The diagnostic read found the colliding occupant already
            # gone — a concurrent delete won the race and the slot is
            # free again. Independent of the asyncpg catches; maps to a
            # 503 + Retry-After so the client resubmits the same request.
            raise_for_transient_write_race(exc)
        except asyncpg.UniqueViolationError as exc:
            detail = _SEQUENCED_SAMPLE_UNIQUE_MESSAGES.get(
                exc.constraint_name, _GENERIC_SEQ_UNIQUE_VIOLATION
            )
            raise HTTPException(status_code=409, detail=detail)
        except asyncpg.ForeignKeyViolationError as exc:
            detail = _SEQUENCED_SAMPLE_FK_MESSAGES.get(exc.constraint_name, _GENERIC_FK_VIOLATION)
            raise HTTPException(status_code=422, detail=detail)
        except asyncpg.RaiseError as exc:
            # The prep_sample_to_study_reject_without_biosample_link trigger
            # raises with a fixed substring; map it to 422 naming the
            # biosample / study and re-raise anything else.
            if _BIOSAMPLE_LINK_TRIGGER_MARKER in str(exc):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "prep_sample cannot be linked to the requested study;"
                        " biosample is not linked (or link is retired) for that study"
                    ),
                )
            raise

    return SequencedSampleCreateResponse(
        prep_sample_idx=result.prep_sample_idx,
        sequenced_sample_idx=result.sequenced_sample_idx,
    )


# Hard cap on the run-scoped bulk-id read. Sized to cover a fully loaded
# illumina run (thousands of pool items per lane, several lanes per run)
# with significant headroom while bounding per-response payload size.
# The biosample roster cap happens to share this numeric value, but the
# two bound conceptually distinct rosters and are sized independently;
# they are intentionally not factored into a shared constant.
_SEQUENCED_SAMPLE_IDXS_HARD_CAP = 500_000


@router.get("/{sequencing_run_idx}/sequenced-sample/list-idxs")
async def list_sequenced_sample_idxs_in_run(
    sequencing_run_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _run_exists: None = Depends(require_sequencing_run_exists),
) -> IdxsListResponse:
    """List sequenced_sample idxs reachable from the path's run, newest first.

    Caller must be a HumanUser with Scope.PREP_SAMPLE_READ and system_role
    at least wet_lab_admin. The require_sequencing_run_exists guard fires
    a 404 before the read runs. Walks run -> sequenced_pool ->
    sequenced_sample -> prep_sample and excludes rows whose supertype
    prep_sample is retired; sequenced_pool itself carries no retirement
    surface. The `truncated` flag indicates the underlying set exceeded
    the hard cap; callers hitting it should narrow their scope.
    """
    # Fetch cap+1 rows so a count strictly greater than the cap signals
    # truncation; the route slices back to the cap before returning.
    rows = await fetch_sequenced_sample_idxs_for_run(
        pool,
        sequencing_run_idx=sequencing_run_idx,
        limit=_SEQUENCED_SAMPLE_IDXS_HARD_CAP + 1,
    )
    truncated = len(rows) > _SEQUENCED_SAMPLE_IDXS_HARD_CAP
    if truncated:
        rows = rows[:_SEQUENCED_SAMPLE_IDXS_HARD_CAP]
    return IdxsListResponse(
        idxs=rows,
        count=len(rows),
        truncated=truncated,
        caller_system_role=user.system_role,
    )


def _sequenced_sample_response_from_row(
    row: asyncpg.Record,
    *,
    global_metadata: dict[str, GlobalMetadataEntry],
    caller_system_role: SystemRole,
) -> SequencedSampleResponse:
    """Shape a JOIN(sequenced_sample, prep_sample) row + decoded global
    metadata into SequencedSampleResponse.

    Centralises the column -> field mapping (ss.idx -> sequenced_sample_idx,
    GREATEST(...) -> effective_updated_at) so the GET (and a future PATCH)
    share one source of truth. The global_metadata dict is supplied by
    the caller — this helper runs no DB queries.
    """
    return SequencedSampleResponse.model_validate(
        {
            "sequenced_sample_idx": row["idx"],
            "prep_sample_idx": row["prep_sample_idx"],
            "biosample_idx": row["biosample_idx"],
            "owner_idx": row["owner_idx"],
            "prep_protocol_idx": row["prep_protocol_idx"],
            "metadata_checklist_idx": row["metadata_checklist_idx"],
            "sequenced_pool_idx": row["sequenced_pool_idx"],
            "sequenced_pool_item_id": row["sequenced_pool_item_id"],
            "ena_experiment_accession": row["ena_experiment_accession"],
            "ena_run_accession": row["ena_run_accession"],
            "last_submission_at": row["last_submission_at"],
            "submission_error": row["submission_error"],
            "last_metadata_change_at": row["last_metadata_change_at"],
            "created_by_idx": row["created_by_idx"],
            "created_at": row["created_at"],
            "effective_updated_at": row["effective_updated_at"],
            "retired": row["retired"],
            "retired_by_idx": row["retired_by_idx"],
            "retired_at": row["retired_at"],
            "retire_reason": row["retire_reason"],
            "global_metadata": global_metadata,
            "caller_system_role": caller_system_role,
        }
    )


@sequenced_sample_router.get("/{sequenced_sample_idx}")
async def get_sequenced_sample_route(
    sequenced_sample_idx: Annotated[int, Field(gt=0)],
    response: Response,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
) -> SequencedSampleResponse:
    """Return the joined sequenced_sample + supertype prep_sample row plus
    its globally-linked prep_sample metadata.

    Auth: any wet_lab_admin or higher with Scope.PREP_SAMPLE_READ passes;
    no per-row owner / study-access fallback today. 401 on Anonymous, 403
    on missing scope or insufficient role, 404 on a missing or retired
    sequenced_sample (the retired-row carve-out mirrors the biosample
    GET surface — a future change will let wet_lab_admin+ retrieve
    retired rows so the audit-trail surface is reachable from the API).

    The response carries an `ETag` header derived from
    `effective_updated_at = GREATEST(prep_sample.updated_at,
    sequenced_sample.updated_at)`. The format is a quoted ISO 8601
    timestamp; clients must treat it as opaque.
    """
    # Single SELECT pulls both tables plus the GREATEST timestamp; 404 fires
    # for either missing row or retired prep_sample (same contract as the
    # biosample GET).
    row = await fetch_sequenced_sample_with_prep_sample(pool, sequenced_sample_idx)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"sequenced_sample {sequenced_sample_idx} not found",
        )

    # Retired-row carve-out (see docstring): treat as not found until the
    # planned admin retired-retrieval surface lands.
    if row["retired"]:
        raise HTTPException(
            status_code=404,
            detail=f"sequenced_sample {sequenced_sample_idx} not found",
        )

    # Pull globally-linked metadata for the supertype prep_sample; the
    # repo function handles the global_field_idx IS NOT NULL filter and
    # the data_type-driven value-column dispatch.
    metadata_rows = await fetch_global_metadata_for_prep_sample(pool, row["prep_sample_idx"])
    global_metadata = {
        internal_name: GlobalMetadataEntry(
            display_name=entry.display_name,
            description=entry.description,
            data_type=entry.data_type,
            value=entry.value,
        )
        for internal_name, entry in metadata_rows.items()
    }

    # ETag from the GREATEST-of-both timestamp; opaque-by-contract to clients.
    response.headers["ETag"] = etag_for_updated_at(row["effective_updated_at"])

    return _sequenced_sample_response_from_row(
        row,
        global_metadata=global_metadata,
        caller_system_role=user.system_role,
    )


# Map of constraint names update_sequenced_sample can trip. Only the two
# ENA-accession unique indexes appear here because the subtype-only PATCH
# does not write any FK column; unknown names fall back to the generic
# string on the matching exception path.
_SEQUENCED_SAMPLE_PATCH_UNIQUE_MESSAGES: dict[str, str] = {
    "sequenced_sample_ena_experiment_accession_unique": ("ena_experiment_accession already in use"),
    "sequenced_sample_ena_run_accession_unique": "ena_run_accession already in use",
}
_SEQUENCED_SAMPLE_GENERIC_UNIQUE_VIOLATION = "conflicts with an existing sequenced_sample"


@sequenced_sample_router.patch("/{sequenced_sample_idx}")
async def patch_sequenced_sample_route(
    sequenced_sample_idx: Annotated[int, Field(gt=0)],
    body: SequencedSamplePatchRequest,
    response: Response,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_WRITE)),
) -> SequencedSampleResponse:
    """Edit the four subtype-table columns of a sequenced_sample.

    Auth bar: caller holds Scope.PREP_SAMPLE_WRITE and is a Principal at
    system_role >= wet_lab_admin. The route's intended audience includes
    the ENA submission subsystem (a service account writing back
    accessions), but require_role_at_least currently rejects every
    ServiceAccount because the auth model treats service-account authz
    as scope-only and ServiceAccount carries no system_role field. A
    wider auth-model change (so ServiceAccount carries a role) is
    required before that path opens; until then the runtime caller set
    is humans-only despite the Principal type.

    If-Match is required: missing -> 428, mismatch -> 412. The body's
    editable fields are validated by SequencedSamplePatchRequest
    (extra=forbid rejects supertype prep_sample columns, identity-level
    columns, and unknown names with 422; an empty body is also 422).
    Inside one connection-scoped transaction the route runs a
    `SELECT ... FOR UPDATE` preflight on the joined row (existence ->
    404, retirement -> 409, ETag -> 412); applies the UPDATE on the
    sequenced_sample subtype; re-reads the joined row + global metadata
    for the response. The FOR UPDATE lock is held from preflight through
    commit, so concurrent PATCHes serialize at the preflight: the
    second caller blocks until the first commits, then sees the
    post-commit `effective_updated_at` and 412s on its now-stale
    If-Match header. ENA-accession uniqueness violations map to 409.

    The response carries an `ETag` header derived from the post-update
    `effective_updated_at` = GREATEST(prep_sample.updated_at,
    sequenced_sample.updated_at); format mirrors the GET endpoint's
    contract and is opaque to clients. The schema trigger
    sequenced_sample_clear_submission_error_on_new_attempt nulls
    submission_error when last_submission_at changes unless the same
    UPDATE explicitly sets submission_error; callers recording a failed
    attempt should patch both fields in one request.
    """
    # Missing If-Match is 428 before any DB work runs.
    if if_match is None:
        raise HTTPException(status_code=428, detail="If-Match header required")

    # Build the column-keyed write set from the model's set fields so the
    # repository sees only what the caller explicitly included; explicit
    # null vs. absent is distinguished by model_fields_set.
    fields = {name: getattr(body, name) for name in body.model_fields_set}

    async with tx() as conn:
        try:
            # Preflight: existence -> 404, retirement -> 409, ETag -> 412.
            # for_update=True acquires row-level locks on both joined
            # tables for the rest of the transaction so a concurrent PATCH
            # on the same sequenced_sample serializes here instead of
            # racing through the ETag check and silently overwriting the
            # first writer's update.
            row = await fetch_sequenced_sample_with_prep_sample(
                conn, sequenced_sample_idx, for_update=True
            )
            if row is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"sequenced_sample {sequenced_sample_idx} not found",
                )
            if row["retired"]:
                raise HTTPException(
                    status_code=409,
                    detail=f"sequenced_sample {sequenced_sample_idx} is retired",
                )
            if if_match != etag_for_updated_at(row["effective_updated_at"]):
                raise HTTPException(status_code=412, detail="If-Match did not match")

            # Apply the UPDATE; re-fetch the joined row under the same
            # lock to pick up the bumped effective_updated_at and any
            # trigger-driven side-effects (notably the
            # submission_error-clearing trigger on the subtype).
            await update_sequenced_sample(conn, sequenced_sample_idx, fields=fields)
            updated_row = await fetch_sequenced_sample_with_prep_sample(conn, sequenced_sample_idx)
            if updated_row is None:
                # The FOR UPDATE preflight rules this out — kept as a
                # defensive backstop so an invariant violation fails
                # loudly rather than indexing into None.
                raise HTTPException(
                    status_code=404,
                    detail=f"sequenced_sample {sequenced_sample_idx} not found",
                )

            # Re-read global metadata in the same transaction so the
            # response and the UPDATE see one consistent snapshot.
            metadata_rows = await fetch_global_metadata_for_prep_sample(
                conn, updated_row["prep_sample_idx"]
            )
        except asyncpg.UniqueViolationError as exc:
            detail = _SEQUENCED_SAMPLE_PATCH_UNIQUE_MESSAGES.get(
                exc.constraint_name, _SEQUENCED_SAMPLE_GENERIC_UNIQUE_VIOLATION
            )
            raise HTTPException(status_code=409, detail=detail)

    # Set the new ETag from the updated row's bumped effective_updated_at.
    response.headers["ETag"] = etag_for_updated_at(updated_row["effective_updated_at"])

    # Reuse the GET route's row -> response shaper so PATCH and GET share
    # one source of truth for the response shape.
    global_metadata = {
        internal_name: GlobalMetadataEntry(
            display_name=entry.display_name,
            description=entry.description,
            data_type=entry.data_type,
            value=entry.value,
        )
        for internal_name, entry in metadata_rows.items()
    }
    return _sequenced_sample_response_from_row(
        updated_row,
        global_metadata=global_metadata,
        caller_system_role=caller.system_role,
    )
