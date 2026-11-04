"""Control-plane routes for the sharded-alignment config identity
(`qiita.alignment_definition`).

The `alignment_idx` that keys the DuckLake `alignment` rows is minted at plan time
by the align-plan route (`POST .../sequenced-pool/{P}/align-plan`), so **no route
here mints one** — the DoGet POSTs below sign Flight tickets against an alignment
that already exists.

The /alignment-definition routes are the alignment lifecycle, system_admin-only
(`alignment_definition:lifecycle`): deprecate a config, withdraw or restore
individual runs, and abandon a plan's unfinished blocks. None of them deletes an
alignment; the record of what aligned published data outlives the judgment that
it was wrong.
"""

import base64
import json
from typing import Annotated

import asyncpg
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_ALIGNMENT_COHORT_DOGET,
    PATH_ALIGNMENT_DEFINITION_ABANDON_PLAN,
    PATH_ALIGNMENT_DEFINITION_PREFIX,
    PATH_ALIGNMENT_DEFINITION_SAMPLE_STATUS,
    PATH_ALIGNMENT_DEFINITION_STATUS,
    PATH_ALIGNMENT_DOGET,
    PATH_ALIGNMENT_PREFIX,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    AlignmentCohortDoGetTicketRequest,
    AlignmentDefinition,
    AlignmentDefinitionStatus,
    AlignmentDefinitionStatusUpdate,
    AlignmentDoGetTicketRequest,
    AlignmentPlanAbandonResponse,
    AlignmentSampleStatusUpdate,
    AlignmentSampleStatusUpdateResponse,
    DoGetTicketResponse,
)

from ..actions.align_abandon import AlignPlanInFlight, abandon_align_plan
from ..auth.guards import (
    require_complete_profile,
    require_human,
    require_scope,
    require_service_with_scope,
)
from ..auth.principal import HumanUser, Principal, ServiceAccount
from ..auth.tickets import sign_ticket
from ..deps import (
    TxConnFactory,
    get_data_plane_url,
    get_db_pool,
    get_flight_signing_key,
    get_tx_conn_factory,
)
from ..feature_table import (
    denovo_alignment_processing_idx,
    denovo_assembly_deprecation_error,
    parse_feature_table_denovo,
    parse_feature_table_scope,
)
from ..repositories.alignment_definition import (
    AlignmentDefinitionNotFound,
    AlignmentHasLiveWork,
    alignment_definition_exists,
    fetch_alignment_definition_by_idx,
    refuse_deprecating_with_live_work,
    set_alignment_sample_states,
    transition_alignment_definition_status,
)
from ..repositories.assembly import classify_denovo_arm_cohort
from ..repositories.block import list_incomplete_alignment_samples
from ..repositories.processing import fetch_processing_by_idx
from ._helpers import ALIGNMENT_NOT_FOUND_DETAIL, authorize_completed_alignment_cohort, first_few

# The DuckLake relation this route signs DoGet tickets for: the exclusion-aware
# VIEW (`alignment` ANTI JOIN the resolved blocklist), never the raw base table —
# so a blocked feature's alignment rows never reach the feature-table (OGU)
# consumer. Must stay in sync with the CP-side _DOGET_ALLOWED_TABLES
# (routes/reference.py) and the data plane's ALLOWED_TABLES. A constant so the
# name has one definition here.
_ALIGNMENT_TABLE = "alignment_visible"

alignment_definition_router = APIRouter(
    prefix=PATH_ALIGNMENT_DEFINITION_PREFIX, tags=["alignment-definition"]
)

# Distinct from alignment_definition_router (the /alignment-definition identity
# lifecycle): this serves the /alignment Flight DoGet-ticket surface for the
# alignment sink, mirroring how read_masked.py holds its doget router beside
# mask-definition.
alignment_router = APIRouter(prefix=PATH_ALIGNMENT_PREFIX, tags=["alignment"])


def _alignment_record_to_response(row: asyncpg.Record) -> AlignmentDefinition:
    return AlignmentDefinition.model_validate({**dict(row), "params": _params(row)})


@alignment_definition_router.patch(PATH_ALIGNMENT_DEFINITION_STATUS)
async def update_alignment_definition_status_route(
    alignment_idx: Annotated[int, Field(gt=0)],
    body: AlignmentDefinitionStatusUpdate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.ALIGNMENT_DEFINITION_LIFECYCLE)),
) -> AlignmentDefinition:
    """Deprecate an alignment CONFIG, or return it to active.

    Deprecating stops the alignment being minted against and makes its consumers
    refuse it. Its rows and its per-prep_sample states are left as they are. A block
    alignment's identity has no version in it, so the same reference, aligner and
    mask cannot be aligned again while this row is deprecated; re-aligning means a
    different config.

    Deprecating is refused (409, listing them) while any work ticket under the
    alignment is unfinished, held ones included: they would still write rows under
    a void config.

    system_admin only (`alignment_definition:lifecycle`).
    """
    try:
        async with tx() as conn:
            if body.status is AlignmentDefinitionStatus.DEPRECATED:
                await refuse_deprecating_with_live_work(conn, alignment_idx)
            row = await transition_alignment_definition_status(
                conn,
                alignment_idx=alignment_idx,
                status=body.status,
                reason=body.reason,
                superseded_by=body.superseded_by,
                principal_idx=caller.principal_idx,
            )
    except AlignmentDefinitionNotFound:
        raise HTTPException(status_code=404, detail=ALIGNMENT_NOT_FOUND_DETAIL)
    except AlignmentHasLiveWork as exc:
        raise HTTPException(
            status_code=409,
            detail={"reason": str(exc), "work_ticket_idx": exc.work_ticket_idxs},
        ) from exc
    except asyncpg.ForeignKeyViolationError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"superseded_by names no alignment_definition: {body.superseded_by}",
        ) from exc
    except asyncpg.CheckViolationError as exc:
        # Reachable for superseded_by = alignment_idx, which the wire model cannot
        # see (it does not know the path parameter).
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _alignment_record_to_response(row)


@alignment_definition_router.patch(PATH_ALIGNMENT_DEFINITION_SAMPLE_STATUS)
async def update_alignment_sample_status_route(
    alignment_idx: Annotated[int, Field(gt=0)],
    body: AlignmentSampleStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.ALIGNMENT_DEFINITION_LIFECYCLE)),
) -> AlignmentSampleStatusUpdateResponse:
    """Withdraw (or restore) specific RUNS of one alignment.

    An invalidated `(alignment, prep_sample)` stops being consumable at once: every
    alignment consumer proceeds only on `'completed'`. Bulk and idempotent; the
    response separates what changed, what already held the state, what has no gate
    row under this alignment, and what is still `'pending'` (left alone — a failed
    plan's pending rows are the abandon-plan route's to clear).

    system_admin only (`alignment_definition:lifecycle`).
    """
    if not await alignment_definition_exists(pool, alignment_idx):
        raise HTTPException(status_code=404, detail=ALIGNMENT_NOT_FOUND_DETAIL)
    async with tx() as conn:
        outcome = await set_alignment_sample_states(
            conn,
            alignment_idx=alignment_idx,
            prep_sample_idxs=body.prep_sample_idx,
            state=body.state,
            reason=body.reason,
            principal_idx=caller.principal_idx,
        )
    return AlignmentSampleStatusUpdateResponse(
        alignment_idx=alignment_idx, state=body.state, **outcome
    )


@alignment_definition_router.post(PATH_ALIGNMENT_DEFINITION_ABANDON_PLAN)
async def abandon_alignment_plan_route(
    alignment_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    _caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.ALIGNMENT_DEFINITION_LIFECYCLE)),
) -> AlignmentPlanAbandonResponse:
    """Give up on an align plan's unfinished blocks and clear the pending gates
    they covered, so `only_missing=true` re-plans those prep_samples under the same
    alignment. Completed and invalidated prep_samples are not touched.

    `actions/align_abandon.py` documents the steps. 409 while a released block
    ticket is still running. 502 if the DuckLake purge fails: the abandon itself has
    committed. Calling this again before re-planning redoes the purge; a call after a
    re-plan would abandon that plan's unfinished blocks too, and is not needed, since
    the re-plan's blocks replace their own footprint over the whole prep_sample.

    system_admin only (`alignment_definition:lifecycle`).
    """
    try:
        outcome = await abandon_align_plan(
            pool,
            alignment_idx=alignment_idx,
            signing_key=signing_key,
            data_plane_url=data_plane_url,
        )
    except AlignmentDefinitionNotFound:
        raise HTTPException(status_code=404, detail=ALIGNMENT_NOT_FOUND_DETAIL)
    except AlignPlanInFlight as exc:
        raise HTTPException(
            status_code=409,
            detail={"reason": str(exc), "work_ticket_idx": exc.work_ticket_idxs},
        ) from exc
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "the plan was abandoned but the data plane purge of its prep_samples'"
                " rows failed; call abandon-plan again before re-planning to retry the"
                f" purge: {exc}"
            ),
        ) from exc
    return AlignmentPlanAbandonResponse(**outcome)


@alignment_router.post(PATH_ALIGNMENT_DOGET, status_code=201)
async def create_alignment_doget_ticket(
    body: AlignmentDoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    _sa: ServiceAccount = Depends(require_service_with_scope(Scope.TICKET_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to one alignment run + its explicit
    prep_sample_idx cohort on the data plane's ``alignment`` table.

    Service-account-only (``ticket:doget``) — the feature-table (OGU) compute job
    mints this at runtime (short TTL; a SLURM queue can outlive a submit-time
    ticket). The body carries only ``work_ticket_idx``; ``alignment_idx`` and the
    cohort are read from that ticket's ``action_context`` (set at plan time),
    keeping the potentially large sample list CP-side rather than on the wire.

    Fail loud: a missing ticket is 404; an ``action_context`` that is not a
    feature-table request (no positive ``alignment_idx`` / non-empty
    ``prep_sample_idx`` list of positive ints) is 422, so the data plane's
    single-``alignment_idx`` + non-empty-filter guards are never even reached with
    a bad scope — an unscoped alignment ticket is never signed here.

    ``denovo`` picks WHICH of the ticket's alignment runs to sign. A combined
    (inverted open reference) feature table estimates over two — the cohort against
    a reference, and each sample against its own assembled contigs — and one call
    signs one, so the job asks twice. The cohort is the same for both: it is the
    ticket's, and ``denovo_alignment_idx`` addresses a run whose hashed params carry
    no ``prep_sample_idx``, so one de novo alignment covers the whole cohort.

    ``denovo=true`` against a ticket carrying no ``denovo_alignment_idx`` is 422,
    never a quiet fall back to the reference arm: the job would then stage one slice
    into both arms and reconcile it against itself, which removes every read from
    the reference side and returns a de-novo-only table under the combined name.

    Cohort completeness is validated at SUBMIT time by the feature-table runner
    resolver (``runner/_feature_table.py``) and checked again here (409), because
    the alignment can be deprecated, or a prep_sample invalidated, between submit and
    this runtime mint. A withdrawal landing after this check is not seen by a ticket
    already signed.

    Authorization is scope-only at this layer (matching the reference/read_masked
    doget routes): any service account holding ``ticket:doget`` can request a
    ticket; row-level visibility is not enforced here.
    """
    raw = await pool.fetchval(
        "SELECT action_context FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        body.work_ticket_idx,
    )
    if raw is None:
        raise HTTPException(status_code=404, detail="work ticket not found")
    # asyncpg hands JSONB back as str under the default codec (or a dict if one is
    # registered) — accept both, mirroring the context read in routes/reference.py.
    ctx = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(ctx, dict):
        raise HTTPException(
            status_code=422, detail="work ticket action_context is not a feature-table request"
        )
    # Same scope contract the runner resolver validates at submit — one shared rule
    # (parse_feature_table_scope), each boundary translating to its own error type.
    try:
        alignment_idx, prep_sample_idx = parse_feature_table_scope(ctx)
        denovo_alignment_idx = parse_feature_table_denovo(ctx)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if body.denovo:
        if denovo_alignment_idx is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"work ticket {body.work_ticket_idx} carries no denovo_alignment_idx,"
                    " so there is no de novo arm to sign"
                ),
            )
        covered = await _denovo_arm_covered(
            pool,
            work_ticket_idx=body.work_ticket_idx,
            reference_alignment_idx=alignment_idx,
            denovo_alignment_idx=denovo_alignment_idx,
            prep_sample_idx=prep_sample_idx,
        )
        alignment_idx = denovo_alignment_idx
    else:
        covered = prep_sample_idx
    await _refuse_withdrawn_since_submit(
        pool,
        work_ticket_idx=body.work_ticket_idx,
        alignment_idx=alignment_idx,
        prep_sample_idx=covered,
    )
    return _sign_alignment_ticket(
        alignment_idx=alignment_idx,
        prep_sample_idx=prep_sample_idx,
        columns=body.columns,
        signing_key=signing_key,
    )


@alignment_router.post(PATH_ALIGNMENT_COHORT_DOGET, status_code=201)
async def create_alignment_cohort_doget_ticket(
    alignment_idx: Annotated[int, Field(gt=0)],
    body: AlignmentCohortDoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.ALIGNMENT_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket for an alignment cohort the CALLER names — the
    scientist-facing counterpart of the work-ticket mint above.

    Human-callable (``alignment:doget``, on every role ceiling — see that scope
    for why, including where the reasoning stops applying): the caller must hold
    ``Tier.VIEWER`` on every study each requested prep_sample is still linked to.
    Service accounts are deliberately excluded — a worker has the other route,
    and one surface with two validation paths is what splitting the scopes
    prevented.

    **The signed cohort IS the authorization boundary.** The data plane serves
    exactly the prep_sample_idx list this ticket carries and knows nothing about
    studies or users, so there is no second line of defence behind this
    function; every check below is the only one there is.

    Validation is `authorize_completed_alignment_cohort`, whose docstring is the
    single copy of why its checks run in the order they do; the signing that follows
    shares one helper with the work-ticket route so the two cannot drift. Specific to
    this route and not to that gate: the cohort is never narrowed because coverage
    filtering makes a feature table cohort-dependent, so trimming the request would
    answer a different scientific question under the name of the one asked.
    """
    cohort = await authorize_completed_alignment_cohort(
        pool,
        caller=caller,
        alignment_idx=alignment_idx,
        prep_sample_idx=body.prep_sample_idx,
        nothing_to="",
    )

    return _sign_alignment_ticket(
        alignment_idx=alignment_idx,
        prep_sample_idx=cohort,
        columns=body.columns,
        signing_key=signing_key,
    )


async def _denovo_arm_covered(
    pool: asyncpg.Pool,
    *,
    work_ticket_idx: int,
    reference_alignment_idx: int,
    denovo_alignment_idx: int,
    prep_sample_idx: list[int],
) -> list[int]:
    """The cohort members the de novo arm must cover: the assembled ones, as decided
    at submit (`classify_denovo_arm_cohort`). A `no_data` member has no de novo
    alignment row and is reference-only, so it is not re-checked against that
    alignment. 409 when the assembly run was deprecated since submit (the wording is
    `denovo_assembly_deprecation_error`, as at submit), or a member's assembly gate
    has since left `completed` / `no_data`."""
    denovo = await fetch_alignment_definition_by_idx(pool, denovo_alignment_idx)
    reference = await fetch_alignment_definition_by_idx(pool, reference_alignment_idx)
    if denovo is None or reference is None:
        raise HTTPException(status_code=404, detail=ALIGNMENT_NOT_FOUND_DETAIL)
    try:
        processing_idx = denovo_alignment_processing_idx(
            denovo_alignment_idx=denovo_alignment_idx,
            denovo_params=_params(denovo),
            reference_params=_params(reference),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    run = await fetch_processing_by_idx(pool, processing_idx)
    if run is None:
        raise HTTPException(status_code=404, detail=f"assembly run {processing_idx} not found")
    deprecated = denovo_assembly_deprecation_error(
        processing_idx=processing_idx, status=run["status"], superseded_by=run["superseded_by"]
    )
    if deprecated is not None:
        raise HTTPException(status_code=409, detail=deprecated)
    cohort = await classify_denovo_arm_cohort(
        pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
    )
    if cohort.unusable:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(cohort.unusable)} prep_sample(s) of work ticket {work_ticket_idx} are"
                f" no longer 'completed' or 'no_data' in assembly run {processing_idx}"
                f" (e.g. {first_few(sorted(cohort.unusable))})"
            ),
        )
    return cohort.expected


def _params(row: asyncpg.Record) -> object:
    """`alignment_definition.params`, decoded: asyncpg returns JSONB as str under the
    default codec, or already decoded when a codec is registered."""
    params = row["params"]
    return json.loads(params) if isinstance(params, str) else params


async def _refuse_withdrawn_since_submit(
    pool: asyncpg.Pool, *, work_ticket_idx: int, alignment_idx: int, prep_sample_idx: list[int]
) -> None:
    """409 when the alignment the ticket was validated against at submit has since
    been deprecated, or any of its cohort is no longer 'completed'."""
    row = await fetch_alignment_definition_by_idx(pool, alignment_idx)
    if row is None:
        raise HTTPException(status_code=404, detail=ALIGNMENT_NOT_FOUND_DETAIL)
    if row["status"] == AlignmentDefinitionStatus.DEPRECATED.value:
        raise HTTPException(
            status_code=409,
            detail=(
                f"alignment {alignment_idx} was deprecated after work ticket"
                f" {work_ticket_idx} was submitted: {row['deprecation_reason']}"
            ),
        )
    incomplete = await list_incomplete_alignment_samples(pool, alignment_idx, prep_sample_idx)
    if incomplete:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(incomplete)} prep_sample(s) of work ticket {work_ticket_idx} are no"
                f" longer completed for alignment {alignment_idx} (e.g."
                f" {first_few(incomplete)})"
            ),
        )


def _sign_alignment_ticket(
    *,
    alignment_idx: int,
    prep_sample_idx: list[int],
    columns: list[str] | None,
    signing_key: bytes,
) -> DoGetTicketResponse:
    """Sign the alignment DoGet ticket both mint routes return.

    Shared so the table name, the filter shape, and the 422-on-bad-projection
    translation have one definition — two mint routes for one data-plane surface
    is otherwise an invitation to drift. The projection allowlist itself lives
    at the signing boundary (`auth/tickets.py`), not here, so no route can mint
    an unvalidated one.
    """
    try:
        ticket_bytes = sign_ticket(
            table=_ALIGNMENT_TABLE,
            filter={"alignment_idx": [alignment_idx], "prep_sample_idx": prep_sample_idx},
            columns=columns,
            secret=signing_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
