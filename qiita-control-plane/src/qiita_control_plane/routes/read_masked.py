"""Masked-read routes: mask_idx minting + reads + the masked-read DoGet ticket.

Two routers live here because they are the two halves of one feature:

* ``POST /mask-definition`` mints (idempotently, deduped on a canonical-config
  hash) the ``mask_idx`` that identifies a read-filtering config. Same config →
  same ``mask_idx`` fleet-wide.
* ``POST /read-masked/ticket/doget`` signs an Ed25519 DoGet ticket scoped to a
  single ``(prep_sample_idx, mask_idx)`` on the data plane's ``read_masked``
  macro — the only Flight-reachable read surface (raw ``read``/``read_mask`` are
  out of Flight by construction, so unmasked/human reads are unreachable).

Both are service-account-only, gated on ``Scope.READ_MASKED_DOGET``. Humans
never mint masks or pull masked reads — the masked-read consumer path is
service-driven, and the lake retains privacy-sensitive (human/host) reads that
the ``read_masked`` macro excludes only via an unconditional ``reason='pass'``.

**The three GETs are the human read surface, and are gated differently.** They
answer which masks exist, what config a mask encodes, and which samples are
masked under it — a ``mask_idx`` is required to submit ``long-read-assembly``,
whose audience includes a plain ``user``, so an admin-only discovery path would
put the workflow out of reach of its own audience. They carry filter metadata and
per-sample completion state, never read data, so they sit at
``Scope.PREP_SAMPLE_READ`` (held by every human role) at ``require_human``.

A caller below ``wet_lab_admin`` sees only samples they could submit against:
the same per-study policy the submission gate applies
(``_check_prep_sample_study_access`` — Tier.ADMIN on every non-retired linked
study), pushed into the query as a predicate so the narrowing happens in one
round trip. Narrowing also restricts *which masks* the list returns, so a
zero-tally row never reveals a mask whose samples were all filtered out.

**Mandatory-filter invariant — now defence in depth, and keep it that way.**
This route MUST inject a non-empty ``prep_sample_idx`` AND a ``mask_idx`` into
every signed ticket. Pydantic's ``gt=0`` on both fields makes an empty/zero
filter unrepresentable at the request layer; the route re-asserts non-empty
before signing.

Historically this was the *only* thing standing between a mis-signed ticket and
a fleet-wide read. It no longer is — the data plane's ``read_masked`` macro
forecloses that read on its own (see its comment in
``qiita-data-plane/src/ducklake.rs``). **Do not delete these checks on that
basis.** They fail a bad ticket at signing time rather than at DoGet time, which
is where a scoping bug should surface, and they keep the guarantee independent of
the data plane's query construction.
"""

import base64
import json
from typing import Annotated

import asyncpg
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import Field
from qiita_common.api_paths import (
    PATH_MASK_DEFINITION_BY_IDX,
    PATH_MASK_DEFINITION_PREFIX,
    PATH_MASK_DEFINITION_PREP_SAMPLE,
    PATH_MASK_DEFINITION_ROOT,
    PATH_MASK_DEFINITION_SAMPLE_STATUS,
    PATH_MASK_DEFINITION_STATUS,
    PATH_READ_MASKED_DOGET,
    PATH_READ_MASKED_PREFIX,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DoGetTicketResponse,
    MaskDefinition,
    MaskDefinitionDeleteResponse,
    MaskDefinitionListResponse,
    MaskDefinitionMintRequest,
    MaskDefinitionStatus,
    MaskDefinitionStatusUpdate,
    MaskDefinitionSummary,
    MaskPrepSample,
    MaskPrepSampleListResponse,
    MaskSampleStatusUpdate,
    MaskSampleStatusUpdateResponse,
    ReadMaskedDoGetTicketRequest,
)

from ..actions.library import delete_mask_data
from ..auth.guards import require_human, require_scope, require_service_with_scope
from ..auth.principal import HumanUser, Principal, ServiceAccount
from ..auth.tickets import sign_ticket
from ..block_read import READ_MASKED_TABLE
from ..deps import (
    TxConnFactory,
    get_data_plane_url,
    get_db_pool,
    get_flight_signing_key,
    get_tx_conn_factory,
)
from ..repositories.block import fetch_mask_sample_state
from ..repositories.mask_definition import (
    MaskDefinitionDeprecated,
    MaskDefinitionNotFound,
    fetch_mask_definition_by_idx,
    fetch_mask_prep_samples,
    list_mask_definitions,
    mint_mask_definition,
    set_mask_sample_states,
    transition_mask_definition_status,
)
from ._helpers import cap_rows, gate_roster_narrowing_idx

_MSG_MASK_NOT_FOUND = "Mask definition not found"

# Hard caps on the two mask reads. The mask list is bounded by how many distinct
# read-filtering configs the fleet has minted; the roster by a pool's sample
# count. Both return `truncated` rather than paginating — a caller that hits
# either cap should narrow with a filter.
_MASK_LIST_HARD_CAP = 1_000
_MASK_PREP_SAMPLE_HARD_CAP = 100_000


mask_definition_router = APIRouter(prefix=PATH_MASK_DEFINITION_PREFIX, tags=["mask-definition"])
read_masked_router = APIRouter(prefix=PATH_READ_MASKED_PREFIX, tags=["read-masked"])


def _mask_record_fields(row: asyncpg.Record) -> dict:
    """Project a qiita.mask_definition asyncpg.Record onto the MaskDefinition
    fields, as a dict the list view extends with its tally.

    `params` is JSONB — asyncpg returns it as a JSON string by default, so
    parse it back to a dict for the wire model. Field access is by name so a
    future column add can't silently shift the projection.
    """
    params = row["params"]
    if isinstance(params, str):
        params = json.loads(params)
    return {
        "mask_idx": row["mask_idx"],
        "filter_workflow": row["filter_workflow"],
        "filter_version": row["filter_version"],
        "params": params,
        "created_at": row["created_at"],
        "status": row["status"],
        "deprecated_at": row["deprecated_at"],
        "deprecated_by_idx": row["deprecated_by_idx"],
        "deprecation_reason": row["deprecation_reason"],
        "superseded_by": row["superseded_by"],
    }


def _mask_record_to_response(row: asyncpg.Record) -> MaskDefinition:
    """The single-mask response: mint, and the by-idx read."""
    return MaskDefinition(**_mask_record_fields(row))


@mask_definition_router.post(PATH_MASK_DEFINITION_ROOT, status_code=201)
async def mint_mask_definition_route(
    body: MaskDefinitionMintRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    sa: ServiceAccount = Depends(require_service_with_scope(Scope.READ_MASKED_DOGET)),
) -> MaskDefinition:
    """Mint (or return the existing) mask_idx for a read-filtering config.

    Idempotent: the same `params` (canonical-JSON hashed) always returns the
    same mask_idx, so a 201 may carry a pre-existing row. Caller must be a
    ServiceAccount holding `read_masked:doget`.

    Maps repository-layer exceptions to HTTP status:
      - asyncpg.ForeignKeyViolationError → 400 (unknown principal_idx; defence
        in depth — the principal is the authenticated service account).
      - asyncpg.InvalidParameterValueError (SQLSTATE 22023) → 400 (a non-32-byte
        hash; the repository helper always passes a 32-byte digest).
      - Any other asyncpg.PostgresError → 500 with a generic detail.
    """
    async with tx() as conn:
        try:
            row = await mint_mask_definition(
                conn,
                filter_workflow=body.filter_workflow,
                filter_version=body.filter_version,
                params=body.params,
                principal_idx=sa.principal_idx,
            )
        except MaskDefinitionDeprecated as exc:
            # The config is void: minting would put new data behind a filter we no
            # longer stand behind. 409 rather than 400 — the request is well-formed
            # and would have succeeded before the mask was deprecated.
            raise HTTPException(status_code=409, detail=exc.detail) from exc
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(status_code=400, detail="invalid principal for mask mint")
        except asyncpg.InvalidParameterValueError:
            raise HTTPException(status_code=400, detail="invalid mask-definition parameters")
        except asyncpg.PostgresError:
            raise HTTPException(status_code=500, detail="database error")

    return _mask_record_to_response(row)


@mask_definition_router.get(PATH_MASK_DEFINITION_ROOT)
async def list_mask_definitions_route(
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    sequenced_pool_idx: int | None = Query(
        default=None,
        gt=0,
        description="Only masks with at least one sample on this sequenced_pool.",
    ),
    prep_sample_idx: int | None = Query(
        default=None,
        gt=0,
        description="Only masks this prep_sample is masked under.",
    ),
    status: MaskDefinitionStatus | None = Query(
        default=None,
        description=(
            "Only masks with this config lifecycle status. Omitted, BOTH are"
            " listed — a deprecated mask stays discoverable so what filtered"
            " published data remains answerable."
        ),
    ),
) -> MaskDefinitionListResponse:
    """List read-filtering masks, newest first, each with its sample tally.

    The tally (`samples_completed` / `samples_pending`) is scoped to the same
    filters as the list and counts the same rows the roster read returns. A pool
    carrying several masks is separable from this one call: `params` distinguishes
    them by config (a non-null `host_rype_reference_idx` is the human-filtered
    one) and the tally says which is usable.

    Caller must be a HumanUser with `Scope.PREP_SAMPLE_READ`. Below
    wet_lab_admin the result narrows to masks over samples the caller has
    study-admin on.
    """
    rows, truncated = cap_rows(
        await list_mask_definitions(
            pool,
            sequenced_pool_idx=sequenced_pool_idx,
            prep_sample_idx=prep_sample_idx,
            status=status,
            visible_to_principal_idx=gate_roster_narrowing_idx(caller),
            limit=_MASK_LIST_HARD_CAP + 1,
        ),
        _MASK_LIST_HARD_CAP,
    )
    masks = [
        MaskDefinitionSummary(
            **_mask_record_fields(row),
            samples_completed=row["samples_completed"],
            samples_pending=row["samples_pending"],
            samples_invalidated=row["samples_invalidated"],
        )
        for row in rows
    ]
    return MaskDefinitionListResponse(
        masks=masks,
        count=len(masks),
        truncated=truncated,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=prep_sample_idx,
    )


@mask_definition_router.get(PATH_MASK_DEFINITION_BY_IDX)
async def get_mask_definition_route(
    mask_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> MaskDefinition:
    """Read one mask's config. 404 if it does not exist.

    `params` is the config the mask was minted from — its host/spike-in reference
    idxs and resolved QC constants — so this is how a caller states what a mask
    filtered on without reading the orchestrator source. See the model docstring
    for what `params` does and does not cover.

    Unnarrowed: the row is one config blob with no sample in it, so there is no
    per-study set to narrow. The samples masked under it are the roster read,
    which does narrow.
    """
    row = await fetch_mask_definition_by_idx(pool, mask_idx)
    if row is None:
        raise HTTPException(status_code=404, detail=_MSG_MASK_NOT_FOUND)
    return _mask_record_to_response(row)


@mask_definition_router.get(PATH_MASK_DEFINITION_PREP_SAMPLE)
async def list_mask_prep_samples_route(
    mask_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    sequenced_pool_idx: int | None = Query(
        default=None,
        gt=0,
        description="Only samples on this sequenced_pool.",
    ),
) -> MaskPrepSampleListResponse:
    """The samples masked under one mask, ascending by prep_sample_idx, each with
    its masking state. 404 if the mask does not exist.

    Reads the `mask_sample` gate row where one exists, and the sample's own
    per-sample masking ticket where it does not — which on that path means the
    ticket has not completed, a state the gate table cannot represent. `source`
    says which answered; `work_ticket_state` separates a running ticket from a
    failed one.

    Existence is checked before the roster read, so a typo'd mask_idx 404s rather
    than returning an empty roster. An empty roster on a real mask means no
    sample the caller may see is masked under it.

    Caller must be a HumanUser with `Scope.PREP_SAMPLE_READ`. Below
    wet_lab_admin the roster narrows to samples the caller has study-admin on.
    """
    if await fetch_mask_definition_by_idx(pool, mask_idx) is None:
        raise HTTPException(status_code=404, detail=_MSG_MASK_NOT_FOUND)
    rows, truncated = cap_rows(
        await fetch_mask_prep_samples(
            pool,
            mask_idx,
            sequenced_pool_idx=sequenced_pool_idx,
            visible_to_principal_idx=gate_roster_narrowing_idx(caller),
            limit=_MASK_PREP_SAMPLE_HARD_CAP + 1,
        ),
        _MASK_PREP_SAMPLE_HARD_CAP,
    )
    samples = [MaskPrepSample.model_validate(dict(row)) for row in rows]
    return MaskPrepSampleListResponse(
        mask_idx=mask_idx,
        samples=samples,
        count=len(samples),
        truncated=truncated,
        sequenced_pool_idx=sequenced_pool_idx,
    )


@mask_definition_router.delete(PATH_MASK_DEFINITION_BY_IDX)
async def delete_mask_definition_route(
    mask_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    _scope: Principal = Depends(require_scope(Scope.MASK_DEFINITION_DELETE)),
) -> MaskDefinitionDeleteResponse:
    """Fully purge a mask — its DuckLake `read_mask` rows then its Postgres
    `mask_definition` row. system_admin only (`mask_definition:delete`).

    Order of operations: existence check first (404 if the mask is absent) →
    DuckLake delete (one all-or-nothing transaction; a 502 on failure removes
    nothing yet, since the existence check is a read and the Postgres delete
    hasn't run) → Postgres `mask_definition` delete last. This ordering makes the
    operation *retriable*: both mutating steps are idempotent and the
    `qiita.mask_definition` row — the thing a retry keys off — is removed last. If
    the lake delete succeeds but the Postgres delete fails, the mask row survives
    and re-issuing the DELETE re-runs both idempotent steps (the second lake
    delete removes 0 rows). A crash therefore leaves at worst a recoverable
    orphan-Postgres row, never an unrecoverable orphan-lake.

    Referencing `qiita.work_ticket` rows detach automatically — the
    `work_ticket.mask_idx` FK is `ON DELETE SET NULL` — so no work-ticket touch
    is needed here.

    Intentional divergence from reference-delete: that route gates on in-flight
    work_tickets (409 via `assert_reference_deletable`); this primitive
    deliberately does NOT. It is an admin-only sharp primitive that lets the FK
    detach any referencing ticket. The shared-mask SAFETY guard — don't delete a
    mask still referenced by a non-failed ticket — lives in the bulk
    purge-failed tool that wraps this route, not in the primitive itself. The
    absence of gating here is a conscious decision, not an oversight.
    """
    # Existence check (a read; safe before the lake delete).
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=_MSG_MASK_NOT_FOUND)

    # DuckLake read_mask rows (idempotent, atomic delete-by-mask_idx in the data
    # plane). Lake-first so a crash before the Postgres delete leaves a
    # recoverable orphan-Postgres row, not an unrecoverable orphan-lake.
    try:
        rows_deleted = await delete_mask_data(
            mask_idx=mask_idx,
            signing_key=signing_key,
            data_plane_url=data_plane_url,
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"data plane mask delete failed; nothing removed yet: {exc}",
        ) from exc

    # Postgres row last. The work_ticket FK is ON DELETE SET NULL, so referencing
    # tickets detach automatically — no need to touch work_ticket here.
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)

    return MaskDefinitionDeleteResponse(mask_idx=mask_idx, rows_deleted=rows_deleted)


@mask_definition_router.patch(PATH_MASK_DEFINITION_STATUS)
async def update_mask_definition_status_route(
    mask_idx: Annotated[int, Field(gt=0)],
    body: MaskDefinitionStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.MASK_DEFINITION_LIFECYCLE)),
) -> MaskDefinition:
    """Deprecate a read-filtering CONFIG, or return it to active.

    Deprecating stops the mask being minted against, so no NEW data can be masked
    under it. It changes nothing about data already masked: the mask keeps its
    rows, its GET keeps answering, and the per-sample runs keep whatever state they
    held. Withdrawing individual runs is the sample-status route below; the two are
    different judgements.

    Deletion is the other tool and the wrong one here: `read_masked` is a
    query-time macro, so dropping read_mask removes the record of the filtering
    decision while leaving the raw reads Flight-reachable.

    system_admin only (`mask_definition:lifecycle`).
    """
    try:
        row = await transition_mask_definition_status(
            pool,
            mask_idx=mask_idx,
            status=body.status,
            reason=body.reason,
            superseded_by=body.superseded_by,
            principal_idx=caller.principal_idx,
        )
    except MaskDefinitionNotFound:
        raise HTTPException(status_code=404, detail=_MSG_MASK_NOT_FOUND)
    except asyncpg.ForeignKeyViolationError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"superseded_by names no mask_definition: {body.superseded_by}",
        ) from exc
    except asyncpg.CheckViolationError as exc:
        # Reachable for superseded_by = mask_idx, which the wire model cannot see
        # (it does not know the path parameter).
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return _mask_record_to_response(row)


@mask_definition_router.patch(PATH_MASK_DEFINITION_SAMPLE_STATUS)
async def update_mask_sample_status_route(
    mask_idx: Annotated[int, Field(gt=0)],
    body: MaskSampleStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.MASK_DEFINITION_LIFECYCLE)),
) -> MaskSampleStatusUpdateResponse:
    """Withdraw (or restore) specific RUNS of one mask.

    A sound config can produce an untrustworthy run — the measured case being a
    job that OOM-escalated into a larger Arrow batch — so this is the granularity
    the judgement is actually made at. An invalidated `(mask, sample)` stops being
    consumable immediately: every masked-read consumer proceeds only on
    `'completed'`, so a third state is refused by construction rather than by a
    check each of them had to remember.

    Bulk, because the judgement is made per cohort. Idempotent, and the response
    separates what changed from what already held the state, what has no gate row
    under this mask, and what is still `'pending'` (left alone — the masking
    pipeline owns that value and would overwrite a withdrawal).

    system_admin only (`mask_definition:lifecycle`).
    """
    if await fetch_mask_definition_by_idx(pool, mask_idx) is None:
        raise HTTPException(status_code=404, detail=_MSG_MASK_NOT_FOUND)
    async with tx() as conn:
        outcome = await set_mask_sample_states(
            conn,
            mask_idx=mask_idx,
            prep_sample_idxs=body.prep_sample_idx,
            state=body.state,
            reason=body.reason,
            principal_idx=caller.principal_idx,
        )
    return MaskSampleStatusUpdateResponse(mask_idx=mask_idx, state=body.state, **outcome)


@read_masked_router.post(PATH_READ_MASKED_DOGET, status_code=201)
async def create_read_masked_doget_ticket(
    body: ReadMaskedDoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    sa: ServiceAccount = Depends(require_service_with_scope(Scope.READ_MASKED_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to (prep_sample_idx, mask_idx) on read_masked.

    Caller must be a ServiceAccount holding `read_masked:doget`. The ticket
    filters the data plane's read_masked macro to exactly one sample under
    exactly one mask config; the macro's unconditional `reason='pass'` excludes human/host
    reads by construction.

    Mandatory-filter invariant: both identifiers are required and positive
    (Pydantic gt=0), so the signed filter is always non-empty. The route
    re-asserts this before signing — an unfiltered read_masked ticket is never
    signed, which would otherwise dump every sample's pass reads fleet-wide.

    Completion gate (contract: see `fetch_mask_sample_state`): the sample must be
    'completed' under this `mask_idx`, else 409 — a 'pending' row (a covering block
    still in flight) or NO row (absence is not exempt) both mean the read_masked
    pass-set would be absent or partial, and a pull would silently truncate. This
    keeps the invariant uniform with the human export ticket route: EVERY path that
    mints a read_masked ticket requires 'completed'.
    """
    filter_ = {
        "prep_sample_idx": [body.prep_sample_idx],
        "mask_idx": [body.mask_idx],
    }
    # Defence in depth against the mandatory-filter invariant: never sign a
    # read_masked ticket whose filter (or any filter value list) is empty.
    if not filter_ or any(not v for v in filter_.values()):
        raise HTTPException(
            status_code=422,
            detail="read_masked ticket requires a non-empty prep_sample_idx and mask_idx filter",
        )

    async with pool.acquire() as conn:
        mask_state = await fetch_mask_sample_state(
            conn, mask_idx=body.mask_idx, prep_sample_idx=body.prep_sample_idx
        )
    if mask_state != "completed":
        raise HTTPException(
            status_code=409,
            detail={
                "reason": (
                    "the sample is not masked-complete under this mask_idx "
                    f"(mask_sample.state={mask_state!r}). Either no read-mask has "
                    "completed for this (prep_sample, mask_idx), a covering block is "
                    "still in flight, or the run was withdrawn as untrustworthy "
                    "('invalidated'). The read_masked pass-set would be absent, "
                    "partial, or unfit. Refusing to sign a ticket; a withdrawn run is not "
                    "retryable — re-mask under a corrected config."
                ),
                "prep_sample_idx": body.prep_sample_idx,
                "mask_idx": body.mask_idx,
                "mask_state": mask_state,
            },
        )

    ticket_bytes = sign_ticket(
        table=READ_MASKED_TABLE,
        filter=filter_,
        secret=signing_key,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
