"""Processing-run identity (`qiita.processing`) reads and lifecycle writes.

A `processing_idx` is the canonical-params hash identifying one processing run.
The runner mints it before the step loop, so there is no POST here: at HTTP
submit time there is nothing to key on. What this router carries is the surface
that was missing around that identity — three reads and the two lifecycle
PATCHes. Today the only workflow threading a `processing_idx` is
`long-read-assembly`, which is why the gate these reads report is
`qiita.assembly_sample`.

**The three GETs are the human read surface.** They answer which runs exist, what
params a run encodes, and which samples it assembled. They carry run metadata and
per-sample gate state, never sequence data, so they sit at
``Scope.PREP_SAMPLE_READ`` (held by every human role) at ``require_human``, the
same gating ``/mask-definition``'s reads use and for the same reason: a
``processing_idx`` is what a de novo alignment is submitted against, and an
admin-only discovery path would put that workflow out of reach of its own
audience. The contig bytes stay where they were — ``POST
/assembly/ticket/doget``, service-account-only.

A caller below ``wet_lab_admin`` sees only samples they could submit against, via
the shared ``repositories._sample_scope``. Narrowing also restricts *which runs*
the list returns, so a zero-tally row never reveals a run whose samples were all
filtered out.

**The two PATCHes are system_admin-only** (``processing:lifecycle``). Which
question each granularity answers, and why the two are separate, is in the
``assembly_lifecycle`` migration header.
"""

import json
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import Field
from qiita_common.api_paths import (
    PATH_PROCESSING_BY_IDX,
    PATH_PROCESSING_PREFIX,
    PATH_PROCESSING_PREP_SAMPLE,
    PATH_PROCESSING_ROOT,
    PATH_PROCESSING_SAMPLE_STATUS,
    PATH_PROCESSING_STATUS,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    AssemblySampleStatusUpdate,
    AssemblySampleStatusUpdateResponse,
    Processing,
    ProcessingListResponse,
    ProcessingPrepSample,
    ProcessingPrepSampleListResponse,
    ProcessingStatus,
    ProcessingStatusUpdate,
    ProcessingSummary,
)

from ..auth.guards import require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.processing import (
    ProcessingNotFound,
    fetch_processing_by_idx,
    fetch_processing_prep_samples,
    list_processing,
    set_assembly_sample_states,
    transition_processing_status,
)
from ._helpers import cap_rows, gate_roster_narrowing_idx

_MSG_PROCESSING_NOT_FOUND = "Processing run not found"

# Hard caps on the two reads. The run list is bounded by how many distinct
# assembly param sets the fleet has minted; the roster by a pool's sample count.
# Both return `truncated` rather than paginating — a caller that hits either cap
# should narrow with a filter. Same values as the mask twin, which bounds the same
# two shapes.
_PROCESSING_LIST_HARD_CAP = 1_000
_PROCESSING_PREP_SAMPLE_HARD_CAP = 100_000

processing_router = APIRouter(prefix=PATH_PROCESSING_PREFIX, tags=["processing"])


def _processing_record_fields(row: asyncpg.Record) -> dict:
    """Project a qiita.processing asyncpg.Record onto the Processing fields, as a
    dict the list view extends with its tally.

    `params` is JSONB — asyncpg returns it as a JSON string by default, so parse
    it back to a dict for the wire model. Field access is by name so a future
    column add can't silently shift the projection.
    """
    params = row["params"]
    if isinstance(params, str):
        params = json.loads(params)
    return {
        "processing_idx": row["processing_idx"],
        "workflow": row["workflow"],
        "version": row["version"],
        "params": params,
        "created_at": row["created_at"],
        "status": row["status"],
        "deprecated_at": row["deprecated_at"],
        "deprecated_by_idx": row["deprecated_by_idx"],
        "deprecation_reason": row["deprecation_reason"],
        "superseded_by": row["superseded_by"],
    }


@processing_router.get(PATH_PROCESSING_ROOT)
async def list_processing_route(
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    sequenced_pool_idx: int | None = Query(
        default=None,
        gt=0,
        description="Only runs with at least one sample on this sequenced_pool.",
    ),
    prep_sample_idx: int | None = Query(
        default=None,
        gt=0,
        description="Only runs this prep_sample was assembled under.",
    ),
    status: ProcessingStatus | None = Query(
        default=None,
        description=(
            "Only runs with this config lifecycle status. Omitted, BOTH are"
            " listed — a deprecated run stays discoverable so what assembled"
            " published contigs remains answerable."
        ),
    ),
) -> ProcessingListResponse:
    """List assembly runs, newest first, each with its sample tally.

    The tally is scoped to the same filters as the list and counts the same rows
    the roster read returns. A pool assembled under several identities is
    separable from this one call: `params` distinguishes them by config (which
    `mask_idx`, which assembler) and the tally says which is usable.

    Caller must be a HumanUser with `Scope.PREP_SAMPLE_READ`. Below wet_lab_admin
    the result narrows to runs over samples the caller has study-admin on.
    """
    rows, truncated = cap_rows(
        await list_processing(
            pool,
            sequenced_pool_idx=sequenced_pool_idx,
            prep_sample_idx=prep_sample_idx,
            status=status,
            visible_to_principal_idx=gate_roster_narrowing_idx(caller),
            limit=_PROCESSING_LIST_HARD_CAP + 1,
        ),
        _PROCESSING_LIST_HARD_CAP,
    )
    runs = [
        ProcessingSummary(
            **_processing_record_fields(row),
            samples_completed=row["samples_completed"],
            samples_pending=row["samples_pending"],
            samples_no_data=row["samples_no_data"],
            samples_invalidated=row["samples_invalidated"],
        )
        for row in rows
    ]
    return ProcessingListResponse(
        processing=runs,
        count=len(runs),
        truncated=truncated,
        sequenced_pool_idx=sequenced_pool_idx,
        prep_sample_idx=prep_sample_idx,
    )


@processing_router.get(PATH_PROCESSING_BY_IDX)
async def get_processing_route(
    processing_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> Processing:
    """Read one assembly run's params and lifecycle. 404 if it does not exist.

    `params` is what the identity was hashed from, so this is how a caller states
    which mask's pass-set a contig set was assembled from, and with which
    assembler, without reading the orchestrator source. See the model docstring
    for what `params` does and does not cover.

    Unnarrowed: the row is one params blob with no sample in it, so there is no
    per-study set to narrow. The samples assembled under it are the roster read,
    which does narrow.
    """
    row = await fetch_processing_by_idx(pool, processing_idx)
    if row is None:
        raise HTTPException(status_code=404, detail=_MSG_PROCESSING_NOT_FOUND)
    return Processing(**_processing_record_fields(row))


@processing_router.get(PATH_PROCESSING_PREP_SAMPLE)
async def list_processing_prep_samples_route(
    processing_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    sequenced_pool_idx: int | None = Query(
        default=None,
        gt=0,
        description="Only samples on this sequenced_pool.",
    ),
) -> ProcessingPrepSampleListResponse:
    """The samples assembled under one run, ascending by prep_sample_idx, each
    with its gate state. 404 if the run does not exist.

    Reads the `assembly_sample` gate and nothing else. Where the mask roster
    falls back to a sample's own masking ticket for pairs the gate cannot
    represent, there is no such fallback here: no `work_ticket` column carries a
    `processing_idx`, so a ticket cannot be attributed to a run. A sample with no
    gate row is absent from this roster rather than shown in some other state.

    Existence is checked before the roster read, so a typo'd processing_idx 404s
    rather than returning an empty roster. An empty roster on a real run means no
    sample the caller may see is gated under it.

    Caller must be a HumanUser with `Scope.PREP_SAMPLE_READ`. Below wet_lab_admin
    the roster narrows to samples the caller has study-admin on.
    """
    if await fetch_processing_by_idx(pool, processing_idx) is None:
        raise HTTPException(status_code=404, detail=_MSG_PROCESSING_NOT_FOUND)
    rows, truncated = cap_rows(
        await fetch_processing_prep_samples(
            pool,
            processing_idx,
            sequenced_pool_idx=sequenced_pool_idx,
            visible_to_principal_idx=gate_roster_narrowing_idx(caller),
            limit=_PROCESSING_PREP_SAMPLE_HARD_CAP + 1,
        ),
        _PROCESSING_PREP_SAMPLE_HARD_CAP,
    )
    samples = [ProcessingPrepSample.model_validate(dict(row)) for row in rows]
    return ProcessingPrepSampleListResponse(
        processing_idx=processing_idx,
        samples=samples,
        count=len(samples),
        truncated=truncated,
        sequenced_pool_idx=sequenced_pool_idx,
    )


@processing_router.patch(PATH_PROCESSING_STATUS)
async def update_processing_status_route(
    processing_idx: Annotated[int, Field(gt=0)],
    body: ProcessingStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PROCESSING_LIFECYCLE)),
) -> Processing:
    """Deprecate a processing run CONFIG, or return it to active.

    Deprecating stops the identity being minted against. It changes nothing that
    already exists: the run keeps its DuckLake rows, every read here keeps
    answering, the assembly DoGet ticket keeps signing, and the per-sample gate
    rows keep whatever state they held. Withdrawing individual runs is the
    sample-status route below.

    All four provenance columns move together — a re-deprecation replaces the
    whole block, `superseded_by` included, so correcting a reason without
    re-supplying the replacement clears it.

    system_admin only (`processing:lifecycle`).
    """
    try:
        row = await transition_processing_status(
            pool,
            processing_idx=processing_idx,
            status=body.status,
            reason=body.reason,
            superseded_by=body.superseded_by,
            principal_idx=caller.principal_idx,
        )
    except ProcessingNotFound:
        raise HTTPException(status_code=404, detail=_MSG_PROCESSING_NOT_FOUND)
    except asyncpg.ForeignKeyViolationError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"superseded_by names no processing run: {body.superseded_by}",
        ) from exc
    except asyncpg.CheckViolationError as exc:
        # Only `processing_supersede_not_self` is reachable: the wire model gates
        # every other CHECK on this UPDATE, and it cannot see the path parameter.
        raise HTTPException(
            status_code=422,
            detail=f"superseded_by cannot name the run itself: {body.superseded_by}",
        ) from exc
    return Processing(**_processing_record_fields(row))


@processing_router.patch(PATH_PROCESSING_SAMPLE_STATUS)
async def update_assembly_sample_status_route(
    processing_idx: Annotated[int, Field(gt=0)],
    body: AssemblySampleStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PROCESSING_LIFECYCLE)),
) -> AssemblySampleStatusUpdateResponse:
    """Withdraw (or restore) specific RUNS of one processing identity.

    Bulk, because the judgement is made per cohort. Idempotent, and the response
    separates what changed from what already held the state, what has no gate row
    under this run, and the two states left alone; which states those are, and
    why, is on `repositories.processing.set_assembly_sample_states`.

    system_admin only (`processing:lifecycle`).
    """
    if await fetch_processing_by_idx(pool, processing_idx) is None:
        raise HTTPException(status_code=404, detail=_MSG_PROCESSING_NOT_FOUND)
    async with tx() as conn:
        outcome = await set_assembly_sample_states(
            conn,
            processing_idx=processing_idx,
            prep_sample_idxs=body.prep_sample_idx,
            state=body.state,
            reason=body.reason,
            principal_idx=caller.principal_idx,
        )
    return AssemblySampleStatusUpdateResponse(
        processing_idx=processing_idx, state=body.state, **outcome
    )
