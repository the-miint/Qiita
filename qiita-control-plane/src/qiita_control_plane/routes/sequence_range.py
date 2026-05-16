"""Sequence-range allocation routes.

POST /sequence-range mints a contiguous bigint range for a prep_sample.
Service-account callers with the `sequence_range:mint` scope only —
humans never mint sequence ranges. The cap on a single allocation is
read from Settings.max_sequence_mint_count (so a runaway compute step
can't burn an unbounded slice of the sequence_idx space).

GET /sequence-range/{prep_sample_idx} reads the row back. Gated on the
existing `prep_sample:read` scope so any caller who can see the
prep_sample can see its allocated range.

Why a dedicated REST router (not a `LibraryPrimitive` dispatch like
`MINT_FEATURES` in `actions/library.py`): sequence-range allocation is
a per-prep_sample synchronous int-shaped operation invoked directly by
a compute step over HTTP. The library-primitive pattern targets bulk,
parquet-path-based work driven by workflow YAML through the in-process
runner — a different invocation model and a different payload shape.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from qiita_common.api_paths import (
    PATH_SEQUENCE_RANGE_BY_PREP_SAMPLE,
    PATH_SEQUENCE_RANGE_PREFIX,
    PATH_SEQUENCE_RANGE_ROOT,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import SequenceRange, SequenceRangeMintRequest

from ..auth.guards import require_scope, require_service_with_scope
from ..auth.principal import Principal, ServiceAccount
from ..deps import TxConnFactory, get_db_pool, get_settings, get_tx_conn_factory
from ..repositories.sequence_range import (
    fetch_sequence_range_by_prep_sample_idx,
    mint_sequence_range,
)

router = APIRouter(prefix=PATH_SEQUENCE_RANGE_PREFIX, tags=["sequence-range"])


def _record_to_response(row: asyncpg.Record) -> SequenceRange:
    """Project an asyncpg.Record from sequence_range onto the response
    model. Field access is by name (not position) so a future column
    add to the table can't silently shift the projection."""
    return SequenceRange(
        prep_sample_idx=row["prep_sample_idx"],
        sequence_idx_start=row["sequence_idx_start"],
        sequence_idx_stop=row["sequence_idx_stop"],
        created_at=row["created_at"],
    )


@router.post(PATH_SEQUENCE_RANGE_ROOT, status_code=201)
async def mint_sequence_range_route(
    body: SequenceRangeMintRequest,
    request: Request,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    sa: ServiceAccount = Depends(require_service_with_scope(Scope.SEQUENCE_RANGE_MINT)),
) -> SequenceRange:
    """Mint a contiguous sequence_idx range for `body.prep_sample_idx`.

    Caller must be a ServiceAccount holding `sequence_range:mint`.
    Pydantic enforces count > 0 and prep_sample_idx > 0; the route adds
    the dynamic cap from Settings. The plpgsql function holds an
    advisory lock for the nextval/setval/INSERT critical section.

    Maps repository-layer exceptions to HTTP status:
      - asyncpg.UniqueViolationError → 409 (prep_sample already has a range)
      - asyncpg.ForeignKeyViolationError → 404 (unknown prep_sample_idx
        OR prep_sample exists but is not eligible for a sequence_range;
        both cases collapse to one observable surface so the route
        doesn't leak the kind discriminator to clients probing idxs).
      - asyncpg.InvalidParameterValueError (SQLSTATE 22023) → 400.
        Unreachable post-Pydantic in normal flow; kept as defence in
        depth, with a static detail so an unexpected SQLSTATE path
        cannot leak Postgres internals.
      - Any other asyncpg.PostgresError → 500 with a generic detail.
        Catches the long tail (connection drop, deadlock, disk full)
        without bleeding constraint names or stack frames.
    """
    settings = get_settings(request)
    if body.count > settings.max_sequence_mint_count:
        raise HTTPException(
            status_code=400,
            detail=(
                f"count {body.count} exceeds per-request cap {settings.max_sequence_mint_count}"
            ),
        )

    async with tx() as conn:
        try:
            row = await mint_sequence_range(
                conn,
                prep_sample_idx=body.prep_sample_idx,
                count=body.count,
                principal_idx=sa.principal_idx,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(
                status_code=409,
                detail=f"prep_sample_idx {body.prep_sample_idx} already has a sequence_range",
            )
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"prep_sample_idx {body.prep_sample_idx} not found "
                    "or not eligible for sequence-range allocation"
                ),
            )
        except asyncpg.InvalidParameterValueError:
            raise HTTPException(status_code=400, detail="invalid sequence-range parameters")
        except asyncpg.PostgresError:
            raise HTTPException(status_code=500, detail="database error")

    return _record_to_response(row)


@router.get(PATH_SEQUENCE_RANGE_BY_PREP_SAMPLE)
async def get_sequence_range_route(
    prep_sample_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> SequenceRange:
    """Return the sequence_range row for `prep_sample_idx`, or 404.

    SECURITY: gated by `prep_sample:read` scope only — there is no
    per-row ACL on this read. Any caller holding the scope can fetch
    the range for any prep_sample_idx. The two columns returned
    (`sequence_idx_start`, `sequence_idx_stop`) are non-sensitive
    monotonic identifiers and reveal nothing about study membership,
    biosample metadata, or processing state, so this is acceptable in
    v1. The compute orchestrator does not consume this endpoint (it
    mints over POST only). Callers that need ownership-scoped row
    visibility must add a row-level ACL here.
    """
    row = await fetch_sequence_range_by_prep_sample_idx(pool, prep_sample_idx)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no sequence_range for prep_sample_idx {prep_sample_idx}",
        )
    return _record_to_response(row)
