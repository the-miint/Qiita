"""Sequence-range allocation routes.

POST /sequence-range mints a contiguous bigint range for a prep_sample.
Service-account callers with the `sequence_range:mint` scope only —
humans never mint sequence ranges. The cap on a single allocation is
read from Settings.max_sequence_mint_count (so a runaway compute step
can't burn an unbounded slice of the sequence_idx space).

GET /sequence-range/{prep_sample_idx} reads the row back. Gated on the
existing `prep_sample:read` scope so any caller who can see the
prep_sample can see its allocated range.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from qiita_common.auth_constants import Scope
from qiita_common.models import SequenceRange, SequenceRangeMintRequest

from ..auth.guards import require_scope, require_service
from ..auth.principal import Principal, ServiceAccount
from ..deps import TxConnFactory, get_db_pool, get_tx_conn_factory
from ..repositories.sequence_range import (
    fetch_sequence_range_by_prep_sample_idx,
    mint_sequence_range,
)

router = APIRouter(prefix="/sequence-range", tags=["sequence-range"])


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


@router.post("", status_code=201)
async def mint_sequence_range_route(
    body: SequenceRangeMintRequest,
    request: Request,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    sa: ServiceAccount = Depends(require_service),
    _scope: Principal = Depends(require_scope(Scope.SEQUENCE_RANGE_MINT)),
) -> SequenceRange:
    """Mint a contiguous sequence_idx range for `body.prep_sample_idx`.

    Caller must be a ServiceAccount holding `sequence_range:mint`.
    Pydantic enforces count > 0 and prep_sample_idx > 0; the route adds
    the dynamic cap from Settings. The plpgsql function holds an
    advisory lock for the nextval/setval/INSERT critical section.

    Maps repository-layer exceptions to HTTP status:
      - asyncpg.UniqueViolationError → 409 (prep_sample already has a range)
      - asyncpg.ForeignKeyViolationError → 404 (unknown / non-sequenced
        prep_sample_idx; the composite FK tightens both cases into one
        observable failure mode)
      - asyncpg.InvalidParameterValueError (SQLSTATE 22023) → 400 (the
        SQL function's `RAISE ... USING ERRCODE = '22023'` — shouldn't
        be reachable post-Pydantic, but defended for completeness)
    """
    settings = request.app.state.settings
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
            # Either the prep_sample_idx is unknown, or it exists but
            # carries a processing_kind other than 'sequenced'. Both
            # collapse to the same observable surface — 404 — because
            # the composite FK cannot distinguish them at the catch site
            # and the route shouldn't leak the distinction either.
            raise HTTPException(
                status_code=404,
                detail=(
                    f"prep_sample_idx {body.prep_sample_idx} not found "
                    "or not eligible (processing_kind must be 'sequenced')"
                ),
            )
        except asyncpg.InvalidParameterValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    return _record_to_response(row)


@router.get("/{prep_sample_idx}")
async def get_sequence_range_route(
    prep_sample_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> SequenceRange:
    """Return the sequence_range row for `prep_sample_idx`, or 404."""
    row = await fetch_sequence_range_by_prep_sample_idx(pool, prep_sample_idx)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no sequence_range for prep_sample_idx {prep_sample_idx}",
        )
    return _record_to_response(row)
