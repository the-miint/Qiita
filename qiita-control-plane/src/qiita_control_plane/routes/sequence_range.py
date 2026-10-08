"""Sequence-range allocation routes.

POST /sequence-range mints a contiguous bigint range for a prep_sample.
Service-account callers with the `sequence_range:mint` scope only —
humans never mint sequence ranges. The cap on a single allocation is
read from Settings.max_sequence_mint_count (so a runaway compute step
can't burn an unbounded slice of the sequence_idx space).

GET /sequence-range/{prep_sample_idx} reads the row back. Who may read it is
on `get_sequence_range_route`.

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

from ..auth.guards import COHORT_MIN_TIER, require_any_scope, require_service_with_scope
from ..auth.principal import Principal, ServiceAccount
from ..deps import TxConnFactory, get_db_pool, get_settings, get_tx_conn_factory
from ..repositories.sequence_range import (
    fetch_sequence_range_by_prep_sample_idx,
    mint_sequence_range,
)
from ._helpers import authorize_prep_sample_cohort

router = APIRouter(prefix=PATH_SEQUENCE_RANGE_PREFIX, tags=["sequence-range"])


def _record_to_response(
    row: asyncpg.Record, *, minted_by_work_ticket_state: str | None = None
) -> SequenceRange:
    """Project an asyncpg.Record from sequence_range onto the response
    model. Field access is by name (not position) so a future column
    add to the table can't silently shift the projection."""
    return SequenceRange(
        prep_sample_idx=row["prep_sample_idx"],
        sequence_idx_start=row["sequence_idx_start"],
        sequence_idx_stop=row["sequence_idx_stop"],
        minted_by_work_ticket_idx=row["minted_by_work_ticket_idx"],
        # Only the read-back joins the minting ticket; the mint's own RETURNING row
        # has no state column (and needs none — the minter is the caller, running).
        minted_by_work_ticket_state=(
            row["minted_by_work_ticket_state"] if "minted_by_work_ticket_state" in row else None
        ),
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
                work_ticket_idx=body.work_ticket_idx,
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
    caller: Principal = Depends(
        require_any_scope(Scope.PREP_SAMPLE_READ, Scope.SEQUENCE_RANGE_MINT)
    ),
) -> SequenceRange:
    """Return the sequence_range row for `prep_sample_idx`, or 404.

    SECURITY: the two scopes admit two kinds of caller (which principals can
    hold each is in `auth/scopes.py`), and they are gated differently.

    A caller without `sequence_range:mint` must also pass
    `authorize_prep_sample_cohort` at `COHORT_MIN_TIER`. The check runs before
    the row is fetched, so a caller without access gets 403 for a minted, an
    unminted and a non-existent prep_sample alike.

    A `sequence_range:mint` caller is not gated per row. Every job presents
    the same compute service-account token, so the request does not identify
    the calling work_ticket; a job that finds a range already minted reads it
    back whichever ticket minted it. The caller can already mint for any
    prep_sample.

    The 200 body gives, to a caller who passes:

    - **Read count** for the prep_sample
      (`sequence_idx_stop - sequence_idx_start + 1`).
    - **Mint timestamp** (`created_at`).
    - **The minting work_ticket and its current state**
      (`minted_by_work_ticket_idx`, `minted_by_work_ticket_state`).
    - **Whether a range has been minted at all** (200 vs 404).
    - **Relative mint order** across prep_samples (compare
      `sequence_idx_start`).

    It carries no study membership, biosample metadata, sequence content or
    submitter identity. The 403 detail is `prep_sample_access_denied_detail`'s.
    """
    if not caller.has_scope(str(Scope.SEQUENCE_RANGE_MINT)):
        await authorize_prep_sample_cohort(
            pool, caller=caller, prep_sample_idx=[prep_sample_idx], min_tier=COHORT_MIN_TIER
        )
    row = await fetch_sequence_range_by_prep_sample_idx(pool, prep_sample_idx)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no sequence_range for prep_sample_idx {prep_sample_idx}",
        )
    # This is the only path that joins the minting ticket — see
    # fetch_sequence_range_by_prep_sample_idx.
    return _record_to_response(row, minted_by_work_ticket_state=row["minted_by_work_ticket_state"])
