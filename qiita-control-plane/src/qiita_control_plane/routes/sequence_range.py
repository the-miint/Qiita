"""Sequence-range allocation routes.

POST /sequence-range mints a contiguous bigint range for a prep_sample.
Service-account callers with the `sequence_range:mint` scope only —
humans never mint sequence ranges. The cap on a single allocation is
read from Settings.max_sequence_mint_count (so a runaway compute step
can't burn an unbounded slice of the sequence_idx space).

GET /sequence-range/{prep_sample_idx} reads the row back. Gated on
`prep_sample:read` OR `sequence_range:mint` (either-or): any caller who
can see the prep_sample can see its allocated range, AND the minter can
read back the range it just minted. The latter is what makes the
ingest_reads retry path transparent — a step that minted then crashed
before the durable write reuses the existing range on retry instead of
failing on the one-shot mint contract.

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

from ..auth.guards import require_any_scope, require_service_with_scope
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
    _scope: Principal = Depends(
        require_any_scope(Scope.PREP_SAMPLE_READ, Scope.SEQUENCE_RANGE_MINT)
    ),
) -> SequenceRange:
    """Return the sequence_range row for `prep_sample_idx`, or 404.

    SECURITY: gated by `prep_sample:read` OR `sequence_range:mint` — no
    per-row ACL. Any caller holding either scope can fetch the row for any
    prep_sample_idx. The `sequence_range:mint` arm lets the compute SA
    (which holds mint but deliberately not `prep_sample:read`) read back
    its own range on the ingest_reads retry path; the disclosure set below
    is the same one the minter already learns by minting. Concrete
    information exposed to such a caller:

    - **Read count** for the prep_sample
      (`sequence_idx_stop - sequence_idx_start + 1`) — a
      sequencing-depth signal.
    - **Mint timestamp** (`created_at`) — when phase 3 of the
      ingest workflow ran.
    - **Processing-state existence** (200 vs 404) — whether this
      prep_sample has been minted at all (i.e., processed at least
      through phase 3).
    - **Relative chronological order** across samples (compare
      `sequence_idx_start` values) — leaks the order in which samples
      were minted even without the timestamp.

    What is NOT exposed: study membership, biosample metadata,
    sequence content, the submitter's identity, or whether the
    work_ticket ultimately succeeded (the sequence_range row persists
    after a step failure).

    Accepted in v1 because the disclosure set above is processing-
    metadata-adjacent, not study/biosample-content. Two hardening
    options remain open: a row-level ACL gate
    (limit reads to the prep_sample's owner + admins) and a response-
    surface trim (drop `created_at` from the wire model). The compute
    orchestrator consumes this endpoint on the ingest_reads retry path
    (range reuse) via the `sequence_range:mint` arm, so a future
    row-level ACL must keep the minter able to read back its own
    freshly-minted range.
    """
    row = await fetch_sequence_range_by_prep_sample_idx(pool, prep_sample_idx)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"no sequence_range for prep_sample_idx {prep_sample_idx}",
        )
    return _record_to_response(row)
