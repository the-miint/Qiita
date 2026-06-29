"""Prep-protocol discovery route.

GET /prep-protocol lists the prep protocols an operator can pass to
`submit-bcl-convert --prep-protocol-idx`, so the valid idxes are discoverable
over the API instead of a raw Postgres query. Read-only catalog, so it is
anonymous-OK — the same posture as GET /reference. Retired protocols are
excluded by default (they stay valid for already-prepared samples but
shouldn't be picked for new submissions); pass `include_retired=true` to see
them.
"""

import asyncpg
from fastapi import APIRouter, Depends
from qiita_common.api_paths import PATH_PREP_PROTOCOL_PREFIX, PATH_PREP_PROTOCOL_ROOT
from qiita_common.models import PrepProtocolResponse

from ..auth.principal import Principal, get_current_principal
from ..deps import get_db_pool

router = APIRouter(prefix=PATH_PREP_PROTOCOL_PREFIX, tags=["prep-protocol"])

# The table's PK column is `idx`; alias it to the wire/consumer name so
# `PrepProtocolResponse(**dict(row))` maps directly.
_PREP_PROTOCOL_RETURNING = (
    "idx AS prep_protocol_idx, name, description, retired, created_by_idx, created_at"
)


@router.get(PATH_PREP_PROTOCOL_ROOT)
async def list_prep_protocols(
    pool: asyncpg.Pool = Depends(get_db_pool),
    _principal: Principal = Depends(get_current_principal),
    include_retired: bool = False,
) -> list[PrepProtocolResponse]:
    """Anonymous-OK list of prep protocols, ordered by idx. Retired protocols
    are excluded unless `include_retired=true`."""
    where = "" if include_retired else " WHERE retired = false"
    rows = await pool.fetch(
        f"SELECT {_PREP_PROTOCOL_RETURNING} FROM qiita.prep_protocol{where} ORDER BY idx"
    )
    return [PrepProtocolResponse(**dict(r)) for r in rows]
