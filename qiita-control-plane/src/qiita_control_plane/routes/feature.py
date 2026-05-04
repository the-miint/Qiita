"""Feature minting routes.

`POST /feature/mint` is the pure-mint primitive: sequence-hash entries in,
feature_idx mapping out, no reference context. Genome associations
(feature_genome) are written when the input carries them, since genomes
are reference-agnostic.

Linking already-minted feature_idx values to a reference lives in
`POST /reference/{reference_idx}/membership` (routes/reference.py).
Reference status transitions live in `PATCH /reference/{reference_idx}/status`
and are driven externally by the orchestrator.

Both routes are thin shells over the action library
(qiita_control_plane.actions.library); the library is the canonical
implementation so workflow-runner invocations and HTTP callers can't
diverge.
"""

import asyncpg
from fastapi import APIRouter, Depends
from qiita_common.auth_constants import Scope
from qiita_common.models import FeatureMintRequest, FeatureMintResponse

from ..actions.library import mint_features as _mint_features
from ..auth.guards import require_scope, require_service
from ..auth.principal import Principal, ServiceAccount
from ..deps import get_db_pool

router = APIRouter(prefix="/feature", tags=["feature"])


@router.post("/mint")
async def mint_features(
    body: FeatureMintRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _service: ServiceAccount = Depends(require_service),
    _scope: Principal = Depends(require_scope(Scope.FEATURE_MINT)),
) -> FeatureMintResponse:
    """Mint feature_idx values for sequence hashes; reference-agnostic.

    Resubmitting the same batch is safe: features dedupe via
    `ON CONFLICT (sequence_hash) DO NOTHING` and feature_genome via
    `ON CONFLICT DO NOTHING`. The mapping returned covers every input
    hash; novel ones go into `minted`, pre-existing into `reused`.

    Caller is responsible for any reference-side bookkeeping
    (`POST /reference/{idx}/membership` to link, status transitions via
    `PATCH /reference/{idx}/status`).
    """
    mapping, minted, reused = await _mint_features(pool, body.entries)
    return FeatureMintResponse(mapping=mapping, minted=minted, reused=reused)
