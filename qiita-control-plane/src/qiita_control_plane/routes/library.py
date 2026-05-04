"""Generic action-library dispatch.

`POST /api/v1/library/{name}` is the single transport between workflow
runners (orchestrator) and library primitives (control-plane). Per-name
request shape is validated inside the handler — each primitive takes
different inputs, so a generic envelope (`scope_target`, `inputs`)
covers them uniformly at the wire level and the handler unpacks.

Auth: service-only. The orchestrator is the canonical caller. Each
primitive name maps to a required scope (mint-features → feature:mint,
write-membership → reference:write, register-files → reference:register_files);
the dispatcher enforces it because the standard `require_scope(...)`
dependency can't see the path parameter at resolution time.

Status-state guards (e.g. write-membership requires the reference to be
in 'minting') stay in the per-primitive dispatch helpers — the library
functions themselves are state-agnostic on the assumption their caller
has already established the right state.
"""

import asyncpg
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import (
    PATH_LIBRARY_NAME,
    PATH_LIBRARY_PREFIX,
    LibraryPrimitive,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    FeatureHashEntry,
    LibraryInvocation,
    LibraryResponse,
    ScopeTargetKind,
)

from ..actions import library as _library
from ..auth.guards import require_service
from ..auth.principal import ServiceAccount
from ..deps import get_data_plane_url, get_db_pool, get_hmac_secret

router = APIRouter(prefix=PATH_LIBRARY_PREFIX, tags=["library"])


# Per-primitive scope requirements. Adding a primitive without an entry
# here will fail the auth check below with a clear "no required scope"
# message — better than silently bypassing scope enforcement.
_PRIMITIVE_SCOPES: dict[str, Scope] = {
    LibraryPrimitive.MINT_FEATURES: Scope.FEATURE_MINT,
    LibraryPrimitive.WRITE_MEMBERSHIP: Scope.REFERENCE_WRITE,
    LibraryPrimitive.REGISTER_FILES: Scope.REFERENCE_REGISTER_FILES,
}


@router.post(PATH_LIBRARY_NAME)
async def invoke_library(
    name: str,
    body: LibraryInvocation,
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_hmac_secret),
    data_plane_url: str = Depends(get_data_plane_url),
    principal: ServiceAccount = Depends(require_service),
) -> LibraryResponse:
    """Dispatch a workflow `action:` entry to the named library primitive."""
    if name not in _library.LIBRARY:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown library primitive: {name!r}",
        )

    required = _PRIMITIVE_SCOPES.get(name)
    if required is None:
        # Defensive: a primitive in LIBRARY without a scope mapping is a
        # development-time mistake.
        raise HTTPException(
            status_code=500,
            detail=f"Library primitive {name!r} has no required-scope mapping",
        )
    if required.value not in principal.scopes:
        raise HTTPException(
            status_code=403,
            detail=f"Missing required scope: {required.value!r}",
        )

    if name == LibraryPrimitive.MINT_FEATURES:
        return await _dispatch_mint_features(body, pool)
    if name == LibraryPrimitive.WRITE_MEMBERSHIP:
        return await _dispatch_write_membership(body, pool)
    if name == LibraryPrimitive.REGISTER_FILES:
        return await _dispatch_register_files(body, pool, hmac_secret, data_plane_url)

    # Unreachable while LIBRARY and the dispatch ladder stay in sync.
    raise HTTPException(
        status_code=500,
        detail=f"Library primitive {name!r} has no dispatch wiring",
    )


async def _dispatch_mint_features(body: LibraryInvocation, pool: asyncpg.Pool) -> LibraryResponse:
    raw = body.inputs.get("entries")
    if not isinstance(raw, list):
        raise HTTPException(
            status_code=422,
            detail="mint-features requires inputs.entries to be a list",
        )
    try:
        entries = [FeatureHashEntry.model_validate(e) for e in raw]
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"invalid entries: {exc}") from exc
    mapping, minted, reused = await _library.mint_features(pool, entries)
    return LibraryResponse(
        outputs={
            "mapping": {str(k): v for k, v in mapping.items()},
            "minted": minted,
            "reused": reused,
        }
    )


async def _dispatch_write_membership(
    body: LibraryInvocation, pool: asyncpg.Pool
) -> LibraryResponse:
    if body.scope_target.kind != ScopeTargetKind.REFERENCE:
        raise HTTPException(
            status_code=422,
            detail="write-membership requires scope_target.kind='reference'",
        )
    reference_idx = body.scope_target.reference_idx
    feature_idxs = body.inputs.get("feature_idxs")
    if not isinstance(feature_idxs, list) or not feature_idxs:
        raise HTTPException(
            status_code=422,
            detail="write-membership requires inputs.feature_idxs (non-empty list)",
        )

    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if status is None:
        raise HTTPException(status_code=404, detail="Reference not found")
    if status != "minting":
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {status!r}, must be 'minting'",
        )

    try:
        linked, already_linked = await _library.write_membership(pool, reference_idx, feature_idxs)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return LibraryResponse(outputs={"linked": linked, "already_linked": already_linked})


async def _dispatch_register_files(
    body: LibraryInvocation,
    pool: asyncpg.Pool,
    hmac_secret: bytes,
    data_plane_url: str,
) -> LibraryResponse:
    if body.scope_target.kind != ScopeTargetKind.REFERENCE:
        raise HTTPException(
            status_code=422,
            detail="register-files requires scope_target.kind='reference'",
        )
    staging_dir = body.inputs.get("staging_dir")
    files = body.inputs.get("files")
    if not isinstance(staging_dir, str):
        raise HTTPException(
            status_code=422,
            detail="register-files requires inputs.staging_dir (string)",
        )
    if not isinstance(files, dict):
        raise HTTPException(
            status_code=422,
            detail="register-files requires inputs.files (filename → table mapping)",
        )

    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        body.scope_target.reference_idx,
    )
    if status is None:
        raise HTTPException(status_code=404, detail="Reference not found")
    if status != "loading":
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {status!r}, must be 'loading'",
        )

    try:
        registered = await _library.register_files(
            staging_dir=staging_dir,
            files=files,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Data plane registration failed: {exc}",
        ) from exc
    return LibraryResponse(outputs={"registered": registered})
