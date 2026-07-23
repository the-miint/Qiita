"""Reference management routes.

POST /reference creates a reference and is the only mutation endpoint
on this router that humans drive. GET /reference/{id} stays anonymous-OK
(`get_current_principal` directly, no guard). PATCH /reference/{id}/status
moves the reference through its lifecycle and is driven by the workflow
runner. POST /reference/{id}/ticket/doget signs a Flight ticket so a
client can pull active reference rows from the data plane.

Feature minting, membership writes, and DuckLake registration used to
live here as per-primitive routes; they're now reached through the
generic POST /api/v1/library/{name} dispatch (routes/library.py) so
workflow runners and HTTP callers share one transport.
"""

import base64
import json
from pathlib import Path
from typing import Annotated

import asyncpg
import httpx
import pyarrow.flight as _flight
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import Field
from qiita_common.api_paths import (
    PATH_REFERENCE_BY_IDX,
    PATH_REFERENCE_DOGET,
    PATH_REFERENCE_EXCLUSION,
    PATH_REFERENCE_EXCLUSION_BY_IDX,
    PATH_REFERENCE_EXCLUSION_SYNC,
    PATH_REFERENCE_INDEX,
    PATH_REFERENCE_PREFIX,
    PATH_REFERENCE_ROOT,
    PATH_REFERENCE_SHARD_INDEX_STATUS,
    PATH_REFERENCE_STATUS,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    DoGetTicketRequest,
    DoGetTicketResponse,
    ReferenceCreateRequest,
    ReferenceDeleteResponse,
    ReferenceExclusionCreateRequest,
    ReferenceExclusionListItem,
    ReferenceExclusionMutationResponse,
    ReferenceExclusionSyncResponse,
    ReferenceIndex,
    ReferenceKind,
    ReferenceResponse,
    ReferenceShardIndexStatus,
    ReferenceStatus,
    ReferenceStatusUpdate,
    WorkTicketState,
)

from ..actions.library import delete_reference_data, sync_reference_exclusion_data
from ..actions.reference import (
    REFERENCE_RETURNING,
    IllegalStatusTransition,
    ReferenceDeleteBlocked,
    ReferenceNotFound,
    assert_reference_deletable,
    delete_reference_cascade,
    transition_reference_status,
)
from ..auth.guards import (
    require_complete_profile,
    require_scope,
)
from ..auth.principal import (
    HumanUser,
    Principal,
    get_current_principal,
)
from ..auth.tickets import sign_ticket
from ..deps import (
    TxConnFactory,
    get_data_plane_url,
    get_db_pool,
    get_flight_signing_key,
    get_scratch_staging,
    get_tx_conn_factory,
)
from ..repositories.reference_exclusion import (
    add_exclusion,
    list_for_reference,
    remove_exclusion,
)
from ..repositories.reference_membership import count_reference_shards
from ..shard_orchestration import (
    BUILD_SHARD_INDEX_ACTION_ID,
    expected_shard_index_types,
)

router = APIRouter(prefix=PATH_REFERENCE_PREFIX, tags=["reference"])


# Single source of truth lives in actions/reference.py (REFERENCE_RETURNING);
# aliased here so the existing in-file references read unchanged.
_REFERENCE_RETURNING = REFERENCE_RETURNING

# Backstop cap for the anonymous catalog list. The table is small (curated
# reference databases), so this never bites in practice; it bounds the
# worst-case payload and is caller-overridable via ?limit=.
_DEFAULT_LIST_LIMIT = 1000
_MAX_LIST_LIMIT = 5000

_MSG_REFERENCE_NOT_FOUND = "Reference not found"


@router.post(PATH_REFERENCE_ROOT, status_code=201)
async def create_reference(
    body: ReferenceCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_WRITE)),
) -> ReferenceResponse:
    """Create a reference. Humans only — service-kind principals can only
    mint features and register files into existing references."""
    try:
        row = await pool.fetchrow(
            "INSERT INTO qiita.reference"
            "  (name, version, kind, is_host, created_by_idx)"
            " VALUES ($1, $2, $3, $4, $5)"
            f" RETURNING {_REFERENCE_RETURNING}",
            body.name,
            body.version,
            body.kind,
            body.is_host,
            user.principal_idx,
        )
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=409,
            detail=f"Reference ({body.name!r}, {body.version!r}) already exists",
        )
    except asyncpg.PostgresError as exc:
        raise HTTPException(status_code=500, detail="Database error") from exc
    return ReferenceResponse(**dict(row))


@router.get(PATH_REFERENCE_ROOT)
async def list_references(
    pool: asyncpg.Pool = Depends(get_db_pool),
    _principal: Principal = Depends(get_current_principal),
    kind: ReferenceKind | None = None,
    is_host: bool | None = None,
    status: ReferenceStatus | None = None,
    limit: int = Query(default=_DEFAULT_LIST_LIMIT, ge=1, le=_MAX_LIST_LIMIT),
) -> list[ReferenceResponse]:
    """Anonymous-OK list of references, optionally filtered by `kind`,
    `is_host`, and `status`. Ordered by reference_idx, bounded by `limit`
    (default 1000) so the anonymous endpoint can't be made to return an
    unbounded payload. Row-level visibility (e.g. hiding private references) is
    not yet implemented — same posture as the single-reference GET."""
    clauses: list[str] = []
    args: list[object] = []
    if kind is not None:
        args.append(kind)
        clauses.append(f"kind = ${len(args)}")
    if is_host is not None:
        args.append(is_host)
        clauses.append(f"is_host = ${len(args)}")
    if status is not None:
        args.append(str(status))
        clauses.append(f"status = ${len(args)}")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    args.append(limit)
    rows = await pool.fetch(
        f"SELECT {_REFERENCE_RETURNING} FROM qiita.reference{where}"
        f" ORDER BY reference_idx LIMIT ${len(args)}",
        *args,
    )
    return [ReferenceResponse(**dict(r)) for r in rows]


@router.get(PATH_REFERENCE_INDEX)
async def get_reference_index(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_READ)),
) -> list[ReferenceIndex]:
    """List built search indexes (e.g. a rype `.ryxdi`) for a reference,
    newest first. 404 when the reference itself doesn't exist; an empty list
    when it exists but has no index built yet — the two are distinct.

    A sharded analysis index surfaces as one flat row per shard (each carrying
    its `shard_id`); grouping shards into "one logical index" is a later
    concern. An unsharded whole-reference index has `shard_id` null.

    `fs_path` is the on-disk index location a future host-filter compute job
    consumes (the runner injects it directly; this endpoint is for general
    visibility / admin). Scoped to reference:read — unlike the anonymous-OK
    reference metadata GETs — because fs_path exposes internal filesystem
    layout; reference:read is held by every human role and service account."""
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    rows = await pool.fetch(
        "SELECT reference_index_idx, reference_idx, index_type, fs_path, params, created_at,"
        " shard_id"
        " FROM qiita.reference_index WHERE reference_idx = $1"
        " ORDER BY created_at DESC, reference_index_idx DESC",
        reference_idx,
    )
    out: list[ReferenceIndex] = []
    for r in rows:
        d = dict(r)
        # params is JSONB — asyncpg returns it as a JSON string by default.
        if isinstance(d["params"], str):
            d["params"] = json.loads(d["params"])
        out.append(ReferenceIndex(**d))
    return out


@router.get(PATH_REFERENCE_SHARD_INDEX_STATUS)
async def get_reference_shard_index_status(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_READ)),
) -> ReferenceShardIndexStatus:
    """Progress of a sharded analysis reference's fan-out index build.

    Surfaces the count-based, fail-closed completion `finalize_shard` gates on,
    so a reference wedged in `indexing` on a permanently-failed shard is
    diagnosable: `expected_shards` is N (the planner's shard count), and
    `registered_shards` maps each expected `index_type` to how many of the N
    shards have registered a `reference_index` row — a type below N is
    incomplete, a type at N is done. `failed_shard_tickets` counts this
    reference's build-shard-index work tickets in `failed`; the operator
    redrives those to unwedge the build (each redriven ticket's finalize_shard
    re-counts and, as the last observer, flips `active`).

    404 when the reference itself doesn't exist. An unsharded reference — or one
    whose sharding fanned out zero shards — reads all-zero / empty (a valid
    "nothing sharded here" answer, not an error). Scoped to reference:read like
    the /index listing: it exposes build progress, not payload."""
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)

    # N = the shards the planner assigned (COUNT(DISTINCT shard_id) over the
    # non-NULL membership rows — the same derivation finalize_shard uses; there
    # is no shard_count column). 0 for an unsharded reference.
    expected_shards = await count_reference_shards(pool, reference_idx)

    # Registered shard rows per index_type — the observed ground truth.
    rows = await pool.fetch(
        "SELECT index_type, count(DISTINCT shard_id) AS n FROM qiita.reference_index"
        " WHERE reference_idx = $1 AND shard_id IS NOT NULL GROUP BY index_type",
        reference_idx,
    )
    registered_shards: dict[str, int] = {r["index_type"]: r["n"] for r in rows}

    # Seed every expected index_type (read from any one build-shard-index
    # ticket's copied context) at 0, so a type whose shards ALL failed to
    # register shows as `type: 0` rather than being silently absent. This is the
    # same expected set finalize_shard checks against N.
    ctx = await pool.fetchval(
        "SELECT action_context FROM qiita.work_ticket"
        " WHERE reference_idx = $1 AND action_id = $2"
        " ORDER BY work_ticket_idx DESC LIMIT 1",
        reference_idx,
        BUILD_SHARD_INDEX_ACTION_ID,
    )
    ctx_dict = json.loads(ctx) if isinstance(ctx, str) else ctx
    # A well-formed fan-out ticket always carries an object; guard the shape so a
    # malformed / JSON-`null` context degrades this diagnostic to observed-only
    # rather than 500ing (asyncpg hands JSON null back as the string "null", so
    # it survives the fetch and json.loads to Python None — caught here too).
    if isinstance(ctx_dict, dict):
        for index_type in expected_shard_index_types(ctx_dict):
            registered_shards.setdefault(index_type, 0)

    failed_shard_tickets = await pool.fetchval(
        "SELECT count(*) FROM qiita.work_ticket"
        " WHERE reference_idx = $1 AND action_id = $2 AND state = $3",
        reference_idx,
        BUILD_SHARD_INDEX_ACTION_ID,
        WorkTicketState.FAILED.value,
    )

    return ReferenceShardIndexStatus(
        reference_idx=reference_idx,
        expected_shards=expected_shards,
        registered_shards=registered_shards,
        failed_shard_tickets=failed_shard_tickets,
    )


@router.get(PATH_REFERENCE_BY_IDX)
async def get_reference(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _principal: Principal = Depends(get_current_principal),
) -> ReferenceResponse:
    """Anonymous-OK. Returns the full ReferenceResponse including
    created_by_idx; row-level visibility (e.g., hiding private references'
    owner) is not yet implemented."""
    row = await pool.fetchrow(
        f"SELECT {_REFERENCE_RETURNING} FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    return ReferenceResponse(**dict(row))


@router.patch(PATH_REFERENCE_STATUS)
async def update_reference_status(
    reference_idx: Annotated[int, Field(gt=0)],
    body: ReferenceStatusUpdate,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_WRITE)),
) -> ReferenceResponse:
    try:
        return await transition_reference_status(pool, reference_idx, body.status)
    except ReferenceNotFound:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    except IllegalStatusTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc))


def _require_exclusion_sync_dest(staging_root: Path | None) -> Path:
    """The shared-scratch Parquet the data plane reads to refresh its exclusion
    mirror. Requires PATH_SCRATCH (path_scratch_staging): the enforcement surface
    is the data plane's anti-join views, so a curation call that can't reach the
    shared scratch cannot take effect — fail loud (503) rather than silently
    updating Postgres only. Checked BEFORE any Postgres write so the mutation is
    fail-fast.

    The filename is fixed (not per-call-unique): concurrent writers to it are
    impossible because `sync_reference_exclusion_data` serializes every sync under
    a transaction-scoped advisory lock held across the parquet write and the
    data-plane read, so exactly one writer touches this path at a time and the
    next sync's overwrite can't race a reader."""
    if staging_root is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "reference exclusion requires PATH_SCRATCH (the shared data-plane"
                " scratch) to be configured"
            ),
        )
    return staging_root / "reference_exclusion" / "reference_exclusion.parquet"


async def _sync_exclusion_to_lake(
    pool: asyncpg.Pool, dest: Path, signing_key: bytes, data_plane_url: str
) -> int:
    """Re-materialize the resolved blocklist into the data-plane mirror. A Flight
    failure is a 502, NOT a 500: the Postgres blocklist row was already written,
    so the state is degraded-but-consistent-on-retry — re-issuing the (idempotent)
    request re-runs the wholesale sync.

    Concurrency: the mirror is a BLIND wholesale replace from a CP-resolved
    snapshot (not commutative), so overlapping mutations — or a mutation racing a
    reference load's post-load sync — must not commit at the data plane out of
    resolve order, or a stale snapshot could clobber a fresher block (fail-OPEN).
    `sync_reference_exclusion_data` prevents this by holding a transaction-scoped
    Postgres advisory lock across the whole resolve → replace (a DB-level lock, so
    it serializes across a multi-instance CP, unlike an in-process asyncio.Lock);
    the last sync to run resolves the latest committed Postgres state and commits
    last, reflecting Postgres exactly. A Flight failure is still a bare 502 (same
    degraded-but-safe handling `delete_reference` uses for its DuckLake purge); the
    caller retries the idempotent request."""
    try:
        return await sync_reference_exclusion_data(
            pool=pool,
            dest=dest,
            signing_key=signing_key,
            data_plane_url=data_plane_url,
        )
    except asyncpg.LockNotAvailableError as exc:
        # Another sync held the serialization lock past lock_timeout (a concurrent
        # curation call or a stuck post-load sync). The Postgres row is committed;
        # this is transient — retry once the other sync clears.
        raise HTTPException(
            status_code=503,
            detail=(
                "blocklist updated in Postgres but a concurrent exclusion sync held"
                " the lock; re-issue the request to retry the sync"
            ),
        ) from exc
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "blocklist updated in Postgres but the data-plane exclusion sync"
                f" failed; re-issue the request to retry the sync: {exc}"
            ),
        ) from exc


@router.post(PATH_REFERENCE_EXCLUSION, status_code=201)
async def add_reference_exclusion(
    body: ReferenceExclusionCreateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    staging_root: Path | None = Depends(get_scratch_staging),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_EXCLUSION_WRITE)),
) -> ReferenceExclusionMutationResponse:
    """Block a bad genome_idx / feature_idx (system_admin, `reference:exclusion:write`).

    The block is GLOBAL — keyed on the id alone, no reference_idx — so future
    references that load the entity inherit it. Writes the Postgres blocklist row
    (idempotent: a re-block keeps the original reason and reports `changed=false`),
    then re-materializes the resolved exclusion set into the data plane's lake
    mirror so the anti-join views stop surfacing the entity immediately. No aligner
    index is rebuilt — exclusion is read-time only.

    Postgres-first then sync is retriable: on a data-plane failure (502) the block
    persists and re-POSTing re-runs the sync. NOTE the failure window is fail-OPEN:
    between the Postgres commit and a successful sync the entity is still visible in
    the anti-join views, so the block has NOT taken effect until a 201 returns — a
    502 means "not yet protected, retry", never "protected"."""
    dest = _require_exclusion_sync_dest(staging_root)
    target_kind = "genome" if body.genome_idx is not None else "feature"
    # Explicit existence check: the target columns are NOT foreign keys (a block
    # deliberately outlives its entity — see the migration), so there is no FK to
    # 404 an unknown idx. Non-atomic with the insert, which is fine — a target
    # deleted in the race just yields a block that re-attaches on re-ingest.
    if target_kind == "genome":
        exists = await pool.fetchval(
            "SELECT 1 FROM qiita.genome WHERE genome_idx = $1", body.genome_idx
        )
    else:
        exists = await pool.fetchval(
            "SELECT 1 FROM qiita.feature WHERE feature_idx = $1", body.feature_idx
        )
    if exists is None:
        raise HTTPException(status_code=404, detail=f"No such {target_kind} to block")
    changed = await add_exclusion(
        pool,
        reason=body.reason,
        excluded_by_idx=user.principal_idx,
        genome_idx=body.genome_idx,
        feature_idx=body.feature_idx,
    )
    synced = await _sync_exclusion_to_lake(pool, dest, signing_key, data_plane_url)
    return ReferenceExclusionMutationResponse(
        target_kind=target_kind,
        genome_idx=body.genome_idx,
        feature_idx=body.feature_idx,
        reason=body.reason,
        changed=changed,
        synced_feature_count=synced,
    )


@router.delete(PATH_REFERENCE_EXCLUSION)
async def remove_reference_exclusion(
    genome_idx: Annotated[int | None, Query(gt=0)] = None,
    feature_idx: Annotated[int | None, Query(gt=0)] = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    staging_root: Path | None = Depends(get_scratch_staging),
    user: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_EXCLUSION_WRITE)),
) -> ReferenceExclusionMutationResponse:
    """Unblock exactly one genome_idx / feature_idx (system_admin,
    `reference:exclusion:write`). The target rides as a query param
    (`?genome_idx=` or `?feature_idx=`). A SOFT delete: the block row is kept and
    stamped `unblocked_at`/`unblocked_by_idx` (the actor — hence the complete
    profile, symmetric with the add), preserving the curatorial record.
    Idempotent: unblocking something not actively blocked reports `changed=false`.
    Re-syncs the lake mirror so the entity is surfaced again. Retriable like the
    add (502 on data-plane failure); the failure window here is fail-CLOSED (the
    mirror over-excludes the entity until a successful re-sync), the safe
    direction."""
    if (genome_idx is None) == (feature_idx is None):
        raise HTTPException(
            status_code=422,
            detail="exactly one of genome_idx / feature_idx must be given",
        )
    dest = _require_exclusion_sync_dest(staging_root)
    target_kind = "genome" if genome_idx is not None else "feature"
    removed = await remove_exclusion(
        pool,
        unblocked_by_idx=user.principal_idx,
        genome_idx=genome_idx,
        feature_idx=feature_idx,
    )
    synced = await _sync_exclusion_to_lake(pool, dest, signing_key, data_plane_url)
    return ReferenceExclusionMutationResponse(
        target_kind=target_kind,
        genome_idx=genome_idx,
        feature_idx=feature_idx,
        changed=removed > 0,
        synced_feature_count=synced,
    )


@router.post(PATH_REFERENCE_EXCLUSION_SYNC, status_code=200)
async def sync_reference_exclusion_mirror(
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    staging_root: Path | None = Depends(get_scratch_staging),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_EXCLUSION_WRITE)),
) -> ReferenceExclusionSyncResponse:
    """Force-resync the data-plane exclusion mirror from the current Postgres
    blocklist (system_admin, `reference:exclusion:write`). Makes NO Postgres change
    — it re-resolves the committed blocklist and re-writes the DuckLake
    `reference_exclusion` mirror wholesale (idempotent, replay-safe), the same sync
    every mutation and reference-load runs.

    For operator recovery when the mirror has drifted from Postgres: a prior sync's
    502/503 was never retried, the DuckLake catalog was rebuilt, or a fresh data
    plane came up with an empty mirror. Unlike add/remove there is no complete-profile
    requirement (no actor is recorded — nothing is curated). A data-plane failure is
    a 502 / a concurrent-sync lock timeout a 503, both retriable, exactly as for the
    mutations (see `_sync_exclusion_to_lake`)."""
    dest = _require_exclusion_sync_dest(staging_root)
    synced = await _sync_exclusion_to_lake(pool, dest, signing_key, data_plane_url)
    return ReferenceExclusionSyncResponse(synced_feature_count=synced)


@router.get(PATH_REFERENCE_EXCLUSION_BY_IDX)
async def list_reference_exclusions(
    reference_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_READ)),
) -> list[ReferenceExclusionListItem]:
    """What the global blocklist filters from THIS reference, and why: one row per
    blocked feature that appears in the reference, with the exclusion reason,
    whether it was blocked directly or via its genome, the genome provenance
    (source, source_id), and the reference's own accession for the feature.

    404s an unknown reference (matching `get_reference_index` /
    `get_reference_shard_index_status`) so a typo'd idx is distinguishable from a
    genuinely clean reference — an existing reference with no blocked features
    yields `[]`."""
    exists = await pool.fetchval(
        "SELECT 1 FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    if exists is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    rows = await list_for_reference(pool, reference_idx)
    return [ReferenceExclusionListItem.model_validate(dict(r)) for r in rows]


@router.delete(PATH_REFERENCE_BY_IDX)
async def delete_reference(
    reference_idx: Annotated[int, Field(gt=0)],
    request: Request,
    force: bool = False,
    pool: asyncpg.Pool = Depends(get_db_pool),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_DELETE)),
) -> ReferenceDeleteResponse:
    """Fully purge a reference — Postgres rows, DuckLake data, and on-disk
    indexes. system_admin only (`reference:delete`).

    Gating: work tickets in-flight (pending/queued/processing) block the delete
    unconditionally (409); terminal tickets (completed/no_data/failed) block it
    unless `force=true` is passed. Shared features (claimed by another reference) are never deleted.

    Ordering is data-plane → orchestrator → Postgres, chosen so the operation
    is *retriable*: every step is idempotent and the `qiita.reference` row — the
    thing a retry keys off — is removed last. The data-plane delete is one
    DuckLake transaction (all-or-nothing), so a failure there leaves DuckLake
    membership fully intact and a retry recomputes the same orphan set. If the
    orchestrator or Postgres step fails, the reference row survives and
    re-issuing the DELETE re-runs every idempotent step. The one residual
    degraded state is a Postgres teardown that fails *after* the data is gone:
    the reference is then empty-but-listed until a retry completes the teardown
    (Postgres membership is still intact, so its orphan GC stays correct).
    Reclaiming DuckLake/disk bytes in that window is not yet automated.
    """
    try:
        await assert_reference_deletable(pool, reference_idx, force=force)
    except ReferenceNotFound:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    except ReferenceDeleteBlocked as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    # DuckLake data (idempotent, atomic delete-by-reference_idx in the data plane).
    try:
        await delete_reference_data(
            reference_idx=reference_idx,
            signing_key=signing_key,
            data_plane_url=data_plane_url,
        )
    except _flight.FlightError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"data plane reference delete failed; nothing removed yet: {exc}",
        ) from exc

    # On-disk index artifacts (orchestrator-side; only it can reach
    # PATH_DERIVED). Skipped when no orchestrator is configured (CP-only/dev),
    # in which case there are no compute-built indexes to remove anyway. An
    # orchestrator transport/5xx error here surfaces as a 502 (not an unhandled
    # 500): DuckLake data is already gone, but the reference row still exists,
    # so the operator can re-run the idempotent DELETE to finish cleanup.
    artifacts_removed = False
    client = getattr(request.app.state, "compute_backend_client", None)
    if client is not None:
        try:
            purge = await client.purge_reference_artifacts(reference_idx)
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502,
                detail=(
                    "orchestrator on-disk artifact cleanup failed after DuckLake"
                    f" data was removed; re-run the delete to finish: {exc}"
                ),
            ) from exc
        artifacts_removed = purge.removed

    # Re-gate inside the teardown transaction to close the precheck→cascade
    # window: a work ticket that went in-flight since the precheck must abort
    # the teardown (and 409 loudly) rather than be silently deleted by the
    # cascade. force=True here means only a *new in-flight* ticket aborts —
    # terminal tickets are still the cascade's to delete.
    async with tx() as conn:
        try:
            await assert_reference_deletable(conn, reference_idx, force=True)
        except ReferenceNotFound:
            raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
        except ReferenceDeleteBlocked as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        counts = await delete_reference_cascade(conn, reference_idx)

    return ReferenceDeleteResponse(
        reference_idx=reference_idx,
        artifacts_removed=artifacts_removed,
        **counts,
    )


# Tables that can appear in a DoGet ticket, CP-side mirror of the data plane's
# ALLOWED_TABLES whitelist in flight_service.rs. Must stay in sync with it.
# `read_masked` (the masked-read surface) is the one the data plane reaches via
# Flight in addition to the reference_* tables; raw `read`/`read_mask` are
# deliberately absent from both allowlists (privacy by construction).
_DOGET_ALLOWED_TABLES = frozenset(
    {
        "reference_sequences",
        "reference_sequence_chunks",
        # The exclusion-aware taxonomy VIEW, not the raw base table: taxonomy
        # reads go through the anti-join view so a curated exclusion can't be
        # bypassed (the read_masked-over-read model). Raw `reference_taxonomy` is
        # the register-files write target but is NOT Flight-readable.
        "reference_taxonomy_visible",
        "reference_phylogeny",
        "reference_placements",
        "reference_annotation",
        "read_masked",
        # The alignment sink's DoGet read-side (feature-table OGU consumer), as the
        # exclusion-aware VIEW (raw `alignment` is not Flight-readable). Like
        # read_masked it is served by its own route (routes/alignment.py), scoped
        # by alignment_idx + prep_sample_idx, so it is excluded from
        # _REFERENCE_DOGET_TABLES below.
        "alignment_visible",
    }
)

# The subset the reference DoGet route below can sign. `read_masked` is reached
# through the dedicated /read-masked/ticket/doget route (routes/read_masked.py),
# whose ticket carries (prep_sample_idx, mask_idx) — not reference_idx — and
# which enforces the mandatory-filter invariant. `alignment_visible` is served by
# routes/alignment.py (scoped by alignment_idx + prep_sample_idx). The reference
# route restricts itself to the reference_* tables whose membership it resolves —
# including `reference_taxonomy_visible`, so external taxonomy reads also go
# through the exclusion view.
_REFERENCE_DOGET_TABLES = _DOGET_ALLOWED_TABLES - frozenset({"read_masked", "alignment_visible"})


@router.post(PATH_REFERENCE_DOGET, status_code=201)
async def create_doget_ticket(
    reference_idx: Annotated[int, Field(gt=0)],
    body: DoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    _scope: Principal = Depends(require_scope(Scope.TICKET_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to a reference.

    The ticket always carries `reference_idx`; when the request body supplies
    a `feature_idx` subset, the ticket additionally scopes to those features
    (filter gains `"feature_idx":[...]`), so a shard builder streams only its
    own roster's sequences. Omitting `feature_idx` yields the whole-reference
    ticket the data plane resolves at query time via the DuckLake
    reference_membership table (JOIN for reference_sequences, direct WHERE for
    taxonomy/phylogeny).

    Status gate admits `active` AND `indexing`: a shard build streams
    mid-ingest (status `indexing`, post-`register-files`) and a re-index
    streams from an `active` reference. An `indexing` reference whose data is
    not yet in DuckLake simply yields an empty stream. `pending`/`loading`
    (pre-DuckLake) are 409; a missing reference is 404.

    Authorization is scope-only at this layer: any principal with
    `tickets:doget` can request a ticket. Row-level visibility (private
    references) is not yet implemented.
    """
    if body.table not in _REFERENCE_DOGET_TABLES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown table {body.table!r}; allowed: {sorted(_REFERENCE_DOGET_TABLES)}",
        )

    _STREAMABLE_STATUSES = (ReferenceStatus.ACTIVE.value, ReferenceStatus.INDEXING.value)
    status = await pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        reference_idx,
    )
    if status is None:
        raise HTTPException(status_code=404, detail=_MSG_REFERENCE_NOT_FOUND)
    if status not in _STREAMABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Reference status is {status!r}, must be one of {list(_STREAMABLE_STATUSES)}",
        )

    filter: dict[str, list[int]] = {"reference_idx": [reference_idx]}
    if body.feature_idx:
        filter["feature_idx"] = body.feature_idx

    ticket_bytes = sign_ticket(
        table=body.table,
        filter=filter,
        secret=signing_key,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
