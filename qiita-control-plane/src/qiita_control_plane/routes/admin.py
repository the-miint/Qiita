"""Admin endpoints — service accounts, principal status / role mutations,
audit log read, bulk token revocation. All routes require system_admin role
PLUS the appropriate admin:* scope, so a token-scoped-narrow system_admin
can't exfiltrate audit data without the right scope.
"""

import base64
import json
from datetime import UTC, datetime, timedelta

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from qiita_common.api_paths import (
    PATH_ADMIN_AUDIT,
    PATH_ADMIN_MASKED_READ_EXPORT_TICKET,
    PATH_ADMIN_PREFIX,
    PATH_ADMIN_PRINCIPAL_DISABLED,
    PATH_ADMIN_PRINCIPAL_RETIRED,
    PATH_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS,
    PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE,
    PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
    PATH_ADMIN_SERVICE_ACCOUNT,
    PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID,
)
from qiita_common.auth_constants import (
    AUDIT_QUERY_DEFAULT_LIMIT,
    AUDIT_QUERY_MAX_LIMIT,
    MSG_PRINCIPAL_NOT_FOUND,
    SYSTEM_PRINCIPAL_IDX,
    AuthEventType,
    Scope,
    SystemRole,
)
from qiita_common.models import (
    AuthEventResponse,
    DoGetTicketResponse,
    MaskedReadExportManifest,
    MaskedReadExportSample,
    MaskedReadExportTicketRequest,
    OwnerBiosampleIdExportResponse,
    OwnerBiosampleIdRow,
    PrincipalDisabledUpdate,
    PrincipalRetiredUpdate,
    PrincipalSystemRoleUpdate,
    RevokeAllTokensResponse,
    ServiceAccountCreate,
    ServiceAccountCreateResponse,
)

from ..auth.audit import AuthEvent, record_event, record_event_bulk
from ..auth.db import insert_principal, rows_affected
from ..auth.guards import require_human_with_role, require_scope
from ..auth.principal import HumanUser, Principal
from ..auth.scopes import (
    SERVICE_ACCOUNT_SCOPE_CEILING,
    validate_scopes_against_ceiling,
)
from ..auth.tickets import sign_ticket
from ..auth.token import mint_api_token
from ..deps import TxConnFactory, get_db_pool, get_hmac_secret, get_tx_conn_factory

router = APIRouter(prefix=PATH_ADMIN_PREFIX, tags=["admin"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decode_detail(raw) -> dict:
    """asyncpg returns JSONB as a string by default; decode once for response."""
    return json.loads(raw) if isinstance(raw, str) else (raw or {})


# ---------------------------------------------------------------------------
# POST /admin/service-account
# ---------------------------------------------------------------------------


@router.post(
    PATH_ADMIN_SERVICE_ACCOUNT,
    status_code=201,
    response_model=ServiceAccountCreateResponse,
)
async def create_service_account(
    body: ServiceAccountCreate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    actor: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_SERVICE_ACCOUNT)),
) -> ServiceAccountCreateResponse | JSONResponse:
    """Create a service-account-kind principal and mint its initial token.

    Scopes are validated against `SERVICE_ACCOUNT_SCOPE_CEILING` — workers
    don't fit the human role hierarchy, so admins must spell out what the
    worker is allowed to do, bounded by the service ceiling. 409 on
    duplicate name; 422 on out-of-ceiling scopes (flat body shape, matches
    /auth/pat).
    """
    # Scope ceiling check
    rejection = validate_scopes_against_ceiling(
        body.scopes,
        SERVICE_ACCOUNT_SCOPE_CEILING,
        ceiling_violation_detail="scopes not granted to service accounts",
    )
    if rejection is not None:
        return rejection

    expires_at = (
        datetime.now(UTC) + timedelta(days=body.ttl_days) if body.ttl_days is not None else None
    )

    async with tx() as conn:
        try:
            principal_idx = await insert_principal(
                conn,
                display_name=body.name,
                created_by_idx=actor.principal_idx,
            )
            await conn.execute(
                "INSERT INTO qiita.service_account"
                "  (principal_idx, name, description)"
                " VALUES ($1, $2, $3)",
                principal_idx,
                body.name,
                body.description,
            )
            # mint inside the same transaction for atomicity
            plaintext, token_idx = await mint_api_token(
                conn,
                principal_idx=principal_idx,
                label=body.label,
                scopes=body.scopes,
                expires_at=expires_at,
            )
            await record_event(
                conn,
                event_type=AuthEventType.TOKEN_MINT,
                principal_idx=principal_idx,
                actor_principal_idx=actor.principal_idx,
                detail={
                    "token_idx": token_idx,
                    "scopes": body.scopes,
                    "kind": "service_account_initial",
                },
            )
        except asyncpg.UniqueViolationError as exc:
            # Dispatch on the constraint name so a future second UNIQUE
            # column on qiita.service_account doesn't get misattributed to
            # the name. PostgreSQL auto-names inline UNIQUE constraints
            # `<table>_<col>_key`.
            if exc.constraint_name == "service_account_name_key":
                raise HTTPException(
                    status_code=409,
                    detail=f"service account named {body.name!r} already exists",
                ) from exc
            raise  # unknown UNIQUE — let FastAPI surface as 500

    return ServiceAccountCreateResponse(
        principal_idx=principal_idx,
        name=body.name,
        description=body.description,
        token=plaintext,
        token_idx=token_idx,
        scopes=body.scopes,
        expires_at=expires_at,
        created_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# PATCH /admin/principal/{idx}/disabled
# ---------------------------------------------------------------------------


@router.patch(PATH_ADMIN_PRINCIPAL_DISABLED, status_code=204)
async def set_principal_disabled(
    principal_idx: int,
    body: PrincipalDisabledUpdate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    actor: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_USER)),
) -> None:
    """Toggle disabled state. `disabled=true` sets the audit columns;
    `disabled=false` clears them (round-trip back to active). The DB CHECK
    `principal_disabled_consistent` ensures atomicity."""
    if principal_idx == SYSTEM_PRINCIPAL_IDX:
        raise HTTPException(status_code=403, detail="cannot disable system principal")

    if body.disabled and not body.reason:
        raise HTTPException(status_code=422, detail="reason is required when disabling")

    async with tx() as conn:
        if body.disabled:
            try:
                result = await conn.execute(
                    "UPDATE qiita.principal SET"
                    "  disabled = true, disabled_at = now(),"
                    "  disabled_by_idx = $2, disable_reason = $3"
                    " WHERE idx = $1 AND disabled = false AND retired = false",
                    principal_idx,
                    actor.principal_idx,
                    body.reason,
                )
            except asyncpg.CheckViolationError as exc:
                # Map the constraint to a stable client message rather than
                # leaking the raw constraint identifier (schema internal).
                if exc.constraint_name == "principal_system_principal_always_active":
                    detail = "system principals cannot be disabled"
                else:  # principal_not_both_disabled_and_retired (race with concurrent retire)
                    detail = "principal cannot be disabled: it is retired"
                raise HTTPException(status_code=409, detail=detail) from exc
        else:
            result = await conn.execute(
                "UPDATE qiita.principal SET"
                "  disabled = false, disabled_at = NULL,"
                "  disabled_by_idx = NULL, disable_reason = NULL"
                " WHERE idx = $1 AND disabled = true",
                principal_idx,
            )

        if rows_affected(result) == 0:
            # Either principal doesn't exist, or already in target state, or
            # retired (terminal). Distinguish via a follow-up read.
            row = await conn.fetchrow(
                "SELECT disabled, retired FROM qiita.principal WHERE idx = $1",
                principal_idx,
            )
            if row is None:
                raise HTTPException(status_code=404, detail=MSG_PRINCIPAL_NOT_FOUND)
            if row["retired"]:
                raise HTTPException(status_code=409, detail="principal is retired (terminal)")
            # Already in target state → idempotent success.
            return

        await record_event(
            conn,
            event_type=(
                AuthEventType.PRINCIPAL_DISABLED
                if body.disabled
                else AuthEventType.PRINCIPAL_ENABLED
            ),
            principal_idx=principal_idx,
            actor_principal_idx=actor.principal_idx,
            detail={"reason": body.reason} if body.disabled else {},
        )


# ---------------------------------------------------------------------------
# PATCH /admin/principal/{idx}/retired
# ---------------------------------------------------------------------------


@router.patch(PATH_ADMIN_PRINCIPAL_RETIRED, status_code=204)
async def retire_principal(
    principal_idx: int,
    body: PrincipalRetiredUpdate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    actor: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_USER)),
) -> None:
    """Retirement is terminal. The DB trigger revokes all the principal's
    active tokens automatically. An admin cannot retire themselves (refuses
    to leave zero active system_admins is enforced by application logic
    here — the DB trigger doesn't know roles)."""
    if principal_idx == SYSTEM_PRINCIPAL_IDX:
        raise HTTPException(status_code=403, detail="cannot retire system principal")

    if actor.principal_idx == principal_idx:
        raise HTTPException(status_code=403, detail="admin cannot retire themselves")

    async with tx() as conn:
        try:
            result = await conn.execute(
                "UPDATE qiita.principal SET"
                "  retired = true, retired_at = now(),"
                "  retired_by_idx = $2, retire_reason = $3"
                " WHERE idx = $1 AND retired = false",
                principal_idx,
                actor.principal_idx,
                body.reason,
            )
        except asyncpg.CheckViolationError as exc:
            # Map the constraint to a stable client message rather than leaking
            # the raw constraint identifier (schema internal).
            if exc.constraint_name == "principal_system_principal_always_active":
                detail = "system principals cannot be retired"
            else:  # principal_not_both_disabled_and_retired (race with a concurrent disable)
                detail = "principal cannot be retired: it is disabled or was disabled concurrently"
            raise HTTPException(status_code=409, detail=detail) from exc

        if rows_affected(result) == 0:
            row = await conn.fetchrow(
                "SELECT retired FROM qiita.principal WHERE idx = $1", principal_idx
            )
            if row is None:
                raise HTTPException(status_code=404, detail=MSG_PRINCIPAL_NOT_FOUND)
            # Already retired → idempotent success.
            return

        await record_event(
            conn,
            event_type=AuthEventType.PRINCIPAL_RETIRED,
            principal_idx=principal_idx,
            actor_principal_idx=actor.principal_idx,
            detail={"reason": body.reason},
        )


# ---------------------------------------------------------------------------
# PATCH /admin/principal/{idx}/system-role
# ---------------------------------------------------------------------------


@router.patch(PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE, status_code=204)
async def set_principal_system_role(
    principal_idx: int,
    body: PrincipalSystemRoleUpdate,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    actor: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_USER)),
) -> None:
    """Set the principal's system_role. The DB enum validates the value;
    Pydantic's SystemRole StrEnum narrows it before we hit the DB."""
    if principal_idx == SYSTEM_PRINCIPAL_IDX:
        raise HTTPException(status_code=403, detail="cannot modify system principal's role")

    async with tx() as conn:
        old_role = await conn.fetchval(
            "SELECT system_role FROM qiita.principal WHERE idx = $1", principal_idx
        )
        if old_role is None:
            raise HTTPException(status_code=404, detail=MSG_PRINCIPAL_NOT_FOUND)

        await conn.execute(
            "UPDATE qiita.principal SET system_role = $1 WHERE idx = $2",
            body.system_role,
            principal_idx,
        )

        await record_event(
            conn,
            event_type=AuthEventType.SYSTEM_ROLE_CHANGE,
            principal_idx=principal_idx,
            actor_principal_idx=actor.principal_idx,
            detail={"from": old_role, "to": body.system_role, "reason": body.reason},
        )


# ---------------------------------------------------------------------------
# GET /admin/audit
# ---------------------------------------------------------------------------


@router.get(PATH_ADMIN_AUDIT)
async def get_audit_log(
    pool: asyncpg.Pool = Depends(get_db_pool),
    _role: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_AUDIT_READ)),
    principal_idx: int | None = Query(default=None),
    event_type: str | None = Query(default=None),
    limit: int = Query(default=AUDIT_QUERY_DEFAULT_LIMIT, ge=1, le=AUDIT_QUERY_MAX_LIMIT),
) -> list[AuthEventResponse]:
    """List audit events, newest first. Optional filters by principal_idx
    and event_type."""
    sql = (
        "SELECT event_idx, event_type, principal_idx, actor_principal_idx,"
        " detail, occurred_at FROM qiita.auth_event WHERE 1=1"
    )
    params: list = []
    if principal_idx is not None:
        params.append(principal_idx)
        sql += f" AND principal_idx = ${len(params)}"
    if event_type is not None:
        params.append(event_type)
        sql += f" AND event_type = ${len(params)}"
    sql += f" ORDER BY event_idx DESC LIMIT ${len(params) + 1}"
    params.append(limit)

    rows = await pool.fetch(sql, *params)
    return [
        AuthEventResponse(
            event_idx=r["event_idx"],
            event_type=r["event_type"],
            principal_idx=r["principal_idx"],
            actor_principal_idx=r["actor_principal_idx"],
            detail=_decode_detail(r["detail"]),
            occurred_at=r["occurred_at"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# POST /admin/principal/{idx}/revoke-all-tokens
# ---------------------------------------------------------------------------


@router.post(PATH_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS)
async def revoke_all_principal_tokens(
    principal_idx: int,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    actor: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
) -> RevokeAllTokensResponse:
    """Bulk-revoke every active token belonging to the target principal.

    Scope policy: targeting a user-kind principal requires `admin:users`;
    targeting a service-kind principal requires `admin:service_accounts`.
    The route resolves the target's kind first and demands the matching
    scope so an admin can be issued a narrower-than-full token (e.g. for
    a workflow that only manages workers) and have the route layer enforce
    the boundary.

    Idempotent on already-revoked tokens (counted separately). Emits one
    token_revoke audit event per newly-revoked token so the audit trail
    records the bulk action atomically per token.
    """
    async with tx() as conn:
        kind = await conn.fetchval(
            "SELECT CASE"
            " WHEN EXISTS (SELECT 1 FROM qiita.user WHERE principal_idx = $1) THEN 'user'"
            " WHEN EXISTS (SELECT 1 FROM qiita.service_account WHERE principal_idx = $1)"
            "      THEN 'service'"
            " ELSE 'bare' END",
            principal_idx,
        )
        required_scope = Scope.ADMIN_USER if kind == "user" else Scope.ADMIN_SERVICE_ACCOUNT
        if kind == "bare":
            # No subtype row, so no tokens either — but still surface 404 instead
            # of silently succeeding so the caller's intent is verified.
            principal_exists = await conn.fetchval(
                "SELECT 1 FROM qiita.principal WHERE idx = $1", principal_idx
            )
            if not principal_exists:
                raise HTTPException(status_code=404, detail=MSG_PRINCIPAL_NOT_FOUND)
        if not actor.has_scope(required_scope):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"missing required scope {str(required_scope)!r} for {kind}-kind principal"
                ),
            )

        rows = await conn.fetch(
            "UPDATE qiita.api_token SET revoked_at = now()"
            " WHERE principal_idx = $1 AND revoked_at IS NULL"
            " RETURNING token_idx",
            principal_idx,
        )
        revoked_idxs = [r["token_idx"] for r in rows]

        already_revoked = await conn.fetchval(
            "SELECT count(*) FROM qiita.api_token"
            " WHERE principal_idx = $1 AND revoked_at IS NOT NULL"
            "   AND token_idx <> ALL($2::bigint[])",
            principal_idx,
            revoked_idxs,
        )

        await record_event_bulk(
            conn,
            events=[
                AuthEvent(
                    event_type=AuthEventType.TOKEN_REVOKE,
                    principal_idx=principal_idx,
                    actor_principal_idx=actor.principal_idx,
                    detail={"token_idx": tidx, "reason": "admin_bulk"},
                )
                for tidx in revoked_idxs
            ],
        )

    return RevokeAllTokensResponse(
        revoked_token_idxs=revoked_idxs,
        already_revoked_count=already_revoked or 0,
    )


# ---------------------------------------------------------------------------
# GET /admin/study/{study_idx}/owner-biosample-id
# ---------------------------------------------------------------------------

# Study-wide export: every active biosample link in the study, paired with the
# owner-submitted original name. Only the per-study link retirement
# (bts.retired) is filtered — entity-level biosample.retired ("withdrawn
# everywhere") is intentionally included (see the route docstring). LEFT JOIN
# on the owner-id metadata row so a biosample missing it surfaces as a NULL
# owner_biosample_id rather than silently dropping out of the export.
_OWNER_ID_STUDY_SQL = (
    "SELECT b.idx AS biosample_idx,"
    "       b.biosample_accession,"
    "       m.value_text AS owner_biosample_id"
    "  FROM qiita.biosample_to_study bts"
    "  JOIN qiita.biosample b ON b.idx = bts.biosample_idx"
    "  LEFT JOIN qiita.biosample_metadata m"
    "    ON m.biosample_idx = b.idx AND m.is_owner_biosample_id = true"
    " WHERE bts.study_idx = $1"
    "   AND bts.retired = false"
    " ORDER BY b.idx"
)

# Pool-filtered export: the study's sequenced_samples in one pool. Restricting
# to the study is via the active prep_sample_to_study link, not the biosample
# link, because a pool's samples are scoped to their prep_sample. Mirrors the
# study-mode retirement handling: only the per-study link (psts.retired) is
# filtered; entity-level prep_sample.retired / biosample.retired are
# intentionally included (see the route docstring). Carries the prep_sample_idx
# and ENA experiment/run accessions in addition to the biosample columns.
_OWNER_ID_POOL_SQL = (
    "SELECT b.idx AS biosample_idx,"
    "       b.biosample_accession,"
    "       ss.prep_sample_idx,"
    "       ss.ena_experiment_accession,"
    "       ss.ena_run_accession,"
    "       m.value_text AS owner_biosample_id"
    "  FROM qiita.sequenced_sample ss"
    "  JOIN qiita.prep_sample_to_study psts"
    "    ON psts.prep_sample_idx = ss.prep_sample_idx"
    "   AND psts.study_idx = $1"
    "   AND psts.retired = false"
    "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
    "  JOIN qiita.biosample b ON b.idx = ps.biosample_idx"
    "  LEFT JOIN qiita.biosample_metadata m"
    "    ON m.biosample_idx = b.idx AND m.is_owner_biosample_id = true"
    " WHERE ss.sequenced_pool_idx = $2"
    " ORDER BY ss.prep_sample_idx"
)


@router.get(PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID)
async def export_owner_biosample_id(
    study_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _role: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_BIOSAMPLE_OWNER_ID_READ)),
    sequenced_pool_idx: int | None = Query(default=None, gt=0),
) -> OwnerBiosampleIdExportResponse:
    """Re-identification export: the owner-submitted original sample names for
    a study, keyed by minted biosample_idx + public accession.

    The owner name (biosample_metadata.value_text where
    is_owner_biosample_id=true) is PII-pinned and masked on the normal read
    path; this route is the only way to recover it, so it is gated by
    system_admin PLUS admin:biosample_owner_id_read.

    Without sequenced_pool_idx, returns one row per active biosample link in
    the study. With it, returns the study's sequenced_samples in that pool,
    adding prep_sample_idx + ENA experiment/run accessions. 404 if the study
    (or the named pool) does not exist — an empty result for a real study is a
    valid answer, but a typo'd idx should fail loudly rather than masquerade as
    "no samples".

    Retirement: both modes filter only the per-study *link* retirement
    (biosample_to_study.retired in study mode, prep_sample_to_study.retired in
    pool mode) — i.e. rows the study currently has permission to use.
    Entity-level retirement (biosample.retired / prep_sample.retired,
    "withdrawn everywhere") does NOT drop study membership and is deliberately
    *included*: an admin re-identifying samples for governance (purge, consent
    reconciliation, notification) needs the withdrawn ones too. The two modes
    treat retirement identically — each filters its own study-membership link
    and neither filters the entity-level flag.
    """
    if await pool.fetchval("SELECT 1 FROM qiita.study WHERE idx = $1", study_idx) is None:
        raise HTTPException(status_code=404, detail=f"no study with idx={study_idx}")

    if sequenced_pool_idx is not None:
        if (
            await pool.fetchval(
                "SELECT 1 FROM qiita.sequenced_pool WHERE idx = $1", sequenced_pool_idx
            )
            is None
        ):
            raise HTTPException(
                status_code=404, detail=f"no sequenced_pool with idx={sequenced_pool_idx}"
            )
        db_rows = await pool.fetch(_OWNER_ID_POOL_SQL, study_idx, sequenced_pool_idx)
        rows = [
            OwnerBiosampleIdRow(
                biosample_idx=r["biosample_idx"],
                biosample_accession=r["biosample_accession"],
                owner_biosample_id=r["owner_biosample_id"],
                prep_sample_idx=r["prep_sample_idx"],
                ena_experiment_accession=r["ena_experiment_accession"],
                ena_run_accession=r["ena_run_accession"],
            )
            for r in db_rows
        ]
    else:
        db_rows = await pool.fetch(_OWNER_ID_STUDY_SQL, study_idx)
        rows = [
            OwnerBiosampleIdRow(
                biosample_idx=r["biosample_idx"],
                biosample_accession=r["biosample_accession"],
                owner_biosample_id=r["owner_biosample_id"],
            )
            for r in db_rows
        ]

    return OwnerBiosampleIdExportResponse(
        study_idx=study_idx,
        sequenced_pool_idx=sequenced_pool_idx,
        row_count=len(rows),
        rows=rows,
    )


# ---------------------------------------------------------------------------
# Masked-read export (system_admin + admin:masked_read_export)
# ---------------------------------------------------------------------------

# The masked-read view table the export ticket is signed for. Must match the
# data plane's ALLOWED_TABLES and the CP-side _DOGET_ALLOWED_TABLES
# (routes/reference.py) and the service-account read_masked route's own constant.
_READ_MASKED_TABLE = "read_masked"

# Export tickets are minted at the data plane's MAX_TICKET_LIFETIME (3600 s).
# The data plane verifies expiry only at DoGet initiation, never mid-stream, so
# this bounds mint -> stream-start (sub-second with just-in-time per-sample
# minting), not the download — a multi-hour single-sample stream is unaffected.
_EXPORT_TICKET_TTL_SECONDS = 3600

# Roster of a sequenced_pool's non-retired samples to export: the prep_sample_idx
# (the read_masked join key) + biosample_accession (the filename's leading part;
# NULL until NCBI submission, surfaced so the export fails loudly rather than
# silently dropping the sample) + the per-(mask_idx, prep_sample) completion gate
# state (LEFT JOIN mask_sample — NULL when no block-mask gate row exists, i.e. the
# per-sample read-mask path or an unmasked sample). The pool-wide run/pool idxs
# live on the manifest. `$2` is the mask_idx the manifest is scoped to.
_MASKED_EXPORT_ROSTER_SQL = (
    "SELECT ss.prep_sample_idx, bs.biosample_accession, msamp.state AS mask_state"
    "  FROM qiita.sequenced_sample ss"
    "  JOIN qiita.prep_sample ps ON ps.idx = ss.prep_sample_idx"
    "  JOIN qiita.biosample bs ON bs.idx = ps.biosample_idx"
    "  LEFT JOIN qiita.mask_sample msamp"
    "    ON msamp.prep_sample_idx = ss.prep_sample_idx AND msamp.mask_idx = $2"
    " WHERE ss.sequenced_pool_idx = $1 AND ps.retired = false"
    " ORDER BY ss.prep_sample_idx"
)


@router.get(PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT)
async def export_masked_read_manifest(
    sequenced_pool_idx: int,
    pool: asyncpg.Pool = Depends(get_db_pool),
    _role: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_MASKED_READ_EXPORT)),
    mask_idx: int = Query(gt=0),
) -> MaskedReadExportManifest:
    """Roster manifest for a per-pool masked-read export: one row per
    non-retired sample on the pool, each with the filename parts the
    qiita-admin masked-read-export CLI needs. The caller then mints a per-sample
    DoGet ticket and streams that sample's read_masked rows from the data plane.

    Gated by system_admin PLUS admin:masked_read_export — the first human
    masked-read pull. `mask_idx` is mandatory (the data plane keys read_masked
    on (prep_sample_idx, mask_idx)). 404 if the pool or mask does not exist — an
    empty roster for a real pool is a valid answer, but a typo'd idx fails loudly
    rather than masquerading as "no samples". Entity-level prep_sample retirement
    is excluded (ps.retired = false); biosample_accession is surfaced even when
    NULL so the CLI fails loudly on an unsubmitted sample rather than the route
    silently dropping it.
    """
    run_idx = await pool.fetchval(
        "SELECT sequencing_run_idx FROM qiita.sequenced_pool WHERE idx = $1", sequenced_pool_idx
    )
    if run_idx is None:
        raise HTTPException(
            status_code=404, detail=f"no sequenced_pool with idx={sequenced_pool_idx}"
        )
    mask_exists = await pool.fetchval(
        "SELECT 1 FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx
    )
    if mask_exists is None:
        raise HTTPException(status_code=404, detail=f"no mask_definition with mask_idx={mask_idx}")

    db_rows = await pool.fetch(_MASKED_EXPORT_ROSTER_SQL, sequenced_pool_idx, mask_idx)
    return MaskedReadExportManifest(
        sequenced_pool_idx=sequenced_pool_idx,
        sequencing_run_idx=run_idx,
        mask_idx=mask_idx,
        samples=[MaskedReadExportSample.model_validate(dict(r)) for r in db_rows],
    )


@router.post(PATH_ADMIN_MASKED_READ_EXPORT_TICKET, status_code=201)
async def create_masked_read_export_ticket(
    body: MaskedReadExportTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    hmac_secret: bytes = Depends(get_hmac_secret),
    _role: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
    _scope: Principal = Depends(require_scope(Scope.ADMIN_MASKED_READ_EXPORT)),
) -> DoGetTicketResponse:
    """Mint a Flight DoGet ticket scoped to one (prep_sample_idx, mask_idx) on
    the data plane's read_masked view — the human (system_admin) counterpart to
    the service-account POST /read-masked/ticket/doget. The export CLI mints one
    just-in-time per sample.

    Gated by system_admin PLUS admin:masked_read_export. Both identifiers are
    mandatory (Pydantic gt=0), so the signed filter is always non-empty; the
    route re-asserts that before signing as defence in depth — the data plane's
    empty-filter path would otherwise dump every sample's pass reads. Minted at
    the 3600 s max (the data plane's ceiling; expiry is checked only at DoGet
    initiation, so it never bounds the download).

    Completion gate (block-masked samples): if a `qiita.mask_sample` row exists
    for this `(mask_idx, prep_sample)` and is NOT 'completed', the sample's mask
    is assembled by several blocks and at least one is still in flight — its
    read_mask is PARTIAL, so a pull would silently truncate. Refuse with 409. A
    sample with NO mask_sample row (the per-sample read-mask path, or unmasked) is
    unaffected: that path writes a sample's read_mask all-or-nothing, so absence
    preserves the old guarantee and the ticket is minted.
    """
    filter_ = {
        "prep_sample_idx": [body.prep_sample_idx],
        "mask_idx": [body.mask_idx],
    }
    if not filter_ or any(not v for v in filter_.values()):
        raise HTTPException(
            status_code=422,
            detail="masked-read export ticket requires a non-empty prep_sample_idx and mask_idx",
        )

    mask_state = await pool.fetchval(
        "SELECT state FROM qiita.mask_sample WHERE mask_idx = $1 AND prep_sample_idx = $2",
        body.mask_idx,
        body.prep_sample_idx,
    )
    if mask_state is not None and mask_state != "completed":
        raise HTTPException(
            status_code=409,
            detail={
                "reason": (
                    "the sample's block-mask is not yet complete "
                    f"(mask_sample.state={mask_state!r}); a covering block is still in "
                    "flight, so its read_mask is partial. Refusing to export a "
                    "partially-masked sample — retry once reconcile marks it completed."
                ),
                "prep_sample_idx": body.prep_sample_idx,
                "mask_idx": body.mask_idx,
                "mask_state": mask_state,
            },
        )

    ticket_bytes = sign_ticket(
        table=_READ_MASKED_TABLE,
        filter=filter_,
        secret=hmac_secret,
        ttl_seconds=_EXPORT_TICKET_TTL_SECONDS,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())
