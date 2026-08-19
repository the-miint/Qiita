"""Auth endpoints — /auth/whoami, /auth/pat, /auth/token, /auth/login,
/auth/handoff, /auth/cli-exchange.

Routes use the `get_current_principal` resolver where they consume a
bearer; the AuthRocket-driven login flow uses cookie-anchored freshness
instead and bypasses the resolver:

  GET  /auth/login        — Set signed cookie, 302 to AuthRocket with
                            prompt=login. Optional cli=1&port=N branches
                            the handoff into the CLI loopback flow.
  GET  /auth/handoff      — Receive ?token=<JWT> from AuthRocket.
                            Verify cookie freshness + JWT, run resolver
                            upsert, mint a PAT.
  POST /auth/cli-exchange — CLI redeems a one-time code captured from
                            the loopback redirect; we return the PAT
                            plaintext exactly once.

POST /auth/pat continues to mint PATs from a bearer JWT for backward
compatibility with the operator out-of-band path; the /auth/login flow
above is the supported route forward.
"""

import html
import time
from datetime import UTC, datetime, timedelta

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from qiita_common.api_paths import (
    LOOPBACK_HOST,
    PATH_AUTH_CLI_EXCHANGE,
    PATH_AUTH_HANDOFF,
    PATH_AUTH_LOGIN,
    PATH_AUTH_PAT,
    PATH_AUTH_PREFIX,
    PATH_AUTH_TOKEN,
    PATH_AUTH_TOKEN_BY_IDX,
    PATH_AUTH_WHOAMI,
    URL_AUTH_HANDOFF,
    URL_AUTH_LOGIN,
)
from qiita_common.auth_constants import (
    BEARER_PREFIX,
    MSG_PRINCIPAL_DISABLED_OR_RETIRED,
    AuthEventType,
    Scope,
)
from qiita_common.models import (
    ApiTokenMintRequest,
    ApiTokenMintResponse,
    ApiTokenSummary,
    CliLoginExchangeRequest,
    WhoAmIAnonymousResponse,
    WhoAmIHumanResponse,
    WhoAmIResponse,
    WhoAmIServiceResponse,
)

from ..auth import TOKEN_PREFIX
from ..auth.audit import record_event
from ..auth.db import rows_affected
from ..auth.guards import require_scope
from ..auth.handoff import (
    LOGIN_COOKIE_MAX_AGE_SECONDS,
    LOGIN_COOKIE_NAME,
    CookieInvalid,
    build_authrocket_login_url,
    generate_ot_code,
    hash_ot_code,
    sign_login_cookie,
    verify_login_cookie,
)
from ..auth.oidc import InvalidJwt
from ..auth.principal import (
    Anonymous,
    HumanUser,
    Principal,
    ServiceAccount,
    get_current_principal,
    get_oidc_verifier,
    resolve_oidc,
)
from ..auth.scopes import (
    role_ceiling,
    validate_scopes_against_ceiling,
)
from ..auth.token import mint_api_token
from ..deps import TxConnFactory, get_db_pool, get_settings, get_tx_conn_factory

router = APIRouter(prefix=PATH_AUTH_PREFIX, tags=["auth"])


# ---------------------------------------------------------------------------
# GET /auth/whoami
# ---------------------------------------------------------------------------


@router.get(PATH_AUTH_WHOAMI, response_model=WhoAmIResponse)
async def whoami(p: Principal = Depends(get_current_principal)) -> WhoAmIResponse:
    """Return a serializable view of the authenticated principal.

    Public route: anonymous callers get `{"kind": "anonymous"}`. Allows clients
    to probe their own auth state without tripping a 401.
    """
    if isinstance(p, HumanUser):
        return WhoAmIHumanResponse(
            kind="human",
            principal_idx=p.principal_idx,
            email=p.email,
            system_role=p.system_role,
            scopes=sorted(p.scopes),
            profile_complete=p.profile_complete,
        )
    if isinstance(p, ServiceAccount):
        return WhoAmIServiceResponse(
            kind="service",
            principal_idx=p.principal_idx,
            name=p.name,
            scopes=sorted(p.scopes),
        )
    if isinstance(p, Anonymous):
        return WhoAmIAnonymousResponse(kind="anonymous")
    # Defensive: shouldn't happen.
    raise HTTPException(status_code=500, detail="unknown principal kind")


# ---------------------------------------------------------------------------
# POST /auth/pat
# ---------------------------------------------------------------------------


@router.post(PATH_AUTH_PAT, status_code=201, response_model=ApiTokenMintResponse)
async def mint_pat(
    body: ApiTokenMintRequest,
    request: Request,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> ApiTokenMintResponse | JSONResponse:
    """Mint a personal access token. Requires a *fresh* OIDC JWT — the
    auth_time claim must be within AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS,
    forcing a real interactive login before minting even when a long-lived
    JWT has been hoarded. PAT-mint via PAT (qk_) is rejected outright.

    On profile-incomplete, returns 422 with a flat body listing the
    missing fields — the CLI surfaces this to the user as an actionable
    error instead of silently failing.

    JWT verification and the freshness/leeway checks run without
    holding a pool connection. The user lookup acquires a pooled
    connection briefly; a transaction opens only around the mint +
    audit pair so 4xx paths return without an empty commit.
    """
    settings = get_settings(request)
    verifier = get_oidc_verifier(request)

    auth = request.headers.get("Authorization", "")
    if not auth.startswith(BEARER_PREFIX):
        raise HTTPException(status_code=401, detail="OIDC JWT required")
    bearer = auth[len(BEARER_PREFIX) :].strip()
    if bearer.startswith(TOKEN_PREFIX):
        raise HTTPException(
            status_code=401,
            detail="PAT mint requires an OIDC JWT, not an existing API token",
        )

    try:
        identity = verifier.verify(bearer)
    except InvalidJwt as exc:
        raise HTTPException(status_code=401, detail=f"invalid jwt: {exc}") from exc

    # Freshness check: auth_time must be present AND recent.
    if identity.auth_time is None:
        raise HTTPException(
            status_code=401,
            detail="PAT mint requires JWT with an auth_time claim",
        )
    age = int(time.time()) - identity.auth_time
    if age > settings.authrocket_pat_max_auth_age_seconds:
        raise HTTPException(
            status_code=401,
            detail=(
                f"auth_time is {age}s old; PAT mint requires a fresh login"
                f" (max {settings.authrocket_pat_max_auth_age_seconds}s)"
            ),
        )
    # An IdP with clock skew can issue auth_time slightly in the future.
    # Without a symmetric guard, a future auth_time produces a negative age
    # that trivially passes the > threshold check and bypasses freshness.
    # Allow up to one leeway window of forward skew, reject anything beyond.
    if age < -settings.authrocket_jwt_leeway_seconds:
        raise HTTPException(
            status_code=401,
            detail=(
                f"auth_time is {-age}s in the future; refusing PAT mint"
                f" (max forward skew {settings.authrocket_jwt_leeway_seconds}s)"
            ),
        )

    # Resolve the user from the verified identity on a pooled (non-tx)
    # connection — the lookup is read-only and the 4xx paths below must
    # not hold a transactional slot for an empty commit. The missing-fields
    # list is computed in SQL via qiita.user_profile_missing_fields so the
    # field set tracks the `profile_complete` generated column's definition
    # (both live in the auth migration; see
    # 20260429000001_user_profile_missing_fields.sql).
    user_row = await pool.fetchrow(
        "SELECT u.principal_idx, p.system_role, p.disabled, p.retired,"
        " u.profile_complete,"
        " qiita.user_profile_missing_fields(u.affiliation, u.address, u.phone)"
        "   AS missing_fields"
        " FROM qiita.user_identity ui"
        " JOIN qiita.user u ON u.principal_idx = ui.principal_idx"
        " JOIN qiita.principal p ON p.idx = u.principal_idx"
        " WHERE ui.issuer = $1 AND ui.subject = $2",
        identity.issuer,
        identity.subject,
    )
    if user_row is None:
        raise HTTPException(
            status_code=401,
            detail="no user matches this OIDC identity",
        )
    if user_row["disabled"] or user_row["retired"]:
        raise HTTPException(status_code=401, detail=MSG_PRINCIPAL_DISABLED_OR_RETIRED)

    if not user_row["profile_complete"]:
        # Flat 422 body so the CLI can pluck reason / missing_fields without
        # nested-detail unwrapping.
        return JSONResponse(
            status_code=422,
            content={
                "detail": "profile incomplete",
                "reason": "profile_incomplete",
                "missing_fields": user_row["missing_fields"],
            },
        )

    # Scope validation against the role ceiling. The rejection response
    # deliberately does NOT echo the caller's full ceiling — the caller
    # already knows it via /auth/whoami; echoing it per-request would leak
    # ceiling structure to a probing attacker.
    role = user_row["system_role"]
    ceiling = role_ceiling(role)
    if body.scopes is None:
        scopes = sorted(ceiling)
    else:
        rejection = validate_scopes_against_ceiling(
            body.scopes,
            ceiling,
            ceiling_violation_detail="scopes not granted by your role",
        )
        if rejection is not None:
            return rejection
        scopes = list(body.scopes)

    # TTL — Pydantic already enforces gt=0 and le=365.
    ttl_days = body.ttl_days if body.ttl_days is not None else settings.token_default_ttl_days
    expires_at = datetime.now(UTC) + timedelta(days=ttl_days)

    # Mint + audit share a tx so an audit failure rolls back the token row.
    # Inline rather than the TxConnFactory dep — the body-wide-tx shape that
    # dep wraps is a poor fit when most of this handler is non-transactional.
    async with pool.acquire() as conn:
        async with conn.transaction():
            plaintext, token_idx = await mint_api_token(
                conn,
                principal_idx=user_row["principal_idx"],
                label=body.label,
                scopes=scopes,
                expires_at=expires_at,
            )

            await record_event(
                conn,
                event_type=AuthEventType.TOKEN_MINT,
                principal_idx=user_row["principal_idx"],
                actor_principal_idx=user_row["principal_idx"],  # self-mint
                detail={"token_idx": token_idx, "scopes": scopes, "kind": "pat"},
            )

    return ApiTokenMintResponse(
        token=plaintext,
        token_idx=token_idx,
        label=body.label,
        scopes=scopes,
        expires_at=expires_at,
        created_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# GET /auth/token — list own tokens (metadata only)
# ---------------------------------------------------------------------------


@router.get(PATH_AUTH_TOKEN)
async def list_own_tokens(
    p: Principal = Depends(require_scope(Scope.SELF_TOKEN)),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> list[ApiTokenSummary]:
    """List the caller's own tokens. Metadata only — no plaintext, no hash.
    Anonymous: 401. Both HumanUser and ServiceAccount can list their own."""
    rows = await pool.fetch(
        "SELECT token_idx, label, scopes, expires_at, revoked_at,"
        "  last_used_at, created_at"
        " FROM qiita.api_token WHERE principal_idx = $1"
        " ORDER BY token_idx",
        p.principal_idx,
    )
    return [ApiTokenSummary.model_validate(dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# DELETE /auth/token/{token_idx} — revoke own token
# ---------------------------------------------------------------------------


@router.delete(PATH_AUTH_TOKEN_BY_IDX, status_code=204)
async def revoke_own_token(
    token_idx: int,
    p: Principal = Depends(require_scope(Scope.SELF_TOKEN)),
    tx: TxConnFactory = Depends(get_tx_conn_factory),
) -> None:
    """Revoke a token belonging to the caller. 401 if Anonymous, 403 if the
    caller's bearer lacks `self:tokens`, 404 for **either** "no such token"
    or "exists but owned by someone else" — same response so probing
    token_idx values does not enumerate the table.
    """
    async with tx() as conn:
        # Atomic UPDATE WHERE — only revokes if owner matches and token is not
        # already revoked. Returns 0 rows if either condition fails.
        result = await conn.execute(
            "UPDATE qiita.api_token SET revoked_at = now()"
            " WHERE token_idx = $1 AND principal_idx = $2 AND revoked_at IS NULL",
            token_idx,
            p.principal_idx,
        )
        if rows_affected(result) == 0:
            # Either the token doesn't exist, is owned by another principal, or
            # is owned by us but already revoked. To avoid leaking existence to
            # an attacker walking token_idx values, conflate the first two as
            # 404. The "already revoked" case is idempotent success, so we let
            # it return 204 silently.
            owner_idx = await conn.fetchval(
                "SELECT principal_idx FROM qiita.api_token WHERE token_idx = $1",
                token_idx,
            )
            if owner_idx is None or owner_idx != p.principal_idx:
                raise HTTPException(status_code=404, detail="token not found")
            # Owned by us but already revoked — idempotent success, no audit.
            return

        await record_event(
            conn,
            event_type=AuthEventType.TOKEN_REVOKE,
            principal_idx=p.principal_idx,
            actor_principal_idx=p.principal_idx,
            detail={"token_idx": token_idx, "reason": "self_revoke"},
        )


# ---------------------------------------------------------------------------
# GET /auth/login — start the AuthRocket LoginRocket Web round-trip
# ---------------------------------------------------------------------------


# Single source of truth for the login cookie's transport attributes —
# `begin_login` sets it with these, `_scrub_login_cookie` clears it with the
# same set so a future tweak (Domain, SameSite=Strict if cross-origin redirect
# rules change, etc.) lands in one place. SameSite=Strict would block the
# AuthRocket → /auth/handoff cross-origin redirect from carrying it.
_LOGIN_COOKIE_TRANSPORT = {
    "httponly": True,
    "secure": True,
    "samesite": "lax",
    "path": "/",
}


@router.get(PATH_AUTH_LOGIN)
async def begin_login(
    request: Request,
    cli: int = 0,
    port: int | None = None,
) -> RedirectResponse:
    """Start the LoginRocket Web flow.

    Sets a signed cookie carrying the timestamp (and CLI loopback port if
    `cli=1`), then 302s the browser to AuthRocket's hosted login UI with
    `prompt=login` appended so AuthRocket forces interactive re-auth.

    The cookie is the freshness anchor for /auth/handoff — qiita doesn't
    rely on the JWT's `iat`/`auth_time` because LoginRocket Web re-emits
    the same JWT across cached sessions. The cookie is HttpOnly, Secure,
    SameSite=Lax. SameSite=Strict would break the AuthRocket-driven
    cross-origin redirect back to /auth/handoff.
    """
    settings = get_settings(request)
    if not settings.authrocket_loginrocket_url:
        raise HTTPException(
            status_code=503,
            detail="AUTHROCKET_LOGINROCKET_URL not configured",
        )
    if not settings.qiita_endpoint_url:
        raise HTTPException(
            status_code=503,
            detail="QIITA_ENDPOINT_URL not configured",
        )

    if cli == 1 and (port is None or not (1 <= port <= 65535)):
        raise HTTPException(
            status_code=400,
            detail="cli=1 requires port=<1-65535>",
        )

    cookie_payload: dict = {
        "timestamp_ms": int(time.time() * 1000),
        "cli": cli == 1,
    }
    if cli == 1:
        cookie_payload["port"] = port

    cookie_value = sign_login_cookie(cookie_payload, settings.login_cookie_secret_key)

    redirect_uri = f"{settings.qiita_endpoint_url.rstrip('/')}{URL_AUTH_HANDOFF}"
    authrocket_url = build_authrocket_login_url(
        loginrocket_base_url=settings.authrocket_loginrocket_url,
        redirect_uri=redirect_uri,
    )

    response = RedirectResponse(authrocket_url, status_code=302)
    response.set_cookie(
        key=LOGIN_COOKIE_NAME,
        value=cookie_value,
        max_age=LOGIN_COOKIE_MAX_AGE_SECONDS,
        **_LOGIN_COOKIE_TRANSPORT,
    )
    return response


# ---------------------------------------------------------------------------
# GET /auth/handoff — receive token from AuthRocket and mint a PAT
# ---------------------------------------------------------------------------


_HANDOFF_BROWSER_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>qiita login</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       max-width: 720px; margin: 2em auto; padding: 0 1em; color: #1a1a1a; }}
h1   {{ margin-bottom: 0.4em; }}
pre  {{ background: #f3f4f6; padding: 1em; word-break: break-all; user-select: all;
       border-left: 3px solid #5a6f80; font-size: 0.95em; }}
code {{ background: #f3f4f6; padding: 0.1em 0.3em; border-radius: 3px; }}
.muted {{ color: #555; font-size: 0.95em; }}
</style>
</head>
<body>
<h1>Logged in</h1>
<p>Logged in as <strong>{email}</strong>.</p>
<p>Save this Personal Access Token to <code>~/.qiita/token</code> (mode <code>0600</code>):</p>
<pre>{plaintext}</pre>
<p class="muted">This token is shown <strong>once</strong>; it cannot be recovered if lost.
To rotate, run <code>qiita-admin login</code> again or visit
<code>{login_path}</code>.</p>
</body>
</html>
"""


# PAT labels surfaced via `api_token.label` and rendered in
# `qiita-admin token list` — pin the spelling so a typo in a future
# refactor doesn't silently change what users see in their token table.
# Audit `via` discriminators are kept side-by-side so the two stay in
# lockstep (they describe the same two minting flows, from two angles).
# The invitation flow mints nothing (it redirects to the anchored login), so
# it has no label/via of its own.
_LABEL_CLI = "qiita-admin login"
_LABEL_BROWSER = "browser login"
_VIA_CLI = "cli_login"
_VIA_BROWSER = "browser_login"


@router.get(PATH_AUTH_HANDOFF)
async def handoff(
    request: Request,
    token: str | None = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> Response:
    """Receive `?token=<JWT>` from AuthRocket and mint a PAT for the user.

    Verifies the JWT through the configured AuthRocketVerifier, runs the
    standard OIDC resolver upsert (first-login, email drift, race
    handling), then mints a PAT scoped to the user's role ceiling.

    The post-resolver writes — PAT mint, TOKEN_MINT audit, and (CLI flow)
    the cli_login_code row — share one transaction so a failure in any
    later write rolls back the earlier ones. The resolver upsert runs on
    the bare pool *before* that transaction opens: it owns its own
    first-login transaction and a deliberately pool-scoped email-collision
    audit that must survive even when this route's transaction rolls back.

    Three flows, discriminated by the presence and contents of the login
    cookie set by `/auth/login`:

    - **CLI flow** (cookie present, `cli=true`): store PAT under a
      one-time code in `qiita.cli_login_code`, redirect browser to
      `http://127.0.0.1:<port>/?ot_code=<plaintext>` with the cookie
      scrubbed. The CLI's loopback HTTP server captures the code and POSTs
      it back to /auth/cli-exchange.
    - **Browser-login flow** (cookie present, no `cli`): render an HTML
      page displaying the PAT plaintext for the user to copy into
      `~/.qiita/token`.
    - **Invitation flow** (cookie absent): AuthRocket's invitation-acceptance
      redirect sends the user here directly without ever traversing
      `/auth/login`, so there is no cookie and thus no freshness anchor.
      Rather than mint a full-ceiling, 90-day PAT from that un-anchored (and
      therefore replayable) JWT, this flow provisions the user via the OIDC
      resolver and then **redirects to `/auth/login`** so the PAT is minted
      only through the cookie-anchored path. No CLI dispatch possible —
      invitations are browser-only.

    **Only the two cookie-anchored flows mint here.** CLI and browser-login
    bound the AuthRocket round-trip with the signed cookie's
    `auth_handoff_freshness_seconds` timestamp; the invitation flow mints
    nothing (it bounces to `/auth/login`), so a replayed invitation URL
    yields only a redirect. The realm emits no `auth_time`, so the JWT
    itself carries no freshness — the cookie is the sole anchor. `POST
    /auth/pat` elsewhere in this file still enforces `auth_time` freshness,
    but that path is legacy on this realm (no `auth_time` is emitted).
    """
    settings = get_settings(request)
    verifier = get_oidc_verifier(request)

    cookie = request.cookies.get(LOGIN_COOKIE_NAME)
    cookie_payload: dict | None = None
    if cookie:
        try:
            cookie_payload = verify_login_cookie(
                cookie,
                settings.login_cookie_secret_key,
                max_age_seconds=settings.auth_handoff_freshness_seconds,
            )
        except CookieInvalid as exc:
            # Static-text 401 to avoid leaking which check failed. A
            # *present-but-invalid* cookie is rejected rather than treated
            # as absent — the difference matters because an attacker
            # cannot forge "no cookie" but can plant a malformed one.
            raise HTTPException(
                status_code=401,
                detail="login session invalid or expired",
            ) from exc

    if not token:
        raise HTTPException(status_code=400, detail="missing token query parameter")

    try:
        principal = await resolve_oidc(pool, verifier, token)
    except InvalidJwt as exc:
        raise HTTPException(status_code=401, detail="invalid jwt") from exc

    if not isinstance(principal, HumanUser):
        # resolve_oidc only returns HumanUser today; defensive guard for
        # any future widening (e.g., service-OIDC) that would need its own
        # PAT-mint policy rather than reusing the human ceiling.
        raise HTTPException(status_code=500, detail="OIDC resolver returned non-human principal")

    is_cli = bool(cookie_payload and cookie_payload.get("cli"))
    is_invitation = cookie_payload is None

    if is_invitation:
        # An invitation redirect reaches /auth/handoff with no login cookie, so
        # it has NO freshness anchor — and the realm emits no auth_time, leaving
        # only the JWT's short exp. Rather than mint a full-ceiling, 90-day PAT
        # from that un-anchored (replayable) JWT, send the now-provisioned user
        # (resolve_oidc upserted them above) through the normal cookie-anchored
        # login. A replayed invitation URL then yields only this redirect, which
        # is useless without the user's actual AuthRocket credentials.
        return RedirectResponse(URL_AUTH_LOGIN, status_code=302)

    # Mint the PAT against the user's role ceiling. The scope set comes from
    # role_ceiling — same as POST /auth/pat with scopes=None — so the auto-
    # minted PAT mirrors what an interactive PAT mint would produce.
    scopes = sorted(role_ceiling(principal.system_role))
    expires_at = datetime.now(UTC) + timedelta(days=settings.token_default_ttl_days)
    # Label + audit `via` distinguish the two anchored entry points (CLI vs
    # browser login) so audit-log readers and end users (via `qiita-admin token
    # list`) can tell at a glance which flow produced a given PAT.
    if is_cli:
        label, via = _LABEL_CLI, _VIA_CLI
    else:
        label, via = _LABEL_BROWSER, _VIA_BROWSER

    # CLI flow: generate the one-time code up front so its cli_login_code
    # INSERT can join the mint + audit transaction below.
    ot_plaintext: str | None = None
    if is_cli:
        ot_plaintext, ot_hash = generate_ot_code()
        ot_expires = datetime.now(UTC) + timedelta(seconds=settings.cli_login_code_ttl_seconds)

    # Mint + audit + (CLI) cli_login_code INSERT share one transaction so a
    # failure in any later write rolls back the earlier ones — never a token
    # without its audit row, never token + audit without a redeemable
    # cli_login_code row. resolve_oidc above ran on the bare pool, outside
    # this transaction, on purpose (see the docstring).
    #
    # Inline pool.acquire() rather than the TxConnFactory dep: the cookie /
    # JWT / resolver work ahead of this block is non-transactional, so the
    # body-wide-tx shape that dep wraps is a poor fit — same call shape as
    # POST /auth/pat.
    async with pool.acquire() as conn:
        async with conn.transaction():
            plaintext_pat, token_idx = await mint_api_token(
                conn,
                principal_idx=principal.principal_idx,
                label=label,
                scopes=scopes,
                expires_at=expires_at,
            )
            await record_event(
                conn,
                event_type=AuthEventType.TOKEN_MINT,
                principal_idx=principal.principal_idx,
                actor_principal_idx=principal.principal_idx,
                detail={
                    "token_idx": token_idx,
                    "kind": "pat",
                    "via": via,
                },
            )
            if is_cli:
                # Persist the PAT plaintext under the single-use ot_code so
                # the CLI loopback can redeem it.
                await conn.execute(
                    "INSERT INTO qiita.cli_login_code"
                    "  (ot_code, principal_idx, token_idx, plaintext_pat, expires_at)"
                    " VALUES ($1, $2, $3, $4, $5)",
                    ot_hash,
                    principal.principal_idx,
                    token_idx,
                    plaintext_pat,
                    ot_expires,
                )

    if is_cli:
        # Redirect the browser to the CLI's loopback so the CLI can redeem
        # the ot_code. The cookie is scrubbed (max-age=0) so a network
        # observer who replays the URL can't repeat the flow.
        port = cookie_payload["port"]
        loopback_url = f"http://{LOOPBACK_HOST}:{port}/?ot_code={ot_plaintext}"
        response = RedirectResponse(loopback_url, status_code=302)
        _scrub_login_cookie(response)
        return response

    # Browser flow — render the PAT plaintext once, scrub the cookie. Email
    # and PAT both come from trusted sources (verifier-extracted email,
    # server-minted PAT) but escape anyway so a future change that pipes
    # user-controlled data through this template can't introduce XSS.
    page = _HANDOFF_BROWSER_HTML.format(
        email=html.escape(principal.email, quote=True),
        plaintext=html.escape(plaintext_pat, quote=True),
        login_path=URL_AUTH_LOGIN,
    )
    response = HTMLResponse(content=page, status_code=200)
    _scrub_login_cookie(response)
    return response


def _scrub_login_cookie(response: Response) -> None:
    """Delete the login cookie by re-setting it with max_age=0.

    Single-use cookie semantics: once /auth/handoff consumes it, a replay
    of the redirect URL must not re-trigger the flow.
    """
    response.set_cookie(
        key=LOGIN_COOKIE_NAME,
        value="",
        max_age=0,
        **_LOGIN_COOKIE_TRANSPORT,
    )


# ---------------------------------------------------------------------------
# POST /auth/cli-exchange — CLI redeems the one-time code
# ---------------------------------------------------------------------------


@router.post(PATH_AUTH_CLI_EXCHANGE, response_model=ApiTokenMintResponse)
async def cli_exchange(
    body: CliLoginExchangeRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> ApiTokenMintResponse:
    """Redeem a one-time code minted by /auth/handoff for the PAT plaintext.

    Atomically flips `consumed_at` so a replay sees no row. Conflates
    "no such code" / "expired" / "already consumed" into a single 404 to
    avoid leaking which condition tripped — an attacker walking ot_code
    values shouldn't be able to distinguish "wrong code" from "right code,
    already used."
    """
    ot_hash = hash_ot_code(body.ot_code)

    # JOIN api_tokens by the pinned token_idx so a parallel mint between
    # handoff and exchange can't pair our plaintext with someone else's
    # metadata. The consume UPDATE's `consumed_at IS NULL` guard makes redemption
    # single-use (a replay sees no row); a second UPDATE in the SAME transaction
    # then scrubs the plaintext so the row never retains a usable PAT past the
    # instant it's handed to the CLI. Both run in one transaction, so either the
    # caller gets the plaintext AND the row is scrubbed, or neither happens.
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "WITH consumed AS ("
                "  UPDATE qiita.cli_login_code"
                "     SET consumed_at = now()"
                "   WHERE ot_code = $1"
                "     AND consumed_at IS NULL"
                "     AND expires_at > now()"
                "   RETURNING token_idx, plaintext_pat"
                ")"
                " SELECT c.plaintext_pat, t.token_idx, t.label, t.scopes,"
                "        t.expires_at, t.created_at"
                "   FROM consumed c"
                "   JOIN qiita.api_token t ON t.token_idx = c.token_idx",
                ot_hash,
            )
            if row is not None:
                await conn.execute(
                    "UPDATE qiita.cli_login_code SET plaintext_pat = NULL WHERE ot_code = $1",
                    ot_hash,
                )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="ot_code not found, already used, or expired",
        )

    return ApiTokenMintResponse(
        token=row["plaintext_pat"],
        token_idx=row["token_idx"],
        label=row["label"],
        scopes=list(row["scopes"]),
        expires_at=row["expires_at"],
        created_at=row["created_at"],
    )
