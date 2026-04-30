"""Auth endpoints — /auth/whoami, /auth/pat, /auth/tokens, /auth/login,
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
from qiita_common.auth_constants import (
    API_PREFIX,
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
from ..auth.tokens import mint_api_token
from ..config import Settings
from ..deps import get_db_pool

router = APIRouter(prefix="/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_settings(request: Request) -> Settings:
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("Settings not initialised — lifespan may not have run")
    return settings


# ---------------------------------------------------------------------------
# GET /auth/whoami
# ---------------------------------------------------------------------------


@router.get("/whoami", response_model=WhoAmIResponse)
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


@router.post("/pat", status_code=201, response_model=ApiTokenMintResponse)
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
    """
    settings = _get_settings(request)
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

    # Resolve the user from the verified identity. The missing-fields list is
    # computed in SQL via qiita.user_profile_missing_fields so the field set
    # tracks the `profile_complete` generated column's definition (both live in
    # the auth migration; see 20260429000001_user_profile_missing_fields.sql).
    user_row = await pool.fetchrow(
        "SELECT u.principal_idx, p.system_role, p.disabled, p.retired,"
        " u.profile_complete,"
        " qiita.user_profile_missing_fields(u.affiliation, u.address, u.phone)"
        "   AS missing_fields"
        " FROM qiita.user_identities ui"
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

    plaintext, token_idx = await mint_api_token(
        pool,
        principal_idx=user_row["principal_idx"],
        label=body.label,
        scopes=scopes,
        expires_at=expires_at,
    )

    await record_event(
        pool,
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
# GET /auth/tokens — list own tokens (metadata only)
# ---------------------------------------------------------------------------


@router.get("/tokens")
async def list_own_tokens(
    p: Principal = Depends(require_scope(Scope.SELF_TOKENS)),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> list[ApiTokenSummary]:
    """List the caller's own tokens. Metadata only — no plaintext, no hash.
    Anonymous: 401. Both HumanUser and ServiceAccount can list their own."""
    rows = await pool.fetch(
        "SELECT token_idx, label, scopes, expires_at, revoked_at,"
        "  last_used_at, created_at"
        " FROM qiita.api_tokens WHERE principal_idx = $1"
        " ORDER BY token_idx",
        p.principal_idx,
    )
    return [ApiTokenSummary.model_validate(dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# DELETE /auth/tokens/{token_idx} — revoke own token
# ---------------------------------------------------------------------------


@router.delete("/tokens/{token_idx}", status_code=204)
async def revoke_own_token(
    token_idx: int,
    p: Principal = Depends(require_scope(Scope.SELF_TOKENS)),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> None:
    """Revoke a token belonging to the caller. 401 if Anonymous, 403 if the
    caller's bearer lacks `self:tokens`, 404 for **either** "no such token"
    or "exists but owned by someone else" — same response so probing
    token_idx values does not enumerate the table.
    """
    # Atomic UPDATE WHERE — only revokes if owner matches and token is not
    # already revoked. Returns 0 rows if either condition fails.
    result = await pool.execute(
        "UPDATE qiita.api_tokens SET revoked_at = now()"
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
        owner_idx = await pool.fetchval(
            "SELECT principal_idx FROM qiita.api_tokens WHERE token_idx = $1",
            token_idx,
        )
        if owner_idx is None or owner_idx != p.principal_idx:
            raise HTTPException(status_code=404, detail="token not found")
        # Owned by us but already revoked — idempotent success, no audit.
        return

    await record_event(
        pool,
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


@router.get("/login")
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
    settings = _get_settings(request)
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

    cookie_value = sign_login_cookie(cookie_payload, settings.hmac_secret_key)

    redirect_uri = f"{settings.qiita_endpoint_url.rstrip('/')}{API_PREFIX}/auth/handoff"
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


@router.get("/handoff")
async def handoff(
    request: Request,
    token: str | None = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> Response:
    """Receive `?token=<JWT>` from AuthRocket and mint a PAT for the user.

    Verifies the signed login cookie (set by /auth/login) is fresh,
    verifies the JWT through the configured AuthRocketVerifier, runs the
    standard OIDC resolver upsert (first-login, email drift, race
    handling), then mints a PAT scoped to the user's role ceiling.

    - **CLI flow** (`cli=true` in cookie): store PAT under a one-time code
      in `qiita.cli_login_codes`, redirect browser to
      `http://127.0.0.1:<port>/?ot_code=<plaintext>` with the cookie
      scrubbed. The CLI's loopback HTTP server captures the code and POSTs
      it back to /auth/cli-exchange.
    - **Browser flow** (no `cli`): render an HTML page displaying the PAT
      plaintext for the user to copy into `~/.qiita/token`.
    """
    settings = _get_settings(request)
    verifier = get_oidc_verifier(request)

    cookie = request.cookies.get(LOGIN_COOKIE_NAME)
    if not cookie:
        raise HTTPException(
            status_code=401,
            detail="login session missing — start at /auth/login",
        )
    try:
        cookie_payload = verify_login_cookie(
            cookie,
            settings.hmac_secret_key,
            max_age_seconds=settings.auth_handoff_freshness_seconds,
        )
    except CookieInvalid as exc:
        # Static-text 401 to avoid leaking which check failed.
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

    # Mint the PAT against the user's role ceiling. The scope set comes from
    # role_ceiling — same as POST /auth/pat with scopes=None — so the auto-
    # minted PAT mirrors what an interactive PAT mint would produce.
    is_cli = bool(cookie_payload.get("cli"))
    scopes = sorted(role_ceiling(principal.system_role))
    expires_at = datetime.now(UTC) + timedelta(days=settings.token_default_ttl_days)
    # Label names the CLI that minted the PAT. `qiita-admin` is the operator
    # CLI; a future end-user `qiita` CLI would mint with its own label.
    label = "qiita-admin login" if is_cli else "browser login"
    plaintext_pat, token_idx = await mint_api_token(
        pool,
        principal_idx=principal.principal_idx,
        label=label,
        scopes=scopes,
        expires_at=expires_at,
    )
    await record_event(
        pool,
        event_type=AuthEventType.TOKEN_MINT,
        principal_idx=principal.principal_idx,
        actor_principal_idx=principal.principal_idx,
        detail={
            "token_idx": token_idx,
            "kind": "pat",
            "via": "cli_login" if is_cli else "browser_login",
        },
    )

    if is_cli:
        # Persist the PAT plaintext under a single-use ot_code; redirect the
        # browser to the CLI's loopback so the CLI can redeem it. The cookie
        # is scrubbed (max-age=0) so a network observer who replays the URL
        # can't repeat the flow.
        ot_plaintext, ot_hash = generate_ot_code()
        ot_expires = datetime.now(UTC) + timedelta(seconds=settings.cli_login_code_ttl_seconds)
        await pool.execute(
            "INSERT INTO qiita.cli_login_codes"
            "  (ot_code, principal_idx, token_idx, plaintext_pat, expires_at)"
            " VALUES ($1, $2, $3, $4, $5)",
            ot_hash,
            principal.principal_idx,
            token_idx,
            plaintext_pat,
            ot_expires,
        )
        port = cookie_payload["port"]
        loopback_url = f"http://127.0.0.1:{port}/?ot_code={ot_plaintext}"
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
        login_path=f"{API_PREFIX}/auth/login",
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


@router.post("/cli-exchange", response_model=ApiTokenMintResponse)
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
    # metadata. The single statement also keeps consume + metadata-read
    # atomic against concurrent revoke/expire.
    row = await pool.fetchrow(
        "WITH consumed AS ("
        "  UPDATE qiita.cli_login_codes"
        "     SET consumed_at = now()"
        "   WHERE ot_code = $1"
        "     AND consumed_at IS NULL"
        "     AND expires_at > now()"
        "   RETURNING token_idx, plaintext_pat"
        ")"
        " SELECT c.plaintext_pat, t.token_idx, t.label, t.scopes,"
        "        t.expires_at, t.created_at"
        "   FROM consumed c"
        "   JOIN qiita.api_tokens t ON t.token_idx = c.token_idx",
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
