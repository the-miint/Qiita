# Authentication

> **Status:** ready to merge from `feat/auth`. Phases A–J landed. The control plane is fully on the real auth surface; the orchestrator authenticates via a service-account `qk_` token loaded from `/etc/qiita/orchestrator.token` (or `CONTROL_PLANE_API_TOKEN` when `QIITA_ALLOW_TOKEN_ENV=true` for dev/CI). 178 unit + 204 integration tests pass; security audit completed.

> **Known gap:** the OIDC PKCE code-exchange (`POST /auth/login` + `qiita-admin login`) is **not** shipped — it requires a code-exchange test harness that we deferred. Today's path: operators obtain an AuthRocket JWT out-of-band (AuthRocket admin UI / external CLI) within `AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS` of login, then call `POST /api/v1/auth/pat` directly to mint a PAT. The `qiita-admin login` subcommand currently exits with status 2 and a message pointing at this path. See `qiita-control-plane/src/qiita_control_plane/cli/admin.py::main` and the `routes/auth.py` module docstring.

Qiita authenticates three kinds of principal against the control plane:

- **Human users** — authenticate via AuthRocket OIDC (RS256 JWT, JWKS-verified).
- **Service accounts** — workers and cron jobs, each with their own long-lived opaque bearer token prefixed `qk_`.
- **Anonymous** — no credentials; accepted only on explicitly public endpoints.

The data plane does not perform user authentication. It verifies HMAC-signed Arrow Flight tickets issued by the control plane.

## Schema

Auth extends the existing `qiita.principal` table (introduced by `20260423000000_principals.sql`) rather than creating a parallel users table. The driving invariant is **"every user is a principal, but not every principal is a user."**

### Subtypes of `principal`

`qiita.user` and `qiita.service_account` are 0..1 subtypes of `principal`, sharing its identifier:

```sql
qiita.user (
    principal_idx  BIGINT PRIMARY KEY REFERENCES qiita.principal(idx),
    email          CITEXT NOT NULL UNIQUE,
    affiliation, address, phone, receive_processing_emails, orcid,
    profile_complete BOOLEAN GENERATED ALWAYS AS (...) STORED,
    created_at, updated_at
)

qiita.service_account (
    principal_idx  BIGINT PRIMARY KEY REFERENCES qiita.principal(idx),
    name           TEXT NOT NULL UNIQUE,
    description, created_at
)
```

The PRIMARY KEY = FK to `principal(idx)` enforces both invariants for free: every subtype row points at a real principal, and at most one subtype row exists per principal. A bare principal with no subtype row is legal and represents an actor that cannot authenticate (e.g., the system principal at `idx=1`, or a PI imported from an external system).

A principal is **at most one** of `{user, service_account}`. A BEFORE INSERT trigger (`tg_principal_subtype_exclusion`) raises if either subtype is inserted for a `principal_idx` that already has the other. The trigger calls `pg_advisory_xact_lock(NEW.principal_idx)` first to serialize concurrent INSERTs across both subtype tables — without it, two parallel transactions inserting opposing subtypes for the same `principal_idx` could each pass their EXISTS check and both succeed.

Both subtypes contain `CHECK (principal_idx <> 1)` to keep the system principal bare.

### System principal (sentinel)

`idx=1` is seeded by the auth migration with `display_name='system'`, `system_role='system_admin'`, `created_by_idx=1` (self-reference via the deferred FK). It cannot have a `user` or `service_account` row, cannot hold tokens, cannot be `disabled` or `retired` (`principal_system_principal_always_active` CHECK), and cannot authenticate. It exists to:

- Backfill pre-auth historical FKs in Phase H (when `references.created_by` migrates from UUID to `principal(idx)`).
- Serve as the audit-log "actor" for system-generated events (e.g., automatic token revocation on retirement).

### Status: `disabled` / `retired`

`principal.retired` (BOOLEAN, terminal) was introduced by `20260423000000_principals.sql`. The auth migration adds `principal.disabled` (BOOLEAN, reversible) plus `disabled_at`, `disabled_by_idx`, `disable_reason` audit columns. Two CHECK constraints govern them:

- `principal_disabled_consistent` — `disabled=true` requires the audit columns; `disabled=false` requires them all NULL.
- `principal_not_both_disabled_and_retired` — they are mutually exclusive.

Auth-layer behavior: login and token-use are rejected when **either** flag is true. Retiring a principal triggers automatic revocation of all their active `api_tokens` (`tg_revoke_tokens_on_retire`). Disabling does **not** revoke tokens — admins can bulk-revoke separately if needed.

### `api_tokens`

Single FK to `principal(idx)` — there's no separate user/service token table. The principal's subtype determines the token kind. `token_hash BYTEA UNIQUE` stores SHA-256 of the plaintext; the partial index `api_tokens_hash_active` (where `revoked_at IS NULL`) keeps the active-token lookup hot. `scopes TEXT[]` carries per-token capability.

### `auth_events`

Append-only audit log. BEFORE UPDATE and BEFORE DELETE triggers raise on any mutation attempt — the only legal write is INSERT. `event_type` is a discriminator string (no ENUM, so values can be added per phase without a migration). `principal_idx` and `actor_principal_idx` (admin-on-behalf-of) both FK to `principal(idx)`; `detail JSONB` carries event-specific context.

### Reuse of `qiita.set_updated_at()`

`qiita.user.updated_at` is maintained by the shared `qiita.set_updated_at()` trigger function defined in `20260423000001_studies.sql`, attached as `user_set_updated_at`.

## Principal model

Every authenticated request resolves to a typed `Principal`:

```python
class Principal:           # base; default capability methods return False
    def has_role(role) -> bool
    def has_role_at_least(role) -> bool
    def has_scope(scope) -> bool

@dataclass(frozen=True, slots=True)
class HumanUser(Principal):       # has a row in qiita.user
    principal_idx, email, system_role, scopes, profile_complete, disabled, retired

@dataclass(frozen=True, slots=True)
class ServiceAccount(Principal):  # has a row in qiita.service_account
    principal_idx, name, scopes, disabled, retired

@dataclass(frozen=True, slots=True)
class Anonymous(Principal):       # no Authorization header
```

- `has_role(role)` is exact match; `has_role_at_least(role)` walks the `system_role` ordering (`user < wet_lab_admin < system_admin`). `ServiceAccount` always returns `False` for both — services don't fit the human hierarchy.
- `has_scope(scope)` is membership in the principal's `scopes` frozenset (token scopes for token-resolved principals, role-implied ceiling for OIDC-resolved principals).

### Resolver dispatch (`auth.principal.get_current_principal`)

| Bearer shape | Path | Behavior |
|---|---|---|
| absent / non-Bearer | — | returns `Anonymous()` |
| `Bearer ` (empty) | — | returns `Anonymous()` |
| `Bearer qk_...` | token | `verify_api_token` → load principal subtype → `HumanUser` or `ServiceAccount` |
| `Bearer eyJ...x.y.z` | OIDC | `JwtVerifier.verify` → upsert / load → `HumanUser` |
| anything else | malformed | 401 |

The resolver returns `503` if a JWT-shaped bearer arrives but the OIDC verifier is not configured (e.g., `AUTHROCKET_*` env vars missing in dev). This is the fail-fast point referenced in the OIDC section above.

### First-login (OIDC), races, and email drift

On a JWT for a previously-unseen `(iss, sub)`, the resolver creates a `principal` row, a `qiita.user` row, and a `qiita.user_identities(iss, sub) → principal_idx` row in one transaction, then writes an `oidc_create_principal` audit event. Two race outcomes are handled:

- **Concurrent first-login for the same `(iss, sub)`** — depending on insert order, either the `user_identities_pkey` or the `user_email_key` UNIQUE constraint trips first under timing. The handler doesn't dispatch on constraint name (fragile); on **any** unique violation in the create path, it re-reads `user_identities` for our `(iss, sub)`. If a row exists, the winner created our identity and we return their `principal_idx`. If no row exists, this is a true email collision (different identity claiming an already-used email) — the handler emits an `oidc_create_principal_email_conflict` audit event with `attempted_email_sha256` (cleartext NOT logged) and returns `409`.

- **Email drift** on a repeat OIDC login (same `(iss, sub)`, JWT email differs from stored): try `UPDATE qiita.user.email`. If it succeeds, emit `email_drift` event with `outcome="updated"` and the from/to cleartext (the principal's own email — they can read it). If it collides with another user's email, no-op and emit `email_drift` with `outcome="collision"` and `attempted_email_sha256` (cleartext NOT logged so cross-user audit reads can't trivially harvest the colliding email).

### Disabled / retired

The resolver refuses to construct a `HumanUser` / `ServiceAccount` for a principal where `disabled=true` OR `retired=true` — both paths (token verify and OIDC upsert) check this. `verify_api_token` already filters tokens by these flags at the SQL level; the OIDC path checks at upsert / re-read time. There is no bypass.

## API tokens

Opaque bearer tokens used by both human PATs and worker service-account tokens. Format:

```
qk_<43 url-safe base64 chars without padding>
```

- Prefix `qk_` ("qiita key") makes leak scanners and grep useful.
- 43-char body is `secrets.token_urlsafe(32)` — 32 random bytes, 256 bits of entropy.
- Total length 46 chars.
- The DB stores `SHA-256(plaintext)` in `qiita.api_tokens.token_hash` (`BYTEA UNIQUE`). Plaintext is shown exactly once at mint time and never logged.

### Mint (`auth.tokens.mint_api_token`)

```python
plaintext, token_idx = await mint_api_token(
    pool,
    principal_idx=...,
    label="my-laptop",
    scopes=["self:profile", "self:tokens", "references:read"],
    expires_at=None,  # or a tz-aware datetime
)
```

Validates every requested scope against `auth.scopes.VALID_SCOPES` (raises `ValueError` on unknown). Surfaces a token-hash collision as `RuntimeError` rather than silently shadowing — collision is astronomically unlikely with 256 bits of entropy, but the principle is "fail loudly."

### Verify (`auth.tokens.verify_api_token`)

```python
verified = await verify_api_token(pool, plaintext)  # → VerifiedToken | None
```

Returns `None` for any rejection condition: malformed prefix or length, no matching active row (`revoked_at IS NULL`), token expired, or owning principal `disabled`/`retired`. Side effect on success: schedules a fire-and-forget `record_token_use` via `asyncio.create_task` to advance `last_used_at`. Verify never blocks on the `last_used_at` write — the helper catches `asyncpg.PostgresError` and logs a warning.

### Last-used coalescing (`auth.tokens.record_token_use`)

```sql
UPDATE qiita.api_tokens SET last_used_at = now()
 WHERE token_idx = $1
   AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute');
```

Coalesces to ≤1 write per token per minute via the predicate. Pure observability — `last_used_at` is never used for auth decisions, so failures here are absorbed.

## OIDC verification

Human users authenticate via AuthRocket OIDC. JWTs are RS256-signed; the verifier fetches the public key from `{issuer}/connect/jwks` via PyJWT's `PyJWKClient`, which caches in-memory and refreshes automatically when it encounters an unknown `kid` (so AuthRocket key rotation never needs a redeploy).

The verifier is split into two layers:

- **`auth.oidc.JwtVerifier`** — pure. Takes `jwks_url`, `issuer`, `audience`, `leeway_seconds`. Tests exercise this directly against a local JWKS harness.
- **`auth.oidc.AuthRocketVerifier`** — config-bound subclass. `from_settings(settings)` raises `RuntimeError` if any of `AUTHROCKET_ISSUER` / `AUTHROCKET_AUDIENCE` / `AUTHROCKET_JWKS_URL` is missing — fail-fast at FastAPI lifespan, no silent run-with-auth-disabled.

### What `verify(token)` checks

| Check | Mechanism |
|---|---|
| Signature | PyJWT against the JWKS-fetched key matching the JWT's `kid`, algorithm pinned to RS256 (HS256 / `none` rejected) |
| `exp` | within `leeway_seconds` (default 30s) |
| `iss` | exact match to configured issuer |
| `aud` | configured audience present (string or list — both accepted) |
| `email_verified` | strict boolean `True` (the string `"true"` is rejected — coerced strings indicate an IdP we don't trust) |
| `email`, `sub` | present and non-empty strings |
| `auth_time` | optional; returned in `OIDCIdentity` if present (callers like `POST /auth/pat` enforce freshness) |

On success, returns `OIDCIdentity(issuer, subject, email, auth_time, raw_claims)`. On any failure, raises `auth.oidc.InvalidJwt` with a static error message — token contents and claim values are never embedded in exception messages.

### Configuration

`Settings.from_env()` reads:

- `AUTHROCKET_ISSUER` (required for verifier construction)
- `AUTHROCKET_AUDIENCE` (required)
- `AUTHROCKET_JWKS_URL` (defaults to `{issuer}/connect/jwks` if unset)
- `AUTHROCKET_JWT_LEEWAY_SECONDS` (default 30)
- `AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS` (default 300; consumed by Phase F's PAT-mint freshness check, not by the verifier itself)
- `QIITA_TOKEN_DEFAULT_TTL_DAYS` (default 90; consumed by PAT mint)

These fields are *optional* on `Settings` so non-auth tests don't have to set every `AUTHROCKET_*` env var. Required-ness is enforced at `AuthRocketVerifier.from_settings` time.

## Scopes and roles

Defined in `auth.scopes`. Two ceilings:

- **`ROLE_IMPLIED_SCOPES`** — hierarchical, per `system_role`. Each entry is the **full** ceiling, not the increment, with `system_admin ⊇ wet_lab_admin ⊇ user`. Future readers don't have to chase inheritance to know what role X can do.
- **`SERVICE_ACCOUNT_SCOPE_CEILING`** — flat, non-inherited. Workers don't fit the human hierarchy; admin-mint of a service-account token must spell out scopes explicitly within this set.

The hierarchical claim is enforced by a Phase E unit test (`test_role_ceilings_are_hierarchical`) that asserts `user ⊊ wet_lab_admin ⊊ system_admin` strictly. If that test ever fails, the inheritance contract is broken and downstream guards become unsound.

### Guards (`auth.guards`)

| Guard | Behavior |
|---|---|
| `require_human(p)` | 401 on Anonymous, 403 on ServiceAccount; returns `HumanUser` |
| `require_service(p)` | 401 on Anonymous, 403 on HumanUser; returns `ServiceAccount` |
| `require_role_at_least(role)` | factory; 401 on Anonymous, 403 on insufficient role (always 403 for ServiceAccount because they don't fit the hierarchy) |
| `require_scope(scope)` | factory; 401 on Anonymous, 403 if `scope` not in the principal's scope set. Works for both kinds. |
| `require_complete_profile` | depends on `require_human`; 422 with `{reason: "profile_incomplete", missing_fields: [...]}` if `profile_complete=False` |

Guards compose: a route can `Depends(require_role_at_least("system_admin"))` AND `Depends(require_scope("admin:users"))` AND `Depends(require_human)` simultaneously. FastAPI dedupes the underlying `get_current_principal` call across deps in one request.

### Token-vs-OIDC scope source

The token path returns the token's **own** `scopes` frozenset (whatever the mint stored). The OIDC path (no token; bearer is a fresh JWT) hands back the **role's full ceiling** — Phase F's `POST /auth/pat` is the route that narrows this when it mints a PAT. Per-request bearers always carry their own scope set via the token path.

## Endpoints

### Auth (Phase F — real auth)

| Route | Method | Notes |
|---|---|---|
| `/api/v1/auth/whoami` | GET | Public. Returns `{kind: anonymous}` for unauthenticated callers; otherwise the resolved principal's profile / scopes. |
| `/api/v1/auth/pat` | POST | **Requires a fresh OIDC JWT** — the `auth_time` claim must be present, ≥ now − `AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS`, and ≤ now + `AUTHROCKET_JWT_LEEWAY_SECONDS` (the upper bound rejects forward IdP clock skew, the lower bound enforces interactive freshness). PAT-via-PAT (`Bearer qk_...`) is rejected. Body: `{label, scopes?, ttl_days?}`. Returns plaintext + metadata exactly once. |
| `/api/v1/auth/tokens` | GET | Lists the caller's own tokens. Metadata only — no plaintext, no hash. Requires `self:tokens` scope. |
| `/api/v1/auth/tokens/{idx}` | DELETE | Revokes the caller's token. Requires `self:tokens`. Returns 404 for both "no such token" and "exists but owned by someone else" so probing `token_idx` values cannot enumerate the table. |

**PAT mint validation:**

- `scopes=None` defaults to the caller's full role ceiling.
- Explicit `scopes` must be a subset of the role ceiling. Out-of-ceiling scopes return 422 with body `{detail: "scopes not granted by your role", rejected_scopes: [...]}`. The body does **not** echo the ceiling — that would leak structure to a probing attacker.
- `ttl_days` defaults to `QIITA_TOKEN_DEFAULT_TTL_DAYS` (90). Pydantic enforces `0 < ttl_days <= 365`.
- Profile must be complete. Otherwise: 422 with flat body `{detail: "profile incomplete", reason: "profile_incomplete", missing_fields: [...]}`.
- Mint emits a `token_mint` audit event with `token_idx`, `scopes`, `kind` — never plaintext.

### Admin (Phase G — system_admin only)

All routes require `system_role >= system_admin` AND the appropriate `admin:*` scope, so a system_admin token minted with narrow scopes can't exfiltrate data outside its grant.

| Route | Method | Notes |
|---|---|---|
| `/api/v1/admin/service-accounts` | POST | Creates a service-account-kind principal and mints its initial token. Scopes are required (no implicit ceiling — workers don't fit the human hierarchy) and validated against `SERVICE_ACCOUNT_SCOPE_CEILING`. 409 on duplicate `name`. Requires `admin:service_accounts`. |
| `/api/v1/admin/principals/{idx}/disabled` | PATCH | Toggle disabled. `disabled=true` requires `reason`; `false` is the round-trip back to active. Cannot transition retired→disabled (DB CHECK). Requires `admin:users`. |
| `/api/v1/admin/principals/{idx}/retired` | PATCH | Retire (terminal). DB trigger auto-revokes all the principal's active tokens. Refuses if the actor is the target (no zero-active-admins). Requires `admin:users`. |
| `/api/v1/admin/principals/{idx}/system-role` | PATCH | Set `system_role`. Audit event records `from`/`to`/`reason`. Requires `admin:users`. |
| `/api/v1/admin/audit` | GET | List audit events (newest first). Optional filters `principal_idx` and `event_type`; `limit ∈ [1, 1000]`. Requires `admin:audit_read`. |
| `/api/v1/admin/principals/{idx}/revoke-all-tokens` | POST | Bulk-revoke all the principal's active tokens. Idempotent on already-revoked tokens (counted separately). Emits one `token_revoke` audit event per newly-revoked token. Requires `admin:users`. |

The system principal (`idx=1`) is rejected by every mutation endpoint above (`disabled`, `retired`, `system-role`) — bare-actor invariant holds at the API layer in addition to the DB CHECK.

### User self-service

| Route | Method | Notes |
|---|---|---|
| `/api/v1/users` | POST | Admin-only (`require_human_with_role("system_admin") + require_scope("admin:users")`). Creates a new principal + user row in one transaction; the new principal's `created_by_idx` points at the requesting admin. Returns `409` on email collision (case-insensitive via CITEXT). In production, OIDC first-login is the typical user-creation path; this route exists for admins to onboard PIs imported from external systems. |
| `/api/v1/users/me` | GET | Returns the authenticated user's profile. `require_human` (rejects service-kind 403). |
| `/api/v1/users/me` | PATCH | Updates profile fields (`affiliation`, `address`, `phone`, `orcid`, `receive_processing_emails`). Requires `self:profile`. `email` and status fields are absent from `UserUpdate` and are silently dropped — email-change requires re-verification via OIDC, status is admin-only. |

## CLI (`qiita-admin`)

Installed as the `qiita-admin` console script via `qiita-control-plane`'s pyproject. Subcommands:

| Subcommand | Path | Notes |
|---|---|---|
| `set-system-role --email X --role Y` | direct DB | Bootstrap path — sets `qiita.principal.system_role` by email lookup against `qiita.user`. Refuses to operate on `idx=1`. The user must have logged in via OIDC at least once (which is what creates their `principal+user` rows). |
| `whoami` | HTTP | Calls `GET /api/v1/auth/whoami`. PAT read from `QIITA_TOKEN` env or `~/.qiita/token` (mode 0600 expected). |
| `token revoke-all --principal-idx N` | HTTP | Calls `POST /api/v1/admin/principals/{N}/revoke-all-tokens`. |
| `login` | DEFERRED | The PKCE + OIDC code-exchange flow is **not** shipped (see the Status banner above). Exits 2 with a message pointing at the alternative: obtain a JWT out-of-band and call `POST /api/v1/auth/pat` directly. |

## Orchestrator integration

The orchestrator authenticates to the control plane via a service-account `qk_` token. `qiita_common.client.ControlPlaneClient` accepts:

- `api_token: str | None` — plaintext, used by tests and dev with `QIITA_ALLOW_TOKEN_ENV=true`.
- `api_token_path: Path | None` — production drop-in (default `/etc/qiita/orchestrator.token`, mode `0400`, owned by the `qiita` user).

Exactly one must be supplied; passing both or neither raises `ValueError`. The plaintext is loaded once at construction time, attached as `Authorization: Bearer <token>` to every request, and never appears in `__repr__` (redacted to `<redacted>`).

`qiita_common.log.AuthorizationScrubFilter` is a `logging.Filter` that rewrites any `Bearer <token>` substring in log messages and args to `Bearer <redacted>`. Install once at application startup so httpx's request logs (or any log line carrying request headers) can't leak the token to disk.

### Resolution order in orchestrator `Settings.from_env`

1. **`CONTROL_PLANE_API_TOKEN_PATH`** (default `/etc/qiita/orchestrator.token`). If the file exists, use it. This is the production path.
2. **`CONTROL_PLANE_API_TOKEN`** (env var). Only honoured when **`QIITA_ALLOW_TOKEN_ENV=true`** — dev / CI explicitly opt in. Production never sets the flag, so a leaked env var alone can't drive auth.

If neither source yields a token, `Settings.from_env()` raises `RuntimeError` with an actionable message pointing at both paths.

<!-- See "First-deploy bootstrap" subsection within Endpoints. -->
## First-deploy bootstrap

See [`docs/runbooks/first-deploy.md`](runbooks/first-deploy.md) for the full sequence: migrate → set env vars → one-shot JWT verify (`scripts/verify_jwt.py`) → start control plane → first OIDC login → `qiita-admin set-system-role` → mint operator PAT → mint orchestrator service account → install token at `/etc/qiita/orchestrator.token` (mode 0400) → start orchestrator.

## Token rotation

See [`docs/runbooks/orchestrator-token-rotation.md`](runbooks/orchestrator-token-rotation.md) for the full zero-downtime rotation procedure: mint replacement → install at `*.token.new` → atomic `mv` → `systemctl reload` → wait for `last_used_at` to advance → revoke old.
