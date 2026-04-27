# Authentication

> **Status:** in development on `feat/auth`. The auth schema (Phase A), user CRUD routes against mock auth (Phase B), and the API-token mint/verify primitives (Phase C) have landed. Routes still use a mock principal-resolver (`get_current_principal_idx` in `deps.py`); the real OIDC/PAT-driven resolver lands in Phase E and the route flip happens in Phase H.b.

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

_Populated when Phase E lands._

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

_Populated when Phase D lands._

## Scopes and roles

_Populated when Phase E lands._

## Endpoints

### User CRUD (Phase B — mock auth)

| Route | Method | Notes |
|---|---|---|
| `/api/v1/users` | POST | Admin creates a new principal + user row in one transaction. The new principal's `created_by_idx` points at the requesting principal. Returns `409` on email collision (case-insensitive via CITEXT). |
| `/api/v1/users/me` | GET | Returns the authenticated principal's user profile. |
| `/api/v1/users/me` | PATCH | Updates profile fields (`affiliation`, `address`, `phone`, `orcid`, `receive_processing_emails`). `email` and status fields are intentionally absent from `UserUpdate` and are dropped silently if sent. |

Authentication is currently the **mock principal-resolver** in `deps.py::get_current_principal_idx`, which looks up a `principal` by `display_name='mock-admin'`. Integration tests seed this row via `mock_authenticated_principal` in `tests/integration/conftest.py`. The real OIDC/PAT-driven resolver replaces this in Phase E.

The auth-specific endpoints (`/auth/pat`, `/auth/whoami`, `/auth/tokens`, `/auth/login`) and admin endpoints (service accounts, audit log, principal status updates) are populated when Phases F and G land.

## CLI (`qiita-admin`)

_Populated when Phase G lands._

## Orchestrator integration

_Populated when Phase I lands._

## First-deploy bootstrap

_Populated when Phase G lands._

## Token rotation

_Populated when Phase I lands._
