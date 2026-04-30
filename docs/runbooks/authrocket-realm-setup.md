# AuthRocket realm setup

**Purpose.** Configure an AuthRocket realm so qiita can authenticate users
through it. This is the realm-side counterpart to the env-var configuration
in [`first-deploy.md`](first-deploy.md). One-time setup per realm.

For the conceptual reference (verifier, principal model, login flow), see
[`docs/auth.md`](../auth.md).

## Why LoginRocket Web

AuthRocket offers four integration methods on its Integration page:

- **LoginRocket Web** — hosted login UI; AuthRocket emits a JWT and
  redirects the browser to your `redirect_uri?token=<JWT>`. No OAuth2
  client setup. Available on the standard plan tier.
- **LoginRocket API** — frontend-embedded login form. Designed for SPAs.
  Qiita has no SPA in scope; skip.
- **AuthRocket API** — server-side admin API for managing the realm
  itself (creating users programmatically, querying sessions). Not a
  login flow. Useful as a complement to LoginRocket Web for admin
  automation, but not required for qiita today.
- **OAuth2 Server** — standard OIDC PKCE. Plan-gated on the qiita-dev
  realm; not available without an upgrade.

Qiita uses **LoginRocket Web**. It's AuthRocket-proprietary (not standard
OIDC), but the JWT it emits is RS256 and consumed by qiita's existing
`JwtVerifier`; only the redirect/handoff routes around the JWT are
AuthRocket-shaped. See [`docs/auth.md`](../auth.md#login-flow) for the
flow.

## What to configure

### 1. Realm settings

In AuthRocket admin → your realm → **Settings**:

- **JWT signing**: RS256. (Qiita rejects HS256 / `none` at the algorithm
  allowlist.)
- **JWT TTL**: ≥ 5 minutes. Short TTLs are fine — the JWT is consumed
  exactly once at `/auth/handoff`. Qiita then issues its own PAT for
  long-lived auth, so there's no benefit to a long-lived AuthRocket JWT.
- **JWKS endpoint** must be public on `https://<realm>.loginrocket.com/connect/jwks`.
  Qiita's `PyJWKClient` fetches from this URL and caches in-memory; new
  keys auto-rotate when AuthRocket rotates.
- **Email verification**: required. AuthRocket realms enforce this as
  policy at signup. Qiita's verifier no longer strict-checks
  `email_verified` at the JWT layer because LoginRocket Web tokens omit
  it; the realm policy is what makes that safe.

### 2. Default Login URL (optional)

Per AuthRocket's docs, "the Default Login URL may be overridden by using
the `?redirect_uri=[url]` query parameter." Qiita always passes
`redirect_uri` explicitly in `/auth/login`, so the Default Login URL is
unused by qiita's code path. You can leave it blank or set it to
`https://<qiita-host>/api/v1/auth/handoff` for direct login attempts that
bypass `/auth/login`.

### 3. Users

In **Users**:

- **Test user for CI smoke tests**: create one account with a known
  password stored in your CI secrets (e.g. GitHub Actions `secrets.AUTHROCKET_TEST_USER_EMAIL`,
  `secrets.AUTHROCKET_TEST_USER_PASSWORD`). Document its email in this
  runbook (or in a separate ops-only doc) so the next operator doesn't
  have to recreate it.
- **Email verification**: confirm the test user's email is verified.
  Otherwise the AuthRocket login UI will redirect to a "verify your
  email" page and the qiita flow will time out.
- **Self-signup**: enable on dev realms so contributors can self-onboard;
  disable on production realms so admin onboarding is the only path.

## The `iss=https://authrocket.com` footgun

The JWT's `iss` claim is **always** `https://authrocket.com` — the
canonical AuthRocket SaaS URL — not your realm's loginrocket subdomain.
Every other AuthRocket endpoint (JWKS, login UI, OAuth2 endpoints) lives
on the loginrocket subdomain, but the issuer is the SaaS URL.

Set `AUTHROCKET_ISSUER=https://authrocket.com` exactly. If you set it to
the loginrocket subdomain, every JWT will fail verification with
"issuer mismatch" — a confusing failure to debug.

## JWT claim shape

LoginRocket Web JWTs contain (from a real test against the qiita-dev
realm, claim names sorted):

| Claim | Type | Notes |
|---|---|---|
| `iss` | string | Always `https://authrocket.com` |
| `sub` | string | AuthRocket user id, e.g. `usr_0we1AJUK2Ha3ymOAVDoSc3` |
| `email` | string | User's email |
| `iat` | int | Issued-at, epoch seconds |
| `exp` | int | Expires-at; TTL = realm setting |
| `rid` | string | AuthRocket realm id |
| `sid` | string | AuthRocket session id |
| `name`, `given_name`, `family_name` | string | Profile fields |
| `preferred_username` | string | If the user has one |
| `locale` | string | e.g. `en` |
| `orgs` | array | AuthRocket org/permission model — qiita doesn't consume |

**Not present** on this realm: `aud`, `email_verified`, `auth_time`,
`nonce`. Qiita's verifier softens around these absences:

- `aud` — verifier's `audience` parameter is optional; leave
  `AUTHROCKET_AUDIENCE` unset.
- `email_verified` — strict check dropped; realm policy enforces.
- `auth_time` — surfaced on `OIDCIdentity` if present, else `None`. Not
  used as a freshness anchor — AuthRocket re-emits the same JWT across
  cached sessions, so `auth_time`/`iat` don't advance between PAT mints.
  Qiita anchors freshness in a server-side signed cookie set before the
  AuthRocket round-trip; see [`docs/auth.md`](../auth.md#login-flow).

## Smoke test

Verify the realm is wired correctly without any qiita code:

```bash
# Open in your browser:
echo "https://<realm>.loginrocket.com/login?redirect_uri=$(python3 -c "import urllib.parse; print(urllib.parse.quote('https://httpbin.org/get', safe=''))")"
```

Log in with your test user. After successful auth, AuthRocket redirects
to `https://httpbin.org/get?token=<JWT>` and httpbin echoes the request
back as JSON. Copy the `token` value.

Decode the JWT (no signature check — just inspect the payload):

```bash
python3 - <<'PY'
import json, base64
token = "PASTE_JWT_HERE"
payload = token.split(".")[1]
payload += "=" * (-len(payload) % 4)
print(json.dumps(json.loads(base64.urlsafe_b64decode(payload)), indent=2))
PY
```

Expected: `iss=https://authrocket.com`, `sub` populated, `email` matches
your test user. Anything else suggests the realm config drifted from this
doc — investigate before relying on it for first-deploy.

## When OAuth2 Server becomes available

If a future AuthRocket plan upgrade enables OAuth2 Server on this realm,
the migration to standard OIDC PKCE is small in concept but real in
practice:

- The verifier is already prepared: pass `AUTHROCKET_AUDIENCE` and the
  verifier will start enforcing `aud` again. `auth_time` will become
  available and (optionally) usable as a freshness anchor.
- Qiita would need new `/auth/login` and `/auth/callback` routes
  implementing the PKCE code-exchange flow; the existing `/auth/handoff`
  route would stay for LoginRocket Web compatibility.
- The transition can be staged: add the new flow under a new entry-point,
  let it bake, deprecate `/auth/handoff` once all clients are updated.

This is a deferred follow-up, tracked in
[`docs/auth.md`](../auth.md#login-flow).
