# First-deploy bootstrap

**Purpose.** Operator runbook for bringing up the auth surface on a fresh
deployment: from a clean database through migrations, env-var configuration,
the first OIDC login, operator promotion to `system_admin`, and minting the
orchestrator's service-account token. End state: control plane and
orchestrator both authenticated and ready to serve traffic. Each step is
independent — re-running it should be safe (idempotent) unless noted.

For the conceptual reference (principal model, scopes, endpoints), see
[`docs/auth.md`](../auth.md). For ongoing rotation of the token minted in
step 8, see [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md).

## 1. Apply migrations

```bash
make migrate
```

Seeds the system principal at `idx=1` and creates the auth tables. After
this, `qiita.principal` has exactly one row (the `system` principal); no
human or service-account principals exist yet.

## 2. Set required env vars

```bash
export DATABASE_URL=postgresql://qiita:...@host/qiita
export HMAC_SECRET_KEY=$(openssl rand -base64 32)
# AuthRocket SaaS issuer — the canonical URL, NOT the loginrocket subdomain.
export AUTHROCKET_ISSUER=https://authrocket.com
# Realm's loginrocket subdomain — both the JWKS endpoint and the LoginRocket
# Web hosted login URL live here.
export AUTHROCKET_LOGINROCKET_URL=https://merry-lion-7652.e2.loginrocket.com
export AUTHROCKET_JWKS_URL=$AUTHROCKET_LOGINROCKET_URL/connect/jwks
# Externally-resolvable URL of the control plane itself; used to construct
# the redirect_uri AuthRocket bounces back to.
export QIITA_ENDPOINT_URL=https://qiita.example.org
# Optional (defaults shown):
# export AUTHROCKET_AUDIENCE=          # leave unset — LoginRocket Web JWTs lack `aud`
# export AUTHROCKET_JWT_LEEWAY_SECONDS=30
# export AUTH_HANDOFF_FRESHNESS_SECONDS=60
# export CLI_LOGIN_CODE_TTL_SECONDS=30
# export QIITA_TOKEN_DEFAULT_TTL_DAYS=90
```

See [`authrocket-realm-setup.md`](authrocket-realm-setup.md) for the
realm-side configuration (test users, email-verification policy, the
`iss=https://authrocket.com` footgun).

## 3. One-shot JWT shape verify

Log into the AuthRocket realm via the hosted UI and capture the raw JWT,
then:

```bash
uv run python scripts/verify_jwt.py "$JWT"
```

The script runs the same `AuthRocketVerifier` the control plane uses; on
success it prints the parsed claims. If anything fails — bad signature,
wrong issuer, missing required claim — file an issue before proceeding.

## 4. Start the control plane

`AuthRocketVerifier.from_settings` runs at FastAPI lifespan; if any
required env var is missing the boot fails fast.

## 5. First human login (operator promotion + PAT mint)

`qiita-admin login` drives the LoginRocket Web flow end-to-end: it spawns
a localhost loopback HTTP server, opens the browser to qiita's `/auth/login`,
captures the AuthRocket round-trip, exchanges the resulting one-time code
for a PAT, and saves it to `~/.qiita/token` (mode 0600).

```bash
qiita-admin --base-url https://qiita.example.org login
```

On the *first* login, the resolver creates a `principal` row
(`system_role='user'`), a `user` row keyed on that principal, and a
`user_identities(iss, sub)` link in one transaction. An
`oidc_create_principal` audit event is recorded. The freshly-minted PAT is
written to `~/.qiita/token`.

Then promote yourself to `system_admin` (direct DB; no HTTP auth needed):

```bash
qiita-admin set-system-role --email operator@example.org --role system_admin
```

After the role change takes effect, the existing PAT carries `user`-level
scopes. Re-run `qiita-admin login` to mint a fresh PAT scoped to the
`system_admin` ceiling.

### Headless / no-browser fallback

For environments without a browser (SSH'd remote, CI, container without DISPLAY),
`qiita-admin login` will print the URL it tried to open. Open it on a machine
with a browser, complete the AuthRocket login, and the redirect will land at
`http://127.0.0.1:<port>/?ot_code=<value>`. If that loopback isn't reachable
on the same host, capture the JWT manually via the AuthRocket admin UI and use
the legacy direct PAT mint:

```bash
curl -X POST https://qiita.example.org/api/v1/auth/pat \
    -H "Authorization: Bearer $JWT" \
    -H "Content-Type: application/json" \
    -d '{"label":"my-laptop","ttl_days":90}'
```

`POST /api/v1/auth/pat` continues to accept fresh OIDC JWTs in the
`Authorization` header for this purpose. Save the plaintext `qk_...` to
`~/.qiita/token` (mode 0600).

## 8. Mint the orchestrator service account

```bash
curl -X POST https://localhost/api/v1/admin/service-accounts \
    -H "Authorization: Bearer qk_<ADMIN_PAT>" \
    -H "Content-Type: application/json" \
    -d '{
      "name":"orchestrator",
      "scopes":[
        "features:mint",
        "references:register_files",
        "references:read",
        "tickets:doget"
      ]
    }'
```

Capture the plaintext token, then on the orchestrator host:

```bash
./scripts/install-orchestrator-token.sh /etc/qiita/orchestrator.token <<<"$TOKEN"
```

The script writes to `<target>.new` with mode `0400` / owner `qiita:qiita`
and atomically renames over the target. See
[`scripts/install-orchestrator-token.sh`](../../scripts/install-orchestrator-token.sh)
for the exact behavior.

## 9. Start the orchestrator

The orchestrator reads `/etc/qiita/orchestrator.token` at startup. See
`docs/runbooks/orchestrator-token-rotation.md` for the rotation flow.
