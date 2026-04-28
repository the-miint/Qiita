# First-deploy bootstrap

This runbook covers bringing up a fresh deployment of the auth surface,
from a clean database to a working orchestrator service account. Each step
is independent — re-running it should be safe (idempotent) unless noted.

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
export AUTHROCKET_ISSUER=https://merry-lion-7652.e2.loginrocket.com
export AUTHROCKET_AUDIENCE=<dev-realm client id from AuthRocket admin panel>
# Optional — defaults computed:
# export AUTHROCKET_JWKS_URL=$AUTHROCKET_ISSUER/connect/jwks
# export AUTHROCKET_JWT_LEEWAY_SECONDS=30
# export AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS=300
# export QIITA_TOKEN_DEFAULT_TTL_DAYS=90
```

Configure the AuthRocket realm itself for short JWT TTLs (≤60 minutes
recommended) so retire / disable actions take effect promptly. This is
realm-side config in the AuthRocket admin panel, not controlled by our env.

## 3. One-shot JWT shape verify

Log into the AuthRocket dev realm via the hosted UI and capture the raw JWT,
then:

```bash
uv run python scripts/verify_jwt.py "$JWT"
```

The script runs the same `AuthRocketVerifier` the control plane uses; on
success it prints the parsed claims. If anything fails — bad audience,
missing claim, signature mismatch — file an issue before proceeding.

## 4. Start the control plane

`AuthRocketVerifier.from_settings` runs at FastAPI lifespan; if any
required env var is missing the boot fails fast.

## 5. First human login

The first operator logs into the AuthRocket dev realm via the hosted UI,
captures a fresh JWT (or uses a CLI flow once the deferred `qiita-admin
login` lands), and calls any authenticated endpoint to trigger first-login
upsert:

```bash
curl https://localhost/api/v1/auth/whoami \
    -H "Authorization: Bearer $JWT"
```

The resolver creates a `principal` row (`system_role='user'`), a `user` row
keyed on that principal, and a `user_identities(iss, sub)` link in one
transaction. An `oidc_create_principal` audit event is recorded.

## 6. Promote the operator to system_admin

On the DB host (no HTTP auth required — `qiita-admin set-system-role` talks
directly to Postgres):

```bash
qiita-admin set-system-role --email operator@example.org --role system_admin
```

The CLI refuses to operate on `idx=1`. After this, the operator can mint
admin-scoped PATs.

## 7. Mint the operator's PAT

```bash
curl -X POST https://localhost/api/v1/auth/pat \
    -H "Authorization: Bearer $JWT" \
    -H "Content-Type: application/json" \
    -d '{"label":"my-laptop","ttl_days":90}'
```

Capture the plaintext `qk_...` token from the response — it is shown
exactly once and is never logged or persisted in cleartext. Save it to
`~/.qiita/token` (mode 0600) or export `QIITA_TOKEN` so the
`qiita-admin` HTTP subcommands can use it.

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
install -m 0400 -o qiita -g qiita /dev/stdin /etc/qiita/orchestrator.token <<<"$TOKEN"
```

## 9. Start the orchestrator

The orchestrator reads `/etc/qiita/orchestrator.token` at startup. See
`docs/runbooks/orchestrator-token-rotation.md` for the rotation flow.
