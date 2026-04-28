# Orchestrator token rotation

**Purpose.** Operator runbook for zero-downtime rotation of the
orchestrator's service-account token (and any cron job using
`ControlPlaneClient`, which follows the same path). In-flight requests
complete with the old token; new requests use the new one. Use this for
scheduled rotation, suspected compromise, or scope changes.

For the initial mint of the orchestrator token, see
[`first-deploy.md`](first-deploy.md). For the conceptual reference
(token format, audit events, `ControlPlaneClient` resolution order), see
[`docs/auth.md`](../auth.md).

## Service accounts vs. tokens

A *service account* is a non-human principal — a row in `qiita.principal`
with a matching row in `qiita.service_account` keyed by `principal_idx`.
It carries the identity (name, scopes) and is the owner of one or more
rows in `qiita.api_tokens`. The orchestrator and each cron job is its
own service account; they are independent.

Conceptually rotation is a *token* operation: mint a new token under the
same principal, swap the file, revoke the old token, leave the principal
intact. The schema supports this — `qiita.api_tokens.principal_idx` is a
plain FK and a principal can hold multiple non-revoked tokens.

In practice today the only mint endpoint is
`POST /admin/service-accounts`, which creates a principal *and* mints
its initial token in one transaction. There is no shipped route to mint
an additional token under an existing service-account principal. So
today's rotation creates a freshly-named service account each cycle
(e.g. `orchestrator-rot-YYYY-MM-DD`) and revokes the prior account's
tokens at the end. Once `qiita-admin token mint --principal-idx
<existing>` lands, this runbook will be revised to keep the original
principal across rotations.

Rotations are scoped to a single service account. Rotating the
orchestrator's token does not touch any cron-job service account, and
rotating a cron job's token does not touch the orchestrator. Each job
follows this same procedure with its own `principal_idx` and its own
token file.

## Prerequisites

- An admin PAT with `admin:service_accounts` scope (see
  `docs/runbooks/first-deploy.md`).
- Shell access to the orchestrator host as the user that owns the token
  file (`qiita` by default).

## Steps

1. **Mint the replacement token** from any host with the admin PAT:

   ```bash
   curl -X POST $CONTROL_PLANE_URL/api/v1/admin/service-accounts \
       -H "Authorization: Bearer qk_<ADMIN_PAT>" \
       -H "Content-Type: application/json" \
       -d '{
         "name": "orchestrator-rot-2026-04-27",
         "scopes": [
           "features:mint",
           "references:register_files",
           "references:read",
           "tickets:doget"
         ]
       }'
   ```

   Copy the returned `token` and `principal_idx` immediately — the token
   is shown exactly once. Take note of both `principal_idx` values: the
   one returned here is the *new* service account (used in step 4), and
   the existing orchestrator's `principal_idx` is the *old* one whose
   tokens you'll revoke in step 5. See the "Service accounts vs. tokens"
   section above for why each rotation creates a new service account
   today.

2. **Install the new token** atomically on the orchestrator host:

   ```bash
   ./scripts/install-orchestrator-token.sh \
       /etc/qiita/orchestrator.token <<<"$NEW_TOKEN"
   ```

   The script stages at `<target>.new` (mode `0400`, owner `qiita:qiita`),
   saves the prior contents at `<target>.previous` for the rollback path
   below, and atomically renames over the target. POSIX same-filesystem
   rename is atomic — readers see either the old or the new file, never
   a partial one.

3. **Reload the orchestrator** so it re-reads the file:

   ```bash
   systemctl reload qiita-orchestrator
   ```

   The SIGHUP handler re-loads the file. In-flight HTTP calls finish
   with the old token; new calls use the new one.

4. **Wait for new-token use** — confirm the orchestrator has actually
   exercised the new token before revoking the old one:

   ```bash
   DATABASE_URL=postgresql://... \
       ./scripts/wait-for-token-use.sh "$NEW_PRINCIPAL_IDX"
   ```

   The script polls `qiita.api_tokens.last_used_at` for the new
   principal until it advances (default timeout 3 minutes). DB-direct
   rather than HTTP because `last_used_at` is intentionally not surfaced
   as an audit event and `GET /auth/tokens` is caller-scoped only — see
   the script header for the full rationale.

5. **Revoke the old token**:

   ```bash
   qiita-admin token revoke-all --principal-idx <OLD_PRINCIPAL_IDX>
   ```

   The audit log captures the rotation event automatically.

## If the new token doesn't work

The orchestrator will hit 401 on every control-plane call. Roll back by:

```bash
mv /etc/qiita/orchestrator.token /etc/qiita/orchestrator.token.bad
mv /etc/qiita/orchestrator.token.previous /etc/qiita/orchestrator.token
systemctl reload qiita-orchestrator
```

`install-orchestrator-token.sh` writes `<target>.previous` on every
install, so the previous token is always present for one rotation cycle.
After a successful rotation (step 5 complete), the `.previous` file is
no longer needed and can be removed.

## Cron jobs

Each cron job has its own service account and its own token file
(`/etc/qiita/cron-<name>.token`). One compromise = one rotation; never
share a token across jobs.
