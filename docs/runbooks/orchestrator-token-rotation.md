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
   is shown exactly once.

   *Alternative if the existing service account is being kept:* call
   `qiita-admin token mint --principal-idx <existing> --label rotation` once
   that subcommand lands. (Today, mint a fresh service account; the old one
   gets retired below.)

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
