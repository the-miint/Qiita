# Orchestrator token rotation

Service-account tokens for the orchestrator (and any cron job using
`ControlPlaneClient`) rotate via the same path. The procedure is
zero-downtime: in-flight requests complete with the old token; new
requests use the new token.

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

2. **Stage the new token** on the orchestrator host:

   ```bash
   install -m 0400 -o qiita -g qiita /dev/stdin \
       /etc/qiita/orchestrator.token.new <<<"$NEW_TOKEN"
   ```

3. **Atomically replace** the active file (same-filesystem `mv` is
   atomic on POSIX):

   ```bash
   mv /etc/qiita/orchestrator.token.new /etc/qiita/orchestrator.token
   ```

4. **Reload the orchestrator** so it re-reads the file:

   ```bash
   systemctl reload qiita-orchestrator
   ```

   The SIGHUP handler re-loads the file. In-flight HTTP calls finish
   with the old token; new calls use the new one.

5. **Wait for new-token use** — the new token's `last_used_at` advances
   within ~2 minutes (per the `record_token_use` coalescing window):

   ```bash
   curl $CONTROL_PLANE_URL/api/v1/admin/audit?event_type=token_use \
       -H "Authorization: Bearer qk_<ADMIN_PAT>" | jq '.[0]'
   ```

6. **Revoke the old token**:

   ```bash
   qiita-admin token revoke-all --principal-idx <OLD_PRINCIPAL_IDX>
   ```

   The audit log captures the rotation event automatically.

## If the new token doesn't work

The orchestrator will hit 401 on every control-plane call. Roll back by:

```bash
mv /etc/qiita/orchestrator.token /etc/qiita/orchestrator.token.bad
mv /etc/qiita/orchestrator.token.previous /etc/qiita/orchestrator.token  # if you saved it
systemctl reload qiita-orchestrator
```

(You should keep the previous token around for at least one rotation
cycle for exactly this reason.)

## Cron jobs

Each cron job has its own service account and its own token file
(`/etc/qiita/cron-<name>.token`). One compromise = one rotation; never
share a token across jobs.
