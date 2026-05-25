# Orchestrator token rotation

> **Applies today.** The compute orchestrator holds a `compute-worker`
> service-account PAT at `/etc/qiita/co-to-cp.token`, used for outbound
> CO→CP callbacks (currently: `POST /sequence-range` from the native
> `fastq_to_parquet` step). It is provisioned per
> [`compute-service-account-provisioning.md`](compute-service-account-provisioning.md);
> this runbook covers the zero-downtime rotation of that PAT. Cron jobs
> with their own `ControlPlaneClient` service-account PATs follow the
> same flow.

**Purpose.** Operator runbook for zero-downtime rotation of a
service-account token used by `ControlPlaneClient` (the compute
orchestrator's `compute-worker` PAT, plus cron jobs with their own
service-account PATs). In-flight requests complete with the old
token; new requests use the new one. Use this for scheduled rotation,
suspected compromise, or scope changes.

For the conceptual reference (token format, audit events,
`ControlPlaneClient` resolution order), see [`docs/auth.md`](../auth.md).

## Service accounts vs. tokens

A *service account* is a non-human principal — a row in `qiita.principal`
with a matching row in `qiita.service_account` keyed by `principal_idx`.
It carries the identity (name, scopes) and is the owner of one or more
rows in `qiita.api_token`. The orchestrator and each cron job is its
own service account; they are independent.

Conceptually rotation is a *token* operation: mint a new token under the
same principal, swap the file, revoke the old token, leave the principal
intact. The schema supports this — `qiita.api_token.principal_idx` is a
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

- An admin PAT with `admin:service_account` scope (see
  `docs/runbooks/first-deploy.md`).
- Shell access to the orchestrator host as the user that owns the token
  file (`qiita-orch` by default; see `scripts/install-orchestrator-token.sh`).

## Steps

> **v1 reality.** The orchestrator reads its PAT once at boot
> (`config.py:_resolve_token` → cached in `Settings`); there is no
> SIGHUP handler and the systemd unit declares no `ExecReload=`. So
> the orchestrator-PAT rotation today is **restart-based, not
> zero-downtime** — step 3 below uses `systemctl restart` rather than
> `reload`. Cron-job rotations stay zero-downtime (each invocation
> re-reads its own token file). The zero-downtime daemon path (SIGHUP
> handler + `ExecReload=`) is future work and will replace step 3 when
> it lands.

1. **Mint the replacement token** from any host with the admin PAT.
   This rotates the *orchestrator's* `compute-worker` token; for a
   cron-job rotation substitute the cron's name + scope set
   accordingly:

   ```bash
   curl -X POST $CONTROL_PLANE_URL/api/v1/admin/service-account \
       -H "Authorization: Bearer qk_<ADMIN_PAT>" \
       -H "Content-Type: application/json" \
       -d '{
         "name": "compute-worker-rot-2026-05-25",
         "scopes": ["sequence_range:mint"]
       }'
   ```

   Copy the returned `token` and `principal_idx` immediately — the token
   is shown exactly once. Take note of both `principal_idx` values: the
   one returned here is the *new* service account (used in step 4), and
   the existing `compute-worker` principal_idx is the *old* one whose
   tokens you'll revoke in step 5. See the "Service accounts vs. tokens"
   section above for why each rotation creates a new service account
   today. The compute-worker scope set is provisioned per
   [`compute-service-account-provisioning.md`](compute-service-account-provisioning.md);
   match its scope list exactly to avoid a drifted-scope ceiling
   between cycles.

2. **Install the new token** atomically on the orchestrator host:

   ```bash
   ./scripts/install-orchestrator-token.sh \
       /etc/qiita/co-to-cp.token <<<"$NEW_TOKEN"
   ```

   The script stages at `<target>.new` (mode `0400`, owner `qiita-orch:qiita-orch`),
   saves the prior contents at `<target>.previous` for the rollback path
   below, and atomically renames over the target. POSIX same-filesystem
   rename is atomic — readers see either the old or the new file, never
   a partial one.

3. **Pick up the new token** in the running service:

   For a short-lived process (cron jobs): no action needed — the next
   scheduled invocation reads the new file on startup. Any invocation
   already in flight finishes with the old token, which is fine: the
   old token stays valid until step 5.

   For the orchestrator daemon (today, restart-based):

   ```bash
   sudo systemctl restart qiita-compute-orchestrator
   ```

   In-flight CO→CP calls (today: only `POST /sequence-range` from the
   native `fastq_to_parquet` step) are interrupted by the restart; the
   step's job-side error path retries on the next workflow attempt, so
   the practical impact is one extra retry per work-ticket that was
   mid-flight at rotation. The zero-downtime SIGHUP path is future
   work — the `reload` form below will become the recommended one once
   it lands:

   ```bash
   # FUTURE — does not work today (no SIGHUP handler, no ExecReload=)
   # systemctl reload qiita-compute-orchestrator
   ```

4. **Wait for new-token use** — confirm the service has actually
   exercised the new token before revoking the old one:

   ```bash
   DATABASE_URL=postgresql://... \
       ./scripts/wait-for-token-use.sh "$NEW_PRINCIPAL_IDX"
   ```

   The script polls `qiita.api_token.last_used_at` for the new
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

The orchestrator will hit 401 on every control-plane call. Roll back
the file swap first; how the running service picks up the rollback
follows the same case-split as step 3 (orchestrator daemon today:
restart; cron jobs: next invocation; zero-downtime SIGHUP path is
future work):

```bash
sudo mv /etc/qiita/co-to-cp.token /etc/qiita/co-to-cp.token.bad
sudo mv /etc/qiita/co-to-cp.token.previous /etc/qiita/co-to-cp.token
# Orchestrator daemon: sudo systemctl restart qiita-compute-orchestrator
# Cron job: wait for the next scheduled invocation.
```

`install-orchestrator-token.sh` writes `<target>.previous` on every
install, so the previous token is always present for one rotation cycle.
After a successful rotation (step 5 complete), the `.previous` file is
no longer needed and can be removed.

## Cron jobs

Each cron job has its own service account and its own token file
(`/etc/qiita/cron-<name>.token`). One compromise = one rotation; never
share a token across jobs.
