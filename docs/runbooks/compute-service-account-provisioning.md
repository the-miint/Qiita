# Compute service-account provisioning

**Audience.** Operator deploying the compute orchestrator's raw-read
ingestion step. Run this once per environment that has the orchestrator
enabled, **before** that orchestrator first tries to POST to
`/api/v1/sequence-range`. Skip on installs that do not run raw-read
ingestion.

**Purpose.** Provision the dedicated compute service-account principal
that the compute orchestrator's raw-read ingestion step uses to call
`POST /api/v1/sequence-range`. Distinct from the cron-job service
accounts covered by [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md);
those are separate principals with their own scope sets.

> **The service-account name is site-chosen.** The examples below use
> `compute` — the name on the live `qiita-miint` deploy. Nothing in the
> code pins a specific name (the orchestrator resolves the *token* by
> file path, not by principal name), so pick whatever your site uses and
> substitute it consistently throughout this runbook and
> [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md).

For the conceptual reference (scopes, ceilings, audit events) see
[`docs/auth.md`](../auth.md). For the route contract and identifier model
see the **Raw-read identifiers** paragraph in [`docs/architecture.md`](../architecture.md).

> **One-time admin task — not per-user, not recurring.** A *service
> account* is provisioned once and never "logs in": the orchestrator
> reads its token from a file. This is unrelated to human logins — end
> users authenticate with `qiita login` (the user CLI), admins with
> `qiita-admin login` (the admin CLI), and neither runs this procedure.
> Do it once per environment; the only follow-up is the occasional
> token rotation below.

## Scope grant

The compute service account needs `sequence_range:mint` for raw-read
ingestion and `ticket:doget` for reference-chunk streaming (native index
build jobs mint a `feature_idx`-scoped DoGet ticket to pull reference
sequences from the data plane). Both are on `SERVICE_ACCOUNT_SCOPE_CEILING`.

`ticket:doget` is a **read** scope (it signs a ticket to read reference
chunks), not cross-domain minting authority, so bundling it with
`sequence_range:mint` on the `compute` principal is the default here.
**Least-privilege alternative:** keep the `sequence_idx`-domain mint isolated
by provisioning a *separate* service account that holds only `ticket:doget`
and pointing the reference build path at its token — see "Why a separate
principal" below. Still keep genuine *mint/write* scopes that span identifier
domains (`feature:mint`, `reference:register_files`) off this principal.

`sequence_range:mint` is on `SERVICE_ACCOUNT_SCOPE_CEILING` and absent
from every role ceiling, so:

- the admin route validates the requested scope set against the
  service-account ceiling and accepts the mint,
- a human PAT (`POST /auth/pat`) cannot acquire it even via the
  `system_admin` role ceiling.

## Prerequisites

- An admin PAT with `admin:service_account` scope.
- The control plane is reachable on `$CONTROL_PLANE_URL`.

## Steps

1. **Mint the service account and its initial token** in one
   transaction:

   ```bash
   curl -X POST $CONTROL_PLANE_URL/api/v1/admin/service-account \
       -H "Authorization: Bearer qk_<ADMIN_PAT>" \
       -H "Content-Type: application/json" \
       -d '{
         "name": "compute",
         "scopes": ["sequence_range:mint", "ticket:doget"]
       }'
   ```

   The response carries the plaintext token **exactly once**. Capture
   it before closing the shell.

2. **Install the token on the orchestrator host** under the orchestrator
   user, mode 0400. The path follows the direction-based naming the
   orchestrator's `Settings` resolves by default (mirrors
   `/etc/qiita/cp-to-co.token` for the inbound side):

   ```bash
   sudo install -o qiita-orch -g qiita-orch -m 0400 \
       /dev/stdin /etc/qiita/co-to-cp.token <<< "$PLAINTEXT_TOKEN"
   ```

   The orchestrator picks this path up via `DEFAULT_CO_TO_CP_TOKEN_PATH`
   in `qiita-compute-orchestrator/src/qiita_compute_orchestrator/config.py`;
   override with the `CO_TO_CP_TOKEN_PATH` env var if you need a
   non-default location.

3. **Verify** the credential authenticates by hitting `whoami`:

   ```bash
   curl -H "Authorization: Bearer $(cat /etc/qiita/co-to-cp.token)" \
        $CONTROL_PLANE_URL/api/v1/auth/whoami
   ```

   The response should show
   `{"kind": "service", "name": "compute", "scopes": ["sequence_range:mint", "ticket:doget"]}`
   (or whatever name you chose above).

4. **Confirm the route works** end-to-end against a known
   prep_sample_idx.

   > ⚠️ **Substitute a real `prep_sample_idx` your operator workflow
   > controls before running this.** The example uses `42` as a
   > placeholder; minting against an arbitrary idx will either fail
   > (404 if absent) or permanently commit a range for a prep_sample
   > you may not own. The underlying `sequence_idx` allocation is
   > **not reversible** — even a `DELETE FROM qiita.sequence_range`
   > doesn't return the consumed bigints to the pool.

   ```bash
   curl -X POST $CONTROL_PLANE_URL/api/v1/sequence-range \
       -H "Authorization: Bearer $(cat /etc/qiita/co-to-cp.token)" \
       -H "Content-Type: application/json" \
       -d '{"prep_sample_idx": 42, "count": 1}'
   ```

   Expect 201 with a body of shape
   `{"prep_sample_idx": 42, "sequence_idx_start": N, "sequence_idx_stop": N, "created_at": "..."}`.

## Rotation

Follow [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md)
with the compute principal (the site-chosen name above) substituted for
`orchestrator`. The same "mint new, swap file, revoke old" flow applies;
the token file path is the only orchestrator-specific detail.

## Why a separate principal

Least-privilege per identifier domain. `sequence_range:mint` allocates
ranges in the `sequence_idx` space; the *mint/write* scopes the orchestrator
will need as more native steps land (`feature:mint`,
`reference:register_files`) authorize writes in the `feature_idx`
space. Bundling those on one principal would mean a single compromised
token carries minting authority across two unrelated identifier
domains, so keep them on separate principals.

`ticket:doget` is the exception that is bundled onto `compute` by default:
it is a **read** scope (it signs a short-TTL ticket to read reference
chunks over Flight), not minting authority, so a compromised token cannot
create identifiers in any domain. If your threat model still wants the
`sequence_idx` mint isolated from every reference-domain capability,
provision a separate service account holding only `ticket:doget` and point
the reference build path's token at it — the same "extra row in
`qiita.service_account`" cost buys the split.
