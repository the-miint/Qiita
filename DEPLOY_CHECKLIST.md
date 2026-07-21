# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- **CP `MIINT_EXTENSION_DIRECTORY`** — the CP runner now LOADs miint in-process to stream masked reads (the `long-read-assembly` input). Copied from the CO's env so the two stay byte-identical; the directory only needs to be **readable** by `qiita-api` (LOAD writes nothing). Not fail-fast, unlike most of this bucket: the CP boots and serves every other route, and only `long-read-assembly` tickets fail — at submission, with a message naming this var. Bucket 5's `cp-miint` check is what catches a missed step. `(#352)`
  ```bash
  sudo bash -c 'set -e
  f=/etc/qiita/control-plane.env
  grep -q "^MIINT_EXTENSION_DIRECTORY=" "$f" && { echo "already set"; exit 0; }
  line=$(grep -h "^MIINT_EXTENSION_DIRECTORY=" /etc/qiita/compute-orchestrator.env) \
    || { echo "MIINT_EXTENSION_DIRECTORY missing from compute-orchestrator.env — set it there first" >&2; exit 1; }
  [ -s "$f" ] && [ "$(tail -c1 "$f" | wc -l)" -eq 0 ] && echo >> "$f"   # no trailing newline: do not concatenate onto the last line
  printf "%s\n" "$line" >> "$f"
  echo "installed: $line"'
  ```

- **`DATA_PLANE_URL` for the control plane** — point it at the new loopback gRPC balancer so CP-side Flight traffic spreads across data-plane instances instead of pinning to instance #1. Not fail-fast (unset falls back to `grpc://localhost:50051`, so the unit still boots) — but leaving it unset means horizontal scaling has no effect on CP traffic. Compute nodes are unaffected: `compute-orchestrator.env` already points at `grpc+tls://<fqdn>:443`, which nginx balances. (#359)

  ```bash
  # [admin]
  grep -q '^DATA_PLANE_URL=' /etc/qiita/control-plane.env \
    || sudo bash -c 'echo "DATA_PLANE_URL=grpc://127.0.0.1:50050" >> /etc/qiita/control-plane.env'
  ```

### 2. One-time host setup

- **Grant `read:doget` to the compute service account.** Block-scoped compute jobs (`read-mask-block`'s qc/host_filter, `align`'s align_sharded) now stream their reads from the data plane and mint a short-TTL ticket at runtime via `POST /read/ticket/doget`. That route is gated on a NEW scope, deliberately **not** the generic `ticket:doget` the reference/alignment doget routes use: `read_block` streams RAW reads (host/human sequence — a strict superset of the `read_masked` surface, which already has its own `read_masked:doget`), so riding the reference-read scope would have let any account minting reference tickets pull raw reads. Without this grant every block work ticket fails its first streaming step with a 403. (#359)

  ```bash
  # [operator] — re-mint the compute SA's PAT with the added scope, then install it.
  # Same procedure as any scope change; see docs/runbooks/compute-service-account-provisioning.md.
  uv run qiita-admin service-account token \
      --name compute \
      --scopes 'feature:mint,reference:register_files,reference:read,ticket:doget,ticket:doput,sequence_range:mint,sequenced_pool_preflight:read,read_masked:doget,read:doget'
  ```

  Install the printed token at `/etc/qiita/co-to-cp.token` (mode `0400`, owner `qiita-orch`) exactly as the provisioning runbook describes, then restart `qiita-compute-orchestrator`.

### 3. Migrations

- `make migrate` applies three files for the operator-cancel `cancelled` state, no
  out-of-band setup: `20260721000000_work_ticket_state_cancelled.sql` (plain
  `ALTER TYPE … ADD VALUE 'cancelled'`), then the notify owed-set index recreate pair
  `20260721000001_drop_email_owed_idx.sql` + `20260721000002_email_owed_idx_with_cancelled.sql`
  (CONCURRENTLY drop+create widening the partial index to the new terminal state — so a
  cancelled ticket's originator still gets the digest). (#350)
- `make migrate` also applies `20260721000003_seed_mouse_gut_terminology.sql`, no
  out-of-band setup: a pure data seed appending three controlled-vocabulary terms to
  the terminologies already seeded — NCBI Taxonomy gains `410661` (mouse gut
  metagenome) and `10090` (Mus musculus), ENVO gains `ENVO:00006776`
  (animal-associated habitat, flagged `source_deprecated` because it is obsolete
  upstream but appears in data we import). Every INSERT is `ON CONFLICT DO NOTHING`,
  so re-running is a no-op. It appends terms rather than running the terminology
  reload pipeline, so `version` / `loaded_at` on the parent NCBI Taxonomy and ENVO
  rows are deliberately left as originally seeded. (#360)

### 4. Deploy

- **Optional — scale the data plane out.** The instance set is now a single knob, `QIITA_DATA_PLANE_PORTS` (default `50051`), read by `deploy/activate.sh` to render the nginx upstream AND to enable/restart the matching `qiita-data-plane@NNNN` units. Previously the upstream was a checked-in literal that `activate.sh` overwrote on every deploy and the restart list was hardcoded to `@50051`, so a hand-added instance lost its upstream entry at the next deploy and never restarted onto new code. Pass it through `sudo -E` so it survives into the privileged half. Deploying without it is a no-op (single instance, as today). (#359)

  ```bash
  # [admin] — one instance per ~core you want to give the data plane
  sudo -E env QIITA_DATA_PLANE_PORTS="50051 50052 50053" make redeploy QIITA_HOSTNAME=qiita-miint.ucsd.edu
  ```

  The unit is a template whose instance specifier IS the listen port, so no new unit files are needed. `activate.sh` `systemctl enable`s each instance, so added ones also survive a reboot.

  **Scaling back DOWN is not automatic.** `activate.sh` only enables/restarts the ports in the list; it never disables one you removed. Drop an instance by hand after redeploying with the shorter list, or it keeps running (out of the nginx upstream, but still bound and holding a DuckLake connection):

  ```bash
  # [admin]
  sudo systemctl disable --now qiita-data-plane@50053
  ```

### 5. Verify

- **`cp-miint`** — new `make verify-deploy` check (no separate command): asserts the control plane can LOAD miint, the masked-read streaming path `long-read-assembly` depends on. A red row here means bucket 1 was missed. `(#352)`
- **Per-instance data-plane health + the balancer.** `make verify-deploy` now health-checks every port in `QIITA_DATA_PLANE_PORTS` individually, plus `localhost:50050` (nginx → the pool). Checking only `:50051` would have reported a healthy data plane while a scaled-out instance was down — and nginx would keep routing a share of every job's traffic into it. Export the same `QIITA_DATA_PLANE_PORTS` you deployed with, or it only checks `50051`. (#359)

  ```bash
  # [admin]
  sudo -E env QIITA_DATA_PLANE_PORTS="50051 50052 50053" make verify-deploy QIITA_HOSTNAME=qiita-miint.ucsd.edu
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- New `work_ticket:cancel` scope (system_admin) gates `qiita-admin ticket cancel`.
  PATs minted before this deploy are frozen and won't carry it, so an admin must
  **re-login** (`qiita-admin login`, or re-mint) to pick it up before the cancel
  command works — a stale-scope 403 otherwise names the fix. (#350)
- **Block reads now stream from the data plane instead of being staged to scratch.** The `read-mask-block` and `align` workflows no longer have the control plane ask the data plane to COPY a `reads.parquet` onto shared scratch at submit time; the compute job mints a short-TTL DoGet ticket at runtime (`POST /read/ticket/doget`) and streams its block's reads. The scope grant this needs is in bucket 2 above. Two visible consequences for an operator reading logs: block work-ticket submission gets faster (the bulk COPY leaves the CP's submit path), and per-ticket `reads.parquet` files stop appearing under the ticket workspaces — a block job now drains its stream to a short-lived Parquet inside its OWN workspace instead. The per-sample `read-mask` path is unchanged and still stages a Parquet. (#359)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
