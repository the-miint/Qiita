# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

### 2. One-time host setup

_None yet._

### 3. Migrations

- `make migrate` applies two new migrations — both plain (nullable `ADD COLUMN` / `CREATE TABLE`, no backfill, no `CREATE EXTENSION`): `20260721000000_reference_membership_accession.sql` (persist the FASTA-header record accession per reference membership) and `20260721000001_reference_exclusion.sql` (the curated global feature/genome blocklist table). (#361)

### 4. Deploy

_None yet._

### 5. Verify

- Reference exclusion wired: `GET /api/v1/reference/{idx}/exclusion` on any active reference returns `200` with `[]` (confirms the new route + the `reference_exclusion` migration reached the CP; read-only, creates no block). (#361)

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- New `system_admin`-only scope `reference:exclusion:write` (a code-defined ceiling, no DB grant) plus new routes `POST`/`DELETE /reference/exclusion`, `POST /reference/exclusion/sync` (operator force-resync of the mirror when it drifts — a failed sync, a rebuilt DuckLake catalog, or a fresh data plane), and `GET /reference/{idx}/exclusion`. CLI: `qiita-admin reference exclusion add/remove/sync` (the write surface) and `qiita reference exclusion list` (the `reference:read` query any user can run). Soft API addition — no existing client breaks. (#361)
- The data-plane build now creates two anti-join views at boot (`alignment_visible`, `reference_taxonomy_visible`) and **removes the raw `alignment` / `reference_taxonomy` tables from the DoGet allowlist** — a DoGet ticket signed for a raw name is now rejected. No in-repo consumer named the raw tables directly (the CP mint routes choose the table, and both were flipped to the views there), so no in-repo action is needed — but an **out-of-repo** client that signs tickets for the raw names will break. A standard redeploy (which rebuilds + restarts the DP, i.e. **don't `SKIP_BUILD`**) is required for the views and the `sync_reference_exclusion` DoAction to exist. (#361)
- All four reference-load workflows (`reference-add`, `local-reference-add`, `host-reference-add`, `local-host-reference-add`) gained a post-load `sync-reference-exclusion` step (no version bump); `qiita-admin actions sync` (run by `activate.sh`) re-upserts them at deploy — no separate action. (#361)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
