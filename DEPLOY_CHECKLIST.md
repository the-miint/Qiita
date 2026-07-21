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

- `make migrate` applies three files for the operator-cancel `cancelled` state, no
  out-of-band setup: `20260721000000_work_ticket_state_cancelled.sql` (plain
  `ALTER TYPE … ADD VALUE 'cancelled'`), then the notify owed-set index recreate pair
  `20260721000001_drop_email_owed_idx.sql` + `20260721000002_email_owed_idx_with_cancelled.sql`
  (CONCURRENTLY drop+create widening the partial index to the new terminal state — so a
  cancelled ticket's originator still gets the digest). (#349)

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- New `work_ticket:cancel` scope (system_admin) gates `qiita-admin ticket cancel`.
  PATs minted before this deploy are frozen and won't carry it, so an admin must
  **re-login** (`qiita-admin login`, or re-mint) to pick it up before the cancel
  command works — a stale-scope 403 otherwise names the fix. (#349)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
