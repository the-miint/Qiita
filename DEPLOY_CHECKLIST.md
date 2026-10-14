# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's the changelog, see [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

### 2. One-time host setup

_None yet._

### 3. Migrations

- `[operator]` (#685) `make migrate` applies `20261009000000_work_ticket_follow_on.sql` (three nullable columns on `qiita.work_ticket`, two CHECKs, one partial index; no backfill).
- `[operator]` (#711) `make migrate` applies `20261014000000_legacy_band_column_comments.sql` (rewords the comments on `study.idx`, `prep_sample.idx` and `sequenced_pool.idx`; no row, sequence or constraint change).

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#685) Work-ticket responses (`GET /work-ticket/{idx}` and the ticket lists) gain `on_success`, `follow_on_work_ticket_idx` and `follow_on_error` (all null for a ticket with no follow-on); clients validating that shape strictly must accept them.
- (#707) New optional `FOLLOW_ON_SWEEP_INTERVAL_SECONDS` (default 60) tunes how often the control plane retries a transiently-failed follow-on; no action required.
- (#711) `POST /study` now answers 422 to a body key it does not define, where it used to drop the key and return 201; a client sending extra keys must stop. It also takes an optional `study_idx` (system_admin only).

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
