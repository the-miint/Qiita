# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

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

- `[operator]` `make migrate` applies `20260925000000_syndna_read_count.sql` (new table `qiita.syndna_read_count`; no data change). (#621)

- **[operator] Between `make migrate` and the bucket-4 restart, a study-field edit that
  declares a field unique answers 500 (#628).** `20260918000000_unique_in_study_propagation_lock.sql`
  makes the propagation refuse a caller that has set no `lock_timeout`, and the control plane
  still running at that point does not set one — only the build this deploy installs does. The
  window is the gap between the two steps, and nothing has to be done about it beyond not
  reporting the 500 as a regression: `PATCH /api/v1/study/{S}/biosample-field/{F}` (and its
  prep-sample twin) carrying `unique_in_study: true` recovers on the restart, with no partial
  state left behind. That file and `20260915000000_sample_field_widen_fn.sql` only create or
  replace functions, so neither adds a lock window on the metadata tables to size.

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

- `[operator]` Write the SynDNA read counts for prep_samples masked before this deploy, from the `syndna` step output left in each read-mask ticket's scratch workspace. With `DATABASE_URL` and `PATH_SCRATCH` exported (as for any `qiita-admin backfill`): `qiita-admin backfill syndna-read-count` (dry run; lists prep_samples whose file is gone), then `qiita-admin backfill syndna-read-count --execute`. prep_samples it lists as gone can only be counted by a re-mask. Run it soon after the deploy: the files are only as durable as the scratch workspace. (#621)

### Notes (no host action)

- **A hand-written `UPDATE ... SET unique_in_study = true`, or a hand-written
  `SELECT qiita.widen_study_field_to_text(...)`, now fails unless the session sets
  `lock_timeout` first (#628).** Both lock the metadata table against concurrent writers
  and refuse an unbounded wait for it, which would queue every metadata write until
  someone noticed. Run `SET LOCAL lock_timeout = '3s';` in the same transaction. The
  deployed API path sets it for itself; the bucket-3 note covers the window before the
  restart in which the running one does not.

- **The study-field edit route accepts a new body key, `data_type` (#628).** Its only
  permitted value is `text`, which redeclares the field and carries its stored values
  into `value_text`. No client has to change, and the route's access bar is unchanged;
  clients that reject unknown response keys are unaffected, since the response shape
  already carried `data_type`.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
