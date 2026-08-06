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

- `20260806120000_alignment_sample_prep_sample_idx.sql` — plain `make migrate`, no
  out-of-band setup. Builds `CREATE INDEX CONCURRENTLY` on `qiita.alignment_sample`, so it
  does **not** lock the table and is safe to run while services are up; it may take a while
  on a large table, and a failed CONCURRENTLY build leaves an INVALID index that
  `make migrate` will not retry — drop it by hand and re-run if that happens.
  (`#m3-human-alignment-mint` — retag with the PR number)

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **A user whose PAT predates this deploy cannot use the new alignment mint
  until they re-mint it.** A new scope `alignment:doget` is added to all three
  role ceilings, so callers on the OIDC path pick it up automatically (that path
  returns the role's full ceiling per request). The token path returns the
  token's **own** stored scope set, so an existing PAT does not — the holder
  runs `qiita login` (or `POST /auth/pat`) once. The 403 says so itself: the
  stale-token hint fires precisely when a scope is in the caller's live ceiling
  but absent from their token. Nothing to do on the host.
  (`#m3-human-alignment-mint` — retag with the PR number)
- **A feature-table (`estimate_feature_table`) job that is already running when
  the data plane restarts will fail with `InvalidArgument: alignment_visible
  requires an explicit projection column list`.** The alignment DoGet now
  requires the column list to be signed into the ticket, and a job launched from
  the pre-deploy orchestrator code does not send one. No host configuration
  changes and no rollback: if a job was in flight, resubmit that ticket and it
  picks up the new code. This is expected, not a regression. (#435)
- **`activate.sh` restarts the control plane BEFORE the data plane, so for a few
  seconds a new CP signs `columns` an old DP ignores.** Harmless on this deploy
  and only on this deploy: the old data plane applies its retired hardcoded
  six-column projection, which is exactly the list the one consumer
  (`estimate_feature_table`) asks for, so the stream is the same either way. The
  next change to a consumer's column set does not have that luck — it must
  restart the data plane first, or a ticket minted in the window streams the old
  six columns while the consumer binds something else. Recorded because the
  direction is silent: an old DP has no `deny_unknown_fields` on its ticket
  payload, so it drops what it does not understand rather than refusing (fixed
  going forward — this build refuses). (#435)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
