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

- `[operator]` `make migrate` applies `20260925000000_syndna_read_count.sql` (new table `qiita.syndna_read_count`; no data change). (#TBD)

### 4. Deploy

- `[operator]` **Before the restart**, confirm no read-mask ticket is stopped between `register-files` and `finalize-mask-sample`. The read-mask workflow gains an entry (`persist-syndna-read-count`) ahead of `register-files`, and resume matches completed steps by position, so such a ticket would resume against the wrong entries. This must return 0 rows; if it does not, redrive those tickets to `completed` on the current code first (#TBD):

  ```sql
  SELECT wt.work_ticket_idx, wt.state
    FROM qiita.work_ticket wt
    JOIN qiita.work_ticket_step s ON s.work_ticket_idx = wt.work_ticket_idx
   WHERE wt.action_id = 'read-mask' AND wt.state <> 'completed'
     AND s.step_name = 'register-files' AND s.state = 'completed';
  ```

### 5. Verify

_None yet._

### 6. After the deploy verifies green

- `[operator]` Write the SynDNA read counts for samples masked before this deploy, from the `syndna` step output left in each read-mask ticket's scratch workspace. With `DATABASE_URL` and `PATH_SCRATCH` exported (as for any `qiita-admin backfill`): `qiita-admin backfill syndna-read-count` (dry run; lists samples whose file is gone), then `qiita-admin backfill syndna-read-count --execute`. Samples it lists as gone can only be counted by a re-mask. Run it soon after the deploy: the files are only as durable as the scratch workspace. (#TBD)

### Notes (no host action)

_None yet._

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
