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

- `make migrate` applies `20260802000000_work_ticket_escalated_resource_floor.sql`, no
  out-of-band setup: an additive `ALTER TABLE qiita.work_ticket ADD COLUMN
  escalated_resource_floor JSONB`, plus a shape CHECK (`NULL` or a JSON object) and the
  column's `COMMENT ON`. Nullable, backfill-free — every existing row reads NULL, which
  the runner treats as "nothing escalated yet". Carries the per-step memory/walltime floor
  the OOM/TIMEOUT retry ladder climbs to, so a CP restart or a `/run` redrive continues
  the ladder instead of restarting at the YAML baseline. (#415)

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- After this deploy, a ticket whose step OOM-kills or times out keeps its escalated size
  across a CP restart and a `/run` redrive — the previous behaviour re-burned one failing
  attempt per affected step climbing back (observed on `long-read-assembly` 6978 / 6980 /
  6989: `assemble` back at 192 GB after redriving from 384 GB). To put a redriven ticket
  back on its YAML baseline (e.g. after correcting an oversized input), NULL the column
  first: `UPDATE qiita.work_ticket SET escalated_resource_floor = NULL WHERE
  work_ticket_idx = <idx>;` — SQL `NULL`, not `'null'::jsonb`, which the CHECK rejects
  precisely because the runner would read it as "nothing escalated yet". (#415)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
