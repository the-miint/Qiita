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

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **`long-read-assembly` baseline memory rose at three steps (#526):** `assemble`
  192 → 250 GiB, `assembly_coverage` 64 → 96, `assembly_load` 16 → 32. The action
  sync in the deploy picks these up; there is no separate step. Two consequences
  worth knowing before the next submission batch:
  - **The `--mem-gb 384` floor is no longer needed for a routine `long-read-assembly`
    submission.** It was covering `assemble`'s upper demand mode (measured 189-246
    GiB), which 250 now covers from the baseline. Passing it still works — it is a
    ticket-wide floor, so it also lifts every other step in the ticket.
  - **Tickets already submitted to SLURM keep their old allocation**, whether running
    or still queued: the dispatcher adopts any attempt that already has a
    `slurm_job_id` rather than re-submitting it. Only steps dispatched after the
    restart get the new numbers.
- **`assemble` still fits two per node at 250 GiB** on 514000 MB nodes (250 × 2 = 500).
  A future raise past 251 would drop it to one per node — the reason 250 was chosen
  over a value with more margin is recorded at the step in the workflow YAML.
- **Reference-workflow resources also changed (#526):** `build-shard-index`'s
  `build_minimap2_index` `cpu: 4` → `1`; `local-reference-add`'s `load` `PT24H` →
  `PT36H` and `build_routing_index` `PT24H` → `PT12H`. Picked up by the same action
  sync, no separate step. Two are walltime reductions on the *local* reference path
  only — the `reference-add` / `host-reference-add` variants are untouched, so a
  reference loaded through those still uses their existing limits.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
