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

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

- (TASK-04) `make verify-deploy`'s workflow-actions check already lists every
  row; confirm it includes `download-ena-study 1.0.0` (new `workflows/`
  entry, picked up by the standing `qiita-admin actions sync` inside
  `activate.sh` — no extra operator step).

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (TASK-06) Soft API change, additive, no host action. New `POST` /
  `GET /api/v1/ena-import-batch` (batch multi-study ENA import driver) —
  admin-only, no client is required to call either. The new migration
  (`qiita.ena_import_batch` / `qiita.ena_import_batch_item`) is a plain
  additive `CREATE TABLE`, handled autonomously by the standing `make
  migrate` step (bucket 3) — no out-of-band setup (no `CREATE EXTENSION`,
  no backfill), so it needs no bucket-3 entry.
- (TASK-04) miint deploy staging (`stage_miint_extension`, run at deploy via
  `scripts/stage-miint-extension.sh`) now also installs DuckDB's own
  `httpfs` extension into the same `MIINT_EXTENSION_DIRECTORY` — the new
  `ingest_ena_reads` job's `open_miint_ena_conn` LOADs it for
  `read_ena_sequences`'s HTTP transport. No new operator action: the
  standing staging step already covers it (a plain `INSTALL`, not `FORCE` —
  httpfs is DuckDB's own signed extension, not the team mirror, so a warm
  cache is always current).

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
