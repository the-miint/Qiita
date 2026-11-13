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

```bash
# [admin] gpl-boundary must report >= 0.5.0, or compute-readiness's miint-gpl-boundary probe fails the deploy.
# If it is older, replace the binary at that path by hand (the deploy does not upgrade it: #859).   (#gene-call-schema)
sudo -u qiita-orch "$(sudo grep -oP '^MIINT_GPL_BOUNDARY_PATH=\K.*' /etc/qiita/compute-orchestrator.env)" --version | grep gpl_boundary
```

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- `.env.compute-orchestrator.example`'s `MIINT_GPL_BOUNDARY_PATH` comment now names the tools behind the boundary (bowtie2, FastTree, Prodigal) and the 0.5.0 minimum; no live env change. (#gene-call-schema)
- New DuckLake tables `gene_call` / `gene_call_contig`, created empty by the data plane at start; nothing writes them yet. (#gene-call-schema)
- The staged miint must have `prodigal_proteins` (duckdb-miint `2b215901` or later); the miint stage step restages when the mirror's build changes, and compute-readiness's `miint-gpl-boundary` probe fails on a build without it. (#gene-call-schema)

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
