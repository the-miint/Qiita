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

- `[admin]` `sudo make verify-deploy QIITA_HOSTNAME=<fqdn>` now carries two ENA egress rows —
  grep its output for both (#584):
  - `ena-reachability` — the control-plane host HEADs `https://www.ebi.ac.uk`. Red means
    outbound HTTPS to the archive is blocked from this host, so **every** ENA import fails at
    metadata resolve. Hatch: `SKIP_ENA_REACHABILITY=1` (this row only).
  - `probe/ena-from-compute` — a SLURM compute node HEADs `www.ebi.ac.uk` and
    `ftp.sra.ebi.ac.uk`. Red means every import's read-download step fails. No per-row hatch;
    it rides the SLURM probe job, so `SKIP_SLURM_PROBE=1` drops it along with every other
    `probe/*` row.

  These replace the manual "confirm outbound HTTPS to the ENA archives" host-setup step this
  deploy's predecessor carried by hand. Green proves egress only: the fetch itself runs through
  DuckDB httpfs, so a proxy or CA problem confined to httpfs still surfaces at the first import.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

_None yet._

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
