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

- (#381) Confirm the synced `align 1.0.0` action carries the
  raised `align_sharded` baseline. `actions sync` runs inside `activate.sh`, so this
  only checks it took — if the row still reads `cpu: 4` / `mem_gb: 32`, align blocks
  keep submitting at the old size and the change is a silent no-op:

  ```bash
  DATABASE_URL=$(sudo grep '^DATABASE_URL=' /etc/qiita/control-plane.env | tail -1 | cut -d= -f2-)
  sudo -u qiita-api psql "$DATABASE_URL" -Atc \
    "SELECT s->'baseline_resources' FROM qiita.action, jsonb_array_elements(steps) s
      WHERE action_id='align' AND version='1.0.0' AND s->>'name'='align_sharded';"
  ```

  Expect `cpu` 8 and `mem_gb` 64.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#381) Each `align_sharded` step now requests **8 cpu / 64 GB**
  (was 4 / 32) — unchanged `action_ceiling`, so nothing new is expressible, but this is
  the first time the ceiling is requested by default. The SLURM partition align tickets
  land on must be able to satisfy it, or blocks will pend instead of running. Both axes
  now sit at the ceiling, which deliberately forgoes cpu/mem escalation on retry
  (walltime still escalates, PT4H → PT8H).

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
