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

- (#fix/long-read-assembly-action-ceiling-headroom) Confirm the synced `long-read-assembly 1.0.0`
  action carries the raised ceiling. `actions sync` runs inside `activate.sh`, so this only
  checks it took — if the row still reads `192|16:00:00`, an `assemble` OOM or timeout keeps
  failing permanently on attempt 0 (baseline == ceiling disables escalation) and the change is
  a silent no-op:

  ```bash
  sudo -u qiita-api bash -c '
    set -a; source /etc/qiita/control-plane.env; set +a
    psql "$DATABASE_URL" -Atc "SELECT mem_ceiling_gb, walltime_ceiling FROM qiita.action
      WHERE action_id = '"'"'long-read-assembly'"'"' AND version = '"'"'1.0.0'"'"';"'
  ```

  Expect `500|2 days` (was `192|16:00:00`).

  Nothing caps the raised ceiling on the SLURM side: the `qiita` partition is
  `MaxMemPerNode=UNLIMITED` / `MaxTime=UNLIMITED`, its nodes are `RealMemory=514000` with no
  `MemSpecLimit`, and the `qiita_norm` QOS carries no limits.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#fix/long-read-assembly-action-ceiling-headroom) `long-read-assembly`'s `action_ceiling`
  rises to **500 GB / P2D** (from 192 GB / PT16H); the `assemble` **baseline is unchanged** at
  32 cpu / 192 GB / PT16H, so an ordinary ticket requests exactly what it does today and
  schedules the same. What changes is failure handling: a ceiling equal to the baseline made
  OOM/TIMEOUT escalation a no-op, so a single `assemble` OOM failed the ticket permanently on
  attempt 0. Retries now climb `192 → 384 → 500` GB and `16h → 32h → 48h`. Consequence for the
  host: a **retrying** assemble step can now request up to 500 GB and 48 h, so the SLURM
  partition must be able to satisfy that or the retry pends instead of running — the `qiita`
  partition's nodes are 514000 MB, which fits 500 GB, but only just, so an escalated job
  effectively reserves a whole node.

  Second consequence, same cause: the ceiling is also the only bound on a per-ticket
  `resource_override.mem_gb`, and that override applies to **every** step in the ticket, not
  just the one that needs it. A `wet_lab_admin`+ submitting with `mem_gb: 500` would now
  inflate even the 2 GB `assembly_run_config` and 8 GB `assembly_hash` steps to 500 GB on
  attempt 0, where the old cap was 192. Role-gated, so not an escalation path — but it is a
  bigger foot-gun than it was, and a ticket submitted that way will pend hard.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
