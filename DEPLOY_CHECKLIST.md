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

- **`long-read-assembly` baseline resources changed at three steps (#526):**
  `assembly_coverage` 64 → 96 GiB and `assembly_load` 16 → 32 both rise; `assemble`
  becomes per-assembler, 192 → 250 for hifiasm_meta but 192 → **128 for myloasm**,
  which is a reduction. The action
  sync in the deploy picks these up; there is no separate step. The measurements and
  the reasoning are at each step in `workflows/long-read-assembly/1.0.1.yaml`. Three
  things worth knowing:
  - **A routine hifiasm_meta submission no longer needs the `--mem-gb 384` floor.** It
    was covering that assembler's demand, which this cohort measured to a 246.03 GiB
    max and 250 covers from the baseline. It is not full coverage: an earlier cohort
    recorded a 259.3 GiB peak, and a sample in that range still OOMs and escalates to
    500. Passing the floor still works, and still lifts every other step in the ticket.
  - **`assemble` is now sized per assembler:** hifiasm_meta 250 GiB, myloasm 128, so a
    myloasm ticket no longer holds an allocation sized for the other assembler. It
    resolves from a `profiles:` lookup rather than a flat number. That lookup keys on
    an output the step already declared, so resuming a ticket does not break on it —
    which is a separate question from which numbers a resumed ticket runs at, below.
  - **Work already submitted to SLURM keeps its old allocation.** The dispatcher adopts
    any attempt that already carries a `slurm_job_id` instead of re-submitting, and
    that is per *step attempt*, not per ticket — a ticket mid-flight at the restart
    finishes its in-flight step at the old numbers and picks up the new ones for the
    steps dispatched after it.
- **Reference-workflow resources also changed (#526):** `build-shard-index`'s
  `build_minimap2_index` `cpu: 4` → `1`; `local-reference-add`'s `load` `PT24H` →
  `PT36H` (an increase) and `build_routing_index` `PT24H` → `PT12H` (a reduction).
  Same action sync, no separate step. Both walltime changes are in
  `local-reference-add` only: `reference-add`, `host-reference-add` and
  `local-host-reference-add` are untouched and keep their existing limits, including
  the same `build_minimap2_index` step at `cpu: 4` in the latter two.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
