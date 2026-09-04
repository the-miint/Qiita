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

- **[DBA, as superuser] enable `btree_gist` in `qiita_miint`**, alongside `citext`
  (#534). **On the DATABASE host, not the app host** —
  Postgres is `qiita-miint-db.ucsd.edu:5432`, separate from `qiita-miint.ucsd.edu`:
  ```bash
  # on qiita-miint-db.ucsd.edu
  sudo -u postgres psql -d qiita_miint -c "CREATE EXTENSION IF NOT EXISTS btree_gist;"
  ```
  Or, without superuser, from the app host as the migration role itself:
  ```bash
  set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS btree_gist;"
  ```
  Idempotent, sub-second, adds operator classes only — no table is locked, no rows
  change. **Optional:** the bucket-3 migration self-installs it. Doing it here matches
  how `citext` is handled and keeps the migration independent of the role's
  privileges; that migration's own comment carries the measurements behind both
  claims. Verify from the app host:
  ```bash
  set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -c "SELECT extname FROM pg_extension ORDER BY 1"
  # expect: btree_gist, citext, plpgsql
  ```
  **Optional, and preferred rather than required.** Measured on the deploy
  2026-09-04: `qiita_miint_rw` is not superuser but *does* hold `CREATE` on
  `qiita_miint`, and `btree_gist` is TRUSTED, so the migration in bucket 3 installs it
  unaided. Doing it here matches how `citext` is handled and makes the migration
  independent of the role keeping that privilege — probed on PG 17, once the
  extension exists `CREATE EXTENSION IF NOT EXISTS` is a no-op even for a role
  without `CREATE`.

### 3. Migrations

- `20260904000000_assembly_membership_one_genome_per_subject.sql` — `CREATE EXTENSION
  IF NOT EXISTS btree_gist` (a no-op if bucket 2 was done) plus an EXCLUDE constraint
  asserting that one assembled subject carries one `genome_idx`
  (#534).

  **This migration takes minutes and holds ACCESS EXCLUSIVE on
  `qiita.assembly_membership` for the whole build.** Measured against a synthetic
  copy of the deploy's shape (2,351,862 rows / 1,798,120 subjects): **260 s** to add
  the constraint, leaving a **397 MB** GiST index beside a 701 MB table. Row inserts
  under the finished constraint are unaffected (10k rows in 0.35 s).

  Migrations run *before* the restart, while the API is still up, and nothing can read
  or write that table meanwhile — so pick a window with **no assembly ticket
  mid-flight and no feature-table submission**, both of which touch it. They do not
  simply wait: the CP pool's `command_timeout` is 10 s, so a submit during the build
  fails after ten seconds rather than queueing behind the lock.

  **Pre-check, already run against the deploy 2026-09-04 — 0 violating subjects.**
  An EXCLUDE constraint cannot be added `NOT VALID`, so a violation aborts the
  migration rather than deferring. Re-run it if anything has assembled since:
  ```bash
  set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -c "
    SELECT prep_sample_idx, processing_idx, kind, bin_id, count(DISTINCT genome_idx)
      FROM qiita.assembly_membership WHERE genome_idx IS NOT NULL
     GROUP BY 1,2,3,4 HAVING count(DISTINCT genome_idx) > 1"
  ```
  Zero rows is expected: the mint upserts on a hash of those same four columns, so it
  cannot produce a violation itself. If it returns rows, reconcile before migrating —
  the constraint is checked per row, so the migration's own DEFERRABLE comment carries
  how.

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **The control plane and data plane must go out together (#534).**
  The feature-table resolver now signs a `bin_quality` DoGet ticket, and `bin_quality`
  joins the data plane's `ALLOWED_TABLES` in the same change. A CP restarted against a
  data plane that predates it gets `unknown table: "bin_quality"` (the
  `ALLOWED_TABLES` check in `flight_service.rs`) and every **combined**
  (`denovo_alignment_idx`)
  feature-table submission fails at submit; reference-only submissions are unaffected.
  The standard `redeploy.md` restarts both, so this is an ordering fact rather than a
  step — it matters only if the two are ever staged apart.

- **`estimate-feature-table` 1.0.0 changes in place, with no version bump
  (#534).** The step gains a second optional input
  (`denovo_genome_quality_path`) beside `denovo_genome_map_path`. The action sync picks
  it up; there is no separate step. Not a new version because this action mints no
  identity and writes no DuckLake row — nothing resolves against `{workflow, version}`
  here, so re-syncing 1.0.0 cannot split a stored run the way it would for
  `long-read-assembly`. Assemblies already on the host need no re-run: the scores come
  from the lake rows `assembly_load` already registered.

- **A data-plane Flight failure at submission no longer reaches the originator's digest (#532).** An expired signing token now classifies `data_plane_transient` rather than `bad_input`, joining the DuckLake serialization conflict and the connect-failure rendering of gRPC UNAVAILABLE already classified that way (a server-returned UNAVAILABLE joins them in the same PR). `notify.sweeper`'s owed set is `failure_type IS DISTINCT FROM 'retriable'`, so these tickets are held for an operator redrive instead of being reported as a settled outcome — and an originator whose whole batch failed this way gets no digest at all, because digest groups are built only from owed rows. Nothing to run; this is a change in what the mail says. Redrive held tickets with `/run` as before.

- **`long-read-assembly` baseline resources changed at three steps (#528):**
  `assembly_coverage` 64 → 96 GiB and `assembly_load` 16 → 32 both rise; `assemble`
  becomes per-assembler, 192 → 250 for hifiasm_meta but 192 → **128 for myloasm**,
  which is a reduction. The action
  sync in the deploy picks these up; there is no separate step. The measurements and
  the reasoning are at each step in `workflows/long-read-assembly/1.0.1.yaml`. Three
  things worth knowing:
  - **A routine hifiasm_meta submission no longer needs the `--mem-gb 384` floor.** It
    was covering that assembler's demand, which this cohort measured to a 246.03 GiB
    max and 250 covers from the baseline. It is not full coverage: an earlier cohort
    recorded a 259.3 GiB peak — also hifiasm_meta, the default for the 1.0.0 version
    that whole cohort ran under — and a sample in that range still OOMs and escalates
    to 500. Passing the floor still works, and still lifts every
    other step in the ticket.
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
- **`build-shard-index` shard builds request smaller SLURM slots (#537).** A 1 Gbp
  shard now asks for ~21 GiB instead of ~29 for `build_minimap2_index`, and ~25 instead
  of ~29 for `build_bowtie2_index`. No YAML baseline changed — both steps keep
  `mem_gb: 32` and the 128 GiB ceiling. What moved is each job's own `plan()` floor
  (minimap2 28 → 20, bowtie2 28 → 24), the advisory the CP applies below baseline, so
  only shard builds shrink. Per-node concurrency rises from 15–17 to 21–23 for minimap2
  (it varies with shard size) and from 17 to 20 for bowtie2.

  Host builds (`host-reference-add`, `local-host-reference-add`) are unaffected:
  `plan()` gives no opinion there and the reserve those use is unchanged at 16 GiB.
  DuckDB's own limit inside a shard build is unchanged at every shard size (9 GB for a
  1 Gbp shard), so a build behaves as it did at the larger allocation. An under-estimate
  still escalates on OOM as before and the rung is unchanged, since escalation grows
  from the YAML baseline rather than from the hint — but a shard build's DuckDB is now
  capped at 12 GB on the escalated retry instead of growing with the cgroup, so the
  extra memory reaches the index builder rather than DuckDB's heap.

  Nothing to run: the action sync does not carry this, it is job code that ships with
  the orchestrator restart.
- **Reference-workflow resources also changed (#528):** `build-shard-index`'s
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
