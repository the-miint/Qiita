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

- `make verify-deploy`'s workflow-actions check already lists every
  row; confirm it includes `download-ena-study 1.0.0` (new `workflows/`
  entry, picked up by the standing `qiita-admin actions sync` inside
  `activate.sh` — no extra operator step).
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

- The rebuilt `long-read-assembly` assemble image can actually run a myloasm assembly end to end (#380): myloasm at the pinned 0.6.0 (the circular/linear split reads a header string probed against exactly that version), the splitter itself, a DuckDB matching the one the miint extension was staged with (DuckDB namespaces the staged dir by engine version, so a skew fails every myloasm ticket at LOAD), and `MIINT_EXTENSION_DIRECTORY` sitting where the step's `derived_inputs` bind expects it (`PATH_DERIVED/duckdb-ext` — it resolves RELATIVE to `PATH_DERIVED`, so a host that points it elsewhere binds a non-existent path). **The bind is unconditional, so a wrong path fails the assemble step for BOTH assemblers, `hifiasm_meta` included** — that check is not myloasm-only. The version pins are. Expect `ASSEMBLE_MYLOASM_OK`.
  ```bash
  sudo -u qiita-orch bash -c 'set -a; . /etc/qiita/compute-orchestrator.env; set +a
  test "$MIINT_EXTENSION_DIRECTORY" = "${PATH_DERIVED%/}/duckdb-ext" || { echo "MIINT bind path mismatch: $MIINT_EXTENSION_DIRECTORY"; exit 1; }
  cd /tmp && apptainer exec --no-home "${PATH_DERIVED}/images/long-read-assembly-assemble-1.0.0.sif" \
    bash -c "micromamba run -n myloasm myloasm --version | grep -Fxq \"myloasm 0.6.0\" \
             && test -s /opt/qiita/myloasm_split.py \
             && python3 -c \"import duckdb; print(duckdb.__version__)\" | grep -Fxq 1.5.4" \
    && echo ASSEMBLE_MYLOASM_OK'
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- Soft API change, additive, no host action. New `POST` /
  `GET /api/v1/ena-import-batch` (batch multi-study ENA import driver) —
  admin-only, no client is required to call either. The new migration
  (`qiita.ena_import_batch` / `qiita.ena_import_batch_item`) is a plain
  additive `CREATE TABLE`, handled autonomously by the standing `make
  migrate` step (bucket 3) — no out-of-band setup (no `CREATE EXTENSION`,
  no backfill), so it needs no bucket-3 entry.
- miint deploy staging (`stage_miint_extension`, run at deploy via
  `scripts/stage-miint-extension.sh`) now also installs DuckDB's own
  `httpfs` extension into the same `MIINT_EXTENSION_DIRECTORY`. It is
  LOADed with miint by `miint_load_sql`, so **every** miint connection in
  both planes needs it staged — a staged directory without it takes out
  `POST /ena-import-batch` at resolve as well as the download job. No new
  operator action, but not because the step is
  unconditional: the staging gate (`staging_is_current`) now reports a
  stage missing `httpfs` as stale, so the standing step re-stages a host
  that was already current on miint alone. `make verify-deploy`'s
  `cp-miint` check LOADs `httpfs` too, so a host that somehow lacks it
  fails the deploy rather than every ENA import at runtime. The install
  itself is a plain `INSTALL`, not `FORCE` — httpfs is DuckDB's own signed
  extension, not the team mirror, so a warm cache is always current.
- (#381) Each `align_sharded` step now requests **8 cpu / 64 GB**
  (was 4 / 32) — unchanged `action_ceiling`, so nothing new is expressible, but this is
  the first time the ceiling is requested by default. The SLURM partition align tickets
  land on must be able to satisfy it, or blocks will pend instead of running. Both axes
  now sit at the ceiling, which deliberately forgoes cpu/mem escalation on retry
  (walltime still escalates, PT4H → PT8H).

- `long-read-assembly` 1.0.0 accepts `assembler: myloasm` from this deploy on — previously it exited 64 mid-step. The **default is unchanged** (`hifiasm_meta`), so no existing ticket changes behaviour; picking myloasm is an assay decision made per action context. The assemble SIF auto-rebuilds to add myloasm 0.6.0 (its own conda env) plus `python-duckdb`, so its build is slower than a routine no-op verify; it bind-mounts the **already-staged** miint extension read-only rather than carrying its own copy, so no extra staging step is needed and it stays byte-identical to the CP/CO/DP build. One standing consequence: the image's DuckDB is now in lockstep with the orchestrator's, so a future `uv lock` DuckDB bump must re-pin `assemble.def` — a unit test enforces it (#380).

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
