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

- The rebuilt `long-read-assembly` binning image carries the `samtools sort` staging step, and `_lib.sh` resolves the allocation from `QIITA_CPUS`/`QIITA_MEM_MB` (#370). Both greps are anchored at start-of-line so a comment mentioning the command cannot satisfy them:
  ```bash
  cd /tmp && sudo -u qiita-orch apptainer exec --no-home \
    "${PATH_DERIVED}/images/long-read-assembly-binning-1.0.0.sif" \
    bash -c "grep -q '^micromamba run -n metawrap samtools sort' /opt/qiita/binning.sh \
             && grep -q '^THREADS=\"\${QIITA_CPUS:-' /opt/qiita/_lib.sh \
             && grep -q '^MEM_MB=\"\${QIITA_MEM_MB:-' /opt/qiita/_lib.sh" \
    && echo BINNING_SORT_OK
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- Every workflow SIF **auto-rebuilds** on this deploy — `binning.sh` changed (it is in the binning image's `HASH_INPUTS`) and `workflows/_shared/_lib.sh` changed, which is hashed into *every* image's build-inputs digest. No manual build step (#370).
- Container steps now receive `QIITA_CPUS` / `QIITA_MEM_MB` (the step's resolved `baseline_resources`) via `apptainer --env`; entrypoints read them through `_lib.sh`. This is internal to the orchestrator — **no host env var to set**. The entrypoints fall back to the old `nproc` chain when the vars are absent, so a SIF that rebuilds before the orchestrator restarts still runs (#370).
- `workflows/long-read-assembly/1.0.0.yaml` changed in **comments only** — `qiita-admin actions sync` will re-upsert the same `long-read-assembly` `1.0.0` row; no new action version, nothing to re-verify beyond the generic `make verify-deploy` action list (#370).
- Tickets that already failed at `binning` with `ERROR: the bam file 'reads.bam' is not sorted!` need a resubmit after this deploy; nothing on the host to change (#370).

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
