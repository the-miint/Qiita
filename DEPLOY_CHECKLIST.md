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

- `[operator]` **Confirm the mirror publishes the DuckDB 1.5.5 miint build** before the
  restart — every component now runs 1.5.5, and the stage step fetches from here. (#651)
  ```bash
  curl -fsSI https://ftp.microbio.me/pub/miint/v1.5.5/linux_amd64/miint.duckdb_extension.gz | head -1   # expect 200
  ```
- `[admin]` **Install the DuckDB 1.5.5 CLI for `make lake-shell` / `scripts/lake-gc.sh`.**
  Both now refuse any `duckdb` that is not the version the data plane links, so neither
  runs until this is done. Replace any per-account copy in `~/.local/bin` the same way.
  (#651)
  ```bash
  ( cd "$(mktemp -d)" \
    && curl -sSfL -O https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-amd64.zip \
    && unzip -q duckdb_cli-linux-amd64.zip && sudo install -m 0755 duckdb /usr/local/bin/duckdb )
  sudo -u qiita-data /usr/local/bin/duckdb --version   # expect v1.5.5
  ```
- `[operator]` **Before the deploy, confirm the live checkm image runs the tool versions
  `checkm.def` now pins.** This deploy rebuilds `long-read-assembly-checkm-1.0.1.sif` (its
  def changed), and the pins are the versions the image built at the 2026-09-15 deploy
  would have resolved — inferred from bioconda's release dates, not read off the host. If
  the output differs, **stop**: the pins must change to the live versions first, or the
  rebuild re-scores MAGs under the same workflow version. (#651)
  ```bash
  sudo -u qiita-orch bash -c 'set -a; . /etc/qiita/compute-orchestrator.env; set +a
  cd /tmp && apptainer exec --no-home "${PATH_DERIVED}/images/long-read-assembly-checkm-1.0.1.sif" \
    ls /opt/conda/envs/checkm/conda-meta' \
    | sed -n -E 's/^(checkm-genome|pplacer|hmmer|prodigal)-([^-]+)-[^-]+\.json$/\1==\2/p' | sort
  # expect exactly: checkm-genome==1.2.5  hmmer==3.4  pplacer==1.1.alpha22  prodigal==2.6.3
  ```

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

- **Both staged miint builds are present, and the rebuilt `long-read-assembly` images carry
  DuckDB 1.5.5** — the assemble and checkm steps load miint only from their own DuckDB
  version's directory, and `v1.5.4/` still serves the frozen 1.0.0 checkm image (see
  Notes). Expect `DUCKDB_155_OK`. (#651)
  ```bash
  sudo -u qiita-orch bash -c 'set -a; . /etc/qiita/compute-orchestrator.env; set +a
  for v in 1.5.5 1.5.4; do
    test -s "$MIINT_EXTENSION_DIRECTORY/v$v/linux_amd64/miint.duckdb_extension" \
      || { echo "no staged miint for DuckDB $v"; exit 1; }
  done
  cd /tmp && for s in long-read-assembly-assemble-1.0.0.sif long-read-assembly-checkm-1.0.1.sif; do
    apptainer exec --no-home "${PATH_DERIVED}/images/$s" \
      python3 -c "import duckdb; print(duckdb.__version__)" | grep -Fxq 1.5.5 \
      || { echo "$s is not on DuckDB 1.5.5"; exit 1; }
  done && echo DUCKDB_155_OK'
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **DuckDB 1.5.4 → 1.5.5 everywhere.** (#651)
  - The redeploy's miint stage re-stages miint and httpfs into
    `MIINT_EXTENSION_DIRECTORY/v1.5.5/` by itself: `stage-miint --check` sees the version
    change, so no `FORCE_STAGE_MIINT`. `make verify-deploy`'s `compute-readiness` and
    `cp-miint` checks LOAD the new build.
  - The `assemble` and `checkm` (`-1.0.1`) SIFs auto-rebuild on deploy to pick up 1.5.5.
    `checkm`'s tools are now pinned, and its build fails if a solve drifts off them.
  - **Work that loads miint between the bucket-4 deploy and the redeploy's miint stage
    (step 5/8) fails at LOAD.** `v1.5.5/` is staged last, after everything that moves to
    1.5.5: step 4 rebuilds the long-read-assembly images and restarts the services, and
    step 5 refreshes the SLURM native venv before it stages. In that window, a
    long-read-assembly ticket (its read export, its assemble and checkm steps) and any
    native job that loads miint find no miint for their version. Resubmit anything that
    failed there.
  - **Keep `MIINT_EXTENSION_DIRECTORY/v1.5.4/`.** `long-read-assembly` 1.0.0's checkm step
    still runs the frozen `long-read-assembly-checkm-1.0.0.sif` (no build spec since 1.0.1),
    on DuckDB 1.5.4, and LOADs miint from there. Staging never removes an old version dir.
  - On first start each data-plane instance installs the 1.5.5 `ducklake` and `postgres`
    extensions under its `HOME` (`/var/lib/qiita-data/<port>`), so it needs to reach
    extensions.duckdb.org, as on every DuckDB bump. DuckLake moves `d318a545` → `d8a1881e`:
    bug fixes, no catalog-schema migration.
  - **A DoGet whose query fails after batches have streamed now ends in an error status**,
    where it used to end like a complete result. A client that reads such a stream to the
    end now raises instead of silently holding a truncated table.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
