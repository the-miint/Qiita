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

- **read-mask synced with lima's BAM input (#313).** The generic `qiita.action` list is
  already covered by `make verify-deploy`; this asserts the re-`sync` actually took (a
  stale `lima_in_fastq` here means lima still gets a FASTQ and never finishes). The SQL
  deliberately carries no quotes — grep does the filtering, so this survives a
  copy-paste through `sudo -u ... bash -c`:
  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a; psql "$DATABASE_URL" -Atc "SELECT action_id, version, steps FROM qiita.action"' \
    | grep read-mask | grep -q lima_in_bam \
    && echo "OK: lima takes lima_in_bam" || echo "FAIL: read-mask not synced (still lima_in_fastq?)"
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **The read-mask `lima` SIF auto-rebuilds on this deploy (#313).** `lima.sh` changed
  (it now hands lima a `.bam` instead of a `.fastq`), so `build-sifs.sh`'s
  build-inputs content hash rebuilds the image before the restart. No manual step.
- **`lima_export` needs a miint build with `COPY … (FORMAT UBAM)` (#313).** Shipped
  in duckdb-miint#157 (mirror build `5509321`). The deploy stages miint
  (`stage-miint-extension.sh`), so it arrives with the mirror — no separate operator
  step. If the staged build predates it, the read-mask lima chain FAILS LOUD (a raw
  DuckDB "FORMAT UBAM does not exist" error at the export step) rather than corrupting
  a mask, so a stale stage is caught, not silent. Re-run the stage step on deploy to
  be sure. No new runtime Python dependency.
- **The parked pool-25016 read-mask tickets can be redriven once this is green (#313).**
  They were cancelled because lima could not finish; a redrive before this deploy hits
  the identical wall. Their ~33 GB `lima_export` FASTQs under
  `<scratch>/ticket/48{35..60}/` are dead weight once the replacements complete.
- **Fan-out dispatch is now throttled — no required host action (#329).** The new
  `work_ticket.dispatch_held` column is a plain additive migration, so the standard
  `make migrate` (bucket 3 gate) applies it with no out-of-band setup. `FANOUT_MAX_INFLIGHT`
  (optional, default 8) caps concurrent fan-out children per cohort; add it to
  `control-plane.env` only to override the default — raise once the data plane's headroom
  is known, lower if it is fd/memory-constrained. A missing var uses the default (it is
  NOT `from_env()` fail-fast), so nothing breaks if it is left unset.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
