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

- [ ] **[operator] Confirm the staged miint extension carries the normalized `read_gff`.**
  The annotation ingest now stores `read_gff`'s `stop_position` verbatim, because
  upstream normalized it to half-open; against an older staged build every annotation
  interval would be one base SHORT, silently. `sudo make redeploy` re-stages
  automatically when it detects a mirror build bump, so this is a check, not a step —
  but it is bypassed by `SKIP_STAGE_MIINT=1` and by running `local-deploy.sh` directly.
  ```bash
  sudo -u qiita-orch env MIINT_EXTENSION_DIRECTORY=<derived>/duckdb-ext \
    <native-python> -m qiita_compute_orchestrator.cli.stage_miint --check \
    && echo "staged build matches the mirror"
  ```
  If it reports work to do, re-stage (`FORCE_STAGE_MIINT=1 sudo make redeploy`, or
  `scripts/stage-miint-extension.sh` per redeploy.md §6) before running any
  `reference load --gff`. (#417)

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **The miint mirror is unpinned and rolling, and this deploy is the first to depend on
  a specific build.** `https://ftp.microbio.me/pub/miint` moved `6ae77c7` → `2b2841e`,
  which changed three behaviours qiita asserts against: `read_gff` normalized to
  half-open, `read_gff` now stops at `##FASTA`, and `COPY … FORMAT BAM` gained a
  defined (name-sorted) `@SQ` order. The code is updated for all three, so it now
  requires `2b2841e` or later — a host pinned to an older staged build will write
  one-base-short annotation intervals with no error. Nothing to do beyond the bucket-5
  check above; noted because it is the first time the staged build's version is
  load-bearing rather than incidental. (#417)

- **`workflows/long-read-assembly/binning.sh` changed, comments only.** Its
  `samtools sort` rationale said the sort could be dropped once miint's `@SQ` gained a
  defined order — which just happened, and the sort is still required (the record side
  is still unsorted). The comment is corrected so the next reader does not act on it.
  No behaviour change; the SIF rebuild at deploy is automatic. (#417)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
