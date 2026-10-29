# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's the changelog, see [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

### 2. One-time host setup

- `[operator]` **Confirm the mirror publishes the DuckDB 2.0.0 miint build** before the
  restart — every component now runs 2.0.0, and the stage step fetches from here.
  (#801)
  ```bash
  curl -fsSI https://ftp.microbio.me/pub/miint/v2.0.0/linux_amd64/miint.duckdb_extension.gz | head -1   # expect 200
  ```
- `[admin]` **Install the DuckDB 2.0.0 CLI for `make lake-shell` / `scripts/lake-gc.sh`.**
  Both refuse any `duckdb` that is not the version the data plane links. The release now
  ships a tarball, not a zip. Replace any per-account copy in `~/.local/bin` the same way.
  (#801)
  ```bash
  ( cd "$(mktemp -d)" \
    && curl -sSfL -O https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-linux-amd64.tar.gz \
    && tar -xzf duckdb-cli-linux-amd64.tar.gz && sudo install -m 0755 duckdb /usr/local/bin/duckdb )
  sudo -u qiita-data /usr/local/bin/duckdb --version   # expect v2.0.0 (Cyanoptera) d54cbbe3df
  ```

### 3. Migrations

_None yet._

### 4. Deploy

- `[admin]` (#767) `activate.sh` now waits, before any restart, for the data plane to go idle (no DoGet stream, DoPut or registration in flight), up to 3900 s, printing `waiting for qiita-data-plane@50051 to go idle …` once a minute; it then restarts whether or not it went idle. `QIITA_DP_IDLE_WAIT_S=<s>` in the deploy's environment changes the bound (0 skips it). The deploy stops before any restart if that value is not whole seconds or the wait cannot query the catalog; `QIITA_DP_IDLE_WAIT_S=0` gets past the second. This deploy still stops the old data-plane binary at once on SIGTERM: the drain ships with the new binary, so from the next deploy the data plane's own restart can take up to the unit's `TimeoutStopSec=3900` too.
- Nothing extra for a single instance (#363): with no `QIITA_DATA_PLANE_*` keys in
  `/etc/qiita/data-plane.env`, the deploy runs the one instance `@50051` as before. To run
  more instances or add data planes on other hosts, see
  [`docs/runbooks/data-plane-scaling.md`](docs/runbooks/data-plane-scaling.md) before
  deploying.

### 5. Verify

- `[admin]` (#767) `systemctl show -p TimeoutStopUSec qiita-data-plane@50051` prints `TimeoutStopUSec=1h 5min`.

- `verify-deploy`'s data-plane rows (#363): `health/data-plane` is now
  `health/data-plane@<port>`, one row per instance; `health/data-plane-peer@<host:port>`
  appears per peer; `health/data-plane-lb` checks nginx's loopback listener
  `127.0.0.1:50050` (skipped when the TLS files are absent); `health/data-plane-upstream`
  fails when verify cannot read the members from the rendered nginx config, and is skipped
  when grpcurl is missing or `SKIP_HEALTH=1`; `data-plane-units/<unit>` fails for a running
  or enabled `qiita-data-plane@` unit whose port is not in `QIITA_DATA_PLANE_PORTS`, and
  the deploy itself stops before installing anything when it finds such a unit, unless
  run with `QIITA_DP_DISABLE_REMOVED=1`, which disables and stops it;
  `commit/data-plane@<port>` and `commit/data-plane-peer@<host:port>` fail for a member
  not running this host's deployed commit (`BUILD_COMMIT` in
  `/opt/qiita/data-plane/build.env`, which the deploy writes). A host running only
  `qiita-data-plane@50051` is unaffected.

- **The staged miint builds are present, and the rebuilt `long-read-assembly` images carry
  DuckDB 2.0.0** — each image loads miint only from its own DuckDB version's directory.
  Expect `DUCKDB_200_OK`. (#801)
  ```bash
  sudo -u qiita-orch bash -c 'set -a; . /etc/qiita/compute-orchestrator.env; set +a
  for v in 2.0.0 1.5.5 1.5.4; do
    test -s "$MIINT_EXTENSION_DIRECTORY/v$v/linux_amd64/miint.duckdb_extension" \
      || { echo "no staged miint for DuckDB $v"; exit 1; }
  done
  cd /tmp && for s in long-read-assembly-assemble-1.0.0.sif long-read-assembly-checkm-1.0.1.sif; do
    apptainer exec --no-home "${PATH_DERIVED}/images/$s" \
      python3 -c "import duckdb; print(duckdb.__version__)" | grep -Fxq 2.0.0 \
      || { echo "$s is not on DuckDB 2.0.0"; exit 1; }
  done && echo DUCKDB_200_OK'
  ```
- **The DuckLake catalog is still format 1.0.** Expect `1.0`; the attach itself fails if
  the catalog was ever migrated. (#801)
  ```bash
  bash scripts/lake-shell.sh --no-cp -noheader -list \
    -c "SELECT value FROM qiita_lake.options() WHERE option_name = 'version'"
  ```

### 6. After the deploy verifies green

- Only on a host running more than one data-plane instance (#363): point the control plane
  at nginx's loopback listener so its calls spread across them —
  [`data-plane-scaling.md` § The control plane through nginx](docs/runbooks/data-plane-scaling.md#the-control-plane-through-nginx).
  Undo it before any rollback to a commit without that listener.

### Notes (no host action)

- (#767) A killed DoPut leaves `staging/uploads/<idx>/upload.parquet.partial`, which nothing reclaims, as nothing reclaims finished uploads. `qiita-admin masked-read-export` gains `--retry-seconds` (default 3900, the data-plane unit's stop timeout).
- **DuckDB 1.5.5 → 2.0.0 everywhere; the DuckLake catalog stays at format 1.0.** (#801)
  - The redeploy re-stages miint and httpfs into `MIINT_EXTENSION_DIRECTORY/v2.0.0/` on its
    own, because `stage-miint --check` sees the version change. `make verify-deploy`'s
    `compute-readiness` and `cp-miint` checks LOAD the new build.
  - The `assemble` and `checkm` (`-1.0.1`) SIFs auto-rebuild on deploy to pick up 2.0.0.
  - **Work that loads miint between the bucket-4 restart and the redeploy's miint stage
    fails at LOAD**: `v2.0.0/` is staged after everything moves to 2.0.0. Resubmit
    anything that failed there; #676 would stage first and close the window.
  - **Keep `v1.5.4/` and `v1.5.5/`.** The frozen `long-read-assembly-checkm-1.0.0.sif` still
    loads miint from `v1.5.4/`, and `v1.5.5/` is what a rollback runs. Staging never
    removes an old version dir.
  - Each data-plane instance installs the 2.0.0 `ducklake` and `postgres` extensions under
    its `HOME` on first start, so it needs to reach extensions.duckdb.org.
  - That ducklake writes catalog format 1.1 by default, so every attach names
    `DUCKLAKE_VERSION '1.0'` and the catalog is not migrated. Never attach it with
    `AUTOMATIC_MIGRATION` by hand: the migration is one-way. A rollback to the 1.5.5 build
    still reads everything 2.0 wrote to the catalog.
  - `host_filter` can count a read pair as host that it did not before: miint's 2.0
    build maps a pair under the `sr` preset as native minimap2 does
    (duckdb-miint#341).

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
