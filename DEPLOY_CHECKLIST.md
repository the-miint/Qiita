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

_None yet._

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

### 6. After the deploy verifies green

- Only on a host running more than one data-plane instance (#363): point the control plane
  at nginx's loopback listener so its calls spread across them —
  [`data-plane-scaling.md` § The control plane through nginx](docs/runbooks/data-plane-scaling.md#the-control-plane-through-nginx).
  Undo it before any rollback to a commit without that listener.

### Notes (no host action)

- (#767) A killed DoPut leaves `staging/uploads/<idx>/upload.parquet.partial`, which nothing reclaims, as nothing reclaims finished uploads. `qiita-admin masked-read-export` gains `--retry-seconds` (default 3900, the data-plane unit's stop timeout).

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
