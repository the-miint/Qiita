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

- `[admin]` If `getenforce` prints `Enforcing`, let nginx bind its new loopback listener
  (#363) — every deploy now renders it, single instance included, and the stock Rocky 10
  policy does not let nginx bind that port:

  ```bash
  # [admin]
  sudo semanage port -a -t http_port_t -p tcp 50050
  ```

  `make preflight` reports it as `selinux/lb-port` (fails while Enforcing and unlabelled).

### 3. Migrations

- `[operator]` `make migrate` applies `20260929000000_sample_field_widen_fn.sql`, `20260929000001_unique_in_study_propagation_lock.sql` and `20260929000002_metadata_field_contract_error_detail.sql` (all three create or replace functions; no data change). (#628)

- **[operator] Between `make migrate` and the bucket-4 restart, a study-field edit that
  declares a field unique answers 500 (#628).** `20260929000001_unique_in_study_propagation_lock.sql`
  makes the propagation refuse a caller that has set no `lock_timeout`, and the control plane
  still running at that point does not set one — only the build this deploy installs does. The
  window is the gap between the two steps, and nothing has to be done about it beyond not
  reporting the 500 as a regression: `PATCH /api/v1/study/{S}/biosample-field/{F}` (and its
  prep-sample twin) carrying `unique_in_study: true` recovers on the restart, with no partial
  state left behind. That file and `20260929000000_sample_field_widen_fn.sql` only create or
  replace functions, so neither adds a lock window on the metadata tables to size.

### 4. Deploy

- Nothing extra for a single instance (#363): with no `QIITA_DATA_PLANE_*` keys in
  `/etc/qiita/data-plane.env`, the deploy runs the one instance `@50051` as before. To run
  more instances or add data planes on other hosts, see
  [`docs/runbooks/data-plane-scaling.md`](docs/runbooks/data-plane-scaling.md) before
  deploying.

### 5. Verify

_None yet._

- `verify-deploy`'s data-plane rows (#363): `health/data-plane` is now
  `health/data-plane@<port>`, one row per instance; `health/data-plane-peer@<host:port>`
  appears per peer; `health/data-plane-lb` checks nginx's loopback listener
  `127.0.0.1:50050` (skipped when the TLS files are absent); `health/data-plane-upstream`
  fails when verify cannot read the members from the rendered nginx config, and is skipped
  when grpcurl is missing or `SKIP_HEALTH=1`. On an Enforcing host, a red
  `health/data-plane-lb` can mean the bucket-2 port label is missing.

### 6. After the deploy verifies green

- Only on a host running more than one data-plane instance (#363): point the control plane
  at nginx's loopback listener so its calls spread across them —
  [`data-plane-scaling.md` § The control plane through nginx](docs/runbooks/data-plane-scaling.md#the-control-plane-through-nginx).
  Undo it before any rollback to a commit without that listener.

### Notes (no host action)

- **A hand-written `UPDATE ... SET unique_in_study = true`, or a hand-written
  `SELECT qiita.widen_study_field_to_text(...)`, now fails unless the session sets
  `lock_timeout` first (#628).** Both lock the metadata table against concurrent writers
  and refuse an unbounded wait for it, which would queue every metadata write until
  someone noticed. Run `SET LOCAL lock_timeout = '3s';` in the same transaction. The
  deployed API path sets it for itself; the bucket-3 note covers the window before the
  restart in which the running one does not.

- **The study-field edit route accepts a new body key, `data_type` (#628).** Its only
  permitted value is `text`, which redeclares the field and carries its stored values
  into `value_text`. No client has to change, and the route's access bar is unchanged;
  clients that reject unknown response keys are unaffected, since the response shape
  already carried `data_type`.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
