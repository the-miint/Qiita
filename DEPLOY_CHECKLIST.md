# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's the changelog, see [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- `[operator]` (#NNN) `data-plane.env` gains five optional vars, all documented in
  `.env.data-plane.example`: `DUCKLAKE_POOL_SIZE`, `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`,
  `DUCKDB_TEMP_DIRECTORY`, `DUCKDB_MAX_TEMP_DIRECTORY_SIZE`. Left unset the data plane derives
  all of them at boot and starts, so no edit is required. The pool size is worth setting
  before the restart, because it is both the number of requests one instance serves at once
  and a catalog-connection budget: each attached connection holds one
  lake-Postgres backend while it lives, against `max_connections = 100` (3 superuser-reserved)
  shared with the control plane and every other instance. The default is 8 per instance.

### 2. One-time host setup

_None yet._

### 3. Migrations

- `[operator]` (#685) `make migrate` applies `20261009000000_work_ticket_follow_on.sql` (three nullable columns on `qiita.work_ticket`, two CHECKs, one partial index; no backfill).

### 4. Deploy

_None yet._

### 5. Verify

- `[operator]` (#NNN) `journalctl -u qiita-data-plane@50051 -n 50` carries one
  `qiita-data-plane pool:` line per instance at boot, naming the pool size, per-connection
  `threads` and `memory_limit`, the ceiling they were divided from and **which detector
  answered**. On this host it should read the ceiling from `/proc/meminfo MemAvailable` (no
  cgroup limit is set on the unit); `fallback default` means detection found nothing and the
  pool sized itself against 8 GiB.
- `[operator]` (#NNN) the same journal should carry no `discarding a pooled DuckLake
  connection` lines. One per request means every return is failing its reset, which costs an
  attach per request (measured: 84–107 ms and ~16 catalog transactions, against 3.0–3.4 ms and
  ~3.7 on a reused one) and shows up only as slowness.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#NNN) Past the pool size, the data plane answers gRPC `RESOURCE_EXHAUSTED` rather than
  serving the request. The control plane classifies that as retriable, so a pre-loop resolver's
  ticket is held for redrive rather than failed permanently. **Compute-side readers do not
  retry a refusal yet** — the client half is tracked separately — so a fan-out wider than the
  pool turns into held tickets rather than slow ones. The Flight edge admits 64 concurrent
  requests per client address, which is the number to weigh the pool size against.
- (#NNN) Per-request wall time rises with the division. A request used to take every core;
  it now takes `threads`, and measured on this host one masked read fitted
  `25.3 s + 611.8 s / threads` — so at 48 cores and the default pool of 8 it is ~127 s where
  it was ~38 s. Concurrency is what the change buys back.

- (#685) Work-ticket responses (`GET /work-ticket/{idx}` and the ticket lists) gain `on_success`, `follow_on_work_ticket_idx` and `follow_on_error` (all null for a ticket with no follow-on); clients validating that shape strictly must accept them.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
