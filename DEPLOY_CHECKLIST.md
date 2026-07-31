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

- (#406) The CP now actually emits INFO to the journal — it was silently dropped before,
  so this is the fix's own smoke test (any INFO line will do):
  ```bash
  sudo journalctl -u qiita-control-plane --since "5 min ago" | grep -m1 'INFO '
  sudo journalctl -u qiita-compute-orchestrator --since "5 min ago" | grep -m1 'INFO '
  ```
- (#406) The fan-out control surface answers, as the operator (needs a system_admin token
  carrying `work_ticket:cancel`). Prints one line per cohort with held or in-flight
  children, or "no active fan-out cohorts":
  ```bash
  qiita-admin fanout list
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#406) **`LOG_LEVEL` is new on both CP and CO but needs no action — and expect more journal volume.** It is optional and defaults to `INFO`, so both units boot without it; it is documented in `.env.control-plane.example` / `.env.compute-orchestrator.example` if you ever want to quiet or widen it (an unknown value fails the boot rather than silently reverting). The reason it matters: neither service configured the root logger at all, so every `_log.info` fell through to Python's WARNING-only fallback and never reached the journal. From this deploy the services narrate normally — pump decisions, dispatch lifecycle, sweeper passes — which is a real increase in journal writes on a busy day. Set `LOG_LEVEL=WARNING` on either service to get the old volume back.
- (#406) **The Authorization-header scrubber was inert in both services and now works.** `install_authorization_scrub()` attaches to the root logger's handlers, and with none configured (above) the loop body never ran — so the filter that rewrites `Bearer <token>` to `Bearer <redacted>` was attached to nothing. It now covers everything propagating to root, including `httpx`. `uvicorn` / `uvicorn.access` keep `propagate=False` and remain outside it, so a bearer token appearing in a uvicorn *access* line would still be unscrubbed; that gap is pre-existing and tracked in #408.
- (#406) **New operator surface for the fan-out throttle: `qiita-admin fanout {list,set,pump}`** (system_admin, reuses the existing `work_ticket:cancel` scope — no new grant to make). `list` shows every cohort's held/running/failed counts, effective cap, and whether it is fail-stopped; `set <kind> <key> --max-inflight N` retunes one cohort at runtime and pumps it immediately, `--clear` reverts it to `FANOUT_MAX_INFLIGHT`; `pump <kind> <key>` re-triggers a stalled cohort without changing its cap. Caps are bounded at 100. **Overrides are in-memory** — a CP restart reverts every cohort to the `FANOUT_MAX_INFLIGHT` default, so re-apply after a restart if a run still needs it. This replaces the old procedure of editing `control-plane.env` and restarting to retune a fan-out, which also triggered an unthrottled resume of every in-flight ticket.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
