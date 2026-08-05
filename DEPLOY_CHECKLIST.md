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

- **`DATA_PLANE_URL` for the control plane** — point it at the new loopback gRPC balancer so CP-side Flight traffic spreads across data-plane instances instead of pinning to instance #1. Not fail-fast (unset falls back to `grpc://localhost:50051`, so the unit still boots) — but leaving it unset means horizontal scaling has no effect on CP traffic. Compute nodes are unaffected: `compute-orchestrator.env` already points at `grpc+tls://<fqdn>:443`, which nginx balances. (#363)

  ```bash
  # [admin]
  grep -q '^DATA_PLANE_URL=' /etc/qiita/control-plane.env \
    || sudo bash -c 'echo "DATA_PLANE_URL=grpc://127.0.0.1:50050" >> /etc/qiita/control-plane.env'
  ```

### 2. One-time host setup

_None yet._

### 3. Migrations

- `make migrate` applies `20260802000000_work_ticket_escalated_resource_floor.sql`, no
  out-of-band setup: an additive `ALTER TABLE qiita.work_ticket ADD COLUMN
  escalated_resource_floor JSONB`, plus a shape CHECK (`NULL` or a JSON object) and the
  column's `COMMENT ON`. Nullable, backfill-free — every existing row reads NULL, which
  the runner treats as "nothing escalated yet". Carries the per-step memory/walltime floor
  the OOM/TIMEOUT retry ladder climbs to, so a CP restart or a `/run` redrive continues
  the ladder instead of restarting at the YAML baseline. (#415)

### 4. Deploy

- **Optional — scale the data plane out on this host.** The local instance set is one knob, `QIITA_DATA_PLANE_PORTS` (default `50051`), read by `deploy/activate.sh` to render the nginx upstream AND to enable/restart the matching `qiita-data-plane@NNNN` units. Previously the upstream was a checked-in literal that `activate.sh` overwrote on every deploy and the restart list was hardcoded to `@50051`, so a hand-added instance lost its upstream entry at the next deploy and never restarted onto new code. Pass it through `sudo -E` so it survives into the privileged half. Deploying without it is a no-op (single instance, as today). (#363)

  ```bash
  # [admin] — one instance per ~core you want to give the data plane
  sudo -E env QIITA_DATA_PLANE_PORTS="50051 50052 50053" make redeploy QIITA_HOSTNAME=qiita-miint.ucsd.edu
  ```

  The unit is a template whose instance specifier IS the listen port, so no new unit files are needed. `activate.sh` `systemctl enable`s each instance, so added ones also survive a reboot.

  **Scaling back DOWN is not automatic.** `activate.sh` only enables/restarts the ports in the list; it never disables one you removed. Drop an instance by hand after redeploying with the shorter list, or it keeps running (out of the nginx upstream, but still bound and holding a DuckLake connection):

  ```bash
  # [admin]
  sudo systemctl disable --now qiita-data-plane@50053
  ```

- **Optional — scale the data plane across HOSTS.** Extra processes on one box share its cores, memory, and NIC, so they stop helping once the box saturates; `QIITA_DATA_PLANE_PEERS` (space-separated `host:port`, empty by default) adds data planes running on *other* hosts to the same nginx upstream. Local instances and peers are separate knobs because only the local list drives systemd here — a peer is another host's deploy, and this one neither starts, restarts, nor upgrades it. **You must deploy the peer host yourself** (same checkout, its own `QIITA_DATA_PLANE_PORTS`, and an env pointing at the SAME Postgres/DuckLake catalog and the same `PATH_SCRATCH`/`PATH_PERSISTENT` — a data plane serving a different lake would return wrong data, not an error). (#363)

  ```bash
  # [admin] — three local instances plus two on a second host
  sudo -E env QIITA_DATA_PLANE_PORTS="50051 50052 50053" \
             QIITA_DATA_PLANE_PEERS="dp2.internal:50051 dp2.internal:50052" \
             make redeploy QIITA_HOSTNAME=qiita-miint.ucsd.edu
  ```

  ⚠️ **Peer traffic is plaintext gRPC.** The upstream is consumed by `grpc_pass grpc://` and the scheme is per-`grpc_pass`, not per-member, so a peer cannot use TLS while the loopback members stay plaintext. Only add a peer reachable over a network you control (private VLAN / VPC / WireGuard) — never across the public internet. Flight tickets stay Ed25519-signed, so a peer cannot be fed forged identifiers, but the payload on the wire is unencrypted.

  A peer hostname that does not resolve aborts the deploy **before** any service restarts, rather than at the `nginx -t` that runs after them. (Skipped where `getent` is unavailable, e.g. a macOS dev box.)

  **Removing a peer IS automatic**, unlike removing a local instance: drop it from `QIITA_DATA_PLANE_PEERS` and redeploy, and it leaves the upstream at the next render — there is no unit here to disable. Stopping the peer host's own instances is that host's deploy.

### 5. Verify

- **Per-member data-plane health + the balancer.** `make verify-deploy` now health-checks every upstream member individually — local instances *and* remote peers — plus `localhost:50050` (nginx → the pool). Checking only `:50051` would have reported a healthy data plane while a scaled-out instance was down, with nginx still routing a share of every job's traffic into it. **You do not need to re-export anything:** the member list is read from the rendered `/etc/nginx/conf.d/qiita.conf`, i.e. what nginx is actually serving, so a forgotten export cannot produce a green run that checked nothing. (It falls back to the env lists only when no config is rendered yet, as on a first deploy.) (#363)

  ```bash
  # [admin] — no QIITA_DATA_PLANE_* needed; the member list comes from the rendered nginx config
  sudo make verify-deploy QIITA_HOSTNAME=qiita-miint.ucsd.edu
  ```

  Each peer gets its own `health/data-plane-peer@<host:port>` row; local instances stay `health/data-plane@<port>`. A peer that is down fails the check rather than hiding behind the local instances, since nginx routes a share of every job's traffic into it either way.

- (#420, closes #411) Confirm the six re-sized actions carry their raised ceilings.
  `actions sync` runs inside `activate.sh`, so this only checks it took — if a row still
  reads its old value, an OOM or TIMEOUT on that workflow's heaviest step keeps failing
  permanently on attempt 0 (a baseline at its ceiling has no rung to climb to) and the
  change is a silent no-op:

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, version, mem_ceiling_gb, walltime_ceiling
    FROM qiita.action
    WHERE (action_id, version) IN
      (('bcl-convert','1.0.0'),('fastq-to-parquet','1.1.0'),('fastq-to-parquet','1.2.0'),
       ('fastq-to-parquet','1.3.0'),('read-mask','1.0.0'),('read-mask-block','1.0.0'))
    ORDER BY action_id, version"
  ```

  Expect (was → is): `bcl-convert|1.0.0|500|1 day` (was `480|12:00:00`),
  `fastq-to-parquet|1.1.0|32|08:00:00` and `|1.2.0|32|08:00:00` (both were `16|04:00:00`),
  `fastq-to-parquet|1.3.0|64|08:00:00`, `read-mask|1.0.0|64|08:00:00` and
  `read-mask-block|1.0.0|64|08:00:00` (all three were `32|08:00:00`).
- (#406) Both services now actually emit INFO to the journal — it was silently dropped
  before, so this is the fix's own smoke test. Each service logs one unconditional line
  at boot; expect exactly those two, naming the resolved log level:
  ```bash
  sudo journalctl -u qiita-control-plane --since "5 min ago" | grep -m1 'INFO qiita_control_plane'
  sudo journalctl -u qiita-compute-orchestrator --since "5 min ago" | grep -m1 'INFO qiita_compute_orchestrator'
  ```
  Match the **dotted logger name**, not a bare `INFO`. uvicorn writes its own
  `INFO:` + padding lines to the journal whether or not app logging came up, so a
  looser pattern passes on a service still carrying the bug — and a bare `INFO ` (with
  the trailing space) matches neither uvicorn nor a pre-fix service, so it fails on a
  healthy deploy too. Only the `<LEVEL> <logger.name>:` shape is unique to a configured
  root logger. If either prints nothing, app logging did not come up: check `LOG_LEVEL`
  on that unit (an unknown value fails the boot outright) before looking further.
- (#406) The fan-out control surface answers, as the operator (needs a system_admin token
  carrying `work_ticket:cancel`). Prints one line per cohort with held or in-flight
  children — plus any cohort still carrying a runtime override — or "no fan-out cohorts
  in flight, and no overrides set". On a quiet deploy the last is the expected answer:
  ```bash
  qiita-admin fanout list
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- (#406) **`LOG_LEVEL` is new on both CP and CO but needs no action — and expect more journal volume.** It is optional and defaults to `INFO`, so both units boot without it; it is documented in `.env.control-plane.example` / `.env.compute-orchestrator.example` if you ever want to quiet or widen it (an unknown value fails the boot rather than silently reverting). The reason it matters: neither service configured the root logger at all, so every `_log.info` fell through to Python's WARNING-only fallback and never reached the journal. From this deploy the services narrate normally — pump decisions, dispatch lifecycle, sweeper passes — which is a real increase in journal writes on a busy day. Set `LOG_LEVEL=WARNING` on either service to get the old volume back.
- (#406) **The Authorization-header scrubber was inert in both services and now works.** `install_authorization_scrub()` attaches to the root logger's handlers, and with none configured (above) the loop body never ran — so the filter that rewrites `Bearer <token>` to `Bearer <redacted>` was attached to nothing. It now covers everything propagating to root, including `httpx`. `uvicorn` / `uvicorn.access` keep `propagate=False` and remain outside it, so a bearer token appearing in a uvicorn *access* line would still be unscrubbed; that gap is pre-existing and tracked in #408.
- (#406) **New operator surface for the fan-out throttle: `qiita-admin fanout {list,set,pump}`** (system_admin, reuses the existing `work_ticket:cancel` scope — no new grant to make). `list` shows every cohort's held/running/failed counts, effective cap, and whether it is fail-stopped; `set <kind> <key> --max-inflight N` retunes one cohort at runtime and pumps it immediately, `--clear` reverts it to `FANOUT_MAX_INFLIGHT`; `pump <kind> <key>` re-triggers a stalled cohort without changing its cap. Caps are bounded at 100. `list` also shows any cohort that has fully drained but still carries an override, flagged as clearable — an override never expires on its own and reapplies if that cohort is re-run. This replaces the old procedure of editing `control-plane.env` and restarting to retune a fan-out, which also triggered an unthrottled resume of every in-flight ticket.
- (#406) **Overrides are in-memory, and a restart undoes a LOWERED cap in the dangerous direction.** A CP restart drops every override and reverts each cohort to `FANOUT_MAX_INFLIGHT`. If you *raised* a cap, that revert is conservative — fewer in flight than you asked for, and the fan-out just drains slower. If you *lowered* one (throttling a cohort that was hurting the data plane), the revert goes the other way: startup reconcile re-pumps the cohort at the default, so a cap of 2 comes back as 8. **Re-apply any lowering after a restart**, and note the units are `Restart=on-failure`, so this can happen without you initiating it — including a crash during the incident you were throttling. The CP logs a WARNING naming the cohort and both numbers whenever it records a lowering, so `journalctl -u qiita-control-plane | grep 'BELOW the FANOUT_MAX_INFLIGHT'` tells you what to re-apply.
- (#426) **Every read mask re-mints after this deploy: `params_hash` changes for all of
  them.** rype's host-call threshold moved (0.0 → 0.05) and now participates in the
  mask identity, so an otherwise-identical filtering config mints a *new* `mask_idx`
  instead of resolving the existing one. Existing `mask_definition` rows stay valid and
  referenced, and existing `read_mask` data is untouched — no migration, no backfill;
  the `params` JSONB simply gains a `resolved_host_filter` key on new rows. Nothing
  re-masks on its own.
- (#426) **A consequence worth knowing before re-planning a pool:** the per-`(mask_idx,
  prep_sample)` gate is what refuses to re-plan an already-masked sample, so under the
  new identity those samples are eligible again and a re-plan will genuinely re-run
  QC + host filtering for them (that is the point — their stored mask was built at the
  old threshold). Expect the compute, and re-plan deliberately rather than by habit.
  To see which masks are old-threshold: `SELECT mask_idx, params->'resolved_host_filter'
  FROM qiita.mask_definition ORDER BY mask_idx;` — `null` means minted before this
  deploy.
- After this deploy, a ticket whose step OOM-kills or times out keeps its escalated size
  across a CP restart and a `/run` redrive — the previous behaviour re-burned one failing
  attempt per affected step climbing back (observed on `long-read-assembly` 6978 / 6980 /
  6989: `assemble` back at 192 GB after redriving from 384 GB). To put a redriven ticket
  back on its YAML baseline (e.g. after correcting an oversized input), NULL the column
  first: `UPDATE qiita.work_ticket SET escalated_resource_floor = NULL WHERE
  work_ticket_idx = <idx>;` — SQL `NULL`, not `'null'::jsonb`, which the CHECK rejects
  precisely because the runner would read it as "nothing escalated yet". (#415)
- (#420, closes #411) **Six workflows' `action_ceiling` was raised so an OOM/TIMEOUT can
  actually retry; no step baseline changed, so nothing schedules differently.**
  `bcl-convert/1.0.0`, `fastq-to-parquet/1.1.0`, `1.2.0`, `1.3.0`, `read-mask/1.0.0` and
  `read-mask-block/1.0.0` each had a ceiling equal to their heaviest step's baseline, which
  makes escalation clamp on the first failure and fail the ticket permanently at
  `retry_count=0` — the shape behind the `long-read-assembly` incident (#393). Only a
  *failing* step now climbs; a healthy ticket requests exactly what it did before. Reaches
  `qiita.action` via `qiita-admin actions sync` inside `activate.sh` — no host action, just
  the bucket-5 checks that it took.
- (#420, closes #411) **The admin `resource_override` envelope widens with these ceilings**
  (`POST /work-ticket` 422s when `resource_override.mem_gb` exceeds `mem_ceiling_gb`). A
  per-ticket nudge that used to be rejected at 17 GB on `fastq-to-parquet/1.1.0` is now
  accepted up to 32. Nothing to do — noted so the wider envelope isn't a surprise.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
