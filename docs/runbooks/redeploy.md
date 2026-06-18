# Redeploy (incremental) runbook

**Purpose.** Operator runbook for deploying changes onto an
**already-running** host — the incremental counterpart to
[`first-deploy.md`](first-deploy.md). Use this when `main` (plus any
just-merged PRs) is ahead of what the host is running and you want to
roll all of it out in one go. This runbook is the **single source of
truth for the deploy procedure**; `DEPLOY_CHECKLIST.md` and `CLAUDE.md` point
here rather than restating the lifecycle.

**The model in one line** (the *why* lives in CLAUDE.md "Deployments"):
you do **not** assemble the deploy yourself — the `## Pending deploy`
section of `DEPLOY_CHECKLIST.md` is already the consolidated, ordered,
deduplicated checklist of everything merged but not yet deployed, folded
in by each PR as it merged. Your job is to run that checklist, then
archive it.

This runbook is the *fixed skeleton* (the order and the invariants); the
*variable* per-deploy steps live in `## Pending deploy`.

Account labels (`[operator]`, `[admin]`) mean exactly what they mean in
[`first-deploy.md`](first-deploy.md#account-model); see there for the
privilege model. Substitute your host's FQDN for `QIITA_HOSTNAME`
throughout (the live deploy is `qiita-miint.ucsd.edu`).

---

## 0. Fast path: one command (recommended)

On an established host the whole skeleton below runs from a **single command
on your `[admin]` account**:

```bash
# [admin] root-run; sudo -u's into the operator (qiita) for pull/migrate and
#         into qiita-api/qiita-orch for the verify checks. Substitute the clone
#         path + FQDN for your host.
sudo make -C /home/qiita/qiita-miint redeploy QIITA_HOSTNAME=qiita-miint.ucsd.edu
```

`deploy/redeploy.sh` drives steps 2–7 in order: pull (as the operator) →
print buckets 1 & 2 and pause for you to apply them (only when they hold real
steps — see below) → `preflight` → migration gate → `local-deploy.sh` → native
venv refresh → miint stage → `verify`, then prints the deployed commit for the
§8 archive hand-off. Key behaviours:

- It **reads `DATABASE_URL` from `control-plane.env` itself** (it is root) and
  hands it to the operator's `make migrate`, so the operator's shell needn't
  have it and you migrate exactly the DB `activate.sh`'s guard checks.
- The migration gate stays **out-of-band**: it *refuses* if anything is
  pending unless you pass `RUN_MIGRATE=1` (which applies after a typed
  confirm — never silently). `activate.sh`'s guard is the backstop.
- **It only stops to ask when there is real work or a real decision** — it does
  not pause on no-ops, and it does not prompt to do work it has already proven is
  needed. The buckets 1 & 2 acknowledgement is skipped when both are empty in
  `DEPLOY_CHECKLIST.md` (nothing to apply out-of-band → nothing to confirm). The
  native-venv refresh (§6) is skipped — no prompt, no `uv sync` — when provably
  already current (the native checkout is the clone just pulled, neither
  `qiita-common` nor `qiita-compute-orchestrator` changed in that pull, and the
  existing venv still imports); when a refresh **is** needed on that same clone it
  now runs automatically (no confirm), prompting only for a *separate* native
  checkout it didn't pull. miint staging is gated the same way: a
  `stage-miint --check` probe skips it when the staged build still matches the
  mirror and stages automatically otherwise — so the two prompts that used to
  fire on every deploy are gone.
- `ASSUME_YES=1` skips the interactive acks (automation); `SKIP_STAGE_MIINT=1`
  skips the miint stage; `FORCE_STAGE_MIINT=1` forces it even when `--check` says
  it is current (use after a mirror bump the `HEAD` can't see, or to recover a
  partial stage); `SKIP_NATIVE_REFRESH=1` skips the native-venv refresh;
  `FORCE_NATIVE_REFRESH=1` forces it even when the "already current" skip would
  fire (use to recover a deploy interrupted mid-`uv sync`).

This **root-run, drop-into-each-account** model is why it works where the
operator account has **no sudo** (the documented default — see
[`first-deploy.md`](first-deploy.md#account-model)); it mirrors how
`local-deploy.sh` already runs as root and `sudo -u qiita` for the build. The
manual steps below remain the source of truth for *what* each step does and
are your fallback when you want to drive one by hand (e.g. resolving a
migration pre-check that the gate surfaces).

---

## 1. Read the Pending-deploy checklist

```bash
# [operator]
git -C ~/qiita-miint fetch origin
sed -n '/^## Pending deploy/,/^## Deployed history/p' ~/qiita-miint/DEPLOY_CHECKLIST.md
```

That section is your deploy, in five ordered buckets: **1. Env vars**,
**2. One-time host setup**, **3. Migrations**, **4. Deploy**,
**5. Verify**, plus **Notes** (no host action). The bucket order *is*
the dependency order — buckets 1–3 must complete before the bucket-4
restart. Steps 2–6 below are those buckets with the surrounding
mechanics; if the checklist and this runbook ever disagree on order,
the runbook's order wins (it encodes the invariants).

## 2. Pull source onto the host

```bash
# [operator] fast-forward the clone so migration files + workflows exist locally
git -C ~/qiita-miint pull --ff-only
```

We pull here (not via `local-deploy.sh`'s own pull) so the migration
files are present for the migrate step *before* the deploy script runs.
The deploy step therefore runs with `SKIP_PULL=1`.

## 3. Apply env-var (bucket 1) and one-time host setup (bucket 2)

Run buckets 1 and 2 of the Pending-deploy checklist verbatim. Env vars
first; everything `from_env()` requires must be in place before the
deploy-time restart, or the affected unit won't come back up. The actual
commands (and which `<scratch>`/FQDN values to substitute) are in the
checklist — copy/paste from there.

Then confirm the config/secret files are mutually consistent **before** the
restart — this catches the silent runtime failures (`PATH_SCRATCH` drift across
the three env files, `HMAC_SECRET_KEY` mismatch between CP and DP, a missing or
mis-permed token file) up front rather than at first request:

```bash
# [admin] read-only; prints non-secret fingerprints, never the values
sudo make -C ~/qiita-miint preflight
```

## 4. Apply migrations

> Doing the §0 fast path? Skip this — `redeploy.sh` runs the gate for you,
> reading `DATABASE_URL` from `control-plane.env` (it is root) and handing it to
> the operator's `make migrate`, so you need neither the ACL nor `DATABASE_URL`
> in your shell. The manual route below is for driving the step by hand.

```bash
# [operator] DATABASE_URL must be in your shell, pointing at the SAME DB as
#            control-plane.env (the guard, running as root, checks that one).
#            With the operator config-read ACL in place (first-deploy.md §0.1),
#            just source it:  set -a; . /etc/qiita/control-plane.env; set +a
#            Otherwise use the value from your provisioning / first deploy
#            (first-deploy.md step 1). A DATABASE_URL pointing elsewhere migrates
#            one DB while the guard checks another — the guard's "wrong-DB" hint
#            flags it.
make -C ~/qiita-miint migrate
```

`make migrate` runs `dbmate up` and is idempotent — already-applied
migrations are skipped. **This is a separate step on purpose:**
`local-deploy.sh` / `activate.sh` do not apply migrations (auto-applying
is unsafe for expand/contract changes). The deploy's migration guard
*does* refuse to restart services if any shipped migration is unapplied,
so forgetting this step fails the deploy loudly instead of producing
runtime 500s — but run it here anyway.

That guard queries the DB with `psql` (sourcing `DATABASE_URL` from
`control-plane.env` as root). On the rare host that has `dbmate` but no
`psql` client it refuses to proceed; install the postgres client, or —
having confirmed migrations are applied — re-run the deploy with
`SKIP_MIGRATION_GUARD=1`.

Verify nothing is pending (this is a manual pre-check; `activate.sh` runs the
same `public.schema_migrations` assertion automatically and aborts the deploy
before any restart if a migration is missing):

```bash
# [operator]
cd ~/qiita-miint/qiita-control-plane && \
  ~/.local/bin/dbmate --migrations-table public.schema_migrations status
```

## 5. Expand/contract migrations: mind the ordering

If the checklist contains a rename / drop / type-change split across two
migrations (expand then contract — see CLAUDE.md "Database migrations"),
only the **expand** half should be deployed alongside code that still
reads the old shape. Do not deploy a contract migration in the same
round as the code that stops using the old column unless every running
instance is already on the new code. For the single-host deploy this is
usually fine in one round; call it out if in doubt.

## 6. Run the deploy

```bash
# [admin]
sudo SKIP_PULL=1 QIITA_HOSTNAME=qiita-miint.ucsd.edu \
  /home/qiita/qiita-miint/deploy/local-deploy.sh
```

This builds the data-plane binary, rsyncs all four components +
`workflows/` into `/opt/qiita/incoming/`, then exec's `activate.sh`,
which: stages into `/opt/qiita/`, `uv sync`s the Python venvs
(`--reinstall-package qiita-common` to defeat cross-package staleness),
**asserts every shipped migration is applied and aborts if not**, runs
`qiita-admin actions sync` (picks up new/changed workflow YAML),
installs systemd units + dropins, and restarts the services whose env
files are present.

If the migration guard aborts here, you skipped or under-ran the migrate
step above — run `make migrate` and re-run this command.

> **The SLURM native compute env is refreshed separately from the service
> venvs — but `redeploy.sh` step 5 now does it for you.** `local-deploy.sh`
> only `uv sync`s the `/opt/qiita` *service* venvs. Native SLURM jobs run from
> the venv `SLURM_NATIVE_PYTHON` points at — a separate clone on the shared
> filesystem the compute nodes mount (e.g.
> `/home/qiita/qiita-miint/qiita-compute-orchestrator`). On every deploy that
> changes `qiita-common` or `qiita-compute-orchestrator`, that venv must be
> refreshed too, or a native job imports stale code (and can keep a stale cached
> miint extension) — issue #106.
>
> `sudo make redeploy` automates this (step 5, before the miint stage): it
> derives the checkout from `SLURM_NATIVE_PYTHON`, runs `uv sync
> --reinstall-package qiita-common` **as the checkout owner `qiita`** (never
> root — a root-owned `.venv` is the #80 footgun), and fails loud if the synced
> venv can't import. It skips cleanly when `SLURM_NATIVE_PYTHON` is unset (local
> backend), and `SKIP_NATIVE_REFRESH=1` opts out. It also **skips the refresh
> entirely — no prompt, no `uv sync`** — when it can prove the venv is already
> current: the native checkout is the clone this run just pulled, that pull
> changed neither `qiita-common` nor `qiita-compute-orchestrator`, and the
> existing venv still imports. When it can't prove that (a separate native
> checkout, an actual code change, or a failing import probe) it refreshes — and
> on that same clone it does so **automatically, without a prompt** (the refresh
> is unambiguously needed; a confirm only appears for a *separate* checkout it
> didn't pull). So the optimisation never skips a refresh a code change requires,
> and never stops to ask you to approve necessary work. The one case it can't see
> is a prior run that died **mid-`uv sync`**: a re-run sees "nothing pulled" and a
> partial venv that may still import, so it would skip. After an interrupted
> deploy, re-run with `FORCE_NATIVE_REFRESH=1` (or refresh by hand per the
> fallback below) to force the resync — this keeps the "re-run after a failed
> deploy is safe" property intact.
>
> **miint staging is gated the same way.** Before staging, `redeploy.sh` runs
> `stage-miint --check` (as `qiita-orch`, same interpreter + env): it compares the
> staged build's fingerprint marker against what the mirror serves now (DuckDB
> version + platform locally; a `HEAD` on the extension URL for a same-version
> mirror bump) and **skips staging when they match, stages automatically when they
> don't** — no prompt either way. Any uncertainty (no marker, network failure, a
> mirror that returns no `ETag`/`Last-Modified`) re-stages, never skips on doubt.
> `FORCE_STAGE_MIINT=1` stages unconditionally — use it after a mirror bump the
> `HEAD` can't see, or to recover a partial stage; `SKIP_STAGE_MIINT=1` skips
> entirely.
>
> If you run `local-deploy.sh` directly (not via `redeploy.sh`), or skipped the
> step, refresh it by hand:
> ```bash
> # [operator] in the SLURM_NATIVE_PYTHON checkout, on the shared FS
> cd <native-checkout>/qiita-compute-orchestrator && uv sync --reinstall-package qiita-common
> ```
> The next native job FORCE-installs miint from the mirror, overwriting any
> stale cached extension. Bucket-5 `compute-readiness` confirms both
> (`probe/native-import=ok`, `probe/miint-read-fastx=ok`).

## 7. Verify

```bash
# [admin] one command runs the three generic post-deploy checks with the
#         correct run-as baked in for each — health aggregate, workflow
#         actions list, and compute-readiness. Use this; do NOT hand-copy
#         the individual invocations (that is how the compute-readiness
#         run-as bug recurred every deploy — see below).
sudo make -C ~/qiita-miint verify-deploy QIITA_HOSTNAME=qiita-miint.ucsd.edu
```

`make verify-deploy` (via `deploy/verify.sh`) runs, each with the account +
env file it actually needs:

- **health** — `curl -fsS https://$QIITA_HOSTNAME/health` (CP+CO+DP aggregate
  + per-service pills), falling back to the localhost checks if TLS isn't up;
- **workflow actions** — as `qiita-api` sourcing `control-plane.env`,
  `SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;`;
- **compute-readiness** — as **`qiita-orch`** sourcing
  **`compute-orchestrator.env`** (NOT `qiita-api`/`control-plane.env`): the
  command subprocesses into the orchestrator venv, reads `COMPUTE_BACKEND` /
  `SLURM*` from that env, and reads the `0400 qiita-orch` `co-to-cp.token` —
  none of which `qiita-api` can reach.

If you need to run compute-readiness by hand (e.g. `make` is unavailable), the
**correct** form is — matching [`first-deploy.md`](first-deploy.md) §10d:

```bash
# [admin] primary: qiita-admin on PATH for qiita-orch
sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; \
  /home/qiita/.local/bin/qiita-admin compute-readiness'

# [admin] PATH-independent fallback: the module the wrapper subprocesses into
sudo -u qiita-orch bash -c 'set -a; source /etc/qiita/compute-orchestrator.env; set +a; \
  /opt/qiita/compute-orchestrator/.venv/bin/python -m qiita_compute_orchestrator.cli.compute_readiness'
```

Run every check in the Pending-deploy bucket-5 list (those are the
deploy-*specific* asserts on top of `make verify-deploy`), plus anything the
Notes bucket flags for downstream clients. When it passes, capture the
deployed commit and hand it off for archiving:

```bash
# [operator] the commit now running on the host — report this for step 8
git -C ~/qiita-miint rev-parse HEAD
```

## 8. Archive the deploy (maintainer, off-host)

The deploy host has no Claude and the operator doesn't edit the repo, so
archiving is **not** an on-host step — it's a repo edit done by a
maintainer on their own machine *after* the operator confirms success.
The operator reports two things: (a) verification passed, (b) the
deployed commit from step 7.

A maintainer then, in a local checkout, runs `/deploy-archive <sha>`
(passing the operator-reported commit — not the maintainer's local
`HEAD`, which may have moved on). It moves the just-deployed `## Pending
deploy` block into `## Deployed history` stamped with the date + that
commit, resets Pending to empty, and the maintainer commits + pushes.
No Claude? Do the same move by hand following the shape in
`DEPLOY_CHECKLIST.md`. Either way, also record the deployed commit somewhere
durable (deploy log, ops channel).
