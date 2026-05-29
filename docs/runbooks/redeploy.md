# Redeploy (incremental) runbook

**Purpose.** Operator runbook for deploying changes onto an
**already-running** host — the incremental counterpart to
[`first-deploy.md`](first-deploy.md). Use this when `main` (plus any
just-merged PRs) is ahead of what the host is running and you want to
roll all of it out in one go. This runbook is the **single source of
truth for the deploy procedure**; `CHANGELOG.md` and `CLAUDE.md` point
here rather than restating the lifecycle.

**The model in one line** (the *why* lives in CLAUDE.md "Deployments"):
you do **not** assemble the deploy yourself — the `## Pending deploy`
section of `CHANGELOG.md` is already the consolidated, ordered,
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

## 1. Read the Pending-deploy checklist

```bash
# [operator]
git -C ~/qiita-miint fetch origin
sed -n '/^## Pending deploy/,/^## Deployed history/p' ~/qiita-miint/CHANGELOG.md
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

## 4. Apply migrations

```bash
# [operator] Source the SAME env file the guard reads, so make migrate and the
#            guard target the same database (the guard sources control-plane.env
#            as root). A hand-set DATABASE_URL risks migrating one DB while the
#            guard checks another — which the guard's "wrong-DB" hint flags.
source /etc/qiita/control-plane.env
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

## 7. Verify

```bash
# [admin] services up + honest health
curl -fsS https://qiita-miint.ucsd.edu/healthz
curl -fsS https://qiita-miint.ucsd.edu/health         # per-service pills (CP/CO/DP)

# [admin] workflow actions registered at expected versions
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
  psql "$DATABASE_URL" -c "SELECT action_id, version, enabled FROM qiita.action ORDER BY action_id;"'

# [admin] compute path end-to-end (host checks + optional SLURM probe job)
sudo -u qiita-api bash -c 'set -a; source /etc/qiita/control-plane.env; set +a; \
  qiita-admin compute-readiness'
```

Run every check in the Pending-deploy bucket-5 list, plus anything the
Notes bucket flags for downstream clients.

## 8. Archive the deploy

Once verification passes, move the just-run checklist out of `## Pending
deploy` and into `## Deployed history`, stamped with the date and the
deployed commit, leaving Pending empty for the next cycle. From a Claude
session in the clone, run `/deploy-archive` (it stamps the date and
`git rev-parse HEAD` and does the move); otherwise do the move by hand
following the shape in `CHANGELOG.md`. Either way, also record the
deployed commit somewhere durable (host deploy log, ops channel).
