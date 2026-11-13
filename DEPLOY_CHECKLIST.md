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

- `[operator]` **After pulling and before the restart, confirm every stored run pre-flight
  still opens under the new kl-run-preflight.** Its schema patch refuses a blank or padded
  identity value (`sample_name`, `external_project_id`, an accession, an export ID,
  `matrix_tube_id`), and a stored pre-flight that fails to open can no longer be read by the
  pool roster, the runner or a re-submit. Expect `unopenable: 0` and exit 0; if any are
  listed, stop before deploying and raise them. On the `redeploy.sh` fast path, pull first
  and run this before it. (#840)
  ```bash
  # [operator] in the pulled checkout; DATABASE_URL as in redeploy.md §4
  set -a; . /etc/qiita/control-plane.env; set +a
  cd ~/qiita-miint/qiita-control-plane && /usr/local/bin/uv run --reinstall-package qiita-common qiita-admin check-stored-preflights
  ```

### 3. Migrations

- `[operator]` (#855) `make migrate` applies `20261110000000_block_rearmed_at.sql` (nullable `rearmed_at` on `qiita.block`, which `rerun-block` sets and abandon-plan reads; no row changes).

### 4. Deploy

- `[operator]` (#851) Deploy every data-plane instance, peers on other hosts included, from this build in the same window. An instance from an earlier build takes no lake lock, so its registrations into the replace-keyed tables overlap the new instances' again and can lose their commits to them.

### 5. Verify

- `[operator]` (#851) `make verify-deploy`'s `catalog-connections` row now counts each data-plane instance as its pool size plus 2 lake lock sessions (`pool=<n>` in the catalog session name). If it fails on the budget, lower `DUCKLAKE_POOL_SIZE` in `/etc/qiita/data-plane.env` or raise `max_connections` on the catalog server, as its message says.
- `[operator]` **Run `qiita-admin check-stored-preflights` again, as in bucket 2.** The old
  build kept accepting submissions and lane edits until the restart, and the bucket-2 run
  did not see those. Expect `unopenable: 0`; if any are listed, raise them with an admin,
  who corrects the stored pre-flight by hand. (#840)

### 6. After the deploy verifies green

- `[operator]` (#849) Disable the retired `myloasm-provenance-recover` action. The deploy removes its directory and job module, and `actions sync` does not disable an action whose directory is gone, so the row stays enabled. `action_disabled_consistent` needs `disabled_at` and `disabled_by_idx`; use your own principal's idx, from the first query. Load `DATABASE_URL` as [`redeploy.md`](docs/runbooks/redeploy.md) §4 does (`set -a; . /etc/qiita/control-plane.env; set +a`):
  ```bash
  psql "$DATABASE_URL" -c "SELECT idx, display_name FROM qiita.principal WHERE system_role = 'system_admin' AND idx <> 1 AND NOT retired;"   # idx 1 is the seeded system principal
  psql "$DATABASE_URL" -c "UPDATE qiita.action SET enabled = false, disabled_at = now(), disabled_by_idx = <your principal idx>, disabled_reason = 'one-off retired' WHERE action_id = 'myloasm-provenance-recover';"
  psql "$DATABASE_URL" -c "SELECT version, enabled FROM qiita.action WHERE action_id = 'myloasm-provenance-recover';"   # expect 1.0.0 | f
  ```

### Notes (no host action)

- (#865) `GET /ena-import-batch/{idx}` `ena_runs[].status` can be `unsupported` (a run whose library strategy and source have no curated protocol, previously `failed`); clients matching on the closed set of statuses must accept it.
- (#851) The data plane no longer updates or creates `qiita_lake.registration_lock`; the existing table stays in the catalog, unused.
- (#849) `qiita-admin backfill myloasm-provenance` is removed, with the `myloasm-provenance-recover` workflow and job: every myloasm run of long-read-assembly 1.0.0/1.0.1 is recovered.
- (#840) `POST /biosample/resolve-roster` takes a new, incompatible row shape (nested `biosample` / `primary_project` / `secondary_projects` refs); a `qiita` CLI installed before this deploy gets a 422 from `submit-bcl-convert`, `submit-pacbio-ingest` and `submit-golay-demux` until it is upgraded.
- (#840) Deploy this before kl-metapool moves to the kl-run-preflight release that adds export IDs: Qiita refuses a pre-flight written against a newer schema than the one it ships, so a pre-flight from the new kl-metapool fails every submit until this is live.
- (#840) Soft output change: in the `submit-bcl-convert` and `submit-golay-demux` JSON, each pre-flight row's `biosample_accession` is replaced by `biosample`, an object holding the identifiers the pre-flight gave for it.

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
