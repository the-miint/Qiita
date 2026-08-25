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

- `20260819000001_assembly_sample.sql` — plain `make migrate`, no out-of-band setup. One
  empty table and its index, `qiita.assembly_sample`: the per-`(processing_idx,
  prep_sample)` completion gate for `long-read-assembly`, alongside the existing
  `qiita.mask_sample` and `qiita.alignment_sample`. The index is created with the table, so
  it is a plain `CREATE INDEX` over zero rows — no `CONCURRENTLY`, nothing to lock.
  **No backfill**: assemblies already completed on this host get no gate row, so they read
  as not-assembled. No code reads the gate yet; whether to backfill them is a separate
  decision. The migrate→restart window has the same shape — an
  assembly ticket completing between bucket 3 and the bucket-4 restart runs under old code
  that writes no gate row. Re-submitting such a sample after the restart is admitted and
  re-writes it (no disallow-without-delete site applies to `long-read-assembly`). (#467)

- `20260825000000_sample_field_comment_corrections.sql` — plain `make migrate`, no
  out-of-band setup. Four `COMMENT` statements on the sample-field tables and columns; no
  DDL, no data touched, nothing locked beyond the momentary catalog write. Ordering versus
  the bucket-4 restart is irrelevant — no code reads these comments. (#485)

### 4. Deploy

_None yet._

### 5. Verify

- **`long-read-assembly` 1.0.0 is edited in place, not versioned** — `activate.sh`'s
  `qiita-admin actions sync` re-syncs it, so the `qiita.action` list check `make
  verify-deploy` already runs is the confirmation it landed. One edit rides this deploy,
  adding no bind mount, resource, or env var: a terminal `finalize-assembly-sample` entry
  appended after `register-files` — an in-process control-plane primitive writing the
  `qiita.assembly_sample` gate, not a SLURM step. Confirm it landed: all three
  `qiita.assembly_sample` writes are gated on the terminal entry being present in the
  synced `steps`, so under a stale copy no gate row is written at all and the table stays
  empty — which reads like a migration that did not apply rather than a sync that did not
  land.
  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -Atc "SELECT steps::text LIKE '\''%finalize-assembly-sample%'\'' FROM qiita.action WHERE action_id = '\''long-read-assembly'\'' AND version = '\''1.0.0'\'';"'
  ```
  Expect `t`. `f` is the stale copy. **Empty output** is a third outcome, not a pass: `-Atc`
  prints nothing for zero rows, so it means no `long-read-assembly` 1.0.0 row matched at
  all. Re-run `qiita-admin actions sync` for either. (#467)

- **The staged miint build must carry `circular_query_coverage`** — `qiita feature-table
  build --circular-gate` calls it. There is no capability probe: an absent function
  surfaces as a bare `Catalog Error` naming the function, so check it once here rather
  than letting a user discover it. Run as the CP service account, against the staged
  extension directory the CP already LOADs from:
  ```bash
  sudo -u qiita-api env MIINT_EXTENSION_DIRECTORY="$(grep -oP '(?<=^MIINT_EXTENSION_DIRECTORY=).*' /etc/qiita/control-plane.env)" \
    python3 -c "import duckdb, os; c=duckdb.connect(':memory:', config={'extension_directory': os.environ['MIINT_EXTENSION_DIRECTORY'], 'allow_unsigned_extensions': 'true'}); c.execute('LOAD miint'); print(c.execute(\"SELECT count(*) FROM duckdb_functions() WHERE function_name='circular_query_coverage'\").fetchone()[0])"
  ```
  Expect `1`. A `0` means the staged build predates the function — re-stage the extension
  before telling anyone `--circular-gate` works. (#475)

### 6. After the deploy verifies green

_None yet._


### Notes (no host action)

_None yet._

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
