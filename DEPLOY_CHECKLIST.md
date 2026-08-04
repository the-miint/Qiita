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

- **PRE-CHECK before the bucket-3 `20260725000001` migration.** That migration adds `UNIQUE(display_name)` to `biosample_global_field` and `prep_sample_global_field`; the `ALTER TABLE ... ADD CONSTRAINT UNIQUE` **fails** if either table already holds duplicate display_names. Find and resolve any before `make migrate`: (#386)

  ```bash
  psql "$DATABASE_URL" -tAc "
    SELECT 'biosample_global_field' AS table_name, display_name, count(*) AS n
      FROM qiita.biosample_global_field GROUP BY display_name HAVING count(*) > 1
    UNION ALL
    SELECT 'prep_sample_global_field' AS table_name, display_name, count(*) AS n
      FROM qiita.prep_sample_global_field GROUP BY display_name HAVING count(*) > 1"
  # expect: zero rows. Any row → resolve the duplicate display_name(s) first.
  ```

### 3. Migrations

- `20260725000000_metadata_global_field_alias_index_comments.sql` — attaches explanatory COMMENTs to the per-global metadata uniqueness indexes (no schema change). Plain `make migrate`. (#386)
- `20260725000001_global_field_display_name_unique.sql` — adds `UNIQUE(display_name)` to both global-field tables. Plain `make migrate` — **but only after the bucket-2 pre-check passes** (the constraint build aborts on existing duplicate display_names). (#386)
- `20260725000002_metadata_reject_link_retired_on_update.sql` — adds a `BEFORE UPDATE` twin of the existing retired-link guard on `biosample_metadata` and `prep_sample_metadata` (reuses the existing trigger functions; no schema change). Plain `make migrate`. (#386)

### 4. Deploy

_None yet._

### 5. Verify

_None yet._

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- Metadata values for a `boolean`-typed biosample/prep_sample field now write and read back (`true`/`false`, case-insensitive; anything else 422s). (#386)
- A metadata write returns a `numeric` value in the form it is stored, so exponent notation comes back resolved (`1e3` → `1000`). A rewrite differing only in scale now reports `updated` and overwrites — `5` then `5.0` stores `5.0` — where it previously reported `unchanged` and wrote nothing. (#386)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
