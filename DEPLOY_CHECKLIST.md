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

- `20260813000000_exported_feature.sql` and `20260813000001_exported_processing.sql` —
  plain `make migrate`, no out-of-band setup. Two empty mint tables
  (`qiita.exported_feature`, `qiita.exported_processing`), each with the same
  retire-on-detach trigger `qiita.exported_identifier` already carries. That behaviour now
  applies on three more delete paths: deleting a **genome**, a **reference**, or an
  `alignment_definition` that has published handles detaches and auto-retires them rather
  than failing or removing them, and the retirement records which identifier was severed.
  (#448)

### 4. Deploy

_None yet._

### 5. Verify

- **The staged miint build must carry `shear_tree`** — `qiita feature-table build --tree`
  calls it, and it is absent from older builds (`d0336e9` does not have it; the mirror's
  current `1ad7fb4` does). There is no capability probe: an absent function surfaces as a
  bare `Catalog Error` naming the function, so check it once here rather than letting a
  user discover it. Run as the CP service account, against the staged extension directory
  the CP already LOADs from:
  ```bash
  sudo -u qiita-api env MIINT_EXTENSION_DIRECTORY="$(grep -oP '(?<=^MIINT_EXTENSION_DIRECTORY=).*' /etc/qiita/control-plane.env)" \
    python3 -c "import duckdb, os; c=duckdb.connect(':memory:', config={'extension_directory': os.environ['MIINT_EXTENSION_DIRECTORY'], 'allow_unsigned_extensions': 'true'}); c.execute('LOAD miint'); print(c.execute(\"SELECT count(*) FROM duckdb_functions() WHERE function_name='shear_tree'\").fetchone()[0])"
  ```
  Expect `1`. A `0` means the staged build predates the function — re-stage the extension
  before telling anyone `--tree` works. (#448)

### 6. After the deploy verifies green

- **Collapse the sequence rows duplicated before this deploy.** `register_files` now
  replaces `assembled_sequence` / `assembled_sequence_chunks` / `reference_sequences` /
  `reference_sequence_chunks` on `feature_idx` instead of appending, but it does not touch
  rows already there. Measured 2026-08-13: 53,698 of 129,290 assembly features had two
  copies, so their `string_agg(chunk_data, '' ORDER BY chunk_index)` is twice
  `sequence_length_bp`. Run AFTER bucket 5 — before the new build is live the next load
  re-duplicates. Report first, then collapse; run as `qiita-data`, the only account that can
  write the lake data path:
  ```bash
  sudo -u qiita-data bash /opt/qiita/checkout/scripts/dedup-lake-sequence-tables.sh
  sudo -u qiita-data APPLY=1 bash /opt/qiita/checkout/scripts/dedup-lake-sequence-tables.sh
  ```
  Substitute the checkout path. `qiita-data` needs a duckdb CLI on PATH or
  `QIITA_DUCKDB_BIN`; match the data plane's DuckDB (the script prints install steps for the
  right version if it finds none). Both passes print `collapsible_features` and
  `ambiguous_features` per table pair. `ambiguous_features` is expected to be **0** — a
  non-zero count means a feature whose copies hold different bytes (a sequence and its
  reverse complement share one `feature_idx`), which the script deliberately leaves alone
  because no column records which chunk came from which load; re-run the producing load
  rather than picking a copy by hand. APPLY validates inside its transaction and exits
  non-zero without committing if the collapse did not converge. Every mode is re-runnable,
  so a failure part-way is safe to repeat once its cause is fixed — and a concurrent load
  touching the same tables makes one of the two abort with a DuckLake transaction conflict
  rather than either losing rows. (#457)

### Notes (no host action)

- **`register-files` now REPLACES the four content-addressed sequence tables on
  `feature_idx` rather than appending.** A load that carries a feature the lake already
  holds deletes the lake's rows for that key in the same transaction, ahead of the
  registration — so the row count for those tables can now go DOWN across a load, and the
  control-plane log records what each one superseded (`register_files replaced rows in
  content-addressed tables`). Where a feature's two copies differ (a sequence and its
  reverse complement share one `feature_idx`), the newest load's bytes win; before this they
  were both kept and read back concatenated. Every other lake table is untouched. (#457)

- **`GET …/sequenced-pool/{pool}/alignment` gains a `params_hash` field, and the new
  `qiita feature-table build` requires it.** Additive, so an older client ignores it and
  nothing on the host changes. The direction that bites is the other one: the new CLI
  recomputes that digest and refuses to build against a server too old to report one, by
  design — a client cannot vouch for params it has no way to check. Anyone pointing this
  build's CLI at an older deployment gets that refusal, not a wrong table. Two new mint
  routes (`POST /exported-feature`, `POST /exported-processing`) ship alongside it under
  the scopes their siblings already use — no new scope, so no PAT re-mint. (#448)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
