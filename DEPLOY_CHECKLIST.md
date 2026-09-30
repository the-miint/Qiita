# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- `[operator]` **`PATH_INGEST_ROOTS` must cover the sequencer run-folder root(s).** No new
  variable — but `submit-golay-demux --instrument-run-id <id>` resolves the run folder by
  scanning `PATH_INGEST_ROOTS` for a directory whose basename matches the run id, so a run
  living under a path the roots don't cover cannot be submitted. Ensure the existing value
  includes wherever instruments copy runs. (#244)

### 2. One-time host setup

_None yet._

### 3. Migrations

- `[operator]` `make migrate` applies `20260929000000_sample_field_widen_fn.sql`, `20260929000001_unique_in_study_propagation_lock.sql` and `20260929000002_metadata_field_contract_error_detail.sql` (all three create or replace functions; no data change). (#628)
- `[operator]` `make migrate` applies `20260930000000_sequenced_sample_ena_status.sql` (two nullable columns on `qiita.sequenced_sample`; no data change). (#634)

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

_None yet._

### 5. Verify

- **Confirm the two amplicon workflows synced.** `golay-demux 1.0.0` and `amplicon 1.0.0`
  reach `qiita.action` via `qiita-admin actions sync` inside `activate.sh` — no migration.
  `make verify-deploy` lists `qiita.action`; check both appear. The new DuckLake
  `amplicon_membership`, `amplicon_sequence`, and `amplicon_sequence_chunks` tables are
  auto-created at data-plane boot (no migration, no action). `make verify-deploy`'s
  compute-readiness probe now includes a `miint-amplicon-fns` check that asserts the
  amplicon deblur functions (`align_sortmerna_rrna`, `detect_chimera_uchime_denovo`,
  `align_mafft`, `deblur`, `sequence_dna_as_regexp`) are registered in the staged miint
  build — a stale build missing one fails the deploy here, not at the first amplicon submit.
  The `amplicon` workflow additionally needs a SortMeRNA 16S database loaded as an ACTIVE
  `sequence_reference` (its `reference_idx` is a submit-time context arg) — a per-study data
  setup, not a deploy step. `golay-demux` now runs bcl-convert (a container step) before the
  demux, so it needs `bcl-convert-4.5.4.sif` present — the same SIF the `bcl-convert` workflow
  uses, rebuilt automatically at deploy — and a compute node that can run it. (#244)

### 6. After the deploy verifies green

_None yet._

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
