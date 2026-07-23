# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- **CP `MIINT_EXTENSION_DIRECTORY`** — the CP runner now LOADs miint in-process to stream masked reads (the `long-read-assembly` input). Copied from the CO's env so the two stay byte-identical; the directory only needs to be **readable** by `qiita-api` (LOAD writes nothing). Not fail-fast, unlike most of this bucket: the CP boots and serves every other route, and only `long-read-assembly` tickets fail — at submission, with a message naming this var. Bucket 5's `cp-miint` check is what catches a missed step. `(#352)`
  ```bash
  sudo bash -c 'set -e
  f=/etc/qiita/control-plane.env
  grep -q "^MIINT_EXTENSION_DIRECTORY=" "$f" && { echo "already set"; exit 0; }
  line=$(grep -h "^MIINT_EXTENSION_DIRECTORY=" /etc/qiita/compute-orchestrator.env) \
    || { echo "MIINT_EXTENSION_DIRECTORY missing from compute-orchestrator.env — set it there first" >&2; exit 1; }
  [ -s "$f" ] && [ "$(tail -c1 "$f" | wc -l)" -eq 0 ] && echo >> "$f"   # no trailing newline: do not concatenate onto the last line
  printf "%s\n" "$line" >> "$f"
  echo "installed: $line"'
  ```

### 2. One-time host setup

_None yet._

### 3. Migrations

- `make migrate` applies three files for the operator-cancel `cancelled` state, no
  out-of-band setup: `20260721000000_work_ticket_state_cancelled.sql` (plain
  `ALTER TYPE … ADD VALUE 'cancelled'`), then the notify owed-set index recreate pair
  `20260721000001_drop_email_owed_idx.sql` + `20260721000002_email_owed_idx_with_cancelled.sql`
  (CONCURRENTLY drop+create widening the partial index to the new terminal state — so a
  cancelled ticket's originator still gets the digest). (#350)
- `make migrate` also applies `20260721000003_seed_mouse_gut_terminology.sql`, no
  out-of-band setup: a pure data seed appending three controlled-vocabulary terms to
  the terminologies already seeded — NCBI Taxonomy gains `410661` (mouse gut
  metagenome) and `10090` (Mus musculus), ENVO gains `ENVO:00006776`
  (animal-associated habitat, flagged `source_deprecated` because it is obsolete
  upstream but appears in data we import). Every INSERT is `ON CONFLICT DO NOTHING`,
  so re-running is a no-op. It appends terms rather than running the terminology
  reload pipeline, so `version` / `loaded_at` on the parent NCBI Taxonomy and ENVO
  rows are deliberately left as originally seeded. (#360)
- `make migrate` applies two new migrations — both plain (nullable `ADD COLUMN` / `CREATE TABLE`, no backfill, no `CREATE EXTENSION`): `20260721000004_reference_membership_accession.sql` (persist the FASTA-header record accession per reference membership) and `20260721000005_reference_exclusion.sql` (the curated global feature/genome blocklist table). (#361)
- `make migrate` also applies `20260722000000_feature_genome_allow_multi_genome.sql`, no out-of-band setup: a plain `ALTER TABLE qiita.feature_genome DROP CONSTRAINT feature_genome_feature_idx_key`, letting a feature (a shared plasmid → one content-hash-global `feature_idx`) belong to multiple genomes. The composite PK `(feature_idx, genome_idx)` already models the many-to-many. See the Notes re-load caveat. (#feat/reference-genome-followups — retag once the PR number exists)

### 4. Deploy

_None yet._

### 5. Verify

- Reference exclusion wired: `GET /api/v1/reference/{idx}/exclusion` on any active reference returns `200` with `[]` (confirms the new route + the `reference_exclusion` migration reached the CP; read-only, creates no block). (#361)
- **`cp-miint`** — new `make verify-deploy` check (no separate command): asserts the control plane can LOAD miint, the masked-read streaming path `long-read-assembly` depends on. A red row here means bucket 1 was missed. `(#352)`

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- New `system_admin`-only scope `reference:exclusion:write` (a code-defined ceiling, no DB grant) plus new routes `POST`/`DELETE /reference/exclusion`, `POST /reference/exclusion/sync` (operator force-resync of the mirror when it drifts — a failed sync, a rebuilt DuckLake catalog, or a fresh data plane), and `GET /reference/{idx}/exclusion`. CLI: `qiita-admin reference exclusion add/remove/sync` (the write surface) and `qiita reference exclusion list` (the `reference:read` query any user can run). Soft API addition — no existing client breaks. (#361)
- The data-plane build now creates two anti-join views at boot (`alignment_visible`, `reference_taxonomy_visible`) and **removes the raw `alignment` / `reference_taxonomy` tables from the DoGet allowlist** — a DoGet ticket signed for a raw name is now rejected. No in-repo consumer named the raw tables directly (the CP mint routes choose the table, and both were flipped to the views there), so no in-repo action is needed — but an **out-of-repo** client that signs tickets for the raw names will break. A standard redeploy (which rebuilds + restarts the DP, i.e. **don't `SKIP_BUILD`**) is required for the views and the `sync_reference_exclusion` DoAction to exist. (#361)
- All four reference-load workflows (`reference-add`, `local-reference-add`, `host-reference-add`, `local-host-reference-add`) gained a post-load `sync-reference-exclusion` step (no version bump); `qiita-admin actions sync` (run by `activate.sh`) re-upserts them at deploy — no separate action. (#361)
- New `work_ticket:cancel` scope (system_admin) gates `qiita-admin ticket cancel`.
  PATs minted before this deploy are frozen and won't carry it, so an admin must
  **re-login** (`qiita-admin login`, or re-mint) to pick it up before the cancel
  command works — a stale-scope 403 otherwise names the fix. (#350)
- References loaded before the `feature_genome_allow_multi_genome` migration
  silently dropped the second genome's association for any feature shared across
  genomes (a shared plasmid). There is **no backfill migration** — RE-LOAD affected
  references to recover the dropped associations. New loads are correct
  automatically. (#feat/reference-genome-followups — retag once the PR number exists)
- **Soft contract change (no host action):** `POST /reference/{idx}/ticket/doget`
  now accepts `reference:read` in addition to the service-only `ticket:doget`
  (any-of) — reference sequences/taxonomy/phylogeny are public reference data, and
  this lets the new `qiita reference export` user CLI stream a genome's sequences.
  Strictly additive: `ticket:doget` stays accepted, so the compute service account
  (which holds `ticket:doget`, not `reference:read`) keeps minting its build/OGU
  tickets — nothing loses access, no re-provisioning.
  (#feat/reference-genome-followups — retag once the PR number exists)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
