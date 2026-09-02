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

- **`20260901000000_assembly_membership_contig_attributes.sql`** — adds four nullable columns
  to `qiita.assembly_membership` (`raw_name`, `circularity`, `depth`, `mult`). `make migrate`
  applies it; no out-of-band setup. **Not backfilled**: the values are read out of the
  assemble step's output, so every existing row keeps NULL and only runs assembled after this
  deploy carry them. (#517)

- **`20260831000000_assembly_membership_genome.sql`** — adds `qiita.assembly_membership.genome_idx`
  (nullable, bare FK to `qiita.genome`, plus an index). `make migrate` applies it; no out-of-band
  setup. (#514)

### 4. Deploy

_None yet._

### 5. Verify

- **Record the rebuilt image's hifiasm_meta version.** (#516, #517)

  ```bash
  # TMPDIR=/tmp because apptainer forwards YOUR shell's TMPDIR into the container. A
  # TMPDIR on a path the container does not bind (an interactive account's scratch,
  # say) makes libmamba abort before it runs anything:
  #   critical libmamba filesystem error: temp_directory_path: No such file or directory
  TMPDIR=/tmp apptainer exec "${PATH_DERIVED}/images/long-read-assembly-assemble-1.0.0.sif" \
      micromamba run -n hifiasm_meta hifiasm_meta --version 2>&1
  ```

  This deploy rebuilds the image (`assemble.sh` is in its `HASH_INPUTS`). #517 pins
  hifiasm_meta to `hamtv0.3.5` and asserts its two internal versions in the def's `%test`, so
  the build itself now fails on a moved solve rather than shipping one quietly — this step
  records what that pin resolved to on this host for the archive entry (the versions go to
  stderr, which is why the command redirects). The gate itself is earlier:
  `deploy/build-sifs.sh` runs the def's `%test` during the image rebuild, and a failure there
  aborts `activate.sh` before any service restarts.

- **Run the assembly-genome backfill.** (#514)

  ```bash
  QA=/home/qiita/qiita-miint/qiita-control-plane/.venv/bin/qiita-admin
  # SOURCE the env file — redeploy.md §5's idiom, and the only one that is correct
  # here. The value is QUOTED in control-plane.env, so grepping the line captures the
  # quotes with it and the DSN parser reports `scheme is expected to be either
  # "postgresql" or "postgres", got ''`. Sourcing lets the shell strip them.
  set -a; . /etc/qiita/control-plane.env; set +a; DB="$DATABASE_URL"
  sudo -u qiita env DATABASE_URL="$DB" "$QA" backfill assembly-genome              # dry run
  sudo -u qiita env DATABASE_URL="$DB" "$QA" backfill assembly-genome --execute
  ```

  Assemblies run after this deploy get the mint inline; earlier ones need this. **No existing
  artifact changes**, but this is no longer optional groundwork: the combined feature table reads
  `assembly_membership.genome_idx`, and both drivers refuse a run whose memberships are not all
  minted rather than build a table over a short map — the REST map with a 422, a server-side
  submit by failing the work ticket. Either way an assembly run that predates the mint cannot be
  used as a de novo arm until this has run. Re-run the dry run
  afterwards; an empty plan ("nothing to do") is the completeness signal. Idempotent, so a repeat
  costs nothing. (#514, #515)

- **Check the combined feature table's two new routes answer.** (#515)

  ```bash
  # A run known to be assembled and backfilled. The backfill's dry run prints
  # AGGREGATE counts only — no pairs — so take one from the table instead:
  #   psql "$DB" -Atc "SELECT prep_sample_idx, processing_idx, count(*)
  #                      FROM qiita.assembly_membership WHERE genome_idx IS NOT NULL
  #                     GROUP BY 1,2 ORDER BY 3 DESC LIMIT 5;"
  curl -sS -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $QIITA_TOKEN" \
    "$BASE/api/v1/assembly/<prep_sample_idx>/<processing_idx>/genome-map"
  ```

  `200` means the map route and the backfill above both landed. `422` means the backfill has not
  covered that run yet — go back to the step above. `404` means that pair assembled nothing, a
  legitimate answer; pick another run. `401` means the PAT is stale, which is why the status code
  is what is printed: a body-only check reads the same whether the route answered or the token
  expired.

- **Submit one 1.0.1 assembly first — the three checks below have nothing to read without it.**
  (#522)

  ```bash
  # EXPORTED, both of them: the `qiita` CLI reads the host from
  # $QIITA_CONTROL_PLANE_URL and the PAT from $QIITA_TOKEN (cli/_common.py), so a
  # plain shell assignment reaches curl but not the CLI — it would silently target
  # localhost:8080 with no token.
  export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu   # your host
  BASE="${QIITA_CONTROL_PLANE_URL}"                             # for the curl steps
  read -rsp 'system_admin PAT: ' QIITA_TOKEN; export QIITA_TOKEN; echo

  MASK_IDX=11                                  # the mask you are re-running; see bucket 6
  qiita mask samples --mask-idx "${MASK_IDX}"  # one JSON row per prep_sample
  PREP_SAMPLE_IDX='<one prep_sample_idx the line above reports as completed>'

  qiita ticket submit --action-id long-read-assembly --action-version 1.0.1 \
      --prep-sample-idx "${PREP_SAMPLE_IDX}" \
      --context-json "{\"mask_idx\": ${MASK_IDX}, \"assembler\": \"hifiasm_meta\"}"
  ```

  The deploy starts no assembly of its own, so on a quiet host nothing has exercised the
  rebuilt images since the restart. All three checks below need a ticket assembled AFTER
  this deploy — the LCG and residue pairs because `lcg_split` and `residue_split` run only
  in the new checkm image, the MetaBAT count because the corrected contig→bin table ships
  in the new bin_refine one — and this submission produces all of them from one assembly.
  An unperformable check is not a passed one.

  Its compute is not spent twice: this is the first of bucket 6's re-runs, and bucket 6 picks
  up the rest of the same mask's roster once these three checks read green. Expect to wait —
  the cost figures are in bucket 6.

- **Record the three-pass `checkm` step's elapsed time AND its MaxRSS.** (#522)

  ```bash
  # The pilot ticket above.
  T='<work_ticket_idx>'
  ls -d "${PATH_SCRATCH}/ticket/${T}"/checkm/attempt-*/
  ```

  1.0.1 makes `checkm` three sequential CheckM runs, and the third (the unbinned
  residue) is the largest at ~148 contigs, against ~104 refined bins and ~86 circular —
  ~338 subjects per ticket. The image measured to date scored refined bins ALONE, one
  run of ~104, so this is roughly three times the work that produced the figures below.
  Count all three off this attempt while you are here. Those figures are carried from
  scoping rather than measured, and the circular one disagrees with the ~98 that
  `1.0.0.yaml` and `CHANGELOG.md` give for the same quantity — same count, two numbers,
  and nothing in the repo can settle which. Count by `kind` in `assembly_membership`
  for this run, or `ls` the three genome directories under the step workspace.

  **The step itself has never been run end to end**, so its walltime is unmeasured:
  the `PT4H` baseline in `1.0.1.yaml` is the old one, carried over rather than
  fitted. Take the step's elapsed off this attempt. Both the counts and the elapsed go
  in the comment on that step's `baseline_resources`, which says so.

  **Take `MaxRSS` too**, from `sacct -j <slurm_job_id> --format=JobID,MaxRSS,Elapsed`.
  `mem_gb` is still 40, which was fitted when the step scored refined bins alone —
  one run of ~104. The runs are sequential so they do not sum, but the LARGEST single
  run is now ~148 subjects, and whether CheckM's peak RSS scales with subject count
  over that range is not measured. An under-set `mem_gb` recovers the same way an
  under-set walltime does — SLURM reports OUT_OF_MEMORY, the runner treats it as
  retriable and re-submits with memory DOUBLED, clamped to the action's mem ceiling
  (`_escalated_mem_floor_after_oom`) — so it costs an attempt and one of the ticket's
  shared retries, not the ticket. If `MaxRSS` on this pilot is near 40 GB, raise
  `mem_gb` in `1.0.1.yaml` before the fan-out and spend neither.

  Overrunning it is recoverable, not fatal: SLURM marks the job TIMEOUT, which the
  runner treats as retriable and re-submits with walltime DOUBLED from the baseline
  (`_escalated_walltime_after_timeout`), clamped to the action ceiling — `P2D` here, so
  4h → 8h has room to climb. Only the `checkm` step re-runs; the assembly above it is
  already stored and is not paid again. What an under-set baseline costs is one wasted
  attempt of up to the cap plus one of the ticket's three shared retries, which is why
  this is worth fitting before the fan-out rather than something to fear.

- **Check the circular genomes (LCGs — the contigs the assembler closed, which bypass binning
  at any length; the >=512 kb "large" cut is a query-time predicate, not a storage one)
  and the unbinned residue came back scored.** (#519, #522)

  ```bash
  # The pilot ticket above — its assemble step must have produced a non-empty circular.fa.
  T='<work_ticket_idx>'
  ls "${PATH_SCRATCH}/ticket/${T}"/checkm/attempt-*/output/checkm/
  ```

  Six files — `lineage.tsv`, `qa.tsv`, `lcg_lineage.tsv`, `lcg_qa.tsv`, `unbinned_lineage.tsv`,
  `unbinned_qa.tsv` — means all three CheckM runs landed. Neither added arm can fail silently:
  `checkm.sh` runs under `set -euo pipefail`, so an `lcg_split` or `residue_split` failure aborts
  the step before it publishes anything and the TICKET fails. So only the first pair, on a FAILED
  attempt, is the shape to read: check that step log for `lcg_split` / `residue_split`, which exit
  64 with the reason on stderr. A missing or mislocated `${PATH_DERIVED}/duckdb-ext` is the
  failure both gained (see the note below). Two legitimate partial cases: an empty `circular.fa`
  writes no `lcg_*` pair, and a residue where nothing clears the 300 kb cut writes no `unbinned_*`
  pair — so pick the ticket before reading the result.

  For the residue pair specifically, absence is NOT self-evidently benign: an
  over-broad subtraction also writes nothing and exits 0. `residue_split` prints
  `residue_split: N unbinned contig(s) >= 300000 bp written as one FASTA each` to the
  step log — read N off it and expect roughly the count above, not merely that the
  step passed.

- **Check DAS_Tool now selects MetaBAT bins.** (#519)

  ```bash
  # The pilot ticket above, whose bin_refine produced bins.
  T='<work_ticket_idx>'
  awk -F'\t' 'FNR==1{c=0; for(i=1;i<=NF;i++) if($i=="bin_set") c=i; next} c{print $c}' \
      "${PATH_SCRATCH}/ticket/${T}"/bin_refine/attempt-*/output/refined_bins/das_tool_summary.tsv \
      | sort | uniq -c
  ```

  Keyed on the `bin_set` header rather than a column position, the way `assembly_load`
  reads this same file, and re-read per file (`FNR==1`) so a retried ticket's second
  `attempt-*` does not leak its header row into the counts. A `MetaBAT` line means the
  corrected contig→bin table landed; the note below says what that count could read
  before. Its absence on one ticket is not a failed deploy — a sample can legitimately
  yield no MetaBAT bin — so read it across the first few, and confirm the step exited 0
  rather than 65, the new refusal described in that note. On the pilot alone a missing
  `MetaBAT` line is therefore not yet a verdict; bucket 6's fan-out gives the rest.

### 6. After the deploy verifies green

- **Re-run mask 11's pre-fix assemblies under `long-read-assembly` 1.0.1, then deprecate that
  1.0.0 run.** (#522) The `bin_refine` consensus fix (#519) does not reach anything already
  assembled: those prep_samples hold a two-binner MAG set, their tickets are `completed`
  (terminal — `/run` refuses), and a re-submit at 1.0.0 resolves to the same `processing_idx`.
  1.0.1 also scores the unbinned residue, so it is a different computation and a
  distinct run rather than a repeat; `CHANGELOG.md` carries why that is necessary.

  **The roster is `processing_idx = 2` — 26 prep_samples, 30438–30463 inclusive. Take it from
  the membership table, never from the mask listing:**

  ```bash
  psql "$DB" -Atc "SELECT DISTINCT prep_sample_idx FROM qiita.assembly_membership
                    WHERE processing_idx = 2 ORDER BY 1;"
  ```

  **`qiita mask samples --mask-idx 11` is the wrong source and the mistake is expensive.** It
  lists every prep_sample ELIGIBLE to assemble — 82 of them — of which only the first 26 have a
  pre-fix assembly. Submitting that list fires 26 legitimate re-runs plus **56 brand-new
  assemblies at ~7 h each** that nobody decided to create. Nothing is superseded and nothing
  corrupts, so it fails silently in the only way that matters: as spent compute and 56 runs
  appearing in the system by accident.

  Two 1.0.0 runs exist, both `hifiasm_meta`, and only one is acted on:

  | processing_idx | mask_idx | prep_samples | action |
  |---|---|---|---|
  | 1 | 9 | 26 | **none — leave exactly as it is** |
  | 2 | 11 | 26 | re-run at 1.0.1, then deprecate |

  **Mask 9 gets nothing: no re-run, and no deprecation either.** Its `processing_idx` 1 stays
  `active` with its assembly results as they are. Do not "tidy it up" — a deprecation there
  would be a judgement nobody made. Nothing in this bucket or bucket 5 touches it: every command
  here is `MASK_IDX=11`.

  **Mask 9 must not be re-assembled, and nothing in the system stops you.** The guard is that
  you type `11`. Be plain about what each mechanism does and does not cover:

  - The action-level disable is per `(action_id, version)`. It stops mask 9 **at 1.0.0** once
    1.0.1 syncs. It stops nothing at 1.0.1, which is enabled and is the version the block below
    submits with — `mask_idx` is a free integer in its context schema, so changing `MASK_IDX=11`
    to `9` assembles all 26 mask-9 prep_samples at ~7 h each under a fresh `processing_idx`.
    Nothing existing is superseded, but the compute is spent and the run should not exist.
  - Deprecating `processing_idx` 1 WOULD make a mint against those params refuse. It is
    deliberately not done, so that guard is not in play.
  - The rollback reopens 1.0.0 as well: reverting `1.0.1.yaml` and re-syncing RE-ENABLES it
    (`sync_actions` clears the disable it set). A mask-9 submit at 1.0.0 then recomputes those
    assemblies and supersedes the stored rows on the DuckLake replace key.

  ```bash
  # Exported for the same reason as bucket 5's block: the CLI reads both from the
  # environment, and a plain assignment reaches curl but not `qiita`.
  export QIITA_CONTROL_PLANE_URL=https://qiita-miint.ucsd.edu   # your host
  BASE="${QIITA_CONTROL_PLANE_URL}"                             # for the curl step below
  read -rsp 'system_admin PAT: ' QIITA_TOKEN; export QIITA_TOKEN; echo

  MASK_IDX=11        # mask 9 is deliberately not re-run; see the table above
  OLD_IDX=2          # the 1.0.0 run for mask 11

  # 1. The mask's roster. One JSON row per prep_sample with its masking state —
  #    take the prep_sample_idx of the `completed` ones.
  qiita mask samples --mask-idx "${MASK_IDX}"

  # 2. The rest of that roster, one at a time — bucket 5's pilot already did the
  #    first, and its three checks must have read green before this fans out.
  PREP_SAMPLE_IDX='<the next prep_sample_idx from step 1>'
  qiita ticket submit --action-id long-read-assembly --action-version 1.0.1 \
      --prep-sample-idx "${PREP_SAMPLE_IDX}" \
      --context-json "{\"mask_idx\": ${MASK_IDX}, \"assembler\": \"hifiasm_meta\"}"

  # 3. Once the roster has finished, retire the old run. NEW_IDX is the processing_idx
  #    the re-runs minted — one for the whole mask, so this waits for step 2.
  #    system_admin only (scope processing:lifecycle).
  NEW_IDX='<the processing_idx the re-runs minted>'
  curl -sS -X PATCH -H "Authorization: Bearer ${QIITA_TOKEN}" \
      -H 'Content-Type: application/json' \
      -d "{\"status\":\"deprecated\",\"superseded_by\":${NEW_IDX},
           \"reason\":\"bin_refine gave DAS_Tool no MetaBAT bins; superseded by the 1.0.1 re-run\"}" \
      "${BASE}/api/v1/processing/${OLD_IDX}/status"
  ```

  The body field is `reason`, not `deprecation_reason` — the latter is the response field, and
  `ProcessingStatusUpdate` is `extra="forbid"`, so sending it 422s. `reason` is required when
  deprecating. The PATCH is a whole-block replace: a later correction must re-supply
  `superseded_by` or it clears.

  Two different deprecations, and only this one is manual. The action-level one is automatic: a
  sync leaves only the highest version of an action_id enabled, so once 1.0.1 is synced
  `long-read-assembly` **1.0.0** stops accepting submissions for any mask. It also blocks a
  redrive of the 52 existing 1.0.0 tickets — `/run` 409s whenever the ticket's action row is not
  enabled — though that is belt-and-braces today: all 52 are `completed`, which `/run` already
  refuses as terminal before the enabled check runs. Nothing already stored is affected.

  The PATCH is narrower and is about `qiita.processing`, not `qiita.action`: it records why mask
  11's run is void and what replaced it, which the action-level disable does not. Nothing is
  deleted either way, and what assembled under the old run stays discoverable. Mask 9's run is
  touched by neither — the PATCH names mask 11's, and the auto-deprecation is on the
  `qiita.action` row, not on any `qiita.processing` row.

  **Cost, so this is scheduled and not squeezed in:** only `bin_refine` onward changes
  (~60 min/prep_sample), but partial re-runs do not exist, so `assemble` is paid again in full —
  measured across 59 completed `assemble` steps at 415.3 min average, 1094.1 min peak, per
  prep_sample — a different step from the 59 completed `checkm` steps in bucket 5, which happen
  to be the same count.
  That cost is why bucket 5 submits one prep_sample and reads it before this fans out: at
  ~7 h of assemble each, starting all 26 against an image whose fix has not been observed once
  spends the whole re-run before anything could show it did not land.

  The assembly-genome backfill above still applies to the 1.0.0 rows and is unaffected by this;
  `processing_idx` is in the genome identity tuple, so old and new genomes never collide. The
  1.0.1 rows need no backfill pass — assemblies run after this deploy mint their genomes inline.

### Notes (no host action)

- **`estimate-feature-table` 1.0.0 gained an optional `denovo_alignment_idx` context key** and a
  second resolver-staged input. The deploy's workflow sync picks it up; nothing to do by hand. A
  submit that omits the key behaves exactly as before, so existing callers are unaffected. (#515)
- **New scope `assembly:doget`, on every human role ceiling and on no service ceiling.** It is
  role-implied, so no token needs re-minting and no grant is needed — a PAT minted after the
  restart carries it automatically. (#515)

- **`assemble` now keeps the assembler's whole output tree, so each attempt holds ~1 GB more
  scratch.** (#516) Measured on 1.24 Gbp of masked reads — 12% of one real ticket's — the
  retained tree is 1.44 GB for myloasm and 697 MB for hifiasm_meta. Assembly output does not
  scale linearly with input and has not been measured at a second point, so that is a floor,
  not a per-ticket figure. It lands in the step's own output directory under the per-attempt
  ticket workspace; nothing new is written outside it and no step reads it. **That scratch is
  not currently reclaimed.** `docs/architecture/storage.md` documents ephemeral per-ticket
  directories as deleted 45 days past a ticket's terminal state, but nothing implements that
  sweep, so ticket workspaces accumulate and these trees will accumulate with them — plan the
  disk on that basis, not on the documented window.

- **The `assemble` SIF rebuilds this deploy, and hifiasm_meta is now pinned.** (#516, #517)
  `assemble.sh` is in the image's `HASH_INPUTS` (`sif-build.d/assemble.env`), so editing it
  invalidates the build hash and forces a full rebuild. #517 pins `hifiasm_meta=hamtv0.3.5`
  and asserts its two internal version strings in `%test`, alongside myloasm's existing 0.6.0
  pin — so this rebuild resolves both assemblers to a fixed version instead of re-solving the
  default one against whatever bioconda serves that day, and a moved solve fails the build.
  Expect a slower-than-usual verify for this one image.

- **The DuckLake `assembly_membership` table gains the four attribute columns on the data
  plane's next start.** (#517) `ensure_assembly_tables` runs `ALTER TABLE … ADD COLUMN IF NOT
  EXISTS` on every DP boot, so the existing lake table widens itself — no operator step.
  Without it `ducklake_add_data_files` would reject every membership Parquet the new
  `assembly_load` writes, since the file would carry columns the table lacks. It refuses the
  other direction too (probed: a Parquet MISSING a column the table has is rejected with
  `Set allow_missing => true`), so a ticket whose `assembly_load` ran on the pre-deploy
  orchestrator and registers after the DP restart fails its register-files step. Both skews
  are narrow — the deploy restarts the DP and the CO together — and a failed register-files
  is a retryable step, not lost data: re-running the ticket's tail after the deploy writes a
  nine-column file. Nothing to do in advance; this is here so the failure is recognisable.

- **The `checkm` step binds the deploy-staged miint extension, and its image lands under a NEW
  filename.** (#519, #522) `checkm.sh` splits `circular.fa` and the unbinned residue into
  per-contig FASTAs with miint's `read_fastx` / `COPY … FORMAT FASTA`, so the step gained
  `derived_inputs: MIINT_EXTENSION_DIRECTORY: duckdb-ext` — resolved against `PATH_DERIVED`
  exactly as the `assemble` step's already is. The bind is **unconditional**: the backend emits it
  for every `derived_inputs` entry, so a missing or mislocated `${PATH_DERIVED}/duckdb-ext` fails
  apptainer for the whole step, not just the split arms, including for a prep_sample with no
  circular contig. Nothing new to stage — it is the same directory the assemble step already
  requires — but this is a second step that now depends on it. The image also gains
  `python-duckdb`, pinned in lockstep with the orchestrator's resolved DuckDB, so expect a
  slower-than-usual verify for it.

  **The build produces `long-read-assembly-checkm-1.0.1.sif`, not a rebuilt `-1.0.0.sif`**, so
  roughly one image's worth of extra space lands under `${PATH_DERIVED}/images`. **Do not delete
  `-checkm-1.0.0.sif`**: `1.0.0.yaml` still names it and no spec builds it any more. Note what that
  image actually is — the copy already on this host, built BEFORE #519, which scores MAGs only. So
  `long-read-assembly` 1.0.0 is retired rather than reproducible. Read its YAML with that in mind:
  #519 edited it to describe an LCG arm, which was true of the image that file WOULD have got had
  the rebuild kept the name, and is not true of the one it keeps. Nothing submits at 1.0.0 after
  this deploy (the
  sync disables it, and bucket 6 deprecates mask 11's run, which makes a re-mint against THOSE
  params refuse outright — mask 9's run stays `active` by decision, so for it the disable is the
  only thing standing between a re-synced 1.0.0 and a new assembly),
  so this is a statement about the archive, not a live path.

- **`bin_quality` gains LCG rows; nothing existing changes.** (#519) The table now carries one row
  per circular genome beside the per-refined-bin rows, tagged by `kind`. Rows written by earlier
  deploys are untouched and are not backfilled — the values come from a CheckM run that did not
  happen, so an assembly stored before this deploy keeps MAG rows only. Re-running the workflow
  for a prep_sample is what produces its LCG rows. Under 1.0.1 the unbinned residue
  above the length cut is scored too; below it, a contig still has a membership row
  and no quality row.

- **The `bin_refine` SIF rebuilds, and MAG composition changes for assemblies run after this
  deploy.** (#519) `bin_refine.sh` and the new `contig2bin_filter.awk` are both in the image's
  `HASH_INPUTS`, so the dastool image rebuilds. The behaviour change is in what DAS_Tool is asked
  to score: metabat2's contig→bin table reached it with every bin id blanked (the id was read from
  a column the table does not have), so no MetaBAT bin was ever selectable — 0 across 60 production
  runs — while each binner's unbinned catch-all WAS offered as a candidate bin. The two are fixed
  together, because correcting the column alone would newly offer metabat2's three catch-alls as
  MetaBAT bins. Re-measured end to end on 150k masked reads from a production ticket, same input
  and same images: 20 bins → 21, MetaBAT 0 → 4, mean redundancy 3.8 → 1.9. Stored assemblies are
  neither re-run nor backfilled — they keep the bins they were produced with, and comparing a MAG
  set from before this deploy with one from after compares two different consensus inputs. New
  failure mode: a contig2bin row that is neither a numbered bin (`bin.<N>`) nor a known binner
  catch-all exits the step **65** with the offending rows on stderr rather than guessing which it
  is; that is what a metaWRAP output-naming change would look like.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
