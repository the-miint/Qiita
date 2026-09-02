# Changelog

The "what changed" log for this repo, one bullet per change. The git history is
the authoritative record; the per-line `(#N)` tag traces each entry to its PR.
Operator deploy steps live separately in
[`DEPLOY_CHECKLIST.md`](DEPLOY_CHECKLIST.md) — keep the two from drifting into
each other (a change can warrant an entry here, a step there, or both).

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The
project does not cut versioned releases yet, so everything lands under
**Unreleased**. Every PR adds an entry here (CI `changelog-check`; opt out with
the `no-changelog` label).

**Where to add your entry.** Add your bullet under the `### Added` /
`### Changed` / `### Fixed` / `### Removed` heading under `## [Unreleased]`, and
**never create a new bucket heading** — duplicate headings are how this file grew
to 5,745 lines with eleven redundant buckets. Entries predating the last rotation
live in [`docs/changelog-archive/`](docs/changelog-archive/).

## [Unreleased]

### Added

- **`long-read-assembly` 1.0.1 scores the large unbinned contigs, so completeness and
  contamination now cover every class the workflow stores (#522).** The `checkm` step gains a
  THIRD CheckM run, over each unbinned contig at or above 300 kb, beside the existing MAG and LCG
  runs. An unbinned contig is what no refined bin claimed, which is not the same as "not a
  genome" — a large one can be a near-complete genome the binners failed to recover, and it was
  the one class stored with nothing to judge it by. `bin_quality` therefore gains UNBINNED rows,
  a SUBSET of the UNBINNED memberships: a contig under the cut keeps its membership row and has
  no quality row, so the two are read with a LEFT join. The cut is where it is because a
  marker-set completeness for a short fragment describes nothing while CheckM's cost is per
  genome; the whole policy lives on `residue_split.py`.
  **The residue is subtracted on the canonical sequence hash, not the contig id.** `noLCG.fa`
  still contains the contigs a refined bin went on to claim, and `assembly_hash` drops those from
  its UNBINNED rows keyed on `canonical_sequence_hash_expr`. Matching ids here instead would keep
  any contig whose BYTES duplicate a binned one under another name, and CheckM would return a
  `"Bin Id"` joining no membership row at all. `residue_split.py` imports that expression from
  `qiita_common.chunking` — the same file `assembly_hash` imports, staged into the image by
  `build-sif.sh` and declared in the checkm image's `HASH_INPUTS`, rather than re-implemented.
  It takes `is_empty_sequence_file` from `qiita_common.duckdb_miint` the same way, so the rule
  that drops an empty refined bin before `read_fastx` is handed it is also one implementation
  rather than two: a zero-record `.fa.gz` is ~20 bytes on disk, so a size check calls it
  non-empty, and `read_fastx` then raises `Empty file:` and aborts the scan for every other bin.
  `test_residue_split.py` executes the shipped splitter against real miint on fixtures that
  isolate each way the rule can be got wrong: a duplicate under a different id, a reverse
  complement, a soft-masked copy, and both sides of the length boundary.
  **This is why it is a version and not a rebuild.** A run's identity is
  `{workflow, version, mask_idx, assembler}`; the result changes, so the version changes. It is
  also the version the pre-`bin_refine`-fix (#519) assemblies are re-run under: those hold a
  two-binner MAG set, their tickets are `completed` (which `/run` refuses as terminal), and
  because the identity does not cover container images a re-submit at 1.0.0 would resolve
  straight back to the run that already exists. Landing new bins on that same `processing_idx`
  would split the stores — the DuckLake `assembly_membership` / `bin_quality` tables are
  replace-keyed on `(prep_sample_idx, processing_idx)` and supersede wholesale, while Postgres
  `assembly_membership` is `INSERT … ON CONFLICT DO UPDATE` with no delete path, so the
  superseded MAG subjects would survive in Postgres and not in the lake.
  The three unchanged images keep their `-1.0.0.sif` names — a SIF name is the IMAGE's, not the
  workflow's — but the checkm image is the one that changed and is now built as
  `long-read-assembly-checkm-1.0.1.sif`. That is not cosmetic: `sync_actions` RE-ENABLES a version
  it previously auto-deprecated as soon as its YAML is synced again, so a plain revert of
  `1.0.1.yaml` would put 1.0.0 back in service, and a checkm image rebuilt in place would have had
  those runs writing residue quality rows under the 1.0.0 `processing_idx` that already exists.
  The consequence, stated plainly: **no spec builds `-checkm-1.0.0.sif` any more**, so
  `long-read-assembly` 1.0.0 is retired rather than reproducible — the image it names exists only
  as the copy already on the deploy host, which the operator must not delete, and cannot be
  rebuilt from this tree at all.
  There is deliberately no test asserting 1.0.1 is 1.0.0's computation under a new identity: that
  is no longer what 1.0.1 is, and a green test making that claim would be worse than none.
  The step's walltime is **not** re-fitted — three sequential runs over ~338 genomes per
  ticket, against a measured predecessor that scored ~104 in a single run, has never been run end
  to end, so the `PT4H` cap is carried over
  and DEPLOY_CHECKLIST.md bucket 5 records the first real elapsed.
  `test_load_actions_loads_on_disk_long_read_assembly_yaml`
  now keys on `(action_id, version)` as `qiita.action` does, rather than on `action_id` alone,
  which collapsed the two onto whichever sorted last. Syncing 1.0.1 also **disables 1.0.0** —
  `sync_actions` auto-deprecates every other version of an action_id and is last-one-wins over
  the loader's output, which is the state `fastq-to-parquet` 1.0.0 through 1.2.0 are already in
  on the deploy host. That is the wanted outcome here. The three `fastq-to-parquet` headers that
  claimed the opposite ("stay available unchanged; submitters choose the version") now state the
  rule instead, and state it without naming a version: a sync is directory-wide, so no header can
  say which file "wins" without going stale at the next bump.

- **CheckM now scores the circular genomes too, so completeness and contamination cover every
  kind the workflow stores except the residue it deliberately does not score.** `checkm.sh`
  splits the assemble step's `circular.fa` into one FASTA per contig (miint `read_fastx` +
  `COPY … FORMAT FASTA`, in the new `lcg_split.py`) and runs `lineage_wf` + `qa -o 2` over them
  in a SECOND CheckM run, publishing `lcg_lineage.tsv` / `lcg_qa.tsv` beside the refined-bin
  pair. `assembly_load` reads both pairs and tags each row with the kind of the run it came
  from, so `bin_quality` now holds LCG rows beside MAG rows. Two runs rather than one merged
  directory because CheckM reports only a `"Bin Id"` — the filename stem — so merged, a row's
  `kind` would have to be recovered by prefixing every stem and parsing the prefix back off
  before the row could join `assembly_membership.bin_id`; scored apart, the file a row came
  from IS its kind and every stem reaches the lake unmodified. An LCG bypasses binning
  entirely, so before this the class most likely to be a complete genome was the only one
  stored with no quality at all. UNBINNED is still not scored under 1.0.0 (superseded by the
  1.0.1 entry above, which scores the residue above a length cut): an unbinned contig is what no
  refined bin claimed, so a completeness figure against a marker set describes nothing. The
  step's `baseline_resources` are unchanged and now fitted rather than assumed — across 59
  completed `checkm` steps on the deploy host the elapsed averaged 27.5 min and peaked at
  58.9 min against a PT4H cap, and the second run scores a comparable genome set (102.5 refined
  bins beside 85.2 circular contigs per ticket, both measured across the 52 stored runs), so a
  doubled peak still sits under half the cap. The `checkm` step gains `genomes_dir` as an input and the deploy-staged miint extension
  as a `derived_inputs` bind; `checkm.def` gains `python-duckdb`, pinned in lockstep with the
  orchestrator's resolved DuckDB for the reason `assemble.def` states. CheckM keying its
  `"Bin Id"` on the stem with only the final extension removed is measured, not assumed: on the
  deploy host `CONCOCT_bin.13_sub.fa` came back as `CONCOCT_bin.13_sub`, which is what lets a
  dotted hifiasm contig id (`s0.ctg000001c`) round-trip as its own `bin_id`. An id that cannot
  be a filename stem stops the step rather than being sanitized into one that joins nothing (#519).

- **The assembler's per-contig report is stored, so circularity can become a query-time
  predicate instead of a routing decision baked into the entrypoint (#517).** Both arms of
  `assemble.sh` now emit a `contig_attributes.tsv` beside the two published FASTAs, carrying
  the raw header or GFA segment name, a normalized `yes`/`possibly`/`no` circularity call,
  depth, and myloasm's k-mer multiplicity. `assembly_load` and the control plane's membership
  write both join it onto `assembly_membership`, in Postgres and in DuckLake. The two
  assemblers disagree on the same molecule — one sample's identical 27 kb sequence is
  `circular-yes` to hifiasm_meta and `circular-possibly` to myloasm — so today which one
  bypasses binning as an LCG depends on which assembler ran; stored, that can be re-asked
  without re-assembling. Routing itself is unchanged: `circular-yes` is still the LCG rule and
  `circular-possibly` still goes to binning. myloasm's depth is the mean of its `depth-A-B-C`
  triple, which is the scalar myloasm itself derives from it (the `avg_cov` its own circularity
  gate tests, per myloasm's own source); hifiasm_meta's is the S-line's `dp:f` tag, previously
  discarded along with the rest of columns 4+. A GFA where NO segment carries that tag fails the
  step rather than storing a depth-less run; some segments lacking it does not, since a partial
  absence is consistent both with a moved tag and with an assembly the tool reported less about,
  and failing on it would discard a finished assembly to distinguish nothing. Such a row also
  stays readable — a NULL depth beside a non-NULL `raw_name` means the assembler reported on
  that contig without a depth, where a pre-sidecar run leaves all four NULL — and gets a count
  on stderr. One real metagenome assembled on the pinned build carried `dp:f` on all 2,899 of
  its contigs (depth 1–145), with every name matching the circular/linear grammar and none
  unmatched; that is one assembly of one input, and the grammar had until now been exercised
  only against synthetic single-contig assemblies. `mult` is NULL below 1 kb, where myloasm
  reports `0.00` for absence of signal rather than a measured zero. Attributes are NULL for every
  row written before this deploy and are not backfilled here: they are read out of the assemble
  step's output, which for an older run is gone. A MAG row reaches them through the binners,
  which are measured to preserve both assemblers' contig id shapes (see the entry under
  `Changed`), so the column comment states no condition; LCG and UNBINNED rows match by
  construction. `kind` still records the routing that was applied, so `circular-yes` stays
  recoverable from `kind` alone; what an older row cannot recover is the `possibly`/`no` split
  among the contigs that went to binning. `ensure_assembly_tables` widens an existing DuckLake
  `assembly_membership` with `ADD COLUMN IF NOT EXISTS` on data-plane start, since
  `ducklake_add_data_files` rejects a Parquet whose columns differ from its target's in
  either direction. The
  attribute half of the membership join (the representative-contig aggregate, the four-column
  projection, the LEFT JOIN) is shared by both writers rather than written twice. Both writers
  read the sidecar through one shared reader, which declares the two numeric columns rather
  than sniffing them (hifiasm_meta leaves `mult` empty on every row), verifies the header
  against the expected column order (`read_csv(columns=)` binds by POSITION and does not check
  it, so a reordered sidecar would silently transpose values), and rejects a repeated
  `contig_id`. Postgres COALESCEs the four columns on a re-run so a replay without a sidecar
  cannot erase them; the DuckLake copy is replace-keyed per run, so it reflects the LAST run.

- **Combined (inverted open reference) feature table: `estimate-feature-table` and `qiita
  feature-table build` can estimate over two alignment arms at once (#515).** Passing a second
  alignment — the cohort aligned against its OWN assembled contigs — builds one table over both:
  a read placed by the de novo arm is counted against its own contig, and the reference arm
  contributes exactly the reads the de novo arm did not place. Server-side it is
  `denovo_alignment_idx` on the ticket's `action_context`; client-side it is
  `--denovo-alignment-idx`. Absent either, both drivers behave exactly as before.

  The analytic lives in the new `qiita_common.analytic.reconcile`, shared by both drivers.
  Precedence is one DELETE applied to the reference slice as it is staged, so every reader after
  it — the coverage filter, the roll-up diagnostics, woltka's input — sees the reconciled arm.
  Three consequences are stated there because each looks like a result rather than an error: a
  reference genome that clears `coverage_threshold` in a reference-only table over the same cohort
  can drop out of the combined one; a read the de novo arm won can then fail that filter and be
  counted on neither arm; and the two arms were admitted by their producers under different rules
  on different axes, which precedence does not re-judge.

  **The de novo map carries `prep_sample_idx` and is joined on the pair.** A contig is
  content-addressed, so two cohort samples that assemble byte-identical contigs share one
  `feature_idx` under two genomes; the contig-only join returns both rows and `woltka_ogu` credits
  half of one sample's read to the other sample's genome. The per-genome length denominators
  deduplicate on `feature_idx` for the mirror of the same reason — a contig streamed once per
  sample would otherwise inflate its genomes' denominators and depress their breadth.

- **`GET /assembly/{prep_sample_idx}/{processing_idx}/genome-map` and `POST
  /assembly/{prep_sample_idx}/{processing_idx}/ticket/doget` (#515).** The de novo arm's two
  client-reachable surfaces: the run's contig→genome lookup (a control-plane read — `genome_idx`
  exists only in Postgres), and a human-callable assembly DoGet mint under the new
  `Scope.ASSEMBLY_DOGET`. That scope is split from `ticket:doget` on the argument
  `alignment:doget` already makes — the two carry different trust models, not different data — so
  it is on every human role ceiling and on no service ceiling. Both routes check per-study access
  BEFORE existence, inverting the alignment routes' ladder: the thing that may not exist here is
  `(this sample, this run)`, so a 404 first would disclose whether an unreadable sample was
  assembled, and both refuse a run whose `assembly_sample` gate does not read `completed` — the
  presence of membership rows never means the assembly finished. The genome map additionally
  refuses (422) a run whose memberships are not all genome-minted, because a map short by a contig
  does not read as short downstream: it reads as a genome covering more of a smaller length than
  it has.

- **Assembly subjects mint a `qiita.genome`, recorded on `assembly_membership.genome_idx` (#514).**
  Every assembled subject — each refined bin, each LCG contig, each unbinned contig — now mints one
  qiita-origin genome, keyed on the SHA-256 of `(prep_sample_idx, processing_idx, kind, bin_id)`
  (`repositories.assembly.assembly_genome_source_id`, the one definition the inline write and the
  backfill share). A bin's contigs group under one genome; an LCG or unbinned contig is its own.
  `write_assembly_membership` mints and stamps in the same batch, so the row and its genome are one
  INSERT. `qiita-admin backfill assembly-genome` converts runs that predate it — a pure Postgres
  replay, since the identity is a hash of columns already on the row.

  **The edge is deliberately NOT `qiita.feature_genome`.** An assembled contig whose bytes match a
  reference sequence resolves to the same content-addressed `feature_idx`, and that junction is both
  how a reference's genome map is derived (there is no reference→genome edge in the schema) and the
  resolution substrate for the GLOBAL `qiita.reference_exclusion` blocklist, which expands a blocked
  genome to all its features through it, unscoped by reference. Writing there would put sample-derived
  genomes inside every reference map sharing a contig and inside the blocklist's reach. A column
  rather than a second junction because a bare `(feature_idx, genome_idx)` table cannot express the
  per-run scoping consumers need: `qiita.genome` carries no `processing_idx`, and `prep_sample_idx`
  is identical for two runs of one prep_sample, so there would be nothing to filter the
  genome side on.

  The sequenced-pool delete now detaches `genome_idx` and deletes that prep_sample's assembly
  genomes before deleting the `prep_sample` — without it, every assembled pool would be undeletable,
  since `genome.prep_sample_idx` is `ON DELETE RESTRICT`. The genome delete is scoped
  `NOT EXISTS (… feature_genome …)`, because a reference load may legitimately declare qiita-source
  genomes and those carry a `prep_sample_idx` too. `assert_sequenced_pool_deletable` refuses such a
  pool up front with a 409 rather than letting the skip abort the delete after the DuckLake purge,
  which the Postgres rollback does not restore; `force` does not override that one, because it cannot
  make the genome deletable. One consequence, accepted: deleting an assembly genome retires any
  published `QF<n>` handle naming it, via `exported_feature.genome_idx`'s `ON DELETE SET NULL`
  trigger.

  `ASSEMBLY_MEMBERSHIP_JOIN_SQL` gained a `DISTINCT`, restoring the parity
  `jobs/assembly_load.py` already claimed: two byte-identical contigs in one bin resolve to one
  `feature_idx`, and the membership write now upserts (`ON CONFLICT … DO UPDATE`, so a replay
  re-stamps `genome_idx` instead of leaving a pre-mint row NULL) — which Postgres refuses when one
  statement touches a conflict target twice. `write_assembly_membership` accordingly returns a single
  count of rows written or re-stamped, rather than the `(linked, already_linked)` pair whose second
  element is now always zero.

- **A `qiita_sample_type` biosample global field, backed by a new internal controlled vocabulary (#509).**
  `Qiita Sample Type` is a terminology this database defines rather than loads from an external
  source: there is no release to load, new terms are appended directly, and `terminology.version`
  carries the date the vocabulary last changed. Twelve terms are seeded, spanning controls, marine
  and aquarium waters and filters, and clinical materials. The field is `terminology`-typed,
  so a value outside the vocabulary is rejected at write time and an import supplies the term_id
  (`sea_water`, `cerebrospinal_fluid`, ...). It is flagged `required`, which the import gate does
  not yet enforce.

- **An assembly run can be deprecated, and one of its samples withdrawn (#505).**
  `qiita.processing` — the canonical-params hash over `{workflow, version, mask_idx,
  assembler}` that `qiita.assembly_membership` and the DuckLake assembly tables are stamped
  with — gains `status` / `deprecated_at` / `deprecated_by_idx` / `deprecation_reason` /
  `superseded_by`, and `qiita.assembly_sample.state` gains `invalidated` with its own
  `invalidated_*` provenance. The assembly twin of the mask lifecycle, at the same two
  granularities: deprecating the CONFIG makes `qiita.mint_processing` raise SQLSTATE 23514
  rather than return the row, so the params that identify a run built from a pass-set later
  judged unsound cannot assemble another sample; invalidating a RUN withdraws one
  `(processing_idx, prep_sample)` pair. Neither deletes — assembly has no delete path, so a
  deprecated run stays listed, readable and DoGet-signable and the record of what produced
  published contigs survives the judgement about it. New `/processing` router: `GET` (list
  with a per-run four-state tally), `GET /{processing_idx}`, `GET /{processing_idx}/prep-sample`
  (all `prep_sample:read`, narrowed per study below `wet_lab_admin`), and `PATCH
  /{processing_idx}/status` / `PATCH /{processing_idx}/sample-status` behind a new
  `processing:lifecycle` scope at the `system_admin` ceiling. The de novo align resolver
  refuses an invalidated subject with a message naming the withdrawal instead of its
  "not complete yet" catch-all, and the two gate writers leave a withdrawn row standing —
  `finalize-assembly-sample` raises `AssemblySampleInvalidated` rather than re-completing it,
  which the runner's dispatch arm records as a BAD_INPUT step failure rather than letting it
  reach the UNKNOWN_PERMANENT catch-all. Three helpers the two lifecycles now share instead of
  duplicating: the per-study roster narrowing (`repositories._sample_scope`), the gate-state
  member assertion (`repositories.gate_state_literal`), and the PATCH bodies' reason gating
  (`models._base.check_withdrawal_reason`).

- **A reference or assembly load reports the records the canonical hash absorbed (#501).**
  `write-membership` and `write-assembly-membership` compare their manifest's record count
  against its distinct canonical hashes and, when they differ, log a warning naming the
  shortfall and the `read_id`s that shared a hash. `canonical_sequence_hash_expr` folds case
  and strand, so two records that are one sequence in two cases, or exact reverse complements,
  mint one `feature_idx` and only the lex-smallest `read_id`'s bytes are stored — the other
  record was previously absent with nothing recording it, because the manifest is a workflow
  artifact and the dropped bytes never reach the lake. Measured on the `fastp-adapters`
  reference: 234 submitted records, 177 features, 57 records absent. The warning reports
  *that* records collapsed and not why — the manifest carries no sequence, and an exact
  duplicate and a strand pair agree on both hash and length — so separating them means reading
  the submitted sequences. For an assembly it also counts one contig placed in several bins,
  which loses nothing (measured 0 across the deploy's assemblies). It warns rather than
  refusing because every one of those shapes is a valid submission.

- **The global sample field registries can be read back (#485).** `GET
  /api/v1/biosample-global-field` and `GET /api/v1/prep-sample-global-field` list every
  row of their registry, so a client can resolve a global field's idx through the
  API. Each row carries `internal_name`, the stable key a caller matches
  on, alongside the display name, data type, default tier, and terminology binding. Gated
  on the entity read scope (`biosample:read` / `prep_sample:read`) over the HumanUser
  baseline every read in these modules carries, with no study check at all: a registry is
  global, so there is no study to hold a tier on and a caller with no study grants still
  gets the full list.

- **A study's sample field definitions can be read back (#485).** `GET
  /api/v1/study/{study_idx}/biosample-field` and `.../prep-sample-field` list the study's
  field definitions, so a client no longer has to infer what already exists by attempting
  a create and interpreting the 409 from the `(study_idx, display_name)` unique
  constraint — which is what made bulk field-creation scripts impossible to re-run safely.
  Each row arrives with the values a globally-linked field inherits already resolved, so
  a caller sees one shape whether the field is linked or purely local. Gated on the
  entity read scope (`biosample:read` / `prep_sample:read`) at viewer tier — lower than
  the create route's admin, because these return field definitions and no metadata
  values.

- **`qiita <entity> list-fields` / `list-global-fields` (#485).** The user-CLI front end
  for the two field reads above, for both `biosample` and `prep-sample`.
  `list-global-fields` prints the registry so the idx `create-field
  --<entity>-global-field-idx` wants is obtainable, and `list-fields` prints a study's
  own definitions.

- **`qiita reference load` warns when the input carries soft-masked bases (#486).** On
  both front-ends — the `--fasta` upload and `--local`'s `stage_local_fasta`. The
  split normalizes case, so a soft-masked FASTA is stored upper case and the masking is
  not recoverable from the lake afterwards. Both front-ends share one predicate and one
  message (`soft_masked_expr` / `SOFT_MASK_WARNING` in `qiita_common.chunking`), and the
  predicate is built from the same `normalized_sequence_expr` the split routes through,
  so neither can disagree with what is stored. Every index builder discards case, so
  only an export of the reference shows the difference.

- **`align-denovo`: align a sample's masked reads back against its own assembly (#486).**
  A prep_sample-scoped workflow that aligns one sample against the contigs its own
  `long-read-assembly` run produced, into the same DuckLake `alignment` table under a de
  novo `alignment_idx`. The `align_denovo` native job streams both inputs at runtime —
  contigs over an assembly-run-scoped DoGet, reads over a `(prep_sample, mask)`-scoped
  `read_masked` DoGet — so nothing is staged onto shared scratch. It gates on the
  circular-pooled axis rather than per-record CIGAR, which is what admits a read crossing
  a circular contig's origin; that fixes `eqx := true` and `max_secondary := 0`, and the
  job docstring carries why and what the second one costs. First producer for
  `alignment_origin_spanning`, recording only groups whose coordinates show a single
  origin crossing. Submit-side, a runner pre-loop resolver mints the identity, refuses
  unless the assembly run's `assembly_sample` gate reads `completed` (NO_DATA when it
  reads `no_data`), re-attaches to `work_ticket.alignment_idx` on a resume rather than
  re-deriving, and writes both ticket columns plus the pending `alignment_sample` row.
  New `finalize-alignment-sample` primitive flips that gate. The step's `cpu:` equals
  the job's DuckDB thread count and `test_align_denovo_cpu_pins_duckdb_threads` keeps
  them equal: `align_minimap2` draws its parallelism from that pool (measured
  near-linear at 1/2/4/8 threads), so a higher `cpu:` allocates cores nothing uses. The
  same measurement closes the identical gap in `long-read-assembly`'s
  `assembly_coverage` step, whose `cpu:` drops 16 → 8 to match the pool it actually
  uses. Closed by lowering the request rather than raising the pool, because `sacct`
  over 49 completed steps puts that job at 87% of its 64 GB at the median, with ten
  further attempts dying OUT_OF_MEMORY at exactly 64.0 GB — the thread count is also a
  memory multiplier for the unspillable extension side, so raising it is the wrong
  direction. Its `mem_gb` is unchanged; see the job's sizing note.
  `test_workflow_params_pin.py`'s aside justifying the gap is corrected: the memory half
  of that rationale stands, but on the CPU axis the aligner's threads ARE the DuckDB
  pool.

- **`docs/duckdb-miint.md` records that `max_secondary := 0` is not "primary alignments
  only" (#486).** Upstream's reference says it is, in three places; a supplementary
  (`0x800`) record from a split alignment survives it, which is not primary under SAM's
  `FLAG & 0x900 == 0`. Measured on mirror `9fc4d12`: a read across a subject's start/end
  junction returns the same two rows at `max_secondary := 0` and at `100`. Filed upstream
  as [duckdb-miint#255](https://github.com/the-miint/duckdb-miint/issues/255) (docs-only —
  we carry no workaround, so it is not an Open-upstream-gaps row). The behaviour is what
  `align_denovo` needs: the setting removes the secondaries `circular_query_coverage`
  refuses and leaves the supplementary fragments it exists to pool.

- **`circular_predicate_sql` / `circular_cleared_join` in `qiita_common.analytic.gate`
  (#486).** The circular gate's predicate and its group-join key, factored out of
  `_circular_gated_sql` so a job that produces alignments applies the same two rather
  than a second copy. Both still take a `GateClearance`, so a producer reaches them only
  after `check_gate_diagnostics`.

- **A per-sample masked-read Flight stream for native jobs (#477).**
  `data_plane_client` gains `fetch_read_masked_doget_ticket` +
  `open_read_masked_stream`: a job names a `(prep_sample_idx, mask_idx)` pair, the CP
  signs a DoGet ticket scoped to exactly that pair, and the data plane's `read_masked`
  macro rows stream into a registered DuckDB relation. Per-SAMPLE, alongside the
  existing per-BLOCK `open_read_block_stream`; both scope keys ride the wire because
  the scope is one pair, not a member list that there would be a reason to keep CP-side.
  **Nothing calls it yet** — the de novo alignment job is the consumer and lands
  separately. The relation carries `sequence_idx`, which the CP-side FASTQ streamer
  (`runner/_read_ingest.py`) does not — it writes the VARCHAR `read_id` — which is
  why an `alignment`-producing consumer will stream here rather than reuse that
  FASTQ. The mask-completion gate needed no code here: the CP already 409s unless
  `mask_sample.state = 'completed'`, so 'pending', 'invalidated' and no-gate-row are
  all refused at mint and no consumer re-derives the check. No new scope
  (`read_masked:doget` is already in `SERVICE_ACCOUNT_SCOPE_CEILING`, and the compute
  service account's active token carries it — verified against the live control-plane
  database, not just the 2026-07-23 deploy archive), so no operator action.

- **Per-run contig read-back over Arrow Flight (#476).** `POST
  /assembly/ticket/doget` takes a `(prep_sample_idx, processing_idx)` pair and
  signs a DoGet ticket for that assembly run's contigs on
  `assembled_sequence` / `assembled_sequence_chunks`, which are now in the data
  plane's `ALLOWED_TABLES`. Neither table has a `prep_sample_idx` column — a
  contig is stored once under the content-deduped `feature_idx` every run
  producing those bytes shares — so `build_assembly_run_query` resolves the run
  through the lake's own `assembly_membership` as a semi join, the same shape a
  `reference_idx` filter takes on the reference tables. The signed scope is the
  pair itself: exactly one `prep_sample_idx`, exactly one `processing_idx`, and
  no third filter column. A `feature_idx` on these tables is refused outright, so
  no ticket can name a contig; an unscoped ticket cannot be built, so "every
  sample's contigs" is not representable; and several `processing_idx` values
  are refused rather than blending two runs into one indistinguishable stream —
  the same guard the single-`alignment_idx` rule makes. A pair no membership row
  names is a 404 at the route, never a ticket. The route reuses the service-only
  `ticket:doget` rather than minting a scope: that scope is on
  `SERVICE_ACCOUNT_SCOPE_CEILING` and on no human role ceiling, so the principals
  gaining contig read-back are exactly the service accounts already holding it —
  but this is the first *sample-derived* sequence surface it reaches, where every
  prior table was reference data or the derived per-read `alignment` slice.
  `assembly_membership` and `bin_quality` remain absent from `ALLOWED_TABLES`, so
  neither is nameable by a ticket; `assembly_membership` is read as the scope
  resolver and no column of it reaches a stream. The orchestrator seam is
  `data_plane_client.open_assembly_chunk_stream`, the assembly twin of
  `open_reference_chunk_stream`.

  Resolving the run in the data plane rather than signing a roster is what keeps
  the read off a per-key cost curve nothing bounds. Measured on DuckDB 1.5.4 /
  ducklake d318a545 against a catalog of 3.6M chunk rows over 200 files: a
  26,129-contig run as a literal `feature_idx IN (...)` is rewritten into a MARK
  join above an unfiltered scan — 3,600,000 rows scanned, 1,793 ms — while the
  semi join pushes the resolved keys' min/max and a Bloom filter into the scan as
  dynamic filters, reading 245,457 rows in 140 ms for the identical 235,161-row
  result. At a 4,968-contig run it is 233 ms against 37 ms. File pruning is
  unaffected: both forms open the same 200 files there, and where per-file
  `feature_idx` ranges are narrow enough to prune at all, both prune to the same
  1 file of 200.

- **`qiita feature-table build --circular-gate` — judge a read pooled over the records it
  was split into (#475).** A read crossing the origin of a circular reference held as a
  linearised contig is emitted as two records covering half of it each, so a per-record
  query-coverage floor discards it — silently, and worst for the small plasmids and phages
  most often recovered as complete circles. The new gate mode asks duckdb-miint's
  `circular_query_coverage` how much of each read one reference explains with every record
  pooled, and keeps a read whose coverage and pooled identity clear their thresholds and
  whose fragments lie on one strand. Both thresholds are parameters —
  `--circular-min-coverage` (0.90) and `--circular-min-identity` (0.95); the same-strand
  requirement is not, because fragments on opposite strands are not one molecule and
  pooling them manufactures coverage. It replaces `--min-identity` / `--min-query-coverage`
  rather than combining with them, refuses a slice holding secondary, unmapped or
  coordinate-less records (the macro cannot see those, so they would leave the table
  without failing any threshold), and refuses paired data, whose mates are separate
  molecules the pooling keeps apart. The bundle manifest records whichever axis was
  applied. (Note: the alignment ingest applies its own per-record 0.90 query-coverage floor,
  so a read split at an origin is dropped before it reaches the lake — this gate can only
  pool what was stored.)

- **A per-`prep_sample` alignment delete (#469).** The alignment delete surface had two
  scopes: `delete_alignment` (a whole `alignment_idx`) and `delete_alignment_block` (a
  block's member sub-ranges). A workflow that aligns one prep_sample per ticket fits
  neither — the whole-idx purge destroys every other sample's rows, and there is no block
  to read a cover-map from — so a re-run would double-count. The new
  `delete_alignment_sample` DoAction deletes the `(alignment_idx, prep_sample_idx)` pair's
  rows from every `ALIGNMENT_DELETE_TABLES` table in one transaction, and the
  `delete-alignment-sample` library primitive signs it. Idempotent (a sample with no rows
  deletes 0), replay-safe, and registered in `REPLAY_SAFE_ACTIONS`. The predicate carries
  no `sequence_idx` bound and is feature_idx-agnostic, so all of a read's rows go.
  `alignment` is not a `REPLACE_KEY_TABLES` entry instead: replace-by-key is matched on the
  destination table name alone, so a pair-keyed entry would also fire on the block-scoped
  `align` workflow's registrations and the second block of a split sample would delete the
  first's rows. `delete_alignment_sample_deletes_the_pair_only` pins the exactness on both
  tables: a sibling sample under the same alignment survives, and the target sample under a
  different alignment survives. The runner arm takes `alignment_idx` from the
  `work_ticket.alignment_idx` column, never from `action_context` (which the submitter
  chooses), refuses on NULL, and refuses when a context value is present and disagrees
  with the column — a step's `params:` binds `alignment_idx` from `action_context`, so a
  disagreement would clear one alignment's rows while the register that follows wrote
  under the other. `block_read.resolve_block_read_scope` makes the same cross-check on
  the block path. Plumbing only — no shipped workflow references the primitive yet, and
  none can until something writes `work_ticket.alignment_idx` on a prep_sample-scoped
  ticket (`align_planner.plan_and_submit_alignments`, the column's only writer today,
  inserts block-scoped ones).

- **Assembly completion is first-class state: `qiita.assembly_sample` (#467).**
  The per-`(processing_idx, prep_sample)` completion gate `long-read-assembly` was missing,
  alongside `qiita.mask_sample` and `qiita.alignment_sample`. Completion existed only as
  work-ticket state, and row presence does not stand in for it: `write-assembly-membership`
  writes `qiita.assembly_membership` several entries before `register-files` lands the
  DuckLake tables, so a ticket that dies in between leaves a partial footprint that looks
  finished. New table (`state` TEXT + CHECK over `pending` / `completed` / `no_data`, plus an
  index on `prep_sample_idx`), a repository layer, and a terminal `finalize-assembly-sample`
  library action appended to the workflow, which writes `'completed'`.
  The runner materializes the row `'pending'` right after it mints the run's
  `processing_idx`, and writes `'no_data'` from its `StepNoData` handler when the sample
  assembled no contig of any kind (the terminal action never runs on that path). Both key on
  the id the mint returned, never on `action_context` — a submitter can put a
  `processing_idx` key there and it reaches the runner's bindings intact. A re-run of the
  same identity reopens `'no_data'` back to `'pending'` and leaves `'completed'` alone; the
  `'no_data'` write reports whether it landed, and the runner WARNs with the run and sample
  when it did not, so the gate reading `'completed'` under a NO_DATA ticket is in the journal.
  `ActionDefinition` now refuses, at construction, an action that declares
  `finalize-assembly-sample` without threading `processing_idx` through some step's
  `params:` — the gate row would have no key. That covers the YAML sweep in CI and the
  `qiita.action` reconstruction alike; the runner keeps the same refusal per ticket.
  `delete_sequenced_pool_cascade` clears the new gate alongside `qiita.mask_sample` and
  `qiita.alignment_sample` before deleting `qiita.prep_sample`.
  A FAILED or cancelled ticket leaves the row `'pending'`: the two terminal writers are the
  only ones and nothing sweeps. Nothing reads the gate at submit time, so a stale `'pending'`
  refuses no re-run, and a re-run under the same params re-resolves the same key and closes
  it.

- **`qiita_lake.alignment_origin_spanning`, a side table for reads that cross a circular
  contig's origin (#465).** An aligner treats a circular contig as a linear one, so a read
  crossing the origin emits one SAM record per side of it, each covering only its own share
  of the query. The new table records the merged read — query interval, reference interval,
  strand, pooled identity and coverage, fragment count — one row per (read, feature), while
  the fragment rows stay in `alignment` unchanged. Its DDL in `ducklake.rs` carries the
  contract, the join key, and which producers it can describe: the sharded reference
  aligner is not one, because `align_sharded` applies `_MIN_QUERY_COVERAGE_MINIMAP2` per SAM
  record on the way into the staging Parquet and so drops an origin-spanning read's
  fragments before they are persisted. Storage and delete plumbing only — nothing writes the
  table yet, and `register-files` needs no change because it derives its filename→table map
  from the staging dir by stem. `delete_alignment` and `delete_alignment_block` now delete
  from a table list (`ALIGNMENT_DELETE_TABLES`) in one transaction rather than from
  `alignment` alone; `rows_deleted` is unchanged, still the `alignment` count.
  `alignment_delete_covers_every_alignment_scoped_lake_table` pins the list against the
  catalog, so a future table keyed by `alignment_idx` cannot skip the purge.

- **A contract test for how miint's minimap2 reports an origin-spanning read (#465).**
  Upstream documents the behaviour and ships the pooling for it
  (`circular_query_coverage`, `cigar_pooled_identity`); what this test pins is the tie to
  our own floor. Measured on miint `9fc4d12` (minimap2 `0477498`), 20 kb contig and a 6 kb
  read built across the origin: 2 SAM records, each `cigar_query_coverage` 0.5 at
  `cigar_sequence_identity` 1.0, under `map-hifi`, `map-ont` and the default preset.
  Concatenated with `string_agg(cigar, '')` the pair scores 0.5 coverage but 1.0 identity
  — a clip consumes query length without being an aligned column — while
  `circular_query_coverage` returns 1.0 coverage over 2 fragments. The control, a read of
  the same length from the middle of the same contig, gives one record at coverage 1.0.
  Every score is asserted against `_MIN_QUERY_COVERAGE_MINIMAP2`, so lowering that floor
  turns the test red instead of silently invalidating the side table's DDL scope claim.
  `test_origin_spanning_read_splits_into_one_record_per_side`.

- **`docs/duckdb-miint.md` records that `cigar_query_coverage` is per-record, and what
  that costs us today (#465).** The entry links upstream's circular-coverage contract
  rather than restating it, and states the standing consequence: because `align_sharded`
  scores the floor per SAM record, long reads crossing the origin of a circular reference
  contig are dropped before they reach `alignment` — silently, and concentrated on closed
  chromosomes, plasmids and phages. Also corrects the `align_minimap2` entry: `query_table`
  is the only positional argument, and `subject_table` / `index_path` are exactly-one-of
  rather than independently optional.

- **`scripts/lake-gc.sh` reports and reclaims unreferenced lake files (#472).** DuckLake
  never reclaims a data file on its own: deleting rows leaves the Parquet on disk and
  still held by the snapshots that predate the delete, so every `register_files`
  replace-by-key and every `delete_reference` / `delete_mask` / `delete_pool_reads` /
  `delete_alignment` has been accumulating. The script drives DuckLake's own
  `ducklake_expire_snapshots` → `ducklake_cleanup_old_files` → `ducklake_delete_orphaned_files`
  in that order, in ONE transaction in one duckdb process, using the documented `CALL`
  form — measured on 1.5.4, `cleanup_old_files` reports nothing until the snapshots
  referencing a file are expired (running it alone is a no-op) but does see that expiry
  from inside the same transaction. The transaction bounds the CATALOG only: an unlinked
  file stays unlinked through a rollback, also measured. It **reports
  by default**, prompts for a typed confirmation before acting, keeps 7 days of snapshot
  history unless `--older-than` says otherwise, and never passes `cleanup_all`. Orphan
  deletion is behind its own `--reclaim-orphans`: it is the only step that can reach a
  file belonging to a registration in flight, because `register_files` moves a Parquet to
  its lake path before opening the catalog transaction that registers it, and in that
  window the file is on disk with no catalog row — indistinguishable from an orphan.
  Steps 1-2 cannot reach it (`cleanup_old_files` only removes files the catalog once
  referenced), so plain `--reclaim` needs no quiescing.
  Registrations must be quiesced before reclaiming: `older_than` filters on filesystem
  mtime and `register_files` places files with `rename`, which carries over the mtime the
  producing job gave them in staging, so the cutoff does not bound "recently placed" —
  the script header carries that argument in full. Runs as `qiita-data`, the account that
  owns the data path.


- **`build_version` / `BUILD_VERSION` is now covered by tests (#309).** The landing page
  renders `settings.build_version or _PACKAGE_VERSION` (`landing.py`), so a from-source
  boot without `BUILD_VERSION` falls back to the static package version instead of the
  literal `None`. The new cases mirror the existing `build_sha` / `BUILD_SHA` pair — one
  asserting the calver is shown, one asserting the empty-string case normalizes to `None`,
  one asserting the package-version fallback — so both arms are covered in `test_landing.py`
  and `test_config.py`.

- **`DEPLOY_CHECKLIST.md` drops a stale `(#324)` note (#431).** That entry was verbatim in
  the archived 2026-07-30 deploy note, so an operator was being told to re-note a schema
  gate (`reference-add` / `local-reference-add` requiring a genome map when `shard_index` is
  true) already on a live deploy. It was the only copy — `main` carried the same guard from
  the moment #324 landed — and `deploy-note-check` ignores this file, so removing it is
  safe.

- **NCBI Taxonomy releases read from a taxdump archive (#439).**  `qiita-admin terminology prepare-taxdump --taxdump` reads a `taxdump.tar.gz`
  into the term rows of a release, so taxa no longer arrive as hand-written seed
  migrations. A live taxon takes its scientific name as its label and its genbank
  common name as its second name; a taxon NCBI merged away becomes an obsolete
  term pointing at the taxon it merged into; and a taxon NCBI deleted outright
  becomes an obsolete term with no replacement, so a reload never mistakes
  routine NCBI deletion for a terminology that silently lost terms. The archive
  is read in place, with nothing unpacked, through duckdb-miint's taxdump
  readers, so a member whose row layout contradicts what the taxdump documents
  refuses the read naming the line at fault — as does an archive recording one
  taxon as live, merged, and deleted at once. Term rows land in taxon-id order,
  so the same archive always yields the same release digest.

- **`terminology_term.alternate_label` — a second name for a term (#439).** Holds the
  name a source vocabulary supplies alongside the one that becomes the term's
  label; for NCBI Taxonomy the label is the scientific name and this is the
  genbank common name, which would otherwise be dropped and is what a person
  looking for a taxon is most likely to type. Nullable and single-valued, and
  bounded to the same width as the label so it stays a name rather than
  accumulating free-text definitions; an empty string is rejected, leaving NULL
  as the only spelling of absence. A release's terms table carries it and is
  authoritative for it, so a release supplying no second name clears any value
  stored against the term. A resolved terminology term carries it through
  metadata reads.

- **`qiita-admin terminology` — prepare and load an ontology release (#439).**
  `robot-command` prints the ROBOT export command to run against a staged OWL file;
  nothing in the control plane executes ROBOT, on any host. `prepare-owl` turns that
  export into the three files `load` consumes — a terms table, a header-only closure
  stub, and a manifest declaring the digests of both — recording a deprecated class
  as obsolete and an absorbed one as merged into the class that absorbed it, and
  keeping only the term ids of a chosen prefix so classes imported from other
  vocabularies stay out. Neither prepare command computes subsumption, so a loaded
  terminology resolves terms while subsumption queries have nothing to answer from.
  `load` takes the three files individually, verifies both tables against the
  manifest before parsing either, applies the release in one transaction, and reports
  how many terms were inserted, relabelled, obsoleted, or merged.
  `--tolerate-anomalies` absorbs three anomalies instead of refusing the load:
  terms the release silently dropped are auto-obsoleted, an unresolvable replacement
  pointer is recorded as a note, and a closure row naming an endpoint the release
  does not define is dropped, which lowers the reported closure count. A live term
  carrying a replacement pointer refuses the load either way.

- **What a terminology release must be (#439).** A manifest may name its tables only
  by bare filename, so no declared path can reach outside the directory the manifest
  itself sits in. A release may not reference a term it does not define — an obsolete
  term's replacement pointer and both endpoints of every closure row have to resolve
  within the release itself. Term rows are upserted, never replaced, so any term
  already referenced by biosample metadata stays resolvable; obsoletion is recorded
  on the row instead. A term the source does not name keeps the label already stored
  for it, falling back to its own term id when the database holds nothing, so a
  release that retires a term id without naming it cannot overwrite the name the term
  was loaded under. Each of a term's two names has its own counter, so a reload that
  changes only second names reports what moved rather than all zeros.
  `TerminologyStatus` and `TerminologyTermObsoletionKind` now live in
  `qiita_common.models.terminology` rather than `models.reference`; both stay
  importable from `qiita_common.models`.

- **Unbinned assembly contigs are stored, as a third `assembly_membership` kind (#460).**
  `assembly_hash` hashes the `noLCG.fa` residue — the contigs no DAS_Tool-refined bin
  claimed — alongside the circular genomes and the refined MAGs, so they are minted a
  `feature_idx` against the shared `qiita.feature` and recorded under `kind = 'UNBINNED'`
  with the contig id as `bin_id` (the `(kind, bin_id)` shape `LCG` already uses). Unbinned
  contigs can be valid sequence, DNA viruses in particular; previously they died with the
  workspace.
  **It is the residue, not all of `noLCG.fa`.** The refined MAGs are drawn from those same
  contigs, so hashing the file whole would give every binned contig a second membership row
  for one `feature_idx` — the bytes dedup, the membership does not. The exclusion is keyed
  on the canonical sequence hash, not the contig id. Id preservation through binning and
  refinement is measured for `hifiasm_meta` only: 198,747/198,747 refined-bin records across
  57 assembly workspaces on the deploy host carried a first-token contig id identical to
  their `noLCG.fa` record (whole-header on a 6-ticket subset, 5,660/5,660). It is
  unmeasured for `myloasm` — no myloasm assembly exists on the host, and its header
  grammar differs — so an id key would rest on an unmeasured assembler; the same match
  measured there is what would make one viable. Two consequences of keying on content —
  a bin holding a contig on the opposite strand still excludes its noLCG record (the
  canonical hash folds both strands), and noLCG records sharing a canonical sequence
  leave the residue together.
  `StepNoData` narrows to match: only an assembler that produced no contig at all is
  no-data, so a sample whose contigs all went unbinned now stores them. The `kind` value set
  moves to `qiita_common.assembly_constants`, the contract layer both Python services
  depend on, and the Postgres and DuckLake `assembly_membership` comments name it instead of
  enumerating members (a comment-only migration).

- **A published feature table's rows can now be labelled without our identifiers (#448).**
  `POST /exported-feature` mints the public handle for a feature-axis entity, the way
  `/exported-identifier` already does for the sample axis — so a table, its taxonomy sidecar
  and its sheared tree can all label a row the same way, which is what lets them be used
  together. It is deliberately a **hybrid**: a real accession wins wherever one exists
  (`genome.source_id` for a genome, `reference_membership.accession` for a feature, which is
  why that kind is keyed on the `(reference, feature)` pair — identical bytes can be named
  differently in two references), and a minted `QF<n>` is the fallback for an entity with no
  accession. An accession is something a reader can actually resolve; replacing
  `GCF_000006605` with a handle of ours would make the artifact worse.
  **A collision is not an error.** Two genomes can share a `source_id` under different
  `source`s, so the published namespace is a UNIQUE index and the mint resolves a clash by
  giving the loser a minted handle — while still reporting the accession it wanted, so nobody
  has to guess why one label changed shape. That arbitration cannot live client-side: a caller
  sees the entities of one artifact, never the accession someone else published last week. Nor
  can it live in the generated column, which sees only its own row. Where two entities in one
  request want the same accession, the lower identifier keeps it.
  An identifier is retired rather than deleted when the thing it named goes away, and what
  happens to its string then depends on the kind: a **genome** releases its accession, because
  the source's name for an organism means the same thing when the genome is re-loaded; a
  **feature** reserves its accession forever, because a FASTA header names nothing outside the
  load that emitted it and releasing it would let one published label come to name two
  different sequences. The cost is accepted: re-loading a reference gives its features minted
  handles instead of the accessions they had.

- **A published bundle can now say what produced it, without naming an `alignment_idx` (#448).**
  `POST /exported-processing` mints `QP<n>` for a processing — the third axis alongside
  `/exported-identifier` (a table's columns) and `/exported-feature` (its rows). Coverage
  filtering makes a feature table a function of the cohort it was built over rather than of the
  samples in it, so a record of the processing is what makes the table reproducible at all, and
  the only handle for it was internal. Unlike the feature mint this one is entirely minted with
  no accession half: a processing is something we performed, so no outside authority has a name
  for it. The request carries the **cohort** as well as the alignment, and takes
  `/exported-identifier`'s gate step for step over that pair — the cohort authorizes and is not
  part of the handle, so two callers publishing different slices of one processing still cite it
  identically. Minting is a write, and without that gate any human could collect a handle for
  every processing in the system, including ones over data they cannot read.
  `GET …/sequenced-pool/{pool}/alignment` now also reports each definition's **`params_hash`**
  (hex) — the digest the control plane deduplicates on. It is the server's own answer to "was
  this the same processing", so it is what a manifest cites for reproducibility, and it is
  reported from storage rather than recomputed on the way out. The CLI **recomputes it from the
  `params` beside it and refuses to build on a mismatch**, which turns "the config arrived
  intact" from an assumption into a checked fact — everything the build derives, starting with
  which reference the whole table is relabelled through, is read off those params.

- **A feature table can now ship its taxonomy, keyed the same way as its rows (#448).**
  `qiita feature-table build --taxonomy` writes a third bundle member: one row per published
  row, carrying the same `feature_id` and the eight ranks with their `d__`/`p__` prefixes
  restored, so the two files join on one column. Parquet with eight columns rather than a
  lineage string, because `concat_ws` skips NULLs — a lineage missing a middle rank silently
  promotes every rank below it, so `d__Bacteria;f__Listeriaceae` reads as though the phylum
  were Listeriaceae. An unclassified genome appears with NULL ranks rather than being left
  out: a row missing from the sidecar reads as unclassified anyway, and nothing would
  distinguish the two. A sidecar that does not describe the table beside it — short,
  duplicated, or unnamed — is refused, since each of those is silent in a file people will
  join regardless. The per-genome reduction behind it is now shared with the shard planner,
  so a genome that tiles under one lineage publishes those same ranks.

- **Every feature-table bundle now carries a manifest, and it is not optional (#448).** Coverage
  filtering makes a table a function of the whole cohort it was built over rather than only of
  the samples in it, so the same processing over a different cohort yields a different table —
  a table without this record cannot be reproduced. `<stem>.manifest.json` names the processing
  by its minted public handle **and** by the content-derived `params_hash` the client verified
  before reading anything off `params`; the reference by name and version; the aligner, coverage
  scope and threshold, and the gate if there was one; the cohort as `export_id`s; the published
  row count; the bundle's own file list; and the versions of duckdb, the loaded miint build and
  this CLI. The miint build is read from the catalog rather than assumed, because every
  bioinformatics primitive is compiled into the extension, so two clients on different builds
  can produce different numbers from identical input.
  **No internal identifier appears anywhere in it** — that is what the two mints were for.
  `params` is not copied verbatim for the same reason (it carries `reference_idx`, `mask_idx`
  and `shard_ids`), so `aligner` is lifted out and the digest stands for the rest. There is no
  build timestamp, deliberately: nothing would use one, and leaving it out makes the manifest a
  pure function of its inputs, so two bundles built the same way are byte-identical and can be
  diffed to show it.

- **A feature table can now ship the reference's tree, sheared to the rows it publishes (#448).**
  `qiita feature-table build --tree` writes the phylogeny pruned to the published keep-set,
  as a node table (`node_index, name, branch_length, edge_id, parent_index, is_tip`) whose tip
  names are the table's own `feature_id`s. Pruned ancestors have their branch lengths **summed
  onto the surviving edge**, so a tip-to-tip distance in the shipped tree is the distance in
  the whole one. Parquet rather than Newick, so the file needs no convention to read and the
  Newick writer's edge-id default is the consumer's decision instead of ours; `edge_id` rides
  along because it is the handle back to the reference's placements. The tips are renamed
  *before* the shear rather than translated after, which is what makes one vocabulary
  structural — the keep-set and the tree cannot disagree about which tip is which — and it
  leaves an unpublished tip nameless, so a reference-internal FASTA header cannot reach a
  published file. Four things are refused rather than approximated, each of them otherwise a
  tree somebody would join to the table anyway: a reference with no phylogeny, a published row
  with no tip, a row owning more than one tip (a contig-level tree, which the shear would
  happily emit with duplicate tip labels), and a tip belonging to more than one **published**
  genome (a plasmid published under two organisms cannot be one genome-named tip — whereas one
  merely *present* under a second genome the table never mentions shears cleanly). Publishing
  no rows at all is not one of these: an empty cohort gets an empty tree, the way it already
  gets an empty table.

- **The build now reports what it could not roll up (#448).** The genome map's INNER JOIN silently
  drops alignments to features with no genome, and for some references that is most of what
  was streamed. A build that cannot carry everything says the share and why — a
  feature-rooted table is not built yet — rather than leaving a table that is quietly a
  fraction of the data.

- **The feature-table analytic is now shared, and breadth of coverage has a per-sample
  scope (#448).** The SQL that turns alignment rows into an OGU table moved out of the
  compute-orchestrator job into `qiita_common.feature_table`, so the upcoming client-side
  recipe runs the same analytic rather than a second copy of it — the relation names, the
  pre-woltka survivor join, and the full-genome-length denominator are all one definition
  now. Text only; `qiita-common` gains no `duckdb` dependency. On top of that, coverage can
  be measured **per `(sample, genome)`** instead of pooled over the whole cohort: pooled
  keeps delegating to miint's `genome_coverage`, while per-sample reproduces that macro's own
  method with one more `GROUP BY` key, over the same denominator, so one threshold means the
  same thing either way. The scopes are asymmetric on purpose — pooling unions intervals, so
  pooled breadth is always at least the best single sample's, and per-sample can only ever
  remove rows. Behaviour is unchanged for the existing server-side job, which stays pooled.

- **Alignments can be gated on CIGAR sequence identity and query coverage (#448).** A gate filters
  the breadth calculation as well as the counts — an alignment that fails it is not a
  placement, so it must not contribute covered bases — and `cigar` rides the signed projection
  only when a gate reads it. Paired data is judged a placement at a time, so mates are kept or
  dropped together rather than orphaned, using the same placement key the orchestrator's
  aligner uses (now shared, one definition, instead of a second copy). Three ways the gate
  could have quietly produced a wrong answer are refused with an actionable message instead:
  a slice whose CIGARs cannot be scored for identity at all (which would have dropped every
  row and returned an empty table), a paired placement whose mate is missing or unscorable
  (which `string_agg` would have scored on the surviving mate alone), and an unpaired gate
  applied to paired data. The gate SQL is unreachable without having run those checks.

- **A feature table can be relabelled to public identifiers (#448).** The counts come out keyed by
  `prep_sample_idx` and `genome_idx`, which mean nothing outside this system; the relabel
  joins the genome map and the exported-identifier mint to name them by the genome's
  `source_id` and the sample's minted `export_id` instead, and the relabelled relation carries
  those two columns and the value — not the identifiers it joined on. That also makes the
  table writable as BIOM at all, which requires both id columns as text. Four ways the
  relabel could have published a wrong table are refused with an actionable message: a label
  relation that repeats a key (which would inflate every count for it), a count whose genome
  or sample has no handle (a NULL id in a published file), and two genomes or two samples
  sharing one handle (which a BIOM write would silently sum into one row). The
  `source_id`-collision refusal is scoped to the genomes a table actually emits, so a
  collision elsewhere in the reference does not fail a build whose output is correct. An empty
  cohort travels the same relabel as a populated one, so it cannot yield a differently-shaped
  file. On the client side, the two maps are fetched, staged into DuckDB with explicit native
  types, and released once copied; the genome map's roll-up key and public label are staged
  from the one response so they cannot disagree about which genomes exist.

- **A feature table can be written as Parquet or BIOM, bundled with the map needed to read
  it (#448).** One format per run — they hold the same numbers, so the choice is only about what
  reads the file next, and Parquet is the default. **The caller names the table** and the
  identifier map is named after it, so a pair stays visibly together and two builds of one
  cohort can share a directory; a name whose extension contradicts the requested format is
  refused rather than quietly rewritten. The bundle is both files or neither: the table names
  its samples by their public handle alone, so without the exported-identifier map beside it
  nobody can join it back to their own records. An artifact already at either name is refused
  before anything is written — the two writers disagree on their own about overwriting, one refusing and one
  replacing silently, so a second run would otherwise destroy a published file in one format
  and fail in the other. The map carries, in the file rather than only in the terminal, the
  warning that it is the one artifact holding an internal identifier and must not be shipped
  onward. BIOM is a new surface for this repo, so what miint's writer does with our data is
  now pinned by its own contract test and documented: it sums duplicate entries silently,
  drops zeros, refuses NULL and empty identifiers, requires `value` as DOUBLE exactly, and
  **ignores any extra column** — so it is the relabel's projection, not the writer, that
  keeps our identifiers out of a published file.

- **`qiita feature-table build` — a feature table, computed on your own machine (#448).** The first
  CLI surface for any of the analytic-export routes, and the first thing that composes them:
  the alignment slice and the reference lengths arrive as Flight streams, the genome map and
  the public sample handles as REST reads, and the analytic, the relabel, and the write all
  run locally under the caller's own token. Nothing is computed server-side and nothing is
  persisted. `qiita alignment list` and `qiita alignment cohort` are the discovery half —
  which alignments ran over a pool, and which of its samples you may build a table from —
  since an `--alignment-idx` was otherwise unobtainable without a psql shell. Both scope
  their answer to what the caller may read, which is what makes them agree with the ticket
  mint. **There is no `--reference-idx`:** the alignment records the reference it ran
  against, so a caller cannot name a different one and fetch the wrong genome map. The
  cohort defaults to the pool's full mintable set and can be narrowed sample by sample —
  which changes the table rather than filtering it, since breadth of coverage is measured
  over the cohort. A CIGAR gate is opt-in per threshold, pools a placement's mates unless
  told otherwise (correct for single-end data too, and the cheap alternative loses
  correctness silently), and pays for the wide `cigar` column on the wire only when it is
  asked for. Everything cheap and refusable happens before the two bulk streams: a wrong
  alignment, an empty cohort, an over-cap genome map, or an output name already in use all
  stop the run before a byte of alignment data moves.
- **Mask lifecycle: a config can be deprecated, and an individual run withdrawn (#445).**
  A mask's `params_hash` covers the resolved thresholds, not the code that applies them
  (`filter_version` is the workflow YAML version, not the miint build), so a config whose
  scoring turned out wrong re-resolved to the same `mask_idx` and masked new data with the
  same defect. There was nowhere to say so: `qiita.mask_definition` had no lifecycle column,
  and `mask_sample.state` was a two-value completion gate.

  Two markers, because they answer different questions. `mask_definition.status` =
  `'deprecated'` says the CONFIG is void — `qiita.mint_mask_definition` refuses to return the
  row (SQLSTATE 23514, surfaced as a 409 by `POST /mask-definition`) and align planning
  refuses it, which is what stops new bad data. `mask_sample.state` = `'invalidated'` says a
  RUN of a sound config is not trustworthy: one measured incident had 26 prep-samples under
  one mask of which 7 classified wrongly, so deprecating the mask would have voided 19 sound
  results to flag 7. Both carry who/when/why, enforced by a biconditional CHECK.

  Invalidation is a `state` VALUE rather than a column beside `state` so every masked-read
  consumer refuses it without being edited — the gate contract is that a consumer proceeds
  only on `'completed'`, so a third value is refused by construction. Neither marker deletes:
  the GET routes keep listing deprecated masks (with an optional `?status=` filter) so what
  filtered published data stays answerable, and `samples_invalidated` is tallied separately
  from `samples_pending` rather than folded in.

  New: `PATCH /mask-definition/{idx}/status`, `PATCH /mask-definition/{idx}/sample-status`
  (bulk, since the judgement is made per cohort), the `mask_definition:lifecycle` scope at
  the system_admin ceiling, and `MaskDefinitionStatus`.

- **The two maps that turn alignment rows into a feature table (#438).** A client can now
  mint a ticket for its alignment cohort but cannot label the result: alignment rows carry
  `feature_idx`, and a published table needs genomes and public sample names. Both
  translations are control-plane REST reads rather than signed Flight tickets, for one
  reason — their columns exist **only in Postgres**, so there is nothing for the data plane
  to serve.

  - `GET /api/v1/reference/{reference_idx}/genome-map` — the whole reference's
    `feature_idx` → `(genome_idx, source, source_id)` lookup, ordered by
    `(feature_idx, genome_idx)`. Both genome columns ship because `qiita.genome`'s
    uniqueness is the composite `(source, source_id)`, so a consumer relabelling
    `genome_idx` to a public id needs the pair to assert no collision. A feature shared
    across genomes (a plasmid) contributes one entry per genome, so the count is of
    PAIRS. Same INNER JOIN as `export_member_genome`, whose Parquet the compute side
    already consumes — pinned by a test that compares the route's pairs against the real
    exported file, because the two silently disagreeing about which features have genomes
    would make the client's roll-up diverge from the cluster's.

  - `POST /api/v1/exported-identifier` — mints the public `export_id` (`QM<n>`) for each
    processed sample in a cohort, backed by a new `qiita.exported_identifier` table.
    **Replaces a composed label that leaked our identifiers.** The earlier design named
    samples `<accession>.<run>.<pool>.<prep_sample>` (or `<accession>.<prep_sample>`
    unpooled), and both forms publish internal idxs — meaningless outside this system,
    revealing of our structure, and not a handle we promise to keep. No accession can
    substitute: a biosample sequenced repeatedly has several prep_samples, so its accession
    cannot say which sequencing a row came from, and `ena_run_accession` is NULL until
    submission. An identifier names a *processed* sample, `(alignment_idx,
    prep_sample_idx)` — unique at rest by `qiita.alignment_sample`'s primary key, with
    `alignment_idx` subsuming reference, aligner, mask and shard-set via
    `alignment_definition`'s params hash — so the same sample under two alignments gets two
    handles. Idempotent by a partial unique index on live rows, so a published identifier
    is stable. `export_id` is a `GENERATED ALWAYS` column: Postgres is its only author, and
    it can be neither forged by a caller nor edited after publication. Never deleted, only
    retired — purging an alignment detaches and retires the row (the `ON DELETE SET NULL`
    plus a retire-on-detach trigger, which is what lets that purge satisfy the
    exactly-one-processing check at all), so a citation keeps resolving and says what
    happened. Forward-planned for other processing types by `num_nonnulls`, the same idiom
    as `qiita.reference_exclusion`. Dropped the label's 422 on a missing
    `biosample_accession`: an identifier is always constructible, so an unaccessioned
    sample is now nameable. POST because a cohort routinely spans pools and would exceed
    nginx's 8 KB request-line cap in query params.

  The genome map **refuses with a 413** above its cap rather than truncating, naming the
  real size — the one capped read here that does. A lookup table silently missing rows
  drops those features from the caller's roll-up, producing a *wrong* feature table rather
  than a partial one, and nobody checks `truncated` on a map. JSON for both is a known,
  accepted limit: the first real reference that trips the 413 is the trigger to build the
  streamed Parquet form, not to raise the cap. Measured at 196 ms for a 250 001-row fetch
  off a 1M-member reference, with no sort node — the existing primary keys serve both the
  filter and the ordering.

  The label map is the human alignment mint's sibling by construction: same cohort cap,
  same `Tier.VIEWER` all-or-nothing gate via `filter_prep_samples_caller_can_read`, same
  refusal wording, and the same access-checked-before-anything-else ordering, so a refusal
  never discloses which samples exist for a cohort the caller may not read. No new scope
  (`reference:read` and `prep_sample:read` respectively), no migration, no deploy note.

- **A client-side way to discover a `mask_idx` (#423, closes #345).** Continuing a masked pool into
  `long-read-assembly` requires a `mask_idx`, and nothing outside a psql shell could
  produce one: `mask-definition` had only `POST` (mint) and `DELETE`, and the admin
  masked-read-export roster takes `mask_idx` as an *input*. Four reads close it:

  - `GET /api/v1/mask-definition` — masks newest-first, each with its config `params`
    and a per-mask `samples_completed` / `samples_pending` tally under the same
    optional `sequenced_pool_idx` / `prep_sample_idx` filters. A pool carrying several
    masks is separable in one call: `params` distinguishes them by config, the tally
    says which is usable.
  - `GET /api/v1/mask-definition/{mask_idx}` — one mask's config, so what a filter ran
    with is quotable rather than read out of the orchestrator source.
  - `GET /api/v1/mask-definition/{mask_idx}/prep-sample` — the per-sample roster.
    Reads the `qiita.mask_sample` gate row where one exists, and the sample's own
    per-sample masking ticket (`read-mask` or `fastq-to-parquet`) where it does not.
    The per-sample path writes its gate row `'completed'` in one upsert at the
    terminal step, so a ticket that ran and did not complete leaves no gate row —
    indistinguishable, in the gate alone, from a sample nobody tried to mask. The
    admin export roster LEFT JOINs the gate table and so reports NULL for exactly
    those; this one names them, with `source` saying which source answered and
    `work_ticket_state` separating a running ticket from a failed one. A ticket the
    runner has not started carries no `mask_idx` yet and so appears under no mask.
  - `mask_idx` on `WorkTicket` / `WorkTicketSummary` (nullable). The column existed and
    the runner wrote it; the API dropped it, so `qiita ticket status` on a read-mask
    ticket could not name the mask it produced.

  Gated `Scope.PREP_SAMPLE_READ` at `require_human` — no new scope, so no deploy note.
  `long-read-assembly`'s audience includes a plain `user`, so an admin-only discovery
  path would put the workflow out of reach of its own audience. Below `wet_lab_admin`
  the reads narrow to samples the caller has study-admin on, the same per-study policy
  `POST /work-ticket` applies at submission; the narrowing also decides which masks the
  list returns, so a zero-tally row never reveals a mask whose samples were filtered
  out. The privacy-sensitive pulls (`read_masked:doget`, `admin:masked_read_export`)
  are unchanged.

- **`qiita mask list` / `show` / `samples` (#423).** The user-CLI front end for the
  reads above — read-only, in the regular CLI rather than `qiita-admin`, which keeps
  the destructive `mask delete` / `purge-failed`.

- **`updated_at` on both sample-metadata tables (#386).** `qiita.biosample_metadata`
  and `qiita.prep_sample_metadata` now carry an `updated_at` bumped by the same
  `set_updated_at()` trigger the rest of the schema uses, so an overwritten metadata
  value records when it was overwritten rather than only when it was first written.
  The trigger is unscoped, so the column tracks any change to the row — including the
  `global_field_idx` denormalization written when a study field is upgraded from
  local to global — which is what makes it safe to use as the row's version. Rows
  predating the migration carry its timestamp; they are deliberately not backfilled
  to their `created_at`, since that UPDATE would be refused for published rows and
  would falsely bump every parent's `last_metadata_change_at`. Not exposed on the
  read wire.

- **Create a study-local prep_sample field — `POST /study/{study_idx}/prep-sample-field`
  (#386).** Mints a prep_sample field definition on one study, either purely-local
  (the caller states `data_type` and its options) or linked to a
  `prep_sample_global_field` whose `data_type` / `required` / terminology / tier are
  then inherited and resolved on read. Not an upsert: a name already on the study is
  a 409. Gated at `Tier.ADMIN` study access (wet_lab_admin+ role bypass) with the
  `prep_sample:write` scope, matching its biosample counterpart; the ADMIN bar is an
  interim stand-in until per-field visibility-tier enforcement lands, after which the
  route returns to `Tier.MEMBER`. Closes the gap that left study-local prep_sample
  fields readable and writable but impossible to create. Reachable from the CLI as
  `qiita prep-sample create-field`, whose flags mirror `qiita biosample create-field`.

- **BOOLEAN-typed sample metadata values (#386).** A biosample or prep_sample
  field declared `data_type=boolean` now accepts values: the text `true` or
  `false` (case-insensitive, surrounding whitespace ignored) is stored in
  `value_boolean` and read back as a JSON boolean. Any other text is a 422
  naming the two accepted forms. The per-data_type value-column map is now the one
  source for every read's column list, so a data_type can no longer be
  writable but absent from a read.

- **Study-scoped biosample and sequenced-sample metadata read + write —
  `GET`/`PATCH /study/{study_idx}/{biosample|sequenced-sample}/{idx}[/metadata]` (#386).**
  Four routes expose a study's view of a sample's metadata. The two GETs return the
  entity's core row plus its globally-linked metadata and this study's purely-local
  metadata (`StudyScopedBiosampleResponse` / `StudyScopedSequencedSampleResponse`);
  the two PATCHes upsert text values keyed by field display_name against the study's
  existing global or study-local fields, reporting per field the write outcome
  (inserted / updated / unchanged) and the `internal_name` the value reads back
  under — populated for a globally-linked field, null for a purely-local one, since
  a global value comes back in `global_metadata` keyed on `internal_name` rather
  than on the display_name the caller wrote. `scope` is derived from that field
  rather than stored, so the two cannot disagree. A PATCH body may set
  `global_internal_names` to key global fields on `internal_name` instead, matching
  the import path's flag, which makes the write key and the read key the same for a
  direct global match; a key naming a study-local alias of a global field still
  resolves through the alias, so `internal_name` remains the reliable read key.
  Sequenced-sample metadata lives on the supertype prep_sample. All four are clamped
  to `Tier.ADMIN` study access (wet_lab_admin+ role bypass), an interim stand-in
  until per-field visibility-tier enforcement lands. A sample not linked to the path
  study is 404 (indistinguishable from nonexistent); a retired sample is 404 on read
  and 409 on write. The PATCH carries no If-Match — a cross-study slot collision is a
  409, but a same-study rewrite is last-writer-wins. The biosample surface also
  returns the owner-biosample-id row on read while refusing to write it (422).
- **Create a study-local biosample field — `POST /study/{study_idx}/biosample-field`
  and `qiita biosample create-field` (#386).** Mints one
  `biosample_study_field` definition (no metadata value) in either mode: purely-local
  (supply `--data-type`, optional `--required`/`--terminology-idx`/`--tier-override`)
  or globally-linked (supply `--biosample-global-field-idx`, inheriting the type
  columns from the global field). Requires `biosample:write` scope and `Tier.ADMIN`
  study access (wet_lab_admin+ role bypass) — an interim stand-in until per-field
  visibility-tier enforcement lands, after which the route returns to `Tier.MEMBER`.
  A field of that name already on the study is a 409; the 201 body is the created
  field, with a linked field's inherited `data_type`/`required`/`terminology_idx`
  resolved on read.

- **`qiita-admin backfill mask-adapter-hash` — re-key mask_definition rows onto
  the current adapter-identity derivation (#428).** The mint converts a row when
  something re-mints its config; this converts the ones nothing re-submits, so
  the contract phase has a column free of NULLs to read as its go-ahead. A row
  records the adapter *hash*, not the reference behind it, and recomputing the
  legacy digest to attribute it would need the adapter Parquet bytes — the
  dependency the change removes. So rows are grouped by stored hash: one distinct
  value is the single canonical adapter set and converts; more than one is
  reported unwritten — `--mask-idx N --attribute-all` is how an operator resolves
  that residue by stating the attribution themselves. Dry-run by default (the
  plan carries every value the write uses, including the collision check),
  `--execute` to write, idempotent.

- **A runtime control surface for the fan-out dispatch throttle (#406).** The
  per-cohort in-flight cap was a single boot-time global (`FANOUT_MAX_INFLIGHT`), so
  retuning one fan-out meant editing an env file and restarting the control plane —
  which, with in-flight tickets, costs an unthrottled resume of every one of them.
  Three routes on the work-ticket router, all reusing `work_ticket:cancel`
  (system_admin, no new scope): `GET /work-ticket/fanout` lists every cohort with
  held or in-flight children and its throttle state; `PATCH
  /work-ticket/fanout/{kind}/{key}` sets or clears that cohort's cap **and pumps it
  in the same call**; `POST /work-ticket/fanout/{kind}/{key}/pump` re-triggers a pump
  without changing the cap. Pumping inline is the feature, not a convenience: the
  pump is edge-triggered (a child's terminal transition, or startup reconcile), so a
  route that only recorded the cap would look inert until unrelated work finished.
  Every response carries the full status block, so `released: []` is never ambiguous
  — `fail_stopped` distinguishes "frozen by a failed child" from "no free slots".
  The override is deliberately **in-memory and process-local**: a restart reverts to
  the `FANOUT_MAX_INFLIGHT` default, which is the conservative direction for the
  raise-the-cap case this serves, and it avoids a migration for an incident-time
  knob. Capped at 100 (`MAX_FANOUT_OVERRIDE`), enforced at the request model, the CLI
  flag, and the registry — the throttle exists because ~1000 concurrent data-plane
  streams exhausted its file descriptors, and a typo'd `1000` would walk back into
  that. Driven by `qiita-admin fanout {list,set,pump}`, whose stderr summary names a
  fail-stopped cohort explicitly and spells out the other reason for a zero.

- **A pool's per-sample read table without a host shell (#427, closes #348).** Two additions,
  because read counts are a property of the SAMPLE while state and step placement
  are properties of the TICKET:
  - `GET /work-ticket` takes `?sequenced_pool_idx=`, `?prep_sample_idx=` and
    `?action_id=`. The pool filter matches a ticket by any of the three ways a
    ticket reaches a pool — pool-scoped (bcl-convert), on one of the pool's samples
    (read-mask, the join that also feeds `read_outcome`), or on a block covering one
    of them (read-mask-block, whose own `prep_sample_idx` is NULL). Filters
    AND-compose with the existing originator scoping, so a pool filter is not a way
    around "you see only tickets you originated". `qiita ticket list` gains
    `--sequenced-pool-idx` / `--prep-sample-idx` / `--action-id`.
  - The pool- and run-scoped `sequenced-sample/list` rosters now carry each sample's
    four per-stage read counts plus `fraction_passing_quality_filter`. A sample with
    no ticket, and a sample masked through the block path, reports its counts here —
    neither is reachable through the ticket list.

  Before this, assembling a pool's per-sample read decay meant paging every ticket the
  caller ever originated and filtering client-side, or `psql` on the deploy host.

- **`make lake-shell`: an ADMIN-ONLY read-only DuckDB shell for debugging the live
  system (#418).** `scripts/lake-shell.sh`. Inspecting DuckLake ad-hoc meant hand-assembling
  an `ATTACH` from the service env files — risking a writable attach against the live
  lake — or borrowing a service account. This attaches both catalogs `READ_ONLY`:
  `qiita_lake` (DuckLake) and `qiita_cp` (Postgres, tables under `qiita_cp.qiita.*`),
  so one query can join lake data to control-plane metadata while DuckDB rejects every
  write. **It is a debugging tool, not a data-access path**: it bypasses every
  authorization check the API enforces and therefore sees all studies, so read-only is
  not permission — treat what it shows as confidential. Not an export path, not a
  user-facing query interface.

  Needs no root: the group-reads the operator already grants on
  `/etc/qiita/data-plane.env` (`root:qiita-data`) and `PATH_PERSISTENT/ducklake`
  (`qiita-data:qiita-data`) suffice, and a `READ_ONLY` attach performs no catalog
  writes at all. miint is loaded from the deploy-staged `MIINT_EXTENSION_DIRECTORY`
  (read from `control-plane.env`, else `compute-orchestrator.env`) so queries behave as
  they do in a job — it is a core dependency, so failing to load it is a hard error and
  the shell refuses to open; core `httpfs` is loaded too. Passwords are split into a
  0600 `PGPASSFILE` and never reach the generated SQL or `argv`
  (`qiita_split_conn_password` in `deploy/_common.sh`, unit-tested). Starts at 4 threads
  / 32GB rather than DuckDB's all-cores/80%-of-RAM defaults, since it shares a host with
  the services. `qiita_cp` is reachability-probed before attaching — the duckdb CLI
  aborts its whole init file on the first error, so a control-plane outage would
  otherwise cost the lake shell too — and is skipped by `--no-cp` or an unreadable
  `control-plane.env`.

- **A dead escalation ladder is now caught at build time, not in production
  (#416, closes #412).** A workflow whose `action_ceiling` equals its heaviest
  step's `baseline_resources` silently disables OOM/TIMEOUT retry for it: the
  runner grows the escalation floor by a fixed factor and clamps it to the
  ceiling, so an equal pair leaves the grown value unchanged, which the retry
  loop reads as saturation and fails the ticket **permanently at
  `retry_count=0`** with `RESOURCE_CEILING_EXHAUSTED`. Nothing caught that — not
  the model, not the loader, not `qiita-admin actions sync` — so it surfaced only
  as a live incident. `ActionDefinition.steps_without_escalation_headroom()`
  reports every step whose `mem_gb`/`walltime` is not strictly below the ceiling
  (checking each lookup profile independently, so bcl-convert's NovaSeq X is
  distinguishable from its iSeq), and a new test enumerates every shipped YAML
  against it. `cpu`/`gpu` are exempt — nothing escalates them, so
  long-read-assembly's deliberate `cpu: 32` pin is not a finding. The list is
  matched for **exact** equality in both directions, so an entry whose workflow
  was since re-sized fails as stale rather than quietly outliving its reason.
  The six action versions carrying the defect today — across four workflow
  directories, `fastq-to-parquet` contributing three (#411) — are listed as
  known-pending in a separate dict rather than fixed here, so re-sizing each one
  (which needs measured peak-RSS data per workflow, not a blanket multiplier)
  must delete its entry to go green. `align/1.0.0` is the one genuine accept,
  documented at the step itself.
  This closes the **YAML-authored** half of the defect; a
  `resource_override.mem_gb` submitted equal to the ceiling still reproduces it
  at runtime, on a workflow this guard passes.

- **A baseline above its ceiling is now caught at build time too (#416).**
  `ActionDefinition.steps_over_ceiling()` plus an unconditional test — no accept
  list, since a workflow whose every ticket fails at dispatch is never intended.
  Split from the headroom guard deliberately: an accept meaning "this step
  knowingly forgoes retry" would otherwise silently also cover "this step can
  never run" if the accepted baseline later drifted past the ceiling.

- **`qiita submit-align-pool`: a CLI for starting an alignment (#400, closes #396).**
  `align-plan` was the only pool-scale entrypoint with no client — the sole way to
  align a pool was a hand-rolled `curl` with a hand-built JSON body, while its
  sibling `submit-block-mask-pool` has had a CLI all along. Same thin-client shape:
  sample selection, aligner choice, reference readiness and block size are all
  resolved server-side, so it validates nothing the server owns and just POSTs.
  Takes `--sequencing-run-idx`, `--sequenced-pool-idx`, `--reference-idx`,
  `--mask-idx`, `--only-missing`. The stderr summary names the planned/skipped
  breakdown **and the per-block read count** — block size is resolved server-side
  from the platform, so the plan response is the first place it is observable, and a
  pool tiled by a stale planner is otherwise only discoverable one walltime ceiling
  per block later.

- **A scientist can pull their own alignment data.**
  `POST /alignment/{alignment_idx}/ticket/doget` signs a Flight DoGet ticket for
  a cohort the caller names, closing the gap between "my samples were aligned"
  and "I can read the alignment" — until now the only mint was
  service-account-only and read its cohort from a work ticket. Guarded by a new
  `alignment:doget` scope, on every role ceiling and deliberately off
  `SERVICE_ACCOUNT_SCOPE_CEILING`. For a plain user the boundary is per-study —
  `Tier.VIEWER` on every study each `prep_sample_idx` is still linked to — while
  `wet_lab_admin` and above bypass that check as they do every other resource
  gate, so for them the role is the boundary.
  Validation runs 404 (no such alignment) → 403 (access) → 422 (cohort
  completeness) → sign, and that order is load-bearing — reversed, the 422 would
  tell a caller which samples are finished for an alignment they cannot read. A
  partially-readable cohort is refused rather than narrowed: coverage filtering
  makes a feature table cohort-dependent, so a trimmed cohort answers a
  different scientific question under the name of the one that was asked.
  Rationale in `docs/architecture.md` and `docs/auth.md`. (#436)
- **Two reads answer "what has been aligned for this pool, and what may I
  mint?"** `GET /sequencing-run/{run}/sequenced-pool/{pool}/alignment` lists the
  alignments over a pool with their config and completion counts;
  `.../alignment/{alignment_idx}/cohort` resolves the prep_samples that are both
  readable and `completed`. Both **narrow** to the caller's slice rather than
  403ing a pool that spans studies they only partly hold — a pool spans studies,
  so rejecting it would make it undiscoverable to someone who legitimately owns
  part of it, and narrowing is safe on a listing where no scientific result
  depends on it. Counts are caller-scoped for the same reason: showing the
  pool's real numbers to someone who may read half of them would set them up for
  a 403 from the all-or-nothing mint. Open to role `user`, unlike the
  wet_lab_admin-gated pool-completion rollup beside them.
  (#436)
- **A Flight DoGet ticket can carry a signed column list, and the alignment
  surface now requires one (#435).** The consumer names the columns it wants, the
  control plane validates them against a per-table allowlist at mint time (422
  on an unknown, duplicated, or empty list — **and on an absent one**, since a
  projectable table's ticket without a list is a ticket the data plane refuses)
  and signs them, and the data plane validates again before projecting exactly
  that set, in that order. Only `alignment_visible` is projectable; every other
  table streams `SELECT *` and rejects a list rather than ignoring it. This makes
  a wide column opt-in instead of a design discussion — `cigar` dominates an
  alignment payload, so anything that needs it can now ask, and everything else
  keeps paying nothing. The measured share, and the shape it was measured on, are
  in `docs/architecture.md`.

  A signed ticket carrying a field the data plane does not recognise is now
  **rejected rather than ignored** (`deny_unknown_fields` on `TicketPayload`).
  Ignoring one means serving a ticket under a wider reading than the signer
  intended, which is the failure mode a mixed-version deploy produces silently;
  it now fails loudly instead. `count_masked`, which reuses a DoGet ticket,
  likewise refuses a projection list rather than disregarding it.
- **DoGet streams can be zstd-compressed, at the client's request (#434).** A client
  sends `qiita-ipc-compression: zstd` as gRPC metadata and the data plane
  compresses that stream's Arrow IPC bodies; anything else is rejected rather
  than ignored, and no header means today's uncompressed behaviour byte for
  byte. Measured 4.4–7.0x on real production shapes. **Off by default on
  purpose**: compression makes a DoGet *slower* over a fast link (break-even
  ~4 Gbit/s), and every in-repo caller is above that line — the control-plane
  runner reaches the data plane over loopback, compute jobs over the cluster
  fabric. `qiita-admin masked-read-export --compress` opts in for off-site runs,
  where it is a large win. Rationale and the break-even arithmetic are in
  `docs/architecture.md`. No operator action: no new env var, no migration.
- **Three DuckLake facts the export path depends on are now pinned by test (#433).**
  `qiita-data-plane/src/ducklake.rs` (integration tier): DuckDB's Arrow export
  emits **no** `DictionaryArray` for VARCHAR even at 2 distinct values but
  **does** for ENUM, so a dictionary on the wire has to be built by us; and a
  plain DuckLake scan through `stream_arrow` comes back near-ordered while an
  equi-join carries no ordering guarantee at all, though its observed disorder
  stays coarse rather than row-level. The tests assert a **lower bound on average
  sorted-run length**, not exact inversion counts, because the counts scale with
  row count and move run to run — so what is pinned is "a scan is near-ordered"
  and "a join is not row-scattered", which is the property the order-sensitive
  encodings actually consume. Measured at 1.6M rows: 0 inversions for the scan,
  a few hundred for the join (~4,000-row runs). Row order was a prose assumption
  behind several encoding options before this. These are facts about DuckDB that
  the export path relies on whatever we decide about compression, which is why
  they are in-tree while the instrument below is not.
- **The DoGet compression and representation axes were measured (#433).** The
  evaluation concluded **ZSTD at the IPC layer**, requested per call and
  defaulting to off, with every array-representation option (run-end encoding,
  `Utf8View`, dictionaries, integer narrowing) and every batch-geometry change
  measured as neutral or negative. Off by default because compression makes a
  DoGet *slower* above roughly 4 Gbit/s of client bandwidth — break-even is
  `bandwidth = encode_rate × (1 − 1/ratio)` — and every in-repo caller is above
  that line; LZ4 measured about half zstd's ratio on every production shape and
  is rejected rather than offered. No production behaviour changes here; the next
  release note implements the decision.

  **What it was measured on**, recorded here because the decision has to outlive
  the instrument: six Arrow payloads extracted from production DuckLake and
  encoded through the same `FlightDataEncoderBuilder` a DoGet uses — masked reads
  (short-read and HiFi), HiFi alignment, and WOL3 phylogeny + taxonomy. Every
  shape's bytes are dominated by a single column. The HiFi alignment payload is
  775.6 MiB, of which `cigar` is 746.1 MiB (96.2%; mean CIGAR ≈ 1,092 bytes over
  716,187 rows) and the six identifier/position columns are 29.5 MiB combined; the
  HiFi read payload splits 46.8% `sequence1` / 52.7% `qual1`. Identifier columns
  are ≤1.1% of those shapes, which is why narrowing and run-end encoding had
  nothing to play for. The equivalent **short-read alignment** shape was never
  available, so no CIGAR share is claimed for it — short-read CIGARs are
  near-degenerate and would be a much smaller fraction.

  **The instrument itself is deliberately not in the repo**, and was never meant
  to land in it. Two of its three run targets consume local-only production
  Parquet, so nobody could re-run them from a clean checkout; what would have
  stayed in CI was a 626-line calibration suite for an instrument nothing else
  called, plus a cargo feature and two dev-dependencies carried solely for it —
  coverage in appearance only. Re-deriving the decision (a new arrow-rs codec, an
  unmeasured column shape, a proposal to change the default) means building an
  instrument again and comparing against the numbers above.
- **Two DuckDB memory behaviours job code reasons about are now pinned by test
  (#391).** `qiita-compute-orchestrator/tests/test_duckdb_memory_behavior.py`: an
  in-memory `CREATE TABLE` far larger than `memory_limit` **spills to
  `temp_directory` and succeeds** rather than failing (so a materialized relation that
  does not fit is a performance problem, not an OOM), and `DROP TABLE` **returns the
  table's bytes to the buffer manager immediately** rather than deferring the release
  to connection close (so dropping a large intermediate mid-job does something). Both
  were prose-only arguments in job comments before this.
- **`long-read-assembly`: the `myloasm` assembler option is implemented (#380).**
  Selecting `assembler: myloasm` previously exited 64 ("not implemented in this
  image yet"); it now runs `myloasm --hifi` and splits circular (LCG) from linear
  (noLCG) contigs. The two assemblers **disagree on how circularity is encoded**,
  so this is a real branch rather than a second tool behind the same tail:
  hifiasm-meta puts it in the GFA segment name (`…tg……c`), myloasm puts it in the
  `assembly_primary.fa` header (`_circular-yes`) and marks nothing in its GFA.
  Reusing the hifiasm regex would have matched nothing and silently demoted every
  closed genome to binning input, so the split is a separate `myloasm_split.py`
  that unit tests execute against real myloasm headers. It reads with miint's
  `read_fastx` and writes with `COPY … (FORMAT FASTA)` — no hand-rolled FASTA
  parser — and **LOADs the deploy-staged extension** rather than a copy baked into
  the image, so the assemble container runs the byte-identical miint the CP/CO/DP
  run. The staged directory reaches the container through the assemble step's new
  `derived_inputs: {MIINT_EXTENSION_DIRECTORY: duckdb-ext}` (read-only bind), which
  is the existing per-step mechanism — `slurm/payload.py` still does not forward
  native-only miint env to containers. Consequence: the image's DuckDB is now in
  **lockstep** with the orchestrator's, because DuckDB namespaces the staged
  extension dir by engine version + platform; `assemble.def` pins
  `python-duckdb=1.5.4` and a unit test fails if it ever diverges from
  `qiita-compute-orchestrator/uv.lock`. Only `circular-yes` counts as circular
  (`circular-possibly` routes to noLCG, the recoverable direction), and the contig
  id is cut at `_len-` so the bin_id carries no per-run coverage statistics (the
  discarded `depth-` field was probed to vary between read samplings of the same
  genome). An unrecognised header shape, a duplicate contig id, or a missing
  `assembly_primary.fa` after a zero exit each fail the step (exit 64) instead of
  yielding an empty `circular.fa` or a silent no-data ticket. hifiasm_meta and
  myloasm now live in **separate conda envs** in the image so the unpinned hifiasm
  solve cannot make the pinned myloasm one unsatisfiable. myloasm is pinned to
  0.6.0 (the version the header format was probed on), asserted in `%test` and by
  the SIF spec's `VERIFY_MATCH`.
- **First-class per-sample `mask_sample` completion gate + `finalize-mask-sample` action (#371).**
  The per-sample read-mask workflows (`read-mask/1.0.0`, `fastq-to-parquet/1.3.0`)
  now record masking completion in `qiita.mask_sample` first-class, via a new
  terminal `finalize-mask-sample` library action that runs after `register-files`
  (so the gate never reads `completed` before the masked reads are durable in
  DuckLake). Previously only the block masking path wrote this gate, so downstream
  consumers carried an unsafe "no gate row ⇒ allowed" carve-out. An idempotent
  backfill migration populates a `completed` row for every historical completed
  per-sample mask. The `finalize-mask-sample` writer refuses when a covering block
  is still masking the same footprint under the shared `mask_idx` (cross-path
  double-mask), so it cannot stomp a block's in-flight gate.
- **`qiita reference export` — pull a genome's sequences to FASTA.gz or Parquet (#366).**
  A user CLI (`reference:read`) that exports one or more genomes'
  sequences: for each `--genome-idx` it resolves the genome's members, mints a
  chunk DoGet ticket, streams the bytes over Flight, and writes
  `<reference_idx>.<genome_idx>.{fasta.gz|parquet}` (one file per genome). FASTA
  is reassembled via miint's `FORMAT FASTA` writer with the reference's accessions
  as headers; Parquet is the raw `reference_sequence_chunks` rows. A plasmid shared
  across genomes is exported for each of its genomes (the payoff of the
  feature_genome many-to-many fix). Exported bytes are the stored original strand
  of the representative record — the feature_idx dedup is by canonical hash, so a
  reverse-complement-equal input exports the representative's orientation. A short
  export (fewer sequences written than the genome has members — e.g. an `indexing`
  reference whose chunks are not yet in DuckLake, or one with a partial delete) is
  a hard error: the incomplete file is removed and the CLI exits non-zero rather
  than leaving a file that looks complete.
- **Resolve a genome to its member features within a reference —
  `GET /reference/{reference_idx}/genome/{genome_idx}/member` (`reference:read`) (#366).**
  Returns each member feature's `feature_idx` + the reference's FASTA-header
  `accession`, feature_idx-ordered — the inverse of the internal
  `export_member_genome`, keyed on `(reference_idx, genome_idx)` because the
  accession is per-`(reference, feature)` and a DoGet ticket is per-reference. A
  feature shared across genomes (a plasmid → one content-hash-global `feature_idx`)
  is returned for each of its genomes. 404s when the pair has no members (unknown
  reference, unknown genome, or a genome in a different reference) — an empty
  export is a fail-loud caller error. The resolution step the genome-export CLI
  runs before a DoGet for the sequence bytes.
- **`ERC000013` "GSC MIxS host associated" metadata checklist (#368).** Seeds
  the checklist under `ERC000011` (ENA default) and re-parents `ERC000014`
  ("GSC MIxS human associated") beneath it, so the inheritance chain becomes
  `ERC000011 → ERC000013 → ERC000014 → ERC000015`.
- **Reference feature/genome exclusion — a curated global blocklist (#361).**
  Bad reference data (a contaminated genome, a mislabelled contig) can now be
  **masked at consumption** without deleting rows or rebuilding aligner indexes.
  A `genome_idx` or `feature_idx` is blocked in a new authoritative Postgres
  table `qiita.reference_exclusion` (global — no `reference_idx`, so future
  references inherit the block) and mirrored to a small DuckLake table that two
  `CREATE OR REPLACE VIEW … _visible` anti-join views consume
  (`alignment_visible`, `reference_taxonomy_visible`). The raw `alignment` /
  `reference_taxonomy` tables are removed from both DoGet allowlists, so only the
  views are Flight-reachable — the `read_masked`-style unbypassable enforcement
  surface. `woltka_ogu` renormalizes over survivors, so feature tables are
  corrected immediately with existing data intact. The Postgres→DuckLake mirror
  is refreshed by a wholesale idempotent `sync_reference_exclusion` DoAction
  (`REPLAY_SAFE_ACTIONS`) fired on every blocklist mutation **and** as the
  post-load `sync-reference-exclusion` step of every reference-load workflow
  (`reference-add`, `local-reference-add`, `host-reference-add`,
  `local-host-reference-add`) — catching a fresh assembly of an already-blocked
  genome so "future references inherit the block" holds on every load path.
  Adds `reference_membership.accession` (the FASTA-header record accession,
  persisted at load — previously dropped) surfaced as external provenance; a
  system-admin curation surface `POST`/`DELETE /reference/exclusion` (block /
  soft-unblock, new `reference:exclusion:write` scope) plus a
  `POST /reference/exclusion/sync` operator force-resync (re-materialize the
  mirror from Postgres with no blocklist change — recovery after a failed sync, a
  rebuilt DuckLake catalog, or a fresh data plane); the reference-scoped query
  `GET /reference/{reference_idx}/exclusion` (`reference:read`); and the CLI —
  `qiita-admin reference exclusion add/remove/sync` (the write surface) and
  `qiita reference exclusion list` (the `reference:read` query any user can run).
  Phylogeny defers to a documented `shear_tree` prune contract (no production tree
  consumer exists yet). Two migrations: `reference_membership.accession`,
  `reference_exclusion`.
- **`assembly_coverage` — binning coverage from miint's minimap2, not bwa.** New native
  step in `long-read-assembly`, between `assemble` and `binning`: it aligns the masked
  HiFi reads back to the noLCG contigs with miint's embedded minimap2 (`map-hifi`) and
  writes a BAM, which `binning.sh` stages into
  `work_files/<sample>.bam`. metaWRAP guards its own `bwa mem` behind that file's
  existence, so it skips self-alignment and derives depth from the minimap2 alignment —
  the same seam qp-pacbio uses, and the only one available (`metawrap binning` has no
  aligner-selection flag). Native rather than in-container because miint is deliberately
  not exposed to containers. **SUPERSEDED IN PART — see the `Fixed` entry below on the
  unsorted coverage BAM.** As shipped, this entry claimed the BAM was coordinate sorted
  because miint emits `@SQ` **reversed** from the `REFERENCE_LENGTHS` table (so the table
  was built DESC to land `@SQ` ascending). That reversal claim is false; the BAM was not
  sorted, and `binning.sh` now runs `samtools sort`. The two behaviours that DID hold, and
  are still pinned by `tests/jobs/test_assembly_coverage.py`: `COPY … (FORMAT BAM)` requires
  `REFERENCE_LENGTHS`; and `SEQUENCE_DATA` (documented upstream but
  easy to miss) is required to write real SEQ, without which jgi's contig-end exclusion
  window (`--maxEdgeBases`, default 75)
  collapses — it sizes that window from the read length it reads out of SEQ — averaging the
  under-covered contig ends in and dropping depth by ~`2*75/contig_length` (0.750% on a
  20 kb contig), a bias inversely proportional to contig length that also inflates the
  variance metabat2 consumes. With `SEQUENCE_DATA` the depth and variance match a real
  `minimap2 | samtools sort` BAM to every printed digit, so the step is at parity with
  qp-pacbio's coverage. (#365)
- **`qiita-admin ticket cancel` — stop in-flight compute without raw scancel (#314).**
  A single ticket or a whole fan-out (explicit idxs and/or an `--action-id` +
  `--sequencing-run-idx`/`--sequenced-pool-idx` filter) can now be cancelled from the
  CLI, replacing the fragile "scancel as the compute account + hand-written job-name
  regex + race the re-driver" recovery. The CP does it **terminal-first**: it flips
  each ticket to a new terminal `cancelled` state (distinct from `failed` so an
  operator stop is legible in `ticket list` / rollups / the notify digest, with NULL
  failure_*) so the runner's poll loop aborts and no new attempt spawns, THEN scancels
  every attempt of the ticket via a new CO `POST /step/cancel` endpoint (matched by the
  `qiita-wt{idx}-` job-name prefix, not just the recorded slurm_job_id). Idempotent
  (already-terminal is a no-op but still reaps any stray job — the same primitive
  #312's orphan-reaping needs); a cancelled ticket is redrivable in place with
  `qiita ticket run` once the blocker is fixed. New `work_ticket:cancel` scope
  (system_admin), `ALTER TYPE work_ticket_state ADD VALUE 'cancelled'` migration,
  `ComputeBackend.cancel` / `ComputeBackendClient.cancel`, and `POST /work-ticket/cancel`.
- **Mouse gut terminology seeds (#360).** Appends the NCBI Taxonomy terms
  `410661` (mouse gut metagenome) and `10090` (Mus musculus), plus the ENVO
  term `ENVO:00006776` (animal-associated habitat, seeded as obsolete since it
  is deprecated at source but appears in data we import), to the existing
  pre-release MVP terminologies.
- **Block-read DoGet: block-scoped compute jobs stream their reads.** New
  `read_block` / `read_masked_block` ticket selectors on the data plane, scoped
  by a block's `(prep_sample_idx, sequence_idx sub-range)` members rather than a
  flat column filter, plus `POST /read/ticket/doget` to mint one at job runtime
  and the `open_read_block_stream` / `bind_step_reads` seams on the compute side.
  This replaces "the control plane asks the data plane to COPY a `reads.parquet`
  onto shared scratch at submit time, then hands the job a path" for the
  `read-mask-block` and `align` workflows: same bytes, same column shape, but the
  bulk work moves off the CP submit path onto compute nodes where it spreads
  across data-plane instances, and the handoff stops assuming a shared
  filesystem. DoGet itself is unchanged — it already streamed RecordBatches
  through a bounded channel; what the retired export DoActions did was bypass
  that with a server-side `COPY … TO parquet`. The selectors reuse the data
  plane's existing `block_read_where_clause` and `EXPORT_READ_COLUMNS`, so a
  block's read footprint and its delete footprint cannot drift. (#364)
  Gated on a new `read:doget` scope rather than the generic `ticket:doget`:
  `read_block` streams RAW reads, a strict superset of the `read_masked` surface
  that already carries its own privacy-sensitive scope, so reusing the
  reference-read scope would have inverted the model.
- **Pool / run summary + rollup endpoints (#236).** Server-side aggregation so
  callers stop paging the per-sample list route and tallying by hand. All
  compute-on-read (never drifts), no migration. (1) `PoolReadMetrics` gains a
  read-outcome breakdown (`samples_unprocessed` / `samples_zero_reads` /
  `samples_with_reads`, splitting the old `samples_with_metrics` into "no metrics
  yet" vs "processed but everything filtered out") and accession-coverage counts
  (per-accession + all-four `samples_fully_submitted_to_ena`); the pool + run
  rollups share one SQL aggregate so they can't drift. (2) `GET /sequencing-run/{run}`
  now nests `read_metrics` (the run-level twin, summed across the run's pools).
  (3) New `GET .../sequenced-pool/{P}/sequenced-sample/exceptions` — only the
  anomalous samples (no usable reads / missing any submission accession / failed
  read-mask ticket), each with the flags naming why. (4) New
  `GET .../sequenced-pool/{P}/work-ticket/summary` — read-mask ticket coverage +
  per-state ticket counts (tickets as denominator), reconciled with the completion
  rollup. (5) `GET /work-ticket` list rows now nest `read_outcome` (the ticket's
  prep_sample read counts + passing fraction) for prep_sample-scoped tickets.
  (6) `GET .../completion` gains an optional `?reference_idx=N` so host-masking
  completion distinguishes "not masked against THIS reference" from the
  reference-agnostic "not masked at all" (folded from #217 ask-4).
- **Control-plane throttle for fan-out dispatch (#329).** A fan-out action
  (sharded reference-index build, bulk read-mask block, bulk sharded-alignment
  block) no longer dispatches all of its child work_tickets at once — which for a
  1000-shard reference opened ~1000 concurrent data-plane streams and took down
  the WOL3 (reference 16) build (fd exhaustion + submit-time ticket-expiry from
  the backlog). Each fan-out now INSERTs its children `dispatch_held` and a
  per-cohort "pump" (`fanout_dispatch.top_up_dispatch`) releases only
  `FANOUT_MAX_INFLIGHT` (default 8) at a time, refilling as each child reaches a
  terminal state. A single child failure **fail-stops** the cohort (releases
  nothing further) so a sick backend halts the fan-out instead of burning through
  every shard; the operator redrives the failed child(ren) and the pump resumes.
  Startup reconcile re-dispatches only non-held in-flight tickets and re-pumps
  cohorts with held tickets, so a CP restart doesn't blow the throttle open. New
  `work_ticket.dispatch_held` column (metadata-only migration) and
  `FANOUT_MAX_INFLIGHT` env var.
- **Spike-in reference load runbook (#310).** `docs/runbooks/spike-in-reference.md`
  documents loading a SynDNA/spike-in reference for `--syndna-reference-idx`: plasmids
  (not bare inserts) so the 0.90 aligned-fraction gate is correct, the required
  GTDB-prefixed **Parquet** taxonomy (not a TSV), and why `--host --no-rype-index
  --minimap2-preset map-hifi` is the sanctioned way to build the map-hifi `.mmi`.
- **Reference feature annotations: GFF3 in, typed interval rows out (#269).** A
  reference can now carry per-interval ANNOTATIONS — a SynDNA insert on its
  plasmid, a gene on a chromosome — supplied as a GFF3 (`qiita reference load
  --gff`, on all four `reference-add` workflows, remote and local). This is the
  prerequisite for per-feature coverage depth: depth is a quantity per
  *annotated interval*, but reads align to the interval's *parent*.
  - Parsed by `hash_sequences` with miint's `read_gff` (no hand-rolled parser).
    Each interval is cut from its parent, canonically hashed, and minted its
    **own `feature_idx`** by the new in-process `mint-annotation-features`
    action — so an interval can key a feature table, and an insert that is also
    ingested standalone deduplicates onto the same `feature_idx` lake-wide.
  - Annotated features are deliberately **not** in `reference_membership`, and
    have no `reference_sequences` / `reference_sequence_chunks` row. Membership
    is what gets INDEXED and aligned against: reads align to the plasmid, never
    to the bare insert, and a membership row would put inserts into the aligner
    index and shard planning, competing with their own parent for alignments.
    The bytes are recoverable from the parent plus the interval, so a second
    copy could only drift.
  - **Coordinates are stored HALF-OPEN `[position, stop_position)`**, converted
    from GFF3's 1-based CLOSED `[start, end]` exactly once, at ingest. This
    matches `alignment_slice` / `read_alignments` / the `alignment` table, so
    every alignment-side consumer compares like with like. Both conventions spell
    the column `stop_position`, so mixing them type-checks, runs, and raises
    nothing — it just silently stops counting the interval's last base. The `+1`
    is pinned by `test_annotation_ingest_smoke.py` against the real miint build,
    including an anti-vacuity control proving the closed form gives a different
    (wrong) answer.
  - A GFF3 with a `seqid` absent from the FASTA, an interval running off the end
    of its parent, or an inverted interval is a hard failure, not a warning — each
    silently corrupts a depth number rather than crashing.
  - **An annotation's identity is a minted `annotation_idx` (BIGINT), not the GFF3
    `ID`.** The spec lets a DISCONTINUOUS feature (a ribosomal-slippage CDS) repeat
    one `ID` across several lines — NCBI's RefSeq annotation of E. coli K-12 MG1655
    carries 20 such repeats — so the `ID` is neither unique nor required. It is
    stored as provenance; nothing joins on it. The occurrence is keyed on its
    NATURAL key (parent + window + type + strand), which is what makes a re-ingest
    idempotent. `feature_idx` is not the identity either: identical bases share one
    (a bacterial 16S occurs in 5–7 byte-identical copies), so a feature is a
    SEQUENCE and an annotation is an OCCURRENCE of it at a place.
  - **New annotation catalog: `qiita.annotation_term` + `qiita.annotation_to_term`.**
    The SEMANTICS of an annotation ('16S rRNA' / `RF00177`) are one row per
    `(system, system_id)`, shared across every occurrence and every reference — the
    same global dedup `qiita.feature` gets — with a MANY-TO-MANY junction to the
    occurrence. Many-to-many because one interval routinely carries several
    cross-references at once: in that same RefSeq file 4,816 features carry three
    `Dbxref` entries and 4,161 carry five, spanning six systems. `definition` and
    `version` are nullable by necessity — `product` is present on only ~50% of a
    RefSeq file's rows, and GFF3 has nowhere to record an annotation database's
    version at all.
  - GFF3 `score` and `phase` are now persisted (both nullable — `score` is empty on
    100% of real RefSeq and prokka rows, `phase` only on CDS), as is `source`. The
    interval's length is *not* stored: it is `stop_position - position`.
  - New DuckLake table `reference_annotation` (created by `ensure_reference_tables`
    at data-plane boot; readable over Flight; purged by `delete_reference`), plus a
    Postgres twin `qiita.reference_annotation` holding the reference's *claim* on
    those features — the same claim/data split `reference_membership` already uses.
    Without it, `delete_reference_cascade` (which computes orphan features from the
    claim tables) could not see annotated features at all, and every one of them
    would survive `DELETE /reference/{idx}` forever while the data plane deleted its
    lake rows — the two stores disagreeing about which features exist.
    `ReferenceDeleteResponse` gains `annotation_deleted`,
    `annotation_term_link_deleted` and `annotation_term_deleted`. The lake's raw
    `attributes` MAP is kept alongside the normalized terms, so a system we do not
    yet parse stays recoverable without a re-ingest.

### Fixed

- **Four `DEPLOY_CHECKLIST.md` commands did not run as written, found by running them during the
  deploy of #514–#522.** Each was written but never executed, which is the one defect a checklist
  review cannot catch. (1) The `hifiasm_meta` version check omitted `TMPDIR=/tmp`; apptainer
  forwards the caller's `TMPDIR` into the container, and one pointing at an unbound path aborts
  libmamba with `temp_directory_path: No such file or directory` before it runs anything.
  (2) The assembly-genome backfill extracted `DATABASE_URL` with `grep -oP '^DATABASE_URL=\K.*'`,
  which captures the surrounding quotes the file writes, so the DSN parser refused it with
  `scheme is expected to be either "postgresql" or "postgres", got ''`. It now sources the env
  file, which is what [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) §5 already
  prescribed — the checklist had simply diverged from it, so that section now states the rule for
  every later step rather than leaving each one to invent an extraction. (3) The `genome-map`
  check said to pick a `(prep_sample_idx, processing_idx)` pair "from the backfill's dry run",
  which prints aggregate counts and no pairs; it now carries the query that yields one.
  (4) The 1.0.1 re-run roster said "mask 11's pre-fix assemblies", which reads as the mask-11
  sample list and is not: `qiita mask samples --mask-idx 11` returns the 82 prep_samples eligible
  to assemble, while only the first 26 (30438–30463) have a pre-fix assembly. Driving the fan-out
  off the CLI listing would have submitted 26 legitimate re-runs plus 56 brand-new assemblies at
  ~7 h each — superseding nothing and corrupting nothing, which is precisely why it would not
  have announced itself. The bucket now names `processing_idx = 2` and carries the roster query.
- **`long-read-assembly`'s per-ticket subject counts were three estimates, one of which the repo
  contradicted itself on.** `1.0.1.yaml` gave ~86 circular contigs per ticket where `1.0.0.yaml`
  and this file gave ~98, with nothing to settle it. Measured over the 52 stored 1.0.0 runs in
  `qiita.assembly_membership`: **5,328 MAG bins (102.5 per run)** and **4,433 LCG contigs
  (85.2 per run over all 52; 92.4 over the 48 that closed any — 4 runs produced no circular
  contig at all)**. So ~98 was the error and ~86 was the all-runs mean rounded. The LCG figure
  carries forward to 1.0.1 because an LCG bypasses binning; the MAG figure does not, since
  changing MAG composition is what 1.0.1 is for. The residue count above the 300 kb cut stays an
  estimate — contig lengths live in the lake, not Postgres — so `~338 subjects` is now two
  measurements and one guess, and says so.
- **The `baseline_resources.cpu` pins read one hardcoded `1.0.0.yaml`, so the version that
  actually runs went unpinned (#522).** `test_assembly_coverage_cpu_pins_duckdb_threads` asserts
  a step's `cpu:` equals its job's `_DUCKDB_THREADS`, because the aligner's parallelism IS that
  pool and a drift between them costs cores or oversubscribes them without failing anything.
  `long-read-assembly` is the first workflow whose SECOND on-disk version is the one that runs
  (`fastq-to-parquet` has carried four for months), and the pin named `1.0.0.yaml` — the retired
  one — leaving `1.0.1.yaml` free to drift. The three pins
  (`align`, `align-denovo`, `assembly_coverage`) now go through
  `_baseline_cpu_every_version`, which globs `workflows/<workflow>/*.yaml`, so a new version
  file is covered the moment it lands instead of when someone remembers the test.
- **`load_actions` ordered versions as strings, so the tenth minor bump of any action would have
  deployed disabled (#522).** `sync_actions` re-enables the version it is syncing and
  auto-deprecates every other version of that `action_id`, so whichever version the loader yields
  LAST is the one a deploy leaves submittable. The order came from `sorted(by_key.items())` on
  `(action_id, version)` — a compare of the version STRING, which puts `"1.10.0"` before
  `"1.9.0"`. An action reaching a two-digit minor would therefore have shipped its newest version
  disabled and refused every submission naming it, with the catalog still listing both. Each
  dotted component now compares as a number when it is one (`loader._version_sort_key`), and the
  key stays total over the free-form `version` string rather than raising on a shape it cannot
  parse: a non-numeric component sorts BEFORE every numeric one in the same position,
  which puts an unparseable version where being wrong is cheapest — last is what stays
  enabled, so a stray `"latest"` gets disabled rather than winning the deploy. No
  workflow on disk has a two-digit component yet, so nothing deployed was affected.

- **`bin_refine` discarded every MetaBAT bin id, and offered each binner's unbinned catch-all as a
  candidate bin (#519).** `Fasta_to_Contig2Bin.sh -e fa` writes two tab-separated fields,
  `<contig>\t<filename minus ".fa">` — measured against the shipped
  `long-read-assembly-dastool-1.0.0.sif` over all three binners' output: 4,910 concoct + 4,910
  metabat2 + 2,930 maxbin2 rows, every one `NF == 2`. metabat2's table was projected
  `{print $1,$4}`, so every row carried the empty string as its bin id and DAS_Tool saw one unnamed
  bin holding the whole assembly. Across 60 production runs it selected 6,023 MaxBin and 201
  CONCOCT bins and 0 MetaBAT, while every ticket paid metabat2's compute. Separately,
  `bin_refine.sh` globbed `*.fa` and so passed on the file each binner writes for the contigs it
  did NOT place — metabat2's `bin.unbinned.fa` / `bin.tooShort.fa` / `bin.lowDepth.fa`, concoct's
  `unbinned.fa`; concoct's held 3,326 of 4,910 contigs on the measured run. The two interact: the
  metabat2 catch-alls were unreachable only because the projection nulled that binner, so `$4` →
  `$2` alone would newly offer three of them as MetaBAT bins. All three binners now take one path,
  through a new `contig2bin_filter.awk` that keeps `bin.<N>` (metaWRAP's name for a real bin in
  every binner's dir), drops the four catch-alls, and writes anything else to a rejects file the
  step fails on. Measured on 150k masked reads from a production ticket, re-run through the shipped
  binning and dastool images with only the table changed: 20 bins → 21, MetaBAT 0 → 4 (DAS_Tool
  replaced 3 MaxBin bins with MetaBAT ones it scored higher, 19 → 16), mean redundancy 3.8 → 1.9,
  bins at medium quality or better 18 → 21. Stored assemblies are unaffected — they were produced
  without MetaBAT and stay that way unless re-run. `bin_refine.sh` was ported from qp-pacbio, which
  carries the same projection at `5.DAS_Tools_prepare_batch3_test.sbatch:44`; the catch-all removal
  sitting above it there was not ported.

- **`bin_refine` aborted instead of succeeding empty when no binner contributed a table (#519).**
  `bin_refine.sh` declared its two accumulator arrays with a bare `declare -a`, which leaves them
  UNSET rather than empty, so the `${#das_bins[@]}` that decides whether any binner produced
  something tripped the `set -u` from `_lib.sh`: `das_bins: unbound variable`, exit 1, on exactly
  the input the check exists to pass cleanly as an LCG-only success. Measured on the image's own
  base (`mambaorg/micromamba:1.5.8`, bash 5.2.15); macOS ships bash 3.2, where the bare form reads
  0, which is why it never showed up on a dev laptop. Reached whenever no binner contributes — an
  empty `bins_dir`, which `binning.sh` hands over as a normal outcome, and now also a binner whose
  only files are its catch-alls, which the filter above drops. Found by running the entrypoint
  under the base image with `Fasta_to_Contig2Bin.sh` and `DAS_Tool` stubbed out.

- **Two `library.py` docstrings pointed at a comment that does not carry the argument they cite
  (#519).** Both `upsert_genomes` and `upsert_genome_associations` said that the reference and
  assembly genome junctions "must stay apart" and referred the reader to
  `assembly_membership.genome_idx`'s column comment for why. That comment states the mint's scope
  and what a NULL means, and explicitly defers the completeness point to the table's comment, but
  it never states the junction argument — nor does the table comment. The only place that does is
  `test_assembly_genome_mint`, whose module docstring gives the reason (an assembly edge in
  `qiita.feature_genome` would put sample-derived genomes inside every reference map sharing a
  contig, and inside the reach of the global `reference_exclusion` blocklist that expands through
  that junction) and whose `test_a_shared_contig_never_reaches_the_reference_graph` pins it. Both
  pointers now name that, matching the third one added beside the de novo genome map's kind
  filter.

- **`assemble` kept only the two FASTAs it published and deleted the rest of the
  assembler's output (#516).** The step ran each assembler into a `mktemp -d` it removed
  on EXIT, read one file back out (`assembly_primary.fa` for myloasm, `asm.p_ctg.gfa` for
  hifiasm_meta) and discarded everything else — myloasm's `alternate_assemblies/`
  demoted contigs, `final_contig_graph.gfa` and `3-mapping/low_quality_regions.bed`;
  hifiasm_meta's `asm.a_ctg.gfa` and unitig graphs. The deletion was assembler-agnostic
  and left nothing to recover from: myloasm removes its own pre-dereplication FASTA
  internally, so `alternate_assemblies/` was the only route to a demoted contig. Both
  arms now point `-o` straight at `$QIITA_OUTPUT_PATH/assembler`, so the tree is written
  where the step's output already lives rather than copied there, and `qiita_finish` lists
  and the verifier checks it without it being a declared output — nothing names a file the
  assembler produces, which is what lets one directory hold both arms' layouts and survive
  a release that renames one (hifiasm_meta is unpinned). No step consumes it. It costs the
  per-attempt workspace roughly a gigabyte per assembly; `DEPLOY_CHECKLIST.md` carries the
  measured figures and the conditions they were taken under. Whether the demoted contigs
  should be *ingested* is a separate assay question and is unchanged here.

- **`stage_local_fasta` feeds the manifest to `read_fastx` in batches, so a large
  reference no longer exhausts the job's file descriptors (#513).** `read_fastx` opens a
  file per path in its `VARCHAR[]` and holds that handle until the whole call ends — a
  reader is never released when its file is exhausted — so open descriptors and zlib state
  grow with the number of paths in a single call, not with the bytes read
  (duckdb-miint#260). Measured on gzipped WoL3 genomes: ~1 descriptor and ~464 KB of RSS
  retained per path, both linear. The 196,062-genome web-of-life 3-rc4 ingest therefore
  needed ~196k descriptors and ~89 GB before its single call could finish: the 32 GB
  baseline attempt was OOM-killed, and the 64 GB escalation stopped at path 131,070 reporting
  `Empty file` for a valid 957 KB genome — two short of the hard descriptor limit a
  SLURM step on this cluster carries (measured: soft 1024, hard 131072) — which is what `read_fastx` reports when an open
  fails, because kseq++ does not check `gzopen`'s NULL return. Both passes now run one
  `read_fastx` call per 500 paths, holding ~500 descriptors and ~230 MB: pass 1
  accumulates into the same temp table so the empty-body and cross-file duplicate-`read_id`
  checks still span the whole manifest, and pass 2 writes one Parquet part per batch.
  `fasta_path` is consequently a DIRECTORY of `part_*.parquet` rather than one file —
  Parquet has no append — which `hash_sequences` consumes unchanged, since it passes the
  value straight to `read_parquet` and that reads a directory as one relation. Same shape
  `hash_sequences` already emits for `reference_sequence_chunks`.

- **`redeploy.sh` re-execs itself when the pull replaced it (#510).** Step 1 pulls the clone
  `redeploy.sh` lives in, so a pull that changes `deploy/redeploy.sh` or `deploy/_common.sh`
  rewrites the running script. `git checkout` replaces a tracked file by rename, so the running
  bash keeps reading the pre-pull inode and steps 2-8 execute the code from before the pull;
  child scripts (`preflight.sh`, `local-deploy.sh`, `verify.sh`) are fresh processes reading the
  pulled bytes, so the log gave no sign of the split. The deploy that shipped the unconditional
  step-5/6 venv refreshes (#507) was itself driven by the old conditional script: it printed
  "Native venv already current", skipped the `uv sync`, and left the SLURM native venv on a
  `qiita_common` predating `ANNOTATION_STRAND_WARNING`, which `jobs/hash_sequences.py` imports —
  surfaced two steps later by `probe/native-import`. Step 1 now fingerprints the script plus the
  `_common.sh` it sourced either side of the pull (`qiita_deploy_self_fingerprint`, digesting each
  file under its name the way `qiita_sif_build_inputs_hash` does) and hands off to
  `qiita_deploy_reexec_if_changed`, which execs the pulled copy. A second change after a re-exec
  aborts instead of re-execing: nothing is deployed at step 1, so the abort costs a re-run.
  `redeploy.sh` also refuses to start when it is not running from inside `$QIITA_CLONE` — the
  pull only rewrites that clone, so from outside the check could never fire. Tests drive the
  helper end to end (execs the replacement and abandons the original, returns when nothing
  changed, aborts under the sentinel, and the sentinel reaches the re-exec'd process), cover the
  fingerprint (rename swap, a change confined to `_common.sh`, bytes moved between the two files,
  fail-loud on either being unreadable), and pin the bash behaviour underneath: a script replaced
  by rename mid-run finishes on its original body.

- **`pool-completion` no longer reports a withdrawn masking run as usable, and now sees the
  block masking path at all (#508).** `fetch_sequenced_pool_completion` bucketed each sample by
  the state of its per-sample `read-mask` work tickets. That answered a different question from
  the one every masked-read consumer asks: the masked-read DoGet, the admin export, the
  assembly input resolver and align planning all gate on `qiita.mask_sample`, and a run
  withdrawn after the fact (`state = 'invalidated'`) keeps its COMPLETED ticket — the ticket
  did complete, which is why there is a run to withdraw. So the summary read `fully_processed`
  ("DONE and clean" in `qiita pool-completion`) on a pool whose reads every consumer then
  refused; `complete` and `fully_processed` are computed from `samples_completed`, so the
  miscount reached the headline flag, not just the tally. The same ticket join was blind to two
  more shapes: a block ticket carries `block_idx` with `prep_sample_idx` NULL (the work_ticket
  scope-target CHECK), so an entire block-masked pool read as never submitted, and only
  `read-mask` was matched, not the `fastq-to-parquet` half of `PER_SAMPLE_MASK_ACTION_IDS`. The
  rollup now resolves the two the way `repositories.mask_definition._MASKED_SAMPLE_CTE` already
  does — the gate wins for every (mask, sample) pair that has a gate row, the ticket arm
  supplies the pairs it does not cover, since the per-sample path has no PENDING phase and a
  mask that ran without completing leaves no row at all — keeps work tickets for the states no
  gate row expresses, and reaches block tickets through `qiita.block_member`. New
  `samples_invalidated` bucket on `PoolCompletionStatus`, ranked above `in_flight` (a re-mask in
  progress does not make a withdrawn pass-set usable), and new `samples_cancelled`, ranked above
  `samples_failed` since `WorkTicketState` keeps CANCELLED distinct so a deliberate stop stays
  legible in these rollups and an operator cancels to stop a failing retry. A gate row still
  `'pending'` counts as outstanding rather than never-submitted, but only when no terminal ticket
  state says what became of it. `samples_not_submitted` becomes the residual, so the seven
  buckets partition the sample set by construction. `GET /sequenced-pool/{P}/work-ticket-summary` keeps asking the ticket
  question and now has its own `fetch_sequenced_pool_read_mask_coverage` rather than
  subtracting the rollup's residual, which no longer means "has no ticket".

- **A deploy no longer skips the venv refreshes that keep native SLURM jobs off stale
  code (#507).** `redeploy.sh` steps 5 and 6 could skip `uv sync --reinstall-package
  qiita-common` when a package-root import probe passed. That probe cannot see the failure
  it was guarding: two production deploys left the native venv on a stale `qiita_common`
  and it passed both times — 2026-08-21 missing the whole `assembly_constants` submodule
  (`import qiita_common` still succeeds), and 2026-08-27 missing only the name
  `URL_ASSEMBLY_DOGET` from `api_paths` (that module still imports, so no widening on the
  `qiita_common` side reaches it). Both refreshes are now unconditional; the skip's other
  condition, "nothing arrived in this pull", was already removed and only ever proved that
  one pull was a no-op. The post-sync verification moves to the consumer side: new
  `qiita_compute_orchestrator.native_import_check` imports every dispatchable job module
  through the orchestrator's own `scan_native_jobs`, so a job's `from qiita_common.x import
  Y` is what fails and a missing module and a missing name are caught alike. The
  compute-readiness `probe/native-import` — which ran `import qiita_compute_orchestrator
  .jobs` alone, shallower still — now invokes that same module, so head node and compute
  node cannot disagree about what "imports cleanly" means, and captures stderr as well as
  stdout so an absent module reports its reason rather than a bare `=fail`. Step 6's import
  covers `cli.admin` as well as `cli.user`: `SYSTEM_PRINCIPAL_IDX` and
  `TERMINAL_WORK_TICKET_STATES` are admin-only at module level, so a `cli.user` import was
  green on exactly the missing-name shape this entry is about. Both abort paths print the
  exact working remedy, `bash -lc` and absolute `uv` included.
  `FORCE_NATIVE_REFRESH` / `FORCE_CLI_REFRESH` are accepted and ignored. Removed with the
  skip: `native_pkgs_changed` / `cli_pkgs_changed` and `qiita_paths_touch_native` /
  `qiita_paths_touch_cli`, dead since the pull-diff condition went, plus the tests that
  described them as backing a live decision.

- **A circular alignment gate no longer refuses every slice holding a secondary record
  (#486).** `check_gate_diagnostics` counted secondary, unmapped and coordinate-less rows
  as one `unpoolable_rows` bucket and refused the slice on any of them, because
  `circular_query_coverage` excludes all three. The three do not share a remedy: a
  secondary carries its own CIGAR and coordinates, so the CIGAR axis scores it per
  record, while an unmapped or coordinate-less row can be scored on neither axis. The
  counts are now split and only the second class refuses. `qiita feature-table
  --circular-gate` (#475) was unusable on real `align_sharded` output as a result —
  that job collects placements with `max_secondary := 100` and prunes by identity, not
  by flag, so its stored rows carry secondaries.

  The circular gated relation gains a second arm for them, and the pooled arm now
  excludes secondaries explicitly: `cleared` is keyed on `(read, is_read1, reference)`,
  which a secondary placed elsewhere on the SAME reference shares with its primary, so
  it previously would have ridden in on that clearance without being scored at all.

- **Two stored sample-field schema comments no longer contradict the schema (#485).** The
  `biosample_study_field` / `prep_sample_study_field` table comments listed
  `tier_override` among the properties a linked row inherits from its global field; a
  global field has no such column — it carries `default_tier` — so a linked row's NULL
  `tier_override` is what the inheritance CHECK requires, not an inherited value. The
  `*_global_field.internal_name` comments said the column is never shown to end users,
  which the new registry read contradicts: `internal_name` is exactly the key a caller
  matches on there. Comment-only migration; no DDL.

- **The QC adapter set refuses a duplicated chunk position instead of concatenating it
  (#494).** `_write_adapter_parquet` reassembles the configured adapter reference by joining
  every chunk row for a feature in `chunk_index` order. Its docstring asserted
  `(feature_idx, chunk_index)` uniqueness and declined to dedup so that a violation would
  surface; nothing surfaced it. On the live lake `feature_idx` 127 carries two rows at
  `chunk_index` 0 — an exact reverse-complement pair, which `canonical_sequence_hash_expr`
  folds into one feature — so the join returned 66 bp for a 33 bp adapter. It now raises
  naming every repeated `(feature_idx, chunk_index)`, which `_resolve_qc_adapters` turns
  into a SUBMISSION `BAD_INPUT` with no partial `adapters.parquet` left behind.
  `DEPLOY_CHECKLIST.md` bucket 6 carries the one-off delete that repairs the row and names
  the surviving orientation.

- **A de novo `alignment_idx` no longer depends on how a submitter spelled its integers
  (#486).** `assembly_processing_idx` and `align_mask_idx` were hashed into the identity
  as received, while the knobs beside them were coerced. JSON Schema `type: integer`
  matches any number with a zero fractional part, so `42.0` validates; canonical JSON
  then renders it differently from `42`, giving one config two `alignment_idx` — a re-run
  that neither replaces its own rows nor is recognized as the same alignment. The mint's
  jsonb round-trip guard cannot see it, because `42.0` stores and re-reads unchanged.
  Both are coerced now, and an absent one is refused rather than hashed as null.

- **`align-denovo` checks the `mask_sample` gate at submit rather than at runtime
  (#486).** The pre-loop verified the mask CONFIG existed but not that it had RUN on this
  sample. `routes/read_masked.py` refuses to sign the `read_masked` DoGet ticket on
  anything but `'completed'`, so a ticket naming a `'pending'` mask was admitted, minted
  its identity, wrote both ticket columns, materialized an `alignment_sample` row at
  `'pending'` that no reconcile sweeps, and only then failed — from a job already holding
  its allocation. The same gate `_read_ingest` applies to its own read stream now runs
  before any of that.

- **A missing `assembly_sample` gate row gets its own refusal (#486).** It previously
  fell into the catch-all reporting `gate reads None`, which reads as a stalled run.
  Absence is reachable for a sample that really did assemble — runs that finished before
  the gate existed wrote no row, and `align-denovo` is its first consumer — so the
  message now names re-submitting `long-read-assembly` as the remedy.

- **`docs/deploy-archive/2026-08-21-4fc4ce3d.md` carries a dated correction (#486).** Its
  claim that the 24 uncollapsed `reference_sequence_chunks` features are
  reverse-complement pairs needing their producing reference load re-run holds for one of
  them; the other 23 differ only in soft-masking case. The archived text below the note is
  unchanged.

- **Four broken relative links in `docs/deploy-archive/`, and a test that keeps them fixed.**
  All four targets existed; the paths were repo-root-relative (`](docs/runbooks/redeploy.md)`)
  inside `docs/deploy-archive/`, where they resolve against the containing directory. They come
  from `/deploy-archive` copying the `## Pending deploy` body out of `DEPLOY_CHECKLIST.md`, which
  sits at the repo root and where those links are correct — so the archive step now says to
  rewrite them as it moves the block, and to drop the `([archived](…))` self-reference a line
  acquires when it lands in the file it points at. `qiita-common/tests/test_doc_link.py` resolves
  every relative markdown link in the repo, target file and anchor both, skipping fenced blocks
  and inline code spans so a doc that spells out a link form is not resolved. Its slug function does
  not model the `-1` suffix GitHub appends to a repeated heading; disambiguate the heading text
  rather than the link.

- **Corrected the claim that miint cannot see TEMP or registered-Arrow relations (#477).**
  It resolves them since [duckdb-miint#193](https://github.com/the-miint/duckdb-miint/issues/193);
  our copies of that claim predated the fix and had gone stale in `docs/duckdb-miint.md`,
  `data_plane_client.open_read_block_stream`, and `read_source`. What still holds is now
  stated separately, because it is different in kind: CTEs never resolve (not catalog
  objects), and miint's fixed-temp-name paths — `massql`, and the per-sample
  (`sample_id := …`) branches of `uchime_ref` / `sylph_profile` / `woltka_ogu`,
  [duckdb-miint#207](https://github.com/the-miint/duckdb-miint/issues/207) — use a
  connection that does not inherit and still need a regular non-temp TABLE. Verified on
  the deploy-staged build `9fc4d12`: `woltka_ogu` with `sample_id` returned
  `Catalog Error` over both a TEMP TABLE and a registered Arrow relation while the same
  call without it resolved all three, so `qiita_common.feature_table`'s "stage a real
  TABLE" comments are correct and are left alone. Independently of visibility, a
  registered Arrow stream is consumed exactly once — a second scan returns zero rows
  silently — which is what the read spill in `read_source` actually rests on.

- **The circular gate's identity check now asks the question the gate answers (#475).**
  Its diagnostics counted scorable alignment RECORDS with `cigar_sequence_identity` while
  the gate itself applies `cigar_pooled_identity` per read: a read whose records mix a
  legacy `M` CIGAR with an extended one has NULL pooled identity, so it was dropped whole
  — including the records that did score — while the diagnostic reported those records as
  scorable and nothing refused. The circular arm now counts unscorable READS over the
  macro's own grouping key and refuses the slice, naming the two ways a read gets no
  pooled identity.

- **A circular threshold given without `--circular-gate` is refused instead of ignored
  (#475).** `qiita feature-table build --circular-min-coverage 0.5` with no
  `--circular-gate` built an entirely UNGATED table and said nothing, because the flags
  were read only inside the mode. They now default to a sentinel rather than to the
  threshold, so "omitted" and "given" are distinguishable, and giving one without the
  mode refuses the way `--unpaired-gate` alone already does. `--circular-min-identity`
  also accepts `none` — the spelling `--lane` already uses — which is the only way to
  express the documented no-identity gate for an alignment whose CIGARs cannot be scored
  (a threshold of 0 does not: a NULL score fails `>= 0` too).

- **The replay registry's `delete_*` claim (#469).** `REPLAY_SAFE_ACTIONS`' comment said
  re-running a delete "deletes zero rows". That holds only for a replay with no write in
  between: `delete_read_mask_block` (in `read-mask-block`) and `delete_alignment_block`
  (in `align`) run as a pre-`register-files` replace, so a token replayed after that
  registration drops the rows it wrote — and `delete_alignment_sample` takes on the same
  exposure once a workflow adopts it. The comment now says so, and names what bounds the
  window — the token expiry the data plane checks on every DoAction body (300s by
  default, `MAX_TICKET_LIFETIME` refusing an expiry more than 3600s out).
  `docs/auth.md#ticket-replay`, which that comment points at, carried the same claim and
  is corrected with it.

- **`alignment_delete_covers_every_alignment_scoped_lake_table` now checks columns too
  (#469).** It pinned that every `alignment_idx`-scoped lake table is in
  `ALIGNMENT_DELETE_TABLES`, but not that a listed table carries the columns the delete
  clauses key on. `delete_lake_rows` applies one clause to every listed table, so a table
  joining the list without `prep_sample_idx` or `sequence_idx` makes the narrower deletes
  unrunnable — loudly (DuckDB raises a Binder Error on the missing column and
  `delete_lake_rows` ROLLBACKs, so the leading table's rows re-count intact), but not
  until a ticket runs.

- **A redriven work ticket whose producer step re-ran can register its files again (#472).** `lake_dest_filename`
  minted `wt<work_ticket_idx>-<basename>`, whose uniqueness assumed one load per ticket.
  A redrive replays a ticket's storage tail into a fresh `attempt-<n>` workspace, so the
  second load targeted the byte-identical path the first had already registered and
  `move_file` refused it — `refusing to overwrite existing lake file
  …/assembled_sequence_chunks/wt6939-part_00000.parquet`, which blocked the unbinned-residue
  backfill for all 57 candidate prep_samples. The name now also carries a digest of the
  registration's staging dir **relative to `PATH_SCRATCH`**, which encodes the attempt
  without pinning the name to where scratch is mounted — a migration or remount changes
  that root without changing which registration a staging dir denotes. It stays
  deterministic for a given (ticket, staging scope), so a replayed DoAction recomputes the
  same name and
  `move_file` still refuses a true double-registration — the refusal that keeps
  `register_files` replay-safe for the tables outside `REPLACE_KEY_TABLES`
  (e.g. `reference_membership`), which have no replace-by-key to absorb one.


- **Two assembled contigs in one FASTA whose headers share a first token store each
  other's bytes (#464).** `read_fastx` returns one row per record, so two such records come
  back under one `read_id`; sharing a file they also share `kind` and `bin_id`, so
  `assembly_hash`'s whole synthetic `kind:bin_id:contig_id` repeats. Pass 2 joins the
  per-hash `winner` on that id over a fresh scan of every record, so each of the pair's two
  `sequence_hash` values receives both contigs' chunks — the bytes stored for a feature then
  include a sequence that is not that feature's, at the same `chunk_index`. Measured on a
  two-record fixture of 16 bp contigs: 2 chunk rows and 32 bytes under each of the two
  hashes, against 1 row and 16 bytes for the byte-identical fixture whose second header's
  first token differs. A colliding run leaves the manifest, bin_map and chunks all
  well-formed, so nothing downstream can notice it. The id's last component is now
  `read_fastx`'s per-file ordinal `sequence_index` rather than the contig id, which makes
  `kind:bin_id:sequence_index` unique by construction and also closes the other route to a
  repeated id, an unescaped `:` inside a bin_id — the job module carries the argument, and
  the two `sequence_index` facts it rests on are pinned against the real miint build in
  `qiita-compute-orchestrator/tests/jobs/test_read_fastx_miint_contract.py`. The contig id
  leaves the key and rides `bin_map` as its own `contig_id` column — no consumer joins on
  it; it is what lets a workspace under investigation say which assembled contig became
  which `feature_idx`, which for a MAG contig nothing else records. The step also counts
  the pass-2 rejoin now: `count(DISTINCT sequence_hash)` over the chunk Parquet against the
  distinct hashes pass 1 saw with bytes to store. A later divergence between the two
  `_READ_ID_EXPR` sites therefore fails the step rather than minting a `qiita.feature` that
  has a manifest row, an `assembly_membership` row and zero stored bytes — an outcome
  nothing downstream raises on, since `register_files` replaces chunks on `feature_idx` and
  a reader just gets an empty `string_agg`. Measured on a two-contig fixture whose composed
  id is NULL for one of them: manifest and `bin_map` carry both rows, the chunk Parquet
  carries one hash, and without the count the step exits 0. A zero-length contig record is
  left out of the count — `sequence_split('')` returns an empty list, so it has no chunk to
  rejoin and is input we did not produce.

- **Two refined-bin FASTAs stemming to one `bin_id` merged into one bin (#464).**
  `_FASTA_GLOBS` accepts `.fa` / `.fna` / `.fasta`, and `_local_id` strips the suffix, so
  `bin.1.fa` and `bin.1.fna` both became `bin.1` — one bin where there were two, in
  `bin_map` and so in `qiita.assembly_membership` and the `bin_quality` join it feeds, both
  of which key a bin on `(prep_sample_idx, processing_idx, kind, bin_id)`. Measured as 1
  distinct `bin_id` in `bin_map` against 2 for the same pair renamed. `_file_meta` now
  raises, naming both filenames; the synthetic read_id above rests on that raise too.

- **xdist workers shared one miint extension directory, so they installed on top of
  each other (#462).** `setup_miint_test_env` named the directory per *component*
  (`qiita-control-plane-duckdb-ext` under the system temp), and the control-plane suite
  is the one that runs `-n auto --dist worksteal` — so N worker processes pointed
  `extension_directory` at one path and each ran its own `INSTALL miint`. A cold INSTALL
  downloads into a `tmp-<uuid>` file and renames that over `miint.duckdb_extension` and
  its `.info`, which makes N workers N concurrent writers to those two paths; CI saw it as
  `IO Error: Could not set lock on file …miint.duckdb_extension.tmp-<uuid>.duckdb_extension.info`
  surfacing through an unrelated `_handle_feature_table_build` assertion, because the
  install that failed is the one the code under test performs (`connect_with_miint()`),
  not a conftest fixture — which is also why a fixture-level lock could not have covered
  it. The directory now carries the worker id when `PYTEST_XDIST_WORKER` is set
  (`qiita-control-plane-gw0-duckdb-ext`) and is unchanged for the single-process suites
  (qiita-common, the orchestrator, the integration tier). The worker also has to *replace*
  the directory it inherits rather than `setdefault` past it: a worker gets its environment
  from the controller, which ran the same helper first and had already exported the shared
  path, so the split alone left every worker pointing back at it — measured, the per-worker
  directories came out 0 bytes and the shared one held the only install. Only that one
  value is replaced, so a directory pinned from outside still reaches the suite as given.
  It costs no extra downloads on a cold cache — each worker already downloaded its own
  copy, only the rename target changes — each directory still caches across runs (24.5s
  cold, 4.9s warm for the tier), and the name still matches the documented
  `rm -rf "$TMPDIR"/qiita-*-duckdb-ext`, which a new unit test pins along with the split
  and the inheritance. It does multiply the cached bytes: 67 MB per worker under the system
  temp, 670 MB across the 10 workers `-n auto` picks on a 10-core machine. The race itself
  was not reproduced locally — 5×8 concurrent cold installs on linux/amd64 and 3×8 on
  macOS, both at DuckDB 1.5.4, produced no lock error — so the shared directory is the
  established defect and the lock error is a reasoned, not demonstrated, consequence of it.

- **`POST /exported-feature` would have published a qiita-derived genome's composed
  `source_id` verbatim (#462).** `_INSERT_GENOME` offered `qiita.genome.source_id` as the
  accession candidate for every genome, on the premise that `source_id` is NOT NULL and so
  the genome kind always has an accession to offer. NOT NULL holds; "is an accession" does
  not, for `source='qiita'` — a genome assembled from one of our own prep_samples has no
  external authority behind its source_id, and `export_feature_id` labels rows in a feature
  table, its taxonomy sidecar and its sheared tree. The statement now offers the accession
  only for a source on an allowlist of external repositories (`genbank`, `refseq`), so an
  internal source gets the hybrid's other half — a NULL accession, `accession_published`
  false, and the minted `QF<idx>` the generated column produces from that pairing. Written
  as an allowlist rather than `<> 'qiita'` so a further internal source mints a handle
  until it is listed, instead of publishing. `_INSERT_FEATURE` is untouched: it already
  reads a nullable `reference_membership.accession`. The one writer of such a genome row is
  a `qiita reference load` genome map declaring `genome_source='qiita'` with a
  `prep_sample_idx`; two new unit tests fail if a `GenomeSource` member is left
  unclassified by the predicate, or if `qiita` is classified external — the latter pinned
  as a literal, since every other assertion reads its expectation out of the predicate and
  so agrees with whatever it says. The route's OpenAPI description names the new case.

- **Re-running `long-read-assembly` over a sample doubled its `assembly_membership` and
  `bin_quality` rows (#460).** `processing_idx` hashes `{workflow, version, mask_idx,
  assembler}`, so a second run resolves to the SAME identity whenever those four hold — an
  edited workflow file included — and the submit path admits it: the prep_sample arm of
  `_check_disallow_without_delete` binds only the non-terminal states, so a COMPLETED ticket
  does not block a fresh one, and no assembly result carries a DELETE gate. Both tables were
  appended rather than replaced, leaving both runs' rows under one `(prep_sample_idx,
  processing_idx)` with nothing on the row to tell them apart. Measured across two
  registrations of the same rows: `assembly_membership` 0 → 2 → 4, `bin_quality` 0 → 1 → 2.
  `register_files` now replaces both on the composite `(prep_sample_idx, processing_idx)`, so
  a re-run supersedes that sample's rows for that run — a row agreeing on one half of the key
  (another sample of the same run, the same sample under a different run) is untouched. The
  replace-by-key statement widened from one key column to a row constructor to carry it; the
  file paths stay bound parameters.
  A re-run that yields no refined MAG is the case the key alone does not cover: CheckM
  covers refined bins only, so `assembly_load` writes `bin_quality` empty-with-schema, the
  file names no key, and a delete reading it removed nothing — measured, the previous run's
  MAG rows survived a re-run whose `assembly_membership` was replaced out from under them
  (`replaced` empty, 1 row before and after; the same load carrying one MAG row replaced it).
  `bin_quality`'s delete now reads the keys `assembly_membership` names in the same
  registration, which carries the run's key on every row and is never empty where the load
  runs at all (`assembly_hash` raises `StepNoData` at zero contigs of any kind).
  The file stated the delete's cost twice and the two disagreed: `LAKE_COMMIT_BUDGET` read a
  40k/400k timing as "the DELETE's cost does not grow with the table … so it prunes rather
  than scanning", while `replace_key_delete_sql` 25 lines above said a `feature_idx` set does
  not prune. That timing was taken on a contiguous incoming key set, which neither comment
  said. `replace_key_delete_sql` is now the only site that states it, and the budget points
  there: what the delete reads follows the SPREAD of the incoming key set against the
  per-file key ranges, not the key's arity and not the table's size. Measured on DuckDB
  1.5.4 / ducklake d318a545 — a composite `(prep_sample_idx, processing_idx)` pair scans
  17,544 rows of 1,000,008 and opens 1 of 57 files; a `feature_idx` set spread over the
  identity space scans 1,003,121 of 1,003,200 and opens all 57; a `feature_idx` set confined
  to one narrow window scans 17,602 of 1,003,200 and still opens all 57; a contiguous
  `feature_idx` block scans 2,000 rows and opens 1 file at both 40k rows over 20 files and
  400k over 200, where the same key count spread over the identity space scans 39,982 of
  40,000 and 399,819 of 400,000. The `WITH … DELETE … USING` comparison now reports the mean
  paired difference and its interval (-0.8 to +1.0 ms across four key sets on statements of
  3-37 ms, widest 95% CI [-3.2, +2.9] ms) rather than a bare non-significant p-value.

- **`test_assembly_hash`'s canonical-hash oracle mis-complemented a soft-masked contig
  (#460).** Its hand-rolled reverse complement translated through an upper-case-only table
  and upper-cased the result afterwards, so a lowercase base passed through uncomplemented
  and the "reverse strand" it hashed was the plain reverse. Over 2,000 random 16 bp
  lowercase sequences the oracle disagreed with `canonical_sequence_hash_expr` on 1,362
  (68.1%); over the same sequences upper-cased, on 0. The oracle now takes its reverse
  complement from miint's `sequence_dna_reverse_complement` — the scalar the production
  expression calls — applied to the upper-cased sequence, since that function preserves
  case. The `LEAST`-over-hashes composition is still re-derived in Python, so a change to
  how the two hashes combine still fails the oracle. A soft-masked fixture covers it; every
  sequence the file hashed before was either upper-case or a palindrome.
  `read_fastx` preserves input case (probed: an all-upper control record comes back
  unchanged, its lowercase twin comes back lowercase), so the soft-masked fixture reaches
  `canonical_sequence_hash_expr` still lowercase and the test pins the `upper()` inside that
  expression rather than a transformation the reader already did. That test now also pins
  which record's bytes reach the chunks: the fold keeps one representative per hash
  (`DISTINCT ON (sequence_hash) … ORDER BY sequence_hash, read_id`) and chunks it as read, so
  a different tie-break stores a different strand and casing with every hash assertion
  unmoved. One happy-path fixture is no longer a reverse-complement palindrome, so its
  `_hash` comparison exercises the fold instead of the identity.

- **A sequence two loads both produced was stored twice, and reassembled twice as long
  (#457).** `feature_idx` is minted from the canonical sequence hash, so identical bytes
  carry ONE feature across every producer — but each load still wrote that feature's rows in
  full: the compute job writing the staging Parquet has no DuckLake access to anti-join
  against, and DuckLake enforces no PK/UNIQUE. Two assembly runs producing the same contig,
  or two references sharing a sequence, therefore left two rows in `assembled_sequence` /
  `reference_sequences` and two per `chunk_index` in their chunk tables, so
  `string_agg(chunk_data, '' ORDER BY chunk_index)` returned the sequence concatenated with
  itself while `sequence_length_bp` still described one copy. Measured on the deploy
  2026-08-13: 182,988 `assembled_sequence` rows over 129,290 distinct features, 53,698 of
  them duplicated. `register_files` now REPLACES those four tables on `feature_idx` — the
  incoming Parquet's keys are deleted from the lake in the same transaction, ahead of every
  `ducklake_add_data_files` — so a second load converges instead of accumulating, and the
  per-table counts ride back to the control plane, which logs them. `REPLACE_KEY_TABLES` in
  `qiita-data-plane/src/flight_service.rs` is the registry and carries the admission
  conditions. The assembly tables were latent (absent from `ALLOWED_TABLES`, so nothing
  reads them over Flight); the reference pair is on the read path. Where a feature's copies
  differ — the canonical hash keeps a sequence, its reverse complement and its case variants
  on one `feature_idx` — the newest load's bytes now win; before, both were kept and read
  back concatenated, which is neither strand and matches no declared length.

- **Concurrent registrations of one feature no longer each kept a copy (#457).** The
  replace-by-key delete above closes the race only where the feature is ALREADY in the lake:
  DuckLake detects a conflict where two transactions touch the same existing row, so those
  writers serialize. When the feature is NEW, nobody's delete matches, nothing conflicts, and
  every writer commits — measured with 4 concurrent writers of one feature: 1 row when it
  already existed, 4 when it did not, and 4 for two bare `ducklake_add_data_files` with no
  delete at all. A registration touching a content-addressed table now bumps the single-row
  `qiita_lake.registration_lock` inside its transaction, giving concurrent writers a row to
  contend for (measured: back to 1 row), and retries its own transaction when it loses rather
  than failing the ticket — it cannot be retried from the top, because its staging files were
  already moved. Registrations touching none of those tables skip the lock and never contend.
  `delete_reference` takes the same lock and retry, because it writes two of the same tables:
  without the lock a registration could add a feature between its snapshot and its commit,
  where the orphan filter would not see the claim, and without the retry a registration's
  delete could newly conflict it out.

- **A retired `exported_feature` row could be edited out of the published namespace
  (#448).** Every CHECK on that table is written `retired OR …`, because a detached row has
  lost the columns they test — so a retired row is exactly where they stop guarding, and it
  is also where the namespace index is still reading `entity_kind`. Three UPDATEs each
  released a reserved accession: flipping `entity_kind` to `genome` drops the row out of the
  index predicate, and clearing `accession_published` or `accession` regenerates
  `export_feature_id` to `QF<idx>`. A second entity could then publish a string that had
  already named a different sequence, which is the one thing the table promises cannot
  happen. All three are now rejected by the trigger, above its retired early-return. No code
  path issued any of them, so nothing shipped wrong — this closes the hole, it does not
  repair damage.

- **Reference ingest cut a fixed three characters to strip a rank prefix (#448).** Latent,
  not live: every prefix in `RANK_PREFIXES` is three characters today. But the check that
  *requires* the prefix is generated from that tuple while the strip hard-coded its length,
  so a four-character prefix would have left a stray character in every published taxonomy
  sidecar with nothing to catch it. The strip now takes its length from the prefix it is
  removing.

- **A blocked contig's tip could be published in a sheared tree, under its genome's public
  name (#448).** The alignment and the taxonomy reach this recipe through exclusion-aware views, so a
  curator's blocklist already governs the table and its sidecar; the phylogeny deliberately has
  no such view — a row-wise anti-join would orphan a tip's internal parents and malform the tree
  — and the tree consumer is expected to apply the blocklist to its own keep-set instead. It was
  not. A genome that publishes on the strength of one contig, with a curator's block on the
  contig its tip is wired to, got that blocked tip's position written into the published tree.
  The build now reads the reference's blocklist and names a tip only when it is both published
  and unblocked. A genome with a second, unblocked tip publishes that one instead of being
  refused as ambiguous; a genome left with no usable tip is refused, naming it, since its only
  position in the tree comes from sequence the blocklist rejects.

- **A duckdb failure during a feature-table build reached the user as a traceback (#448).** The
  analytic runs on the caller's own machine, which makes duckdb's own failures both the
  likeliest thing to go wrong on a large reference — the peak is a whole tree, and the shear
  is single-threaded and allocation-bound — and the hardest to place, since an out-of-memory
  or a spill with nowhere to go says nothing about whose memory ran out. Now a message that
  says the analytic failed here, alongside the transport and refusal messages that were
  already handled.

- **The roll-up's coverage report understated the share it could not carry (#448).** It counted the
  rows of a join to the genome map, and a feature belonging to several genomes — which
  `feature_genome` allows on purpose, since identical bytes are one feature and a plasmid two
  organisms carry belongs to both — fans that feature's alignment row out once per genome. Only
  the denominator inflated, so a build reporting "1 of 4 (25.0%)" was really dropping 1 of 3
  (33.3%). Counted directly now, the way the relabel's own diagnostics already did.

- **The per-genome taxonomy reduction could return two rows for one genome (#448).** Picking a
  representative member and joining its row back matches twice if the caller's relations hold a
  duplicate at the winning position, where the aggregate this replaced could not multiply at
  all. The sidecar's row-count check would have caught it; the shard planner, the other consumer,
  has none — and two items sharing one id tile one genome into two shards. One row per genome is
  now a property of the SQL. A reference whose taxonomy genuinely repeats a feature is refused
  instead, measured on the streamed rows: two rows for one feature can disagree, and choosing
  between two lineages silently is not ours to do.

- **An alignment config whose stored form would stop matching its own digest is now refused at
  the mint (#448).** Postgres stores a JSON number as `numeric` and renders it back in plain decimal,
  so `1.5e30` returns as an integer literal that Python re-reads as an `int`, and `-0.0` returns
  as `0.0`. Either way the config read back no longer hashes to the `params_hash` stored beside
  it — permanently, for that `alignment_idx`. Nothing hits this today (no alignment config
  carries a float), but now that the digest is published and re-verified by clients, such a row
  would refuse every build over it with nothing to point at. The mint asks Postgres to normalize
  the blob and refuses a config that would not survive, so the failure lands on whoever adds the
  value. A small-magnitude float like `1.23e-05` does round-trip and is still allowed.

- **The documented `phylogeny_tip_feature` table never existed (#448).** Sites across
  `docs/architecture.md` and `CLAUDE.md` — an ER-diagram entity, three table inventories, an
  ingestion step, a worked clade-scoped query — described a Postgres junction table mapping
  `(reference_idx, node_index) → feature_idx`, contradicting the migration that deliberately
  did not build one. A phylogeny tip carries `feature_idx` as a **column on its own DuckLake
  row**; clade queries stay inside that one table instead of crossing to Postgres. The
  architecture doc also now records why phylogeny has no exclusion-aware `_visible` view (a
  row-wise anti-join would orphan internal parents and malform the tree, so a consumer shears
  to its keep-set instead). Alongside it, `CLAUDE.md` claimed `feature_idx` is "stored as
  Postgres `uuid`" — it is a BIGINT identity, and the uuid is the separate `sequence_hash`, so
  the claim was an invitation to write `feature_idx::uuid`.

- **`shear_tree` and the Newick writer are now documented from a probe rather than assumed (#448).**
  `docs/duckdb-miint.md` had no `shear_tree` entry at all and the architecture doc recorded its
  presence in the pinned miint mirror as unverified. It is present, and the entry now carries
  its real signature, the separate-connection relation resolution, the float branch-length
  summation, and both distinct failure messages. The load-bearing find is in the writer:
  **`COPY … (FORMAT NEWICK)` turns on jplace-style `{N}` edge annotations by default whenever an
  `edge_id` column is present**, so passing a sheared tree straight through — as upstream's own
  example does — writes a file plain-Newick parsers may reject.
- **A workflow that consumes a read mask now records it on `work_ticket.mask_idx` (#444).**
  The column was introduced for the minting path (`read-mask`, `fastq-to-parquet`), where
  the runner persists the mask it minted. `long-read-assembly` consumes an existing mask's
  `read_masked` pass-set instead, taking `mask_idx` from `action_context`, and never wrote
  it to the ticket — so every assembly ticket read NULL while depending on a mask. The
  shared-mask guard in `qiita-admin mask purge-failed` keys on that column, so a mask a
  completed assembly reads looked unreferenced and was eligible for deletion; the
  dependency survived only in `action_context` JSONB and `qiita.processing.params`, which
  no guard reads. The runner now persists the consumed `mask_idx` before staging the reads
  — the staging resolver's other terminal exit is `NO_DATA` (an empty pass-set), which is
  not a failure, so persisting afterwards would leave exactly those tickets NULL in a state
  the guard still protects. A context naming a mask that does not exist is now rejected by
  name rather than by foreign-key violation. A migration backfills existing tickets from
  `action_context->>'mask_idx'`, joining on text so no untrusted value is ever cast, and
  skipping rows whose named mask no longer exists (which `ON DELETE SET NULL` would have
  left NULL anyway). The `work_ticket.mask_idx` column comment now describes both minting
  and consuming.

  `purge-failed`'s mask-idx coverage gate — which refuses `--execute` while any non-failed
  ticket has a NULL `mask_idx`, since the guard would be blind to it — was scoped to the
  run's candidate actions, so selecting one with `--action` also narrowed what counted as a
  blind spot while the guard itself reads tickets of every action. It now uses its own
  list, which includes `long-read-assembly`, and the refusal message and dry-run banner
  name that list rather than the candidate one.

- **Deleting a reference whose feature an assembly also claims no longer 500s (#443).**
  Assembled contigs and reference sequences are minted through the same
  `mint_features` path on the same canonical hash, so a contig whose bytes match a
  reference sequence collapses to one `feature_idx`. The orphan-feature computation
  in `delete_reference_cascade` counted only `reference_membership` and
  `reference_annotation` claims, so such a feature looked orphaned and the
  `DELETE FROM qiita.feature` hit `qiita.assembly_membership`'s NO ACTION foreign
  key — a `ForeignKeyViolationError` that aborted the whole cascade as an unmapped
  500. `qiita.assembly_membership` now counts as a claim that keeps a feature, so the
  delete succeeds and the shared feature survives exactly as one claimed by a second
  reference does. The data plane's `delete_reference` orphan filter is unchanged, so
  the two stores now GC on deliberately different rules: the lake still drops the
  `reference_sequences` copy of a feature no REFERENCE claims, and the assembly's own
  bytes in `assembled_sequence` / `assembled_sequence_chunks` carry it from there,
  keyed by the retained `qiita.feature` row.

- **A study link retired mid-request answers 404 instead of 500 (#386).** The
  study-scoped metadata write for a biosample or sequenced-sample checks the
  caller's study link before writing, and the database re-checks it at the write
  itself; a link retired in the window between the two was refused by the
  database and surfaced as an unmapped 500. The refusal now answers the same 404
  a link retired before the request would have, so the status reflects what the
  caller may do rather than which of the two checks noticed, and a retry sees
  the same answer. Both retired-link trigger functions tag their rejection with
  a structured error DETAIL naming the raising function, which is what lets a
  route tell that rejection from the other guards on those tables sharing its
  SQLSTATE; every other rejection still surfaces unchanged.

- **A blank sample metadata value is rejected at the wire boundary (#386).** An
  empty or whitespace-only value is a 422 naming the field it was sent for,
  across every request body carrying a metadata dict (biosample import,
  sequenced-sample create, and the study-scoped metadata write). Values are
  outer-stripped before being parsed into their field's data type, so a blank
  one reached storage as `''` on a `text`-typed field — occupying the field's
  slot while carrying no information, and satisfying a presence check without
  answering it. Declining to answer remains expressible with a missing-value
  marker (`not applicable`, `missing: control sample`, …); a field with nothing
  to say is omitted.

- **A numeric metadata write reports the value as it is stored, and a change of
  scale counts as a change (#386).** A NUMERIC value is now parsed into the
  representation the database stores, so the value written, the value compared
  against an occupied slot, and the value reported back are one and the same
  form — a caller sending `1e3` gets `1000` back rather than a form the row does
  not hold. Occupied-slot comparison for NUMERIC follows the stored
  representation rather than numeric equality, so rewriting `5` as `5.0`
  preserves the added precision instead of being reported as an unchanged no-op,
  while a notation that resolves to what is already stored still reports
  unchanged and writes nothing. Repeated `NaN` writes now report unchanged too,
  matching how the database compares them.
- **Metadata values can no longer be overwritten through a retired study link
  (#386).** The non-retired-link invariant on `biosample_metadata` /
  `prep_sample_metadata` was enforced only on INSERT, which was equivalent to
  guarding every write while metadata rows were insert-only. The new in-place
  upsert made an overwrite another way for new data to arrive, so a migration
  adds a `BEFORE UPDATE` twin of the guard, scoped to the value columns and the
  source field so a local-to-global link upgrade still propagates onto rows
  whose link is retired. The existing trigger functions are reused unchanged.
  Enforcing this in the database also closes the window between a caller's link
  check and its write, which no application-level check can make atomic.

- **`assembly_coverage` writes its BAM coordinate sorted, and `binning.sh` stages it
  instead of running `samtools sort` over it (#422, closes #374).** The sort ran unconditionally
  on every long-read-assembly ticket — 19 s wall and 11.1 GiB peak RSS at 16 cpu,
  measured inside the binning image on the 2.0 GB BAM that first exposed the problem
  (~13.5 s on the ticket itself) — and it existed only because miint's `@SQ` order
  was then derivable from nothing, so no `ORDER BY` the step could write made the file
  sorted. `@SQ` is now sorted by reference name
  ([duckdb-miint#173](https://github.com/the-miint/duckdb-miint/issues/173)), so tid
  order is name order and the step's `ORDER BY reference, position` on the `COPY` is a
  genuine coordinate sort. The sort is one row per alignment and carries no read bytes
  (SEQ/QUAL come from `SEQUENCE_DATA` at write time), and DuckDB spills it — the side of
  that step's memory split that is allowed to.

  **Measured against the consumers, not inferred from the header order**, on the binning
  image's own pins (metabat2 2.15, samtools 1.10), with the same BAM written without the
  `ORDER BY` as the control: `jgi_summarize_bam_contig_depths` rejects the control
  ("the bam file is not sorted!") and accepts the ordered file, reporting a
  depth/variance matrix identical to the one it produces from a `samtools sort` of that
  same file; `samtools index` — which metaWRAP's concoct block runs — likewise fails on
  the control and succeeds on the ordered file, despite miint writing no
  `@HD SO:coordinate` tag ([duckdb-miint#202](https://github.com/the-miint/duckdb-miint/issues/202)).
  And at production scale, since a small write cannot exercise a parallel sink: 2M records
  over 20k references (the ticket had 925,483 over 20,975) with `threads=8` and the sort
  spilling came out fully tid-monotonic under `preserve_insertion_order=false` — identical
  to the `=true` control, against 999,778 backsteps for the same relation written without
  the `ORDER BY`. So the `preserve_insertion_order=true` override this job once carried
  "solely to protect that ORDER BY" is not reinstated. Pinned by
  `test_written_bam_is_tid_monotonic`, which runs the real step on a 13-contig fixture
  with shuffled reads and goes red (38 of 72 records backwards) if the `ORDER BY` is
  deleted; the 3-contig fixture that let the original defect through orders identically
  whichever way you look at it.

  The staged file is a plain copy: `coverage_bam`'s directory and `QIITA_OUTPUT_PATH` are
  separate apptainer `--bind`s and `link()` refuses to cross a mount even within one
  filesystem (measured), and `LocalBackend` — the one backend that could have shared a
  mount — refuses container steps. So the second reads-sized artifact remains; what goes
  away is the sort's CPU, RSS and spill. **`binning.sh`'s assembly-FASTA reordering stays
  and is unaffected**: metabat2 requires the depth matrix and the assembly in the same
  contig order, `@SQ` is name-sorted while the assembler emits numeric order, and ordering
  records does not move an `@SQ` line. `test_binning_coverage_sort_pin.py` becomes
  `test_long_read_assembly_entrypoint_pins.py`, covering the bin_refine/checkm/image pins
  in it as well as the staging ones.

- **Both services now narrate one unconditional line at boot, and the deploy check for
  it can actually fail (#406).** Review found the CO half of that check passing on
  nothing: it grepped the journal for a bare `INFO ` and got the same result — no match
  — on a healthy CO and on one with `configure_logging()` stubbed out, so an operator on
  a correct deploy would conclude the fix had not landed. Two independent causes, both
  reproduced. uvicorn's formatter emits `INFO:` plus padding, which a trailing-space
  pattern excludes; and the CO had no unconditional boot INFO at all, its only two
  `.info()` sites being work-triggered (a SLURM JWT inside its refresh margin, a miint
  re-stage), so an idle CO narrated nothing. Both lifespans now log one line naming the
  resolved log level — the CO's also names its compute backend, the CP's its fan-out
  default — and `configure_logging()` returns the level it resolved so they can. The
  check greps the **dotted logger name**, which only a configured root logger produces:
  verified against a real uvicorn boot in both arms (as-shipped matches, pre-fix does
  not). Dropping the space instead would have been the worse fix — it matches uvicorn's
  own lines and would pass on a service still carrying the bug.

- **A fan-out override lowered below the default was silently undone by a restart
  (#406).** The revert-to-default on restart was documented as "the conservative
  direction", which holds for the raise case the surface exists to serve and not for the
  other one: a cohort capped at 2 came back at 8 after a restart — 4× what the operator
  set — because `reconcile_inflight_tickets` re-pumps every held cohort with
  `settings.fanout_max_inflight` and the registry is already empty by then. Still
  deliberately in-memory (an incident knob, not durable state), but no longer silent:
  `set_override` now WARNs when it records a cap below the default, naming the cohort and
  both numbers, and the asymmetry is spelled out where someone hits it rather than read
  as true in both directions. The restart is not necessarily operator-initiated — both
  units are `Restart=on-failure`, so the plausible case is a crash during the very
  incident being throttled. Both directions are pinned by a two-arm test, since the whole
  design rests on these semantics.

- **`GET /work-ticket/fanout` hid an override the moment its cohort drained (#406).**
  Nothing evicts from the override registry except an explicit clear, while the listing
  showed only cohorts with held or in-flight children — so an override became
  unenumerable at exactly the point it turned into a surprise, still set and still
  reapplying if that `(kind, key)` were ever re-run, with no surface left to show it. An
  operator who set three during an incident could not ask "what have I set?". The listing
  now unions in every overridden cohort, deduped by identity; a drained one appears with
  zero counts and a non-null `override`, and `qiita-admin fanout list` flags it as
  clearable. Also hardened `_cohorts_matching`'s trust boundary from a docstring into an
  assert over the two module-constant predicates, so a later refactor that threads
  caller input into that interpolated SQL fails at once instead of opening an injection.
  Two remaining review findings are deferred rather than silently dropped: the hand-typed
  work-ticket state literals in this module (#424) and the multi-acquisition read behind
  this listing (#425).

- **Neither Python service configured logging, so every `_log.info` was silently
  dropped in production — and the Authorization scrubber was inert (#406).** The CP
  and CO both called `install_authorization_scrub()` but nothing ever configured the
  root logger. Records from module loggers therefore fell through to Python's
  `lastResort` handler, which is WARNING-only, so the services' entire operational
  narration — fan-out pump decisions, dispatch lifecycle, sweeper passes — never
  reached the journal. Diagnosing a frozen alignment fan-out took an afternoon
  because the pump's own `fail-stop` line was among the invisible ones. Worse,
  `install_authorization_scrub()` walks `root.handlers` and uvicorn installs its
  handlers on its own loggers, so with root empty the loop body never ran: the filter
  that keeps `Bearer` tokens out of logs was attached to nothing in both processes.
  New `qiita_common.log.configure_logging()` installs a root handler and sets the
  level from an optional `LOG_LEVEL` (default INFO; an unknown name fails the boot
  rather than silently reverting), called first in both lifespans. It now covers
  everything that propagates to root, including `httpx`; `uvicorn` and
  `uvicorn.access` keep `propagate=False` and stay outside it (#408).

- **A fan-out cohort could be stranded with no way to recover it (#406).** Two
  distinct paths. `_pump_ticket_cohort` swallowed pump failures on the theory that
  "the next child's completion will re-attempt it" — false at the tail of a fan-out,
  where the failing child was the last in flight, every remaining ticket is held, and
  no terminal transition is left to re-trigger anything; only a CP restart recovered.
  It now retries once and then logs an ERROR naming the cohort. Separately,
  `top_up_dispatch` commits its release *before* dispatching, so a raise partway
  through the dispatch loop abandoned the rest of the batch in the one unrecoverable
  state: no longer `dispatch_held` (so no pump would re-release it) yet never
  dispatched, while still counting as `running` and so reading as healthy. Each
  dispatch is now individually guarded, naming the casualty and its `POST
  /work-ticket/{idx}/run` recovery. The pump's `fail-stop` message also moved
  INFO → WARNING: a frozen fan-out is operator-actionable and at INFO was
  indistinguishable from an ordinary "no free slots".

- **`make lake-shell` could not start under bash 3.2, which left CI red on macOS (#406,
  fixing #418).** `add_pgpass_entry`'s seen-set was an associative array. bash 3.2 — what
  macOS ships as `/bin/bash`, and what `test_deploy_scripts.py` runs the script under on
  the mac runner — has none, so the shell died at `declare: -A: invalid option` before
  reaching any of its own error paths, and the test asserting a *specific* hard failure
  saw exit 2 instead of 1. It is now two parallel indexed arrays with a linear scan,
  bounded by an explicit count rather than `${#array[@]}`, because bash 3.2 under `set -u`
  treats an empty array as unset; a single delimited-string map would have needed escaping
  the scan does not, since a password may hold any byte. Verified against bash 3.2.57 —
  both the seen-set semantics and the end-to-end refusal path. No live behaviour changes:
  the deploy host is Linux. Carried in this PR because it also blocked its CI.

- **An escalated memory/walltime floor now survives a restart or a redrive, instead
  of restarting the ladder from the YAML baseline (#415, closes #413).** The floor
  `_run_entry_with_retry` climbs on an OOM/TIMEOUT retry lived only in a local
  variable, so a control-plane restart or a `/run` redrive discarded it and the
  ticket re-burned a failing attempt getting back to a size it had already reached.
  Observed on `long-read-assembly` tickets 6978 / 6980 / 6989: each auto-escalated
  `assemble` from 192 GB to 384 GB, then came back at 192 GB after the redrive and
  had to OOM again (~40 min apiece) before re-climbing. Deriving the floor from
  `work_ticket_step` history would not have covered it — the redrive DELETEs every
  `failed` row in the same transaction as the state reset — so the floor is now
  persisted on the ticket itself, in a new `qiita.work_ticket.escalated_resource_floor`
  JSONB column keyed **per step**, so a ticket that learned `assemble` needs 384 GB
  doesn't also hand 384 GB to `bin_refine` (YAML: 32) the way the ticket-wide
  `resource_override` would. Both escalating axes are covered, written independently
  and merged, so raising the memory floor never drops a walltime floor the same step
  learned earlier. On a CP restart the saving is more than wall-clock: `retry_count`
  survives a resume while the floor did not, so the re-climb also spent a rung of a
  3-rung budget. The column's own `COMMENT ON` carries the semantics, including how
  to clear it.

- **Drop the GFF `+ 1` now that miint normalizes `read_gff` to half-open (closes #410).**
  `read_gff` / `read_ncbi_annotation` used to emit GFF3's closed `end` under the same
  `stop_position` column every other miint reader uses for a half-open one;
  [duckdb-miint#200](https://github.com/the-miint/duckdb-miint/pull/200) normalized them.
  `hash_sequences._write_annotation_manifest` compensated with its own `+ 1`, which became a
  silent double-conversion once the mirror picked that up — every annotation interval one base
  too long, no error. Stored values are unchanged from before the upstream fix, so nothing
  already ingested is affected, and the capability is not in production use yet. Its
  `WHERE type IS NOT NULL` guard goes too: `read_gff` now honours `##FASTA`
  ([duckdb-miint#186](https://github.com/the-miint/duckdb-miint/issues/186)).

- **Re-pin the `@SQ` contract tests: miint now sorts `@SQ` by reference name
  ([duckdb-miint#173](https://github.com/the-miint/duckdb-miint/issues/173)).** The two canaries
  in `test_assembly_coverage.py` exist to fail when `@SQ` gains a defined order, and did; they
  now pin the order rather than its absence. `binning.sh`'s `samtools sort` and FASTA reordering
  **stay** — `assembly_coverage` applies no `ORDER BY`, so its output is unsorted regardless, and
  retiring them needs measuring against metabat2 (#374). The test helper that hand-parsed the BAM
  binary header is replaced by `read_alignment_header`
  ([duckdb-miint#174](https://github.com/the-miint/duckdb-miint/issues/174)).
- **Six workflows had `action_ceiling` equal to their heaviest step's baseline,
  silently disabling OOM/TIMEOUT retry (#420, closes #411).** With `baseline == ceiling`
  the escalation helpers (`_escalated_mem_floor_after_oom` /
  `_escalated_walltime_after_timeout`) grow the floor and clamp it to the ceiling, so the
  grown value never exceeds the resolved one; the runner reads that as saturation and
  raises `RESOURCE_CEILING_EXHAUSTED` **permanently, at `retry_count=0`, without ever
  retrying** — the same shape that made a single `assemble` OOM unrecoverable in
  `long-read-assembly` (#393). Each ceiling now sits above its heaviest step on the
  escalating axes: `bcl-convert/1.0.0` 480→500 GB and PT12H→P1D (dead on *both* axes for
  the NovaSeq X profile), `fastq-to-parquet/1.1.0` and `1.2.0` 16→32 GB and PT4H→PT8H,
  `fastq-to-parquet/1.3.0`, `read-mask/1.0.0` and `read-mask-block/1.0.0` 32→64 GB.
  **Every step baseline is unchanged**, so ordinary tickets request exactly what they did
  before and schedule identically — only a *failing* step climbs. `cpu`/`gpu` are
  untouched: nothing escalates them, so a step whose cpu equals the ceiling gives nothing
  up. `align/1.0.0` keeps its documented accept. Also raises the admin
  `resource_override` envelope, which is bounded by the same ceiling.
  Sized from `sacct` on the `qiita` partition rather than a blanket multiplier: NovaSeq X
  demuxes peak at 364.7/282.2 GB against a 480 GB request; `host_filter` OOM-kills at
  16 GB and completes at 32 GB peaking at 22.0/22.6/25.8 GB — real demand, since
  `host_filter` deliberately does not size DuckDB from the cgroup. All of it measured on
  `read-mask/1.0.0` (4558 tickets), which carries essentially all `host_filter` traffic;
  the module is shared, so the sizing carries to the other workflows that run it. `bcl-convert`'s 500 is
  the node bound (`RealMemory=514000` MB, no `MemSpecLimit`; a 500 GB request is confirmed
  schedulable on the partition), so its rung is +4%, not a doubling: strictly better than
  a terminal first OOM, but not a guaranteed save. `cpu` stays 16 there deliberately —
  more threads would raise memory demand, and memory is the axis already at the node bound.
  Step *baseline* sizing is deliberately out of scope — `fastq-to-parquet/1.1.0` and
  `1.2.0` keep a 16 GB `host_filter` baseline and so still pay an OOM plus a retry to
  reach 32; raising a baseline changes what every healthy ticket requests, which is a
  separate call from giving a failing one somewhere to climb. Note `1.0.0`/`1.1.0`/`1.2.0`
  are disabled in `qiita.action` with no live tickets: their raise is insurance against a
  future re-enable restoring a dead ladder, not a fix for traffic. Retiring them outright
  is the better answer and is deliberately not attempted here — `actions sync` has no
  prune path, so it needs a DB call as well as a YAML deletion.
- **The six workflows tracked in `_ESCALATION_PENDING_RESIZE` are re-sized, so that dict
  is now empty (#420, closes #411).** #421 landed the build-time guard and listed the six
  defective versions there rather than fixing them, because re-sizing each needs measured
  peak-RSS data per workflow. Raising their ceilings deletes their entries — the guard's
  exact-equality check fails while a suppression outlives its reason, so the two changes
  interlock rather than merely coexisting. The dict itself stays, empty: the guard's
  failure message points a future tracked-but-unfixed defect at it.
- **Single-end blocks no longer pay double the rype index reads: the routing classify
  gets `sequence1` alone.** `align_sharded` and `host_filter` both handed
  `rype_classify` a relation with a `sequence2` column that is entirely NULL for
  single-end (PacBio HiFi) data. miint derives rype's `is_paired` from the column's
  **presence**, never its values (`ValidateSequenceTable`) — where the RYpe CLI derives
  it from **content** — so rype assumed a query twice as long and **halved its Arrow
  batch size**, and it reloads the whole index once per batch. That CLI/miint asymmetry
  is why the bug survived: a `rype classify run` on the same Parquet reports
  `is_paired: false` and the un-halved batch, so a CLI reproduction looks healthy. On a 750k-read HiFi block against the 193 GB `w=20` WoL3 router that
  turned 2 full index reads into 4, at ~54 min each: **~1.8 h of a 4 h walltime budget,
  spent re-reading the index**. Both jobs now project a narrowed view
  (`align_sharded._ROUTING_QUERY`, `host_filter._RYPE_QUERY`); the aligners keep both
  mates, since `is_paired` reaches rype only through batch sizing and never through
  `rype_classify_arrow`. Filed upstream as
  [duckdb-miint#199](https://github.com/the-miint/duckdb-miint/issues/199) (derive
  `is_paired` from content) and
  [the-miint/RYpe#21](https://github.com/the-miint/RYpe/issues/21) (load the index once
  per invocation — index load is 98.4% of classify wall clock); removal of our
  workaround tracked at #403.
- **`qiita.alignment_sample` is now indexed by `prep_sample_idx`.** Its primary
  key is `(alignment_idx, prep_sample_idx)`, which serves every consumer that
  leads with a known alignment — but the new pool-alignment discovery read asks
  the opposite question ("which alignments touch THESE samples?") with no
  `alignment_idx` predicate, and a composite btree cannot be used on its
  non-leading column. That sequential-scanned an unboundedly-growing table (one
  row per alignment config × sample, across every reference, aligner and rerun)
  from a route open to any authenticated user. Added
  `(prep_sample_idx, alignment_idx)`, built `CONCURRENTLY`, turning that
  sequential scan into an index scan. Not index-only — the query counts
  `state = 'completed'`, which is off the index — see the migration for why
  `INCLUDE (state)` was not taken. (#436)
- **A multi-sample masked-read DoGet scanned the entire `read` table; `read_masked`
  is now a scoped table macro instead of a view (#433).** DuckDB derives a transitive
  predicate across a join equality for `col = const` but **not** for
  `col IN (list)`, so a view could only ever receive a multi-sample scope on one
  side of the `read`/`read_mask` join: the `read` scan got no filter, DuckLake
  pruned nothing, and every file in the lake was read. Production `EXPLAIN`
  confirms it — a single-sample equality returns 5,356 rows in 0.147 s, while a
  ~100-sample `IN` list fully scans a ~20.7-billion-row table (32 GB RAM and
  >250 GB swap before being killed). `read_masked(p_mask_idx, p_preps)` takes the
  scope as arguments so it lands on **both** inputs. On a local DuckLake of
  1,000,000 rows over 200 samples, a realistic block (one partial head sample, 18
  complete, one partial tail) selecting 84,600 rows went from 1,033,599 rows
  scanned to 178,599, against a floor of 169,200. Affects `read_masked` and
  `read_masked_block` — production's main read path — but **not** single-sample
  blocks (a one-element `IN` is rewritten to `=`), which is why it went unnoticed.
  Rejected after measuring: passing the block's `sequence_idx` range as further
  parameters (identical row counts) and pushing per-member `(sample, range)` pairs
  down as an `EXISTS` (1,900,000 rows — worse than the view, it defeats file
  pruning). Result rows, column set and column order are unchanged. Side effect:
  an unscoped fleet-wide masked read is now **unrepresentable** rather than
  refused, so the control plane's mandatory-filter invariant is defence in depth
  instead of the only guard.
- **Restart recovery no longer resumes into a dead step attempt, and `/run` no longer strands a live SLURM job (#402).**
  Establishes one invariant across the runner and the redrive route: **only a LIVE
  attempt is adoptable.** Both defects below were latent until a step could have
  more than one attempt — an OOM-killed step used to fail permanently at attempt 0,
  so neither path was ever exercised. Together they killed three
  `long-read-assembly` tickets whose `assemble` had OOM-escalated: each died with
  `manifest.json missing (…/assemble/attempt-0/output/manifest.json)` while its
  escalated attempt-1 job sat queued and untouched.
  - **Resume adopted a terminated attempt.** The per-invocation `attempt` counter
    restarts at 0, so a control-plane restart re-entered a step at attempt 0 even
    when attempt 1 was live. `_attempt_is_unowned` only asks whether a row *exists*,
    so a terminal `failed` row read as "owned" and the runner re-attached to the
    ENDED job. slurmrestd had purged it, so the poll loop's filesystem tiebreaker
    synthesized COMPLETED and verified the dead attempt's workspace — which has no
    manifest precisely because that attempt failed — failing the ticket with a
    CONTRACT_VIOLATION. A new `_attempt_is_terminal` predicate skips any attempt
    already terminal, so recovery lands on the live one. Skipping now also consults
    the retry budget, which the fresh-submit path would otherwise bypass (it never
    passes through the `except BackendFailure` arm that enforces `max_retries`).
  - **`/run` deleted in-flight progress rows.** The redrive dropped every
    non-`completed` row, justified by "a FAILED ticket has no in-flight job" — no
    longer true, since escalation can leave attempt N+1 `submitted` with a real
    `slurm_job_id` while the ticket fails. Deleting that row orphaned the job
    permanently (adoption re-attaches by exactly that persisted id). The redrive now
    keeps a live row **when it names an adoptable job**: `completed` survives for
    fast-forward, `failed` is always dropped, an in-flight row with a job id
    survives a FAILED redrive, and two carve-outs still drop it — after a CANCEL
    (whose reap already killed the job, so adopting it would reproduce the same
    failure through a live row) and for a write-ahead `submitting` row with no
    persisted id (whose find-by-name closer only runs under `resume=True`, so
    keeping it would let a fresh submit collide with a possibly-live orphan in the
    same attempt dir).
  - `TERMINAL_STEP_PROGRESS_STATES` / `LIVE_STEP_PROGRESS_STATES` join
    `TERMINAL_WORK_TICKET_STATES` in `qiita-common`, derived the same
    name-the-terminal-side-and-complement-it way, so a new `StepProgressState`
    becomes adoptable only by an explicit edit.
- **`docs/duckdb-miint.md` audited against a built extension; stale warnings that cost us work are gone (#401).**
  Every claim re-verified against duckdb-miint `97a3fff`. All 84 functions the file
  named still exist and nothing had been removed upstream — the damage was mirrored
  upstream detail going stale, plus warnings for bugs since fixed. Removed advice that
  was actively wrong: `alignment_slice` does **not** coerce `read_id` to VARCHAR (fixed
  upstream in `e739376`, so the "cast back" advice would have broken the join it claimed
  to fix); the GPL boundary hosts **only** bowtie2 + FastTree, so `merge_pairs_vsearch`,
  `detect_chimera_uchime*`, `cluster_sequences_vsearch`, `search_sequences_vsearch`,
  `align_mafft` and `align_sortmerna*` need no `install_gpl_boundary()`; and
  `save_bowtie2_index` **does** require it, where the file said the opposite. Every
  filename in the upstream docs map was dead after miint's 2026-07 docs reorg, and the
  documented refresh script `curl -fsSL`'d all of them — silent 404s, exit 0 — which is
  why the rot went unnoticed. The hand-maintained embedded-tool version table is
  replaced by `miint_versions()` (it claimed WFA2-lib 2.3.5 against a 2.3.6 build), and
  the file now leads with a catalog-first recipe: `duckdb_functions()` answers existence,
  named params, overload arities and macro bodies, with COPY writers called out as the
  one blind spot. Filed the-miint/duckdb-miint#186 (`read_gff` does not stop at
  `##FASTA`) and #188 (QC bracket notation implies partial arg lists that don't exist);
  signposts #196 (make miint's surface self-describing).
- **`qiita reference load --shard-index` now requires `--genome-map`, failing fast instead of after a full ingest (#324).**
  Without a genome map, `plan-shards` derives zero genome-bearing features from
  `qiita.feature_genome` and fails with `N=0` ("reference … has no genome-bearing
  features to shard") after hash → mint → load → register-files have all run —
  observed in production on Web-of-Life 3 after an ~18h load. A CLI guard fires
  before any network call, and an `if/then` conditional in both reference-add
  workflow schemas catches direct `POST /work-ticket` submissions that bypass
  the CLI. The existing `plan-shards` `N==0` guard is retained as backstop.
- **`reference-add` / `local-reference-add` schemas now validate `gff_upload_idx` / `gff_path` (#324).**
  Restructuring the `not:` block into an `if/then` for the `shard_index` guard
  re-parented `gff_upload_idx` and `gff_path` from inert unknown keywords into
  declared `properties`. With `additionalProperties` unset, the GFF keys were
  previously accepted without type or range checking; a malformed GFF handle
  (e.g. `gff_upload_idx: 0` or `gff_path: "rel/x"`) slipped through to a
  server-side failure. Both now reject at submission with a 422.
- **Purging an alignment no longer re-types its block tickets as read-mask blocks (#400, closes #394).**
  `work_ticket.alignment_idx` is `ON DELETE SET NULL`, and `alignment_idx IS NULL`
  was also the discriminator for "this block ticket is a read-mask block". So
  `DELETE /alignment-definition` turned every align block ticket of that alignment
  into an apparent read-mask block of the `mask_idx` it still carried. A `failed`
  one then landed in `read_mask_block_cohort(mask_idx)`, where the pump's fail-stop
  releases nothing — silently halting **all** future block-mask fan-out for that
  mask, a fleet-wide config hash, in a subsystem the operator was not touching. It
  also defeated the align-block exclusion in `has_incomplete_covering_block`, whose
  docstring already named this exact wedge as the thing it was written to prevent.
  Block kind is now read from `action_id`, which is NOT NULL and which no FK action
  can clear, at all four sites (`read_mask_block_cohort`, `cohort_for_ticket_row`,
  `held_cohorts`, `has_incomplete_covering_block`). Same conclusion
  `block_read.resolve_block_read_scope` already reached from the other direction, for
  a sharper reason — trusting the nulled column there would stream raw,
  non-host-depleted reads into an aligner. The two block action ids join
  `READ_MASK_ACTION_ID` / `BCL_CONVERT_ACTION_ID` in `qiita_common.actions` as bare
  ids; each submitter still pins its own version. Deliberately version-agnostic: a
  cohort and a finalize gate must span every in-flight version of an action, so
  filtering those on version would split one cohort's concurrency accounting across a
  routine bump. **The fix is retroactive**: existing detached tickets stop
  contaminating their mask cohort as soon as this deploys, with no cleanup.
- **`long-read-assembly`: raise the action ceiling above the `assemble` baseline so OOM/TIMEOUT escalation can actually retry (#393).**
  `action_ceiling` was `32 cpu / 192 GB / PT16H`, byte-identical to the `assemble` step's
  `baseline_resources` on every axis. A ceiling equal to the baseline silently disables
  retry on that axis — the runner grows the floor and clamps to the ceiling, so the grown
  value never exceeds the resolved one, which reads as saturation and fails the ticket
  **permanently on attempt 0**. A single `assemble` OOM was therefore unrecoverable, dying
  with `retry_count=0` and "escalation exhausted, not retrying" instead of retrying larger.
  The ceiling is now `mem_gb: 500` / `walltime: P2D`; every step baseline is unchanged, so
  ordinary tickets request exactly what they did before and only a *failing* step climbs.
  Sizing is measured: hifiasm_meta's peak scales with thread count, and the 16-thread
  reference for this assay peaked at 164.8 GiB across 26 samples with a slowest sample of
  28h16m, while our 32-thread configuration peaked at 182.3 GiB on the *smallest* input and
  exceeded 192 GiB on a mid-sized one. `cpu` stays 32 (nothing escalates it).
- **Native/CLI venv refresh no longer skips qiita-common reinstall, closing a deploy gap that broke every native SLURM job (#332).**
  `redeploy.sh` steps 5 and 6 dropped the "prove it's current" skip that could fire
  incorrectly when an operator `git pull`ed manually first (redeploy's own pull was
  a no-op → `native_pkgs_changed()` returned "provably unchanged" → skip)
  or when `local-deploy.sh` ran directly (it only syncs the `/opt/qiita` service
  venvs, never the checkout native venv). Either way the native venv kept the old
  `qiita-common` while its editable source moved forward. `uv sync
  --reinstall-package qiita-common` is a sub-second local copy; the optimization
  traded negligible time for a real correctness gap. The native-import probe also
  deepens to import `qiita_compute_orchestrator.config` so a stale venv fails
  deploy verify as a backstop.
- **Compute-orchestrator no longer floods logs with Acero "poorly aligned buffer" warnings on Flight-sourced DuckDB scans (#333).**
  pyarrow's Acero engine warns per batch when it receives Arrow buffers whose
  base address is not 64-byte aligned. The misalignment is introduced by gRPC
  transport buffers on the receive side — arrow-rs already writes IPC with
  `alignment=64`, so a producer-side fix is not possible. 8-byte-aligned buffers
  are valid on all modern x86_64/ARM; the warning is a defensive hint, not a
  correctness issue. Setting `ACERO_ALIGNMENT_HANDLING=ignore` at module load
  silences it; `setdefault` preserves operator override.
- **`long-read-assembly` `checkm` no longer dies with `AF_UNIX path too long` —
  `checkm.sh` shortens `TMPDIR` for CheckM's multiprocessing socket (#379).**
  CheckM's `markerGeneFinder` runs `multiprocessing.Manager()`, which binds an
  AF_UNIX socket at `$TMPDIR/pymp-XXXXXXXX/listener-XXXXXXXX`. The SLURM payload
  sets `TMPDIR=<workspace>/tmp` (~85 chars, on real disk so temp doesn't fill the
  tiny `--containall` `/tmp` tmpfs), and Python's ~32-char suffix pushes the socket
  path over the ~108-char AF_UNIX `sun_path` limit — crashing `lineage_wf` on every
  run. `checkm.sh` now points `TMPDIR` at a short `/tmp` symlink into the same
  workspace temp (socket path short, temp files still on disk). First reached only
  after `binning` + `bin_refine` were fixed; reproduced on a real ticket and
  cleared by the symlink.
- **`long-read-assembly` `bin_refine` no longer crashes DAS_Tool on a bad flag —
  `--write_bins`, not `--write_bins 1` (#379).** `--write_bins` is a boolean flag
  in DAS_Tool 1.1.x; the spurious `1` is an unexpected positional that r-docopt
  0.7.2 surfaces as `'short' is not a valid field or method name for reference
  class "Argument"`, Execution-halting before DAS_Tool runs. qp-pacbio passes it
  bare; probed on das_tool 1.1.7 / r-docopt 0.7.2 (bare parses, `1` crashes).
  First reached only after `binning` was fixed (every prior run died earlier).
  Also pins `das_tool=1.1.7` in `bin_refine.def` — the create line was unpinned
  despite the "1.1.x summary-columns" invariant, and the image rebuilds on any
  `bin_refine.sh` change.
- **`long-read-assembly` `binning` image can finally run concoct — `binning.def`
  installs `libgfortran=3.0.0` (#379).** concoct's `vbgmm` C-extension links
  `libgfortran.so.3`, but the metawrap solve ships only `libgfortran.so.5`, so
  `import vbgmm` died at runtime (`ImportError: libgfortran.so.3`) and metaWRAP's
  concoct binner failed — taking the whole step down, since metaWRAP exits
  non-zero when any binner hard-fails (metabat2 + maxbin2 succeeded regardless).
  Latent until now: every prior run died at an earlier wall (missing binners →
  unsorted BAM → metabat2 contig order), so concoct was never reached. The old
  conda-forge `libgfortran` (3.0.0) provides `.so.3` and coexists with
  `libgfortran5`, restoring concoct without perturbing the pinned solve; verified
  by running metaWRAP binning to completion on a real assembly. `binning-verify.sh`
  now asserts `import vbgmm` at build time — the prior tool-runnability check
  missed this because the failure is a Python `ImportError` (exit 1), not a loader
  verdict (126/127), so a concoct-broken image shipped green.
- **`long-read-assembly` `binning` no longer aborts on a contig-ORDER mismatch —
  `binning.sh` reorders the assembly to the BAM's `@SQ` order (#379).** With the
  unsorted-BAM failure fixed (#370), the same production ticket reached `metabat2`
  and died with `the order of contigs in abundance file is not the same as the
  assembly file: s10.ctg000011l`. Root cause is the *same* miint gap
  (duckdb-miint#173) at a second consumer: `jgi_summarize_bam_contig_depths` writes
  the depth matrix in the BAM's `@SQ` order (lexicographic, as miint emits it),
  while the assembly FASTA is in hifiasm's numeric order — and `metabat2` requires
  the two to agree. `samtools sort` fixes record order but never `@SQ` order, so it
  surfaced only after #370 landed. `binning.sh` now reorders `noLCG.fa` into the
  staged BAM's `@SQ` order (via `samtools faidx`) before handing it to metaWRAP, and
  fails loud if the `@SQ` and assembly contig sets ever diverge. Confirmed by probe
  on the shipped `samtools 1.10` / `metabat2 2.15`: a numeric-order assembly
  reproduces the abort, the `@SQ`-reordered one binds. Pinned by
  `test_binning_coverage_sort_pin.py`; removable together with the `samtools sort`
  when duckdb-miint#173 lands (tracked in Qiita#374).
- **`long-read-assembly` `binning` no longer dies on an unsorted coverage BAM —
  `binning.sh` runs the `samtools sort` metaWRAP skipped (#370).** A production
  ticket failed in `jgi_summarize_bam_contig_depths` 2.15 with
  `ERROR: the bam file 'reads.bam' is not sorted!`. Two causes, both fixed here:
  metaWRAP's own `samtools sort` sits inside the *same*
  `if [[ ! -f work_files/<sample>.bam ]]` guard as its `bwa mem`, so pre-placing
  our minimap2 BAM to skip the aligner silently skipped the sort as well; and the
  BAM was not sorted to begin with, because `assembly_coverage` relied on a miint
  contract that turned out to be false. Measured on that ticket's BAM: 11,390 of
  925,483 records step backwards in tid across 20,975 contigs; after the sort,
  zero. `binning.sh` now sorts into a `.partial` staging name inside `work_files/`
  and renames it into place (so a killed sort cannot leave a truncated BAM at the
  name metaWRAP reads), replacing the old `ln`-else-`cp` staging.
- **The `binning` image pins `samtools=1.10` and `metabat2=2.15`, and asserts both
  at build time (#370).** Neither was named in `binning.def` — both arrived through
  `metawrap-mg`'s solve, so a rebuild could move either with no change to this
  repo. They are the two tools the sort fix depends on: samtools provides the
  `samtools sort`, and metabat2 owns `jgi_summarize_bam_contig_depths`, the tool
  whose `is not sorted!` rejection the sort exists to satisfy — pinning the
  producer of the sort order while leaving the consumer that adjudicates it free
  to move would have been half a fix. A solver pin is invisible in the built
  image, so `binning-verify.sh` now reads each tool's own reported version and
  fails the build on drift; it runs as both the def's `%test` and the spec's
  `VERIFY_CMD`. The sort itself is pinned by `test_binning_coverage_sort_pin.py`,
  which needs no binary: the behavioural test needs the samtools binary (so it is
  skipped wherever samtools is absent, CI included) and invokes samtools directly,
  pinning samtools' behaviour rather than ours. The new test asserts the parts
  that are ours — the entrypoint still sorts `${COVERAGE_BAM}`, the only thing
  written to the path metaWRAP reads is the sorted BAM, staging stays atomic
  inside `work_files/`, and the sort budget still derives from `MEM_MB`.
- **Container steps are told their own allocation: `QIITA_CPUS` / `QIITA_MEM_MB`
  (#370).** `apptainer exec --containall` scrubs the environment, so no `SLURM_*`
  var reaches a container entrypoint — measured on the deploy host: zero survive.
  `SLURM_CPUS_PER_TASK` was therefore always unset in `workflows/_shared/_lib.sh`
  and in `workflows/bcl-convert/entrypoint.sh`, and `THREADS` came from the `nproc`
  fallback; it happened to equal the allocation only because SLURM cpuset-binds the
  step (`nproc` reports the cpuset — 16 under a 16-cpu allocation on a 64-core
  node). Memory had no equivalent: nothing inside the container exposes the cgroup
  ceiling, though the ceiling is real (`memory.max` = the step's `--mem`), so a
  per-thread tool budget could overshoot it. `slurm/payload.py` now forwards both
  values from the step's resolved `baseline_resources`, and `binning.sh` sizes
  `samtools sort -m` (which is PER THREAD) so the total is a third of `MEM_MB`
  regardless of thread count. Measured in the binning image on the 2.0 GB
  production BAM at 16 cpu: peak RSS 11.1 GiB / 19 s wall, unchanged between a
  12.75 GiB and a 34 GiB budget.
- **Corrected the miint `FORMAT BAM` `@SQ`-order claim in `docs/duckdb-miint.md`,
  `assembly_coverage`'s docstring, and `test_assembly_coverage.py` (#370).** Those
  three asserted that `@SQ` is emitted in the *reverse* of the `REFERENCE_LENGTHS`
  table's physical order, so that building the table `ORDER BY … DESC` lands `@SQ`
  ascending and makes `ORDER BY reference, position` a genuine coordinate sort.
  Probed 2026-07-24 (miint `v1.5.4`, reproduced standalone on the deploy host):
  the emitted `@SQ` order matches neither the table's row order, nor its reverse,
  nor `ORDER BY reference` — at n = 5, 10, 64, 300, 2000, whether the table is
  built ASC, DESC or shuffled. It is deterministic run to run and preserves the
  *set* of names, but the rule is unknown (miint's source was not read). The
  reversal holds only at tiny n and only for some naming schemes, which is why the
  old `test_reflen_order_is_reversed_in_sq` passed on its three-contig fixture;
  it is replaced by `test_sq_order_is_not_derivable_from_reflen`, which pins the
  probe finding and fails loudly if a miint bump gives `@SQ` a defined order.
  Filed upstream as duckdb-miint#173 (a defined or steerable `@SQ` order) and
  duckdb-miint#174 (no SQL access to a SAM/BAM header, which is why the contract
  test hand-parses the BAM binary layout to see `@SQ` at all); the removal of the
  `samtools sort` the first one forces is tracked at #374.
- **A miint workaround now has to carry an issue, and `docs/duckdb-miint.md` has
  an *Open upstream gaps* table to carry it in (#370).** New rule in `CLAUDE.md`'s
  miint section: when miint's behaviour doesn't match what we expected, the
  upstream issue is filed **in the PR that lands the workaround**, named by
  qualified number (`duckdb-miint#173`) at the code comment, the
  `docs/duckdb-miint.md` entry and the changelog line — and if the workaround is
  code we mean to delete once upstream fixes it, a Qiita issue with exit criteria
  plus a row in the new table. The table is the standing list of what qiita
  carries for miint's sake; a row is deleted by the PR that deletes its
  workaround. Motivated by this PR: the `@SQ`-order defect was found, documented
  in three places and worked around, and nothing filed it — so the workaround
  would have become permanent by default and the next reader could not have told
  whether it still applied.
- **`assembly_coverage` drops both now-inert `ORDER BY`s and its
  `preserve_insertion_order` override (#370).** With the `@SQ` reversal disproven,
  the reflen table's `ORDER BY read_id DESC` steered nothing, and the COPY's
  `ORDER BY reference ASC, position ASC` was a name sort — not the tid sort a BAM
  is sorted by — over a read-set-sized relation that the module's own memory split
  says can spill to `temp_directory`. Its only consumer, `binning.sh`, now
  `samtools sort`s regardless. The `SET preserve_insertion_order=true` existed
  solely to protect that ORDER BY, so it goes with it and the shared helper's
  `false` stands. `test_records_are_in_reference_name_order` is replaced by
  `test_writer_output_is_not_tid_sorted`, which pins the property the production
  failure was actually about.
- **`mask_sample` gate hardening — cascade delete, align-block false-positive, export partial-output, and a third ungated ticket door (#371).**
  Four defects surfaced once the `mask_sample` (and `alignment_sample`) completion
  gate became live on both masking paths:
  - `delete-sequenced-pool` 500'd once a gate row existed — `qiita.mask_sample` and
    `qiita.alignment_sample` both reference `prep_sample` `ON DELETE RESTRICT`, and
    the cascade did not clear them before deleting `prep_sample`. It now deletes both
    gate tables' rows first. Same fix for the bulk-block cover-map
    (`qiita.block_member`, also `ON DELETE RESTRICT` on `prep_sample`): the cascade
    now tears down the pool's block-scoped work_tickets and blocks (→ `block_member`
    CASCADEs) too, so a pool that ran block masking/alignment can be deleted. (The
    `qiita.genome.prep_sample_idx` origin-sample FK is a narrower remaining gap,
    tracked separately.)
  - `has_incomplete_covering_block` (the per-sample read-mask finalize gate) matched
    ALIGN blocks too — align blocks carry both `mask_idx` and `alignment_idx`, so a
    pending/failed align block would wedge a read-mask re-run for that `(sample, mask)`
    forever. Added the `alignment_idx IS NULL` read-mask-block discriminator.
  - The masked-read-export CLI validated accessions up front but minted the export
    ticket inside the per-sample download loop, so a now-tightened 409 aborted the run
    AFTER earlier samples' files were written — a partial output set. It now pre-filters
    on the manifest's `mask_state` and fails the whole export up front.
  - `POST /read-masked/ticket/doget` (service-account only) signed a `read_masked`
    ticket with no completion check — the third door that mints such a ticket. It now
    409s a non-`completed` `(prep_sample, mask_idx)`, uniform with the human export
    ticket route (every path that mints a `read_masked` ticket requires `completed`).
  Also closed the cross-path double-mask race: the per-sample `finalize-mask-sample`
  and the block planner now take a `(mask_idx, prep_sample)` `pg_advisory_xact_lock`
  held across their check→write, so exactly one wins and the other refuses.

- **A feature shared across genomes (a plasmid) no longer causes a lossful load (#366).**
  `feature_idx` is content-hash-global, so two organisms carrying an identical
  mobile element (e.g. a shared plasmid) resolve to the *same* `feature_idx` under
  different `genome_idx`. The `qiita.feature_genome` table had a standalone
  `UNIQUE(feature_idx)`, so the second genome's association collided on load and
  was silently dropped (`ON CONFLICT DO NOTHING`) — the second genome lost the
  shared feature. Dropped the standalone UNIQUE (new migration
  `feature_genome_allow_multi_genome`); the composite PK `(feature_idx, genome_idx)`
  already models the many-to-many correctly and keeps feature→genome lookups
  indexed. The shard planner (`_compute_shards`) now dedups a feature shared across
  genomes tiled into different shards to its lowest `shard_id`, so shard lists stay
  disjoint and `write_shard_assignment` never stamps a membership row twice. The
  load path needed no code change (composite PK + `ON CONFLICT DO NOTHING` is
  already many-to-many-correct). **Operator note:** references loaded before this
  fix stay lossful — RE-LOAD affected references to recover the dropped shared-
  feature associations (no backfill).
- **`GET /reference/{reference_idx}/exclusion` reports deterministic, correct
  genome provenance for a shared feature (#366).** With `feature_genome` now
  many-to-many, a shared feature fans to one candidate row per genome, and the
  listing's `DISTINCT ON (feature_idx)` picker had no tiebreak beyond direct-vs-
  via — so which genome's `(genome_idx, source, source_id)` was reported flipped
  on heap order, and a feature blocked directly could report an *unblocked*
  genome as its provenance. The picker now prefers a genome that is itself
  actively blocked, then the lowest `genome_idx`, so the reported provenance is
  stable and always names a blocked genome when a genome-level block applies.
- **Shard placement no longer regresses when a genome's lowest contig is excluded (#366).**
  The reference shard planner reduces each genome to one representative lineage via
  `arg_min(lineage, feature_idx)` over its members. Blocking the lowest-`feature_idx`
  member of a multi-contig genome made its exclusion-filtered taxonomy row disappear,
  so the representative reduced to `''` and the **whole** genome — healthy siblings
  included — tiled to the unclassified shard on the next re-plan. `_genome_lineages`
  now filters to the lowest *classified* member (`FILTER (WHERE concat_ws(...) <> '')`),
  so a genome keeps its real lineage as long as ≥1 member survives classified;
  only a fully-excluded genome sorts unclassified (moot — its features never surface
  post-exclusion). Given the biology invariant (a genome is one organism → its
  contigs share one lineage) this is the correct representative in the normal case,
  not merely a mitigation.
- **The `long-read-assembly` binning image shipped with zero binners.** `binning.def`
  installed bioconda's `metawrap-mg`, which is metaWRAP's *scripts only* — it declares
  none of the tools those scripts invoke. All **nine** were absent (bwa, samtools,
  metabat2, jgi_summarize_bam_contig_depths, run_MaxBin.pl, concoct, cut_up_fasta.py,
  concoct_coverage_table.py, merge_cutup_clustering.py); `bwa: command not found` was
  simply the first one a job reached, after the step had allocated 16 CPU / 100 GB. Now
  named explicitly, with `maxbin2>=2.2.6` pinned because 2.2.1's executable is `MaxBin`,
  not the `run_MaxBin.pl` metaWRAP calls. The build check missed it because
  `micromamba env list | grep metawrap` passes on an empty env: replaced by
  `binning-verify.sh`, baked into the image and driving both the def's `%test` and the
  spec's `VERIFY_CMD`, which asserts each tool and prints its sentinel only when all are
  present. Neither the full `metawrap` package (unsolvable) nor `metawrap-binning`
  (declares bowtie2, which the module never invokes; omits bwa and metabat2) is a
  substitute. (#365)
- **Empty control wells end `no_data`, not `samples_failed`, on the live read-mask
  path (#177).** On the live `bcl-convert → ingest_reads → read-mask` pipeline an
  empty well produces zero stored reads; the per-sample read-mask ticket then failed
  at input binding (`_resolve_staged_reads` → `BAD_INPUT` → FAILED), so *every* empty
  well — a legitimate blank / no-template control included — landed in the pool's
  `samples_failed`, burying real failures among blanks doing their job (the #164
  defect re-manifested after the store-once/mask-many split orphaned the old
  `fastq_to_parquet` `no_data` path). The zero-read branch now splits on the persisted
  biosample control marker (`host_taxon_id == "missing: control sample"`, reused via
  the host-filter resolver's `is_control_sample` so "what is a control" stays defined
  once): an expected-empty **control** raises `StepNoData` → terminal `no_data`
  (counted under `samples_no_data`); an unexpected-empty **data** well keeps the
  `BAD_INPUT` → failure it gets today. No schema or rollup change — the completion
  rollup already buckets `no_data` separately from `failed`.
- **`long-read-assembly` could never stream a sample's masked reads (#352).**
  Every ticket failed at submission with DuckDB's `IO Error: Can't find the home
  directory at '/dev/null'`. The CP runner's masked-read streamer
  (`_stream_masked_reads_to_fastq`) called `connect_with_miint()` — the helper
  documented for the **client** CLI, which runs `INSTALL miint` then `LOAD`.
  `INSTALL` resolves DuckDB's extension directory, defaulting to
  `$HOME/.duckdb/extensions`, and the `qiita-api` service account's home is
  `/dev/null`. This was the control plane's first *service-side* miint consumer;
  the helper's other callers are CLIs that have so far only run from hosts with a
  real `$HOME`, so it had never surfaced. (That is a property of where they run,
  not of which CLI they are — `qiita-admin` subcommands *are* run as `qiita-api`
  on the deploy host, so `qiita-admin masked-read-export` would hit the same wall
  if it were ever invoked that way.) Service-side miint is now LOAD-only from the
  deploy-staged directory via a new `connect_with_miint_staged()`, mirroring the
  cluster paths (which are LOAD-only precisely so no node "depends on mirror
  reachability, or needs a writable `$HOME`"). Requires `MIINT_EXTENSION_DIRECTORY`
  in the control plane's env, byte-identical to the CO's and DP's; unset or
  non-directory now fails with a message naming the variable and the service
  instead of a DuckDB IOException. A read-only staged directory is sufficient —
  `LOAD` writes nothing. `make verify-deploy` gains a `cp-miint` check, since a
  missing var takes nothing down at boot and would otherwise stay invisible until
  the next assembly submission.
- **The staged-directory requirement is single-sourced (#352)** as
  `qiita_common.duckdb_miint.require_staged_extension_directory`, and
  `MIINT_EXTENSION_DIRECTORY` is now named once (`MIINT_EXTENSION_DIRECTORY_VAR`)
  instead of spelled as a literal across the connect config, the job-env
  allowlist, and the orchestrator's staging gate. The **orchestrator
  deliberately does not adopt the check**: a slurm CO already requires the var at
  boot (`_resolve_slurm_settings`), its native jobs get a writable per-ticket
  `HOME` (`slurm/payload.py` points HOME at the workspace so DuckDB can cache
  extensions there), and a `COMPUTE_BACKEND=local` dev run legitimately has
  neither — so requiring it there would guard an unreachable state on the deploy
  while breaking local development. The helper is pure Python; qiita-common
  imports no duckdb, so each component keeps its own connect.
- **`make preflight` now checks `MIINT_EXTENSION_DIRECTORY` byte-identity across
  the CP/DP/CO env files (#352)**, the way it already did for `PATH_SCRATCH` —
  both name a shared path every component must resolve identically, so a per-file
  typo was a silent divergence. The comparison is now a helper called twice
  rather than a copied loop.
- **Native SLURM jobs can now reach the miint GPL-boundary host (#331).** The
  boundary (bowtie2/vsearch/MAFFT/SortMeRNA run out-of-process behind it) installs
  under `$HOME/.cache/miint/bin`, but native jobs run with an ephemeral per-ticket
  `HOME`, and the slurmrestd job environment is an allowlist that only forwarded
  `MIINT_EXTENSION_DIRECTORY` — so every `build_bowtie2_index` step died
  `gpl-boundary not installed` (the WOL3 reference-16 sharded build). `miint_job_env()`
  now also forwards `MIINT_GPL_BOUNDARY_PATH`. miint is a core dependency, so this is
  enforced fail-loud, not fail-soft: `miint_job_env()` **raises** if either miint var
  is unset (was a silent empty dict), `_resolve_slurm_settings()` keeps the CO **down**
  at boot without them, and a new compute-readiness `miint-gpl-boundary` probe builds a
  tiny bowtie2 index to fail the *deploy* if the boundary is unreachable. New required
  env var `MIINT_GPL_BOUNDARY_PATH` (CO, `COMPUTE_BACKEND=slurm`).
- **Data plane: DoGet now streams instead of materializing the whole result (#328).**
  `stream_ducklake_batches` executed queries with DuckDB's MATERIALIZED
  `query_arrow`, which computes the ENTIRE result set into memory before the
  first RecordBatch is drainable — so the bounded batch channel it fed could
  never cap peak memory. Harmless for feature-scoped shard rosters (small), but a
  **whole-reference** DoGet (the rype router's stream over every genome's
  `chunk_data`) OOM-killed the data plane at ~374 GB, aborting the WOL3 router
  build mid-stream (`RST_STREAM`). Switched to STREAMING execution
  (`stream_arrow` → `duckdb_execute_prepared_streaming`), which fetches one chunk
  at a time; peak memory is now the streaming query's working set plus the
  channel depth. The same one shared path backs the `alignment` (OGU
  feature-table) and `read_masked` whole-scope DoGets, so all three are fixed at
  once. A zero-row schema probe supplies the schema `stream_arrow` needs up front.
- **read-mask `lima` SIF was missing `python3`, failing every step after lima
  succeeded (#320, follow-up to #313).** `_lib.sh`'s `qiita_finish` — the last line of every
  container step — runs `python3 manifest_writer.py`, but `lima.def` (a
  micromamba base) installed only `jq`/`gawk`/… and no `python`, so the step died
  `exit 127 python3: command not found`. It was latent until now: the old FASTQ
  hang meant lima never reached `qiita_finish`, so the fix that made lima complete
  is what exposed it. Added `python` to the base install and a `%test` guard that
  fails the build if `python3` or the staged `manifest_writer.py` is not resolvable.
  lima itself needs no python; nothing else in the read-mask chain is a container.
- **PacBio read-mask: lima now gets a CCS BAM, not a multi-GB FASTQ (#313).** lima
  decides CCS-vs-CLR from the input FORMAT, not from `--hifi-preset`: handed the
  ~33.5 GB FASTQ `lima_export` used to write, it warned "non CCS data … will
  proceed to demultiplex each sequence individually" and never finished. Probed at
  lima 2.13.0, that CLR path **does not finish** — it is not merely slow: the FASTQ
  run produced zero bytes until killed at a timeout while the byte-identical reads
  as a CCS BAM completed in ~2 s, so there was nothing to parallelize.
  `lima_export` now rebuilds a minimal CCS unaligned BAM from the lake reads with
  miint's `COPY … TO (FORMAT UBAM)` (duckdb-miint#156, shipped in #157) — an `@RG`
  carrying `DS:READTYPE=CCS`, the field lima keys on — and feeds it to lima, which
  completes in seconds. lima's output stays FASTQ, so `lima_mask` still reads it with
  miint's `read_fastx`. No FASTQ is written at all now, so the landed intermediate
  shrinks to the BAM. Verified end-to-end: real `lima_export` → real lima 2.13.0
  (2 s, no CLR warning) → real `lima_mask`, every read correct at `sequence_idx >
  2^31`. **The key is the lake's `read_id`, not `sequence_idx`.** `bam_to_parquet`
  keeps the instrument's PacBio `<movie>/<zmw>/ccs` name verbatim, so `lima_export`
  writes it back as the record name with `zm` = the hole number parsed out of it;
  lima reconstructs that name byte-identically (probed on real production names), and
  `lima_mask` joins its output straight back on `read_id` — no map file, no synthetic
  name. `sequence_idx` cannot serve: lima rewrites the name from the **int32** `zm`
  tag, so a lake-wide idx past 2^31 would come back TRUNCATED (5000000000 →
  705032704) and mask the wrong read. `lima_export` rejects at export — where the
  cause is legible — a `read_id` whose hole number exceeds int32, a read set spanning
  more than one movie (multi-movie / block-scoped read-mask is not yet supported:
  miint's `FORMAT UBAM` has no per-read `@RG`), or a non-PacBio `read_id` (whose
  strict `[A-Za-z0-9_]+/[0-9]+/ccs` shape also keeps the movie safe to interpolate
  into the `@RG`). Also corrected: `lima_mask`'s claim that an empty lima output is a
  legitimate all-`twist_no_adaptor` mask — probed, an adapter-free BAM makes lima
  exit 1 (`Could not find matching barcodes!`), so that branch is unreachable and is
  now documented as the guard it is. Pinned by `test_lima_chain_smoke.py`.
- **read-mask `lima` container step was missing its `entrypoint` (#311).** The step
  declared `container: lima-2.13.0.sif` but no `entrypoint:`, so the SLURM job ran
  `apptainer exec <sif>` with no command and died with "exec requires at least 2
  arg(s), only received 1" — `apptainer exec` does not fall back to the image's
  runscript. Added `entrypoint: /opt/qiita/lima.sh` (the launcher lima.def already
  bakes in), and closed the gap that let it ship: a `container:` step with no
  `entrypoint:` is now rejected at `actions sync` (the `WorkflowStep` validator) and
  again when the SLURM payload is assembled (`_build_script`), instead of failing
  opaquely in a job.
- **Data-plane UNAVAILABLE on a read-mask fetch is now retriable, not permanent (#311).**
  A DP read-materialization that failed with a transient gRPC UNAVAILABLE
  (`FlightUnavailableError` — the DP briefly unreachable during a fan-out or a deploy
  restart) was filed as permanent `BAD_INPUT`, failing the ticket for good. It is now
  classified `DATA_PLANE_TRANSIENT` (retriable, a redrive self-heals) alongside the
  existing SQLSTATE 40001 case.
- **DuckLake concurrent-attach serialization crash on read-mask fan-out (#310).**
  `connect_ducklake` ran two `set_option()` calls (parquet compression/version) on
  every per-request attach, each persisting to the catalog-global `ducklake_metadata`
  row. A burst of concurrent Flight requests (a 26-way read-mask submit, each doing a
  DP fetch) all UPDATE'd that one row and failed with Postgres SQLSTATE 40001
  (`could not serialize access due to concurrent update`). The options are now set
  **once at boot** (`set_catalog_options` in `main.rs`); per-request attaches only
  `LOAD` + `ATTACH`.
- **Data-plane serialization failures are now retriable, not permanent (#310).** A DP
  Flight fetch (adapter sequences, read materialization) that hit the DuckLake 40001
  conflict was wrapped as a permanent `BAD_INPUT`, failing the read-mask ticket for
  good. New `FailureKind.DATA_PLANE_TRANSIENT` (retriable) + `_submission_dp_fetch_failure`
  classify a serialization conflict as retriable (a redrive self-heals) while keeping
  every other DP-fetch failure permanent.
- **PacBio read-mask no longer trims Illumina adapters (#310).** QC always fetched the
  deploy's default (Illumina TruSeq) adapter set and trimmed against it, including on
  PacBio HiFi (which carries no TruSeq adapters — SMRTbell is instrument-removed, Twist
  is handled by lima), which was also the fetch that hit the crash above. A new
  `qc_adapter_enabled` gate (default true) is set false for PacBio by
  `submit-host-filter-pool`; the runner then skips the adapter fetch, and the `qc` step
  (its `adapter_parquet` now optional) runs polyG + the length/quality filter with no
  adapter trim.
- **`--gff` was unusable on prokka and bakta output (#269).** Both annotators always
  append the genome to their GFF3 as a `##FASTA` section, and miint's `read_gff` does
  not stop there — it returns one row per line of the embedded FASTA, with the
  nucleotide line itself in `seqid` and NULL in every other column (a real prokka file
  gives 1,638 rows for 99 features). Those rows reached the parent check and killed the
  ingest with a misleading error. They are identified by a NULL `type` — which a GFF3
  feature line cannot have — and dropped. Pinned, with an anti-vacuity control, by
  `test_annotation_ingest_smoke.py`.
- **`--gff` rejected NCBI RefSeq (#269).** Two independent reasons, both fixed: the
  duplicate-`ID` hard failure (see the `annotation_idx` note above — repeated IDs are
  valid GFF3), and the `region` landmark line that every NCBI record opens with. A
  landmark declares the extent of the sequence rather than annotating an interval of
  it, so it necessarily spans its whole parent and would hash to the PARENT's
  `feature_idx`. Landmark types are now dropped by type; a row of any *other* type
  spanning its whole parent still raises, since that is genuinely ambiguous.
- **`align-plan` would have `TypeError`d on every submission once #268 and #270 were
  both on `main` (#269).** A semantic merge conflict, invisible to either PR's CI:
  #270 added two required keyword arguments to `_build_mask_params`
  (`resolved_lima` / `resolved_syndna`), and #268's `align_planner` calls it to
  reconstruct the mask params a block-masked sample was minted under. Each branch is
  green alone; the merge is not. `align_planner` now passes both as `None`, matching
  what `block_planner` actually mints (the block workflow is `qc → host_filter` only
  — no lima chain, no syndna step). Had it passed anything else the lookup would have
  silently missed, every sample would have looked unmasked, and the align plan would
  have quietly produced nothing.

- **A local reference-add carrying a tree or jplace crashed (#269).** The local
  ingest path DoPuts nothing — "no bytes cross the wire" — so `tree_path` /
  `jplace_path` arrive as the raw `.nwk` / `.jplace` file, but `reference_load`
  unconditionally `read_parquet()`'d them to unwrap a chunked-BLOB upload
  envelope, which raises on a raw file. Both now go through the shared
  `resolve_blob_input`, which sniffs which shape it was handed. Found while
  wiring the GFF3 companion through the same seam.

- **`qiita-admin backfill host-taxon-id` — populate the host organism on samples that
  predate the field (#299).** `host_taxon_id` was added as a biosample global field after
  every sample we hold was ingested, so **none** carry it — which means the host-filter
  resolver correctly reports every sample UNRESOLVED, and the submit-path swap would abort
  every pool. This backfills it. **Dry-run by default**; `--execute` writes. Idempotent, so
  it can be re-run as curation lands.

- **The host is decided by two facts, in order, and anything else is reported rather than
  guessed (#299).** First: is it a CONTROL? A blank has no host of its own whatever taxon
  it carries — and it must be checked first, because blanks *do* carry a taxon and **every
  pool contains blanks**, so checking taxon first would abort every pool. The signal is the
  pre-flight's own `is_control` (`input_sample.project_idx IS NULL`), never the `BLANK.*`
  naming convention. Second: the sample's own `taxon_id` is mapped to a host through a
  small **curated** table — `human gut metagenome` → human; `seawater metagenome` → `not
  applicable` (no host). The mapping cannot be computed: `terminology_term` carries no
  lineage, and NCBI's metagenome taxa do not sit under their host. A sample neither rule
  settles is left unwritten and stays UNRESOLVED, which aborts at submit rather than
  passing an un-depleted sample through. The residue is the curation worklist, and the
  command prints it.

### Changed

- **The unbinned residue is no longer admitted to the de novo genome map, so a feature table
  reports assembled genomes rather than single fragments.** `ASSEMBLY_GENOME_MAP_PAIRS_SQL` — the row
  set shared verbatim by the REST contig→genome map and the cohort Parquet
  `estimate-feature-table` reads — is now an allowlist, `kind IN ('MAG', 'LCG')`. An unbinned
  contig is what no refined bin claimed, and for that kind `bin_id` is the contig id, so the
  genome mint gives each one a genome of a single fragment: on the deploy host that is 820,094 of
  the 1,002,979 membership rows, five single-fragment genomes for every assembled one. Those
  contigs are NOT removed from the alignment — the assembly DoGet scopes on `(prep_sample_idx,
  processing_idx)` with no `kind` predicate, so they are still streamed and still aligned against;
  what changes is that their alignments no longer roll up to a reported genome. They stay
  queryable in `qiita.assembly_membership`. An allowlist rather than `<> 'UNBINNED'` because
  `assembly_constants` states the kind set is meant to extend without a migration, so a denylist
  would admit a future kind into every de novo feature table by default.
  The mint is unchanged: `write_assembly_membership` still mints a `qiita.genome`
  per `(prep_sample_idx, processing_idx, kind, bin_id)`, UNBINNED included, so those genome rows
  exist and are simply not admitted here — store broadly, filter at the read. They are inert off
  the map because the assembly path records its edge on `assembly_membership.genome_idx` and
  never on `qiita.feature_genome`, so an assembly genome cannot reach the reference graph or the
  global `reference_exclusion` blocklist that expands through that junction.
  `count_assembly_membership_without_genome`, the completeness guard a caller refuses on, takes
  the same filter so an unminted UNBINNED row cannot refuse a cohort over a gap the map never
  reads. No stored result changes: the de novo arm has never run on the deploy host, whose
  `assembly_membership` has no `genome_idx` column yet (last applied migration
  `20260827010000`) (#519).

- **The binners are measured to preserve both assemblers' contig id shapes, so the attribute
  join's caveat is dropped (#519).** `qiita.assembly_membership`'s four attribute columns reach a
  MAG row through a LEFT JOIN on `contig_id`, whose left side comes from the refined bin FASTA —
  sound only if the binners write the assembler's header through unchanged. That was measured for
  hifiasm_meta and unmeasured for myloasm, whose ids have a different shape (`u<N>ctg`, no dot).
  Measured now for both: a run carrying myloasm-shaped ids beside hifiasm-shaped ones as an in-run
  control, through the deployed SIFs, returned every id verbatim from metabat2, maxbin2, concoct
  and DAS_Tool, with no record renamed at any stage and every refined id present in the input.
  The caveat comes off `assembly_hash`'s docstring and off the `raw_name` and `circularity` column
  comments (a new migration — the one that shipped them is merged). One assembly and one binner
  configuration, so this establishes that these tools preserve both shapes, not that a future
  version must.

- **`hifiasm_meta` is pinned, and an unrecognised GFA segment name now fails the `assemble`
  step (#517).** The pin is `hamtv0.3.5`, with both of the binary's internal version strings
  asserted at build time, matching how myloasm is pinned — unpinned, every rebuild re-resolved
  the solve against whatever bioconda had that day, and editing anything in the image's
  `HASH_INPUTS` forces a rebuild, so the DEFAULT assembler could move on a change that had
  nothing to do with it. The fail-loud follows from the same PR storing the circularity call: a
  name matching neither the circular nor the linear shape used to cost a misroute to binning,
  and would now also write a call into the lake for a contig nothing classified. A missing GFA
  is still an empty assembly, not a violation.

- **Reference chunk bytes stay on the submitted strand, and a `--gff` load now says so (#502).**
  `normalized_sequence_expr` normalizes case and deliberately does not normalize strand; its
  docstring stated the second half as a fact with no reason. It now carries why, and
  `canonical_sequence_hash_expr` points at it.

  A load carrying a GFF warns (`ANNOTATION_STRAND_WARNING`) — at the CLI before any network
  call, and again in `hash_sequences`, which is the chokepoint every GFF-bearing workflow
  routes through. An annotation's coordinates are an interval of the FASTA record it names, and
  that record is stored in the orientation it was submitted in: a sequence and its reverse
  complement are kept as one sequence, so the interval stops describing the bases it was taken
  from if one FASTA carries a record in both orientations (`hash_sequences` stores the
  lex-smallest `read_id`'s chunks but cuts the interval from the GFF seqid's record), or if a
  later reference load overwrites that record (`reference_sequence_chunks` is replaced on
  `feature_idx` alone). Both probed against the real code paths; neither is detected, at load or
  afterwards, and the warning does not fix either — they are tracked in #503. Measured against
  the live deploy: no reference currently carries annotations, and 27 of 781,637 features are
  claimed by more than one reference.

- **`align-denovo` collects secondary alignments (#486).** `max_secondary` moves from 0
  to `qiita_common.analytic.MAX_SECONDARY` (100), the cap `align_sharded` already uses,
  so a read placing against several near-identical contigs is recorded against each
  rather than only the best. The pooled circular gate judges a read's fragments as
  before; secondaries are judged per record against the same two thresholds. The cap is
  hashed into `alignment_idx` alongside the aligner, so changing it mints a new
  alignment rather than replacing rows collected under the old policy.

  Measured on the deploy-staged miint build under `map-hifi`: the cap is applied
  literally and the realised count is min(cap, equally-good subjects − 1), so it
  saturates on how many near-identical contigs an assembly holds rather than on 100. A
  subject scoring materially below the best is not reported whatever the cap; the knob
  that decides that is not exposed
  ([duckdb-miint#189](https://github.com/the-miint/duckdb-miint/issues/189)).

- **The chunk reassembly contract has one definition in tests too (#486).** Six inlined
  `string_agg(chunk_data, '' ORDER BY chunk_index)` copies — in `test_reference_load.py`,
  `test_assembly_hash.py`, `test_assembly_load.py` and
  `tests/integration/test_reference_stream.py` — now call `reassemble_chunks_expr`. The
  literal in `test_chunking.py` stays, since that assertion is what pins the expression.

- **`ALIGNMENT_IDX_BINDING` and a new `ALIGN_DENOVO_ACTION_ID` in `qiita_common.actions`
  (#486).** The binding moved there from `runner/_mask.py` so the action contract can
  assert on it: an action declaring `finalize-alignment-sample` must thread it through
  some step's `params:`, or the gate would record a sample aligned under an identity none
  of its rows carry. The action id joins `LONG_READ_ASSEMBLY_ACTION_ID` in the mask-purge
  guard's coverage set — both actions consume a mask rather than minting one, so a ticket
  of either carries NULL `mask_idx` until it runs.

- **`qiita.alignment_sample`'s table comment names both writers (#486).** Comment-only
  migration: the gate now has a per-sample writer alongside the block one, and the old
  comment described the block path as the only one.

- **`CLAUDE.md` trimmed from 11,020 to 5,776 tokens, and `docs/architecture.md` split into
  `docs/architecture/` (#482).** Every token in `CLAUDE.md` is re-sent in front of every API call
  an agent makes, and compaction never evicts it — measured over 46 sessions, the always-loaded
  prefix was 18.9% of all input tokens, rising to 32.5% once a compact cadence is adopted. The
  lookup-shaped sections move to `docs/container-images.md`, `docs/deployments.md`,
  `docs/testing.md`, and `docs/architecture/cross-cutting.md` (component map, identifier
  ownership, data-plane design, orchestrator pattern, workflow runner). `Enum parity` and
  `Operator-facing changes` are condensed to their rules. Every rule that has to fire without
  being looked up — the development ethos, miint-is-core, naming, REST path constants,
  never-edit-an-applied-migration, the changelog gate — stays resident, and every moved section
  leaves a pointer plus an index table under `## Architecture`. `docs/architecture.md`'s 1,600
  lines become eight files — overview, data model, reference data, Flight surface, processing,
  storage, build/CI, cross-cutting — leaving `architecture.md` a 92-line index that lists every
  file's sections. Heading levels are normalized per file, and every inbound reference is
  retargeted to the file carrying the section it cites, `test_makefile_doc_sync.py` and
  `test_architecture_tree_sync.py` included: both read fenced blocks that now live in
  `docs/architecture/build-and-deploy.md`. The split is for human navigation, not agent token
  cost — across 54 transcripts the file took 23 accesses from 8 sessions with zero whole-file
  reads and a median per-session coverage of 2.4% of its lines.

- **Data-plane inline test modules split into `#[path]` submodules (#481).** `flight_service.rs`
  was 9,398 lines of which 5,712 (61%) were `#[cfg(test)]`; `auth.rs` was 1,143 with 514 (45%).
  The tests move to `flight_service_tests.rs` / `auth_tests.rs` and stay child modules via
  `#[cfg(test)] #[path = "..."] mod tests;`, so they still reach private items through
  `use super::*` — no visibility changes. `cargo test` reports the same 121 tests passing
  before and after. `ducklake.rs` and `main.rs` are left alone: #244 has a hunk inside
  `ducklake.rs`'s test module and also touches `main.rs`, so those two wait for it to land.

- **Sequence chunks are stored upper case (#479).** `sequence_split_expr` now normalizes
  through a new `normalized_sequence_expr`, which `canonical_sequence_hash_expr` also
  routes through — so the bytes in `reference_sequence_chunks` / `assembled_sequence_chunks`
  and the hash that keyed them cannot disagree about case. The hash already folded case
  before folding strands, so a soft-masked record and its uppercase twin shared one
  `feature_idx`; the split preserving case meant two loads wrote different bytes under that
  one key, leaving the survivor a function of which reference loaded last. Every index
  builder that reads `chunk_data` discards case — measured 2026-08-24 against the
  team-mirror miint build on a reference differing only in a 20 kb soft-masked repeat,
  `rype_index_create`, `save_minimap2_index` (sr / map-hifi / map-ont / asm5) and
  `save_bowtie2_index` all produce byte-identical output for it, while a one-base edit to
  the same reference changes all of them. Strand is not normalized and still follows load
  order. Case is discarded at load, so `qiita reference export` no longer reproduces a
  submitted FASTA's soft-masking. `normalized_sequence_expr` carries the detail.

- **`qiita reference export` reassembles chunks through the shared expression (#479).** The
  genome-FASTA writer inlined its own `string_agg(chunk_data, '' ORDER BY chunk_index)`
  instead of calling `reassemble_chunks_expr`, a fourth copy of the chunk contract outside
  the module that single-sources it.

- **`qiita_common.feature_table` is now the `qiita_common.analytic` package (#475).** Closes
  #456. The one module became eight — `relations`, `stage`, `coverage`, `gate`, `ogu`, `label`,
  `sidecar`, `write` — re-exported from `analytic/__init__.py`, so a consumer's only change is
  the import line. No SQL text, no error message, and no assertion changed: every builder's
  output and every check's message is byte-identical, and the string-level tests moved verbatim
  into per-module files. `qiita-common` now declares `duckdb>=1.5.4`, which is what lets the
  analytic's behavioural tests — the only home of the per-sample coverage scope — live beside
  the code they pin rather than in the control-plane suite.

- **`CHANGELOG.md` rotated: the historical strata moved to `docs/changelog-archive/`.**
  The file had reached 5,745 lines under a single never-rotated `## [Unreleased]`,
  carrying eleven redundant bucket headings below the four that PRs write to. The
  305 entries under those redundant headings (PRs #29–#386) now live in
  `docs/changelog-archive/2026-08-24-eeb3bd0c.md`; the live file keeps 185 entries
  and one heading per bucket. Measured across 46 agent sessions, `CHANGELOG.md` was
  the most-accessed file in the repo — 387 tool calls, 127,150 tokens — while the
  union of lines any session ever read was 885 of 5,668 (16%).

- **`mint-features` pins its declared `inputs:` list (#464).** The runner's dispatch resolved
  the manifest as `entry.inputs[0]`, so a workflow naming any other binding minted features out
  of whatever path sat there. It now requires `inputs: [manifest]` and reads `bound["manifest"]`
  by name, failing the entry otherwise — the shape `mint-annotation-features`,
  `write-membership` and `write-assembly-membership` already use. The optional genome map is
  unchanged: it stays an `action_context` binding (`genome_map_path`), not a declared input.

- **`qiita.assembly_membership` documents its key prefix and its `bin_id` column (#464).**
  A comment-only migration. The table comment: `(prep_sample_idx, processing_idx, kind,
  bin_id)` is the subject identity, with `feature_idx` completing the row per member contig,
  and `kind` tells a refined bin from a circular or an unbinned contig (value set still
  enumerated only in `qiita_common.assembly_constants`, which the comment points at). How
  far that identity separates two subjects depends on what `bin_id` holds, so the table
  comment points at the `bin_id` column comment instead of restating it. A subject records
  grouping and nothing about completeness, for any kind; the `bin_quality` lake table
  measures that, per refined bin, from CheckM. `bin_id` gains its first column comment: a
  refined bin's FASTA filename stem — one file, one bin, which `_file_meta` enforces — or,
  for a circular or unbinned contig, that contig's own assembler-given id, its FASTA
  header's first token. Producer-chosen either way, and two headers in one file can share a
  first token, which is why the key scopes `bin_id` rather than treating it as globally
  unique or as one contig per row. None of that is recoverable from the bare `TEXT` column.

- **A feature-table build now reads its reference before it streams anything (#448).** The
  reference's name and version are only needed by the manifest, written last, so the read that
  fetches them ran last too — which meant a reference this alignment names but the caller cannot
  read was discovered *after* a whole cohort had crossed the wire. It is the cheapest thing in the
  build that can refuse, so it now runs first. The processing mint stays last, deliberately: it is
  a write, and a public handle minted for a build that then fails names a bundle nobody has.

- **A BIOM file written by a feature-table build now names `qiita-miint` as its generator
  (#448).** `qiita` named the project rather than the system that produced the file.

- **The paired-row count asks miint instead of masking the SAM flag (#448).** `flags & 1 <> 0`
  became `alignment_is_paired(flags)`, which is what `docs/duckdb-miint.md` tells callers to
  use. Same numbers either way, which is why it is pinned by a test — nothing else would
  notice a regression.

- **Every route keyed on an `alignment_idx` now returns one wording for "no such alignment"
  (#448).** The delete route answered `Alignment definition not found` while the three cohort
  routes answered `alignment not found`, and no test asserted either, so one condition had two
  bodies a client could come to depend on. Converged on the shared constant and pinned.

- **The gate every alignment-cohort route runs is now one function, not three copies (#448).**
  `POST /alignment/.../ticket/doget`, `POST /exported-identifier` and `POST
  /exported-processing` all check that the alignment exists, then that the caller may read
  the whole cohort, then that the cohort is completed — and that *order* is a disclosure
  decision: reversed, the completeness refusal would list samples of an alignment the caller
  has no right to read. Three routes hand-writing a security-relevant sequence is three
  chances for one to be reordered alone, so it is now written once, with the reasoning
  beside it, and the routes differ only in the clause that completes their 422.

- **Reference ingest's rank-prefix check and its column projection are now generated from
  the shared rank tuples (#448).** They were spelled out, eight clauses each, in the job — which
  was harmless until the published taxonomy sidecar started *restoring* those prefixes by
  position from the same tuples. A drift between the two would have silently relabelled
  ranks in a file people join, and nothing would have caught it; now a change to the tuple
  changes what ingest accepts.

- **The all-or-nothing multi-file commit is now shared by both CLIs' exports (#448).** The
  masked-read export's commit — write partials, then rename each into place, and on any
  failure remove every partial *and* every already-committed file — moved to the shared CLI
  helpers so the feature-table bundle uses the same one rather than a second copy. The
  privacy chmod stays with the masked-read export, the only caller whose bytes need it. The
  rollback of an already-committed file now has direct tests, which no caller's own suite
  reached, and the one gap it cannot close — a kill between two renames — is written down
  where the guarantee is stated.

- **`docs/architecture.md` updated: N=0 sharded path no longer exists (#431).**
  `plan-shards` returns zero shards on N=0 rather than raising; the
  plan-shards **runner arm** treats that zero-shard result as an error
  and fails the ticket, and a CLI guard (`--shard-index requires
  --genome-map`) additionally catches the case before any network call.
  Two bullets in the sharded-index fan-out section corrected to reflect
  the current behavior.

- **One `cap_rows` helper behind every capped list route (#427).** The
  fetch-`cap + 1` / slice-back / set-`truncated` split was written inline at each
  list route — the two sequenced-sample rosters, the prep-sample study roster,
  `build_idxs_list_response`, and the work-ticket list this PR adds. It is now
  `routes/_helpers.cap_rows`, which all five call. No wire change: the same rows
  and the same `truncated` value come back from each route. The two
  mask-definition reads call it too, in place of their own copy (#423).

- **BREAKING: `GET /work-ticket` returns an envelope, not a bare array (#427).**
  `{tickets, count, truncated}` — `WorkTicketListResponse`, the same shape
  `IdxsListResponse` and `SequencedSampleListResponse` already use; this route was
  the only list route without it. The page is capped at `limit` (default 50, max
  500), and until now a capped page was indistinguishable from a complete one. That
  became load-bearing with the pool filter above: one read-mask ticket per sample
  against a pool of a few hundred samples silently returned the newest 50 rows, so
  a per-sample read table assembled from it would be a prefix with nothing saying
  so. Truncation is now decided server-side (fetch `limit + 1`, slice back).

  A client that indexed the response as a list reads `["tickets"]` instead;
  `qiita ticket list` prints the envelope. No `caller_system_role` field, unlike
  the two sibling envelopes: this route admits service accounts, whose authz is
  scope-only and which carry no system_role.

- **Sequenced-sample import accepts study-local prep_sample fields (#386).** The
  `metadata` dict on `POST /sequencing-run/{idx}/sequenced-pool/{idx}/sequenced-sample`
  now resolves a name against the study's existing purely-local prep_sample fields as
  well as the global fields, writing the value through the local field row — matching
  what biosample import already does. A name matching neither is still a 422.

- **Mask identity keys on the adapter sequences, not on serialized Parquet bytes
  (#428, expand phase).** `resolved_qc.adapter_set_hash` was the SHA-256 of the
  materialized adapter Parquet. The pyarrow writer stamps its version into the
  file footer, so that digest changed on every pyarrow bump for the same adapter
  sequences — measured on one fixed two-row table: 19.0.0 `a1677d2f…`, 21.0.0
  `29a6a873…`, 23.0.1 `53117c74…`, identical across two runs of one version. A
  block plan re-deriving under a different pyarrow than the mint then mints a
  second mask for the same filter, and an align run naming the original mask_idx
  finds no `mask_sample` rows for those samples (`samples_skipped_no_mask`). It
  is now the SHA-256 over the reference's sorted `qiita.feature.sequence_hash`
  values, read from Postgres — a strand-canonical hash, so an adapter and its
  reverse complement are one member. `qiita.mint_mask_definition` takes the
  legacy digest as a fallback lookup key and re-keys the row it matches in place,
  keeping its mask_idx, so nothing re-masks. A new
  `mask_definition.adapter_hash_scheme` column records which derivation produced
  a row's stored hash; it sits outside `params` because `params` is the hashed
  blob. The scheme is stated by the caller that derived the hash, never inferred
  from the blob — the public `POST /mask-definition` route mints caller-supplied
  `params`, so its rows stay unstamped and surface in the backfill's report
  instead of reading as converted.

- **The read-mask identity (`mask_idx`) now carries rype's host-call threshold
  (`resolved_host_filter`).** The hash covered the host *references* a mask depletes
  against but none of the params it depletes *with*, so the threshold change below
  would have been invisible to it: reads depleted at two different cutoffs would share
  one `mask_idx` whose stored params describe neither, and — worse than a mislabel —
  the per-`(mask_idx, prep_sample)` gate would read every already-masked sample as
  done under the "same" mask and never re-mask it, so the new threshold would never
  reach existing data at all. Same defect class the `resolved_lima` / `resolved_syndna`
  widening closed, and the same fix: a nested block, `None` when no rype stage ran, so
  a future threshold move re-mints only masks that actually depleted.
  `test_host_filter_pins` pins the CP mirror to the job's constant by AST (the CP
  cannot import the orchestrator), including a name-shaped guard so a *new* depletion
  knob must be pinned deliberately. The minimap2 stage's `preset` is deliberately not
  hashed: it is pinned in the job to the preset its `.mmi` was built with, not chosen
  per mask, and as of this deploy only the illumina `host_filter_profile` runs that
  stage at all.
  **Consequence: `params_hash` changes for every existing mask.** Existing rows stay
  valid and referenced; a re-run of an identical config mints one new `mask_idx`
  rather than reusing the old.

- **The `host_filter` rype threshold is 0.05, up from 0.0.** rype emits a row per
  bucket scoring at or above the threshold and `host_filter` calls host on any
  emitted row, so this value *is* the host call. At 0.0 a single incidental
  minimizer match masked a read; 0.05 still sits below rype's own 0.1 default, so
  host depletion stays deliberately aggressive relative to upstream. Applies to
  every read set — the threshold has no per-platform or per-mask variant — and
  shifts reads scoring in [0.0, 0.05) from `host_rype` to their `qc_mask` reason
  (`pass` for a QC-pass read), which makes them visible through `read_masked`. The
  second host stage (minimap2 on rype's survivors) is unchanged.

- **`BaselineResources.as_flat()` is now the single narrowing of the flat
  population (#416).** The runner's dispatch path and the new headroom queries
  both resolve through it instead of each re-asserting the three Optional fields
  and rebuilding a `FlatBaselineResources` by hand, so what actually runs and what
  the guard checks cannot drift.
- **Landing-page footer now shows the deploy date (calver) instead of the static package version (#431).**
  `QIITA_BUILD_VERSION` is derived from the deployed commit's date in `local-deploy.sh` and
  injected via `build.env`; `landing.py` prefers it, falling back to the package version for
  dev boots.
- **`align_sharded` probe comment corrected: the count probe is kept for correctness, not
  because `LIMIT 1` is slower (#431).** Row-group stats make `LIMIT 1` faster in most shapes;
  the count probe is needed because the mixed-batch rejection requires `total`.

- **`align_sharded` hands the aligner a materialized query relation instead of the lazy
  Parquet view (#391).** Both sharded aligners read the query relation once per shard, so
  a block's sequences are re-read 1000 times at the current shard count. Against the
  Parquet-backed view each of those reads pays for the block's whole sequence *bytes* — a
  Parquet scan must decompress a whole column chunk to yield any row from it — while a
  shard wants ~0.1% of the reads; against a materialized table DuckDB scans the narrow
  `read_id` column and fetches sequences only for the rows the shard asked for. Measured
  per shard (1000 shards, scattered membership, `threads=8`, warm): 20.0 ms view /
  3.4 ms table at 1M × 160 bp, 169.0 ms / 27.2 ms at 10M × 160 bp, 330.5 ms / 40.0 ms
  for a 10M paired-end block. Over 1000 shards that is 169 s → 27 s for a single-end
  short-read block and 331 s → 40 s for the paired-end one; scaling the view along the
  byte axis puts a 1M-read HiFi block (~15 GB) near 26 min against seconds. Building the
  copy costs one Parquet scan — 22 ms at 1M reads, 241 ms at 10M.
  **The two costs scale on different axes:** the view tracks total bytes (so read count
  *and* read length), the table tracks reads-per-shard and is flat in read length. An
  earlier revision of this change materialized for minimap2 only, on the strength of a
  per-1000-bp/read slope measured at 1M reads and then applied to the 10M-read short-read
  block — understating bowtie2's re-read by 10×. The copy is now unconditional, created
  after routing has committed to an align (`rype_classify` holds its own resident copy of
  the corpus while classifying, so building ours first would hold two), and dropped once
  phase 1 is done with it. The win is contingent on the copy fitting in memory — ~15 GB
  at the 1M long-read block target (#389), under ~1 GB for a short-read block, against a
  ~57 GB resolved limit (#381) — and degrades rather than fails if it does not: DuckDB
  spills it to the Lustre workspace and the per-shard fetches read spill files 1000
  times, plausibly worse than the view. Raising a block's sequence bytes well above the
  current targets needs this reconsidered, not just the target. Filed upstream as
  duckdb-miint#184, with removal tracked at #392 and a row in
  `docs/duckdb-miint.md` → Open upstream gaps; the overdue duckdb-miint#175 row (sharded
  aligners pin subject ids to VARCHAR) is added there too. Also corrects three stale
  claims this reasoning rests on — `align_sharded` said `bind_step_reads` "materializes
  to a real table" (it binds a lazy `read_parquet` view), and `read_source` said a block
  is tiled to 10M reads "regardless of platform" against a DuckDB "capped at 8 GB"
  (long-read align blocks are 1M since #389, and align resolves its limit from the
  allocation since #381), plus its "node-local scratch" description of the drain file (it
  lands under `PATH_SCRATCH`, which is Lustre on the deploy).
- **`align_sharded` streams the aligner into its output instead of buffering it three
  times (#385).** The tail was `CREATE TABLE … AS SELECT * FROM align_*_sharded(…)`,
  then a pooled-identity `WINDOW`, then a sorted `COPY` — three full buffers of the
  alignment set, with the selective identity filter running *after* the first two
  (`EXPLAIN`: `SEQ_SCAN → HASH_JOIN → WINDOW → FILTER → ORDER_BY → BATCH_COPY_TO_FILE`).
  Both align seams now return `(sql, params)` rather than materializing a table, and
  `execute()` runs two phases: phase 1 streams align → (single-end) filter →
  `read_meta` join into a transient staging Parquet inside the DuckDB temp dir, and
  phase 2 applies the pooled paired-end filter plus the single 6-column identifier
  sort into `alignment.parquet`. Measured on a 20M-row SAM-shaped fixture at a 2 GB
  `memory_limit`, with a deliberately non-selective filter so this is pipeline shape
  alone: 12.0 s and ~4.9 GB spilled → 5.5 s and zero spill. The split is load-bearing
  rather than cosmetic — `memory_limit` is a ceiling, not a reservation, so sorting in
  the same statement as the aligner would hold the whole alignment set while the rype
  router, per-shard indexes and GPL-boundary daemons are still resident. The identity
  filter now branches on the BATCH SHAPE, not the aligner: the floors stay per-aligner
  (0.99 bowtie2 / 0.90 minimap2, query coverage minimap2-only) but the pooling is
  per-shape, and a single-end batch — whose pooled window was a partition of one row,
  provably equal to a per-row predicate — now filters with a plain `WHERE` in phase 1.
  A batch that MIXES single- and paired-end reads is rejected with the counts instead
  of surfacing as bowtie2's opaque `gpl_boundary` bind error or, on minimap2, as a
  silently mis-pooled filter — and that rejection now runs ahead of the routing pass,
  so it cannot be skipped by a batch whose reads route nowhere (which previously exited
  0 with an empty output) and does not pay for a `rype_classify` pass first. Also pins
  a miint contract the paired-end gate silently depended on: `cigar_sequence_identity`
  and `cigar_query_coverage` are permutation-invariant over a concatenated CIGAR, which
  is what lets the pooled `string_agg(cigar, '')` window omit an `ORDER BY`. Upstream
  documents neither as order-insensitive, so a mirror build changing that would have
  made the gate nondeterministic; it is now verified over all 120 permutations of a
  5-fragment CIGAR and recorded in `docs/duckdb-miint.md`.

- **Long-read align blocks are tiled at 1M reads, not 10M (#389).** The align planner
  tiled every platform at `block_planner._BLOCK_TARGET_READS` (10M), a target sized on
  read COUNT because short-read work is count-bound. The sharded aligner's cost is
  driven by BYTES: each of the reference's ~1000 shards re-reads the block to pull its
  own routed subset, so a block's re-scan is `n_shards × block_bytes`. At ~15 kb/read a
  10M-read HiFi block is ~150 GB, whose re-scan alone is ~4.5 h against the align
  step's PT4H baseline — the ticket could not finish. `pacbio_smrt` and
  `oxford_nanopore` now tile at 1M reads (~15 GB, ~27 min); `illumina` is unchanged at
  10M. Both timings are *floors* — they assume a scan rate measured warm, on local
  disk, with idle cores, where the real job reads Lustre while aligning — so the
  decision rests on the ordering (10M cannot fit even ideally; 1M can be several times
  worse than ideal and still fit), not the absolute numbers. Applies to NEW plans only:
  block ranges are persisted, so an alignment planned before this lands keeps its
  10M-read blocks and must be deleted and re-planned to re-tile. The target is resolved
  from the run's platform beside the aligner (also
  platform-derived, never a caller choice), via a new
  `_BLOCK_TARGET_READS_BY_PLATFORM` whose keys are pinned equal to
  `_ALIGNER_BY_PLATFORM`'s, so adding a platform forces an explicit block-size
  decision rather than inheriting the short-read default. Note the total re-scan across
  a sample's blocks is `n_shards × total_bytes` and therefore *invariant* to block
  size — what block size controls is the per-JOB share, i.e. whether one ticket fits
  its walltime. `block_planner._BLOCK_TARGET_READS` is deliberately untouched: read
  masking has no 1000-shard fan-out, so 10M stays correct there.

- **`align_sharded` gets the memory and cores it was allocated (#381).** The job
  hardcoded DuckDB to `memory_limit=8GB` / `threads=4`, so a 64 GB allocation reached
  DuckDB as 8 GB and the alignment output spilled gigabytes to shared scratch.
  `memory_limit` now resolves from the SLURM cgroup via `resolve_duckdb_memory_gb()`
  (a small reserve for the co-resident rype router and per-shard aligner indexes), and
  the `align` workflow's baseline rises to `cpu: 8, mem_gb: 64` — at the existing
  `action_ceiling`, so an OOM retry has no memory headroom to grow into. The thread
  count is load-bearing beyond parallelism: `SET threads` **is** miint's cross-shard
  concurrency (it ignores its own `threads` argument in sharded mode, and defaults
  `max_threads_per_shard` to 1), so the old literal capped the job at 4 concurrent
  shards regardless of allocation. Cores are NOT cgroup-resolved the way memory is —
  the thread count stays a module literal that must be kept equal to the workflow's
  `cpu:` by hand, now pinned by `test_align_cpu_pins_duckdb_threads`. Also drops a
  `DISTINCT` from the `read_to_shard` build
  that deduplicated a set already unique by construction (distinct `sequence_idx` per
  query row × one rype row per bucket), materializes the two-column `read_meta`
  relation instead of re-scanning the reads Parquet through a view, and sets bowtie2
  `ignore_quals := true` explicitly — quality was already unused (SHOGUN's
  `mismatch_penalty == mismatch_penalty_min` makes it a constant, and the align query
  projects sequences only), but as a side effect of a projection rather than a stated
  decision.

- **`align-plan` is told the mask (`mask_idx`); it no longer re-derives it — BREAKING wire change (#371).**
  `POST /sequencing-run/{idx}/sequenced-pool/{idx}/align-plan` now takes a required
  `mask_idx` and aligns the pool's samples whose `mask_sample` gate is `completed`
  under it. The server-side reconstruction of each sample's mask config (host refs
  from biosample metadata / `force`, adapter hash, lima/syndna) is removed: it
  matched the real mask only by coincidence — per-sample masks are minted from the
  submitter's `action_context`, a different source of truth — so `align-plan`
  returned `AlignNoMasksFound` for every pool on this deployment. `AlignPlanRequest`
  drops `force`, `host_rype_reference_idx`, and `host_minimap2_reference_idx` and
  adds the required `mask_idx`; a nonexistent `mask_idx` is a new 404
  (`AlignMaskNotFound`), distinct from the repurposed `AlignNoMasksFound` (422 — the
  mask exists but no pool sample is masked-complete under it).
- **The masked-read export and long-read-assembly readers now require a `completed` `mask_sample` gate (#371).**
  Both consumers previously treated the ABSENCE of a `mask_sample` row as "allowed".
  With completion now written first-class by every masking path, absence means "not
  masked-complete", so both reject it (export 409; assembly SUBMISSION/BAD_INPUT)
  rather than stream an absent or partial pass-set. The backfill above keeps
  historical per-sample masks passing across the deploy.
- **The reference DoGet ticket route now accepts `reference:read` in addition to
  `ticket:doget` (any-of) (#366).** `POST /reference/{reference_idx}/ticket/doget` mints
  a read ticket for reference sequences / chunks / taxonomy / phylogeny — all
  public reference data — so minting it is now also a `reference:read` capability
  (held by every human role), which is what lets the `qiita reference export` user
  CLI pull a genome's sequences. Strictly additive: the service-only `ticket:doget`
  stays an accepted scope, so the compute service account (which holds
  `ticket:doget`, not `reference:read`) keeps minting the feature_idx-scoped
  build/OGU tickets exactly as before — no principal loses access, no
  re-provisioning. Reader-set change, intentional: beyond the export CLI, a
  whole-reference ticket (no `feature_idx`) now lets **any authenticated human**
  bulk-egress a reference's entire sequence set, uncapped — by design, since
  reference data is public; a resource/bandwidth cap can come later if it proves
  necessary. `ticket:doget` also still solely gates the alignment DoGet
  (`POST /alignment/ticket/doget`), whose rows are sample-derived, not public.
- **Sample-metadata import can key global fields by internal_name.** The
  biosample and sequenced-prep-sample import surfaces gained an optional
  `global_internal_names` flag (`--global-internal-names` on both `qiita
  biosample create` and `qiita sequenced-sample create`): when set, a metadata
  column naming a global field is resolved against the field's machine-facing
  `internal_name` instead of its `display_name`, while purely-local fields stay
  display-name-keyed and the coincidental-collision shadow check is skipped.
  Defaults off, so existing callers are unaffected. Internally, one resolver
  now emits a single ordered list of resolved fields, and the pre-write
  resolution errors report the caller's field key namespace-neutrally. No env
  var, migration, scope, route, or wire change. (#386)
- **CLI surfaces a clean re-login prompt on a stale-scope 403 (#161).** When a
  PAT predates a scope its principal's role now grants (or was deliberately
  minted below the ceiling), a scope-gated route 403s even though the role
  allows it. The scope guards now flag that condition with a machine-readable
  `X-Qiita-Stale-Token-Scope` response header (twin of the existing #258 detail
  hint), and the CLI's single HTTP-error chokepoint (`run_http_subcommand`) keys
  off it to print a clean "your token predates a scope your role now grants — run
  `qiita login`" message instead of the raw JSON error envelope. Structured
  signal, so the CLI needs no drift-prone client-side copy of the role ceiling;
  every other HTTP error keeps the generic body echo. Closes the last direction
  of #161 — PAT authority stays immutable-once-minted (no auto-widening); this is
  the reactive re-login nudge, not a capability grant.
- **`qiita pool-completion` reads accurately and answers "done and clean?" at a
  glance (#217).** The subcommand's `--help`/description still described the command
  in the retired `fastq-to-parquet` / `prep-generation` / `GenPrepFileJob` terms
  (the API surfaces were corrected earlier but the parser text was missed); it now
  says demux (bcl-convert) + host-masking (read-mask), matching `PoolCompletionStatus`.
  The handler also gained a `render=` that, alongside the full JSON, prints a
  one-line human summary to stderr surfacing the three questions an operator asks —
  `fully_processed` (a DONE-and-clean verdict), `demux_state`, and
  `samples_not_submitted` (stranded samples) — so the answer no longer has to be
  picked out of the raw body. No route/schema change.
- **The bulk-block mask + align planners now resolve host filtering per sample, not
  pool-wide (#305).** `block_planner.plan_and_submit_blocks` and
  `align_planner.plan_and_submit_alignments` no longer take
  `host_rype_reference_idx` / `host_minimap2_reference_idx` pool-wide arguments;
  each sample's decision comes from its own `host_taxon_id` metadata via the shared
  `resolve_pool_sample_decisions` (the same `plan_pool_host_filter` seam the #303
  submit path uses), so a heterogeneous pool tiles into several mask partitions and
  each block's `action_context` carries ITS partition's host refs. This closes the
  drift where a pool masked through the block path ignored the per-sample plan the
  fan-out path already honoured.
  - `POST .../block-mask-plan` and `POST .../align-plan` gain a `force: bool`; their
    `host_*_reference_idx` become a **force-only override** (a host ref without
    `force` is a 422, mirroring the CLI). An UNRESOLVED / multi-host pool (or a
    resolved reference whose index isn't built) is refused **422**, naming the
    offending samples, before anything is minted. `align-plan` additionally
    refuses **422** when NONE of the pool's samples resolves to a minted mask
    (never block-masked, or a `--force` mismatch between the two plans) rather than
    returning a silent 202/0.
  - Response shape: the pool-wide `host_filter_enabled` / `host_rype_reference_idx` /
    `host_minimap2_reference_idx` move from the top level of `BlockMaskPlanResponse`
    onto each `BlockPlanPartition` (there is no single pool-wide answer any more);
    the align response's top-level host refs are dropped.
  - `submit-block-mask-pool` becomes a thin client: it POSTs the pool and lets the
    server resolve, instead of resolving client-side and refusing a non-uniform pool.
    Multi-host union stays deferred (#298); a mixed-host pool is still refused.

- **Host filtering is now resolved per sample from metadata, not chosen on the command
  line (#303).** `submit-host-filter-pool` and `submit-block-mask-pool` drive each sample's
  read mask from its own `host_taxon_id` metadata (via the resolver and roster added in
  #293/#294) instead of a pool-wide `--host-rype-reference-idx` flag. A blank's host comes
  from its pool (the shared `qiita_common.host_filter_plan`): one host → blanks inherit it;
  no host → pass through; more than one → refuse (multi-host union is not built). Anything
  UNRESOLVED aborts rather than masking against the wrong thing.
  - `--host-*-reference-idx` become an **override**: a bare flag is now an error, and
    `--force` applies it pool-wide (blanks included), bypassing resolution.
  - `--dry-run` prints the resolved per-sample plan and exits without submitting — the way
    to see what a pool would do before fanning out hundreds of tickets.
  - The Illumina and PacBio submit paths collapse to one: they differed only in where the
    decision came from, and now share it.

- **`host_taxon_id` is enforced at biosample import (#303).** The field was marked
  `required` in the schema but never checked, which is how every sample we hold came to lack
  it. An import that omits it is now rejected (422) before any write; a missing-value marker
  ('not applicable', 'missing: control sample') counts as supplied, since declining to
  answer is a decision the resolver understands. Deliberately narrow — only `host_taxon_id`
  is enforced, not every schema-required field.

- **SynDNA read-masking keeps its alignment and gates on the whole plasmid (#269, part 2).**
  The `syndna` step no longer reduces each read to a boolean and discards the alignment
  coordinates — it materializes the alignment and emits it as a second output, groundwork
  for a coverage-measurement consumer (see the deferral below). The spike-in gate (identity
  ≥ 0.95 AND aligned fraction ≥ 0.90, settled with the assay owner) is now single-sourced
  in `jobs/_coverage` and shared by the masking predicate; the aligned-fraction threshold
  enters the mask identity hash so a change re-mints. Inert until the SynDNA reference is
  re-ingested as plasmids + a per-insert GFF3 — a read-mask run without `syndna_enabled` is
  byte-identical to today.
  - **Per-feature coverage depth itself is deferred to a follow-up (tracked in #306).** Per
    review, it will land with its first consumer (the cell-count / BIOM path) as a
    **compute-on-demand** model — no persisted DuckLake `coverage` table, no minted
    `coverage_idx` — keyed by the **annotated element** (interval coordinates) rather than a
    per-feature sum, so copy-number variation among occurrences is preserved.

- **Sharded-alignment review revisions (#268).** Reworked the sharded-alignment
  path per review: the aligner is now derived from the run's sequencing platform
  (Illumina → bowtie2, PacBio HiFi / Nanopore → minimap2) at align-plan rather than
  chosen by the caller (`AlignPlanRequest` drops `aligner`; an unsupported platform
  is refused 422); bowtie2 runs the modified-SHOGUN parameter set (all concordant
  placements via `report_all`) and a pooled `cigar_sequence_identity` filter keeps
  only high-identity pairs (kept/dropped as a unit, never orphaning a mate),
  minimap2 uses `map-hifi` + `eqx` + `max_secondary := 100` (its analogue of
  `report_all` — dropping the arg falls back to a finite default that truncates
  multi-mapping reads). The identity floor is per-aligner: bowtie2 0.99 (short
  reads match nearly end-to-end), minimap2 0.90 (long reads carry more per-read
  divergence); the DuckLake `alignment` table drops the raw
  `reference`/`mate_reference` VARCHARs (`feature_idx`/`mate_feature_idx` carry the
  identity). A sharded reference's per-shard `.mmi` is now always built with the
  fixed `map-hifi` preset (not tunable on load). The GPL boundary is installed once
  at deploy (miint staging) instead of per job. Added a neutral `INDEX_TYPE_MINIMAP2`
  constant for the analysis-reference context (the host-filter-branded alias stays).
  (#268)

- **The deploy history moved out of `DEPLOY_CHECKLIST.md` into `docs/deploy-archive/`,
  one file per deploy.** 97% of that file was 36 archived deploys, whose bucket
  headings differ from the live ones by a single `#` — so every grep for a bucket
  returned ~37 hits and the file every PR folds into was 123 KB. It is now 67 lines.
  `/deploy-archive` writes the next archive file there instead of appending in place,
  and `/deploy-note` was given the same scoped-read recipe `redeploy.md` §1 already
  handed the human operator. Both `sed` contracts the deploy path depends on still
  hold, and both are now pinned by `test_deploy_scripts.py`: `qiita_buckets_12()`'s
  `### 1. Env vars` → `### 3. Migrations` span (already covered), and — newly —
  `## Deployed history`, which survives as an empty pointer stub *because* it
  terminates the range that prints the live section, and would otherwise read as
  dead weight for a future tidy-up to delete. (#296)

- **Enum parity is now checked without a database, so `make test` catches it.**
  `test_enum_parity.py` was `pytestmark = pytest.mark.db` in its entirety, so the
  rule most likely to be broken — adding a `StrEnum` value without its
  `ALTER TYPE ... ADD VALUE` twin — was only caught under Docker or in CI after a
  push. The Postgres value sets are now also reconstructed by replaying the enum DDL
  in `db/migrations/`, and the DB-backed checks are retained (plus a new one pinning
  the replay to the live schema, so the cheap tier cannot go green on a stale parse).
  (#296)

- **Agent tool output is quiet by default** via a checked-in `.claude/settings.json`
  (`PYTEST_ADDOPTS`, `CARGO_TERM_QUIET`, `UV_NO_PROGRESS`), newly tracked in
  `.gitignore` alongside `.claude/commands/`. A green `cargo test` printed 69
  `... ok` lines and a green `pytest` a header and warnings block on every invocation;
  failures still print in full. Human and CI runs are untouched — the vars are set in
  the agent's environment, not the shell's or the Makefile's, and CLAUDE.md now records
  the invariant that only *presentation* may live there (anything changing selection,
  ordering, or exit status belongs in the Makefile, where CI sees it too). (#296)

- **The work-ticket notification email now accounts for every ticket the recipient
  has, not just the ones that reached a terminal state.** Notifications land
  per-batch as tickets terminate, so during a fanout the recipient gets a stream of
  emails each reporting a slice — and none of them said where in the batch they
  were. "2 failed" could mean 2 of 26 still running or the tail of a batch that
  already finished, and the only way to tell them apart was to go run
  `qiita ticket list --active`. The digest now carries three buckets that between
  them cover every ticket the recipient has:
  - what just **finished** (unchanged — the owed set);
  - what is **still active**: `23 still active (3 queued, 20 processing)`, in the
    subject and both bodies, broken down per action when the active set spans more
    than one. Nothing in flight is now stated outright rather than left silent —
    that is the "everything else is done, act now" signal. The active set is
    `NON_TERMINAL_WORK_TICKET_STATES`, the same predicate `GET /work-ticket?active=true`
    filters on, so the email answers exactly the question that command would, and a
    parity test pins the terminal and non-terminal sets as exact complements over
    `WorkTicketState` (the "nothing still active" line is only true if they
    partition the enum);
  - what is **held for redrive**: a ticket that exhausts its infrastructure retries
    lands in FAILED with `failure_type=retriable`, which the owed set deliberately
    withholds from email (so a redrive-and-complete reports the *true* outcome) — but
    it is terminal, so it was in neither half of the notification. A user whose
    tickets all died on NODE_FAIL could get no email at all, and the new "nothing
    still active" line would have positively asserted everything was accounted for.

  Two defects surfaced while building it, fixed here. **A redrive landing inside the
  send window was stamped away, so the ticket was never emailed again**:
  `POST /work-ticket/{idx}/run` resets `notified_at` to NULL precisely so a redriven
  ticket re-notifies at its true terminal state, but the sweeper's send-then-stamp
  UPDATE guarded only on `notified_at IS NULL` — a redrive between the owed-set
  SELECT and the stamp was clobbered, and the ticket went out reported as `failed`
  and then went permanently silent. The stamp now re-asserts the whole owed-set
  predicate, so a redriven ticket (back to `pending`) no longer matches and stays
  owed. And **the plain-text digest collapsed every detail row onto one line**: the
  optional failure-reason clause closes with a `{% endif %}` at end-of-line, which
  Jinja's `trim_blocks` swallows along with the row's newline, so all N rows and the
  footer behind them ran together (HTML readers were unaffected — the rows are a
  `<table>` there). The receipt's `template_context` records the claim the email made
  (`active_total`, `active_counts`, `active_actions`, `held_total`), rendered from
  the same rollup rather than a second tally that could drift from it. (#283)

- **Deploy checklist: archived the 2026-07-12 deploy (`56ce7d4`, 13 PRs) and added a
  post-verify bucket 6.** `HMAC_SECRET_KEY` retirement moves into it. Bucket 1
  previously told the operator to delete it *before* the restart, which buys
  nothing — the new build never reads it (both config loaders look up named vars,
  so an unknown one is inert) — while it strands the still-running OLD build (which
  boots on it) and discards the rollback path during the riskiest part of the
  deploy. Bucket 6 is now the home for any irreversible cleanup that burns the way
  back: it runs only once bucket 5 is green and needs no restart of its own. The
  archived block records that this deploy already followed that order. `redeploy.md`
  (source of truth for bucket order), `/deploy-note` and `/deploy-archive` updated
  to match. (#276)
- **CP library primitives now use `duckdb_connect()` instead of bare
  `duckdb.connect(":memory:")` (#349).** Ten call sites in `library.py` switched to
  a new `miint.duckdb_connect()` helper that always passes `miint_connect_config()`
  (sets `extension_directory` when `MIINT_EXTENSION_DIRECTORY` is present). No
  behavior change today — none of these paths loads an extension — but the first
  one that gains INSTALL/LOAD would otherwise resurrect the `/dev/null` `$HOME`
  failure that took down every `long-read-assembly` ticket. The helper also
  documents why we defer `SET home_directory=` (prod sets the var; deploy checks
  enforce it; dev/CI have writable `$HOME`).


### Removed

- **The single-end rype projections are gone (#478).** `align_sharded._ROUTING_QUERY` and
  `host_filter._RYPE_QUERY` narrowed the classify relation to `sequence1` so miint would not
  read rype's `is_paired` off the mere presence of an all-NULL `sequence2` column, which
  halved the Arrow batch and doubled the full index reloads. `rype_classify` now derives
  `is_paired` from that column's CONTENTS
  ([duckdb-miint#199](https://github.com/the-miint/duckdb-miint/issues/199), closed
  2026-08-01), sampled from the first chunk of its single pass over the relation, so the
  narrowing can no longer change batch sizing either way. Both jobs hand rype the full query
  relation. The restated `rype_classify` contracts in both job docstrings and in
  `docs/duckdb-miint.md`'s function inventory now link the upstream page instead of copying
  it — the copies had drifted to the inverse of current behaviour — and the
  `docs/duckdb-miint.md` "Open upstream gaps" row for #199 is dropped with its workaround.
  `test_align_sharded_routes_from_a_view_carrying_both_mates` and
  `test_host_filter_hands_both_tools_both_mates` keep the two properties that outlived the
  workaround: routing reads a VIEW, and both aligners still receive `sequence2`. Closes the
  removal tracked at #403. One sizing behaviour does change for a block that mixes single-
  and paired-end reads: upstream samples the first chunk of its single pass, so such a
  block is now sized from whichever shape leads, where the old gate forced the paired
  (larger) estimate. `align_sharded` rejects a mixed batch outright; `host_filter` does not,
  but a read-mask block is planned per `sequencing_run_idx` and a run carries one platform.
  The comments and `docs/duckdb-miint.md` entries describing rype's per-call TEMP-table
  corpus copy are corrected too — `rype_classify` streams the relation as of
  [duckdb-miint#245](https://github.com/the-miint/duckdb-miint/pull/245).

- **The intake `human_filtering` policy flag (#303).** Host filtering no longer reads a
  per-project intent recorded at intake — a sample's host is a property of the sample, not
  of the project it was booked under. The pre-flight readers, the roster field, the
  submit-time intent cross-check, and the CLI mismatch flags are gone; nothing stored it.
