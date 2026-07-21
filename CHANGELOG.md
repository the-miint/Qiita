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

**Where to add your entry.** This file is long, and `Unreleased` accumulated
duplicate bucket headings because each wave of PRs added a fresh one at the top
rather than scrolling to find the existing one. Don't do that: add your bullet
under the **first** `### Added` / `### Changed` / `### Fixed` / `### Removed`
heading under `## [Unreleased]`, and **never create a new bucket heading**. The
duplicates further down are historical strata; leave them where they are.

## [Unreleased]

### Added

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
- **Data-plane horizontal scaling is now a single, deploy-durable knob.** The
  instance set is `QIITA_DATA_PLANE_PORTS` (default `50051`), read once by
  `qiita_data_plane_ports` and used to render the nginx upstream, to
  enable/restart the matching `qiita-data-plane@NNNN` units, and to health-check
  each instance in `verify-deploy`. Previously scaling out did not survive a
  deploy: `activate.sh` overwrites `/etc/nginx/conf.d/qiita.conf` from the
  checked-in file, so a hand-added upstream member disappeared at the next
  deploy, and the restart list was hardcoded to `@50051`, so an added instance
  kept running stale code. Also adds a loopback-only plaintext gRPC balancer
  (`127.0.0.1:50050`) for on-host clients: compute nodes already reach the pool
  through nginx at `grpc+tls://<host>:443`, but the control plane's default
  `DATA_PLANE_URL` addressed instance #1 directly, so CP-side Flight traffic
  bypassed the balancer entirely.
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
  filesystem. The selectors reuse the data plane's existing
  `block_read_where_clause` and `EXPORT_READ_COLUMNS`, so a block's read
  footprint and its delete footprint cannot drift. Gated on a new
  `read:doget` scope rather than the generic `ticket:doget`: `read_block`
  streams RAW reads, a strict superset of the `read_masked` surface that
  already carries its own privacy-sensitive scope, so reusing the
  reference-read scope would have inverted the model.

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
- **`qiita-data-plane@.service` set `LISTEN_ADDR` before `EnvironmentFile=`.**
  systemd applies the two in file order, last writer wins, so a `LISTEN_ADDR` in
  the SHARED `/etc/qiita/data-plane.env` would have overridden the per-instance
  value for every instance and collapsed them all onto one port. Latent until
  now (the live host leaves it unset), and a trap for the first operator to scale
  out. The per-instance assignment now comes last.
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


### Removed

- **The intake `human_filtering` policy flag (#303).** Host filtering no longer reads a
  per-project intent recorded at intake — a sample's host is a property of the sample, not
  of the project it was booked under. The pre-flight readers, the roster field, the
  submit-time intent cross-check, and the CLI mismatch flags are gone; nothing stored it.

### Fixed

- **Sharded-alignment review — silent-wrong-data and pre-flight-failure fixes (#268).**
  A second review pass surfaced latent defects in the (never-yet-run) sharded path,
  fixed here:
  - `pyarrow` is now an explicit `qiita-compute-orchestrator` dependency — the sharded
    index-build steps import `pyarrow.flight`, so without it the first `reference-add`
    / `build-shard-index` ticket died `ModuleNotFoundError`.
  - Deleting an alignment definition mid-flight no longer silently realigns RAW
    (non-host-depleted, un-QC'd) reads: the align/mask discriminator now reads the
    trusted `action_context` alignment_idx and fails loud when it disagrees with the
    `ON DELETE SET NULL` `work_ticket.alignment_idx` column.
  - Deleting a reference that any alignment definition aligns against is refused, even
    with `force` — the cascade cannot clean the DuckLake `alignment` rows it owns
    (keyed on orphaned `feature_idx`); the operator deletes the alignment definition
    first.
  - `finalize_shard` no longer flips a reference to `active` with the current shard
    generation unbuilt: a re-plan invalidates the reference's per-shard
    `reference_index` rows in the same transaction, re-scoping the completeness gate
    to the current generation.
  - `build_routing_index` scopes the rype corpus to the shard-mapped feature set
    instead of hard-failing after the fan-out when a reference has no-genome members —
    a partial genome map is a supported input, not a post-fan-out failure.
  - `shard_index=true` on a reference with no shardable features now fails the ticket
    (redrivable `failed → pending`) instead of finalizing a terminal, unroutable
    `active` reference.
  - The minimap2 identity floor is 0.90 (was sharing bowtie2's 0.99), so long-read
    placements are no longer silently discarded; `build_routing_index` also cleans up
    its multi-GB `router_chunks.parquet` intermediate instead of leaking it. (#268)

- **One definition of "a migration's `up` half," shared by every test that replays the
  schema without a DB** (`qiita_control_plane.testing.migrations`). Splitting a
  migration on the bare substring `migrate:down` truncates any file whose up-half prose
  *mentions* the marker — `20260624000000_drop_sequenced_sample_host_references.sql`
  does, at line 19, cutting its up half from 28 lines to 19 and silently dropping the
  DDL below. The marker is now matched anchored to a line start. Comment stripping is
  likewise quote-aware, because the migrations really do contain `--` inside
  `COMMENT ON ... IS '...'` literals, and a naive strip cuts the literal in half and
  leaves an unbalanced quote; it now asserts quote balance rather than trusting itself.
  `test_work_ticket_state_parity.py` (which had its own correct-but-separate scanner)
  uses the shared helper too, so the two cannot drift apart. (#296)

- **`CLAUDE.md` described the native-job contract as "exactly two symbols."** It has
  three: `Inputs` and `execute` are required, and `plan(inputs) -> JobPlan` is an
  optional submit-time resource-sizing hook — dispatched by `run_native_job_plan`,
  exposed at `/step/plan`, and implemented today by `jobs/qc.py`.
  `docs/writing-a-job.md` had it right; `CLAUDE.md` now agrees with it and the code.
  Also documents the `sif-build.d/<image>.env` multi-image spec form, which
  `deploy/build-sifs.sh` globs and `read-mask` / `long-read-assembly` use, and which
  `CLAUDE.md` did not mention at all. (#296)

- **`/deploy-archive` told the agent to reset `## Pending deploy` to "five" bucket
  sub-headings; the checklist has six.** An agent following it literally dropped
  bucket 6 — the irreversible-cleanup bucket, whose whole purpose is to hold the
  steps that burn the rollback path. It is now told to empty the buckets *in place*
  rather than retype the list, so the file remains the source of truth for its own
  shape. (#296)

- **A DB-tier test leaked terminal work tickets, reddening `main` on macOS.**
  `test_sequence_range_backfill`'s fixture seeded `work_ticket` rows and never removed
  them; terminal with `notified_at IS NULL`, they matched the notify sweeper's owed-set
  predicate, and the sweeper scans the whole database — so it emailed that file's
  principals and an unrelated assertion counted five. The DB is isolated per xdist
  *worker*, not per test, so this only fired when the runner's core count co-located the
  two files. The fixture now cleans up after itself, and an autouse tripwire fails the
  test that leaks an owed ticket rather than the innocent one that trips over it. (#291)
- **CI can now run the macOS matrix on a PR, before merge.** macOS coverage ran only on
  push to `main`, so a defect only it can see — like the one above — was found with
  `main` already red. Label a PR `ci-macos` to fan the matrix out over macOS too; use it
  for anything touching shared test fixtures or cross-test DB state. (#291)

- **`no_data` work tickets were invisible to the reference and sequenced-pool delete
  gates, and to the reference-load CLI's watch loop.** `WorkTicketState` has three
  terminal states — `completed`, `no_data`, `failed` — but the terminal/non-terminal
  split was maintained by hand in five separate places, and several of them spelled
  terminal as `("completed", "failed")`. `no_data` was added later (#176) and never
  folded back in, so it fell through every one of them:
  - **Sequenced-pool delete (live bug).** The gate counted a `no_data` ticket as
    neither in-flight nor terminal, so it blocked nothing. An unforced `DELETE`
    returned 200 and the state-blind cascade purged the tickets anyway — the exact
    outcome the gate exists to prevent. This was not a corner case: `no_data` is the
    *expected* result for an empty well, so an all-blank plate, or a pool whose reads
    were entirely masked out, deleted with no 409 and no `force`.
  - **Reference delete (latent twin).** Same hole; unreachable today only because no
    reference workflow can produce `no_data`.
  - **`reference-load --watch` (latent).** The poll loop stopped on `completed`/
    `failed` only, so a `no_data` ticket would be polled to the 24 h ceiling and then
    reported as never having reached a terminal state — which it had.

  The split now has one home: `TERMINAL_WORK_TICKET_STATES` and
  `NON_TERMINAL_WORK_TICKET_STATES` live beside the enum in
  `qiita_common.models.work_ticket`, with the non-terminal side **derived** as the
  complement rather than written out, so the two can never disagree and a seventh
  state lands in exactly one of them by construction. Every consumer — the runner's
  abort check and guarded transitions, `dispatch`, the work-ticket routes'
  disallow-without-delete gate and `?active=true`, the notify digest, both delete
  gates, the force-fail CLI, the pool-completion rollups' inline SQL — imports from
  there; the five hand-maintained copies are gone. A unit test pins the partition, and
  the delete gates' 409 `detail` is now derived from the tuple, so it can't go stale
  the way `"completed/failed"` did. (#286)

- **`bam-to-parquet` could not ingest a real PacBio HiFi sample: 24 of 26 failed on
  the first production run.** Two compounding defects, both fixed here.
  *The write:* the job wrote its reads with one `COPY … ORDER BY sequence_idx` — a
  BLOCKING sort over the full seq+qual payload. A HiFi read is ~15-25 kB against
  Illumina's ~150 bp, so a routine 2M-read sample is tens of GB to sort, against a
  *hardcoded* 7 GB DuckDB `memory_limit`; only the two control-sized samples in the
  pool (11k and 44k reads) fit. That sort was never needed: `PARQUET_OPTS` does not
  produce a globally sorted file anyway (row groups land in thread-finish order) —
  what it buys is per-row-group clustering on the sort key, for DuckLake pruning. And
  the data is already monotone (`sequence_idx = sequence_index + start - 1`). The job
  now writes `read/part_*.parquet` in bounded monotone batches (~1 GiB of payload
  each), so every part's row groups carry a tight, disjoint `sequence_idx` window —
  the same pruning, with peak memory flat in the batch instead of the sample. The
  multi-file table shape is the one `reference_load`/`hash_sequences` already use, for
  exactly this reason. `memory_limit` is now sized from the real cgroup
  (`resolve_duckdb_memory_gb`, as nine other jobs already did) so `--mem-gb` and the
  OOM escalation can actually reach it, and the workflow allocation is modest
  (12 GB / PT4H, ceiling 32 GB / PT12H) rather than the 32/96 GB an unbatched sort
  would have demanded.
  *Retry:* `bam_to_parquet` and `fastq_to_parquet` mint a `sequence_range` and
  *then* do the heavy durable write — exactly the window an OOM/walltime kill lands
  in — but never read an orphaned range back. The runner re-runs the whole module on
  such a (transient) failure, the re-mint hit the one-shot contract, and the step
  died *permanently* with `already has a sequence_range`: hiding the OOM that
  actually killed the first attempt, and defeating the OOM escalation, which can
  only pay off if the escalated attempt gets past the mint. `ingest_reads` already
  handled this; its private helper is now the shared `mint_or_reuse_sequence_range`
  in `sequence_range_retry` and all three jobs use it (409 → read back → validate
  width against the read count → reuse). Both halves were needed: without the
  read-back the escalation cannot land, and without the memory it has nothing to
  land on.
  *Guard:* reusing an orphaned range is only safe when the range belongs to a prior
  ATTEMPT OF THE SAME ticket. A *different* ticket hitting that 409 means the
  sample's reads are already registered, and reuse would register them a second time
  — silently, since DuckLake has no uniqueness. Nothing else could tell the two
  apart: the submit-time disallow-without-delete gate blocks only NON-terminal
  tickets, so a COMPLETED sample can be resubmitted. `qiita.sequence_range` now
  records `minted_by_work_ticket_idx`, and a reads job reuses a range only when it
  matches its own ticket AND that ticket is still in flight — a `completed` minter's
  reads are already registered, so even its own stale attempt must not re-write the
  range. That second gate is an ALLOWLIST of in-flight states, derived from the
  canonical terminal/non-terminal split rather than spelled out: a denylist would let
  a `work_ticket_state` added later fall through to the permissive path by default.
  A different, terminal, or unknown minter fails permanently, with a recovery message
  that differs by state. Without this the read-back would have *removed* the
  accidental guard the one-shot mint was providing. (#285)
- **Container steps had no usable `TMPDIR`, so a step doing real work would die
  partway through.** `apptainer exec --containall` mounts a *tmpfs* `/tmp`, sized
  by the host's `sessiondir max size` (64 MiB on the live deploy), and scrubs the
  environment — so `TMPDIR` was unset and an entrypoint's bare `mktemp -d` landed
  on a 64 MiB in-memory disk. Every `long-read-assembly` entrypoint stages its
  working set there (hifiasm_meta's assembly, the decompressed reads FASTQ,
  DAS_Tool/CheckM working dirs), and what did fit was charged to the job's cgroup
  memory — silently eating the allocation its own resource sizing assumed. The
  payload now forwards `TMPDIR=<workspace>/tmp`: real disk, already bound via
  `--home`, cleaned up with the workspace. Container steps only; a native step has
  ordinary node-local `/tmp`. (`bcl-convert`, the one container workflow that has
  actually run in production, never hit this — it uses no `mktemp`.) (#275)
- **The four `long-read-assembly` SIFs could not build, and could not have run.**
  Three defects, none previously exercised — the workflow merged but was never
  deployed, so its `%test` had never once executed:
  - `apptainer build` runs `%test` inside the finished, read-only image with
    `HOME=/root`, so libmamba could not create its cache dir and hard-aborted.
    `%test` now sets a writable `HOME`, as the SLURM payload does at run time.
  - `checkm`'s `%test` invoked CheckM without `CHECKM_DATA_PATH`, so its
    `DBManager` fell through to writing `DATA_CONFIG` inside site-packages —
    another write into the read-only image. `checkm.sh` always sets that variable,
    so the test was exercising a state production never reaches.
  - **Runtime-fatal:** the images shipped no Python, but `_lib.sh` runs `python3
    manifest_writer.py` to emit `manifest.json` — which every step must write and
    the backend verifies before registering output. Every step would have finished
    its full tool run and died on its final line. `python=3.11` is now in each
    base env, pinned, with a lockstep grep guard on `_lib.sh` — both mirroring
    `bcl-convert`, which got this right.

  The build aborted the deploy rather than restarting into a broken state, via
  `build-sifs.sh`'s refuse-on-unbuildable-image guard. All four images have since
  been built and smoke-tested on the deploy host under production apptainer flags.
  (#275)

### Removed

_None yet._

### Added

- **Metagenomic OGU feature-table estimation (compute-on-demand) (#304).** Adds
  the `estimate-feature-table` reference-scoped workflow: given one `alignment_idx`
  and an explicit `prep_sample_idx` cohort (on the ticket's `action_context`), the
  native `estimate_feature_table` job builds a genome-keyed OGU feature table via
  duckdb-miint `woltka_ogu`, filtered to genomes whose breadth of coverage —
  POOLED over the cohort — meets a user-provided `coverage_threshold`, and emits a
  single `ogu_table.parquet` (v2+zstd). The OGU key is `genome_idx` (counts +
  coverage roll `feature_idx → genome_idx`, so multi-contig genomes are handled
  natively). The table is **computed on demand and never persisted** (deterministic
  but cohort-dependent), so no `processing_idx`/identity is minted and no DuckLake
  row is written. Mechanism: the DuckLake `alignment` table is now DoGet-readable
  (new projected DP `alignment` DoGet, scoped `{alignment_idx, prep_sample_idx[]}`);
  a CP mint route (`POST /alignment/ticket/doget`, service-account `ticket:doget`)
  signs that ticket at job runtime, deriving the scope from the work ticket's
  `action_context`; a runner resolver stages the reference's `feature_idx →
  genome_idx` map from Postgres and refuses an incomplete cohort; the job streams
  the alignment slice + per-feature lengths from the data plane over Arrow Flight
  (no disk) and runs `genome_coverage` + `woltka_ogu`. All identifier columns pass
  as native `BIGINT` with no `::VARCHAR` casts (requires the woltka_ogu id-type
  preservation fix in the miint build). No new scope, migration, or enum. Deferred:
  the user-facing REST trigger, BIOM export, persistence/identity, and downstream
  diversity (the consumer this feeds). Also hardens the (not-yet-deployed)
  `align_sharded` persist filter so this breadth estimate is sound at the source:
  minimap2 placements must now clear a query-coverage floor (0.90, via
  `cigar_query_coverage`) in addition to sequence identity, so a soft-clipped
  high-identity long read can no longer contribute a low-coverage placement (bowtie2
  aligns end-to-end, so its query coverage is ~1.0 and it is unaffected).

- **Sharded-reference alignment consumer (C2b).** Wires the C1 `align_sharded`
  native job into a runnable `align` workflow: an operator submits an align run
  for a sequenced-pool against an ACTIVE sharded reference + an aligner, the CP
  mints an `alignment_idx` (deduped on the align config — reference + aligner +
  mask + the reference's sorted shard-set; growth is not yet supported), tiles the
  pool's already-MASKED samples into blocks, fans out one `align` block ticket
  per block, each streams that block's masked reads (new Rust
  `export_read_masked_block` DoAction over the `read_masked` view), runs
  `align_sharded`, and registers an `alignment.parquet` into a new DuckLake
  `alignment` table (keyed by `alignment_idx`, NOT `processing_idx` — the formal
  hierarchy is deferred). A per-`(alignment_idx, prep_sample)` gate
  (`alignment_sample`, twin of `mask_sample`) flips `completed` once every
  covering block finishes; re-submitting a completed sample is refused until its
  rows are DELETEd (disallow-without-delete). Adds the `alignment_definition` /
  `alignment_sample` identity + gate tables and `mint_alignment_definition`
  (migrations `20260712000000`/`010000`), a nullable `work_ticket.alignment_idx`
  (`20260712020000`), the `delete-alignment-block` / `reconcile-alignment-block`
  library primitives (backed by new replay-safe `delete_alignment_block` /
  `delete_alignment` data-plane DoActions over the `alignment` table), the
  `align_planner` fan-out, and `POST .../sequenced-pool/{}/align-plan`. The
  disallow-without-delete escape hatch is `DELETE
  /alignment-definition/{alignment_idx}` (new system_admin-only
  `alignment_definition:delete` scope) — it purges the alignment's DuckLake rows
  and its `alignment_definition` row, cascading the `alignment_sample` gate so a
  fresh plan can re-align. (#268)

- **Sharded-reference alignment foundation (C2a).** Makes a *sharded* reference
  index-complete and resolvable — the piece C1 left missing (it shipped the
  `align_sharded` consumer but nothing built the whole-reference router in
  production or resolved its path). A sharded `reference-add` /
  `local-reference-add` now builds and registers the ONE whole-reference
  `rype_router` after `plan-shards`: the `plan-shards` runner arm stages a
  `shard_mapping` Parquet from `reference_membership.shard_id` (Postgres — the
  authoritative store) and returns `router_pending`, which gates three new
  workflow entries (`build_routing_index` → `register-index` → `finalize-shard`)
  run by the PARENT ticket in parallel with the per-shard build children.
  `finalize_shard` now gates `indexing → active` on the `rype_router` row being
  present (shard_id NULL) in addition to every per-shard index — so `active`
  guarantees a routable, alignable sharded reference. Adds a shard-aware resolver
  `_resolve_sharded_align_indexes(reference_idx, aligner) → (router_paths,
  shard_directory)` (router paths returned as a LIST for the forward growable-
  reference case; `shard_directory` derived from a per-shard row's fs_path parent)
  — shipped tested but UNWIRED (the align workflow / DuckLake alignment sink /
  block fan-out are the deferred C2b). The `reference_index.index_type` CHECK now
  admits `rype_router` (migration `20260711000000`). (#268)

- **Sharded alignment consumer (C1, native-job-only).** Three native jobs that
  *consume* the per-shard indexes B5 produces, so a read aligns against only the
  shard(s) it minimises into. `build_routing_index` builds a whole-reference
  MULTI-bucket rype router (`references/{idx}/rype-router.ryxdi`, one bucket per
  shard) that one `rype_classify` pass turns into a `read_to_shard` table.
  `align_sharded` (aligner `minimap2`|`bowtie2`) streams that routing, makes a
  SINGLE `align_{minimap2,bowtie2}_sharded` call over the whole read block
  (modelled on `host_filter` — no SE/PE split; a read set is uniformly SE or PE
  by construction and the aligners handle the mode natively), passes the aligner's
  FULL output through verbatim, and adds only `prep_sample_idx`, `feature_idx`
  (`CAST(reference)`), and `mate_feature_idx` (`CAST(mate_reference)`), emitting a
  sorted `alignment.parquet`. NO dedup — `(sequence_idx, feature_idx)` is not a
  key: a read routed to K shards yields K distinct-`feature_idx` rows, and a PE
  read's two mate rows are ONE read's alignment to a feature (the pairing explicit
  in `flags` + the mate columns), not two independent alignments. The
  `derived_store` per-shard aligner layout was
  revised to the exact `shard_directory` shape miint expects
  (`minimap2-shards/{shard_id}.mmi`, `bowtie2-shards/{shard_id}/index*`), and the
  `align_{minimap2,bowtie2}_sharded` + multi-bucket `rype_classify` contract is
  now qiita-verified in `docs/duckdb-miint.md`. Adds `INDEX_TYPE_RYPE_ROUTER`.
  Not wired into a workflow (C2 wires the runner block × shard fan-out + the
  DuckLake alignment sink); the `reference_index.index_type` CHECK gains
  `rype_router` only when C2 registers the router. (#268)

- **Sharded-index status endpoint (B5, observability).** New
  `GET /api/v1/reference/{idx}/shard-index-status` (model `ReferenceShardIndexStatus`)
  surfaces a sharded reference's fan-out build progress: `expected_shards` (N, derived
  from `reference_membership.shard_id` — the same count `finalize-shard` gates on),
  per-`index_type` `registered_shards` (each expected type seeded to 0 so a wholly-unbuilt
  type is visible rather than absent), and `failed_shard_tickets` (build-shard-index
  tickets in `failed`). Makes a reference wedged in `indexing` on a permanently-failed
  shard diagnosable; remediation is an operator redrive of the FAILED ticket. Scoped to
  `reference:read` like the `/index` listing; an unsharded reference reads all-zero /
  empty. (#268)

- **Sharded reference-add wiring + build-shard-index workflow + CLI (B5,
  live end-to-end).** `reference-add` / `local-reference-add` gain an opt-in
  `shard_index` context flag (+ `build_rype`/`build_minimap2`/`build_bowtie2`
  gates, `rype_w`/`minimap2_preset` knobs, and a both/all-off backstop); when set,
  a `plan-shards` action runs after register-files to assign shards and fan out
  the build. A new `build-shard-index/1.0.0` workflow (target_kind reference, NO
  success_status) builds one shard's rype/minimap2/bowtie2 indexes (each gated,
  each with a register-index sibling) and ends with `finalize-shard`. The runner's
  finalize now skips the parent's success_status patch while a sharded fan-out is
  in progress (`_shard_fanout_owns_finalize`) so `indexing → active` is owned by
  the terminal finalize-shard — unsharded / sharded-but-N=0 / host paths patch
  `active` inline unchanged. The CLI's `qiita reference load` gains `--shard-index`
  (mutually exclusive with `--host`, requires `--taxonomy`) and `--no-bowtie2-index`,
  and the existing index knobs (`--no-rype-index`/`--no-minimap2-index`/`--rype-w`/
  `--minimap2-preset`) now apply to `--shard-index` as well as `--host`. Default
  (no `shard_index`) is byte-identical to today's `loading → active`. (#268)

- **Runner shard-roster staging + rype shard build streams (B5).** For a
  reference-scoped ticket carrying a non-NULL `shard_id`, the runner now stages
  the shard's feature roster before the step loop (`_stage_shard_roster`): it
  reads the shard's members from `reference_membership.shard_id`, signs a
  `feature_idx`-scoped `reference_sequences` DoGet (so each shard transfers only
  its own slice, not the whole reference N times), and writes
  `shard_roster.parquet`, binding `shard_features` + `shard_id` for the build
  steps' `Inputs`. `build_rype_index` shard mode now STREAMS its chunks from the
  data plane (`open_reference_chunk_stream`, scoped to the roster) instead of
  reading a staging Parquet — a shard build runs after the ingest ticket's
  register-files has moved the staging chunks into DuckLake, so there is no
  staging Parquet to read (matching how B4's minimap2/bowtie2 shard modes already
  stream). Host/whole-reference rype mode is byte-identical (staging read).
  A Flight failure / empty shard is wrapped as a SUBMISSION BAD_INPUT.
  (#268)

- **Sharded-index fan-out + count-based completion (B5).** A new
  `shard_orchestration.plan_and_submit_shards` turns a `plan_shards` assignment
  into N build tickets: it transitions the reference `loading → indexing` and,
  in one transaction, INSERTs one PENDING `build-shard-index` `work_ticket` per
  shard (scope `reference`, carrying `shard_id=k` + the index-selection context
  copied from the parent), then dispatches each fresh ticket. N = 0 (no genomes)
  is a no-op. Idempotent on redrive (`ON CONFLICT DO NOTHING` on the per-shard
  index; the `loading → indexing` transition tolerates an already-`indexing`
  reference). The runner threads a `dispatch_cb` (`schedule_dispatch`) from the
  dispatch layer down through `run_workflow` → `_run_action_primitive` so the
  fan-out fires child dispatches; a crash between INSERT and dispatch leaves the
  tickets PENDING for the next startup reconcile. A new `finalize-shard`
  primitive (`actions.library.finalize_shard`, registered in `LIBRARY`) is each
  build ticket's terminal step: it counts registered shards per expected
  `index_type` against N (derived from `reference_membership`) and does the
  guarded `indexing → active` only when every type is complete — fail-closed
  (a missing shard leaves `indexing`; it never flips to `failed`), and
  last-observer-race-safe (the guarded UPDATE lets exactly one racer win;
  a finalize that finds the reference already `active` is idempotent success).
  Dormant — no workflow YAML references these actions yet. (#268)

- **`plan-shards` assignment core (B5).** A new CP-side `action:` primitive
  (`plan_shards`, registered in `LIBRARY`) turns the B2 tiler + persistence into
  an end-to-end shard assignment for one reference: it streams
  `(feature_idx, genome_idx)` from Postgres to a Parquet, DoGets the reference's
  `reference_taxonomy` from the data plane to a Parquet, reduces to one
  lineage per genome in a local DuckDB (`arg_min` over the lowest feature_idx —
  a deterministic representative; all-NULL taxonomy → unclassified `''`), tiles
  lineage-sorted via `tile_by_lineage`, expands genome→feature back in DuckDB,
  and persists onto `reference_membership.shard_id`. Returns N (=
  `min(num_shards, genome_count)`; 0 for a reference with no genomes).
  No-genome features are dropped by the inner JOIN and stay `shard_id NULL`
  (16S / deferred). `write_shard_assignment` now clears every membership row's
  `shard_id` first, so a re-plan that drops a feature leaves it NULL rather than
  stale. Dormant — the N-ticket fan-out over the assigned shards is a later
  commit. (#268)

- **`work_ticket.shard_id` fan-out discriminant (B5 schema).** A nullable
  `INTEGER` column (CHECK: only legal on `reference` scope, `>= 0`) lets N
  concurrent same-action build tickets fan out over one reference without
  colliding. The existing `work_ticket_one_in_flight_per_reference` partial
  UNIQUE is re-partitioned with `AND shard_id IS NULL` (preserving the exact
  one-per-reference guarantee for every non-sharded action and the ingest
  ticket), and a new `work_ticket_one_in_flight_per_shard` gates at most one
  non-terminal ticket per `(action, reference, shard)`. The `WorkTicket` model
  and the runner/route read paths carry `shard_id`; a racing INSERT maps to 409
  like every other scope. Dormant — nothing sets `shard_id` yet. (#268)

- **Per-shard aligner-subject builders (B4): minimap2 `.mmi` + bowtie2 `.bt2`,
  streaming via B6s.** `build_minimap2_index` gains a shard mode and a new
  `build_bowtie2_index` native job lands alongside it. Given a `shard_id` + a
  runner-staged feature roster, each builds one shard's analysis subject index over
  just that shard's features, pulling the chunk bytes from the data plane over Arrow
  Flight (the B6s stream) instead of staging Parquet, and writing to
  `{PATH_DERIVED}/references/{idx}/shards/{shard_id}/{minimap2,bowtie2}/index*`
  (new `derived_store` helpers). Host/whole-reference mode is unchanged
  (byte-identical staging read). The chunk reassembly is single-sourced in a new
  `subject.stage_subject`; the ticket-fetch+stream composition is a new
  `data_plane_client.open_reference_chunk_stream`. Both builders expose a `plan()`
  that sizes shard memory down from the whole-reference baseline. bowtie2's index is
  preset-independent (`save_bowtie2_index` takes no preset, unlike minimap2) and
  needs no GPL boundary — both verified against the team-mirror miint build by the
  host-mode real-miint smokes. The builders are unwired (jobs only); shard
  assignment, roster staging, fan-out, and workflow wiring are B5. (#268)

- **`reference_index.index_type` admits `'bowtie2'` (B4 precursor).** A one-line
  additive CHECK migration extends the `reference_index_index_type_check` allow-list
  to `rype`/`minimap2`/`bowtie2`; a matching `INDEX_TYPE_BOWTIE2` constant lands in
  `qiita-common`. bowtie2 is an analysis-only subject index, so it is deliberately
  absent from `HOST_FILTER_REQUIRED_INDEX_TYPES` (unlike the dual-purpose
  rype/minimap2). No Postgres ENUM twin (TEXT+CHECK), no `register_index`/runner
  change (already generic over `index_type`). (#268)

- **Compute-side reference-chunk streaming (B6s).** The orchestrator can now pull a
  reference's sequence chunks from the data plane over Arrow Flight instead of
  reading staging Parquet — the streaming foundation the B4 shard builders sit on.
  A new orchestrator `DATA_PLANE_URL` setting (default `grpc://localhost:50051`, not
  fail-fast) is propagated into the SLURM job env like `PATH_DERIVED` so the native
  launcher resolves the real data-plane origin on the compute node. A new
  `data_plane_client` module carries the two-step retrieval path: an async
  `fetch_reference_doget_ticket` (CO→CP, compute SA PAT) that obtains a
  `feature_idx`-scoped ticket at job runtime, and a streaming `stream_reference_chunks`
  context manager (CO→DP Flight DoGet) that registers the `(feature_idx, chunk_index,
  chunk_data)` stream into DuckDB for lazy, unbuffered reassembly. The live `compute`
  service account needs the `ticket:doget` scope (already within
  `SERVICE_ACCOUNT_SCOPE_CEILING`) to mint the ticket. (#268)

- **`docs/runbooks/pacbio-ingest.md`** — the operational knowledge from the first
  production PacBio ingest, which until now lived only in a gitignored local file.
  Covers the pre-flight `.db` read-write trap (pool identity is the SHA-256 of the
  blob's bytes, so submitting against an unpatched file and re-running mints a
  *second* pool), why `--prep-protocol-idx` is not validated against the platform,
  why `--force` must never be used to retry an ingest, and why `pool-completion`
  reports `fully_processed: false` permanently for a PacBio pool. (#296)

- **A read-discipline rule in `CLAUDE.md`.** Several files here run past 4,000 lines;
  reading one whole costs more context than every instruction file in the repo
  combined. Nothing previously said so. (#296)

- **The pool roster now reports what host filtering each sample WOULD get (#294).**
  Every item in `GET .../sequenced-pool/{idx}/sequenced-sample/list` carries a
  `host_filter` block — `filter` (with the resolved rype/minimap2 references),
  `pass_through`, `control`, or `unresolved`, each with a human-readable reason —
  resolved from the sample's own `host_taxon_id` metadata plus the run's platform.
  Read-only: nothing acts on it, and the submit path still reads the intake
  `human_filtering` flag. Both are exposed side by side on purpose, because they
  answer the same question from opposite ends (recorded intent vs. the sample's own
  metadata) and will disagree until the metadata is backfilled — which is exactly
  what an operator needs to see before that switchover.

- **`GET /host-filter-profile` — the catalog of what we can deplete (#294).**
  Optionally narrowed by `?platform=`. This is the menu that makes an override
  well-defined: you cannot sensibly force a sample onto a host profile without
  first being able to see which profiles exist. Gated to `wet_lab_admin` with the
  existing `reference:read` scope — a profile names no sample, only which reference
  builds deplete which host, so it is reference config.

- **`resolve_host_filter_many` — whole-pool resolution in two queries (#294).**
  Live pools run to hundreds of samples, so resolving one at a time would turn a
  single roster GET into ~1300 round trips. The batch path shares its classification
  core with the single-sample path, so a roster and a per-sample submit cannot
  disagree about the same sample.
- **PacBio case-5 read-mask chain.** The read-mask workflow gains two optional,
  `when:`-gated stages around its always-on QC and host filter, so one workflow
  serves all five PacBio protocols and Illumina unchanged:
  `syndna? → [lima_export → lima → lima_mask]? → qc → host_filter`.
  - **SynDNA spike-in marking (syndna)** runs FIRST, on the RAW reads. In case 5
    (`syndna_is_twisted == False`) the spike-ins are added *after* Twist
    amplification and so carry no Twist adaptor: if lima ran first it would mask
    them `twist_no_adaptor`, and because every later step only re-classifies rows
    still `pass`, the spike-in count would be **structurally zero**. Marking them
    up front also makes `twist_no_adaptor` a correct "artifactual" signal on the
    reads that remain. A read is a spike-in when it has a **PRIMARY** alignment to the
    SynDNA reference (minimap2, `map-hifi`) at ≥ 0.95 identity, computed by miint's
    `alignment_seq_identity` — an identity floor host filtering does not need, because
    a spike-in call is a claim about a read's ORIGIN: a false positive silently removes
    a genuine biological read from `biological`. The primary-only rule is there for the
    same reason: identity is scored per alignment ROW and then DISTINCT'd to a read, so
    without it a single short high-identity SUPPLEMENTARY segment marks a whole read —
    the local-alignment false positive a chimeric HiFi read produces. coverm does not
    credit a reference on a supplementary alignment either (measured, coverm 0.8.0), so
    excluding them is what porting its spec requires. Both rules, plus the coverage floor
    deliberately NOT applied and the open questions that belong to the assay owner, are
    argued at the job's constants; both are folded into the read-mask identity, so a
    change to either re-mints rather than silently reusing a mask built under the old
    rule. Spike-in reads are RETAINED in
    `read_mask` (their `sequence_idx` survives), so a later per-insert quantification
    needs no re-ingest. Two notes on `spikein_read_count_r1r2`: because a non-`pass`
    verdict is carried verbatim through the rest of the chain it is a **raw-space**
    count, not a QC'd / host-depleted one; and it is a **masking diagnostic** (it makes
    the read accounting balance), NOT the cell-count model's input — that is per-insert
    coverage depth, a different quantity. (#270)
  - **Adapter removal (lima)** runs before QC, so lima sees the intact adaptor and
    QC's length filter judges the insert. The Twist adapter FASTA is vendored
    into the lima image rather than loaded as a reference: lima is invoked with
    `--neighbors`, which only keeps barcode pairs adjacent in the FASTA, and the
    reference store cannot round-trip an ordered sequence set (no ordinal, no
    record name, and a revcomp-canonical `feature_idx` under a
    `(reference_idx, feature_idx)` PK). Reads with no Twist adaptor are masked
    `twist_no_adaptor`. Trims come from miint's `infer_trim`, not from parsing
    lima's output. (#270)
  - `qc` gained an optional incoming partial mask, extending it with trims that
    stay **cumulative from the raw read**.
- **`sequenced_sample.spikein_read_count_r1r2`** and a `spikein` bucket on the
  pool read-metrics rollup. A spike-in is added in the lab, so it is disjoint
  from `biological`. (#270)
- **Review follow-ups on the read-mask chain.** The two incoming-partial-mask guards that
  re-checked the mask's SHAPE at every consumer (one row per read; trims within the read) are
  REMOVED: both producers establish those invariants by construction — `syndna` emits `reads LEFT
  JOIN hits` over a DISTINCT hit set, and `lima_mask`'s `infer_trim` returns one row per original
  read and fails loud on a non-substring — so the checks guarded our own code against itself. The
  invariants are now pinned at the producers, where they are actually established. The guard that
  REMAINS is the one our construction does not establish: the `syndna_enabled` / `lima_enabled`
  gates are client-supplied, so a submission can still ask for the long-read chain over a
  paired-end read set. `lima_mask`'s check on lima's own output also remains — lima is an external
  container binary, and `infer_trim` would absorb a broken contract silently. (#270)

- **`qiita submit-host-filter-pool --syndna-reference-idx`**, and per-sample gate
  derivation for PacBio pools: `lima_enabled`, `syndna_enabled`, and per-sample
  `host_filter_enabled` are read back from the pool's stored pre-flight blob. The
  SynDNA reference carries a **minimap2 (`.mmi`)** index — the same index type the
  host filter's minimap2 arm uses, so no new builder or index type is needed. (#270)
- **PacBio protocol facts on the sequenced-sample roster** (`sheet_type`,
  `twist_adaptor_id`, `syndna_is_twisted`), derived at request time from the
  stored pre-flight — the same single-source-of-truth path `human_filtering`
  already used. (#270)
- **`compute-readiness` probes `infer_trim`**, invoking the macro rather than
  checking registration. A stale `extension_directory` (a plain `INSTALL` never
  refreshes a warm cache) otherwise yields a build with every other function
  present and fails at the first real submit. (#270)

- **`qiita.host_filter_profile` — the config layer mapping a host taxon + platform to the
  reference build(s) to deplete against (#293).** It keeps *which organism* on the sample
  (biosample metadata, the terminology-typed `host_taxon_id` field) and *which build* in
  config, so rebuilding the host DB repoints existing samples instead of rewriting them.
  Stage 1 (rype) is required; stage 2 (minimap2) is optional, and NULL means the profile
  stops after stage 1. `UNIQUE (host_term_idx, platform)` means a rebuild UPDATEs the
  existing row rather than racing a second one, so the lookup is unambiguous by
  construction. The migration ships the table EMPTY — its rows point at `reference_idx`
  values that exist only on a live deploy, so seeding is an operator step (see
  `DEPLOY_CHECKLIST.md`), not a migration INSERT with nothing to reference in a fresh
  test DB.

- **The two host-filter stages are keyed on platform, not constrained by it (#293).**
  Which stages a given (host, platform) wants is an assay decision, so the schema does not
  freeze today's answer into a CHECK — revisiting it would then cost a migration. The
  `rype` stage being required, and `minimap2` optional, describes the profiles we actually
  run (illumina, pacbio_smrt); it is not a claim about what any aligner can or cannot do.
  Whether rype is usable on a high-indel platform such as ONT is an open question — if it
  is not, an ONT profile would need to be minimap2-only, which is the one case the current
  `rype NOT NULL` would have to relax.

- **`host_filter_resolver.resolve_host_filter` — resolves what host filtering one
  biosample should get (#293).** Given a biosample and a platform it returns FILTER (with
  the references), PASS_THROUGH (`not applicable` — a water sample deliberately has no
  host), CONTROL (a blank, whose filtering is a pool-level fact, so the resolver reports
  it and stops), or UNRESOLVED. It returns reference *identity*, not on-disk readiness —
  ACTIVE/index-built checks stay in the runner. Nothing calls it yet.

- **The resolver fails closed on anything ambiguous (#293).** An absent `host_taxon_id`, a
  host with no build on the platform, and an uninformative missing-reason such as `not
  collected` all abort rather than degrade to "no filtering". `not collected` and `not
  applicable` are both missing-reasons, and quietly reading the first as the second is
  precisely how an un-depleted human sample would slip through.

- **`qiita submit-pacbio-ingest` — one-gesture PacBio HiFi ingest.** The PacBio
  analogue of `submit-bcl-convert`: it reads a kl-run-preflight blob, stands up
  the `sequencing_run` (platform `pacbio_smrt`) / `sequenced_pool` (blob attached)
  / `sequenced_sample` roster, and — because PacBio arrives already demultiplexed
  (one uBAM per barcode) — FANS OUT one existing `bam-to-parquet` ticket per
  sample rather than a single pool-scoped demux ticket (no new job or workflow).
  Each sample's identity (and `sequenced_pool_item_id`) is its `pacbio_sample_idx`,
  the parallel of `illumina_sample_idx`; the barcode only LOCATES the sample's BAM
  under `{run_folder}/{smrt_cell}/hifi_reads/` (it is not unique across all PacBio
  protocols), and a barcode reused across SMRT cells fails loud rather than
  silently binding the wrong cell's reads. Per-sample facts (biosample + ENA
  bioproject accessions, barcode, twist/syndna columns, and the SMRT cell) come
  from kl-run-preflight's `get_pacbio_sample_info` (the PacBio analogue of the
  Illumina reader; run-preflight pin bumped to the merged PR that adds it); the
  project's `human_filtering` intent is read from the canonical `run_pacbio_sample`
  view (tracked for removal in #271, with the `sheet_type` dependency in #272).
  Studies resolve on the **bioproject** accession like the Illumina path (the
  earlier draft keyed on the QiitaID, which the study lookup route cannot match).
  The SMRT cell rides onto each row; keying `(smrt_cell, barcode)` BAM
  disambiguation off it is a follow-up. Verified against kl-run-preflight's own
  `good_pacbio_absquantv11.csv` case-5 fixture. (#260)
- **`derived_inputs` on container workflow steps** — the container-side mirror
  of the native-only `params`. A step declares `derived_inputs: {ENV_VAR:
  <path relative to PATH_DERIVED>}`; the orchestrator joins each value against
  its own `PATH_DERIVED`, bind-mounts the result, and forwards the absolute path
  under that env var name. This is the only way an operator-provisioned artifact
  too large to bake into a SIF (CheckM's ~1.4 GB DB) can reach a container:
  apptainer runs `--containall`, so an unforwarded host env var is invisible
  inside it. Values stay **relative** on the wire — the control plane never names
  a compute-node absolute path — and both the wire validator and the backend
  reject an absolute path or a `..` escape, so a workflow cannot name an
  arbitrary host directory for the orchestrator to bind in. (#273)

- **Seed water/marine metadata for early data entry** — the `GSC MIxS water`
  checklist (ERC000024, under the ENA default); the `depth_m` and
  `host_taxon_id` biosample global fields (the latter terminology-bound to NCBI
  Taxonomy, required); the `seawater metagenome`, `estuary metagenome`, and
  `Homo sapiens` NCBI taxa; and eight marine/aquatic ENVO environmental-context
  terms. (#267)

- **Key-rotation runbook** (`docs/runbooks/key-rotation.md`) — restart-based
  rotation for the Ed25519 Flight signing keypair and the login-cookie secret,
  with a `make preflight` keypair check before the coordinated CP+DP restart.
  `first-deploy.md` provisioning and `auth.md` updated to match the keypair
  model. (#265)

- **`long-read-assembly` workflow — per-sample PacBio HiFi assembly → MAG
  recovery** (a port of qp-pacbio pipeline B). Runs on a prep_sample's masked
  reads, selected by a required `mask_idx`: the runner's
  `_resolve_staged_masked_reads` STREAMS the `read_masked` pass-set from the data
  plane (the existing `read_masked` DoGet — **no bespoke DoAction or payload**) to
  a gzip FASTQ via miint's native `COPY … FORMAT FASTQ` (the `masked_reads_fastq`
  binding, the same capability the admin masked-read export uses), so no
  intermediate Parquet and the data plane never writes a file. It enforces the
  `mask_sample` completion gate (a partially-masked sample is rejected, not
  assembled from a partial pass-set) and treats a fully-masked-out sample as a
  terminal NO_DATA. A native step records the assembler; four container steps run
  hifiasm_meta → metawrap binning → DAS_Tool → CheckM, each in its OWN per-tool
  image (`long-read-assembly-{assemble,binning,dastool,checkm}`, one micromamba env
  each) so a change to one tool's solve rebuilds only that image. CheckM requires
  its reference DB — the `checkm` step fails loud without it (a required deploy
  step) — and DAS_Tool fails loud on a real crash while treating a genuine no-bins
  result as a benign empty. The storage tail REUSES the reference-add pipeline
  rather than a bespoke parser: `assembly_hash` reads the LCG + MAG contigs with
  miint `read_fastx` (circular contigs arrive as one multi-FASTA, no per-contig
  split) and emits the manifest + hash-keyed chunks + bin_map, `mint-features`
  mints the contig features against the SHARED `qiita.feature` (identical bytes
  collapse to one feature_idx — an assembled contig and a reference sequence share
  identity), `write-assembly-membership` links them to `qiita.assembly_membership`,
  and `assembly_load` (reusing reference_load's re-key writers) + register-files
  load four DuckLake tables — `assembled_sequence` / `assembled_sequence_chunks`
  (feature-keyed contig sequences), `assembly_membership` (which features each
  (prep_sample, run, bin) contains), and `bin_quality` (per-MAG CheckM + DAS_Tool
  provenance, read with DuckDB's CSV reader). Each run has a `processing_idx`
  identity (deduped on the canonical params hash — workflow/version/mask_idx/
  assembler, so a different mask's pass-set is a distinct run that never collides a
  prior run's bins). The step-1 assembler is a parameter (`hifiasm_meta` default;
  `myloasm` reserved), threaded through the native steps' `params:` (a container
  step can't take a scalar). Empty-branch semantics: LCG-only samples store
  successfully, zero-contig samples are a terminal NO_DATA; sub-512 kb circular
  contigs (plasmids / small replicons) are kept as LCG and separated from
  chromosome-scale genomes by length at query time rather than deleted. The
  cross-sample dereplication / taxonomy / abundance stage
  (galah/gtdbtk/GToTree/coverm) is a separate follow-on that reads these per-sample
  results across many preps. (#255)
- **`bam-to-parquet` workflow — load a sample's BAM into the `read` table.** The
  BAM analogue of `fastq-to-parquet`, structurally near-identical: a single native
  `bam` step reads the file with miint's `read_sequences_sam` (which emits a
  `read_fastx`-compatible schema, so `sequence_idx` comes from its per-file
  `sequence_index` just like the FASTQ path), keeps only the read payload
  (read_id, sequence, quality), mints a `sequence_idx` range, and writes
  `read.parquet` that a `register-files` step loads into the existing DuckLake
  `read` table. A plain read loader — it discards all alignment fields and every
  aux tag (methylation MM/ML, kinetics). It targets an unaligned basecaller uBAM:
  the caller declares `expect_unaligned` (default true), which the job trusts
  (an `expect_unaligned=false` ticket is rejected — aligned loading is
  unsupported; verifying the FLAGs can be layered on later); a duplicate-QNAME
  guard rejects a paired uBAM so one read never gets two `sequence_idx`. No new
  table, migration, container, or env var. The
  `PreMintedRange` retry-recovery model moved from `jobs/fastq_to_parquet.py` to
  `sequence_range.py` so both read-ingest jobs share it. (#254)
- **`export_read_block` DoAction** — the block-compute sibling of `export_read`,
  the first piece of bulk-block read masking. The data plane materializes the
  UNION of a block's `(prep_sample_idx, sequence_idx sub-range)` members from its
  DuckLake `read` table into one per-ticket `reads.parquet`. The selector is
  exact by construction (a per-member `prep_sample_idx = p AND sequence_idx
  BETWEEN start AND stop` predicate — a split sample never leaks a sibling
  block's rows) while a coarse `prep_sample_idx IN (...) AND sequence_idx BETWEEN
  block_min AND block_max` pair drives DuckLake file-level pruning (measured
  scale-invariant: a fixed block reads only its own files as the table grows 5×;
  the coarse pair is load-bearing — the per-member `OR` alone would full-scan). New
  `ExportReadBlockPayload`/`verify_export_read_block` (Rust) and
  `_do_action_export_read_block` / `_resolve_staged_reads_block` (control plane);
  `export_read` and `_do_action_export_read` refactored to share
  `export_read_where_to_parquet` / `_do_action_export`. No caller yet (wired in a
  later block-compute phase); no new env var, migration, scope, or operator
  action. (#243)
- **Block-compute schema — `block`, `block_member`, `mask_sample`, and a `block`
  work-ticket scope.** The persistence layer for bulk-block read masking. A
  `block` is the compute unit (a fixed ~10M-read slice from prep_samples sharing
  one `mask_idx`, run as one work ticket); `block_member` is the block↔sample
  cover-map (`[min_sequence_idx, max_sequence_idx]` per sample); `mask_sample`
  is the per-`(mask_idx, prep_sample)` completion gate the masked-read export
  path will consume (absence of a `read_mask` row must never read as "pass").
  `qiita.work_ticket` gains a nullable `block_idx` scope arm, a `block`
  `scope_target_kind` value (Python twin `ScopeTargetKind.BLOCK` +
  `BlockScopeTarget`), and a `work_ticket_one_in_flight_per_block` partial unique
  index. New `repositories/block.py` (`create_block`, `add_block_members`,
  `set_block_state`, `set_block_work_ticket`, `create_mask_sample_pending`). The
  planner and runner that drive these are in this same PR (below). (#243)
- **Block planner + `submit-block-mask-pool` — plan a whole pool as fixed
  ~10M-read blocks in one call.** The bulk-block analog of the per-sample
  `submit-host-filter-pool` fan-out. A new server-side planner
  (`block_planner.py`) resolves each sample's `mask_idx` at submit time (shared
  `"read-mask"` identity, so a block-masked sample and a per-sample read-mask of
  the same config collapse to one mask), partitions the pool by mask, tiles each
  partition into ≤~10M-read blocks (pure arithmetic over `qiita.sequence_range`,
  splitting a straddling sample on exact boundaries), then persists the
  `block`/`block_member` cover-map + a PENDING `mask_sample` gate per sample,
  creates one block work-ticket per block, and dispatches each. Exposed as
  `POST /sequencing-run/{R}/sequenced-pool/{P}/block-mask-plan` (wet_lab_admin +
  `prep_sample:write`, `BlockMaskPlan{Request,Response}` models) and the
  `qiita submit-block-mask-pool` CLI, which reuses the same host-ref /
  intent-mismatch preflight as `submit-host-filter-pool` (factored into a shared
  helper) but makes a single call instead of N. The block runner path and
  `read-mask-block` workflow that execute the dispatched tickets are in this same
  PR (below); no new env var, migration, or scope. (#243)
- **Block runner path — `read-mask-block/1.0.0` workflow + reconcile.** Wires the
  block tickets the planner mints (previously they `RuntimeError`ed): the runner
  gains a `block` scope arm that binds `reads` from the block's `block_member`
  sub-ranges (via `export_read_block`) and `mask_idx` from the ticket (plan-time,
  never re-minted). The new `workflows/read-mask-block/1.0.0.yaml` reuses the same
  `qc` + `host_filter` native modules as `read-mask` (steps qc → host_filter →
  register-files → reconcile-block). `host_filter` now stamps each `read_mask`
  row's `prep_sample_idx` PER ROW from the reads parquet (was a per-run scalar) so
  one multi-sample block records each read's true owner — a strict generalization
  (a single-sample block is byte-identical to the per-sample path); `qc`/`host_filter`
  `Inputs.prep_sample_idx` becomes optional (a block flows no such scope scalar).
  New `reconcile-block` library primitive: in one transaction it marks the block
  completed, then finalizes each covered sample once ALL its covering blocks are
  done — a per-sample `mask_sample` `FOR UPDATE` lock serializes concurrent block
  finalizers so exactly one wins, and the per-stage read counts are rolled onto
  `sequenced_sample` from a new `mask_metrics` DoAction (the DuckLake aggregate
  across the sample's blocks, replacing the per-sample path's local-parquet
  rollup) with a fail-loud count assertion against `sequence_range`. No new env
  var, migration, or scope; the `read-mask-block` workflow syncs via
  `qiita-admin actions sync` at deploy. (#243)
- **Masked-read export gate + block re-plan disallow-without-delete.** Two new
  invariants that block masking requires (a sample's mask is now assembled by
  several blocks, so real partial states exist). The masked-read export ticket
  route (`POST /admin/masked-read-export/ticket`) now refuses (409) to mint a
  DoGet ticket for a `(prep_sample, mask_idx)` whose `qiita.mask_sample` gate is
  not `completed` — a partially-masked sample would silently truncate on pull.
  A sample with NO gate row (the per-sample read-mask path, or an unmasked
  sample) is unaffected, preserving that path's all-or-nothing guarantee. The
  export manifest (`GET …/masked-read-export`) surfaces each sample's `mask_state`
  (`MaskedReadExportSample.mask_state`) so the CLI reports skips before minting
  tickets. The block planner (`plan_and_submit_blocks`) refuses (409, new
  `BlockMaskResubmitError`) a fresh (`only_missing=False`) re-plan of a pool whose
  samples already carry a `mask_sample` gate for the resolved mask — whether
  COMPLETED (re-masking double-writes `read_mask`; DuckLake has no uniqueness) or
  still PENDING (a prior plan's covering block is in-flight or failed, so minting
  a fresh same-footprint block would wedge finalize forever); the operator DELETEs
  the mask or passes `only_missing=true` (mirrors the sequenced_pool
  resubmit rule). No new env var, migration, scope, or operator action. (#243)
- **Idempotent block replace — `delete_read_mask_block` DoAction + `delete-block-mask`
  workflow step.** Makes a block re-run self-cleaning so `read_mask` never
  double-counts (DuckLake is append-only with no unique key). The new
  `delete_read_mask_block` DoAction deletes exactly one block's footprint — the
  `read_mask` rows for the ticket's `mask_idx` whose `(prep_sample_idx,
  sequence_idx)` fall in the block's member sub-ranges, using the SAME
  exact-by-construction selector as `export_read_block` (per-member `OR` residual,
  so a split sample's sibling-block rows survive). The `read-mask-block/1.0.0`
  workflow gains a `delete-block-mask` `action:` step immediately BEFORE
  `register-files` (steps are now qc → host_filter → delete-block-mask →
  register-files → reconcile-block): a fresh block deletes 0 rows, a re-run (retry,
  or an operator resubmit covering the same footprint) deletes-then-re-registers so
  exactly one copy lands and the reconcile count-assertion holds. New Rust
  `DeleteReadMaskBlockPayload`/`verify_delete_read_mask_block` + `delete_read_mask_block`;
  control-plane `delete_read_mask_block_data` DoAction wrapper + `delete_read_mask_block`
  library primitive + runner block-scope dispatch arm; `LibraryPrimitive.DELETE_READ_MASK_BLOCK`.
  End-to-end coverage in `tests/integration/test_read_mask_block_e2e.py` (split-sample
  per-sample reconcile + export gate, and the delete-then-register no-duplicate
  guarantee against a live data plane). No new env var, migration, or scope; the
  updated `read-mask-block` workflow syncs via `qiita-admin actions sync` at deploy. (#243)
- **Email notification on work-ticket terminal transitions.** When a work
  ticket reaches a terminal state (`completed` / `no_data` / `permanent`-failed),
  the control plane emails the originator. A new in-process asyncio sweeper
  coalesces a user's finished tickets into one digest via a trailing-debounce
  with a max-wait cap, gated on `qiita.user.receive_processing_emails`. Sends
  go through a pluggable transport (`aiosmtplib` SMTP relay when `SMTP_HOST` is
  set, else a no-op) and every send writes a `qiita.email_receipt` audit row.
  The digest footer carries the configured `CONTACT_EMAIL` (also set as the
  message `Reply-To`) as a contact line. Retriable failures are withheld until
  their true outcome, and a `/run` redrive re-arms notification. New `SMTP_*` /
  `NOTIFY_*` settings (all defaulted); migrations add
  `work_ticket.notified_at` / `notify_attempts`, the
  `qiita.email_receipt` table, and a partial owed-set index. (#238)
- **Optional `plan()` phase for native jobs — input-driven resource sizing.** A
  native job module may now export an optional `plan(inputs) -> JobPlan` (a
  third symbol alongside `Inputs` + `execute`; absent → today's behavior). The
  control-plane runner fetches the hint ONCE per native step before its retry
  loop via a new backend-agnostic `POST /step/plan` route
  (`ComputeBackendClient.plan_step`), and composes it in
  `_resolve_baseline_for_step` as a raise-NEVER **down-size**: a hint lowers a
  step below its YAML baseline, is applied before the raise-only escalation
  floors (so an OOM/TIMEOUT retry always restores at least the baseline), and is
  fully advisory (any failure — unreachable CO, broken module, malformed
  response — degrades to the baseline). First consumer: the `qc` step sizes its
  **walltime** (not memory — qc streams, so peak RAM is ~flat in read count)
  from the input read count, tightening SLURM backfill for small inputs. New
  `JobPlan`/`JobResourcePlan` contract types, `run_native_job_plan` dispatcher,
  `job_resource_plan` helpers, `StepPlanRequest`/`StepPlanResponse` wire models,
  and `PATH_/URL_STEP_PLAN` constants; `docs/writing-a-job.md` documents the full
  native-job contract. No new env var, scope, migration, or operator action. (#237)
- **CLI discovery commands for prep-protocol and host-reference idxes.** Two new
  read-only subcommands so operators stop hand-querying Postgres for the idxes
  `submit-bcl-convert` / `submit-host-filter-pool` need. `qiita prep-protocol
  list` (`--all` to include retired) is backed by a new anonymous-OK `GET
  /prep-protocol` route (same posture as `GET /reference`; retired protocols
  excluded by default). `qiita reference list` (`--host` / `--active` /
  `--index-type {rype,minimap2}`) reuses the existing `GET /reference` plus a
  per-row `GET /reference/{idx}/index`, enriching each reference with its built
  `index_types` and filtering by `--index-type` so the result is exactly the set
  `submit-host-filter-pool`'s `_assert_host_reference_ready` gate accepts. New
  `PrepProtocolResponse` model + `PATH_/URL_PREP_PROTOCOL` constants; anonymous-OK
  so no new scope, migration, or operator action. (#232)
- **Walltime escalation on TIMEOUT retry**, mirroring the existing OOM→memory
  growth. When a step's SLURM job exceeds its walltime (`TIMEOUT`, a retriable
  kind), the runner now grows that step's walltime floor ×2 on each retry,
  clamped to `action_ceiling.walltime`, instead of re-running every attempt at
  the same limit (which timed out identically). Process-local like the memory
  floor: a CP restart re-attaches to the in-flight job and re-escalates from the
  YAML baseline. (#216)
- Pool completion now reports **end-to-end processing**, not just host-masking.
  `GET /sequencing-run/{R}/sequenced-pool/{P}/completion` (`qiita pool-completion`)
  gains `demux_state` (the pool-scoped bcl-convert stage: completed / in_flight /
  no_data / failed / not_submitted) and a computed `fully_processed` (demux
  completed AND every sample's read-mask `complete`) — the single "this pool is
  done and clean" signal. Also corrects the route/repo/`api_paths`/CLI docstrings,
  which described the rollup as "fastq-to-parquet / prep-generation" though it has
  measured **read-mask** (host-masking) since the read-storage/masking split. No
  new route/migration; the `PoolCompletionStatus` response gains two fields. (#218)
- Admin per-pool **masked-read export**: pull a sequenced_pool's masked sequence
  data to local disk, per sample, as parquet or fastq. New `qiita-admin
  masked-read-export --sequenced-pool-idx P --mask-idx M [--format parquet|fastq]
  --output-dir DIR --data-plane-url U` CLI, backed by two routes — `GET
  /api/v1/admin/sequenced-pool/{idx}/masked-read-export?mask_idx=` (roster
  manifest) and `POST /api/v1/admin/masked-read-export/ticket` (per-sample DoGet
  ticket). The CLI streams each sample's `read_masked` rows straight from the data
  plane into a local DuckDB+miint `COPY` (bounded memory, no server-side scratch),
  writing `<biosample_accession>.<run>.<pool>.<prep>` files atomically at mode
  0600: `.parquet`, or `.fastq` (single-end) / `.R1.fastq`+`.R2.fastq` (paired,
  via miint's `{ORIENTATION}`; pairing is detected by peeking the first streamed
  batch, so the stream is never materialized). Dual-gated by `system_admin` + a new
  `admin:masked_read_export` scope. Privacy invariant unchanged: the `read_masked`
  view (`WHERE reason='pass'`) is the only Flight-reachable read surface, so
  host/QC reads are never exported. The data-plane DoGet now streams its result
  set instead of buffering it whole. (#192)
- `qiita run-preflight update-lane` — wet_lab_admin+ correction of a stored run
  preflight's lane assignment. New `POST /api/v1/sequencing-run/{R}/sequenced-pool/{P}/preflight/update-lane`
  route loads the pool's run-preflight SQLite blob, applies `run_preflight.update_lane`
  (bulk `from_lane` → `to_lane` reassignment on the illumina/tellseq sample table,
  one `change_log` audit row per reassigned sample), and writes the blob back — all
  server-side, so the SA-only "humans can't read the preflight" boundary is
  preserved. Gated on the run not having been processed: an in-flight or completed
  work ticket on the pool or its samples → 409 (a failed or unsubmitted run stays
  editable, since a stale lane may be why it failed); update_lane's
  uniformity/collision `ValueError` → 422. Reuses the existing pinned `run-preflight`
  dependency (no version bump). (#190)
- New `GET /api/v1/admin/study/{study_idx}/owner-biosample-id` route + `qiita-admin
  owner-biosample-id` CLI: a system_admin-only re-identification export mapping a
  study's `biosample_idx` + `biosample_accession` back to the owner-submitted
  original sample name (the PII-pinned `biosample_metadata` value flagged
  `is_owner_biosample_id`, masked on every other read path). With
  `?sequenced_pool_idx=` it restricts to that pool's `sequenced_sample`s in the study
  and adds `prep_sample_idx` + ENA experiment/run accessions. Dual-gated by
  `system_admin` + a new `admin:biosample_owner_id_read` scope; the CLI writes a TSV
  to `--output` (mode 0600, never stdout, so the names stay off the terminal). (#188)
- An `export_read` data-plane DoAction that re-materializes one prep_sample's
  reads from the DuckLake `read` table into a per-ticket `reads.parquet` on shared
  scratch (DuckDB `COPY` run on the blocking pool, written to a `.partial` sibling
  then published atomically; the destination is validated lexically and via a
  symlink-resolving containment check under the scratch root; row count is read
  back from the written file). The control plane signs the HMAC action token but
  the data plane writes the file, so the bulk (human-containing) read bytes never
  transit the control plane. Raw `read` remains absent from the Flight DoGet
  `ALLOWED_TABLES` — it is reachable only via this admin-gated write path. (#187)
- A `runner._do_action_export_read` control-plane client for the above. (#187)
- A `delete_mask` primitive for removing a registered read mask. New
  `mask_definition:delete` scope (system_admin via the role ceiling),
  `DELETE /api/v1/mask-definition/{mask_idx}` route (lake-first: a new
  `delete_mask` data-plane DoAction logically `DELETE`s the mask's rows from the
  DuckLake `read_mask` table, then the `mask_definition` Postgres row is removed),
  and a `delete_mask_data` CP client. Idempotent (0 rows deleted is success); no
  raw parquet unlink (mirrors `delete_reference`); 404 on an absent mask. Surfaced
  as `qiita-admin mask delete <mask_idx>`. (#181)
- `qiita-admin mask purge-failed --action {read-mask,fastq-to-parquet,all}` — bulk
  recovery tooling that selects FAILED read-mask / fastq-to-parquet tickets stranded
  by the move-then-read ordering bug, deletes each ticket's orphaned mask, and
  resubmits it clean on the reordered workflow (so the re-run mints a fresh
  `mask_idx` rather than appending a duplicate to the append-only `read_mask`
  table). Dry-run by default; `--execute` to act, `--with-tickets` to also delete
  the FAILED ticket rows, `--limit` / `--rate` / `--wait` to bound and throttle the
  batch. A shared-mask guard refuses to delete a mask referenced by any non-FAILED
  ticket, and a pre-flight refuses to run if the `work_ticket.mask_idx` backfill is
  incomplete. (#181)
- `qiita-admin work-ticket backfill-mask-idx [--apply]` — one-time idempotent
  backfill that populates the new `work_ticket.mask_idx` column for existing
  read-mask / fastq-to-parquet tickets by recomputing the mask params hash and
  looking it up in `mask_definition` (no new mask minted). For adapter-bearing
  tickets it re-materializes the canonical adapter set via DoGet to reproduce the
  hash, so it needs `DATABASE_URL`, `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX`,
  `HMAC_SECRET_KEY`, and a reachable `DATA_PLANE_URL`; the dry-run reports
  `populated` so the operator can confirm `populated > 0` before `--apply`. (#181)
- New nullable `work_ticket.mask_idx` column (FK → `mask_definition`, ON DELETE SET
  NULL, partial index) recording the mask a read-mask / fastq-to-parquet ticket
  produced, for durable traceability and a cheap shared-mask guard. The runner
  writes it at mint time; existing rows are populated by the
  `backfill-mask-idx` command above (migration
  `20260624110000_work_ticket_mask_idx.sql`; additive, backfill-free at migrate
  time — existing rows read NULL). (#181)
- A first-class terminal `no_data` outcome for empty FASTQ wells, distinct from
  failure, so a real plate full of blank / no-template-control / failed-yield
  wells can still reach a "done" signal. New `WorkTicketState.NO_DATA` enum value
  (additive `ALTER TYPE ... ADD VALUE 'no_data'` migration + the Python twin;
  `WorkTicketState` already in `ENUM_PAIRS`). `fastq_to_parquet` on empty input
  now raises a new typed terminal `StepNoData` signal (in
  `qiita-common/backend_failure.py`, parallel to `BackendFailure` — its own wire
  body + `X-Qiita-Step-No-Data` header round-tripping the `/step/*` boundary, NOT
  a `FailureKind`) instead of minting a sequence range or writing `read.parquet`;
  it mints no identifiers and writes no output. The dispatcher re-raises
  `StepNoData` unchanged (above the generic `ValueError → BAD_INPUT` arm), both
  backends round-trip it, and the runner transitions the ticket `PROCESSING →
  NO_DATA` with NULL failure columns — no `failure_status` PATCH, no
  `success_status` advance, transient markers cleared. `NO_DATA` is terminal for
  resubmission (DELETE-gated) and the `/run` redrive (409). (#176)
- A `prep_sample` retire surface so an operator can disposition a sample (drop an
  empty / failed-yield well out of a pool's active set) without raw SQL. New
  reversible `PATCH /api/v1/prep-sample/{idx}/retired` (gated on
  `Scope.PREP_SAMPLE_WRITE` + the wet_lab_admin role the prep_sample read route
  uses; `retired=false` un-retires a misclassified well) plus `qiita prep-sample
  retire` / `qiita prep-sample un-retire` CLI subcommands. The `prep_sample.retired`
  column + CHECK already existed and the completion rollup already excludes retired
  rows. (#176)
- Producer cutover for the full-read+mask feature (PR 3). The orchestrator now
  PRODUCES the reads and masks the DuckLake tables consume, replacing the
  destructive host/QC read-dropping. `ReadMaskReason` (a `qiita-common`
  `StrEnum`: `pass` / `qc_too_short` / `qc_too_long` / `qc_low_quality` /
  `qc_too_many_n` / `host_rype` / `host_minimap2`; backs a DuckLake VARCHAR, not
  a Postgres ENUM, so no `ENUM_PAIRS` entry). `fastq_to_parquet` writes the full
  reads with a `prep_sample_idx` column (sorted `(prep_sample_idx,
  sequence_idx)`) and exposes a staging dir so a `register-files` step loads them
  into the DuckLake `read` table. `qc` stops dropping reads and emits a partial
  mask (`sequence_idx`, reason, per-end trims) via `filter_read` fail-reason →
  `ReadMaskReason`. `host_filter` runs rype/minimap2 on the trimmed QC-pass
  subset, merges host hits into the QC mask under a privacy precedence (host
  wins over `pass`; QC-failed reads keep their `qc_*` reason), and registers the
  final `read_mask` (tagged with the CP-minted `mask_idx`) into the DuckLake
  `read_mask` table. The runner mints `mask_idx` before the step loop (host
  references from the sample's `sequenced_sample` row + the resolved QC config,
  deduped on a config hash) and threads it into `host_filter`. `persist-read-metrics`
  is re-sourced from the mask, counting `COUNT(*) + COUNT(right_trim2)` per
  reason bucket so paired-end `_r1r2` totals don't silently halve. New workflow
  `fastq-to-parquet/1.3.0` reflects the new step shape; the dead
  `qc_reads.parquet` / `filtered_reads.parquet` outputs are removed, and the new
  COPY/read_parquet/CREATE VIEW path literals route through
  `validate_parquet_path`. (#173)
- Data-plane read tables + masked-read view (PR 2 of the full-read+mask
  feature). The data plane now creates the DuckLake `read` and `read_mask`
  tables and the `read_masked` view at startup (`ensure_read_tables`, called
  alongside `ensure_reference_tables`; idempotent via `CREATE TABLE/VIEW IF NOT
  EXISTS`, the view is catalog-stored so it persists across DP restarts).
  `read_masked` joins `read` to `read_mask`, applies the recorded per-mate trims
  (`substr` on the sequence, list-slice on the `UTINYINT[]` qual), and excludes
  every non-`pass` row (`WHERE m.reason = 'pass'`), so host/human and QC-failed
  reads are unreachable by construction. `read_masked` added to the Flight
  `ALLOWED_TABLES`, and `mask_idx`/`prep_sample_idx` to `ALLOWED_FILTER_COLUMNS`;
  raw `read`/`read_mask` are deliberately NOT Flight-reachable. No producer of
  read data yet (PR 3). (#171)
- Read-mask identity + masked-read DoGet route (PR 1 of the full-read+mask
  feature). New `qiita.mask_definition` table + `qiita.mint_mask_definition`
  function mint a `mask_idx` identifying a read-filtering config, deduplicated on
  a canonical-JSON SHA-256 of the config so the same config resolves to the same
  `mask_idx` fleet-wide (idempotent upsert; no advisory lock). New
  `POST /api/v1/mask-definition` (mint) and `POST /api/v1/read-masked/ticket/doget`
  (signs an HMAC DoGet ticket scoped to a mandatory `(prep_sample_idx, mask_idx)`
  filter on the data plane's `read_masked` view — an unfiltered ticket is never
  signed). Both service-account-only under a new `read_masked:doget` scope.
  `read_masked` added to the CP-side DoGet table allowlist (the data-plane view
  itself lands in PR 2). New `qiita_common.hashing` canonical-hash helper. (#170)
- `qiita ticket run <idx>` CLI subcommand — wraps the existing
  `POST /work-ticket/{idx}/run` operator override (reset a FAILED ticket and
  re-dispatch; the only retry mechanism, no auto-retry worker). The runner
  fast-forwards already-COMPLETED steps and resumes at the first incomplete one,
  so an expensive finished step (e.g. `stage_local_fasta`) is not recomputed.
  Mirrors `qiita ticket status`; no server change (the route and api_paths
  constants already existed) (#157)
- Pool prep-generation completion rollup. New `GET
  /api/v1/sequencing-run/{run}/sequenced-pool/{pool}/completion` route (and
  `qiita-user pool-completion`) classifies each non-retired `sequenced_sample` by
  the state of its `fastq-to-parquet` work tickets (any version) and tallies
  `samples_completed` / `samples_in_flight` / `samples_failed` /
  `samples_not_submitted` over the pool, with a `complete` flag (every sample
  COMPLETED, pool non-empty). The SPP `GenPrepFileJob` end-state equivalent: it
  tells the operator whether the per-sample fan-out finished. Compute-on-read
  over the work tickets, so it never drifts when a sample is re-processed,
  re-submitted, or deleted. Read-gated like the other pool rollups
  (prep_sample:read + wet_lab_admin). Part of #146 (#158)

- Per-sample host-filter references. `sequenced_sample` gains two nullable FK
  columns (`host_rype_reference_idx`, `host_minimap2_reference_idx` → `reference`,
  with a CHECK that minimap2 only accompanies rype) recording which host the
  sample is depleted against — both NULL means no host filtering. They map 1:1
  onto `fastq-to-parquet/1.2.0`'s `host_rype_reference_idx` /
  `host_minimap2_reference_idx`, so a later pool fan-out is a pass-through; the
  reference being `(name, version)` pins the exact host build per sample, and a
  non-human host is just a different reference (no schema change). The
  sequenced-sample composer request/response and the pool/run sample-list items
  carry them. `qiita-user submit-bcl-convert` gains `--host-rype-reference-idx`
  (+ optional `--host-minimap2-reference-idx`): it reads each sample's project
  `human_filtering` flag from the preflight and records the host reference(s) on
  `human_filtering` samples (blanks/controls follow their project) while leaving
  `human_filtering=0` samples unfiltered, pre-flighting the references ACTIVE +
  indexed up front (#156)

- Merged (multiqc-equivalent) run-level QC report for a pool. `sequenced_sample`
  gains two nullable `jsonb` columns (`raw_qc_report`, `filtered_qc_report`)
  holding the per-sample `qc_report.json` documents; a new `persist-qc-report`
  library primitive — added as the final `action:` step of
  `fastq-to-parquet/1.2.0` — writes them from the `qc_report_raw` /
  `qc_report_filtered` sidecars (the same persist-from-sidecar pattern as
  `persist-read-metrics`). New `GET
  /api/v1/sequencing-run/{run}/sequenced-pool/{pool}/qc-report` route returns the
  pool's merged report: the read-metric rollup, every non-retired sample's
  persisted raw/filtered report, and a run-level `merged` aggregate (per-mate
  histograms summed across samples, means base/read-weighted). Compute-on-read —
  the merge runs at request time, so it never drifts when a sample is
  re-processed or deleted. Read-gated like the pool roster (prep_sample:read +
  wet_lab_admin). implements #145 (#154)
- New `qc_report` native job: a fastqc-equivalent per-sample QC summary computed
  in DuckDB straight from `reads.parquet` (no container, no miint extension). Per
  mate (r1/r2) it reports read/base counts, mean quality, GC and N content,
  length stats, and per-sequence mean-quality / GC-percent / length histograms,
  written as a `qc_report.json` sidecar. Wired into `fastq-to-parquet/1.2.0` as
  two steps mirroring SPP's bclconvert/filtered_sequences split — `qc_report_raw`
  (on the raw fastq output, before qc) and `qc_report_filtered` (on the
  host-filtered output) — sharing one module, disambiguated by input/output
  binding (`reads`→`raw_qc_report`, `filtered_reads`→`filtered_qc_report`). The
  artifacts feed the upcoming merged-report step; reporting only, no filtering
  change (#152)

- New `GET /api/v1/sequencing-run/{run}/sequenced-pool/{pool}` route returning a
  pool's metadata plus a compute-on-read read-metric rollup (#143): per-stage
  read-count SUMS over the pool's non-retired `sequenced_sample` rows
  (`raw_read_count_r1r2` / `biological_read_count_r1r2` /
  `quality_filtered_read_count_r1r2`), a `fraction_passing_quality_filter`
  recomputed from the sums (not a mean of per-sample fractions), and
  `sample_count` / `samples_with_metrics` so a partially-processed pool is
  interpretable. Nothing is stored at the pool level — the rollup is aggregated
  at request time, so it never drifts when a sample is re-processed or deleted.
  Read-gated like the pool roster (prep_sample:read + wet_lab_admin). implements
  #143 (#149)
- Per-`sequenced_sample` read metrics: `sequenced_sample` gains three nullable
  `BIGINT` columns (`raw_read_count_r1r2`, `biological_read_count_r1r2`,
  `quality_filtered_read_count_r1r2`) with a CHECK enforcing
  quality_filtered <= biological <= raw. A new `persist-read-metrics` library
  primitive — added as the final `action:` step of `fastq-to-parquet/1.2.0` —
  reads the three `read_count.json` sidecars (#141) and writes them onto the
  sample's 1:1 `sequenced_sample`; `GET /sequenced-sample/{idx}` surfaces them
  plus a computed-on-read `fraction_passing_quality_filter`
  (quality_filtered / raw). The workflow now declares `prep_sample:write` (in the
  USER ceiling, so its audience is unchanged). implements #142 (#148)
- The `fastq`, `qc`, and `host_filter` native steps now emit a `read_count.json`
  sidecar recording how many reads survive each parquet stage, captured as the
  three SPP boundary counts per `prep_sample`: raw (`fastq` → `raw_read_count`),
  biological (`qc` → `biological_read_count`), and quality-filtered
  (`host_filter` → `quality_filtered_read_count`). The count is
  `count(*) + count(sequence2)` (both mates, the `*_r1r2` convention) via a new
  shared `read_count.write_read_count` helper. `fastq-to-parquet/1.2.0` declares
  the three outputs so the runner forwards them in `bound`; persisting them onto
  `sequenced_sample` is a follow-up (#142). Emission only — no schema change;
  implements #141 (#147)
- New `GET /api/v1/sequencing-run/{sequencing_run_idx}` route returning a run's
  caller-visible metadata (notably `instrument_model`). Read-gated like the pool
  roster route (prep_sample:read + wet_lab_admin). `qiita submit-host-filter-pool`
  reads it to forward QC's polyG-gating `instrument_model` per sample (#129)
- New `fastq-to-parquet/1.2.0` workflow: an additive successor to 1.1.0 that
  inserts an ALWAYS-ON `qc` step between `fastq` and `host_filter`
  (`fastq → qc → host_filter`). Each stage re-emits the `reads` binding it
  consumes (a transform in place), so `host_filter` is identical to 1.1.0 and
  consumes the QC'd reads. `context_schema` gains `instrument_model` (forwarded to
  qc's polyG gate via the step's `params`) and the two-reference host-filter keys
  (`host_rype_reference_idx` + optional `host_minimap2_reference_idx`); the qc
  step lists `adapter_parquet`, which triggers the runner's adapter materialization.
  1.0.0 and 1.1.0 stay available unchanged (#129)
- Verified and documented the duckdb-miint fastp-port QC functions
  (`filter_read`, `trim_adapters` / `trim_adapters_pe`, `trim_polyg`) that the
  upcoming `qc` native job builds on. New
  `qiita-compute-orchestrator/tests/jobs/test_qc_miint_contract.py` pins their
  **positional-arg-only** contract and fastp-default values against the
  team-mirror build (the upstream `docs/qc.md` documents named params the build
  rejects); `docs/duckdb-miint.md` gains a QC section. Groundwork for the
  bcl-convert → `fastq` → `qc` → `host_filter` pipeline (#129)
- New `artifact_sequence_set` reference kind — an indexless set of artifact
  sequences (the canonical adapter set the QC step trims against), ingested
  through the same kind-agnostic reference-add flow (no taxonomy, no index).
  `qiita reference load --kind artifact_sequence_set` and a `reference.kind`
  CHECK widen back it. The control plane gained
  `QIITA_DEFAULT_ADAPTER_REFERENCE_IDX` (the canonical set's reference_idx) and a
  runner resolver (`_resolve_qc_adapters`) that DoGets that set's sequences from
  the data plane and stages them as a one-`sequence`-column Parquet for the QC
  step — materialized only for a workflow whose steps need it (#129)
- New `qc` native job (`qiita_compute_orchestrator.jobs.qc`): a fastp-equivalent
  read-QC transform `reads.parquet` → `qc_reads.parquet` over the duckdb-miint
  fastp-port functions. Per read it runs adapter trim (`trim_adapters` SE /
  `trim_adapters_pe` PE) → optional polyG trim (`trim_polyg`, gated on a 2-color
  `instrument_model`) → length/quality filter (`filter_read`, fastp `-l 100`
  defaults); drop-only and `sequence_idx`-preserving, dropping a read pair when
  EITHER mate falls below min_length after trimming. The canonical adapter set
  is read from the runner-staged `adapter_parquet` via `read_parquet` and inlined
  as a constant `VARCHAR[]`; the two SE/PE seams emit SELECTs that UNION ALL
  straight into one streaming COPY (no intermediate accumulator table). Slots into
  the bcl-convert → `fastq` → `qc` → `host_filter` pipeline (#129)
- Remove a full preparation (sequenced_pool) from the system. New
  `DELETE /sequencing-run/{run}/sequenced-pool/{pool}` hard-deletes a
  sequenced_pool and everything under it — the pool row, every
  `sequenced_sample`/`prep_sample` it holds, their `prep_sample_metadata`,
  `prep_sample_field_exception`, and `prep_sample_to_study` links, and any
  pool-/sample-scoped `work_ticket` rows (`work_ticket_step` and `sequence_range`
  cascade) — in one FK-ordered transaction. The parent `sequencing_run` and the
  underlying `biosample` rows are intentionally retained (a biosample is a
  physical sample, not pool-owned). Because each prep_sample is exclusive to one
  pool, this removes those samples from every study they link to. system_admin
  only, gated by a new `sequenced_pool:delete` scope. Gating mirrors
  `DELETE /reference`: in-flight work tickets (pending/queued/processing) block
  unconditionally; completed/failed tickets, prep_samples published into a study,
  and ENA-submitted samples block unless `?force=true`. Exposed as the
  `qiita delete-sequenced-pool` CLI command. The data-plane DuckLake purge is a
  no-op until processing-result tables exist; on-disk demux FASTQ cleanup is a
  follow-up (#125)
- Per-host-reference index selection and tunable build params. `qiita reference
  load --host` gains `--no-rype-index` / `--no-minimap2-index` (build only one
  of the two host-filter indexes; default still builds both, at least one
  required), plus `--rype-w N` (rype minimizer window, default now **20**, was
  25) and `--minimap2-preset` (one of
  `sr`/`map-ont`/`map-pb`/`map-hifi`/`asm5`/`asm10`/`asm20`, default `sr`). These
  ride in `action_context`
  (`build_rype`/`build_minimap2`/`rype_w`/`minimap2_preset`), validated by the
  `(local-)host-reference-add` `context_schema` (a `not` backstop rejects
  building neither). Backed by two reusable workflow-engine additions:
  `WorkflowEntry.when` (skip an entry when its named action_context flag is
  falsy — default-on) gates each build step and its `register-index`, and
  `WorkflowStep.params` (action_context key → native `Inputs` field) carries
  scalar build params to a native step without a wire-contract change. The
  fastq-to-parquet host-filter consumer now accepts a single-index host
  reference too: `_resolve_host_filter_indexes` binds whichever of rype/minimap2
  exist (requiring at least one) and the `host_filter` step skips the stage
  whose index is absent. Index selection is initial-build-time only (the status
  FSM is terminal at `active`) (#124)
- List a sequencing run's sequenced-samples in one call. New
  `GET /sequencing-run/{idx}/sequenced-sample/list` returns the run's active
  sequenced-samples as `SequencedSampleListResponse` rows, and
  `SequencedSampleListItem` now also carries `biosample_idx`, both ENA
  accessions (`ena_experiment_accession`, `ena_run_accession`), and both
  biosample accessions (`biosample_accession`, `ena_sample_accession`) — enough
  for the ENA experiment fan-out (which needs the BioSample accession as the
  experiment sample_descriptor) without per-sample GETs. Gated on
  `prep_sample:read` + wet_lab_admin; the existing idx-only `…/list-idxs`
  run route is unchanged. The `qiita` CLI gains `sequenced-sample list` (#135)
- List the studies a prep-sample belongs to. New
  `GET /prep-sample/{idx}/study/list` returns the active (non-retired) linked
  studies ascending by idx as `StudyListResponse` rows, each carrying the
  study's `bioproject_accession` and `ena_study_accession` — enough for the ENA
  experiment fan-out (which uses the BioProject accession as the experiment
  study_ref) without a per-study GET; 404 on an unknown prep-sample. Gated on
  `prep_sample:read` + wet_lab_admin. The `qiita` CLI gains
  `prep-sample list-studies` (#135)
- Resolve sequencing runs by instrument_run_id. New
  `POST /sequencing-run/lookup-by-instrument-run-id` bulk-resolves
  instrument_run_id values to sequencing_run idxs (idx-only, mirroring the
  study/biosample accession lookups), gated on `prep_sample:read`. The `qiita`
  CLI gains `sequencing-run get` and `sequencing-run lookup` for the
  resolve-then-read flow against `GET /sequencing-run/{idx}` (#135)
- Delete a reference database from the system. New
  `DELETE /reference/{idx}` fully purges a reference — Postgres rows
  (`reference`, `reference_membership`, `reference_index`, plus orphaned
  `feature`/`feature_genome`/`genome` no surviving reference claims), DuckLake
  data (taxonomy/phylogeny/placements by `reference_idx`; sequences/chunks only
  for orphan features, computed in the data plane via a new `delete_reference`
  DoAction), and on-disk indexes (`rype`/`minimap2` under `PATH_DERIVED`, removed
  by a new orchestrator `DELETE /reference-artifact/{idx}` endpoint). system_admin
  only, gated by a new `reference:delete` scope. Work tickets that reference it
  block the delete: in-flight (pending/queued/processing) unconditionally,
  completed/failed unless `?force=true`. Shared features (claimed by another
  reference) are never deleted. Lets operators remove test references accumulated
  by repeated re-loads (#29)
- Work-ticket step logs are now retrievable without sudo. New
  `GET /work-ticket/{idx}/step/{step_index}/logs` returns a bounded
  stdout/stderr tail (defaulting to the latest attempt; `attempt` and
  `tail_lines` query params override), and `qiita ticket logs <idx>
  --step-index N [--attempt N] [--tail-lines N]` surfaces it from the CLI. The
  control plane reads the logs straight off shared scratch
  (`PATH_SCRATCH/ticket/...`) via its existing `qiita-pipeline` group access,
  so an operator can diagnose an OOM / bad input / contract violation without a
  host shell. Auth mirrors `GET /work-ticket/{idx}` (originator or
  wet_lab_admin+; non-owners get an enumeration-safe 404) (#104)
- Per-run memory override for workflow steps. `qiita reference load` and `qiita
  ticket submit` gain `--mem-gb N`, carried as an optional `resource_override`
  on `POST /work-ticket`: at dispatch the runner raises each SLURM step's memory
  floor to `max(step baseline, N)` (raise-only — never lowers a step the YAML
  sized higher), still clamped to the action's mem ceiling. Lets an operator
  load a genome-scale host reference (e.g. a human genome that OOMs the
  conservative 8 GB default) without editing the workflow YAML. Gated to
  wet_lab_admin / system_admin (a regular user who can otherwise submit the
  workflow still gets 403); an override above the ceiling is a clean 422.
  Persisted on `qiita.work_ticket` so a control-plane restart re-attaches
  in-flight work with the same override (#102)
- `qiita submit-host-filter-pool` — a bundled operator gesture that fans out
  one host-filtered `fastq-to-parquet/1.1.0` work-ticket per sample in a
  completed bcl-convert pool. It pre-checks the `--host-reference-idx` is ACTIVE
  and carries both a rype and a minimap2 index, lists the pool's samples via a
  new pool-scoped route, resolves each sample's R1/R2 FASTQ under `--convert-dir`
  by the `sequenced_pool_item_id` prefix (recursive, single-lane), and submits
  every ticket with `host_filter_enabled` — resolving all samples before any POST
  so a misconfiguration aborts with zero side effects. Backed by a new
  `GET /sequencing-run/{run}/sequenced-pool/{pool}/sequenced-sample/list`
  returning `(sequenced_sample_idx, prep_sample_idx, sequenced_pool_item_id)`
  per active sample (#99)
- `compute-readiness` now probes that the deploy-staged miint build registers the
  short-read host-filter functions `save_minimap2_index` (the
  `build_minimap2_index` step) and `align_minimap2` (the `host_filter` step), via
  a `miint-host-filter-fns` check against `duckdb_functions()`. These were the
  newest miint additions with no probe, so a v1.5.3 mirror build missing either
  was only caught at the first `host-reference-add` run; it now surfaces at
  deploy alongside `miint-read-fastx` / `miint-sequence-split` (#101)
- The public landing page footer now shows the deployed commit's short git
  SHA next to the package version (e.g. `v2026.3.0 (a28c96e)`), linked to its
  GitHub commit. The SHA is captured at deploy (`deploy/local-deploy.sh` from
  the git clone, or `GITHUB_SHA` on the CI path) and passed to the control
  plane via an optional `BUILD_SHA` env var written into a deploy-owned
  `build.env`; a from-source / first-deploy boot leaves it unset and the
  footer renders the version alone (#94)
- Short-read host filtering. A new `host_filter` native job depletes host reads
  from `reads.parquet` in two stages — rype `rype_classify` against a host's
  POSITIVE `.ryxdi` (host = any match, not rype's `negative_index`), then
  minimap2 `align_minimap2` (`preset 'sr'`) on the survivors — dropping any read
  flagged by either tool. Paired-end is handled natively: a read pair's R1/R2
  ride one `sequence_idx` as `(sequence1, sequence2)` straight into the tools
  (`rype_classify` reads both mates; `align_minimap2` aligns the pair in PE
  mode), so either mate matching drops the whole pair without flattening. It's a
  gated, optional step in a new `fastq-to-parquet/1.1.0` workflow
  (`host_filter_enabled` + `host_reference_idx` context; pass-through when
  disabled; `1.0.0` is kept and the submit route picks the version) (#89)
- `build_minimap2_index` native job + a `minimap2` value for
  `reference_index.index_type` (migration
  `20260612000000_reference_index_minimap2_type`), so a host reference now
  carries BOTH a rype `.ryxdi` and a minimap2 `.mmi`. The `host-reference-add` /
  `local-host-reference-add` workflows gain the minimap2 build + a second
  `register-index`. Like `build_rype_index`, it consumes the feature-keyed
  `reference_sequence_chunks` (reassembling whole contigs via `string_agg`), so
  the minimap2 index is built from the same data-plane bytes as everything else
  — no raw-FASTA side channel (#89)
- `make verify-deploy` (`deploy/verify.sh`) — one command runs the generic
  post-deploy checks (health aggregate, `qiita.action` list, and
  compute-readiness) each with the correct service account/env baked in, so the
  compute-readiness run-as line is no longer hand-copied into every deploy
  (#72)
- `make preflight` (`deploy/preflight.sh`) — read-only config/secret consistency
  check (PATH_SCRATCH byte-identity across env files, HMAC CP==DP, token-file
  perms, connection-string shape) that prints non-secret SHA-256 fingerprints;
  catches the silent runtime-failure class before a restart (#72)
- `make redeploy` (`deploy/redeploy.sh`) — guided incremental redeploy that
  codifies redeploy.md's skeleton (pull → preflight → migration gate →
  local-deploy.sh → stage → verify); migrations stay out-of-band (verify and
  refuse, never auto-apply) (#72)
- New nullable `bioproject_accession` column on the study table (unique
  when present), for NCBI/ENA BioProject tracking (#87)
- Exposed study `bioproject_accession` through create, get, and patch: the
  REST request/response field and the `qiita study create`/`patch`
  `--bioproject-accession` flag (#91)
- The study and biosample lookup-by-accession endpoints accept an
  `accession_field` selector so a caller can resolve by either accession
  column (study: `ena_study_accession` or `bioproject_accession`; biosample:
  `biosample_accession` or `ena_sample_accession`) (#91)

### Fixed

- **SynDNA spike-in count was structurally zero in case 5.** The chain ran
  `lima -> qc -> host_filter -> syndna`, but a case-5 spike-in
  (`syndna_is_twisted == False`) carries no Twist adaptor, so lima marked it
  `twist_no_adaptor` first — and every later step (including syndna) only
  re-classifies still-`pass` rows, so syndna never saw it. Reordered to
  `syndna -> lima -> qc -> host_filter`: syndna marks the spike-ins on the raw
  reads before lima can drop them, lima then processes only the biological reads
  (which all carry the adaptor), and a single `partial_mask` binding threads the
  verdict through so a `spikein_syndna` mark is never overwritten. A real-miint
  case-5 chain test reproduces the bug and pins the fix. (#270)

### Changed

- **Reference-arc efficiency/latency/DRY follow-ups.** Dropped a redundant
  `ORDER BY rm.feature_idx` from `_export_member_genome` (no consumer relies on
  the row order — every downstream reducer/tiler re-sorts — so it only risked an
  explicit Sort over millions of `(feature_idx, genome_idx)` rows at GG2 scale).
  Moved the `delete_read_mask_block` / `delete_alignment_block` data-plane
  DoActions onto `tokio::task::spawn_blocking` so their blocking DuckLake delete
  transactions never starve a tonic async worker (matching the four sibling
  delete arms). Single-sourced the reference shard-set queries
  (`count(DISTINCT shard_id)` / sorted `DISTINCT shard_id` over the non-NULL
  `reference_membership` rows) into a new `repositories/reference_membership.py`
  (`count_reference_shards` / `reference_shard_ids`), replacing four
  byte-identical copies whose drift would make the reference-add finalizer gate
  on a different threshold than the planner assigned. Pure quality — no change to
  persisted data. (#268)

- **Single-sourced the sequence-chunk reassembly SQL.** Added
  `reassemble_chunks_expr` to `qiita_common.chunking` next to the existing
  `sequence_split_expr`, so the `string_agg(chunk_data, '' ORDER BY chunk_index)`
  reassembly (previously hand-written in `build_minimap2_index` and
  `hash_sequences`) has one home for both directions of the chunk contract.
  Pure refactor — both call sites emit byte-identical SQL. (#268)

- **`reference_taxonomy` is now 1-1 with a reference's features; taxonomy
  coverage gaps warn loudly instead of dropping silently.** `reference_load`'s
  `_write_taxonomy` used an INNER JOIN on `read_id`, which silently dropped both
  features with no supplied taxonomy row and taxonomy rows keyed to an unknown
  `feature_id` (the same ID-namespace-mismatch class that already bit the genome
  map). It now writes exactly one row per reference feature: a feature with no
  supplied taxonomy is recorded at rest as an all-NULL-rank ("unclassified")
  row rather than dropped. Coverage anomalies — missing taxonomy, stray/unmatched
  `feature_id`s, and duplicate supplied rows (collapsed to one per feature) — are
  logged as loud `WARNING`s (landing in the SLURM job log) rather than failing
  the ingest, because real corpora are not strictly 1-1 (GG2's 2024.09 backbone
  has ~29 features with no taxonomy). The supplied-content format checks (≤8
  ranks, no blank fields, prefix order) stay hard `ValueError`s. No schema change
  (the rank columns are already nullable) and no migration; already-ingested
  references are not backfilled — only new ingests get the 1-1-at-rest shape.
  (#268)
- **The read-mask `biological` count predicate is now a whitelist.** It was
  `reason NOT LIKE 'qc_%'` — fail-OPEN, so every reason added since would have
  been counted as biological by default, which is exactly how `spikein_syndna`
  and `twist_no_adaptor` would have inflated it. Buckets now derive from
  `READ_MASK_BUCKET` in qiita-common (`biological` = `pass` + `host_*`), and a
  coverage test fails on any unclassified reason. The data plane's
  `mask_metrics_counts` carried the same predicate and changed in lockstep; its
  `mask_metrics` JSON gains a `spikein` key, so control plane and data plane
  must deploy together. (#270)
- **Read-mask identity (`mask_idx`) now carries `resolved_lima` and
  `syndna_reference_idx`.** Nothing in the hash distinguished the five PacBio
  protocols: `prep_protocol_idx` is an operator CLI flag, uniform across them,
  and no run identifier participates by design. A case-5 run and a case-1 run
  submitted with the same flags hashed identically and shared one `mask_idx`
  whose stored params described only one of them. `resolved_lima` is nested and
  `None` when lima is off, so a future lima knob re-mints only lima masks.
  **Consequence: `params_hash` changes for every existing mask.** The existing
  rows stay valid and referenced; a re-run of an identical config mints one new
  `mask_idx` rather than reusing the old. (#270)
- **The pre-flight `human_filtering` derivation is platform-aware.** It keyed on
  `illumina_sample_idx` and walked `run_illumina_sample`, so a PacBio pool's
  samples all came back with a null intent — and `submit-host-filter-pool`
  aborts on a null intent. It could not run against a PacBio pool at all. PacBio
  now keys on the barcode (which is the `sequenced_pool_item_id`). PacBio host
  filtering is per sample; Illumina keeps its pool-uniform guard for now. (#270)
- **`read-mask`'s `context_schema` requires `host_filter_enabled`,
  `lima_enabled`, and `syndna_enabled`.** `when:` is default-ON — an absent gate
  key RUNS its step — so a ticket that omitted `lima_enabled` would have executed
  the long-read lima chain on a short-read sample. (#270)


- **`submit-bcl-convert` re-run is now convergent (create-missing roster).** The
  run → pool → sequenced-sample provisioning is unified with `submit-pacbio-ingest`
  in one shared `_provision_run_pool_roster` (was duplicated between the two). As a
  result bcl-convert now GETs the pool roster and creates only the missing samples,
  so a re-run after a partial failure reuses existing rows instead of 409ing on the
  first already-created sample. (#260)
- **Data-plane public-edge hardening.** The Arrow Flight service is reachable
  from the internet through nginx on 443 (by design — clients connect directly
  through nginx). Tightened that edge: the nginx Flight `location` now sets
  `client_max_body_size 0` (a DoPut streams an entire reference through one
  client-streaming RPC, so nginx's whole-body cap and the data plane's
  per-message decode ceiling measure different quantities — nginx's 1 MB default
  would 413 a reference-load upload, and any finite cap would still 413 a
  multi-GiB reference even with every gRPC message under the DP ceiling), bumps
  `grpc_read_timeout`/`grpc_send_timeout` to 3600s (the 60s default cut off large
  DoGet exports before the first batch materialized), and caps concurrent Flight
  connections per client (`limit_conn qiita_flight_conn 64`) to blunt connection
  floods. The data plane's `build_query` now refuses an empty filter on the
  `read_masked` view (which would `SELECT *` every sample's pass-reads across all
  studies) as defense-in-depth; the reference_* tables still allow an unfiltered
  read by design. CLI `--data-plane-url` help now shows the public
  `grpc+tls://<host>:443` form — the old `grpc://<host>:50051` example is the
  on-host/direct port and is not reachable off the deploy host. (#261)

- **Login-cookie secret split off the Flight-ticket secret.** The `/auth/login`
  → `/auth/handoff` cookie now signs with a dedicated `LOGIN_COOKIE_SECRET_KEY`
  (new required control-plane env var) instead of reusing `HMAC_SECRET_KEY`. The
  two were the same key, so one leak forged both Flight tickets and login
  cookies; they are now independent. Control-plane only — the data plane never
  sees the cookie key. Operator note: at the restart, login cookies signed with
  the old key fail verification — a clean 401 (`CookieInvalid`), not a 500 — so
  users mid-handoff in the ≤5-minute cookie window (`Max-Age` 300s) simply
  re-login once. No dual-verify or coordinated cutover needed. (#262)

- **Flight tickets are now Ed25519-signed (asymmetric), not HMAC-SHA256.** The
  control plane signs with an Ed25519 private seed (`FLIGHT_TICKET_SIGNING_KEY`);
  the publicly-reachable data plane holds only the matching public key
  (`FLIGHT_TICKET_PUBLIC_KEY`) and verifies — so a data-plane host/env compromise
  can no longer forge tickets (previously the symmetric `HMAC_SECRET_KEY` on the
  DP could both verify and forge). Ticket wire version bumps to 2 (64-byte
  signature); the DP accepts only v2. `HMAC_SECRET_KEY` is removed from both
  services (the cookie moved to `LOGIN_COOKIE_SECRET_KEY`, tickets to the
  keypair). End-user CLIs are unchanged — tickets are minted server-side. (#263)

- **DuckLake catalog parquet write-options aligned with our register-time format.**
  Set `parquet_compression='zstd'` + `parquet_version=2` as DuckLake catalog-global
  options (DuckLake's defaults are snappy / v1) and `parquet_row_group_size=16384`
  per-table on the chunk tables (`reference_sequence_chunks`,
  `assembled_sequence_chunks`), matching `qiita_common.parquet.PARQUET_OPTS` /
  `CHUNK_ROW_GROUP_SIZE` — so DuckLake's OWN rewrites (compaction, merge, any future
  direct insert) stay consistent with the format `register_files` writes rather than
  drifting to DuckLake's defaults. Set idempotently at data-plane boot
  (`connect_ducklake` + the `ensure_*_tables`), persisted in `ducklake_metadata`.
  (#255)
- **SIF build tooling supports N per-tool images per workflow.** A container
  workflow may now ship several single-tool images under
  `workflows/<wf>/sif-build.d/<image>.env` (each declaring its own `DEF_FILE` and a
  `HASH_INPUTS` of the entrypoint(s) + shared helper it %files-copies; the def is
  auto-included in the scoped hash) alongside — and fully
  backward-compatible with — the legacy single `sif-build.env` + `Apptainer.def`
  form (bcl-convert untouched). `build-sif.sh` takes an optional `<image>`
  selector, `deploy/build-sifs.sh` discovers both layouts, and a new
  `qiita_sif_build_inputs_hash_scoped` keys the two-gate idempotency hash to each
  image's own inputs so a change to one tool's def or entrypoint rebuilds only its
  image. `long-read-assembly` is the first consumer, shipping four per-tool images
  (assemble / binning / dastool / checkm). (#255)
- **Internal decomposition — no behavior change.** Consolidated the six
  near-identical control-plane Flight `DoAction` wrappers into one `_do_action`
  helper; split the orchestrator's all-nullable `StepHandle` into typed
  `LocalStepHandle` / `SlurmStepHandle` (the `StepHandleWire` wire form is
  byte-for-byte unchanged, so no migration and no CP↔CO contract change); and
  broke four monoliths into cohesive packages that re-export every name at the
  same import path — `qiita_common.models` (→ health / reference / step /
  upload / user / biosample / study / auth / work_ticket / sequencing + a
  shared `_base`), the `qiita_control_plane.cli.user` and `.cli.admin` CLIs, and
  `qiita_control_plane.runner`. Pure moves: every top-level definition is
  carried over verbatim and each module's public-name set is a superset of the
  original, so no consumer needed editing. No env var, migration, scope, route,
  or wire change. (#248)

- **Scope-403s now flag when your token lacks a scope your role grants, and
  point at re-login.** A human's PAT scopes are fixed at mint time, so a scope
  that's in the caller's live `role_ceiling` but absent from the token yields a
  confusing `missing required scope 'X'` 403 even though the role grants X —
  whether the scope was added to the ceiling after mint, or the PAT was minted
  below the ceiling. `require_scope` / `require_any_scope` now append an
  actionable "run `qiita login` to mint a fresh token with your full role scopes"
  hint in that case. Genuinely-unentitled callers and service accounts get the
  plain 403 unchanged — no security-model change (nothing is granted, only the
  message improves). (#161)
- **Control-plane test suite parallelized and de-latencied.** Several changes so
  the ~1980 control-plane tests stop dominating CI wall-clock: (1) the
  `test-control-plane-with-db` / `-without-db` targets run under `pytest-xdist`
  (`-n auto --dist worksteal`) — the `postgres_url` fixture is now xdist-aware,
  DROP+CREATEing and migrating a per-worker `qiita_test_<worker>` DB (from
  `template0`, so concurrent creates don't race on a shared template), while
  serial runs (no `PYTEST_XDIST_WORKER`) keep the shared base DB, leaving the
  integration tier and single-test runs untouched; `worksteal` keeps an
  end-of-run block of slow DB tests from stranding on one idle-surrounded worker.
  (2) The JWKS / loopback test harnesses pass `poll_interval=0.01` to
  `HTTPServer.serve_forever`, cutting a ~0.5s-per-test shutdown wait (the 0.5s
  default) that ~120 auth/CLI tests each paid. Local Docker harness: serial
  suite ~95s → ~34s, parallel run ~30s → ~9s; 1979/1979 pass in both modes. (#253)
- **CI runs the macOS matrix leg only on `main`, not on PRs.** macOS runners are
  ~6-15× slower than Ubuntu for the test suite and set the whole PR run's
  wall-clock, while the deploy target is Linux. A `config` job now emits the
  matrix OS list per event — Ubuntu-only for `pull_request`, Ubuntu + macOS for
  `push` to `main` — so PR feedback is fast and macOS still gates every merge.
  (#253)
- **Data plane fails fast on a missing `PATH_PERSISTENT`.** The var is now
  required and must be absolute (previously optional, falling back to
  `$TMPDIR/qiita`), matching the fail-fast posture of `HMAC_SECRET_KEY` /
  `DUCKLAKE_CATALOG_CONNSTR` / `PATH_SCRATCH`. A forgotten value used to silently
  root durable DuckLake Parquet under `/tmp` (lost on reboot, never backed up);
  the instance now refuses to start instead. `.env.data-plane.example` promotes
  it to the required block. (#246)
- **Data plane `register_files` is now catalog-atomic.** The per-file
  `ducklake_add_data_files` registration loop is wrapped in a single DuckLake
  transaction (BEGIN/COMMIT/ROLLBACK), matching the `delete_*` actions — a
  mid-loop failure rolls back every prior registration rather than leaving a
  reference half-registered in the catalog. Filesystem moves are intentionally
  not rolled back (dest names are ticket-unique and `move_file` refuses to
  overwrite, so a failure leaves only inert orphan Parquet). Adds end-to-end,
  filename-traversal, and do_action dispatch-trust tests. (#246)
- **Data plane offloads blocking DoAction work off the async runtime.** The
  `register_files` / `delete_reference` / `delete_mask` / `delete_pool_reads`
  arms ran their blocking DuckLake transactions inline on the tonic async
  worker; each now runs on `tokio::task::spawn_blocking` (mirroring
  `export_read` / `count_masked`), so a long registration or delete can't starve
  the async runtime and stall concurrent requests. Each helper opens and drops
  its own DuckDB connection, so nothing non-`Send` crosses the task boundary.
  (#246)
- **Data plane DoPut writes Parquet off the async runtime.** The DoPut handler
  interleaved blocking Parquet write / `fsync` / `chmod` with awaiting the live
  Flight stream, running file I/O inline on the tonic async worker. It now
  bridges the two with a bounded mpsc channel: an async loop pulls decoded
  batches off the stream and forwards them to a `spawn_blocking` writer task that
  owns the file and does all blocking I/O. The bounded channel backpressures the
  network when the writer falls behind, so peak memory stays bounded (same
  posture as the DoGet streaming path). Behavior (durability fsync, `create_new`
  concurrent-upload guard, partial-file cleanup on error, sha256/row-count
  reporting) is unchanged. (#246)
- **Data plane DoAction replay is a classified, tripwired accepted risk.** Flight
  tickets have no single-use ledger, so a still-valid token can be replayed
  within its lifetime; this is accepted because every DoAction is idempotent or
  otherwise replay-safe. A new `REPLAY_SAFE_ACTIONS` registry now gates the
  `do_action` dispatcher (an unlisted action is rejected), with a test pinning
  the registry to the dispatcher's arms and an anchored `# replay:` comment — so
  a newly-added action fails the build until it is consciously classified
  replay-safe. Documented in `docs/auth.md#ticket-replay` and the CLAUDE.md
  data-plane section. (#246)
- A human PAT's effective scopes are now intersected with the principal's **current** role ceiling at token-resolve time, so a role downgrade (or a shrunk `ROLE_IMPLIED_SCOPES`) immediately narrows an already-minted token on scope-only-gated routes without revocation. Service-account tokens are unaffected (their ceiling is fixed, not role-derived). Note: this also narrows any human token that was minted *above* its role ceiling via low-level tooling that skips the mint-time ceiling check — such a token silently loses the out-of-ceiling scopes at the next resolve. (#242)
- Auth integer env-knobs (`AUTHROCKET_JWT_LEEWAY_SECONDS`, `AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS`, `QIITA_TOKEN_DEFAULT_TTL_DAYS`, `AUTH_HANDOFF_FRESHNESS_SECONDS`, `CLI_LOGIN_CODE_TTL_SECONDS`) are now validated at boot instead of parsed with a bare `int()` — a non-int or non-positive value fails loudly (leeway may be 0). (#241)
- `GET /reference` and `GET /prep-protocol` accept a bounded `limit` query param (default 1000, max 5000) so the anonymous catalog lists can't return an unbounded payload. (#241)
- Flight-ticket and login-cookie signing now share `qiita_common.hashing.canonical_json` instead of three hand-rolled `json.dumps(sort_keys=…)` spellings, removing the risk of the HMAC'd wire serialization drifting. (#241)
- Accepting an AuthRocket invitation redirects to the cookie-anchored `/auth/login` instead of minting a full-ceiling PAT from the un-anchored invitation JWT. (#241)
- `qiita-admin masked-read-export` is now **re-runnable**: it creates `--output-dir`
  (with parents) if missing instead of erroring, and for parquet it skips a sample
  whose output file already exists when the count matches and overwrites it only
  when it differs. The count comes from a new `count_masked` data-plane DoAction —
  a cheap `count(*)` against the light `read_mask` table (no read sequences
  streamed or materialized) that reuses the sample's existing signed export ticket,
  so there's no new control-plane route. fastq has no cheap on-disk count, so an
  existing fastq target is refused up front rather than re-exported. (#230)
- **CI build speedups.** The `test-integration` job now sets up the Rust
  toolchain + `Swatinem/rust-cache` + cached `libduckdb` (mirroring the `rust`
  job), so the data-plane debug build it drives — previously a cold ~80s
  recompile of all deps and the largest slice of the job — is incremental on
  repeat runs. The separate `lint-rust` / `test-rust` jobs merged into one
  `rust` job that shares a checkout, toolchain, and warm `rust-cache` (no more
  two jobs racing to write the same cache). The macOS host-Postgres provisioning,
  previously inlined and duplicated across two jobs, moved into a reusable
  `.github/actions/setup-host-postgres` composite action with a weekly-refreshable
  Homebrew download cache. No change to what is tested. (#225)
- **Native DuckDB jobs share one spill-dir context manager** (`duckdb_tmp_dir` in
  the orchestrator's `miint.py`), making `<workspace>/.duckdb_tmp` teardown
  structural across all ten jobs instead of a per-job `try/finally`. This closes a
  leak in `build_rype_index`, which created the spill dir but never removed it
  (spilled bytes accumulated in the shared work-ticket workspace — SLURM has hit
  "no space in /tmp"). Same consistency sweep: the two index builders and
  `qc_report` now route their `read_parquet` path literals through
  `validate_parquet_path` (the repo's fail-fast quote/backslash/control-char
  reject) like the sibling jobs, and the builders' `index_type` meta JSON uses the
  shared `HOST_FILTER_INDEX_TYPE_{RYPE,MINIMAP2}` constants instead of bare
  `"rype"`/`"minimap2"` literals. Behavior-preserving (the existing per-job unit
  suites are the guard); no env var, host dir, scope, migration, or workflow
  change. (#229)
- A job's input `params.json` and a native step's output `manifest.json` are now
  pretty-printed (2-space indent, trailing newline; the manifest also sorts keys
  to mirror the container-side `manifest_writer.py`) instead of dumped as a single
  dense line — far easier to read when debugging a job's input/output dir. Both
  files are parsed (`model_validate_json` / `json.loads`), so the whitespace change
  is transparent to every consumer. (#208)
- `qc` step walltime raised in both actions that run it (`read-mask/1.0.0` and
  `fastq-to-parquet/1.3.0`): `baseline_resources.walltime` PT2H → PT4H and
  `action_ceiling.walltime` PT4H → PT8H, giving the first attempt more time and
  the new TIMEOUT escalation (above) room to climb to PT8H. The ceiling is
  action-wide, so `host_filter` (baseline PT4H) can now also escalate to PT8H on
  a TIMEOUT retry. YAMLs edited in place; re-synced via `qiita-admin actions
  sync`. (#216)
- bcl-convert re-submission over an already-**COMPLETED** sequenced_pool is now
  refused by default and requires `--force` (wet_lab_admin+). A re-run
  re-registers the pool's reads into the lake, and DuckLake has no uniqueness, so
  a silent re-submit duplicated read rows. `WorkTicketCreateRequest` gains a
  `force` flag (privileged like `resource_override`); `submit_work_ticket` /
  `_check_disallow_without_delete` gate a COMPLETED-pool resubmit; `qiita
  submit-bcl-convert` gains `--force`. Non-force recovery for a stored result is
  `delete-sequenced-pool` then resubmit; FAILED tickets remain freely resumable
  via `qiita ticket run`. (#206)
- `host_filter` step memory raised 16 → 32 GB in both actions that run it
  (`read-mask/1.0.0` and `fastq-to-parquet/1.3.0`): the step's
  `baseline_resources.mem_gb` and the `action_ceiling.mem_gb` both go 16 → 32, so
  a `host_filter` run lands at 32 GB directly (the genome-scale rype/minimap2 host
  index didn't fit in 16). YAMLs edited in place; re-synced via `qiita-admin
  actions sync`. (#209)
- `qiita submit-host-filter-pool` no longer takes a `--preflight-blob` file. Its
  pool-wide host-filter guard needs each sample's intake `human_filtering` intent,
  which already lives in the pool's **stored** run-preflight blob — so requiring
  the operator to re-supply the file was redundant, and impossible once the stored
  preflight diverged from any local copy (e.g. after `run-preflight update-lane`
  edits it in the database). The intent is now derived server-side: the pool
  sample-list route (`GET
  /api/v1/sequencing-run/{R}/sequenced-pool/{P}/sequenced-sample/list`) gains a
  per-sample `human_filtering` field (additive, nullable), read at request time
  from the stored blob — the single source of truth, so a later `update-lane` is
  reflected automatically. The command reads that field from the roster it already
  fetches; an unparseable/absent stored preflight degrades the field to null
  (listing never 500s, and the parse failure is logged) and the command's
  existing guard turns a null intent into an actionable abort at submit time.
  (#205)
- `qiita-admin masked-read-export` is faster and its fastq output is now
  gzip-compressed. The **parquet** path streams the Flight reader straight to a
  `pyarrow.parquet.ParquetWriter` instead of `DuckDB COPY`, so the bulk read bytes
  are no longer materialized into DuckDB vectors (one fewer full copy) and the
  parquet path no longer loads the miint extension at all (measured ~1.6× faster
  on a synthetic stream, and zero Acero passes). The streamed batches are coalesced
  into row groups sized by `qiita_common.parquet.ROW_GROUP_SIZE_BYTES` (the 64 MB
  byte cap from `PARQUET_OPTS`, now exported as an int), so the file keeps the
  byte-sized row-group layout qiita uses everywhere instead of one tiny row group
  per ~2048-row DataChunk. The **fastq** path writes
  `<stem>.fastq.gz` / `<stem>.R1.fastq.gz` + `<stem>.R2.fastq.gz` (`FORMAT FASTQ,
  COMPRESSION 'gzip'`) instead of uncompressed `.fastq`, and reuses a single
  miint DuckDB connection across all samples rather than opening one (with an
  extension `LOAD`) per sample. (#198)
- `ingest_reads` now parses each sample's FASTQ(s) **once** instead of twice: the
  read count that sizes the `sequence_range` mint is taken from the staged
  intermediate Parquet's `COPY` row-count return rather than a separate
  `read_fastx` counting pass. The intermediate is written before the mint (it is
  mint-independent, keyed by the per-file `sequence_index`), so the count comes
  for free off the parse we already do. On a large paired-end sample (~13.5M
  pairs) this removes a full ~20s serial FASTQ parse per sample. The durable
  `read.parquet` is now sorted by `sequence_idx` alone — `prep_sample_idx` is a
  constant for a single sample, so dropping it from the `ORDER BY` orders nothing
  and yields identical output while shrinking the sort (~66s→~46s, 12GB→9GB peak
  on the same sample). (#201)
- `fastq_to_parquet` likewise sorts its durable `read.parquet` by `sequence_idx`
  alone (dropping the constant `prep_sample_idx` from the `ORDER BY`) — same
  identical-output sort shrink. It was already single-pass (it counts off the
  intermediate Parquet footer), so the parse-once change does not apply there.
  (#201)
- `ingest_reads` now processes up to 4 pool samples **concurrently** (bounded
  `asyncio.gather` + a semaphore; each sample's DuckDB stage/sorted-write runs in
  a worker thread, the mint stays async on the loop) instead of one at a time.
  Per-sample work is independent (own FASTQ, atomic mint, own output file) so
  results are unchanged — this just overlaps the inherently-serial `read_fastx`
  parses across samples. Per-slot DuckDB memory/threads are derived from the SLURM
  cgroup (2 threads/slot to keep the sort parallel and clear wells fast); the
  bcl-convert `ingest_reads` step's `baseline_resources` rise to `cpu: 8` /
  `mem_gb: 56` to match (still well under the action ceiling). (#201)
- `runner._resolve_staged_reads` now falls back to the data plane when a
  read-mask workflow can't find the prep_sample's ephemeral durable staging copy:
  it signs an `export_read` action token and binds the per-ticket `reads.parquet`
  the data plane writes from the persistent DuckLake `read` table (an empty result
  or unreachable data plane still FAILs the ticket cleanly as a SUBMISSION
  BAD_INPUT). This lets `submit-host-filter-pool` reprocess a run whose staging
  copy has been reaped, instead of hard-failing "no stored reads". (#187)
- Tightened the `read-mask/1.0.0` action audience from
  `[user, wet_lab_admin, system_admin]` to `[wet_lab_admin, system_admin]`:
  submitting a read mask (host filter / QC reprocessing) now drives the data plane
  to re-materialize the sample's RAW (human-containing) reads via `export_read`,
  so it is a privileged operation — never a plain `user`. (#187)
- Split read storage from masking so a sample's reads are stored ONCE and can be
  masked repeatedly against different host references. Previously the single
  `fastq-to-parquet` workflow parsed FASTQ, minted a `sequence_idx` range, AND
  masked in one ticket — so re-masking the same sample against a second host
  reference hit the `sequence_range` UNIQUE(prep_sample_idx) constraint (409) and
  failed. Now: the **bcl-convert** workflow gains an `ingest_reads` step that,
  after demux, parses every pool sample's FASTQ(s), mints the range, and writes
  the full reads into the DuckLake `read` table once (plus a durable per-sample
  `read.parquet` under `<scratch>/reads/<prep_sample_idx>/`). A new **`read-mask`**
  workflow (`qc → host_filter → register read_mask → persist-read-metrics`) binds
  those stored reads and records one mask per submission — `qc.py`/`host_filter.py`
  are unchanged. `submit-bcl-convert` now embeds the pool roster
  (`prep_sample_idx ↔ pool_item_id`) in the ticket's `action_context` so the
  pool-scoped ingest step (which has no DB access) can store reads; the runner
  materializes it to a Parquet (`_resolve_sample_map`) and binds the staged reads
  for a mask ticket (`_resolve_staged_reads`). `submit-host-filter-pool` is now
  mask-only: it drops `--convert-dir` and FASTQ resolution and submits one
  `read-mask/1.0.0` ticket per sample, so the SAME pool can be re-submitted later
  against host reference 4 to produce a side-by-side mask over host reference 2's
  reads — neither re-runs ingest. The pool-completion rollup now keys on the
  `read-mask` action (a sample is "processed" once it has a mask). The legacy
  `fastq-to-parquet` workflows remain registered but dormant (no gesture submits
  them); full retirement is a fast-follow.
- `build_rype_index` rebalances the DuckDB/rype memory split now that
  `rype_index_create` windows its chunk feed (miint windowed-feed fix): DuckDB's
  under-SLURM hard cap drops 30 → 8 GB (the windowed feed bounds DuckDB's working
  set to ~256 MiB per window rather than the whole corpus), handing the freed
  ~22 GB to rype's in-process index build. rype's `max_memory` now starts ~50 GB
  at the 64 GB baseline and grows to ~114 GB at the 128 GB OOM-retry ceiling (was
  ~30 → ~92 GB). The off-SLURM fallbacks (DuckDB 4 GB, rype 30 GB floor) are
  unchanged. Relies on the windowed-feed miint build being live on the mirror.
  (#179)
- The sequenced-pool completion rollup gains a `samples_no_data` bucket and its
  `complete` flag now fires when every active sample is in a terminal-accounted
  state — COMPLETED **or** NO_DATA — instead of requiring every sample COMPLETED.
  A plate of real data containing empty wells now reaches `complete=True` rather
  than sitting `false` forever behind permanent empty-well failures. The per-sample
  precedence is `completed > in_flight > no_data > failed > not_submitted` (no_data
  outranks failed, so a well with both a no_data and a stale failed ticket counts
  as no_data); empty wells are no longer folded into `samples_failed`. The
  `GET .../sequenced-pool/{P}/completion` response gains the `samples_no_data`
  field (soft contract addition). Until expected-empty control-well preflight
  marking lands (deferred), EVERY empty well becomes `no_data` — data wells
  included, not only flagged controls. (#176)
- Host-filter references moved off `sequenced_sample` onto the human-filter
  submission (PR 4 of the full-read+mask feature). Host references are a
  filtering-config choice, not a sample property, so two configs can coexist over
  the same reads. `submit-host-filter-pool` now takes `--host-rype-reference-idx`
  / `--host-minimap2-reference-idx` (pool-wide for the submission; omit for a
  QC-only pass-through), pre-flights them once at submission, and threads them
  into the work-ticket `action_context` where the runner reads them to mint the
  `mask_idx` and drive `host_filter`. `submit-bcl-convert` no longer accepts or
  records host references (it only demultiplexes the run); the preflight's
  per-project `human_filtering` flag is still echoed per sample for reference.
  `prep_protocol_idx` stays on the sample. Soft API change: sequenced-sample GET
  responses and the pool/run sample-list rows no longer carry host references.
  `submit-host-filter-pool` also takes `--preflight-blob` (the same SQLite given
  to `submit-bcl-convert`) and guards against a pool-wide host-ref choice that
  disagrees with the samples' intake `human_filtering` intent: a mismatch aborts
  before any ticket is submitted unless `--force` downgrades it to a warning.
  (#175)
- `build_rype_index` resized for large host sets (many human genomes that OOMed
  at 32 GB). The step's `baseline_resources.mem_gb` rises 32 → 64 in both
  `host-reference-add/1.0.0` and `local-host-reference-add/1.0.0`, and
  `local-host-reference-add`'s `action_ceiling.mem_gb` rises 64 → 128 (matching
  `host-reference-add`) so an OOM-killed retry can double the step 64 → 128 GB
  (the escalator clamps to the ceiling). The job now hard-caps DuckDB at 30 GB
  (was 16) regardless of allocation, so the larger cgroup — and the bigger one
  an OOM retry escalates to — flows to rype: rype's `max_memory` starts at 30 GB
  and grows with the allocation (≈92 GB at the 128 GB ceiling). Builds on the
  OOM-retry escalation below (#169)
- Workflow steps now escalate their memory allocation on an OOM-killed retry.
  Previously every retry re-ran at the same `mem_gb`, so an OOM just OOM'd again
  until the retry budget was exhausted. `_run_entry_with_retry` now grows the
  step's memory floor ×2 (clamped to the action's `mem_gb` ceiling) on each
  `OOM_KILLED` retry; other transient kinds still retry unchanged. The escalated
  floor is process-local — a CP restart re-attaches and re-escalates from the
  ticket's static `resource_override`. The `reference-add` and
  `host-reference-add` action ceilings are raised 64 → 128 GB so the OOM-prone
  `reference_load` step can climb 32 → 64 → 128 GB across retries (#167)
- `qiita-user submit-host-filter-pool` now host-filters each pool sample against
  the reference(s) recorded on it at `submit-bcl-convert` time, instead of a
  single uniform reference for the whole pool. **Operator-facing CLI contract
  change:** the global `--host-rype-reference-idx` / `--host-minimap2-reference-idx`
  flags are removed (host filtering is per-sample now). Samples with a recorded
  `host_rype_reference_idx` are depleted against it (plus their optional minimap2
  reference); samples with none recorded (preflight `human_filtering=0`) get a
  QC-only `host_filter_enabled=false` pass-through ticket — the first fan-out path
  for unfiltered samples. The gesture pre-flights each distinct recorded reference
  (ACTIVE + the required index) once up front, so a misconfiguration aborts with
  zero side effects. Part of #146 (#158)
- Stripped this repo's GitHub issue/PR numbers from code comments, docstrings,
  and string literals across all components (comment-only; no behavior change),
  and recorded the convention in `CLAUDE.md`: provenance lives in git / CHANGELOG
  / the PR, not the source. External-tracker refs (e.g. `DuckDB #23229`) and the
  `(#N)` tags in CHANGELOG/DEPLOY_CHECKLIST are kept (#150)
- The `stage_local_fasta` step in `local-host-reference-add/1.0.0` now requests
  `cpu: 4` / `mem_gb: 64` (was `cpu: 8` / `mem_gb: 32`) — fewer cores, more
  memory for staging many host FASTA files into one chunked Parquet. Still within
  the action's `cpu: 16` / `mem_gb: 64` ceiling (#140)
- All Parquet writes now add `ROW_GROUP_SIZE_BYTES '64MB'` — row groups flush at
  ~64 MB encoded size instead of buffering one large group, sharpening row-group
  predicate pushdown (tighter per-group min/max) and lowering peak write memory.
  The canonical `PARQUET_OPTS` / `PARQUET_OPTS_INTERMEDIATE` constants moved to
  `qiita_common.parquet` (single-sourced for both services); the orchestrator
  re-exports them and derives `PARQUET_OPTS_CHUNKED`, and the control-plane
  `mint_features` write now imports `PARQUET_OPTS` instead of hardcoding the
  string. The option requires `preserve_insertion_order=false`, already set on
  every orchestrator pipeline connection via `apply_duckdb_settings`; the
  control-plane write now sets it explicitly. Output stays clustered on each
  COPY's `ORDER BY` key (what pruning reads), so the sorted-result contract is
  unaffected. The Rust data-plane DoPut writer is unchanged — parquet-rs has no
  byte-based row-group knob (#140)
- Bumped the pinned DuckDB across all components from **1.5.3** to **1.5.4** to
  track the team miint mirror's current build. Python floor raised to
  `duckdb>=1.5.4` in control-plane, compute-orchestrator, and integration tests
  (locks regenerated); data-plane Rust crate `1.10503.1` → `1.10504.0` (DuckDB
  1.5.4); the `setup-libduckdb` action default and the `deploy.yml` extension
  cache key moved to `1.5.4` so CI links a matching libduckdb. The miint mirror
  already publishes v1.5.4 builds for `linux_amd64` and `osx_arm64`. The
  `test_duckdb_version_sync` guard keeps the crate, action default, and cache key
  in lockstep (#138)
- The `stage_local_fasta` native job now caps `read_fastx`'s per-batch buffer at
  128MB (was 512MB), lowering peak memory during FASTA staging. One of the job's
  three memory levers, alongside the DuckDB `memory_limit`/`temp_directory` spill
  and the Parquet write buffer (#137)
- Retired the manual "rebuild the SIF" deploy step now that the deploy
  auto-builds. `/deploy-note` and `CLAUDE.md` ("Container image tier") now direct
  a container-artifact change to a Notes entry + an optional verify, never a
  bucket-2 manual build — the auto-build's content hash picks the change up on the
  next deploy. Bucket 2 keeps only genuinely new host setup the build depends on
  (e.g. staging a new licensed source). The out-of-band manual `build-sif.sh` is
  documented as a root-only escape hatch (`apptainer build` mounts the caller's
  home; `qiita-orch`'s is `/dev/null`), and `/deploy-note` now requires any
  `apptainer exec` verify to be home-/cwd-independent (`cd` + `--no-home`). (#134)
- The deploy now builds container SIFs automatically. `activate.sh` runs a new
  `deploy/build-sifs.sh` after the rsync and before any service restart: it
  iterates `workflows/*/sif-build.env`, builds each via the existing generic
  `scripts/build-sif.sh` (as root), then chowns the produced SIF to `qiita-orch`.
  It is idempotent — `build-sif.sh` now also stamps a content hash of the in-repo
  build inputs (`Apptainer.def`/`entrypoint.sh`/`manifest_writer.py`) next to the
  SIF, so an edit to any of those (which `VERIFY_MATCH`, version-only, could not
  see) triggers a rebuild without the old manual `FORCE=1`. Missing prerequisites
  (no `apptainer`, no `PATH_DERIVED`, an unstaged licensed `SOURCES`, or
  `AUTO_BUILD=0` in a spec) clean-skip an image; only a real build/chown failure
  aborts the deploy, before any restart. `local-deploy.sh` now also rsyncs
  `scripts/` into the staging tree so the CI deploy path can build too
- `qiita submit-host-filter-pool` now fans out fastq-to-parquet/**1.2.0** (QC +
  two-reference host filter) instead of 1.1.0. `--host-reference-idx` is replaced
  by `--host-rype-reference-idx` (required) and `--host-minimap2-reference-idx`
  (optional), each pre-flighted for ACTIVE status + its named index; the run's
  `instrument_model` is read once (GET /sequencing-run) and forwarded per sample
  so QC's polyG gate is set correctly (#129)
- Host filtering can now draw its two indexes from two INDEPENDENT references.
  The runner's `_resolve_host_filter_indexes` gained a two-reference layout
  (fastq-to-parquet/1.2.0): `host_rype_reference_idx` (required) supplies the rype
  `.ryxdi` and the optional `host_minimap2_reference_idx` supplies the minimap2
  `.mmi`, each from its own ACTIVE reference that MUST carry the named index (a
  designated reference missing its index is a hard error). The legacy
  single-reference `host_reference_idx` layout (1.1.0, ≥1-of-either, skip on
  missing) is unchanged and back-compatible; the two layouts are mutually
  exclusive (mixing them, or enabling with no reference key, is a clear
  SUBMISSION BAD_INPUT). `host_filter.py` itself is untouched — it still skips the
  stage whose index path is None (#129)
- `stage_local_fasta` now ingests the whole manifest in a single
  `read_fastx(VARCHAR[])` scan and streams read → `sequence_split` → Parquet
  without ever materialising sequences in a temp table. The previous per-file
  `INSERT … SELECT` staged every genome's bytes into a `reads` table and spilled
  hard when loading hundreds of human genomes; sanity checks (empty-body,
  duplicate read_id) now run over a small `(read_id, length, filepath)` table
  instead. The duplicate-read_id error names the offending files;
  read_id stays globally unique (#128)
- Raised compute resources for genome-scale reference loads in the
  `local-reference-add` and `local-host-reference-add` workflows:
  `stage_local_fasta` and `hash_sequences` to cpu=8/mem_gb=32,
  `build_rype_index` to cpu=8 and `build_minimap2_index` to mem_gb=32, and step
  walltimes to PT24H under a PT48H `action_ceiling`. The matching DuckDB
  `_DUCKDB_THREADS` bumps (`hash_sequences`, `build_rype_index` → 8) keep the
  caps in lockstep so the extra cores are actually used (`build_minimap2_index`
  stays at 4 — minimap2 index build is single-threaded). The orchestrator's
  SLURM poll-loop timeout default rises 24h → 48h to allow the longer walltimes
  (override via `SLURM_JOB_TIMEOUT_SECONDS`) (#128)
- `make redeploy` no longer prompts to do work it has already proven is needed.
  The SLURM native-venv refresh now runs automatically (no confirm) when the
  native checkout is the same clone redeploy just pulled — the prompt remains
  only for a separate checkout, where redeploy is about to mutate a tree it
  didn't pull. miint staging is now gated like the native-venv refresh: a new
  `stage-miint --check` probe (`qiita_compute_orchestrator.miint_staging`) skips
  staging when the staged build still matches the mirror and stages
  automatically otherwise (not staged, DuckDB-version/platform change, or a
  mirror build bump detected via an HTTP `HEAD` on the extension URL + a
  fingerprint marker written at stage time). `FORCE_STAGE_MIINT=1` stages
  unconditionally; `SKIP_STAGE_MIINT=1` still skips entirely. Removes the two
  recurring deploy prompts that fired every run regardless of need (#127)
- The orchestrator's **derived-storage** path layout now has a single owner. The
  `{PATH_DERIVED}/references/{idx}/...` convention for the persistent host-filter
  indexes was reconstructed by hand in three places (`build_rype_index`,
  `build_minimap2_index`, and the `DELETE /reference-artifact/{idx}` purge
  endpoint); it now lives in one module, `qiita_compute_orchestrator/derived_store.py`
  (`reference_derived_dir` / `rype_index_path` / `minimap2_index_path`), which all
  three call. No behavior change — the paths are byte-identical — this names
  derived storage as an explicit orchestrator concern (distinct from the data
  plane's persistent DuckLake data and the ephemeral per-attempt workspace) and
  gives the in-tree-vs-out-of-tree boundary one home. `docs/architecture.md` gains
  the matching note: a derived/persistent artifact is never a step output (it
  can't resolve under `$QIITA_OUTPUT_PATH`), so its location travels in an in-tree
  meta JSON. Also corrects a docs/test drift — the minimap2 `reference_index.params`
  shape is `{preset, source_chunks, num_subjects}` (what `build_minimap2_index`
  actually writes), not the stale `{preset, source_files}` (#119)
- `deploy/redeploy.sh` (`make redeploy`) now **only stops to ask when there is
  real work or a real decision** — it no longer pauses on no-ops. The buckets
  1 & 2 acknowledgement (env vars + one-time host setup) is skipped when both
  are empty in `DEPLOY_CHECKLIST.md` (nothing to apply out-of-band → nothing to
  confirm), via a new unit-tested `qiita_buckets_12` helper in
  `deploy/_common.sh`. The SLURM native-venv refresh is skipped entirely — no
  prompt, no `uv sync` — when it can prove the venv is already current (the
  native checkout is the clone this run just pulled, that pull changed neither
  `qiita-common` nor `qiita-compute-orchestrator`, and the existing venv still
  imports); any doubt (a separate checkout, an actual code change, an unreadable
  checklist, or a failing import probe) falls back to prompting and refreshing
  exactly as before, so the optimisation never skips work a change requires.
  `FORCE_NATIVE_REFRESH=1` overrides the skip for the one case it can't see — a
  re-run after a deploy that died mid-`uv sync`. Both new decisions delegate to
  pure, unit-tested helpers in `deploy/_common.sh` (`qiita_buckets_12` and
  `qiita_paths_touch_native`), matching the existing
  `qiita_native_checkout_from_python` pattern. The migration gate, `RUN_MIGRATE`
  confirm, and miint-stage prompt are unchanged (#113)
- `deploy/redeploy.sh` (`make redeploy`) now **runs** the SLURM native-venv
  refresh in step 5 instead of only printing a reminder. It derives the
  `qiita-compute-orchestrator` checkout from `SLURM_NATIVE_PYTHON`, runs `uv sync
  --reinstall-package qiita-common` there as the checkout owner (`qiita`, never
  root — a root-owned `.venv` is the #80 footgun), and fails loud if the synced
  venv can't import `qiita_common` / `qiita_compute_orchestrator.jobs`. It skips
  cleanly when `SLURM_NATIVE_PYTHON` is unset (local backend) and aborts rather
  than `uv sync` a wrong path; `SKIP_NATIVE_REFRESH=1` opts out. This closes the
  recurring footgun where a deploy that changed `qiita-common` /
  `qiita-compute-orchestrator` left native jobs importing stale code unless the
  operator remembered to refresh by hand. The derivation lives in a pure
  `qiita_native_checkout_from_python` helper in `deploy/_common.sh` (unit-tested)
  (#106)
- `deploy/redeploy.sh` (`make redeploy`) is now an all-in-one **root-run**
  orchestrator: run it as `sudo make redeploy` from the admin account and it
  `sudo -u`'s into the operator (`qiita`) for pull/migrate and into the service
  accounts (`qiita-api`/`qiita-orch`) for the verify checks. This fixes the
  prior "run as the operator, elevate via sudo" model, which could not work on
  the documented default where the operator account has no sudo. It also reads
  `DATABASE_URL` from `control-plane.env` itself (handing it to the operator's
  `make migrate`), so the operator's shell no longer needs it and the #72 ACL
  is no longer required for a normal redeploy. Migrations stay out-of-band
  (`RUN_MIGRATE=1` opts in after a typed confirm). `deploy/verify.sh` also gains
  `QIITA_API_USER` / `QIITA_ORCH_USER` overrides (defaults unchanged) for
  consistency with the new `QIITA_USER` knob. The deploy scripts'
  copy-pasted helpers (root-gate, env-file reader, operator/clone resolution,
  pass/fail/skip reporters, `/etc/qiita/*.env` path + service-account constants)
  are consolidated into `deploy/_common.sh` — single-source, no behavior change.
  Docs updated in `redeploy.md` / `first-deploy.md` / `CLAUDE.md` (#101)
- The `collection_date` global biosample field is now a `text` field instead of
  a formal `date`, so it can hold partial dates such as a bare year (`2025`)
  (migration `20260616000000_collection_date_text`) (#98)
- Pruned the seeded `prep_sample_global_field` registry to the two fields
  actually in use: removed the seven fields (`alias`,
  `library_name`, `library_strategy`, `library_source`, `library_selection`,
  `library_layout`, `library_construction_protocol`) , all of which but alias should come from sequenced_pool, and made the retained
  `title` and `design_description` optional (migration
  `20260616000002_prune_prep_sample_global_fields`) (#98)
- The `qiita.sequenced_pool.idx` identity sequence now starts at 25000,
  reserving `[1, 25000)` for legacy-Qiita import rows (matching the existing
  `study` / `prep_sample` reservation) (migration
  `20260616000001_sequenced_pool_idx_bump`) (#98)
- Reference index artifacts now live under a new orchestrator `PATH_DERIVED`
  root (`{PATH_DERIVED}/references/{idx}/{rype,minimap2}/…`), relocated from
  `PATH_SCRATCH`. `build_rype_index` / `build_minimap2_index` read
  `Settings.path_derived` and the SLURM backend propagates `PATH_DERIVED` into
  the job env (no host references exist in prod, so no migration of existing
  artifacts) (#89)
- The runner's `register-index` action reads its YAML-declared input
  (`entry.inputs[0]`) instead of a hardcoded `rype_index_meta`, so one workflow
  can register multiple index types (rype + minimap2) from their own metas (#89)
- `ActionDefinition` now rejects duplicate `step:` entry names within an action
  at load time — SLURM job naming and in-flight job adoption (`_find_job_by_name`)
  key on the entry name, so two same-named steps would collide silently.
  `action:` entries run in-process (keyed on step index) and may still repeat,
  e.g. the two `register-index` actions in the host-reference workflows (#89)
- The compute service-account name is documented as **site-chosen** (`compute`
  on the live deploy) across the provisioning/rotation runbooks, `docs/auth.md`,
  `first-deploy.md`, `CLAUDE.md`, and the orchestrator `config.py` comments —
  the docs no longer imply a fixed `compute-worker` name that drifts from the
  live SA (#72)
- Operators now get a narrow POSIX ACL read on the three `/etc/qiita/*.env`
  files (granted to the existing operator account, e.g. `u:qiita:r`; not the
  bearer tokens, not lake data), so `make migrate` can source `DATABASE_URL` and
  config consistency is verifiable without sudo or hand-copied secrets. Operators
  still join no service group, preserving DuckLake/scratch isolation. Documented
  in `first-deploy.md` §0.1 + the deploy checklist (#72)
- The study lookup-by-accession default is now `bioproject_accession`
  (was `ena_study_accession`): callers omitting `accession_field` resolve
  against the BioProject column. This aligns the `qiita submit-bcl-convert`
  preflight, whose project accessions are BioProject identifiers, with the
  column it actually matches (#91)
- miint is no longer installed lazily on every compute run. The deploy stages
  the extension **once** into a shared `MIINT_EXTENSION_DIRECTORY`
  (`scripts/stage-miint-extension.sh` →
  `python -m qiita_compute_orchestrator.cli.stage_miint`), and the CO service,
  all five native jobs, and the compute-readiness probe only `LOAD` it
  (`miint.open_miint_conn`) — no per-job download, no compute-node mirror
  dependency, no writable-`$HOME` requirement (the latter was the deploy
  footgun: a slurmrestd job has no login `$HOME`, so `FORCE INSTALL` couldn't
  write `~/.duckdb`). New orchestrator env var `MIINT_EXTENSION_DIRECTORY` (see
  `DEPLOY_CHECKLIST.md`). The client-side `qiita reference load` CLI keeps an
  install but plain + cached (was `FORCE INSTALL`, which re-downloaded every
  invocation). `miint_install_sql()` is now plain `INSTALL` with opt-in
  `force=` for deploy staging; new `miint_load_sql()` / `miint_job_env()`
  single-source the load + remote-job-env contract (#90)
- The compute-readiness probe now reports *why* a miint check failed (the
  captured DuckDB/Python error) instead of a swallowed bare `=fail`, and LOADs
  the staged build exactly like the native jobs — a broken miint deploy is now
  diagnosable from the probe output alone (#90)
- The data-plane `miint_extension_smoke` test installs from the team mirror
  (honoring `MIINT_EXTENSION_REPO` / `MIINT_EXTENSION_DIRECTORY`) instead of the
  hardcoded `community` channel, so it verifies the same build the rest of the
  system runs (#90)
- The bcl-convert flow now derives the instrument run ID and model from the
  run folder's top-level `RunInfo.xml` (`Run@Id` plus the `Instrument` serial number
  resolved against the vendored prefix table) instead of parsing the folder
  basename, which operators rename. Both the `qiita submit-bcl-convert` CLI
  and the orchestrator's `bcl_convert_prep` step fail fast on a missing or
  malformed `RunInfo.xml` (#88)
- Renamed the study EBI accession to ENA across the stack: the study table
  column and its UNIQUE constraint (`ebi_study_accession` →
  `ena_study_accession`), the REST request/response field of the same name,
  and the `qiita study create`/`patch` CLI flag (`--ebi-study-accession` →
  `--ena-study-accession`). Clients sending the old field name must update (#87)
- Sequence chunking now uses miint's native `sequence_split` (single linear
  pass) instead of the pure-SQL `list_transform`/`substring` macro, which was
  **O(L²)** on large single records (host reference genomes) due to DuckDB
  #23229 — inside a lambda a captured column loses the statistics that select
  `substring`'s O(1) ASCII fast path, so it rescans from byte 0 on every chunk.
  Affects `stage_local_fasta` and the CLI `reference load` FASTA path; ~480×
  faster on a 256 MB record. Requires the miint build with `sequence_split`
  (duckdb-miint #121) on the mirror — gated in `DEPLOY_CHECKLIST.md` (#86)
- Completed the DuckDB 1.5.3 bump (#85) by updating the three spots it missed:
  the `setup-libduckdb` action default (`1.5.2` → `1.5.3`, so the Rust data-plane
  CI links the libduckdb matching its `1.10503.1` crate), the `deploy.yml`
  extension cache key, and the `v1.5.2` mentions in `docs/architecture.md` and
  the data-plane README. Added a CI guard (`test_duckdb_version_sync`) asserting
  the action default and deploy cache key match the data-plane `duckdb` crate, so
  a future DuckDB bump can't half-land (#86)
- Bumped the pinned DuckDB across all components to **1.5.3** to match the team
  miint mirror's current build: `duckdb>=1.5.3` in control-plane,
  compute-orchestrator, and the integration tests (locks re-resolved), and the
  data-plane Rust crate `1.10502.0` → `1.10503.1`. The compute env was on DuckDB
  1.5.2, so the native `stage_local_fasta` job installed the **stale** `v1.5.2`
  miint build from the mirror instead of the current `v1.5.3` one — DuckDB
  resolves the miint extension for its *own* version, so running 1.5.3 is the
  only way onto the current build (#85)
- Bumped the pinned `run-preflight` dependency to a newer upstream SHA in both
  the control-plane and compute-orchestrator, kept in lockstep by the SHA parity
  test (#82)
- `matrix_tube_id` must now be exactly 10 digits (previously 8–10), tightened on
  both the Pydantic field pattern and the `qiita.biosample` column CHECK (#81)
- Biosample/sequenced-sample create and biosample patch now take a checklist
  **name** (e.g. `ERC000015`) instead of a `metadata_checklist_idx`, resolving
  it to the idx server-side and returning a clean 422 for an unknown name —
  mirroring how terminology term_ids resolve. CLI flag is now
  `--metadata-checklist-name` on `biosample create`, `biosample patch`, and
  `sequenced-sample create` (#81)
- `BiosampleResponse` and `SequencedSampleResponse` now carry the checklist as
  a `metadata_checklist` ref (`{idx, name}`, where name is the ENA accession)
  instead of a bare `metadata_checklist_idx`, mirroring the missing-reason /
  terminology-term read-back refs (#81)

### Removed

- **Per-shard rype build (C2a).** The whole-reference `rype_router` replaces the
  per-shard `.ryxdi` for read routing, so the vestigial per-shard rype build is
  gone: `build_rype_index` is now host/whole-reference only (its SHARD-mode
  Inputs, both-or-neither validator, streaming branch, shard bucket, and `plan()`
  shard sizing removed); `build-shard-index` drops the `build_rype_index` step +
  its `register-index` + the `build_rype`/`rype_w` context keys (per-shard indexes
  are now minimap2 + bowtie2 only); `reference-add` / `local-reference-add` drop
  `build_rype`/`rype_w` from their sharded context (rype/`rype_w` now apply to
  `--host` only in the CLI); and `derived_store` drops `shard_rype_index_path` +
  `reference_shard_dir`. (#268)

- **`qiita-admin work-ticket backfill-mask-idx` retired.** The one-time mask_idx
  backfill (for tickets predating the column) has run in production; the CLI
  command — the only ticket signer outside the control-plane web process, which
  read the raw signing key from the environment — and its `backfill_work_ticket_mask_idx`
  runner helper are removed. `mask purge-failed`'s NULL-mask_idx safety guard is
  unchanged; only its guidance text (which named the removed command) is
  reworded. (#263)
- Dead SLURM poll/timeout config in the compute-orchestrator (`SlurmBackend` `poll_interval_seconds` / `job_timeout_seconds`, their `SlurmSettings` fields, the `SLURM_POLL_INTERVAL_SECONDS` / `SLURM_JOB_TIMEOUT_SECONDS` env vars, and the `DEFAULT_SLURM_*` constants) — assigned but never read since the CP took over the poll loop. (#241)
- **`.github/workflows/deploy.yml`** — the unused `v*`-tag auto-deploy workflow.
  It SSH'd to `$DEPLOY_HOST` and ran a real production deploy on any `v*` tag
  push, but production has only ever deployed manually via `deploy/local-deploy.sh`
  / `redeploy.sh` — so it was a latent footgun (a stray release tag could trigger
  an unattended deploy onto a host that hadn't done the bucket 1–3 pre-steps).
  Reconciled the now-contradictory deploy docs + script comments
  (`docs/runbooks/first-deploy.md`, `docs/architecture.md`,
  `deploy/{activate,local-deploy,build-sifs}.sh`) to state plainly that deploys
  are manual and there is no CI/tag-triggered deploy path. (#233)

### Fixed

- **`plan-shards` is now genuinely opt-in (B5).** The `when: shard_index` gate
  defaults ON for an absent key (correct for the `build_*` gates, which default to
  building all index types), so `plan-shards` ran on a plain `reference-add` that
  never set `shard_index` — fanning out (or, in production with a dispatch
  callback, even sharding a genome-bearing reference nobody asked to shard). The
  runner's `plan-shards` arm now self-defends — no-op when `bound.get("shard_index")`
  is falsy, mirroring the finalize `_is_sharded_fanout_in_progress` check — before
  the fan-out precondition (`dispatch_cb`) is required. `when: shard_index` is kept
  as the explicit-opt-out gate. Fixes the two reference-add smokes. (#268)

- **Data plane: ambiguous `feature_idx` under the reference-membership JOIN.**
  `build_query` qualified only `reference_idx` (with the membership alias `m.`)
  when a `reference_sequences` / `reference_sequence_chunks` filter triggered the
  membership JOIN, leaving any other column unqualified. A combined
  `{reference_idx, feature_idx}` filter — exactly what the B6 `feature_idx`-scoped
  DoGet ticket mints — then failed to bind with "Ambiguous reference to column name
  feature_idx" (`feature_idx` exists on both joined tables). The non-reference_idx
  columns are now qualified with the base-table alias `t.`. (#268)

- **`feature_idx`-scoped DoGet ticket (B6).** `POST /reference/{idx}/ticket/doget`
  gains an optional `feature_idx` subset on its request body: omitted ⇒ today's
  whole-reference ticket (`filter={"reference_idx":[idx]}`), byte-identical;
  present ⇒ the ticket additionally scopes to those features
  (`filter` gains `"feature_idx":[...]`, bounded at 100k) so a shard builder
  streams only its own roster's sequences from `reference_sequences` /
  `reference_sequence_chunks`. The status gate now admits `active` **and**
  `indexing` (a shard build streams mid-ingest, post-`register-files`);
  `pending`/`loading` stay 409, missing stays 404. No new route (the existing
  `URL_REFERENCE_DOGET` triple is reused), no migration, no data-plane change
  (`feature_idx` filtering already exists and is tested there). (#268)

- **Per-shard rype `.ryxdi` build (parameterized `build_rype_index` + `plan()`).**
  The `build_rype_index` native job gains an optional **shard mode**: given a
  `shard_id` and a runner-staged feature roster (`shard_features`, a Parquet of
  `(feature_idx, sequence_length_bp)`), it builds one shard's rype `.ryxdi` routing
  index over just that shard's features and writes it to
  `{PATH_DERIVED}/references/{idx}/shards/{shard_id}/index.ryxdi`
  (`derived_store.shard_rype_index_path`), recording `shard_id` in the meta JSON
  (the register-index arm already threads it, B1). Both shard fields unset =
  today's whole-reference host build, byte-identical. Adds a `plan()` that sizes
  the shard build's `mem_gb` down from the whole-reference baseline (floored at the
  runtime-consistent rype+DuckDB+headroom minimum, scaled by the shard's total bp),
  so a fleet of small shards doesn't each grab the 64 GB whole-reference slot.
  Re-verified `rype_index_create` against upstream miint (`docs/duckdb-miint.md`
  refreshed). No shard fan-out / workflow wiring yet — that's a later milestone;
  16S/no-genome sharding deferred. (#268)

- **Lineage-sorted shard planner + `reference_membership.shard_id` persistence.**
  The deterministic partition of an analysis reference's features into shards. A
  pure tiler `qiita_control_plane.shard_planner.tile_by_lineage(items, num_shards)`
  sorts the sharding units lexicographically by taxonomy lineage string and cuts
  the sorted list into a fixed `_SHARD_COUNT = 1000` approximately-even shards
  (fixed count / variable size — the mirror image of the read-block planner). The
  tiler is generic over units (the caller chooses `item_id = genome_idx` for a
  genome reference so shards balance by genome count and a genome's contigs stay
  together); it is deterministic and re-derivable (internal `(lineage, item_id)`
  sort). A new nullable `qiita.reference_membership.shard_id INTEGER` (+ a `>= 0`
  CHECK) records the assignment (NULL = unassigned/unsharded), written by
  `write_shard_assignment` (idempotent, replay-safe, reference-scoped). No shard
  builds, fan-out, routing, or workflow wiring yet — those are later milestones;
  16S / no-genome sharding is deferred. (#268)

- **Per-shard `reference_index.shard_id` + register/GET/derived-store plumbing.**
  The foundation for per-shard *analysis* reference indexes: `qiita.reference_index`
  gains a nullable `shard_id INTEGER` (+ a `>= 0` CHECK) so a sharded index writes
  one row per shard (`shard_id` 0..N-1) while the existing unsharded host index
  keeps `shard_id` NULL. Additive and backward-compatible — no `shard_count`
  (it's `COUNT(*)` per `(reference_idx, index_type)`), no new UNIQUE (the
  `(reference_idx, index_type, fs_path)` idempotency key already dedups
  path-distinct shard rows). `register_index` and `GET /reference/{idx}/index`
  thread `shard_id`; the runner's register-index arm reads it from the meta JSON
  (`meta.get` → NULL for host metas); the whole-reference resolver
  (`_resolve_reference_index_path`) filters `shard_id IS NULL` so a shard row is
  never served as the unsharded index; and `derived_store.reference_shard_dir`
  lays down the `references/{idx}/shards/{shard_id}/` layout (under the existing
  purge subtree). No shard builds, planner, or routing yet — those are later
  milestones. (#268)

- **`genome_source` controlled vocabulary + qiita-origin sample link.** `qiita.genome.source`
  is now a closed vocabulary (`genbank`, `refseq`, `qiita`), enforced both up front at ingest
  (fail-fast in `_associate_genomes`, before any DB write) and by a `CHECK`, mirrored by the new
  `GenomeSource` `StrEnum`. Qiita-derived genomes (`source='qiita'`) now record the exact
  originating sample via a new nullable `qiita.genome.prep_sample_idx` (FK to `qiita.prep_sample`,
  required iff the source is `qiita` — a biconditional `CHECK`); the genome-map Parquet gains an
  optional `prep_sample_idx` column. (#268)

- **Reference-load's sequence-chunk re-key no longer sorts `chunk_data` (fixes a
  GG2-scale OOM).** `_write_reference_sequence_chunks` re-keyed hash → feature_idx
  by materializing each batch and sorting it `ORDER BY feature_idx, chunk_index` —
  putting the 64 KB `chunk_data` rows through a sort, the exact anti-pattern
  `hash_sequences` was built to avoid when writing the same table shape upstream.
  The parallel sort's working set ballooned far past the batched input and OOM'd
  DuckDB at genome scale (a sort can't spill rows that fat). It now writes each part
  with a SINGLE streaming COPY — the narrow per-batch `feature_map` subset is the
  hash-join build side and `chunk_data` rides the probe straight to the writer,
  never buffered into a build or a sort (peak ~1 GB/thread, constant in file size).
  File-level DuckLake pruning is preserved by bin-packing features into disjoint,
  contiguous feature_idx ranges (one per part); the within-part sort is dropped
  (on-disk order isn't load-bearing — reassembly sorts `chunk_index` in memory and
  DoGet filters by feature_idx), and `_CHUNK_BUDGET_PER_BATCH` drops 50k→10k to keep
  the per-part ranges narrow for pruning. (#268)

- **`long-read-assembly` could never complete a run**, for two independent
  reasons. Container dispatch was gated to `{reference, sequenced_pool}`-scoped
  tickets, so all four of its `prep_sample`-scoped container steps (assemble /
  binning / bin_refine / checkm) failed `CONTRACT_VIOLATION` at submit;
  `prep_sample` is now admitted, which costs nothing else because the dispatch
  path already treated `scope_target` opaquely. And the `checkm` step never
  declared its operator-staged reference DB, so no bind was computed and
  `checkm.sh` fell back to an `/opt/checkm_data` absent from the SIF (exit 78) no
  matter how correctly the DB was staged; it now rides the new `derived_inputs`
  field. A pin test (`test_workflow_container_scope_pin`) walks every workflow
  YAML and fails `make test` when a workflow declares container steps under a
  kind the backends won't dispatch — both bugs only surfaced on a live submit,
  and this is the static check that catches the next one at CI. The operator
  steps this shipped with were also unrunnable as written; `DEPLOY_CHECKLIST.md`
  is corrected (an unset `$PATH_DERIVED` that would have `mkdir`ed `/checkm_data`
  at the filesystem root, an md5 that only printed instead of verifying, and an
  Ed25519 keygen one-liner that needs cryptography >= 38 but ran under a system
  `python3` that can be older). (#273)
- **Apptainer arguments are shell-quoted.** The `apptainer exec` line is
  interpolated into a bash script, and its `--bind` paths, `--env` values, image
  path and entrypoint all originate in workflow YAML. Unquoted, a `;` or a space
  in any of them would terminate the command and run whatever followed. Every arg
  is now `shlex.quote`d at the one place they become shell text. Derived-artifact
  binds are also mounted `:ro` — one shared copy (CheckM's 1.4 GB DB) is read by
  every concurrent job, and a writable mount let one container corrupt it for all
  of them. (#273)
- The workflow runner no longer strands a work ticket on a pre-loop failure. The action fetch, PENDING→PROCESSING transition, workspace `mkdir`, and step-progress load now run inside the failure-handling `try`, so an action disabled between submit and dispatch (or a DB/filesystem blip there) transitions the ticket to FAILED (attributed to the `submission` stage) instead of leaving it stuck in PENDING/PROCESSING with no failure recorded. (#242)
- **CLI-login plaintext PATs are no longer stored at rest.** `cli_login_code.plaintext_pat` is scrubbed the instant an ot_code is redeemed and a background sweeper deletes consumed/expired rows; previously a consumed row kept a usable bearer token for the token's full life (up to 90 days). (#241)
- `sign_ticket` rejects an empty Flight-ticket filter (which the data plane treats as `SELECT * FROM <table>`) at the signing boundary, not just per-route. (#241)
- DB constraint/trigger violations (principal disable/retire, prep_sample publication lock) return stable client messages instead of leaking internal constraint/trigger names. (#241)
- `verify_api_token` retains its fire-and-forget `record_token_use` task (was GC-droppable before the `last_used_at` write landed); `_parse_job` guards a null `exit_code` (was an untyped `AttributeError`); the SLURM payload rejects `gpu>0` at submit instead of silently dropping it. (#241)
- Doc drift: architecture.md marks the unbuilt processing-results subsystem as *planned*; CLAUDE.md's data-plane test example names a real test; auth.md corrects the empty-`Bearer` behavior. (#241)
- Integration-test harness: the `data_plane` fixture's gRPC-startup wait ceiling
  is raised from 10s to 30s (override via `QIITA_DP_START_TIMEOUT_S`), fixing an
  intermittent `test-integration` setup failure where the first module to use the
  fixture paid the coldest start (catalog reset → boot → load DuckDB + miint →
  create DuckLake tables) and occasionally exceeded the old window on a loaded CI
  runner. The poll still returns the instant the port opens, so the higher ceiling
  costs nothing on success; the timeout message now names the pid/port and is
  honest about the actual timeout instead of a hardcoded "10s". (#202)
- A transient control-plane **Postgres** error on the workflow runner's own DB
  calls — most often a per-statement `command_timeout` (a bare
  `asyncio.TimeoutError`) on the poll loop's force-fail check under a lock wait /
  checkpoint / load spike, or a brief connection blip — no longer permanently
  fails an otherwise-healthy work ticket (which orphaned the still-running SLURM
  job and left its output unregistered). The poll loop now gives that cheap read
  a generous per-call timeout and retries it in place a few times, so the common
  brief hiccup is absorbed without abandoning the job; and if a transient DB
  error still escapes any other runner DB call, `run_workflow` records it
  `failure_type='retriable'` (not `permanent`) — once Postgres is reachable again
  to write that row — so a `/run` redrive re-attempts. A real SQL error
  (constraint violation, query bug) is unaffected and stays permanent. (#214)
- CLI HTTP subcommands (`qiita` / `qiita-admin`) now print a friendly, actionable
  message when the control plane is unreachable — a connection refusal, DNS
  failure, TLS error, or timeout — instead of dumping a raw `httpx` traceback.
  The message names the target URL and points at `--base-url` /
  `$QIITA_CONTROL_PLANE_URL`, so a wrong base URL or a down server is obvious.
  `run_http_subcommand` gains an `httpx.RequestError` branch alongside the
  existing `HTTPStatusError` (a non-2xx *response*) handling. (#120)
- A step that OOM-kills (or times out) while **already at its action resource
  ceiling** no longer burns its remaining retry budget re-running at the same
  size. The runner escalates memory on `OOM_KILLED` and walltime on `TIMEOUT`,
  but once the floor is pinned at the ceiling there is no larger allocation to
  try — a re-run would fail identically. The runner now detects that the
  escalation can't grow and fails the ticket immediately with a new permanent
  `RESOURCE_CEILING_EXHAUSTED` failure kind (failure_type `permanent`), whose
  reason names the ceiling and tells the operator to raise it or shrink the
  input — instead of looping through every retry to the same OOM/timeout. (#210)
- A transient HTTP 5xx or network error on the per-sample `POST /sequence-range`
  callback the native `ingest_reads` and `fastq_to_parquet` steps make back to
  the control plane no longer permanently fails the whole pool ingest. Each
  callback now gets a small in-job bounded retry (on a 5xx / 408 / 429, or an
  httpx transport error like a connection reset / read timeout), so a single
  blip on one of a pool's N per-sample callbacks self-heals instead of
  discarding hours of demux and every already-ingested sample. If the retries
  exhaust, the step raises a new retriable `CONTROL_PLANE_UNREACHABLE` failure
  (the CO→CP mirror of `ORCHESTRATOR_UNREACHABLE`) so the runner re-dispatches
  the idempotent step, rather than the old `UNKNOWN_PERMANENT` that consumed no
  retries. 401/403 stay permanent (`CONTRACT_VIOLATION` — a token/scope misconfig
  a retry can't fix) and other 4xx stay `UNKNOWN_PERMANENT`. The retry +
  classification is shared by both steps via `sequence_range_retry` so they
  can't drift. (#212)
- `submit-host-filter-pool` no longer abandons the rest of a pool when one
  sample's `POST /work-ticket` fails. The per-sample fan-out now isolates each
  POST: a transient 5xx, a 409 in-flight, or a network blip is recorded and the
  fan-out continues to the remaining samples, the summary lists every submitted
  and failed sample, and the command exits non-zero if any failed — instead of
  an uncaught raise silently stranding every later sample (those after the
  failure in `sequenced_pool_item_id` order). New `--only-missing` flag submits
  only samples that have no read-mask ticket yet (via a new `has_read_mask_ticket`
  field on the pool- and run-scoped sequenced-sample list responses), so a pool
  whose prior fan-out was interrupted can be filled in without duplicating
  already-submitted work; off by default so a deliberate re-submit against a
  different host reference still fans out pool-wide. (#218)
- Deleting a sequenced_pool now purges the DuckLake data its prep_samples
  produced, not just the Postgres rows. `DELETE
  /sequencing-run/{R}/sequenced-pool/{P}` (`qiita delete-sequenced-pool`) issues a
  new `delete_pool_reads` data-plane DoAction that drops the `read` and
  `read_mask` rows keyed by the pool's prep_sample set (one DuckLake transaction,
  idempotent, retriable — same data-plane → Postgres-last ordering as DELETE
  /reference, so a Flight failure 502s with nothing removed), and the control
  plane reaps the durable `reads/{prep_sample_idx}/read.parquet` staged copies
  on disk. Previously the delete left those reads/files orphaned in DuckLake on
  every pool delete — a storage leak and a surprise for operators who expect a
  `--force` delete to be complete. The response and CLI help now report the
  DuckLake/disk counts (`read_rows_deleted`, `read_mask_rows_deleted`,
  `staged_reads_reaped`). Reclaiming the orphaned Parquet bytes the logical
  DuckLake delete leaves behind remains a future maintenance pass (as with
  reference delete); pre-existing orphans from past deletes are not swept. (#204)
- sequenced_pool find-or-create now keys on the preflight **content**
  (`run_preflight_sha256`, a STORED generated column) instead of its filename, so
  a byte-identical preflight re-uploaded under a different basename resolves to
  the same pool instead of minting a duplicate (the root cause of the run-15
  duplicate pools). Adds the `sequenced_pool_one_per_run_and_hash` partial unique
  index and repoints `insert_sequenced_pool`'s `ON CONFLICT` to it. The existing
  `sequenced_pool_one_per_run_and_filename` index is kept as an independent,
  permanent uniqueness rule — distinct pools in a run must differ in both content
  and filename, so a different-content upload reusing an existing filename is a
  409 by design (never a 500). (#206)
- `qiita-admin masked-read-export` no longer floods stderr with Arrow Acero
  `An input buffer was poorly aligned` warnings (one per column per batch). The
  PyArrow Flight client zero-copies the gRPC message body, whose absolute base
  address carries no element-alignment guarantee, so a column buffer routinely
  lands off its natural alignment even though the data plane writes 64-byte-aligned
  IPC; DuckDB then scans the registered reader through `pyarrow.dataset` → Acero,
  which warns. The fastq path (which still uses DuckDB+miint) now asks the Flight
  reader to realign each buffer to its type's required alignment on receive
  (`IpcReadOptions(ensure_alignment=DataTypeSpecific)`, copying only the small
  offset/validity/fixed-width buffers); the parquet path bypasses DuckDB/Acero
  entirely (see Changed). Benign on x86_64 (output was always correct) — this only
  silences the noise. Upstream: apache/arrow#37195. (#198)
- bcl-convert `ingest_reads` now retries transparently after an OOM mid-write.
  A pool sample whose range was minted by a prior attempt that then crashed
  before publishing its durable `read.parquet` (the classic case: OOM-killed
  writing an oversized sample) used to fail the retry with `prep_sample N has a
  sequence_range but no durable read.parquet … delete the prep_sample` — which
  defeated the runner's OOM memory-escalation, since the escalated attempt died
  on the one-shot mint contract before spending its extra memory. The step now
  reads the existing range back (`GET /sequence-range/{idx}`), validates it still
  covers exactly the FASTQ's read count, and reuses it, so the escalated retry
  completes with no orphaned range and no operator action. The GET route now
  accepts `sequence_range:mint` (via a new `require_any_scope` guard) in addition
  to `prep_sample:read`, letting the scope-minimal compute SA read back its own
  range; a count mismatch fails `BAD_INPUT`, a range deleted mid-retry fails
  `UNKNOWN_PERMANENT`. (#196)
- A dropped-row step re-run (a `run-preflight update-lane` redrive, or a `/run`
  redrive) no longer fails when its prior attempt dir is still on disk. The
  previous fix had the control-plane runner `shutil.rmtree` that dir, but a
  container step's output is owned by the SLURM job user with read-only (0550)
  dirs — the control-plane process can neither unlink nor chmod it, so the wipe
  died with `EACCES` ("could not clear stale attempt dir … Permission denied")
  and failed the ticket at `step_run`. The runner now **advances to a fresh
  attempt dir** instead of deleting the orphaned one (which is left intact for
  postmortem), reusing the existing per-attempt isolation rather than reaching
  into a foreign-owned tree. Resume-adoption still reuses a dir owned by a live
  progress row. (#195)
- `run-preflight update-lane` now actually takes effect on a `ticket run`
  redrive. Correcting a pool's preflight makes any samplesheet a *successful*
  `bcl_convert_prep` already produced stale, but a redrive fast-forwards that
  COMPLETED step (rebuilding its output from the workspace manifest), so the
  corrected lanes were never re-read and `bcl_convert` re-failed. The edit now
  drops the pool's COMPLETED `work_ticket_step` rows in the same transaction as
  the blob write, forcing the redrive to re-run from prep. Paired with a runner
  fix: a step that re-runs after its progress row was dropped (this path, or a
  `/run` redrive clearing failed rows) now clears its stale attempt dir first,
  so the prior run's read-only (0o440) output can't trip the output verifier or
  block the overwrite — guarded so resume-adoption never touches a live dir.
  (#193)
- `read-mask` (1.0.0) and `fastq-to-parquet` (1.3.0) workflows ran
  `persist-read-metrics` *after* `register-files`, but `register-files` MOVES
  `read_mask.parquet` out of the staging dir into permanent DuckLake storage —
  so `persist-read-metrics` re-opened a path that no longer existed and failed
  with `FileNotFoundError: read_mask parquet not found`. Reordered both
  workflows so `persist-read-metrics` reads the staged parquet first, then
  `register-files` moves it. (#181)
- The `qiita` / `qiita-admin` CLIs now emit an actionable error instead of a raw
  import-time traceback when launched against a **stale `qiita_common`** (the
  cross-package staleness trap: a plain `uv sync` skips reinstalling the
  unchanged-version path-dep, leaving stale sources in the venv). The console-script
  entry points now target a new import-clean shim (`qiita_control_plane.cli._bootstrap`)
  that imports the real CLI `main` lazily; a `qiita_common` `ImportError` is
  translated to a one-line message naming the exact fix
  (`uv sync --reinstall-package qiita-common`) and echoing the original error,
  while any unrelated `ImportError` is re-raised untouched (real bugs are never
  masked). Complements the `make redeploy` checkout-venv refresh above — this
  covers the case where the CLI is run without going through the redeploy script.
  The real `cli.user:main` / `cli.admin:main` are unchanged and still used by the
  shim, tests, and the redeploy import probe (#163)
- `make redeploy` now refreshes the operator's **checkout** CLI venv
  (`$QIITA_CLONE/qiita-control-plane/.venv`, where `uv run qiita` / `qiita-admin`
  resolve), closing a two-tree gap: `activate.sh` `uv sync`s only the `/opt/qiita`
  service venvs and the existing native-venv step covers only
  `qiita-compute-orchestrator`, so a pull that changed `qiita-common` without a
  version bump left the checkout CLI ImportError-ing on a stale path-dep until the
  operator ran `uv sync --reinstall-package qiita-common` by hand. A new step 6
  runs that reinstall as the checkout owner (never root), with a cheap skip when
  neither `qiita-common` nor `qiita-control-plane` changed in the pull and the venv
  still imports the CLI entrypoint (`FORCE_CLI_REFRESH=1` overrides). The skip
  delegates to a new pure `qiita_paths_touch_cli` helper in `deploy/_common.sh`
  (unit-tested), mirroring `qiita_paths_touch_native` (#163)
- `POST /work-ticket/{idx}/run` (`qiita ticket run`) can now redrive a FAILED
  multi-transition reference workflow instead of dead-ending at a `permanent`
  `IllegalStatusTransition`. The redrive resets a `failed` reference to `pending`
  (its only legal exit from `failed`) while keeping the COMPLETED step rows, but
  the runner's fast-forward used to *skip* those completed steps' `target_status`
  PATCHes — so the reference stayed at `pending` while the first re-run step tried
  to advance from mid-FSM (e.g. `minting → loading`), which is illegal. The
  fast-forward now RE-WALKS each completed step's status edge, advancing the
  resource forward along the FSM only when it is behind; on a normal
  startup-recovery resume (resource not rewound) the re-apply is a no-op or a
  rejected backward edge, both benign. Fixes redrives of `local-host-reference-add`
  / `host-reference-add` (which walk `pending → hashing → minting → loading →
  indexing → active`) after a `load`-step failure (#165)
- `mint-features` no longer starves the control-plane event loop on genome-scale
  reference loads. The in-process primitive read every `sequence_hash` from the
  manifest with a blocking, ORDER-BY (full-sort) DuckDB `fetchall()` and then
  built an O(N) Python list + dict + string-pair list — all on the single
  uvicorn event loop — so a human-comprehensive host reference pinned the API at
  high CPU and made every request (even a one-row `ticket status`) time out.
  Rewritten to stream the manifest in `_CHUNK_SIZE` batches (matching
  `write-membership`), drop the needless input sort, accumulate into a spillable
  DuckDB temp table de-duplicated at write time, and offload the final Parquet
  COPY to a thread. `_associate_genomes` likewise streams and resolves
  `feature_idx` via a DuckDB JOIN against the written feature_map instead of an
  in-memory mapping. The CP-side analog of the `hash_sequences` genome-scale fix
  below; output Parquet schema and idempotency are unchanged.
- `reference_load` no longer OOMs writing `reference_sequence_chunks` on
  genome-scale reference loads. Each per-part `COPY` did scan + join +
  `ORDER BY (feature_idx, chunk_index)` + write in one statement; the sort is a
  pipeline breaker, so it buffered the batch's wide ~64 KB chunk rows while the
  full ~30 GB glob scan and the 8-thread write buffers were all still live,
  blowing the cap (observed 38.7 GiB against ~39 GiB). Split into two phases per
  part: phase 1 streams the batch's chunks into a temp table (re-keyed
  hash → feature_idx, no sort), phase 2 sorts that isolated temp table (≤ one
  batch, never the 30 GB glob) and writes the part. The sort is kept on purpose
  — it clusters row groups so a `WHERE feature_idx IN (...)` DoGet prunes within
  a part, and feature_idx-ascending batches keep the parts a disjoint-range
  dataset for catalog-level file pruning; without it a point query would scan a
  whole part, since input order is parallel-scrambled upstream
  (`preserve_insertion_order=false`). Sibling to the `hash_sequences`
  genome-scale fix below.
- `hash_sequences` no longer OOMs writing `reference_sequence_chunks` on
  genome-scale reference loads. The per-batch output COPY joined the full
  `hashed` table (which grows 1:1 with the input), so at scale the optimizer
  could reorder that join ahead of the batch filter and materialize the entire
  file's `chunk_data` (observed 38 GiB against a 39 GiB cap). It also re-scanned
  the whole upload once per batch (~420× at 21M rows) and globally sorted by
  `sequence_hash` — a sort no consumer needs (`reference_load` re-keys to
  `feature_idx` with its own scan, the data plane's DoGet filters by
  `feature_idx`, and reassembly sorts `chunk_index` in-memory per feature).
  Replaced with a single streaming scan that relabels read_id → canonical
  `sequence_hash` in one pass: `canonical` (one narrow row per distinct hash) is
  always the lower-cardinality join input, so it's the hash-join build side and
  `chunk_data` streams through the probe to the writer — peak memory ~1 GB/thread
  and constant in file size, and the upload is scanned once instead of per batch.
  Output schema and canonical-dedup semantics are unchanged (one
  `part_00000.parquet` in the directory). (#155)
- `reference_load`'s per-batch chunk re-key (`sequence_hash` → `feature_idx`)
  carried the same latent OOM as the hash_sequences output side: each batch
  joined the full `feature_map` table, which grows 1:1 with the feature count,
  so at reference scale the optimizer could reorder that join ahead of the batch
  filter and materialize the whole glob's `chunk_data`. The join is now bounded
  to the batch's hashes by construction (an `fmb` CTE pre-filtered to the batch),
  so no join order can exceed one batch. The feature_idx-clustered, disjoint-range
  part layout (load-bearing for DuckLake / row-group pruning on DoGet's feature_idx
  lookups) and per-batch sort are unchanged. (#155)
- Native (`module:`) SLURM steps no longer collapse to a single CPU. The
  generated launcher ran a bare `srun`, but SLURM >= 22.05 srun no longer
  inherits `--cpus-per-task` from the batch allocation, so it laid the single
  task out at cpus-per-task=1 and its default `--cpu-bind` pinned that task —
  and every thread it spawned — to one allocated CPU. Native jobs run DuckDB
  with a multi-thread pool, so an N-CPU allocation silently ran on a single
  core (a TB-scale `stage_local_fasta` host-reference load crawled at ~75 MB/s
  on 1 of 4 allocated cores while the job's cgroup cpuset granted all of them).
  The launcher now exports `SRUN_CPUS_PER_TASK` from the allocation and passes
  `srun --cpu-bind=none`, letting the thread pool float across the whole cgroup
  cpuset (which already constrains the job to its allocation). Container
  (`apptainer exec`, no srun) steps were never affected. (#153)
- Data-plane file registration no longer collides with — and attempts to
  overwrite — an already-registered DuckLake data file. `register_files` placed
  each Parquet at `DATA_PATH/<table>/<producer-basename>`, but the reference-load
  job emits fixed basenames (`part_00000.parquet`, `reference_<table>.parquet`)
  on every load, so a second registration into a shared table targeted the first
  load's live, catalog-registered file. Because registered files are mode `0440`,
  the clobber surfaced on the live host as a cryptic `cross-fs copy failed …
  Permission denied` rather than silent lake corruption. The data plane now mints
  a unique, ticket-traceable destination name (`wt{work_ticket_idx}-{basename}`)
  — it owns lake-storage layout, as DuckLake does for its own INSERT-written
  files — and `move_file` refuses to overwrite an existing destination
  (`AlreadyExists`) as a hard safety net. The control plane threads the
  originating `work_ticket_idx` into the signed `register_files` action payload.
  Fixes loading more than one reference into a lake (and reloading). (#136)
- bcl-convert SIF auto-build no longer fails its `dnf install`. The
  `Apptainer.def` staged the licensed RPM to `/tmp/bcl-convert.rpm`, but a
  privileged `apptainer build` (the deploy's auto-build runs as root) bind-mounts
  the host `/tmp` over the image's `/tmp` during `%post`, shadowing the staged RPM
  → `Could not open … bcl-convert.rpm`. The RPM now stages to `/opt` (not
  bind-mounted), and `%post` runs under `set -e` with an explicit "RPM missing in
  image" guard so a future staging regression fails the build loudly instead of
  mid-`dnf`. (#134)
- SIF build no longer aborts when run as a service account from a directory that
  account can't read. `qiita_sif_build_inputs_hash` ran `find`, which restores its
  initial working directory on exit; a manual `sudo -u qiita-orch build-sif.sh`
  launched from an admin's `0700` home left `find` unable to restore that cwd, so
  it exited non-zero and tripped `set -o pipefail`, aborting the build before it
  started (`find: Failed to restore initial working directory`). `build-sif.sh`
  now `cd`s to `/` after its precondition checks (and the hash helper does so in
  a subshell), so the whole build — the hash `find` and the `apptainer exec`
  verify steps — is independent of the caller's cwd. All paths used are absolute,
  so the build keeps producing a `qiita-orch`-owned SIF without needing root or a
  `cd` workaround (#132 follow-up)
- The bcl-convert step no longer fails with `chmod: changing permissions of
  '.../bcl_convert/attempt-0/output': Operation not permitted` after bcl-convert
  and `manifest_writer.py` both succeed. The entrypoint's final mode-fixing
  `find … -exec chmod` walked `$QIITA_OUTPUT_PATH` itself — created on the host
  by the orchestrator (owned by the orchestrator user, not the in-container
  user) — so chmod returned EPERM and `set -e` failed the otherwise-successful
  job. Both finds now carry `-mindepth 1`, re-moding only what the container
  created inside `output/`. Separately, `scripts/build-sif.sh` gains a `FORCE=1`
  opt-out of its version-only idempotency check, so an image-baked change to
  `entrypoint.sh`/`manifest_writer.py`/`Apptainer.def` that does not bump the
  vendored binary version can still force a rebuild (without it the fix would
  never reach the host). Surfaced running bcl_convert end-to-end (SLURM job
  156785) (#130)
- The bcl-convert step no longer fails with `TypeError: 'type' object is not
  subscriptable` after bcl-convert itself succeeds. Its container is built
  `From: oraclelinux:8`, whose default `python3` is 3.6 — predating PEP 585
  builtin generics (`list[str]`), which the workflow-agnostic
  `workflows/_shared/manifest_writer.py` uses and evaluates at import. The
  container now installs the appstream `python3.11` and the entrypoint invokes
  it explicitly; the `%test` block additionally `exec_module`s
  `manifest_writer.py` under the shipped interpreter so a too-old Python fails
  the SIF build instead of a live SLURM job. Surfaced running the first
  container step (bcl-convert) end-to-end (SLURM job 153623) (#126)
- Container workflow steps no longer exit 64 (`QIITA_INPUT_PATH not set`) once
  they reach the entrypoint. `apptainer exec --containall` contains the
  environment as well as the filesystem, so the `QIITA_*` vars set in the SLURM
  job env never crossed into the container; the entrypoint reads
  `$QIITA_INPUT_PATH/params.json` and writes to `$QIITA_OUTPUT_PATH`, so it
  bailed immediately. The orchestrator now forwards the container-contract vars
  (`QIITA_INPUT_PATH`, `QIITA_OUTPUT_PATH`, `QIITA_WORK_TICKET_IDX`) via
  apptainer `--env`; native-only env (CO→CP token, miint dirs) is deliberately
  not exposed to containers. Surfaced running the first container step
  (bcl-convert) end-to-end, after #116 fixed the upstream container-creation
  failure (#122)
- Host-reference index builds (`build_rype_index`, `build_minimap2_index`) no
  longer fail at the post-success manifest step on a real SLURM run. Both jobs
  write a *persistent* index under `PATH_DERIVED` (outside the per-attempt
  workspace, so it outlives the ticket) and also declared that out-of-tree path
  as a step output (`rype_index_path` / `minimap2_index_path`). The native-step
  launcher and the verifier both require every declared output to resolve under
  `$QIITA_OUTPUT_PATH`, so the launcher's `relative_to` blew up *after* the job
  succeeded — a `CONTRACT_VIOLATION` (`"... is not in the subpath of ..."`). The
  binding was vestigial: nothing consumes it — `register-index` reads the index
  location from the in-tree meta JSON's `fs_path`. Both jobs now return only the
  meta output, and the workflow YAMLs declare only `*_index_meta`. The local
  backend never writes/verifies a manifest, so this only surfaced under SLURM;
  the launcher now also rejects an out-of-tree output with an actionable message
  (naming the output and the rule) instead of leaking the opaque `relative_to`
  error, covered by a new launcher unit test (#118)
- Container workflow steps no longer fail at apptainer container creation under
  the locked-down SLURM job account. The orchestrator now passes
  `--home <workspace>` to `apptainer exec --containall` for container steps:
  `--containall` derives the container's home mount target from the job user's
  passwd entry, and `qiita-job` is a service account whose passwd home is
  `/dev/null`, which collided with the device of the same name in the container
  layout (`failed to add /dev/null as session directory`). Pinning the home mount
  to the per-ticket workspace (matching the `HOME` env already set for native
  steps) resolves it; native steps are unaffected (they run no container) (#116)
- `make redeploy`'s SLURM native-venv refresh no longer fails with `uv: command
  not found`. The refresh ran `sudo -u qiita bash -lc '... uv sync ...'` with a
  bare `uv`, trusting the login PATH — but `uv` lives in `/usr/local/bin`, which
  is absent from sudo's `secure_path` and need not be on qiita's login PATH. It
  now invokes uv by absolute path (`$UV=/usr/local/bin/uv`), matching the
  long-standing pattern in `activate.sh`; the manual-fallback hint and the
  `SKIP_NATIVE_REFRESH` echo use the absolute path too (#114)
- Retrying a `failed` reference load no longer dies instantly. A fresh
  `POST /work-ticket` bound to an existing reference (the `qiita reference load
  --reference-idx N` retry) now resets a `failed` reference back to `pending`
  before dispatch — mirroring the `/run` redrive path — so the run's first
  status PATCH is the legal `pending → hashing` instead of the illegal
  `failed → hashing` that killed the ticket at the first step. Only a `failed`
  reference is touched (any other state is a no-op, an unrewindable in-progress
  state is logged at WARNING); the shared reset helper is reused by both the
  submit and redrive paths (#112)
- `build_rype_index` no longer OOMs DuckDB on a genome-scale host reference.
  The step split the SLURM cgroup DuckDB(4 GB, capped) / rype(elastic) on the
  assumption DuckDB "never needs more" than the 4 GB off-SLURM fallback — but
  feeding the full chunk scan to rype's read needs far more, so a human host
  reference (T2T-CHM13) OOMed DuckDB at ~3.7 GB while reading `rype_chunk_input`,
  before rype's `max_memory` was ever exercised (and `--mem-gb` could not raise
  it — it only grew rype's share). DuckDB's under-SLURM cap is now
  `_DUCKDB_MEMORY_CAP_GB` (16 GB) instead of the 4 GB fallback; rype stays the
  elastic consumer (its share still grows with the allocation). The 16 GB cap is
  a heuristic and should be tuned against a real genome-scale MaxRSS (#111)
- The `--mem-gb` per-run override (#102) now actually reaches the DuckDB-backed
  reference-load steps, instead of being silently clamped by a hardcoded
  per-job DuckDB `memory_limit`. Each native job pinned its DuckDB cap to a
  literal (`stage_local_fasta` 7 GB, `hash_sequences` 24 GB, `load` 31 GB,
  `build_rype_index` rype 24 GB, `build_minimap2_index` 8 GB) sized to the YAML
  baseline, so raising the SLURM allocation grew the cgroup but DuckDB still
  OOM'd at the literal — a genome-scale human host reference died in
  `stage_local_fasta` at ~6.5 GiB even under `--mem-gb 48`. The jobs now size
  DuckDB (and rype's `max_memory` / minimap2's reserve) from the real cgroup via
  `SLURM_MEM_PER_NODE`, falling back to the literal off SLURM (local backend /
  tests unchanged). This also resolves the latent `hash_sequences` case where
  its 24 GB literal exceeded its own 8 GB YAML allocation. Scoped to the
  reference-add workflow: `fastq_to_parquet` (the read-ingest path) is a
  deliberate follow-up, and `host_filter` is intentionally left as-is — its
  genome-scale memory is the rype/minimap2 indexes held out of DuckDB's heap,
  which already grow into the cgroup a `--mem-gb` raise provides, so converting
  its DuckDB cap would starve them (#107)
- OOM-killed workflow steps are no longer mis-reported as a bare
  `NonZeroExitCode`. A cgroup step-level `oom_kill` surfaces to slurmrestd only
  as a coarse job-level `FAILED`/`exit_code=1`, so the orchestrator's
  `OUT_OF_MEMORY → OOM_KILLED` classifier never fired and the launcher's
  structured stderr line was never written (the process was killed first). The
  SLURM backend now scans the job's stderr on an otherwise-unclassified
  terminal failure: an OOM signature upgrades the classification to the
  (retriable) `OOM_KILLED`, and a short stderr tail is folded into
  `failure_reason` for every state-based failure — so `qiita ticket status`
  reports a memory-related reason directly instead of requiring a root shell to
  read the SLURM log. A specific infra kind (NODE_FAIL/TIMEOUT/PREEMPTED) is
  never downgraded (#104)
- `docs/runbooks/redeploy.md` §7 no longer tells the operator to run
  `compute-readiness` as `qiita-api` sourcing `control-plane.env` (which fails:
  it needs the `qiita-orch` account + `compute-orchestrator.env` + the 0400
  `co-to-cp.token`) — the recurring deploy defect from #72. It now points at
  `make verify-deploy` and documents the correct `qiita-orch` form (#72)
- `qiita-admin compute-readiness` / `python -m …compute_readiness` now fail
  loudly with the correct `sudo -u qiita-orch …` invocation when misinvoked,
  instead of silently exiting 0: a non-slurm backend on a real orchestrator host
  (env file present) is a `fail` row, and a present-but-unreadable
  `co-to-cp.token` raises an actionable error naming the file + ownership (#72)
- `qiita submit-bcl-convert` now opens the preflight blob via
  run-preflight's `open_db_file` instead of a hand-rolled read-only
  `sqlite3.connect`, opening it the way the library expects (#92)
- `docs/runbooks/first-deploy.md` now documents the `PATH_DERIVED/references/`
  host-reference index directory in the filesystem-bootstrap table (owner
  `qiita-orch`, group `qiita-pipeline`, mode `2770`, setgid). The host-reference
  index build and its `host_filter` consumer both run as `qiita-job`, which
  `mkdir`s `{idx}/{rype,minimap2}/` at runtime; without the group-writable
  setgid leaf the first `host-reference-add` build fails Permission Denied on
  the `root:root 0755` base root. Previously only `…/images` was listed (#100)

### Added

- ENVO terminology seed for the environmental-context biosample fields
  (`broad_scale_environmental_context`, `local_environmental_context`,
  `environmental_medium`), plus a reusable
  `rebind_biosample_global_field_data_type` migration helper that guards a
  field's data_type flip against existing metadata rows (#81)
- Study submission tracking: `qiita.study` gains `last_submission_at` /
  `submission_error`, exposed for read in `StudyResponse`. The three tables
  now share one `clear_submission_error_on_new_attempt` trigger function.
  These columns are subsystem-owned and are not on the (owner-accessible)
  study PATCH surface; on biosample and sequenced_sample, whose PATCH routes
  are wet_lab_admin-gated, they remain PATCHable. Not exposed through the
  CLI. (#81)
- `qiita study patch`, `qiita biosample patch`, and `qiita sequenced-sample
  patch` — update a study's or sample's editable fields (including ENA
  accession write-back) over the REST API, under If-Match optimistic
  concurrency (#81)
- `qiita study get`, `qiita biosample get`, and `qiita biosample list-idxs` —
  read a study or biosample by idx, and list a study's biosample idxs, over the
  REST API (#81)
- `qiita biosample create --ena-sample-accession` and `qiita sequenced-sample
  create --ena-experiment-accession` / `--ena-run-accession` — set an entity's
  ENA accession(s) at create time when ingesting already-submitted data
  (allowed, not required), matching `study create --ebi-study-accession` (#81)
- `qiita study create --extra-metadata` — attach a free-form JSON object
  (stored as JSONB) when minting a study, matching the existing
  `--extra-metadata` flag on `sequencing-run create` / `sequenced-pool create`
  (#81)
- Work-ticket in-place-retry visibility: `transient_reason` / `transient_since`
  on the work-ticket status (`GET /work-ticket/{idx}` and the list view) and two
  matching `qiita.work_ticket` columns. While the runner retries an unreachable
  orchestrator/slurmrestd in place, it records *why* and *since when* so a
  ticket stuck in `processing` is explainable instead of looking silently
  wedged; cleared once it makes progress or fails (#80)
- `GET /work-ticket` — list work tickets, each with a snapshot of its current
  step's compute placement (`compute_target`, `slurm_job_id`, `step_state`,
  `current_step_index/name`) from a single join against the new
  `qiita.work_ticket_step` progress table. Caller-relative by default;
  `?all=true` (wet_lab_admin+) widens to every originator; filters `state` /
  `active` / `limit` (#77)
- `qiita ticket list [--state … --active --all --limit N]` — CLI over the new
  list endpoint (#77)
- `POST /step/find-by-name` (CP→CO) — look up live SLURM jobs by their
  deterministic name so the runner can adopt a job it submitted but never
  recorded the id for, instead of launching a duplicate on resume (#77)
- `qiita.work_ticket_step` table — per-`(work_ticket_idx, step_index, attempt)`
  write-ahead progress (compute_target, slurm_job_id, job_name, state, failure
  surface) that is the spine of restart recovery (#77)
- Local-host FASTA ingest: `qiita reference load --local --fasta-manifest <path>`
  builds a reference from many host-resident FASTA files **by path** (no DoPut
  upload), backed by the `stage_local_fasta` native job and two new workflows,
  `local-reference-add` and `local-host-reference-add`; companions
  (taxonomy/tree/jplace/genome_map) ride as raw absolute paths
  (#78)
- Host references for host-read filtering: `is_host` column on `qiita.reference`,
  the `reference_index` table tracking built indexes, an `indexing` reference
  status (`loading → indexing → active`), and the `host-reference-add` workflow
  that builds a rype `.ryxdi` host-filter index (`build_rype_index` native
  job + `register-index` library action) (#70)
- `GET /reference` (list; filter by `kind` / `is_host` / `status`) and
  `GET /reference/{reference_idx}/index` (list a reference's built indexes) (#70)
- `qiita reference load --host` — create a host reference (or bind an existing
  one) and run `host-reference-add`; requires `--taxonomy` (#70)
- Arrow Flight DoPut upload domain + chunked reference-load pipeline (#49)
- Support for known-missing and terminology-term metadata values (#56)
- `/health` aggregator probing CP + CO + DP with cached aggregation, a
  three-pill per-service status strip on the landing page, and gRPC reflection
  on the data plane (closes #54) (#58)
- bcl-convert workflow: container image, workflow YAML, and build script;
  `QIITA_IMAGES_DIR` with container bind/path resolution; the `bcl_convert_prep`
  native job; per-sample sequenced-sample minting via `qiita submit-bcl-convert`;
  the `SEQUENCED_POOL` scope target (#62)
- Self-hosted OpenAPI docs at `/docs` (Swagger UI) and `/redoc` (ReDoc), linked
  from the landing page and served from vendored assets (no CDN) (#64)
- `changelog-check` CI gate requiring every PR to record its change here (opt
  out with the `no-changelog` label) (#65)
- `matrix_tube_id` column on biosample with digit-only format and uniqueness
  constraints, exposed via the biosample REST routes and the
  `qiita biosample create --matrix-tube-id` CLI flag (#68)
- `POST /study/lookup-by-accession` for bulk `ebi_study_accession` →
  `study_idx` resolution; body-shaped so a long accession list rides
  past nginx's default URL-line cap (#74)
- `PATCH /study/{idx}` for editing the post-create study columns
  (PI, title, alias, description, abstract, funding,
  `ebi_study_accession`, notes, `extra_metadata`) under required
  `If-Match` optimistic-concurrency control (#74)
- `UNIQUE` constraint on `study.ebi_study_accession` (NULLs distinct,
  so "unique when present") (#74)

### Changed

- The user-CLI quickstart now documents the headless / remote-host auth path:
  `qiita login` needs a co-located browser + loopback receiver, so on an SSH
  session / HPC login node / CI runner you carry the PAT instead — log in once
  on a browser machine, then `export QIITA_TOKEN=…` (+ `QIITA_CONTROL_PLANE_URL`)
  on the headless host. `$QIITA_TOKEN` already took precedence over the token
  file; this just makes the supported path discoverable (#80)
- miint now installs from the team mirror by default in every component (CP CLI,
  CO service, native SLURM jobs): `miint_install_sql()` always `FORCE INSTALL`s
  from `MIINT_MIRROR_URL` (override with `MIINT_EXTENSION_REPO`) instead of
  falling back to the DuckDB community channel — so one host can't drift to a
  different `read_fastx` build, and `FORCE` overwrites a stale cached extension
  (#80)
- Decoupled compute-step execution: the orchestrator's single blocking
  `POST /step/run` is replaced by the stateless `submit` / `status` / `result`
  trio, and the **control plane** now owns the poll loop. A long SLURM job no
  longer holds the CP→CO connection open, and the orchestrator keeps no
  in-flight state between calls (the `StepHandle` it returns carries everything
  status/result need; the CP persists it) (#77)
- Restart recovery re-attaches instead of failing: on CP startup,
  `reconcile_inflight_tickets` resumes every non-terminal ticket through
  `run_workflow(resume=True)` — fast-forwarding completed entries, re-attaching
  a live SLURM job by its persisted id (or adopting an orphan by deterministic
  name), and deciding a purged job from its on-disk output manifest — rather
  than the old blanket-fail of all in-flight work on every deploy (#77)
- CO-unreachable during submit/poll/result (transport error or HTTP 5xx) is now
  a transient `ORCHESTRATOR_UNREACHABLE` the runner retries in place, so
  stopping the orchestrator mid-deploy never fails a running ticket (#77)
- `qiita reference load` now parses FASTA with miint's `read_fastx` and a shared
  DuckDB `chunk_list` macro (in new `qiita_common.chunking` /
  `qiita_common.duckdb_miint` modules) instead of a hand-rolled Python FASTA
  chunker; the control-plane CLI loads the miint DuckDB extension client-side.
  No sequence bytes pass through Python and memory stays bounded for
  genome-scale records (#78)
- The SLURM backend now propagates `PATH_SCRATCH` into the compute-node job
  environment, so native steps that derive a path from the shared scratch base
  (the per-ticket workspace root) resolve the real value instead of the
  `$TMPDIR/qiita` default (#70). (Persistent index artifacts later moved off
  `PATH_SCRATCH` to `PATH_DERIVED` — see the #89 Changed entry above.)
- Centralized all REST path string literals into `qiita-common`'s
  `api_paths.py` (closes #12) (#60)
- Bumped the study / prep_sample identity sequence start to 25000 (#61)
- Moved the `reference load` command from `qiita-admin` to the `qiita` end-user
  CLI (it is a credentialed API call, not a host operation) (#63)
- Renamed the operator deploy checklist `CHANGELOG.md` → `DEPLOY_CHECKLIST.md`;
  `CHANGELOG.md` is now this per-change log (#65)
- Scoped the `push` CI trigger to `main` so PR branches get a single
  `pull_request` run instead of duplicate push + PR runs (#65)
- Restructured the filesystem env vars onto three base roots with derived
  leaves: `WORK_TICKET_WORKSPACE_ROOT` + `SHARED_FILESYSTEM_ROOT` →
  `PATH_SCRATCH/ticket`, `UPLOAD_STAGING_ROOT` → `PATH_SCRATCH/staging`,
  `DUCKLAKE_DATA_PATH` → `PATH_PERSISTENT/ducklake`, and `QIITA_IMAGES_DIR`
  → `PATH_DERIVED/images`. Operators now set `PATH_SCRATCH` /
  `PATH_PERSISTENT` / `PATH_DERIVED`; the services derive the fixed
  subdirs. Hard cutover — the old names are gone and boot fails fast until
  the new ones are set (#73)
- Switched `bcl_convert_prep` from `run_preflight.legacy.api.save_legacy_csv`
  to the public `run_preflight.save_bclconvert_v1_csv`, and `qiita
  submit-bcl-convert` from a hard-coded JOIN against the kl-run-preflight
  SQLite schema to upstream's `get_illumina_sample_info` plus the new
  `POST /study/lookup-by-accession`; bumped the `run-preflight` SHA pin
  in lock-step across CP + CO and guarded the parity with a new
  `tests/integration/test_run_preflight_pin_parity.py` (#74)
- Tightened `min_length=1` on `biosample_accession` / `ena_sample_accession`
  (`BiosampleImportRequest`, `BiosamplePatchRequest`) and on
  `ebi_study_accession` (`StudyCreate`) so empty strings no longer reach
  the DB (#74)
- SIF builds go through a single generic `scripts/build-sif.sh <workflow>`
  driven by a declarative `workflows/<workflow>/sif-build.env`; replaces the
  per-workflow `scripts/build-bcl-convert-sif.sh`. The builder stages into a
  temp root owned by the invoking user (the checkout is read-only), so a
  service account can build without write access to the qiita-owned checkout.
  A `test_sif_build_spec.py` guard forbids per-workflow build scripts, requires
  each spec to be complete, and asserts `SIF_FILENAME` matches the workflow
  YAML's `container:`; `make test-workflows` builds a `_sif-build-smoke`
  sentinel through `build-sif.sh` so the temp-root staging is covered against
  real apptainer in CI (#75)

### Fixed

- `qiita-admin compute-readiness` no longer aborts its SLURM probe at parse
  time. A newline-escape inside an f-string comment in `build_probe_script`
  expanded to a real newline, splitting the comment and leaving an unmatched
  backtick, so the generated probe script failed `bash` parsing (exit 2) before
  any check ran — meaning the `native-import` / `miint-read-fastx` compute-env
  guards #80 added never actually executed. Fixed the comment, added a `bash -n`
  regression test over the generated script (the existing substring tests
  couldn't catch it), and defaulted the probe log onto the shared filesystem
  (`PATH_SCRATCH/ticket`) instead of node-local `/tmp` so the head node can read
  the compute-node probe results back (#84)
- `qiita reference load --local` no longer hard-fails when the
  `--fasta-manifest` path isn't visible from the host running the CLI (e.g. a
  login node without the compute node's shared-FS view). The manifest is read
  by `stage_local_fasta` on the compute node, not by the CLI, so a missing path
  is now a warning (still flags a real typo) and the submit proceeds —
  consistent with the companion paths, which were never existence-checked. The
  absoluteness check is unchanged (a relative path still errors) (#80)
- SLURM JWT recovery no longer depends on a clean 401. `SlurmrestdClient` now
  proactively reloads the JWT from its file when the cached token is within 60s
  of its `exp`, *before* sending the request — so a long-lived orchestrator
  can't run on a boot-cached token past expiry until a restart when slurmrestd
  rejects an expired token with a 5xx / dropped connection instead of a 401
  (the reload-on-401 path only fires on a 401). The 401-reload path is kept as a
  fallback, and both the 401 reload and the submit-error classification now log
  the exact status so the next stuck-on-submit incident is diagnosable without a
  repro (#80)
- The runner's in-place infra-unreachable retry is now escapable and bounded.
  An operator `qiita-admin ticket force-fail` (a direct-DB FAILED transition) is
  now noticed: every infra-retry/poll iteration re-checks the ticket's DB state
  and bails if it has gone terminal, instead of spinning forever against a
  ticket it no longer owns — without clobbering the operator's failure surface.
  The retry sleep is now capped exponential backoff (base = poll interval,
  doubling to a 60s cap) rather than a flat hammer, and the never-fail-on-outage
  invariant is preserved — there is still no hard give-up (#80)
- Redriving a FAILED reference workflow via `POST /work-ticket/{idx}/run` now
  actually works. Two redrive defects fixed in the same atomic reset: the
  `reference` scope_target was left pinned at `failed`, so the redriven
  workflow's first status PATCH (`failed → hashing`) was illegal and the redrive
  died immediately — `/run` now resets the reference `failed → pending` (the
  FSM's only legal exit from `failed`); the prior run's terminal `failed`
  `work_ticket_step` rows survived, so the runner's fresh attempt-0 collided with
  the dead row (the step-progress writers reject any transition out of `failed`)
  — `/run` now drops every non-`completed` step row (keeping `completed` ones so
  fast-forward still works). A reference that failed at a later step still fails
  cleanly on redrive (the FSM can't rewind past `pending`); that multi-step case
  is not yet supported (#80)
- A rejected SLURM submit no longer looks like a success: slurmrestd answers
  HTTP 200 even when slurmctld refuses the job (unavailable partition, QOS
  limit, …), and `SlurmrestdClient.submit_job` trusted the echoed `job_id`
  blindly. It now inspects `result.error_code` and the top-level `errors[]`
  array first and raises (classified as a permanent `CONTRACT_VIOLATION`, since
  re-submitting the same payload won't help); benign `warnings[]` (e.g. the
  `nodes` type warning) stay non-fatal (#80)
- Stale compute-environment failures now surface at deploy, not at the first
  job: `compute-readiness` probes that the compute node's miint build binds
  `read_fastx(max_batch_bytes:=…)` (the call `stage_local_fasta`/`reference load`
  issue), and the redeploy runbook now documents refreshing the separate
  `SLURM_NATIVE_PYTHON` checkout so native jobs don't import stale `qiita-common`
  (#80)
- Long compute steps no longer self-fail: under the old held-connection model a
  step exceeding the 600s CP→CO client timeout tripped an httpx error that
  skipped the retry loop and marked the ticket FAILED while the SLURM job kept
  running. The CP-driven poll loop has no such ceiling (#77)
- No duplicate concurrent SLURM jobs: a write-ahead progress row + deterministic
  job name `qiita-wt{idx}-{step}-a{attempt}` let resume adopt a job whose id was
  never persisted (via `find-by-name`) instead of re-submitting; retriable
  failures no longer resubmit without checking the prior job (#77)
- Corrected stale identifier field names in `docs/architecture.md` to match the
  current schema: `sample_idx` → `biosample_idx` (the physical sample is
  `biosample`; there is no `sample` table), noted design issue to resolve the
  non-existent `prep` entity and dangling `prep_idx` surviving only as a
  vestigial `work_ticket` scope tuple), documented that `study`/`biosample` are
  many-to-many with `prep_sample`, and dropped `study_idx`/`biosample_idx` from
  the result-Parquet identifier columns (recovered via control-plane joins),
  resolving the prior `(prep_idx, processing_idx)` vs `(prep_sample_idx,
  processing_idx)` inconsistency (#76)
- Assert `HumanUser` before reading `.system_role` in the sequenced-sample /
  biosample routes (closes #45) (#59)
- Closed deploy gaps surfaced by the first user-CLI fastq-to-parquet smoke
  test (#57)
- Added a lightweight CP `/healthz` liveness route so `qiita-admin
  compute-readiness` (and its SLURM probe) stops reporting a false 404 against
  a healthy deploy — the checker hit `/healthz`, which the CP never served
  (closes #67) (#69)

### Removed

- `sequenced_sample.host_rype_reference_idx` / `host_minimap2_reference_idx`
  columns (their FKs and the minimap2-requires-rype CHECK drop with them). Host
  references are now a human-filter submission argument, not a sample column
  (PR 4 of the full-read+mask feature). Single drop migration, no
  expand/contract: the deploy wipes all legacy sequenced/pool samples first
  (their reads predate the lake-read model). (#175)
- The legacy synchronous step path: `POST /step/run`, `ComputeBackend.run_step`
  (+ the SLURM/Local overrides and the CO `_poll_until_terminal` poll loop),
  `ComputeBackendClient.run_step`, and the `StepRunRequest` / `StepRunResponse`
  wire models. The decoupled submit/status/result trio fully replaces it; CP
  and CO must deploy together since the route contract changed (#77)

[Unreleased]: https://github.com/the-miint/Qiita/commits/main
