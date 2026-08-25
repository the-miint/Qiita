# Data Model

## Identifier Hierarchy

All identifiers are uint64, minted exclusively by the control plane. The data plane treats all identifiers as opaque integers.

- **`study_idx`** — unique identifier for a study. A study is a logical collection of biosamples and prep_samples — both linked **many-to-many** (a biosample or prep_sample can belong to several studies; a study includes many of each), including meta-analyses that group `prep_sample_idx` values from other studies without uploading new data. The junction tables are `biosample_to_study` and `prep_sample_to_study`.
- **`biosample_idx`** — unique identifier for a physical sample (table `biosample`). (Note that **sample** is an ambiguous term and therefore is NOT used to represent any entity in the data model.) A biosample can appear across multiple studies via `biosample_to_study`.
- **`prep_protocol_idx`** — identifies a lab preparation procedure (amplicon, shotgun, …). **Warning: The prep_protocol_idx is not linked to any explicit `prep` entity nor is it intended to be. The token `prep_idx` survives as a vestigial `work_ticket` scope tuple `(study_idx, prep_idx)` with no backing table (see [Work Ticket Lifecycle](processing.md#work-ticket-lifecycle)).  This is a design issue that must be clarified by the system designers before building on it in any way.**  *preparation* is an ambiguous term and therefore is NOT used to represent any entity in the data model.
- **`prep_sample_idx`** — a specific instance of a specific biosample prepared under a specific protocol; the finest-grained unit of raw input data and the supertype of the downstream-measurement hierarchy. Each prep_sample has exactly one parent biosample (`prep_sample.biosample_idx`); a biosample may yield multiple prep_samples (technical or biological replicates), and a prep_sample may appear in many studies (e.g. shared extraction plate controls) via the many-to-many `prep_sample_to_study` table.
- **`processing_idx`** — unique identifier for a processing method: a specific `(workflow_name, workflow_version, parameter_set)` combination. Immutable once created. Reusable across prep_samples and studies. Opaque integer in the data plane; detail lives in the control plane `processing_methods` table.
- **`processed_prep_sample_idx`** — unique identifier for the result of applying a `processing_idx` to a `prep_sample_idx`. Minted by the control plane before job submission. Processing cannot create new prep samples — all `processed_prep_sample_idx` values for a job are pre-assigned from the known `prep_sample_idx` set in the submission.

Reference identifiers form a parallel hierarchy for reference databases:

- **`reference_idx`** — unique identifier for a specific `(name, version)` pair of a reference database (e.g., "Greengenes2 2024.09", "WoL3 v1.0"). A reference is a curated, versioned collection of features. The `kind` field distinguishes sequence references from taxonomy authorities. Minted by the control plane.
- **`genome_idx`** — logical entity that spans references, representing a single genome regardless of which reference collections include it (e.g., "E. coli K-12 GCF_000005845"). Not all features are genomes — `genome_idx` is nullable for features like full-length 16S records or ASVs. Carries provenance: `source` (genbank, refseq, collaborator, qiita) and `source_id` (external accession when applicable, e.g., a GenBank or RefSeq accession). Minted by the control plane.
- **`feature_idx`** — unique identifier for a specific sequence, deduplicated by MD5 hash: identical bytes always resolve to the same `feature_idx`. One genome has one or more features (contigs, chromosomes). Features are the unit of coordinate space — alignments and annotations use positions relative to a `feature_idx`. Minted by the control plane; sequence data stored in the data plane.

`feature_idx` is the bridge between sample processing results (alignment detail, counts) and reference data (sequences, taxonomy, annotations, phylogeny). Alignment output contains `feature_idx` but not `reference_idx` — reference scoping is a query-time join against `reference_membership`.

Raw-read identifiers extend the prep_sample hierarchy:

- **`sequence_idx`** — globally-unique bigint identifying a single raw read stored in the data plane. Minted by the control plane in contiguous ranges of caller-specified size via `POST /api/v1/sequence-range`, recorded in `qiita.sequence_range` (1:1 with `prep_sample_idx`, kind-pinned to `processing_kind='sequenced'` via composite FK), and never recycled — a deleted range's `sequence_idx` values stay consumed in `qiita.sequence_idx_seq`. The endpoint is service-account-only (`sequence_range:mint` scope); the compute orchestrator obtains a range before writing raw-read Parquet to the data plane.

## Legacy import reserved range — study and prep_sample

The live deployment partitions `study.idx` and `prep_sample.idx` into two non-overlapping bands:

- **`[1, 25000)`** — reserved for the one-time legacy import from the previous Qiita installation. Historic rows preserve their original integer identifiers and are inserted with `OVERRIDING SYSTEM VALUE` (both columns are `GENERATED ALWAYS AS IDENTITY`).
- **`[25000, ...)`** — every new row minted on this deployment.

A schema migration fast-forwards the implicit identity sequences for `qiita.study.idx` and `qiita.prep_sample.idx` past the reserved block; existing rows in `[1, 25000)` are untouched. The threshold itself is pinned on each column in the Postgres catalog via `COMMENT ON COLUMN`, so `\d+ qiita.study` and `\d+ qiita.prep_sample` surface the invariant without consulting the migration history. The bump is one-way — the underlying sequence operation only advances and cannot rewind without risking PK collisions — so the migration's down step is a deliberate no-op.

The reservation is intentionally scoped to `study` and `prep_sample` only — those are the identifiers legacy callers will continue to quote. Adjacent rows (`biosample`, `sequenced_sample`, `sequencing_run`, `prep_protocol`, the per-row `*_metadata` / `*_field_exception` tables, references) receive freshly-minted identifiers during the legacy import rather than preserving their originals, so their identity sequences are left on the default start.

## Processing Methods

`processing_idx` detail lives in the control plane `processing_methods` table:

| Column | Type | Notes |
|---|---|---|
| `processing_idx` | uint64 PK | |
| `workflow_name` | text | |
| `workflow_version` | text | |
| `parameters_hash` | text | SHA-256 of canonical JSON parameters; deduplication key |
| `parameters_jsonb` | jsonb | full parameter set |
| `created_at` | timestamp | |
| `created_by` | text | |

Two submissions with identical `(workflow_name, workflow_version, parameters)` resolve to the same `processing_idx` via `parameters_hash` — the control plane upserts on this key rather than minting a duplicate.

## Identifier Columns in Parquet

All result Parquet files must include these columns, in this order, and be sorted by them:

```
prep_sample_idx  processing_idx  processed_prep_sample_idx
```

Neither `study_idx` nor `biosample_idx` is embedded: a prep_sample maps to studies **many-to-many** (so files are never keyed by `study_idx` — see [Data Storage](storage.md#data-storage)), and its parent `biosample_idx` is a single-FK lookup; both are recovered at query time via control-plane joins (`prep_sample_to_study`, `prep_sample.biosample_idx`) rather than duplicated into every row.

This sort order provides two compounding layers of query optimisation:

- **DuckLake catalog** (`ducklake_file_column_stats`): min/max per column per file stored in Postgres. Any query filtering on identifier columns prunes whole files before DuckLake opens anything.
- **Parquet row group statistics**: sorted data produces tight, non-overlapping min/max ranges per row group — predicate pushdown skips row groups with zero false positives for point lookups and range scans. Without sorting, row group ranges overlap and pushdown degrades to near-useless for selective queries.

The sort is enforced in the reduce step (see Compute Orchestrator), so it is consistent regardless of what the map phase produces.

Alignment and count result tables extend this base sort with reference columns. Alignment detail Parquet uses the sort order `(prep_sample_idx, processing_idx, processed_prep_sample_idx, feature_idx, position)` — the trailing `feature_idx, position` exploits genome locality so that reads hitting the same region of the same feature are physically adjacent. Count/aggregation Parquet uses `(..., feature_idx)` without `position`. Crucially, these tables contain `feature_idx` but **not** `reference_idx` — this means alignment and count data do not need to be recomputed when a feature is added to or removed from a reference version. Scoping results to a specific reference version is a query-time join against the `reference_membership` table.

## Biosample and Prep Sample Metadata

Sample metadata — descriptive attributes of physical biosamples (specimen type, collection site, host age, treatment status, environmental parameters, etc.) or of lab-prepared prep samples (elution volume, sequencing indexes, etc.) — lives exclusively in the control plane (Postgres app DB). It does not exist in the data plane.

Reasons:
- Metadata is structured relational data tightly coupled to the identifier model already in the control plane
- Schema validation against BioSample / MIMARKS compliance requirements is enforced at insert time
- Metadata and processed measurement data have different update semantics — a metadata correction (e.g., fixing a mislabelled collection site) does not invalidate or require reprocessing of any Parquet files in the data plane
- Access control decisions are already made in the control plane; metadata-driven filtering is a natural extension of the same authorization layer

**Search pattern:** the control plane exposes search and filter endpoints over metadata. A client submits a query (e.g., "fecal samples from antibiotic-naive subjects in study X"), receives the authorized set of `prep_sample_idx` (or `processed_prep_sample_idx`) identifiers matching the criteria, and uses those IDs directly against the data plane. The control plane search is the access control gate — clients only receive IDs they are authorized to access. The data plane never evaluates metadata; it serves measurements for the requested IDs, relying on the sorted Parquet structure and DuckLake column statistics for efficient lookup.

### Global vs. study-local fields and multi-alias linkage

Every metadata *value* — for a biosample or a prep_sample — is stored attached to a **study-local field** row (`biosample_study_field` / `prep_sample_study_field`); the EAV row's `*_study_field_idx` FK is mandatory, so there is no other way in. A *field* (note, not value) is **global** when its study-local row links to a **global field** (`biosample_global_field` / `prep_sample_global_field`) through the `*_global_field_idx` FK (and inherits `data_type` / `required` / `terminology` / `tier` from the global field and may override only `display_name` and `description`, while a purely study-local field leaves that FK null and owns its own `data_type` / `required` / `terminology` / `tier`). The *value* (EAV row) carries a trigger-maintained `global_field_idx`, denormalized from the linked global field, so "every value for global field G" is a single indexed predicate regardless of which study-local field routed it. Each global-field table enforces `UNIQUE (display_name)` (alongside the existing `internal_name` uniqueness), so a global concept has exactly one canonical user-facing label — matching the per-study `display_name` uniqueness already on study-local fields.

**Why not allow non-unique global `display_name`s?** The pressure to allow them is real: the global-field table spans every study, so demand for several fields named, say, "disease state" is high. They are forbidden anyway, for two reasons. First, a read of all global metadata across a large sample set would emit two columns with the same name and different contents, leaving the consumer no way to tell them apart. Second, `display_name` is already unique per study for study-local fields, so a looser rule for global fields is an inconsistency in exactly the surface users read and search by — `display_name` is the label that reaches any output.

Note that **multiple study-local fields in one study may link to the same global field and this is intended.** For example, two contributors to one study can arrive with distinct sets of biosamples that carry different column names for the same global concept (say "host age" and "host age in years"); each becomes its own study-local field, with its own `display_name`, and both linked to the `host_age` global field. EAVs for a given contributor-provided study-local field are populated only for that contributor's biosamples. A whole-study read for host age is `WHERE global_field_idx = host_age`, which reunites the aliases under the global field's canonical label and ignores which study-local field each value came through.

**What a metadata read is keyed on.** A globally-linked value reads back under its global field's `internal_name`; a purely study-local value reads back under its study-local `display_name`. A client that wrote a value keyed on a display_name therefore cannot assume it reads back under that key — each per-field write result reports the `internal_name` the value will read under, and null when the write resolved to a purely-local field. The `display_name` still travels alongside every global value as its canonical label.

The guardrail that keeps this coherent is the uniqueness on globally-linked values — `biosample_metadata_one_value_per_global_field` / `prep_sample_metadata_one_value_per_global_field`, `UNIQUE (entity_idx, global_field_idx)` — which forbids a *single* entity from carrying two values for one global field while permitting the disjoint-coverage aliasing above. It is deliberately keyed per entity, **not** per study: a `UNIQUE (study_idx, global_field_idx)` would forbid disjoint aliasing and must not be added. Consistent with this, resolving a column to a global field is keyed on `display_name` by default, or on the global field's `internal_name` when the caller sets `global_internal_names`. A globally-linked write then get-or-creates the study-local row it needs on `(study_idx, display_name)` — a new label mints a new alias, a matching label reuses the existing one — using the global field's own `display_name` when the caller keyed by `internal_name`, since the key it sent is not a label. Neither the import nor the metadata-write path creates a global field or a purely-local one: a key matching no existing field is rejected, and minting a field is the study-local field-create route's job.

A study-local field's global link may be **added** (a local→global upgrade propagates the link onto the field's existing values, gated by the per-entity uniqueness index) but never **rebound** to a different global field, nor **unlinked** while values exist; the propagate trigger rejects both, and the correct move is to create a new study-local field.

### Metadata visibility tiers (not yet enforced)

The schema models per-field and per-value access tiers, but no code reads or enforces them yet. Every metadata read and write today ignores these columns entirely; visibility is instead controlled coarsely, at the route's `require_study_access` tier gate. Building the enforcement described here is outstanding work.

Two mechanisms compose to set the tier a caller must hold — on the study, via their `study_access` row — to read *or* write a given metadata field or value:

- **Field-level tier** — the minimum tier for every value of one field within a study. For a globally-linked field this is `*_global_field.default_tier` (NOT NULL, defaults to `public`); for a purely study-local field it is `*_study_field.tier_override` (nullable; NULL means no field-level restriction). A linked `*_study_field` row leaves `tier_override` NULL and inherits the global field's `default_tier`.
- **Value-level exception** — a row in `*_field_exception` narrows one individual `(biosample, field)` value below its field-level tier, carrying a NOT NULL `tier_override`. It is keyed on `global_field_idx` (so a globally-linked value's exception follows it across studies) or on `*_study_field_idx` (for a purely-local value). The motivating case is a field that is broadly visible in general but where one biosample's value must be restricted — e.g. a free-text field into which one submitter incautiously entered PII.

When enforcement is built, the effective required tier for a field or value is the most restrictive that applies (field-level tier, further tightened by any matching value-level exception). A caller whose study-access tier is below that threshold must not see the value on any read path, and must be refused when attempting to write it. This applies uniformly to reads and writes and to both globally-linked and study-local metadata.

This is the mechanism that will ultimately decide who can see or change the owner-biosample-id (a study-local field pinned to `member` tier) and any other tier-restricted field or value — replacing today's coarse interim rule, which clamps to admin-tier callers every study-scoped route that reads or writes sample-family metadata, or that mints a study-local field. The idx-listing reads stay at viewer tier: they expose no metadata. Field creation sits under the clamp only for want of the finer gate — when enforcement lands it returns to member tier, since minting a study-local field is work a study member is meant to do.

## Raw Data Fingerprint

A SHA-256 fingerprint of uploaded raw data is recorded per `prep_sample_idx` at upload time in the control plane. Its purpose is **upload-time duplicate detection only** — it is not the processing deduplication key:

- Warns users when uploaded data appears identical to an existing `prep_sample_idx`
- Surfaces accidental duplicate uploads before compute is wasted
- Provides the basis for storage deduplication (one physical file, multiple logical references) as a future optimisation

## Processing Deduplication and Disallow-Without-Delete

The control plane gates all job submission on the current state of each `(prep_sample_idx, processing_idx)` pair. Before submitting any work:

- **COMPLETED**: disallow — require explicit DELETE before resubmission
- **PENDING, QUEUED, or PROCESSING**: disallow — work is already in flight; submitting again would produce duplicate compute for the same result
- **FAILED** or absent: allow submission

This check applies at both the work ticket level (is there an active ticket for this prep + processing combination?) and the individual sample level (does any `prep_sample_idx` in the request already have a result in a non-terminal state?), preventing both whole-prep and partial duplicate submissions.

Since DuckLake does not support explicit constraints (no unique constraints, no foreign keys at the DuckLake level), the data plane is intended to assert identifier integrity programmatically. **These checks are planned, not yet built** — `processed_prep_sample_idx` and the processing-results tables it keys are part of the processing hierarchy that has not been implemented (`register_files` performs path-safety and existence checks, plus the replace-by-key pass described below; no startup scan runs today):

- **At registration** *(planned)*: before `ducklake_add_data_files`, verify the Parquet file's `processed_prep_sample_idx` values are a subset of the expected set provided by the control plane, and that no value from the file already exists in the catalog for this `(prep_sample_idx, processing_idx)` combination.
- **At service startup** *(planned)*: scan the DuckLake catalog to verify no `processed_prep_sample_idx` appears in multiple active files for the same `(prep_sample_idx, processing_idx)`. Violations logged as critical errors; affected combinations blocked from serving until reconciled.

A *different* integrity rule, on a different set of tables, **is** enforced at registration today — the two are unrelated, and the one below is built. `feature_idx` is minted from the canonical sequence hash, so identical bytes carry one feature across every producer, and the writer of a staging Parquet (a native compute job) has no DuckLake access to anti-join against — two references sharing a sequence, or two assembly runs producing the same contig, each emit that feature's rows in full. `register_files` therefore **replaces** `reference_sequences`, `reference_sequence_chunks`, `assembled_sequence`, and `assembled_sequence_chunks` on `feature_idx`: the keys the incoming Parquet carries are deleted from the lake in the same transaction, ahead of every `ducklake_add_data_files`, so a second load converges rather than accumulating a copy whose only symptom is a `string_agg(chunk_data, '' ORDER BY chunk_index)` twice as long as `sequence_length_bp`. `assembly_membership` / `bin_quality` are replaced too, on the composite `(prep_sample_idx, processing_idx)` — a re-run resolving to the same `processing_idx` supersedes that sample's rows for that run instead of leaving both runs under one identity. `REPLACE_KEY_TABLES` in `qiita-data-plane/src/flight_service.rs` is the registry and carries the admission conditions — including why the newest load's strand wins. Writers of those tables additionally **serialize** on the single-row `qiita_lake.registration_lock`, and a writer that loses retries its own transaction rather than failing the ticket; `register_files` carries why the replace alone does not close that race.
