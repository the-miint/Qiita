# duckdb-miint reference (internal cheat sheet)

> **Audience:** developers writing or reviewing data-plane / orchestrator / reference-ingestion code that calls miint. Not an end-user reference for the extension itself — upstream owns that.
> **Upstream:** https://github.com/the-miint/duckdb-miint
> **Last checked:** 2026-07-13 (`align_minimap2`'s full column set + `alignment_seq_identity` / `cigar_query_coverage` / `alignment_slice` / `read_gff` — **qiita-verified by probe** against team-mirror build `5447847`. Recorded because the read-mask `syndna` step now depends on `cigar` + `tag_nm`/`tag_md` (they feed `alignment_seq_identity`) and on `reference`. Two corrections fell out: the SAM-flag predicates are the **`alignment_is_*` family** (all 13 present and probed; use them instead of bit math on `flags` — an earlier revision of this file listed them under a wrong name and wrongly concluded they were absent), and `read_gff`'s `stop_position` is INCLUSIVE (GFF3 1-based closed) while `read_alignments` / `alignment_slice` are half-open — same column name, different convention. Pinned by `qiita-compute-orchestrator/tests/jobs/test_syndna_smoke.py`.) 2026-07-09 (`infer_trim` — the read-mask lima chain's trim reconciler, added upstream in duckdb-miint #140 and merged into `v1.5-variegata` on 2026-07-07 — **qiita-verified by probe** against team-mirror build `ec2ef3e` (exactly that merge commit): signature, `NULL/NULL` for an omitted read, and the fail-loud non-substring guard. Also re-verified `read_fastx`'s column set (it splits the record name at the first whitespace into `read_id` + `comment`, and its `sequence_index` is POSITIONAL, resetting per file) and that `COPY ... (FORMAT FASTQ)` requires a **VARCHAR** `read_id` — a BIGINT raises an `INTERNAL Error` and invalidates the connection (reported upstream as duckdb-miint #145; until it is fixed, cast on write and recover with `read_id::BIGINT` on read). Pinned by `qiita-compute-orchestrator/tests/jobs/test_lima_chain_smoke.py` and the `miint-infer-trim` compute-readiness probe.) 2026-06-25 (`COPY ... TO ... (FORMAT FASTQ)` writer — verbatim `read_id`/`sequence1`/`qual1`(+`sequence2`/`qual2`) columns, phred+33 qual encoding, and the `{ORIENTATION}`→`R1`/`R2` split-output placeholder — verified against team-mirror build `eca0e79` via probes + `qiita-compute-orchestrator/tests/jobs/test_masked_export_fastq_contract.py`; consumed by the `qiita-admin masked-read-export` CLI). 2026-06-18 (QC fastp-port functions — `filter_read`, `trim_adapters`/`trim_adapters_pe`, `trim_polyg` — verified against the team-mirror build via probes + `qiita-compute-orchestrator/tests/jobs/test_qc_miint_contract.py`; the **positional-arg-only** contract and fastp-default values are pinned there — the upstream `docs/qc.md` shows named params that the mirror build does not accept). 2026-06-15 (host-filter functions re-verified directly against the team-mirror build `e6f598d` via probes + smokes — `align_minimap2` paired-end query support, `rype_classify` reading `sequence2`, and the mirror's VARCHAR `read_id` output for rype). The 2026-06-13 pass diffed the default branch `v1.5-variegata` (docs commit `2cb50cd3`); the rest of the inventory was not re-diffed since.
> **Refresh trigger:** if today's date is more than 7 days after **Last checked** above, re-verify against upstream `docs/` before relying on any signature here — the project ships frequently and function signatures, parameter names, and embedded-tool versions drift. See [Keeping this doc fresh](#keeping-this-doc-fresh).

`duckdb-miint` is the DuckDB extension that powers all bioinformatics SQL in qiita — the data plane links it in, native orchestrator jobs invoke its functions, and several reference-data ingestion steps are thin wrappers around its table functions. Treat this file as the index: when you need a real signature, click through to the upstream doc rather than trusting paraphrased examples here.

## Install / version probe

```sql
INSTALL miint FROM community;
LOAD miint;
SELECT miint_version();        -- always the first sanity check
SELECT * FROM miint_warnings();-- non-fatal load-time issues (e.g. GPL boundary not installed)
```

Local builds need `allow_unsigned_extensions=true` and `LOAD '/path/to/miint.duckdb_extension'`. There is also a companion Python CLI named `miint` (lives under `python/` in the repo) that wraps the extension for `convert`/`transform`/`align` subcommands — not on PyPI; install from a local checkout with `pip install -e python/`.

**In qiita, don't `INSTALL ... FROM community`.** Every component runs the same team-mirror build, single-sourced in [`qiita_common.duckdb_miint`](../qiita-common/src/qiita_common/duckdb_miint.py) (`MIINT_EXTENSION_REPO` overrides for a local/dev build; the mirror build is unsigned, so `allow_unsigned_extensions=true` is always set). On the cluster the extension is **pre-staged once at deploy** into `MIINT_EXTENSION_DIRECTORY` (`scripts/stage-miint-extension.sh`); the CO service, the five native SLURM jobs, and the compute-readiness probe then only `LOAD miint` (`miint.open_miint_conn`) — no per-job download, no compute-node mirror dependency, no writable-`$HOME` requirement. The client-side `qiita reference load` CLI, which can't reach a deploy-staged dir, does a plain cached `INSTALL` instead. The Rust data plane honors the same `MIINT_EXTENSION_REPO` / `MIINT_EXTENSION_DIRECTORY` env contract independently (it can't import the Python module — keep the two in sync). DuckDB namespaces the staged dir by version + platform, so re-run the stage step on a miint or DuckDB bump.

## Upstream docs map

The upstream `docs/` is the source of truth. The boilerplate at `docs/README.md` is the DuckDB extension template README (ignore it); same for `docs/UPDATING.md` which is generic "bump the DuckDB submodule" instructions.

| Upstream file                          | What lives there                                                                                             |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `docs/installation.md`                 | Install via community extensions; local-build flags; Python CLI usage                                        |
| `docs/scalar-functions.md`             | Per-row SQL functions (SAM flags, CIGAR ops, sequence masking, pair merging)                                 |
| `docs/analysis-functions.md`           | Aggregate / record-set analysis (Woltka OGU, coverage, pairwise alignment, MassQL, formula)                  |
| `docs/table-functions.md`              | All `read_*` ingest fns + every aligner/classifier/cluster/chimera/UniFrac runner (largest file by far)      |
| `docs/copy-formats.md`                 | `COPY ... TO ... (FORMAT FASTQ\|FASTA\|SAM\|BAM\|BIOM\|NEWICK)` writers                                      |
| `docs/rype.md`                         | RYpe sequence classifier (Rust, Arrow FFI) — minimizer extraction + classification                           |
| `docs/unifrac.md`                      | UniFrac PCoA / PERMANOVA / Faith's PD, including subsampling                                                 |
| `docs/massql.md`                       | Mass-spectrometry queries via MassQL DSL                                                                     |
| `docs/ena.md`                          | EBI/ENA reading **and** submission (Webin V2) — catalog attach, lifecycle, audit log                         |
| `docs/testing.md`, `docs/wasm-testing.md` | How the extension is tested (SQL logic + C++ Catch2 + shell; WASM headless harness)                       |
| `docs/internals/architecture.md`       | Extension entry point, design patterns (record abstraction, dual-path lateral, pushdown, GPL boundary)       |
| `docs/internals/embedded-tools.md`     | Which external tools are statically linked vs. runtime — version pins live here                              |
| `docs/internals/per-sample-pattern.md` | How `sample_id='col'` parameter works on per-sample table functions                                          |
| `docs/internals/arrow-zero-copy.md`    | Consuming Arrow streams from extension code without copying                                                  |
| `docs/internals/reading-tables-views.md` | Why table-function bind/execute must open a **separate** connection to read DB tables/views                |

## Function inventory

Signatures are abbreviated — go to the upstream `.md` for full parameter docs. `[param=default]` denotes a named optional parameter.

### Scalar (per-row) — `docs/scalar-functions.md`
- SAM flag predicates: the **`alignment_is_*` family** — `alignment_is_paired` / `_proper_pair` / `_unmapped` / `_mate_unmapped` / `_reverse` / `_mate_reverse` / `_read1` / `_read2` / `_secondary` / `_primary` / `_qc_failed` / `_duplicate` / `_supplementary`. Each takes the `USMALLINT` `flags` column and returns BOOLEAN. All 13 verified present by probe against the team-mirror build (2026-07-13); HTSlib-compatible aliases (`is_unmapped`, `is_secondary`, `is_supplementary`, `is_dup`, `is_qcfail`) are also present, and upstream documents further ones this probe did not enumerate. **Use these — do not hand-roll bit math on `flags`.** One trap: `alignment_is_primary` means "neither secondary nor supplementary" and is **TRUE for an unmapped read** (probed: `flags=0x4` → `primary=true`), so it does not imply mappedness — an "aligned primary" test is `alignment_is_primary(flags) AND NOT alignment_is_unmapped(flags)`.
- `alignment_seq_identity(cigar, nm, md, type)` — identity from CIGAR + NM/MD tags
- `cigar_sequence_identity(cigar)`
- `cigar_query_length(cigar, [include_hard_clips=true])`
- `cigar_query_coverage(cigar, [type='aligned'])`
- `mask_dust(sequence, [hardmask=false])` — low-complexity masking
- `merge_pairs_vsearch(fwd_seq, fwd_qual, rev_seq, rev_qual, [options])`
- `phylogeny_fasttree_available()` — capability probe
- `install_gpl_boundary([force])` — install the out-of-process GPL tool host (see Internals)

### Analysis / aggregate — `docs/analysis-functions.md`
- `woltka_ogu(relation, sequence_id_field [, sample_id])` — OGU feature table
- `sequence_dna_reverse_complement(seq)` / `sequence_rna_reverse_complement(seq)`
- `sequence_dna_as_regexp(seq)` / `sequence_rna_as_regexp(seq)` — IUPAC-aware regex compile
- `sequence_split(seq, chunk_size)` → `LIST(STRUCT(chunk_index INTEGER, chunk_data VARCHAR))` — fixed-width chunking in a single linear pass; `UNNEST` for chunk rows. Backs qiita's chunked-Parquet write (`stage_local_fasta`, CLI `reference load`), replacing an O(L²) SQL macro (duckdb-miint #121 / DuckDB #23229; added 2026-06)
- `compress_intervals(start, stop)`
- `compute_coverage_depth(position, stop_position, cigar, reference_length, mode)`
- `genome_coverage(alignments, subject_total_length, subject_genome_id)`
- Pairwise alignment helpers (see source for variants)
- `formula(formula_string)` — chemical formula parser
- `massql(query, source)` — MassQL DSL → spectra match (also see `docs/massql.md`)
- `miint_version()`

### Table functions (`SELECT * FROM …`) — `docs/table-functions.md`
**Sequence/alignment readers:**
- `read_alignments(filename, [reference_lengths='table_name'], [include_filepath=false], [include_seq_qual=false])` — SAM/BAM/CRAM
- `read_sequences_sam(filename)` — SAM/BAM/CRAM as reads (not alignments), emitting a `read_fastx`-compatible schema (`sequence_index, read_id, comment, sequence1/2, qual1/2`; `qual*` phred-decoded `UTINYINT[]`). **Undocumented upstream**; one row per SAM record (no FLAG column, no secondary/supplementary filter; empty input → 0 rows, not a throw). Contract pinned in `qiita-compute-orchestrator/tests/jobs/test_bam_to_parquet_miint_contract.py`; consumed by `bam_to_parquet`.
- `alignment_slice(table_name, start, stop, [include_deletions=false])` — pileup-style projection
- `read_fastx(filename, [sequence2=filename], [include_filepath=false], [qual_offset=33])` — FASTQ/FASTA, paired
- `read_sequences_sff(filename, [include_filepath=false], [trim=true])`
- `read_biom(filename, [include_filepath=false])`

**Mass spec:** `read_mzml`, `read_mzxml`, `read_mzml_chromatograms` (all `(filename, [include_filepath=false])`)

**Annotation / public data:**
- `read_gff(path)`
- `read_ncbi(accession, [api_key], [batch_size=500])`, `read_ncbi_fasta`, `read_ncbi_annotation`
- `read_ena(accession, [result='read_run'], [fields])`, `read_ena_attributes`, `read_ena_sequences`, `ena_searchable_fields(result_type)`

**Phylogeny:**
- `read_jplace(path)`, `read_jplace_newick(path, [include_filepath=false])`
- `read_newick(filename, [include_filepath=false])`
- `tree_resolve_placement(tree_table, placements_table)`
- `phylogeny_fasttree(table_name, [options])`

**Alignment / mapping (table-valued):**
- `align_minimap2(query_table, [subject_table=NULL], [index_path=NULL], [options])` — SAM-like rows. **Full column set (qiita-verified 2026-07-13, mirror build `5447847`):** `read_id, flags, reference, position, stop_position, mapq, cigar, mate_reference, mate_position, template_length, tag_as, tag_xs, tag_ys, tag_xn, tag_xm, tag_xo, tag_xg, tag_nm, tag_yt, tag_md, tag_sa`. `reference` names WHICH subject the read hit (so per-feature counts need no special index); `cigar` is eqx-style (`=`/`X`) and `tag_nm`/`tag_md` feed `alignment_seq_identity` — the `syndna` job depends on all three. `stop_position` is EXCLUSIVE (half-open), unlike `read_gff`'s inclusive one. any row = a hit (a non-matching read emits none). **qiita-verified 2026-06-15** (host_filter probe + smoke, mirror build `e6f598d`): the query table's first positional arg is the table NAME; `read_id` round-trips its input type (BIGINT in → BIGINT out). It reads a `sequence1` column and, when present, `sequence2` — a `(read_id, sequence1, sequence2)` query table aligns in **paired-end mode** (sets the mate / `template_length` SAM fields; one row per mate). A NULL `sequence2` is tolerated (single-end). Named opts include `preset` (e.g. `'sr'`) and `max_secondary` (`:= 0` to drop secondary alignments). Consumed by `host_filter`.
- `save_minimap2_index(subject_table, output_path, [options])` — writes a minimap2 `.mmi`. **qiita-verified 2026-06-15** (build_minimap2_index smoke): exactly **two positional args** (subject-table NAME, output path) + named opts `eqx`/`w`/`k`/`preset` (`preset := 'sr'`); the subject table needs `(read_id, sequence1)` (`read_id` may be BIGINT); returns one row `(success BOOLEAN, index_path VARCHAR, num_subjects INTEGER)` — assert `success`. Consumed by `build_minimap2_index`.
- `align_minimap2_sharded(query_table, shard_directory, read_to_shard, [options])`
- `align_bowtie2(query_table, subject_table, [options])` / `align_bowtie2_sharded(...)`
- `align_mafft(table_name, [sample_id='col'])`
- `align_sortmerna(query_table, ref_paths=paths, [options])` / `align_sortmerna_rrna(...)`

**Chimera / search / cluster / denoise:**
- `detect_chimera_uchime(query_table, db='refs_table', [sample_id='col'], [options])`
- `detect_chimera_uchime_denovo(input_table, [sample_id='col'], [options])`
- `search_sequences_vsearch(query_table, db='ref_table', id=threshold, [options])`
- `cluster_sequences_vsearch(input_table, id=threshold, [options])`
- `deblur(input_table, [sample_id='col'], [options])`

**Diversity:** `unifrac_pcoa(observations, tree, [options])`, `unifrac_permanova(observations, tree, metadata, [options])`, `unifrac_faith_pd(observations, tree, [options])` — see `docs/unifrac.md` for variant strings + subsampling.

**Diagnostics:** `miint_warnings()`.

### RYpe classification — `docs/rype.md`
- `rype_classify(index_path, sequence_table, [id_column='read_id'], [threshold=0.1], [negative_index=path])` — one row per matched read (**≤1 per read**) `(read_id, bucket_id, bucket_name, score)`. **qiita-verified 2026-06-15** (host_filter probe + smoke, mirror build `e6f598d`): qiita passes the **POSITIVE** host index (host = any emitted row, low explicit `threshold` — NOT `negative_index`/`-N`). It reads `sequence1` and, when present, `sequence2` (a host match in EITHER mate emits the read; a NULL `sequence2` is tolerated). `id_column` accepts `{VARCHAR, BIGINT, UUID}` on input. Output id type is build-dependent (a probe of the ftp.microbio.me build `e6f598d` returned VARCHAR for a BIGINT input; #126's input-type round-trip may differ on a newer build), so `host_filter` does not depend on it: it appends `read_id` into a BIGINT accumulator column, which coerces either type on insert. It also DISTINCTs the result — the table-function interface does not guarantee one best-hit row per read. Consumed by `host_filter`.
- `rype_log_ratio(numerator_path, denominator_path, sequence_table, [id_column='read_id'], [skip_threshold=0.5])`
- `rype_extract_minimizer_set(sequence_table, k, w, [salt=6148914691236517205], [id_column='read_id'])`
- `rype_extract_strand_minimizers(sequence_table, k, w, [salt=…], [id_column='read_id'])`
- `rype_index_create(chunk_table, output_path, [mapping_table], [k=64], [w=50], [salt=6148914691236517205], [orient=true], [max_memory=0])` — builds a `.ryxdi` minimizer index. **Exactly two positional args** (`chunk_table`, `output_path`); everything else is named. `chunk_table` is a table/view with fixed columns `(feature_idx BIGINT, chunk_index INTEGER, chunk_data VARCHAR|BLOB)`; optional `mapping_table` is `(feature_idx BIGINT, bucket_name VARCHAR)` (omit → one unnamed bucket). Returns one status row `(output_path, k, w, status)` — assert `status = 'ok'`. **qiita caveat:** only in the **team-mirror** build (`https://ftp.microbio.me/pub/miint`), not the community channel as of the last check — the orchestrator's reference-indexing tests install via `MIINT_EXTENSION_REPO`. Signature **qiita-verified 2026-06-01 via a real smoke test** (the published `docs/rype.md` was slightly off — note the two-positional/rest-named split above). Consumed by the `build_rype_index` native job ([host references](reference-data-staging.md#host-references-and-the-rype-index)).

- `infer_trim(original_reads, qcd_reads)` — **table macro**, not a scalar. Both args are relations exposing `sequence_index` (BIGINT, the join key) and `sequence` (VARCHAR). Returns ONE ROW PER ORIGINAL read: `(sequence_index BIGINT, trimmed_5p UINTEGER, trimmed_3p UINTEGER)`, with **`NULL`/`NULL` when the tool omitted the read**. `LEFT JOIN … USING (sequence_index)`, then locates the QC'd sequence inside the original via `position()`; leftmost match wins. **Fails loud** if a kept read is not a contiguous substring of its original (the tool edited internal bases) — that is a feature, do not suppress it. No uniqueness policing on `qcd_reads.sequence_index`: a duplicate fans the join out.
  **qiita gotcha:** the caller must carry its OWN key through the external tool. `read_fastx` assigns `sequence_index` POSITIONALLY and resets per file, so it is NOT recoverable by re-parsing the tool's output — the `lima_export` job writes `sequence_idx` as the FASTQ record NAME (CAST to VARCHAR; the writer rejects a BIGINT) and `lima_mask` recovers it as `read_id::BIGINT AS sequence_index`. **qiita-verified 2026-07-09 by probe** (mirror build `ec2ef3e`) + `tests/jobs/test_lima_chain_smoke.py`; probed at deploy by `compute-readiness`'s `miint-infer-trim` check. Consumed by the `lima_mask` native job (read-mask's adapter chain).

### QC — fastp algorithm port (`docs/qc.md` upstream)
The fastp-equivalent read-QC functions, consumed by the `qc` native job (the bcl-convert → `fastq` → **`qc`** → `host_filter` pipeline). **qiita-verified 2026-06-18** against the team-mirror build (`SELECT qc_version()` → `qc 0.1.0 (port of fastp algorithms)`); pinned by `qiita-compute-orchestrator/tests/jobs/test_qc_miint_contract.py`. **All take POSITIONAL args only — named params (`min_length := 100`) raise a `BinderException`** (the upstream `docs/qc.md` shows named optionals; the mirror build does not accept them). `qual` is `UTINYINT[]` (phred-decoded, exactly what `read_fastx` / `fastq_to_parquet` emit). Each returns a `STRUCT`.
- `filter_read(seq, qual [, min_length, max_length, qualified_q, max_unqualified_pct, max_n, min_avg_q])` → `STRUCT(passed BOOLEAN, fail_reason VARCHAR, length UINTEGER, n_bases UINTEGER, low_qual_bases UINTEGER, mean_quality FLOAT)`. Only a **2-arg** and an **8-arg** overload exist (no partial). The 2-arg defaults equal `(15, 0, 15, 40, 5, 0)` — these ARE fastp's defaults — so a faithful `fastp -l 100` is the 8-arg call `filter_read(seq, qual, 100, 0, 15, 40, 5, 0)`. `fail_reason` is `NULL` when passed, else `'length'` / `'n_base'` / `'quality'` / `'too_long'`.
- `trim_adapters(seq, qual, adapter [, match_revcomp, min_match, allow_pre_start])` → `STRUCT(sequence VARCHAR, quality UTINYINT[], trimmed_5p UINTEGER, trimmed_3p UINTEGER)`. `adapter` is `VARCHAR` **or** `VARCHAR[]` (a known-adapter set). The single-end path.
- `trim_adapters_pe(seq1, qual1, seq2, qual2 [, adapters VARCHAR[], overlap_require, overlap_diff_limit, overlap_diff_percent_limit, match_revcomp, min_match, allow_pre_start])` → `STRUCT(sequence1, quality1, sequence2, quality2, overlap_len INTEGER, adapter_trimmed BOOLEAN, trimmed1_3p UINTEGER, trimmed2_3p UINTEGER)`. Only a **4-arg** (overlap-only) and an **11-arg** overload. The 11-arg form with an **empty** adapter list + `(30, 5, 20, false, 0, false)` reproduces the 4-arg result exactly — those are fastp's overlap defaults — so pass a non-empty `adapters` to add the by-sequence adapter fallback fastp applies *after* overlap analysis, without changing overlap behavior.
- `trim_polyg(seq, qual [, min_len, max_mm, max_window_mean_q])` → `STRUCT(sequence, quality, trimmed_5p, trimmed_3p)`. Trims a 3' G-run **only when its quality is low** (2-color no-signal); a high-quality G-run is left intact. Defaults `(10, 5, 5)`. fastp enables polyG only for 2-color instruments (NextSeq/NovaSeq), so the `qc` job calls it gated on `instrument_model`.
- `qc_version()` → `VARCHAR` — QC-port version sanity probe.

### COPY writers — `docs/copy-formats.md`
`COPY <query> TO 'path' (FORMAT { FASTQ | FASTA | SAM | BAM | BIOM | NEWICK })` — see the doc for required column shapes (e.g. SAM/BAM expects HTSlib-style columns; BIOM expects a feature table layout).

**`FORMAT FASTQ` writer — qiita-verified 2026-06-25** (probes + `qiita-compute-orchestrator/tests/jobs/test_masked_export_fastq_contract.py`, build `eca0e79`; consumed by the `qiita-admin masked-read-export` CLI writing the data plane's `read_masked` view to per-sample FASTQ):
- Requires the **verbatim** columns `read_id`, `sequence1`, `qual1` (and `sequence2`, `qual2` for paired) — the exact names `read_masked` emits. Aliasing `read_id` away raises a `BinderException`; select the view's columns by name.
- `qual1`/`qual2` are `UTINYINT[]` (phred-decoded, as `read_fastx` emits) and are written back ASCII **phred+33** (Q40 → `I`, Q30 → `?`).
- A row with `sequence2` set into a **single** output path errors; paired output needs either the **`{ORIENTATION}`** placeholder in the path (split files) or **`INTERLEAVE true`** (one interleaved file).
- `{ORIENTATION}` expands to exactly **`R1`** / **`R2`**, so `TO '<stem>.{ORIENTATION}.fastq'` yields `<stem>.R1.fastq` + `<stem>.R2.fastq`.

## Internals worth knowing before extending qiita usage

These are not just curiosities — they constrain how qiita's data-plane and orchestrator code may call miint.

- **GPL boundary (`install_gpl_boundary`).** Several embedded tools are GPL (notably `vsearch`, MAFFT, SortMeRNA). The extension itself is BSD; GPL-licensed code runs in a separately-distributed out-of-process host. If a function returns "gpl-boundary not installed", call `install_gpl_boundary()` first. Probe availability with `phylogeny_fasttree_available()` etc.
- **Per-sample `sample_id='col'` knob** (see `internals/per-sample-pattern.md`). When the user passes `sample_id='some_column'`, the function partitions by that column and runs the tool per group. Reserved output column names are listed in that doc — avoid colliding with them in qiita's table schemas.
- **Reading tables/views inside table functions** (`internals/reading-tables-views.md`). miint table functions use a **separate** DuckDB connection during bind/execute. Implication for us: any miint table-function call that names another table (e.g. `read_alignments(..., reference_lengths='ref_lens')`, `align_minimap2(query_table, subject_table=...)`) resolves persistent `table` and `view` names but **does not** resolve `TEMP TABLE`s, and CTEs are not reliably visible either. Stage inputs as a regular table (or view) before the miint call rather than relying on temp tables / CTEs in the same statement.
- **Arrow zero-copy** (`internals/arrow-zero-copy.md`). RYpe and a few others move Arrow batches across the FFI without copying — lifetime is tied to the source batch. If we ever embed miint outside DuckDB, mind the lifetime rules.
- **Identifier-column codec** (`id_column_codec` / `id_column_utils` in `internals/architecture.md`). miint preserves arbitrary user identifier columns (e.g. `read_id`) through alignment/classification pipelines. We rely on this to carry our `prep_sample_idx` and friends through alignment.
- **Dual-path table functions (standard + lateral) and filter pushdown.** Most readers support both `SELECT * FROM read_fastx('x.fq')` and `… JOIN LATERAL read_fastx(t.path)`. Predicate pushdown into `read_alignments` is real — `WHERE flag & 4 = 0` will prune at the HTSlib layer. Don't write helper macros that materialize before filtering.

## Embedded tool versions (as of last check)

From `docs/internals/embedded-tools.md` — pinned at the extension's build, not at runtime. If qiita runs in a container, the versions baked in **depend on which miint binary is loaded**, not which conda env is active.

| Tool                                   | Version              | Link type                            |
| -------------------------------------- | -------------------- | ------------------------------------ |
| HTSlib                                 | 1.22.1               | static (ExternalProject)             |
| minimap2                               | 2.30                 | static                               |
| WFA2-lib                               | 2.3.5                | static                               |
| vsearch                                | 2.30.5-miint fork    | static (via GPL boundary)            |
| MAFFT PartTree                         | —                    | static (via GPL boundary)            |
| SortMeRNA                              | 4.4.0 fork           | static (via GPL boundary)            |
| unifrac-binaries + scikit-bio-binaries | —                    | static                               |
| rype (Rust, Arrow FFI)                 | —                    | static                               |
| kseq++                                 | —                    | header-only                          |
| IBM Aspera `ascp`                      | —                    | **runtime binary** (not compiled in) |
| gpl-boundary host                      | —                    | runtime binary                       |

**Refresh trigger:** if a qiita workflow result diverges from expectation in a way that smells tool-version-dependent (e.g. minimap2 secondary-alignment counts, vsearch chimera scoring), re-read `embedded-tools.md` upstream before debugging our side.

## Keeping this doc fresh

To keep this file honest:

1. **Look at the `Last checked:` date in the header.** If `today − Last checked > 7 days`, refresh before relying on a specific signature.
2. **Refresh procedure (5–10 min):**
   ```bash
   # latest commit touching upstream docs/
   gh api 'repos/the-miint/duckdb-miint/commits?path=docs&per_page=1' \
     | python3 -c "import json,sys;d=json.load(sys.stdin)[0]; \
       print(d['sha'][:12], d['commit']['committer']['date'], \
       d['commit']['message'].splitlines()[0])"

   # default branch (changes per minor release, e.g. v1.5-variegata → v1.6-foo)
   gh api repos/the-miint/duckdb-miint --jq .default_branch

   # full headings inventory across docs/ and docs/internals/ (compare against
   # this file's Function inventory, Internals section, and version table)
   BRANCH=$(gh api repos/the-miint/duckdb-miint --jq .default_branch)
   for f in scalar-functions analysis-functions table-functions copy-formats \
            rype unifrac massql ena installation \
            internals/architecture internals/embedded-tools \
            internals/per-sample-pattern internals/arrow-zero-copy \
            internals/reading-tables-views; do
     echo "===== $f.md ====="
     curl -fsSL "https://raw.githubusercontent.com/the-miint/duckdb-miint/$BRANCH/docs/$f.md" \
       | grep -E '^#{1,3} '
   done
   ```
   `internals/embedded-tools.md` is the most version-volatile of these — re-read it whenever a workflow result smells tool-version-dependent.
3. **What to update:** the `Last checked` line; the default-branch tag in the header; any new/renamed/removed function in the inventory; the embedded-tool version table if `internals/embedded-tools.md` changed.
4. **What NOT to copy in:** full prose, examples, or parameter tables. This file is an index — depth lives upstream. Keep it concise.
5. **If signatures here disagree with upstream**, upstream wins. Fix this file in the same commit as the consumer-code change that exposed the drift.
