# duckdb-miint reference (internal cheat sheet)

> **Audience:** developers writing or reviewing data-plane / orchestrator / reference-ingestion code that calls miint. Not an end-user reference for the extension itself — upstream owns that.
> **Upstream:** https://github.com/the-miint/duckdb-miint
> **Last checked:** 2026-06-13 (against default branch `v1.5-variegata`, latest docs commit `2cb50cd3` from 2026-06-13). This refresh focused on the host-filter functions (`save_minimap2_index`, `align_minimap2`, `rype_classify`), each qiita-verified via a real smoke; the default branch is unchanged since the prior check, so the rest of the inventory was not re-diffed.
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
- SAM flag predicates: `sam_flag_*` family (paired, mapped, secondary, supplementary, duplicate, etc.)
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
- `align_minimap2(query_table, [subject_table=NULL], [index_path=NULL], [options])` — SAM-like rows `(read_id, flags, reference, position, …)`; any row = a hit (a non-matching read emits none). **qiita-verified 2026-06-13** (host_filter smoke): `read_id` is returned in the input type (BIGINT in → BIGINT out); named opts include `preset` (e.g. `'sr'`). Consumed by `host_filter`.
- `save_minimap2_index(subject_table, output_path, [options])` — writes a minimap2 `.mmi`. **qiita-verified 2026-06-13** (build_minimap2_index smoke): exactly **two positional args** (subject-table NAME, output path) + named opts `eqx`/`w`/`k`/`preset` (`preset := 'sr'`); the subject table needs `(read_id, sequence1)`; returns one row `(success BOOLEAN, index_path VARCHAR, num_subjects INTEGER)` — assert `success`. Consumed by `build_minimap2_index`.
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
- `rype_classify(index_path, sequence_table, [id_column='read_id'], [threshold=0.1], [negative_index=path])` — one row per matched read `(read_id, bucket_id, bucket_name, score)`. **qiita-verified 2026-06-13** (host_filter smoke): qiita passes the **POSITIVE** host index (host = any emitted row, low explicit `threshold` — NOT `negative_index`/`-N`). `id_column` accepts `{VARCHAR, BIGINT, UUID}`; since **duckdb-miint #126** it mirrors the input type on output, so a BIGINT id round-trips as BIGINT (pre-#126 it always returned VARCHAR — `host_filter` CASTs to BIGINT defensively, correct either way). Consumed by `host_filter`.
- `rype_log_ratio(numerator_path, denominator_path, sequence_table, [id_column='read_id'], [skip_threshold=0.5])`
- `rype_extract_minimizer_set(sequence_table, k, w, [salt=6148914691236517205], [id_column='read_id'])`
- `rype_extract_strand_minimizers(sequence_table, k, w, [salt=…], [id_column='read_id'])`
- `rype_index_create(chunk_table, output_path, [mapping_table], [k=64], [w=50], [salt=6148914691236517205], [orient=true], [max_memory=0])` — builds a `.ryxdi` minimizer index. **Exactly two positional args** (`chunk_table`, `output_path`); everything else is named. `chunk_table` is a table/view with fixed columns `(feature_idx BIGINT, chunk_index INTEGER, chunk_data VARCHAR|BLOB)`; optional `mapping_table` is `(feature_idx BIGINT, bucket_name VARCHAR)` (omit → one unnamed bucket). Returns one status row `(output_path, k, w, status)` — assert `status = 'ok'`. **qiita caveat:** only in the **team-mirror** build (`https://ftp.microbio.me/pub/miint`), not the community channel as of the last check — the orchestrator's reference-indexing tests install via `MIINT_EXTENSION_REPO`. Signature **qiita-verified 2026-06-01 via a real smoke test** (the published `docs/rype.md` was slightly off — note the two-positional/rest-named split above). Consumed by the `build_rype_index` native job ([host references](reference-data-staging.md#host-references-and-the-rype-index)).

### COPY writers — `docs/copy-formats.md`
`COPY <query> TO 'path' (FORMAT { FASTQ | FASTA | SAM | BAM | BIOM | NEWICK })` — see the doc for required column shapes (e.g. SAM/BAM expects HTSlib-style columns; BIOM expects a feature table layout).

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
