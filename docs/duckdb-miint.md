# duckdb-miint reference (internal cheat sheet)

> **Audience:** developers writing or reviewing data-plane / orchestrator / reference-ingestion code that calls miint. Not an end-user reference for the extension itself — upstream owns that.
> **Upstream:** https://github.com/the-miint/duckdb-miint
> **Last checked:** 2026-07-15. This header is only the freshness marker for the [Refresh trigger](#keeping-this-doc-fresh) below — **per-function verification dates + build hashes live inline in the [Function inventory](#function-inventory)**, each entry carrying its own **qiita-verified DATE (build)** tag. Most recent passes: `alignment_is_mapped_primary` + the reference-annotation `read_gff` / `compute_coverage_depth` ⚠️ conventions (see their inventory entries), and the `woltka_ogu` id-type-preservation fix the OGU feature-table needs (native BIGINT `reference`/`sample_id`, no `::VARCHAR` casts) — now on the team mirror.
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
- SAM flag predicates: the **`alignment_is_*` family** — `alignment_is_paired` / `_proper_pair` / `_unmapped` / `_mate_unmapped` / `_reverse` / `_mate_reverse` / `_read1` / `_read2` / `_secondary` / `_primary` / `_qc_failed` / `_duplicate` / `_supplementary`. Each takes the `USMALLINT` `flags` column and returns BOOLEAN. All 13 verified present by probe against the team-mirror build (2026-07-13); HTSlib-compatible aliases (`is_unmapped`, `is_secondary`, `is_supplementary`, `is_dup`, `is_qcfail`) are also present, and upstream documents further ones this probe did not enumerate. **Use these — do not hand-roll bit math on `flags`.** One trap: `alignment_is_primary` means "neither secondary nor supplementary" and is **TRUE for an unmapped read** (probed: `flags=0x4` → `primary=true`), so it does not imply mappedness — an "aligned primary" test is `alignment_is_primary(flags) AND NOT alignment_is_unmapped(flags)`. The mirror build also carries **`alignment_is_mapped_primary(flags)`**, the single-call form of exactly that test — probed equivalent across mapped / unmapped / secondary / supplementary flags (2026-07-15), and pinned in `qiita-compute-orchestrator/tests/jobs/test_syndna_smoke.py::test_mapped_primary_predicate`. Prefer it over the two-conjunct form.
- `alignment_seq_identity(cigar, nm, md, type)` — identity from CIGAR + NM/MD tags
- `cigar_sequence_identity(cigar)` — identity from the CIGAR **alone, no tags**. **qiita-verified 2026-07-13** (mirror `5447847`): on minimap2's eqx CIGAR (`=`/`X`) it reproduces `alignment_seq_identity(..., 'blast')` EXACTLY (probed 0.98 / 0.993 / 0.9709 on reads carrying mismatches and a 30 bp deletion). This matters because **`alignment_slice` NULLs `tag_nm`/`tag_md` on any read it TRIMS**, which makes `alignment_seq_identity` return NULL on precisely the boundary-spanning reads — so `cigar_sequence_identity` is the function to score a SLICED alignment with. Pair it with `cigar_query_coverage` for the alignment ratio; between them the tags are redundant. Used by `align_sharded`'s identity gate.
- `cigar_query_length(cigar, [include_hard_clips=true])`
- `cigar_query_coverage(cigar, [type='aligned'])`
- `mask_dust(sequence, [hardmask=false])` — low-complexity masking
- `merge_pairs_vsearch(fwd_seq, fwd_qual, rev_seq, rev_qual, [options])`
- `phylogeny_fasttree_available()` — capability probe
- `install_gpl_boundary([force])` — install the out-of-process GPL tool host (see Internals)

### Analysis / aggregate — `docs/analysis-functions.md`
- `woltka_ogu(relation, sequence_id_field [, sample_id])` — OGU feature table. **qiita-verified 2026-07-14** (estimate_feature_table smoke + `tests/integration/test_estimate_feature_table.py`, local build `9f31276`): the `relation` + field args are **quoted string literals** resolved on a SEPARATE connection → source must be a real non-temp TABLE (not a registered view/stream/TEMP/CTE); `sample_id` is a NAMED arg (`sample_id := 'col'`) → per-sample output `({sample_id}, feature_id, value DOUBLE)`, else `(feature_id, value)`. Required source cols: `sequence_id_field`, `reference` (the OGU key — counted per read, fractionally split across a read's UNIQUE `reference` values), `flags` (USMALLINT). Takes **native-integer** `reference`/`sample_id` (BIGINT/UUID/VARCHAR; `feature_id` inherits `reference`'s type) with **no `::VARCHAR` casts** — requires the id-type-preservation fix (`9f31276`; earlier builds forced a VARCHAR `feature_id` and aborted on a BIGINT `reference`). Rejects a non-id `reference` type with a clean BinderException; rejects an all-NULL `sample_id` source (empty input). Consumed by `estimate_feature_table`.
- `sequence_dna_reverse_complement(seq)` / `sequence_rna_reverse_complement(seq)`
- `sequence_dna_as_regexp(seq)` / `sequence_rna_as_regexp(seq)` — IUPAC-aware regex compile
- `sequence_split(seq, chunk_size)` → `LIST(STRUCT(chunk_index INTEGER, chunk_data VARCHAR))` — fixed-width chunking in a single linear pass; `UNNEST` for chunk rows. Backs qiita's chunked-Parquet write (`stage_local_fasta`, CLI `reference load`), replacing an O(L²) SQL macro (duckdb-miint #121 / DuckDB #23229; added 2026-06)
- `compress_intervals(start, stop)`
- `compute_coverage_depth(position, stop_position, cigar, reference_length, mode)` → `UINTEGER[]` — **an AGGREGATE, not a table function** (`qiita-verified 2026-07-13`, mirror `5447847`). Element `i` is the depth at 1-based position `i+1`. `position` is 1-based INCLUSIVE, `stop_position` 1-based EXCLUSIVE (matches `read_alignments` / `align_minimap2`). `mode` must be a CONSTANT: `'exclude_deletions'` (only `M`/`=`/`X`; = `samtools depth` default) or `'include_deletions'` (`D` also counts; = `samtools depth -J`). The mode is not cosmetic — probed 2.200 vs 2.210 mean depth on the same reads.
  **Scaling trap:** it allocates an array of `reference_length` PER GROUP (documented cap 2e9). Fine for a 4 kb plasmid; fatal for a genome. To quantify a sub-interval, `alignment_slice` to the window and SHIFT `position`/`stop_position` by `start - 1` so `reference_length` is the WINDOW length — probed bit-identical to slicing the whole-reference array, but O(window) instead of O(reference).
- `genome_coverage(alignments, subject_total_length, subject_genome_id)` — per-genome breadth of coverage. **qiita-verified 2026-07-14** (same smoke, local build `9f31276`): a table MACRO taking three **unquoted relation names** resolved in the caller's connection (VIEW or TABLE both work). `alignments(reference, position, stop_position)` — `position`/`stop_position` HALF-OPEN (matches our `alignment`); `subject_total_length(genome_id, total_length BIGINT)`; `subject_genome_id(contig_id, genome_id)` maps contig→genome (multi-contig native). Native-integer ids, no casts. Returns `(genome_id, covered BIGINT, proportion_covered DOUBLE)` = merged covered bases ÷ FULL genome length (the length table must include unaligned contigs). Consumed by `estimate_feature_table`.
- Pairwise alignment helpers (see source for variants)
- `formula(formula_string)` — chemical formula parser
- `massql(query, source)` — MassQL DSL → spectra match (also see `docs/massql.md`)
- `miint_version()`

### Table functions (`SELECT * FROM …`) — `docs/table-functions.md`
**Sequence/alignment readers:**
- `read_alignments(filename, [reference_lengths='table_name'], [include_filepath=false], [include_seq_qual=false])` — SAM/BAM/CRAM
- `read_sequences_sam(filename)` — SAM/BAM/CRAM as reads (not alignments), emitting a `read_fastx`-compatible schema (`sequence_index, read_id, comment, sequence1/2, qual1/2`; `qual*` phred-decoded `UTINYINT[]`). **Undocumented upstream**; one row per SAM record (no FLAG column, no secondary/supplementary filter; empty input → 0 rows, not a throw). Contract pinned in `qiita-compute-orchestrator/tests/jobs/test_bam_to_parquet_miint_contract.py`; consumed by `bam_to_parquet`.
- `alignment_slice(table_name, start, stop, [include_deletions=false])` — restrict alignments to a reference REGION. `table_name` is a table/view NAME; `start` is 1-based INCLUSIVE, `stop` 1-based EXCLUSIVE. **qiita-verified 2026-07-13** (mirror `5447847`): non-overlapping reads are dropped; an overlapping read is TRIMMED — the clipped CIGAR portion becomes a hard clip (`H`), and `position` is advanced to the window start, while coordinates stay in PARENT (not window-local) space. Two traps, both silent:
  1. **Tags and `template_length` are set to NULL on any trimmed read**, so `alignment_seq_identity(cigar, tag_nm, tag_md, …)` returns NULL for exactly the boundary-spanning reads — an `identity >= x` predicate over sliced rows drops them (`NULL >= x` → NULL → filtered). Score sliced rows with `cigar_sequence_identity` / `cigar_query_coverage` instead (see above); on an eqx CIGAR they need no tags.
  2. **`read_id` is coerced BIGINT → VARCHAR** (`align_minimap2` emits BIGINT). Cast back or the join silently matches nothing.
- `read_fastx(filename, [sequence2=filename], [include_filepath=false], [qual_offset=33])` — FASTQ/FASTA, paired
- `read_sequences_sff(filename, [include_filepath=false], [trim=true])`
- `read_biom(filename, [include_filepath=false])`

**Mass spec:** `read_mzml`, `read_mzxml`, `read_mzml_chromatograms` (all `(filename, [include_filepath=false])`)

**Annotation / public data:**
- `read_gff(path)` → `seqid, source, type, position, stop_position, score, strand, phase, attributes`. **qiita-verified 2026-07-13** (mirror `5447847`). `attributes` is already a `MAP(VARCHAR, VARCHAR)` — no parsing needed (`attributes['ID']`, `attributes['mass_ng']`); `parse_gff_attributes(kvp_string)` exists for a raw string. **⚠️ `position`/`stop_position` are GFF3's 1-based CLOSED `[start, end]`** — `stop_position` is INCLUSIVE. `read_alignments` / `alignment_slice` / `compute_coverage_depth` / the `qiita_lake.alignment` table all use 1-based HALF-OPEN. **The two spell the column the same name**, so feeding one to the other type-checks, runs, raises nothing, and just silently drops the interval's last base. qiita converts ONCE at ingest (`hash_sequences._write_annotation_manifest` stores half-open); pinned, with an anti-vacuity control, by `tests/jobs/test_annotation_ingest_smoke.py`. **⚠️ `read_gff` does NOT stop at a `##FASTA` directive** (qiita-verified 2026-07-14 by probe on a real prokka GFF3): it keeps reading and returns ONE ROW PER LINE of the embedded FASTA, with the nucleotide line itself in `seqid` and NULL in every other column — 1638 rows for a 99-feature file, versus exactly 99 for the byte-identical file with the section stripped (the control). prokka and bakta ALWAYS append the genome that way, so this is the common case, not a corner one, and the junk rows fail downstream with a misleading error rather than at the parse. Filter on `type IS NOT NULL` — a GFF3 feature line always has one. Note also that a GFF3 `ID` is **not unique by spec**: a DISCONTINUOUS feature (a ribosomal-slippage CDS) repeats one `ID` across N lines, and NCBI's E. coli K-12 RefSeq has 20 such repeats — so never key on it. Consumed by the reference-annotation ingest (`--gff`).
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
- `align_minimap2_sharded(query_table, shard_directory:=, read_to_shard:=, [preset, max_secondary, include_shard_name, …])` — per-shard minimap2 alignment. **qiita-verified 2026-07-09** (`align_sharded` + `test_sharded_alignment` smoke + schema probe). `query_table` (positional) + `read_to_shard` are table NAMEs resolved on a SEPARATE connection (non-temp VIEW/TABLE). `shard_directory` is a FLAT dir of `{shard_name}.mmi` files; the shard set = distinct `shard_name` in `read_to_shard`. `read_to_shard(read_id, shard_name)` — `read_id` type must EXACTLY match `query_table.read_id`; a read under K `shard_name`s aligns against all K. Output = 21 standard SAM columns `(read_id, flags, reference, position, stop_position, mapq, cigar, mate_reference, mate_position, template_length, tag_*)` (+ trailing `shard_name` VARCHAR when `include_shard_name := true`); `reference`/`mate_reference` are VARCHAR = the subject's stored id (our builders store `feature_idx`, so `feature_idx = CAST(reference AS BIGINT)`), `position`/`stop_position` BIGINT, `flags` USMALLINT, `mapq` UTINYINT. A PE read emits one row per mate. Tolerates a query MIXING null/non-null `sequence2`. Consumed by `align_sharded`.
- `save_bowtie2_index(subject_table, output_path, [threads])` — writes a bowtie2 index (a `.bt2` set). **qiita-verified 2026-07-08** (build_bowtie2_index host smoke, team-mirror build `ec2ef3e`): exactly **two positional args** (subject-table NAME, output path PREFIX), one named opt `threads`; **no `preset`** (unlike `save_minimap2_index` — the bowtie2 index is preset-independent). The subject table needs `(read_id, sequence1)` (`read_id` may be BIGINT). Returns one row `(success BOOLEAN, index_path VARCHAR, num_subjects BIGINT)` — assert `success`. Writes SIX files under the prefix (`index.{1..4}.bt2`, `index.rev.{1,2}.bt2`), so `output_path` is a prefix and `reference_index.fs_path` is that prefix. Needs **no** `install_gpl_boundary()`. Only in a mirror build **newer than `5392909`** — pull the current mirror head. Consumed by `build_bowtie2_index`.
- `align_bowtie2(query_table, subject_table, [options])`
- `align_bowtie2_sharded(query_table, shard_directory:=, read_to_shard:=, [max_secondary, include_shard_name, preset, …])` — the bowtie2 twin of `align_minimap2_sharded`. **qiita-verified 2026-07-09** (smoke + probe): same table-NAME resolution, `read_to_shard` shape, and 21-column output, EXCEPT `shard_directory` holds one SUBDIR per shard (`{shard_name}/index.*.bt2`), and — unlike minimap2 — a query batch must be UNIFORMLY single- OR paired-end: a MIX of null and non-null `sequence2` raises `gpl_boundary: … all must be non-null for paired-end`. A read set is uniformly SE or PE by construction (a prep/run is one or the other), so this rejection is the aligner enforcing that invariant, NOT a signal to split: `align_sharded` feeds a single uniform query and lets both aligners handle the mode natively (a mix is invalid input). No `preset` needed (a bowtie2 index is preset-independent). Consumed by `align_sharded`.
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
- `rype_classify(index_path, sequence_table, [id_column='read_id'], [threshold=0.1], [negative_index=path])` — `(read_id, bucket_id, bucket_name, score)`, **≥0 rows per read: ONE ROW PER BUCKET the read matches above `threshold`**. A SINGLE-bucket index (the host `.ryxdi`) therefore emits ≤1 row/read; a MULTI-bucket index (the whole-reference shard router, one bucket per `shard_id`) emits one row per matching bucket, so a read whose minimisers span K shards yields K rows — this is how `align_sharded` builds `read_to_shard`. **qiita-verified 2026-07-09** (router smoke: a read spanning two shard buckets emitted two `bucket_name` rows) and **2026-06-15** (host_filter probe + smoke, mirror build `e6f598d`): qiita passes the **POSITIVE** host index (host = any emitted row, low explicit `threshold` — NOT `negative_index`/`-N`). It reads `sequence1` and, when present, `sequence2` (a host match in EITHER mate emits the read; a NULL `sequence2` is tolerated). `id_column` accepts `{VARCHAR, BIGINT, UUID}` on input. Output id type is build-dependent (a probe of the ftp.microbio.me build `e6f598d` returned VARCHAR for a BIGINT input; #126's input-type round-trip may differ on a newer build), so `host_filter` does not depend on it: it appends `read_id` into a BIGINT accumulator column, which coerces either type on insert. It also DISTINCTs the result — the table-function interface does not guarantee one best-hit row per read. Consumed by `host_filter`.
- `rype_log_ratio(numerator_path, denominator_path, sequence_table, [id_column='read_id'], [skip_threshold=0.5])`
- `rype_extract_minimizer_set(sequence_table, k, w, [salt=6148914691236517205], [id_column='read_id'])`
- `rype_extract_strand_minimizers(sequence_table, k, w, [salt=…], [id_column='read_id'])`
- `rype_index_create(chunk_table, output_path, [mapping_table], [k=64], [w=50], [salt=6148914691236517205], [orient=true], [max_memory=0])` — builds a `.ryxdi` minimizer index. **Exactly two positional args** (`chunk_table`, `output_path`); everything else is named. `chunk_table` is a table/view with fixed columns `(feature_idx BIGINT, chunk_index INTEGER, chunk_data VARCHAR|BLOB)`; optional `mapping_table` is `(feature_idx BIGINT, bucket_name VARCHAR)` (omit → one unnamed bucket). Returns one status row `(output_path, k, w, status)` — assert `status = 'ok'`. **qiita caveat:** only in the **team-mirror** build (`https://ftp.microbio.me/pub/miint`), not the community channel as of the last check — the orchestrator's reference-indexing tests install via `MIINT_EXTENSION_REPO`. Signature **qiita-verified 2026-06-01 via a real smoke test** (the published `docs/rype.md` was slightly off — note the two-positional/rest-named split above). Consumed by the `build_rype_index` native job ([host references](reference-data-staging.md#host-references-and-the-rype-index)) with a SINGLE-bucket mapping, and by the `build_routing_index` job with a MULTI-bucket mapping (`bucket_name = str(shard_id)`, one bucket per shard) to build the whole-reference shard router `rype_classify` reads to route reads.

- `infer_trim(original_reads, qcd_reads)` — **table macro**, not a scalar. Both args are relations exposing `sequence_index` (BIGINT, the join key) and `sequence` (VARCHAR). Returns ONE ROW PER ORIGINAL read: `(sequence_index BIGINT, trimmed_5p UINTEGER, trimmed_3p UINTEGER)`, with **`NULL`/`NULL` when the tool omitted the read**. `LEFT JOIN … USING (sequence_index)`, then locates the QC'd sequence inside the original via `position()`; leftmost match wins. **Fails loud** if a kept read is not a contiguous substring of its original (the tool edited internal bases) — that is a feature, do not suppress it. No uniqueness policing on `qcd_reads.sequence_index`: a duplicate fans the join out.
  **qiita gotcha:** the caller must carry its OWN key through the external tool. `read_fastx` assigns `sequence_index` POSITIONALLY and resets per file, so it is NOT recoverable by re-parsing the tool's output. For the lima chain the key channel is *not* the record name: lima requires PacBio's `<movie>/<zmw>/ccs` naming and **rewrites** each emitted record's name from its int32 `zm` tag, so a lake-wide `sequence_idx` cannot ride there (it would come back truncated). `lima_export` writes a dense per-file ZMW plus a `lima_zmw_map.parquet` (`zmw -> sequence_idx`), and `lima_mask` joins it back. The `infer_trim` signature itself is **qiita-verified 2026-07-09 by probe** (mirror build `ec2ef3e`) + `tests/jobs/test_lima_chain_smoke.py`; the lima-side naming/`zm` facts in this paragraph are **not** miint claims — they were established against **lima 2.13.0 on 2026-07-16** (see the `FORMAT SAM|BAM` note below and `tests/jobs/test_lima_chain_smoke.py`). Probed at deploy by `compute-readiness`'s `miint-infer-trim` check. Consumed by the `lima_mask` native job (read-mask's adapter chain).

### QC — fastp algorithm port (`docs/qc.md` upstream)
The fastp-equivalent read-QC functions, consumed by the `qc` native job (the bcl-convert → `fastq` → **`qc`** → `host_filter` pipeline). **qiita-verified 2026-06-18** against the team-mirror build (`SELECT qc_version()` → `qc 0.1.0 (port of fastp algorithms)`); pinned by `qiita-compute-orchestrator/tests/jobs/test_qc_miint_contract.py`. **All take POSITIONAL args only — named params (`min_length := 100`) raise a `BinderException`** (the upstream `docs/qc.md` shows named optionals; the mirror build does not accept them). `qual` is `UTINYINT[]` (phred-decoded, exactly what `read_fastx` / `fastq_to_parquet` emit). Each returns a `STRUCT`.
- `filter_read(seq, qual [, min_length, max_length, qualified_q, max_unqualified_pct, max_n, min_avg_q])` → `STRUCT(passed BOOLEAN, fail_reason VARCHAR, length UINTEGER, n_bases UINTEGER, low_qual_bases UINTEGER, mean_quality FLOAT)`. Only a **2-arg** and an **8-arg** overload exist (no partial). The 2-arg defaults equal `(15, 0, 15, 40, 5, 0)` — these ARE fastp's defaults — so a faithful `fastp -l 100` is the 8-arg call `filter_read(seq, qual, 100, 0, 15, 40, 5, 0)`. `fail_reason` is `NULL` when passed, else `'length'` / `'n_base'` / `'quality'` / `'too_long'`.
- `trim_adapters(seq, qual, adapter [, match_revcomp, min_match, allow_pre_start])` → `STRUCT(sequence VARCHAR, quality UTINYINT[], trimmed_5p UINTEGER, trimmed_3p UINTEGER)`. `adapter` is `VARCHAR` **or** `VARCHAR[]` (a known-adapter set). The single-end path.
- `trim_adapters_pe(seq1, qual1, seq2, qual2 [, adapters VARCHAR[], overlap_require, overlap_diff_limit, overlap_diff_percent_limit, match_revcomp, min_match, allow_pre_start])` → `STRUCT(sequence1, quality1, sequence2, quality2, overlap_len INTEGER, adapter_trimmed BOOLEAN, trimmed1_3p UINTEGER, trimmed2_3p UINTEGER)`. Only a **4-arg** (overlap-only) and an **11-arg** overload. The 11-arg form with an **empty** adapter list + `(30, 5, 20, false, 0, false)` reproduces the 4-arg result exactly — those are fastp's overlap defaults — so pass a non-empty `adapters` to add the by-sequence adapter fallback fastp applies *after* overlap analysis, without changing overlap behavior.
- `trim_polyg(seq, qual [, min_len, max_mm, max_window_mean_q])` → `STRUCT(sequence, quality, trimmed_5p, trimmed_3p)`. Trims a 3' G-run **only when its quality is low** (2-color no-signal); a high-quality G-run is left intact. Defaults `(10, 5, 5)`. fastp enables polyG only for 2-color instruments (NextSeq/NovaSeq), so the `qc` job calls it gated on `instrument_model`.
- `qc_version()` → `VARCHAR` — QC-port version sanity probe.

### COPY writers — `docs/copy-formats.md`
`COPY <query> TO 'path' (FORMAT { FASTQ | FASTA | SAM | BAM | BIOM | NEWICK })` — see the doc for required column shapes (e.g. SAM/BAM expects HTSlib-style columns; BIOM expects a feature table layout).

**⚠️ `FORMAT SAM` / `FORMAT BAM` is an ALIGNMENT writer, NOT a reads writer — qiita-verified 2026-07-16 by probe** (mirror build `ee7015b`, DuckDB v1.5.4; pinned by `qiita-compute-orchestrator/tests/jobs/test_sam_bam_writer_miint_contract.py`). It is **not** a way to write an unaligned BAM, and the miint-first rule does not apply to that job:
- **It never emits SEQ/QUAL.** Every record lands with `*` in both fields, under every column naming tried (`sequence1`/`qual1`, `sequence`/`quality`, `seq`/`qual`, `sequence`/`qual`). Established with an anti-vacuity **mapped-record control** (real `@SQ` reference + `10M` cigar), so it is not an artifact of an unmapped record. The reads themselves are simply not part of this writer's output.
- Required columns are the alignment ones, verbatim: `read_id`, `flags` (`USMALLINT`), `reference`, `position` (**BIGINT**), `mapq`, `cigar`, `mate_reference`, `mate_position`, `template_length`.
- `REFERENCE_LENGTHS '<table>'` is **mandatory** (it builds the `@SQ` header) and the table must be non-empty — so a header-less / reference-less uBAM is not expressible.
- There is **no read-group option**: `READ_GROUP` / `RG` / `HEADER` / `EXTRA_HEADER` all raise `Unknown option for COPY FORMAT SAM`.

Consequence: `lima_export` writes its CCS uBAM with **pysam**, not miint — it needs `@RG … DS:READTYPE=CCS` (the one field that keeps lima off the CLR path that hangs) and it needs SEQ/QUAL to actually be in the file. That is the sole sanctioned non-miint sequence writer in the repo; revisit it if miint grows a uBAM writer.

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
