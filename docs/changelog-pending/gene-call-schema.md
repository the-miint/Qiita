### Added

- **Prodigal gene calls on assembled genomes, in two new lake tables (#888).**
  `gene_call` has one row per gene and `gene_call_contig` one row per contig of each bin
  or contig CheckM scores, with the settings, the outcome and Prodigal's model. Each row
  names its run, the subject by its `genome_idx`, and its call set: `prodigal_checkm`
  (CheckM's own rules) or `prodigal_meta_closed` (metagenomic mode, no gene running off
  a contig's edge). A gene is named by a `feature_idx` minted from its bytes, as a
  reference annotation is, and its contig by `parent_feature_idx`; coordinates are on
  the contig's canonical strand. Each registration replaces the run's rows.
  `qiita_common.gene_call_constants` holds the columns.
- **long-read-assembly 1.0.2 calls the genes (#888).** Edited in place: a `gene_call`
  step runs Prodigal through miint's `predict_genes` on every refined bin, circular
  contig and unbinned contig of 300 kb or more, once per call set; a
  `mint-gene-features` action mints the genes' `feature_idx`; an
  `export-subject-genome-map` action hands `assembly_load` each subject's `genome_idx`;
  and `assembly_load` stages the two tables. The step also writes each subject's
  `prodigal_checkm` genes as `genes.faa` and `genes.gff` in the layout CheckM reads,
  which CheckM does not read yet. It needs gpl-boundary 0.5.0 or later, and a miint
  build with `prodigal_proteins`.
- **`assembly_membership` and `bin_quality` carry `genome_idx` in the lake (#888).**
  The subject's `qiita.genome`, the key the gene-call tables use. Rows registered
  before this are NULL until their run is registered again.
- **A one-off `assembly-gene-call-recover` workflow and its admin backfill (#888).**
  For each long-read-assembly run finished before this deploy, it calls the genes from
  the `assemble` and `bin_refine` outputs left in the run's ticket workspace and
  registers, under the run's own `processing_idx`, its gene calls and its
  `assembly_membership` and `bin_quality` again with `genome_idx`.

### Changed

- **compute-readiness fails on a gpl-boundary older than 0.5.0
  (#888).** The `miint-gpl-boundary` probe now calls `predict_genes` as
  well as `save_bowtie2_index`; `predict_genes` refuses an older binary.
- **Registration refuses a table that borrows its replace keys without the table it
  borrows them from (#888).** `bin_quality`, the myloasm tables and the
  gene-call tables delete a run's earlier rows on the keys of `assembly_membership` or
  `myloasm_run` in the same registration; registered without it, an empty file deleted
  nothing. Every load registers each set together.
- **`assembly_hash` refuses an absent `refined_bins_dir` (#888).** It
  read one as no bins, which would count every binned contig as unbinned; bin_refine
  always creates the directory.

- **`docs/duckdb-miint.md` lists Prodigal among the GPL-boundary tools
  (#888).** It said miint did not route to the boundary's Prodigal; since
  `predict_genes` it does.
- **`CLAUDE.md` has a "Writing DuckDB SQL" section (#888).** It points at DuckDB's
  join types, and asks for `SEMI` / `ANTI JOIN` over `IN` / `NOT IN` subqueries, a CTE
  over a temp table read once, and join keys stored as columns.
