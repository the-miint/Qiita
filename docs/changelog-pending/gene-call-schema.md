### Added

- **Lake tables for Prodigal gene calls on assembled genomes (#gene-call-schema).** Two
  new DuckLake tables, created empty at data-plane start: `gene_call`, one row per gene,
  and `gene_call_contig`, one row per contig of each bin or contig CheckM scores, with
  the settings, the outcome and Prodigal's model. Each row names its run, the bin or
  contig (kind and bin_id, as in `bin_quality`), and its call set: `prodigal_checkm` (CheckM's own rules) or
  `prodigal_meta_closed` (metagenomic mode, no gene running off a contig's edge).
  Coordinates are on the contig's canonical strand. Each registration replaces the
  run's rows, including when a re-run has nothing to call. Nothing writes them yet; `qiita_common.gene_call_constants` holds the
  columns.
- **A `gene_call` job that calls those genes (#gene-call-schema).** A native job that
  runs Prodigal through miint's `predict_genes` on every bin or contig CheckM scores (each
  refined bin, circular contig and unbinned contig of 300 kb or more), once per call
  set. It writes the rows for both tables, keyed by canonical sequence hashes for
  minting, and each subject's `prodigal_checkm` genes as `genes.faa` and `genes.gff`
  in the layout CheckM reads. No workflow runs it yet. It needs gpl-boundary 0.5.0 or
  later. It builds Prodigal's protein FASTA headers itself until miint can write them
  (duckdb-miint#446).

### Changed

- **compute-readiness fails on a gpl-boundary older than 0.5.0
  (#gene-call-schema).** The `miint-gpl-boundary` probe now calls `predict_genes` as
  well as `save_bowtie2_index`; `predict_genes` refuses an older binary.
- **Registration refuses a table that borrows its replace keys without the table it
  borrows them from (#gene-call-schema).** `bin_quality`, the myloasm tables and the
  gene-call tables delete a run's earlier rows on the keys of `assembly_membership` or
  `myloasm_run` in the same registration; registered without it, an empty file deleted
  nothing. Every load already registers them together.
- **`assembly_hash` refuses an absent `refined_bins_dir` (#gene-call-schema).** It
  read one as no bins, which would count every binned contig as unbinned; bin_refine
  always creates the directory.

- **`docs/duckdb-miint.md` lists Prodigal among the GPL-boundary tools
  (#gene-call-schema).** It said miint did not route to the boundary's Prodigal; since
  `predict_genes` it does.
