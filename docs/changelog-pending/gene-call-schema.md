### Added

- **Lake tables for Prodigal gene calls on assembly subjects (#gene-call-schema).** Two
  new DuckLake tables, created empty at data-plane start: `gene_call`, one row per gene,
  and `gene_call_contig`, one row per contig of each called subject with the settings,
  the outcome and Prodigal's model. Each row names its run, its subject as `bin_quality`
  does, and its call set: `prodigal_checkm` (CheckM's own rules) or
  `prodigal_meta_closed` (metagenomic mode, no gene running off a contig's edge).
  Coordinates are on the contig's canonical strand. Each registration replaces the
  run's rows. Nothing writes them yet; `qiita_common.gene_call_constants` holds the
  columns.
- **A `gene_call` job that calls those genes (#gene-call-schema).** A native job that
  runs Prodigal through miint's `predict_genes` on every subject CheckM scores (each
  refined bin, circular contig and unbinned contig of 300 kb or more), once per call
  set. It writes the rows for both tables, keyed by canonical sequence hashes for
  minting, and each subject's `prodigal_checkm` genes as `genes.faa` and `genes.gff`
  in the layout CheckM reads. No workflow runs it yet. It needs gpl-boundary 0.5.0 or
  later.

### Changed

- **`docs/duckdb-miint.md` lists Prodigal among the GPL-boundary tools
  (#gene-call-schema).** It said miint did not route to the boundary's Prodigal; since
  `predict_genes` it does.
