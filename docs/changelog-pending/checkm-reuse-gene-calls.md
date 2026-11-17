### Changed

- **CheckM scores the genes that the gene_call step called, instead of calling Prodigal
  again (#TBD).** In long-read-assembly 1.0.2 (edited in place), the `checkm` step takes
  `gene_calls` and copies each subject's `prodigal_checkm` `genes.faa` / `genes.gff` to
  where CheckM looks for a bin's genes, so the CheckM scores and the `gene_call` lake rows
  come from the same genes, and CheckM no longer calls Prodigal on a subject that
  gene_call found genes for. A subject that gene_call found no genes for is still called
  by CheckM. The checkm image moves to
  `long-read-assembly-checkm-1.0.2.sif`. Runs finished before keep their CheckM scores.
