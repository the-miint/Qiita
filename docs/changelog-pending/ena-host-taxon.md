### Changed

- **An ENA import now writes `host taxon id` and `taxon id` from ENA instead of
  `not provided` for every biosample (#653).** Each run's `host_tax_id` and `tax_id` are read with
  the run list; a taxon id is written only if it is a loaded NCBI Taxonomy term, and
  otherwise the field is `not provided` and the gap is reported in `metadata_warnings` on
  the run entry of `GET /ena-import-batch/{idx}`. A `host_tax_id` equal to the
  biosample's own `tax_id`, to a taxon in the curated table of biosample taxa with an
  implied host (or none), or to the bare `metagenome` taxon is ignored with a warning, and
  so is a biosample attribute tagged `taxon id` or `host taxon id`. With no `host_tax_id`,
  any free-text `host` that is not a missing-value term gives `not provided`; otherwise
  the host comes from the table, which gains the human, human skin, mouse gut and mouse
  skin metagenomes and nine natural hostless environments such as soil and marine
  sediment (`not applicable`). `qiita-admin backfill host-taxon-id` shares the table, so
  biosamples with those taxa are now written instead of reported unresolved. Biosamples
  imported earlier are unchanged. The `qiita submit-host-filter-pool` refusal for an
  unresolved host also names `qiita biosample patch-metadata-by-unique-field`, which
  replaces a stored `not provided`; the backfill skips it.
