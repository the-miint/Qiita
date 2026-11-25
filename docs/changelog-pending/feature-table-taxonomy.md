### Added

- **A feature table's taxonomy, on request.** A `POST /feature-table` request may name a
  reference by its public `(name, version)` handle (`taxonomy`); the answer then also
  carries each feature's lineage under that reference, keyed by the same exported-feature
  handle the table uses, with the reference's ranks `(domain … strain)` and their
  `d__/p__…` prefixes, and records which reference classified it. A feature's
  `feature_idx` joins straight to the reference's taxonomy, so a feature the reference does
  not place keeps its row with NULL ranks rather than being dropped. An unknown reference
  is a 404; one that is not `active`/`indexing`, or that has no taxonomy loaded, is a 409.
  `qiita feature-table select --taxonomy NAME VERSION` writes it beside the bundle as
  `<stem>.taxonomy.parquet` and records the reference in the manifest.
