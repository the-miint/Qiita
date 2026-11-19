### Added

- **`POST /study/{study_idx}/biosample/lookup` resolves many biosample identifiers within one
  study (#N).** The body names one identifier kind: `export_entity_id`, `idx`,
  `biosample_accession`, `ena_sample_accession`, `matrix_tube_id`, or `unique_field` with the
  study field's display name. Each value maps to the identifier that `return_key` chooses.
  A value naming no live biosample linked to the study, or a unique-field value that the
  caller's tier cannot read, is listed in `missing`. Access is that of the study-scoped
  biosample reads.

### Changed

- **`POST /study/lookup` and `POST /biosample/lookup` return each value's
  `export_entity_id` by default (#N).** A new body field, `return_key`, chooses what each
  resolved value maps to: `export_entity_id` (the default) or `idx`, the study's or
  biosample's idx. `qiita biosample bulk-create` and `qiita biosample link` ask for the idx
  when they resolve `--study-export-id`. In the OpenAPI spec, `NaturalKeyLookupResponse`'s
  `resolved` values become integer-or-string, which a generated client sees on every route
  that returns it, including the deprecated lookups and
  `POST /sequencing-run/lookup-by-instrument-run-id`; those still return integers.
