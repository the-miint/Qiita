### Added

- **`POST /study/{study_idx}/biosample/lookup` resolves many biosample identifiers within one
  study (#N).** The body names one identifier kind: `export_entity_id`, `idx`,
  `biosample_accession`, `ena_sample_accession`, `matrix_tube_id`, or `unique_field` with the
  study field's display name. Each value maps to the identifier that `return_key` chooses.
  A value naming no live biosample linked to the study, or a unique-field value that the
  caller's tier cannot read, is listed in `missing`. Access is that of the study-scoped
  biosample reads.
- **`qiita biosample lookup` resolves a TSV file's column of biosample identifiers within one
  study (#N).** `--key` names the identifier kind, or `--unique-field-name` a unique field of
  the study, and the file's column with that exact header holds the values. The command writes
  to `--output` each row's value and its `export_entity_id` (or, with
  `--return-key biosample_idx`, its `biosample_idx`), one row per input row in input order, so the result
  pastes beside the input file. Any value that does not resolve makes the command list every
  such value and write no file. A file whose data lines are not one row each (an empty line
  between rows, or a line break inside a cell) is refused, since its output would not line up.

### Changed

- **`POST /study/lookup` and `POST /biosample/lookup` return each value's
  `export_entity_id` by default (#N).** A new body field, `return_key`, chooses what each
  resolved value maps to: `export_entity_id` (the default) or `idx`, the study's or
  biosample's idx. `qiita biosample bulk-create` and `qiita biosample link` ask for the idx
  when they resolve `--study-export-id`. In the OpenAPI spec, `NaturalKeyLookupResponse`'s
  `resolved` values become integer-or-string, which a generated client sees on every route
  that returns it, including the deprecated lookups and
  `POST /sequencing-run/lookup-by-instrument-run-id`; those still return integers.
- **`qiita biosample bulk-create` writes the created biosamples' TSV to a file named by a
  required `--output` (#N), instead of printing it.** The command refuses an existing file
  or a missing directory before it creates anything, and writes the file only on success,
  so a failed run leaves no partial file behind.
