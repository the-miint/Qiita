### Added

- **`POST /study/lookup` and `POST /biosample/lookup` resolve by any natural key, including
  `export_entity_id` (#896).** The body names the key (required) and up to 10,000 values; the
  response is the `resolved` / `missing` shape that the older lookups return. Study keys:
  `export_entity_id`, `bioproject_accession`, `ena_study_accession`. Biosample keys:
  `export_entity_id`, `biosample_accession`, `ena_sample_accession`, `matrix_tube_id` (a
  malformed tube id is a 422). A retired biosample is reported `missing` under every key.
- **`qiita biosample bulk-create` creates one biosample per row of a TSV file on a study,
  in one all-or-nothing request (#896).** `--tsv-file` names a tab-separated file with a header
  row: the column named by `--owner-biosample-id-field-name` holds each row's owner's
  biosample name, columns named `biosample_accession`, `ena_sample_accession`,
  `matrix_tube_id` or `metadata_checklist_name` set those attributes, and every other column
  is metadata keyed on its header; a blank cell is left unset. The study is named by
  `--study-export-id` (preferred) or `--study-idx`. It prints a TSV giving, for each created
  biosample in the file's row order, the owner's biosample name, `biosample_idx` and
  `export_entity_id`; the owner column therefore cannot be named `biosample_idx` or
  `export_entity_id`.
- **`qiita biosample link` accepts `--study-export-id` in place of `--study-idx` (#896).**

### Changed

- **`POST /study/lookup-by-accession`, `POST /biosample/lookup-by-accession` and
  `POST /biosample/lookup-by-matrix-tube-id` are deprecated in favor of the routes above
  (#896).** They still work, and the OpenAPI spec marks them deprecated.
- **The OpenAPI response schema of the lookup routes is named `NaturalKeyLookupResponse`
  (#896).** This covers the deprecated routes above and
  `POST /sequencing-run/lookup-by-instrument-run-id`; the JSON they return is unchanged, and
  the former Python model names remain importable from `qiita_common.models`.
