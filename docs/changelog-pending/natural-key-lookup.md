### Added

- **`POST /study/lookup` and `POST /biosample/lookup` resolve by any natural key, including
  `export_entity_id` (#N).** The body names the key (required) and up to 10,000 values; the
  response is the `resolved` / `missing` shape the older lookups return. Study keys:
  `export_entity_id`, `bioproject_accession`, `ena_study_accession`. Biosample keys:
  `export_entity_id`, `biosample_accession`, `ena_sample_accession`, `matrix_tube_id` (a
  malformed tube id is a 422, and a retired biosample is reported `missing`).

### Changed

- **`POST /study/lookup-by-accession`, `POST /biosample/lookup-by-accession` and
  `POST /biosample/lookup-by-matrix-tube-id` are deprecated in favor of the routes above
  (#N).** They still work, and the OpenAPI spec marks them deprecated.
- **The OpenAPI response schema of the lookup routes is named `NaturalKeyLookupResponse`
  (#N).** This covers the deprecated routes above and
  `POST /sequencing-run/lookup-by-instrument-run-id`; the JSON they return is unchanged, and
  the former Python model names remain importable from `qiita_common.models`.
