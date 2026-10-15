### Changed

- **`POST /biosample/resolve-roster` names each entity by any of its external identifiers, in a
  new request shape (#N).** Each row carries a required `biosample` (any of
  `biosample_accession`, `ena_sample_accession`, `export_entity_id` and `matrix_tube_id`), a
  required `primary_project` and optional `secondary_projects` (each any of
  `bioproject_accession`, `ena_study_accession` and `export_entity_id`). This replaces the flat
  `matrix_tube_id`, `biosample_accession`, `primary_project_accession` and
  `secondary_project_accessions` fields, so a CLI from before this change gets a 422 from the
  route. Every identifier given for an entity must resolve, and all of them must name the same
  entity. The primary and every secondary study must be among the biosample's active study links,
  so a control whose biosample is not linked to a secondary project is refused before anything is
  created, rather than partway through the submission. A primary project is required; the study
  no longer falls back to the biosample's single link. A tube repeated across rows is no longer
  refused, since technical replicates share one. Problems name each identifier by its key.
  `submit-bcl-convert`, `submit-pacbio-ingest` and `submit-golay-demux` send the new shape.
