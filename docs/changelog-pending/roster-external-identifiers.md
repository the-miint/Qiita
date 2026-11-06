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
  no longer falls back to the biosample's single link. A miss or a disagreement names each
  identifier by its field name. `submit-bcl-convert`, `submit-pacbio-ingest` and
  `submit-golay-demux` send the new shape.
- **The submit commands read a pre-flight's biosamples and projects by any of their
  identifiers (#N).** `submit-bcl-convert`, `submit-pacbio-ingest` and `submit-golay-demux`
  read the pre-flight through kl-run-preflight's identifier records, so a row whose
  biosample is named only by `biosample_export_entity_id` or `matrix_tube_id`, or whose
  project is named only by `study_export_entity_id`, is accepted. Each sends every
  identifier the pre-flight holds to `POST /biosample/resolve-roster`. A biosample or
  project with no identifier is refused by the library, with the message "missing required
  identifier" in place of "missing required accession". In the `submit-bcl-convert` and
  `submit-golay-demux` output, each sample's `biosample_accession` is replaced by
  `biosample`, which holds the biosample's identifiers from the pre-flight, absent ones
  left out.

### Added

- **`qiita-admin backfill check-stored-preflights` lists each stored run pre-flight that
  the installed kl-run-preflight cannot open (#N).** Opening a stored pre-flight applies
  the library's pending schema patches, and a patch can refuse values the pre-flight
  already holds. Run from a new build before it is deployed, the command names every
  pre-flight that build would make unreadable, with the library's reason, and exits 1 if
  there is one. It writes nothing.

### Removed

- **The submit commands no longer refuse a blank or padded accession themselves (#N).**
  kl-run-preflight now refuses to store an empty identifier, or one with surrounding
  whitespace, so a pre-flight never holds one.
