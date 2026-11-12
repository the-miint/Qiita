### Changed

- **A biosample with no `host_taxon_id` is now a sample with no host (#NNN).** The host-filter
  resolver returns `pass_through` for it instead of `unresolved`, so its pool is no longer
  refused at submit. `host_taxon_id` is no longer a required field
  (`GET /biosample/global-field` reports `required: false`) and a biosample import no longer
  needs it. `not provided` and any other unrecognised missing-reason still resolve
  `unresolved`; `not applicable` is still read as no host but is no longer written. An ENA import
  and `qiita-admin backfill host-taxon-id` leave `host taxon id` out for a hostless taxon, and
  the backfill writes `not provided` for a taxon the curated table does not cover. The
  `qiita submit-host-filter-pool` refusal for an unresolved host points at
  `qiita biosample patch-metadata-by-unique-field` rather than the backfill.

### Removed

- **The 422 `missing required metadata field(s)` from `POST /study/{study_idx}/biosample` (#NNN).**
  No metadata field is enforced as required on import.
