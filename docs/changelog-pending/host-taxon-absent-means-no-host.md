### Changed

- **A biosample with no `host_taxon_id` now has no host (#864).** The host-filter
  resolver returns `pass_through` for it instead of `unresolved`, so its pool is no longer
  refused at submit. `host_taxon_id` is no longer a required field
  (`GET /biosample-global-field` reports `required: false`) and a biosample import no longer
  needs it. `not provided` and any other unrecognized missing-reason still resolve
  `unresolved`; `not applicable` is still read as no host, and neither an ENA import nor the backfill
  writes it any more. An ENA import
  and `qiita-admin backfill host-taxon-id` leave `host taxon id` out for a hostless taxon, and
  the backfill writes `not provided` for a taxon the curated table does not cover. The
  `qiita submit-host-filter-pool` refusal for an unresolved host points at
  `qiita biosample patch-metadata-by-unique-field` rather than the backfill.
  An absent `host_taxon_id` is still `unresolved` when the biosample's own `taxon_id` implies a
  host (e.g. `human gut metagenome`), and a pool that mixes host-filtered biosamples with
  absent-host ones logs a warning. `qiita-admin backfill host-taxon-id` is no longer
  re-runnable: run it once in this deploy's bucket 2 before the restart, and never after.

### Removed

- **The 422 `missing required metadata field(s)` from `POST /study/{study_idx}/biosample` (#864).**
  No metadata field is enforced as required on import.
