### Added

- **Alignment lifecycle (#N).** Mirrors the mask and assembly lifecycles, system_admin
  only under the new `alignment_definition:lifecycle` scope:
  - `PATCH /alignment-definition/{alignment_idx}/status` deprecates an alignment
    config (or returns it to active). A deprecated alignment is refused by
    feature-table submit, both alignment DoGet mints, the exported-identifier and
    exported-processing mints, the de novo align resolver, `/run` on its tickets, and
    the QC verdict. A block alignment's identity has no version, so the same
    reference, aligner and mask resolve to the deprecated row again: re-aligning
    needs a different config.
  - `PATCH /alignment-definition/{alignment_idx}/sample-status` invalidates (or
    restores) individual prep_samples' completed runs.
  - `POST /alignment-definition/{alignment_idx}/abandon-plan` gives up on an align
    plan's unfinished blocks: it cancels their never-released tickets, sets the
    blocks `abandoned`, clears the gates of the prep_samples still pending, and then
    purges those prep_samples' DuckLake rows, so `only_missing=true` re-plans them
    under the same alignment. If the purge fails, calling the route again retries
    it. It refuses while a released block ticket is still running. Completed
    prep_samples are untouched. An abandoned block's ticket cannot be redriven, and
    its failure no longer fail-stops the align fan-out.

### Removed

- **Alignments can no longer be deleted (#N).** Removed:
  - `DELETE /alignment-definition/{alignment_idx}`.
  - The `alignment_definition:delete` scope. A personal access token minted with it
    keeps working; the scope is dropped from its effective set.

### Changed

- **The align plan's 409 for an already-gated prep_sample no longer says to delete the
  alignment (#N).** It names the alignment (`alignment_idx` in the body) and the way
  out for each case: abandon the plan for prep_samples left pending by failed blocks,
  deprecate a bad completed alignment. A plan whose config resolves to a deprecated
  alignment is a 409.
- **Reference delete proceeds past deprecated alignments (#N).** It purges their
  DuckLake `alignment` rows first and keeps their definitions
  (`alignment_rows_deleted` in the response). An active alignment against the
  reference still blocks the delete, now with a message saying to deprecate it; so
  does a deprecated alignment's block ticket that is still running. A deprecated
  alignment whose reference was deleted cannot be returned to active.
- **The service-account alignment DoGet re-checks its cohort (#N).** It returns 409
  when the alignment was deprecated, or a prep_sample invalidated, after the
  feature-table ticket was submitted.
