### Added

- **Alignment lifecycle (#N).** Mirrors the mask and assembly lifecycles, system_admin
  only under the new `alignment_definition:lifecycle` scope:
  - `PATCH /alignment-definition/{alignment_idx}/status` deprecates an alignment
    config (or returns it to active). Deprecation is refused (409, listing them)
    while any work ticket under the alignment is unfinished, held ones included. A deprecated alignment is refused by
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
  - The data plane's `delete_alignment` DoAction.

### Changed

- **Deprecating a mask waits for its unfinished work tickets (#N).**
  `PATCH /mask-definition/{mask_idx}/status` to `deprecated` is refused (409, listing
  them) while any work ticket carrying the mask is unfinished, held ones included,
  so no masking run or alignment over its pass-set is still to write under it.

- **The align plan's 409 for an already-gated prep_sample no longer says to delete the
  alignment (#N).** It names the alignment (`alignment_idx` in the body) and the way
  out for each case: abandon the plan for prep_samples left pending by failed blocks,
  deprecate a bad completed alignment. A plan whose config resolves to a deprecated
  alignment is a 409.
- **Reference delete's refusal for an aligned-against reference no longer says to
  delete the alignment (#N).** Alignment data is never purged, so a reference with
  any alignment against it, deprecated or not, cannot be deleted.
- **The service-account alignment DoGet re-checks its cohort (#N).** It returns 409
  when the alignment was deprecated, or a prep_sample invalidated, after the
  feature-table ticket was submitted.
