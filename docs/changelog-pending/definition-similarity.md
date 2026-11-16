### Added

- **A system_admin can decide that two masks, two alignments or two processings are
  similar, and the prep_samples split across them are then read together (#874).**
  `qiita-admin pair list|show|open|decide|revoke` calls the new
  `/api/v1/admin/definition-pair` routes, gated on the kind's lifecycle scope. A
  `similar` decision puts the two in one group; alignments and processings whose
  params differ only in grouped masks (or, for de novo alignments, grouped
  processings) are grouped without a decision. Every decision and revoke is kept in
  the pair's history. `docs/runbooks/deciding-similar-definitions.md` is the
  procedure.
- **Two masks that count SynDNA against different references, or of which only one
  counts it, cannot be decided `similar` (#874)** (409, `syndna_reference_change`);
  `not_similar` is allowed. The SynDNA count export's 409 for masks of different
  references now compares the masks that the prep_samples resolved to, whichever
  member was named, and its 409 for a mask without SynDNA comes after the
  resolution.
- **`POST /api/v1/definition-group/resolve` (#874)** reports, per prep_sample, the
  group member that its results are read from (the newest one with a run), or that it
  is `incomplete` or `not_processed`, with the reason. A processing whose runs it
  does not read per prep_sample (any workflow but `long-read-assembly`) is refused
  (422). The `work_ticket_idx` an `incomplete` prep_sample waits on is returned to a
  system_admin only, here, in the SynDNA count export's `left_out` and in the
  alignment cohort DoGet's 409 `samples`.
- **Public handles for a mask and for a prep_sample under a mask (#874):**
  `POST /api/v1/exported-processing/mask` and `POST /api/v1/exported-identifier/mask`
  (system_admin, `admin:masked_read_export`).
- **`qiita-admin masked-read-export` writes `masked-read-export.manifest.json` (#874)**
  mapping each file that it writes, or finds up to date, to the prep_sample's and the
  mask's public handles, and refuses to overwrite a file that the manifest records
  under another mask. Two exports into one directory merge their entries under a lock.

- **Outputs record the pair decisions they were built under, and can be rebuilt under
  earlier ones (#874).** The resolve route, the masked-read export roster and ticket,
  the SynDNA count export and the alignment cohort DoGet return `decisions`: a
  `sha256:` token of every decided pair's verdict (`decision_set`), when its newest
  decision was recorded, and the count and token of the `similar` pairs that shaped
  the groups it read (`pairs_used`, `pairs_used_set`; `not_similar` pairs are not
  counted). The feature-table manifest, each masked-read export
  manifest entry and the SynDNA sidecar record it. Passing the token back as
  `as_of_event` (`--as-of-event` on `qiita feature-table build`, `qiita-admin
  masked-read-export` and `qiita mask syndna-read-count`) groups the definitions as
  those decisions did; runs, gates, tickets and deprecation are read as they are now.
  A token that names no decision set is refused (422). A feature-table build sends
  the decision set its anchor resolved under with every later resolve and DoGet.

### Changed

- **The masked-read export and the SynDNA count export read each prep_sample from the
  mask of the named mask's group that it resolves to (#874).** Each roster row gains
  `resolution` and the roster gains `group`; the export skips incomplete and
  not-processed prep_samples, lists them on stderr and exits 0. The export ticket
  route refuses (409) a prep_sample that does not resolve to the mask it names. The
  SynDNA response gains `samples[].mask_idx` and `left_out`, and returns the
  processed prep_samples instead of refusing the request when some are not; masks
  of the group with another SynDNA reference are refused. A processed prep_sample
  with no stored counts is left out too, as `not_processed` / `no_syndna_counts`
  (previously the whole request was refused with a 409 naming `qiita-admin backfill
  syndna-read-count`); that 409 now comes only when no processed prep_sample has
  counts.
- **The SynDNA count export records where each count came from (#874).** The route
  mints each counted mask's public handle (`QP<n>`) and returns it as
  `samples[].mask_export_processing_id`, with `left_out_biosample_accession`.
  `qiita mask syndna-read-count` writes `<output>.metadata.json` beside the table:
  each sample_id's biosample accession and mask handle, the left-out prep_samples by
  biosample accession with state and reason, and whether the read was of a
  deprecated mask (`deprecated`, `deprecation_reason`). The Parquet table also gains
  a `mask_export_processing_id` column. The CLI refuses an existing sidecar as it
  refuses an existing `--output`.
- **A deprecated mask named by the masked-read export, the SynDNA count export or
  the two mask-handle mints is refused (409, `reason` `deprecated`) unless the caller
  passes `--include-deprecated` (API `include_deprecated`) (#874).** With it, each
  prep_sample is read from that mask alone, never another of its group: its own
  completed runs (a withdrawn run stays unreadable), and the SynDNA counts are the
  mask's own stored ones. The roster and the SynDNA response are marked `deprecated`
  with the `deprecation_reason`, the CLIs say so on stderr, and each masked-read
  export manifest entry records `"deprecated": true`. For a mask that is not
  deprecated the flag changes nothing. Before this change the SynDNA export did not
  refuse a deprecated mask: it read its group, leaving out each prep_sample that
  resolved to the deprecated mask.
- **A deprecated alignment named by the alignment cohort DoGet, the pool alignment
  cohort listing, `POST /api/v1/exported-identifier`, `POST
  /api/v1/exported-processing` or `qiita feature-table build` (`--alignment-idx` or
  `--denovo-alignment-idx`) is refused (409, `reason` `deprecated`, naming the flag)
  unless the caller passes `--include-deprecated` (API `include_deprecated`) (#874).**
  With it, the cohort DoGet reads that alignment's own completed, non-withdrawn rows,
  never another member of its group, and still refuses a prep_sample whose mask run
  is withdrawn; the build reads it as one part (or one de novo part: the prep_samples
  completed under it have the arm, the others stay reference-only), marks the part
  `deprecated` with the `deprecation_reason` in the manifest and says so on stderr.
  The 409's `detail` is now an object (`detail`, `reason`, `deprecation_reason`)
  where it was the message. For an alignment that is not deprecated the flag changes
  nothing. With the flag, the pool cohort listing leaves out the prep_samples that the
  exact read does not resolve processed (a failed or running run, a withdrawn one, or
  a withdrawn mask run) and lists them in `left_out`; the build records them by
  biosample accession (`table.left_out_by_accession`, `table.denovo.left_out`) instead
  of failing a default-cohort build on them. A prep_sample named with
  `--prep-sample-idx` is still refused. The flag also reads a de novo arm whose
  assembly run is deprecated.
- **An active alignment whose mask, or (de novo) whose assembly run, is deprecated is
  refused up front (409, `reason` `deprecated_upstream`) by the alignment cohort DoGet,
  the pool alignment cohort listing and both alignment mints unless the caller passes
  `--include-deprecated-upstream` (API `include_deprecated_upstream`) (#874).** With
  it the group is resolved without the upstream deprecation checks (withdrawn runs
  stay unreadable); `POST /api/v1/definition-group/resolve` takes the flag too, the
  cohort DoGet response names the deprecated upstreams read (`deprecated_upstreams`),
  and `qiita feature-table build` says so on stderr and records each on the manifest
  part by `params_hash` and reason.
- **`qiita feature-table build` takes repeated `--sequencing-run-idx` /
  `--sequenced-pool-idx` pairs and builds one table across the alignment's group
  (#874).** It reads each prep_sample from the alignment that it resolves to. Over the
  default cohort it leaves out a prep_sample that is incomplete or not processed,
  lists it on stderr with its state and reason, and records it in the manifest's
  `table.left_out` by `export_id`; it fails when nothing is left. Over prep_samples
  named with `--prep-sample-idx` it refuses such a prep_sample. The manifest's
  `processing` now holds `parts` (one handle, `params_hash` and aligner per alignment
  read) and `sample_part` (each `export_id` to its part's handle), and `table.denovo`
  the same for the de novo arm, in place of the single `export_processing_id`,
  `params_hash` and `aligner`, and of `denovo.requested_alignment_idx` /
  `processing_idx`. The manifest gains `schema_version` (2); one without it has the
  earlier shape.
- **The alignment cohort DoGet refuses a prep_sample that does not resolve to the
  alignment it names (#874)** (409, `reason` `resolved_elsewhere`, `incomplete` or
  `not_processed`), including one whose mask run is withdrawn or deprecated. Two of
  these refusals apply with no pair decided: `incomplete` for a prep_sample whose
  gate is completed but whose newest ticket under the alignment, or a block ticket
  covering it, failed or is waiting or running; and `incomplete` / `upstream_moved`
  for one whose mask now resolves to another mask of its group, which a newer
  member's run causes once masks are decided similar.
- **Deprecating a mask, alignment or processing is refused (409, `similar_pairs`)
  while it is a side of a `similar` pair (#874).** The mask and alignment
  deprecation 409 for unfinished work tickets now has the same shape: `reason` is the
  code `live_work` (it was the message), the message moved to `detail`, and
  `work_ticket_idx` is unchanged.
- **A mask's, alignment's or processing's settings are locked after its mint
  (#874).** A hand `UPDATE` of its params, params_hash or (for a mask or a processing)
  workflow and version columns is refused (SQLSTATE 55000); status and deprecation stay
  editable. `qiita-admin definition edit-settings` (`PATCH
  /api/v1/admin/definition-settings/{kind}/{idx}`, system_admin with the kind's
  lifecycle scope) is the one way to change them: it records who, when, the old and new
  values and a required reason in the append-only `qiita.definition_settings_edit`, is
  refused while a `similar` pair names the definition, and recomputes the `change` of
  its pending and not_similar pairs. The mask adapter-hash re-key (the mint's legacy
  path and `qiita-admin backfill mask-adapter-hash`) passes and is recorded with no
  actor.
- **A hand `DELETE` of a mask, alignment or processing is refused (SQLSTATE 55006)
  while a `similar` pair names it (#874);** revoke the pair first. A `pending` or
  `not_similar` pair is detached by the delete. No route deletes these
  rows.
- **Deleting a sequenced pool whose prep_samples have public `QM<n>` handles
  (`exported_identifier`, either kind) succeeds and retires those handles (#874)**
  instead of failing with a 500 on their foreign key. A retired handle keeps its
  kind and records the biosample's accession and public `QB<n>` handle
  (`named_biosample_accession`, `named_biosample_export_entity_id`). The same holds
  for any other delete of a prep_sample.
- **`qiita-admin ticket cancel` (`POST /api/v1/work-ticket/cancel`) also cancels a
  failed ticket named by idx (#874)**, clearing its `failure_*` columns (the control
  plane logs them) and reaping its SLURM jobs as for any cancel. The `--action-id`
  filter still selects only pending, queued and processing tickets. A failed
  `download-ena-study` ticket with a live job, cancelled to reap the job, is now
  `cancelled`, and `/run` on it is admitted.
- **A redrive is refused while a cancel's reap of the SLURM jobs has not succeeded
  (#874).** A failed reap was returned as `reap_error` and not kept. Every cancel,
  whatever the ticket's state, now marks the ticket (`work_ticket.reap_pending_since`,
  `reap_note`) before the scancel: a reap without error clears the mark, a failed reap
  keeps it with the error, and a cancel interrupted mid-reap leaves it until the next
  cancel's reap clears it. While marked, `/run` and `rerun-block` refuse the ticket
  (409, `reap_pending`, saying whether the reap failed or has not finished, and naming
  `qiita-admin ticket cancel <idx>` to reap it again).
- **The definition-group resolver ignores a stale failed ticket (#874):** one created
  before the last change of the prep_sample's gate under that definition, when the
  gate is `completed`, `no_data` or `invalidated`. Withdrawing a run, re-running it
  (which fails at `finalize-mask-sample` because the withdrawn gate does not reopen)
  and restoring the gate now leaves the prep_sample `processed` instead of
  `incomplete`. A failed ticket under a `pending` gate or with no gate row, and a
  waiting or running ticket, count as before.
- **`qiita-admin masked-read-export` names each file `<biosample_accession>.<export_id>`
  (#874)**, where `export_id` is the prep_sample's public handle under the mask it is
  exported from, in place of `<biosample_accession>.<run>.<pool>.<prep_sample>`.
- **`qiita-admin masked-read-export` exits 1 when a prep_sample is left out (#874)**
  (not processed in the group, or its ticket refused with a 409 partway through the
  run, which is now listed and skipped instead of ending the run), unless
  `--allow-partial`, and exits 1 when nothing is exported, including an empty roster.
  The files written are kept and recorded either way.
- **`qiita-admin masked-read-export` refuses an output directory holding an export file
  that its manifest does not record (#874)**, before minting anything, unless
  `--adopt-unrecorded`. Every file of the earlier naming counts. With the flag, each
  such file that the run writes is rewritten from the current resolution and
  recorded, and an earlier-named file is removed once its replacement is written and
  recorded; the run reports how many it replaced and removed.
- **A work ticket records the processing that it minted (`work_ticket.processing_idx`,
  #874)**, and the definition-group resolver reads it to tell which processing a
  `long-read-assembly` ticket ran. A migration sets it on the tickets started before
  the deploy.
