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
  system_admin only.
- **Public handles for a mask and for a prep_sample under a mask (#874):**
  `POST /api/v1/exported-processing/mask` and `POST /api/v1/exported-identifier/mask`
  (system_admin, `admin:masked_read_export`).
- **`qiita-admin masked-read-export` writes `masked-read-export.manifest.json` (#874)**
  mapping each file that it writes, or finds up to date, to the prep_sample's and the
  mask's public handles, and refuses to overwrite a file that the manifest records
  under another mask. Two exports into one directory merge their entries under a lock.

### Changed

- **The masked-read export and the SynDNA count export read each prep_sample from the
  mask of the named mask's group that it resolves to (#874).** Each roster row gains
  `resolution` and the roster gains `group`; the export skips incomplete and
  not-processed prep_samples, lists them on stderr and exits 0. The export ticket
  route refuses (409) a prep_sample that does not resolve to the mask it names, and,
  with the roster, a deprecated mask. The SynDNA response gains `samples[].mask_idx`
  and `left_out`, and returns the processed prep_samples instead of refusing the
  request when some are not; masks of the group with another SynDNA reference are
  refused.
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
- **A hand `DELETE` of a mask, alignment or processing is refused (SQLSTATE 55006)
  while a `similar` pair names it (#874);** revoke the pair first. A `pending` or
  `not_similar` pair is detached by the delete. No route deletes these
  rows.
- **`qiita-admin ticket cancel` (`POST /api/v1/work-ticket/cancel`) also cancels a
  failed ticket named by idx (#874)**, clearing its `failure_*` columns (the control
  plane logs them) and reaping its SLURM jobs as for any cancel. The `--action-id`
  filter still selects only pending, queued and processing tickets. A failed
  `download-ena-study` ticket with a live job, cancelled to reap the job, is now
  `cancelled`, and `/run` on it is admitted.
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
