### Added

- **A system_admin can decide that two masks, two alignments or two processings are
  similar, and samples split across them are then read together (#TBD).**
  `qiita-admin pair list|show|open|decide|revoke` calls the new
  `/api/v1/admin/definition-pair` routes, gated on the kind's lifecycle scope. A
  `similar` decision puts the two in one group; alignments and processings whose
  params differ only in grouped masks (or, for de novo alignments, grouped
  processings) are grouped without a decision. Every decision and revoke is kept in
  the pair's history. `docs/runbooks/deciding-similar-definitions.md` is the
  procedure.
- **`POST /api/v1/definition-group/resolve` (#TBD)** reports, per prep_sample, the
  group member its results are read from (the newest one with a run), or that it is
  `incomplete` or `not_processed`, with the reason.
- **Public handles for a mask and for a sample under a mask (#TBD):**
  `POST /api/v1/exported-processing/mask` and `POST /api/v1/exported-identifier/mask`
  (system_admin, `admin:masked_read_export`).
- **`qiita-admin masked-read-export` writes `masked-read-export.manifest.json` (#TBD)**
  mapping each file to the sample's and the mask's public handles, and refuses to
  overwrite a file that manifest records under another mask.

### Changed

- **The masked-read export and the SynDNA count export read each sample from the
  mask of the named mask's group it resolves to (#TBD).** The roster gains `group`
  and per-sample `mask_state`, `state`, `reason`, `resolved_mask_idx` and
  `work_ticket_idx`; the export skips incomplete and not-processed samples, lists
  them on stderr and exits 0. The SynDNA response gains `samples[].mask_idx` and
  `left_out`, and returns the processed samples instead of refusing the request
  when some are not; masks of the group with another SynDNA reference are refused.
  Both refuse a deprecated mask (409), as does the masked-read export ticket.
- **`qiita feature-table build` takes repeated `--sequencing-run-idx` /
  `--sequenced-pool-idx` pairs and builds one table across the alignment's group
  (#TBD).** It reads each sample from the alignment it resolves to and refuses a
  sample that is incomplete or not processed. The manifest's `processing` now holds
  `parts` (one handle, `params_hash` and aligner per alignment read) and
  `sample_part` (each `export_id` to its part's handle), and `table.denovo` the same
  for the de novo arm, in place of the single `export_processing_id`, `params_hash`
  and `aligner`, and `denovo.requested_alignment_idx` / `processing_idx`.
- **The alignment cohort DoGet refuses a sample that does not resolve to the
  alignment it names (#TBD)** (409, `reason` `resolved_elsewhere`, `incomplete` or
  `not_processed`), including a sample whose mask run is withdrawn or deprecated.
- **Deprecating a mask, alignment or processing is refused (409, `similar_pairs`)
  while it is a side of a `similar` pair (#TBD).**
