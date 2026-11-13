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
- **`POST /api/v1/definition-group/resolve` (#874)** reports, per prep_sample, the
  group member that its results are read from (the newest one with a run), or that it
  is `incomplete` or `not_processed`, with the reason.
- **Public handles for a mask and for a prep_sample under a mask (#874):**
  `POST /api/v1/exported-processing/mask` and `POST /api/v1/exported-identifier/mask`
  (system_admin, `admin:masked_read_export`).
- **`qiita-admin masked-read-export` writes `masked-read-export.manifest.json` (#874)**
  mapping each file to the prep_sample's and the mask's public handles, and refuses to
  overwrite a file that the manifest records under another mask.

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
  (#874).** It reads each prep_sample from the alignment that it resolves to and
  refuses a prep_sample that is incomplete or not processed. The manifest's
  `processing` now holds `parts` (one handle, `params_hash` and aligner per alignment
  read) and `sample_part` (each `export_id` to its part's handle), and `table.denovo`
  the same for the de novo arm, in place of the single `export_processing_id`,
  `params_hash` and `aligner`, and of `denovo.requested_alignment_idx` /
  `processing_idx`.
- **The alignment cohort DoGet refuses a prep_sample that does not resolve to the
  alignment it names (#874)** (409, `reason` `resolved_elsewhere`, `incomplete` or
  `not_processed`), including one whose mask run is withdrawn or deprecated.
- **Deprecating a mask, alignment or processing is refused (409, `similar_pairs`)
  while it is a side of a `similar` pair (#874).**
