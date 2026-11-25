### Added

- **A feature table of stored values over REST.** `POST /feature-table/{sample,value,feature}`
  answers one cohort request — one or more runs of one method whose values the lake
  stores per (processed sample, feature), today amplicon, over a study's prep_samples or a
  named prep_sample list — with the table's columns (each processed sample under its QM),
  its values as sparse COO `(sample, feature, value)`, and its features
  `(feature, sequence, length, md5)`. Features are named through `exported_feature` the
  same way a genome is (a feature Qiita observed itself, such as a de-novo ASV, gets a
  minted `QF`), and the relabel is the one `feature-table build` uses. Values and
  features are Arrow IPC streams; access is all-or-nothing over the cohort, checked
  before the data plane is read. `qiita feature-table select` writes the three as a
  bundle with the map needed to read it.
- **Exclusion-aware amplicon read surfaces on the data plane.** The counts are served
  through a new `amplicon_membership_visible` view (`amplicon_membership` anti-joined
  with the resolved blocklist on `feature_idx`), and the ASV sequences resolve the
  cohort through that same view — so a blocked ASV reaches neither a feature table nor
  a published sequence, mirroring `alignment_visible` / `reference_taxonomy_visible`.
  The raw `amplicon_membership` stays off the Flight allowlist.
