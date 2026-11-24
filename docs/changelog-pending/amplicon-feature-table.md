### Added

- **A run feature table over REST.** `POST /feature-table/{sample,value,feature}`
  answers one cohort request — one or more amplicon runs over a study's prep_samples or a
  named prep_sample list — with the table's columns (each processed sample under its
  public name), its values as sparse COO `(sample, feature, value)`, and its features
  `(feature, sequence, length, md5)`: each ASV under its exported-feature handle, mapped
  to its sequence and reproducible md5. Features are named through `exported_feature`
  the same way a genome is — a de-novo ASV, which names nothing outside its load, gets a
  minted `QF` — so the amplicon and metagenomic tables share one identifier mechanism, not
  two. Values and features are Arrow IPC streams; access is all-or-nothing over the
  cohort, checked before the data plane is read. `qiita feature-table runs` writes the
  three as a bundle with the map needed to read it.
- **Exclusion-aware amplicon read surfaces on the data plane.** The counts are served
  through a new `amplicon_membership_visible` view (`amplicon_membership` anti-joined
  with the resolved blocklist on `feature_idx`), and the ASV sequences resolve the
  cohort through that same view — so a blocked ASV reaches neither a feature table nor
  a published sequence, mirroring `alignment_visible` / `reference_taxonomy_visible`.
  The raw `amplicon_membership` stays off the Flight allowlist.
