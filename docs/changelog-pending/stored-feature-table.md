### Added

- **A feature table of stored values over REST.** `POST /feature-table` takes a method
  whose values the lake stores per (processed sample, feature) (the methods in
  `SOURCES`), its parameters, and a study's prep_samples or a named prep_sample list,
  and draws on the method's active runs over that cohort whose settings match; runs
  that disagree on what makes features comparable are refused with the settings found.
  It answers one zip of the table's columns (each processed sample under its QM, with
  the runs' shared settings), its values as sparse COO `(sample, feature, value)`, and
  its features `(feature, sequence, length, md5)`. Features are named through
  `exported_feature` the same way a genome is (a feature Qiita observed itself, such as
  a de-novo ASV, gets a minted `QF`), and the relabel is the one `feature-table build`
  uses. Access is all-or-nothing over the cohort, checked before the data plane is read.
  `qiita feature-table select --method M [--parameter KEY=VALUE]` writes it as a bundle
  with the map needed to read it.
- **Exclusion-aware amplicon read surfaces on the data plane.** The counts are served
  through a new `amplicon_membership_visible` view (`amplicon_membership` anti-joined
  with the resolved blocklist on `feature_idx`), and the ASV sequences resolve the
  cohort through that same view — so a blocked ASV reaches neither a feature table nor
  a published sequence, mirroring `alignment_visible` / `reference_taxonomy_visible`.
  The raw `amplicon_membership` stays off the Flight allowlist.
